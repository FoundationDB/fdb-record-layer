
/*
 * ComparatorCursorBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Base implementation for a cursor that compares all records from sub-cursors as they execute. All sub-cursors are
 * assumed to return
 * records in compatible sort order. The iteration stops and fails if:
 * <UL>
 * <LI>some record in one of the cursors does not match any of the others</LI>
 * <LI>one of the cursors is exhausted while others are not</LI>
 * </UL>
 *
 * @param <T> the type of elements returned by each child cursor
 * @param <U> the type of elements returned by this cursor
 */
abstract class ComparatorCursorBase<T, U> extends MergeCursor<T, U, KeyedMergeCursorState<T>> {
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_COMPARATOR);
    @Nonnull
    private static final Set<StoreTimer.Count> matchesCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_COMPARATOR_MATCH);
    @Nonnull
    private static final Set<StoreTimer.Count> compareCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_COMPARATOR_COMPARED);

    protected ComparatorCursorBase(@Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                                   @Nullable FDBStoreTimer timer) {
        super(cursorStates, timer);
    }

    /**
     * Compute the set of next result states. This would wait for the states to have a value, then compare all values to
     * make sure they are identical.
     * If one of the values doesn't match the others, or one of the cursors ends prematurely, the cursor fails and stops
     * the iteration.
     * Return empty list in case one of the states is done (but not exhausted), signaling that subsequent iteration
     * using a continuation is required.
     *
     * @return the list of states included in the next result
     */
    @Override
    @Nonnull
    protected CompletableFuture<List<KeyedMergeCursorState<T>>> computeNextResultStates() {
        final List<KeyedMergeCursorState<T>> cursorStates = getCursorStates();
        // Wait for all cursors to have a valid state
        return whenAll(cursorStates).thenApply(vignore -> {
            final long startTime = System.nanoTime();
            // if all cursors have a value, compare and continue
            if (cursorStates.stream().allMatch(cursorState -> hasNext(cursorState))) {
                if (compareAllStates(cursorStates)) {
                    // All states are the same, can flow the record up.
                    logCounters(cursorStates, startTime);
                    return cursorStates;
                } else {
                    // Some states are not the same, fail
                    throw new RecordCoreException("Comparison failed for cursor result: Not all results are the same");
                }
            }
            // some cursors have no next result, if all are exhausted, we are done
            long exhaustedCount = cursorStates.stream().filter(cursorState -> isSourceExhausted(cursorState)).count();
            if (exhaustedCount == cursorStates.size()) {
                return Collections.emptyList();
            }
            // if any (but not all) are exhausted, we fail
            if (exhaustedCount > 0) {
                throw new RecordCoreException("Comparison failed for cursor result: Some cursors are exhausted but not all");
            }
            // some are done (but not exhausted), return empty list to signal that the iteration should resume later
            return Collections.emptyList();
        });
    }

    private boolean compareAllStates(final List<KeyedMergeCursorState<T>> cursorStates) {
        List<Object> firstKey = cursorStates.get(0).getComparisonKey();
        for (KeyedMergeCursorState<T> cursorState : cursorStates.subList(1, cursorStates.size())) {
            int compare = KeyComparisons.KEY_COMPARATOR.compare(cursorState.getComparisonKey(), firstKey);
            if (compare != 0) {
                return false;
            }
        }
        return true;
    }

    private boolean hasNext(final KeyedMergeCursorState<T> cursorState) {
        return ((cursorState.getResult() != null) && cursorState.getResult().hasNext());
    }

    private boolean isSourceExhausted(final KeyedMergeCursorState<T> cursorState) {
        if (hasNext(cursorState)) {
            return false;
        } else {
            return cursorState.getResult().getNoNextReason().isSourceExhausted();
        }
    }

    private void logCounters(@Nonnull List<?> states, long startTime) {
        if (getTimer() != null) {
            getTimer().record(duringEvents, System.nanoTime() - startTime);
            getTimer().increment(matchesCounts, 1);
            getTimer().increment(compareCounts, states.size() - 1);
        }
    }

    /**
     * Calculate and return the reason the cursor returns a no-next result. At this point, it is assumed that the
     * {@link #computeNextResultStates} method has finished and the cursor has stopped. This can be because all sub-cursors
     * are exhausted, or otherwise they all have non-exhaustion reason. Calculate the weakest reason and return it so that
     * continuation can pick up from that point.
     *
     * @return the weakest reason for stopping
     */
    @Override
    @Nonnull
    protected NoNextReason mergeNoNextReasons() {
        return getWeakestNoNextReason(getCursorStates());
    }

    @Override
    @Nonnull
    public ComparatorCursorContinuation getContinuationObject() {
        return ComparatorCursorContinuation.from(this);
    }

    @Nonnull
    protected static <T> List<KeyedMergeCursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
                                                                           @Nullable byte[] byteContinuation,
                                                                           @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction) {
        final List<KeyedMergeCursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final ComparatorCursorContinuation continuation = ComparatorCursorContinuation.from(byteContinuation, cursorFunctions.size());
        int i = 0;
        for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(KeyedMergeCursorState.from(cursorFunction, continuation.getContinuations().get(i), comparisonKeyFunction));
            i++;
        }
        return cursorStates;
    }
}
