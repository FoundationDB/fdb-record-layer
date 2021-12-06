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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base implementation for a cursor that compares all records from sub-cursors as they execute. All sub-cursors are
 * assumed to return records in compatible sort order. The iteration logs a failure if:
 * <UL>
 * <LI>some record in one of the cursors does not match the reference cursor</LI>
 * <LI>one of the cursors is exhausted while others are not</LI>
 * </UL>
 * The "Reference Cursor" is the cursor whose value is always returned and is assumed to be the "truth".
 *
 * @param <T> the type of elements returned by each child cursor
 * @param <U> the type of elements returned by this cursor
 */
abstract class ComparatorCursorBase<T, U> extends MergeCursor<T, U, KeyedMergeCursorState<T>> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ComparatorCursorBase.class);
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_COMPARATOR);
    @Nonnull
    private static final Set<StoreTimer.Count> matchesCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_COMPARATOR_MATCH);
    @Nonnull
    private static final Set<StoreTimer.Count> mismatchesCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_COMPARATOR_MISMATCH);
    @Nonnull
    private static final Set<StoreTimer.Count> compareCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_COMPARATOR_COMPARED);

    // The "reference plan" whose values are to be compared with the rest of the cursors, and whose values are actually returned
    private final int referencePlanIndex;

    private boolean errorLogged = false;

    @Nonnull
    private final Supplier<String> planStringSupplier;
    @Nonnull
    private final Supplier<Integer> planHashSupplier;

    protected ComparatorCursorBase(@Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                                   @Nullable FDBStoreTimer timer,
                                   final int referencePlanIndex,
                                   @Nonnull final Supplier<String> planStringSupplier,
                                   @Nonnull final Supplier<Integer> planHashSupplier) {
        super(cursorStates, timer);
        this.referencePlanIndex = referencePlanIndex;
        this.planStringSupplier = planStringSupplier;
        this.planHashSupplier = planHashSupplier;
    }

    /**
     * Compute the set of next result states. This would wait for the states to have a value, then compare all values
     * to make sure they are identical.
     * If one of the values doesn't match the reference, or one of the cursors ends prematurely, the cursor logs the
     * failure.
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
            // if all cursors have a value, compare and continue
            if (cursorStates.stream().allMatch(cursorState -> hasNext(cursorState))) {
                compareAllStates(cursorStates);
                // Regardless of comparison result, return all states (comparison logged results)
                return cursorStates;
            }
            // some cursors have no next result, if all are exhausted, we are done
            long exhaustedCount = cursorStates.stream().filter(cursorState -> isSourceExhausted(cursorState)).count();
            if (exhaustedCount == cursorStates.size()) {
                // Success
                return Collections.emptyList();
            }
            // if any (but not all) are exhausted, we fail
            if (exhaustedCount > 0) {
                logTerminationFailure(getReferenceState(cursorStates));
            }
            if (isSourceExhausted(getReferenceState(cursorStates))) {
                // Reference exhausted - we are done
                return Collections.emptyList();
            } else {
                // Reference not exhausted - continue the iteration
                return cursorStates;
            }
        });
    }

    @Nonnull
    protected KeyedMergeCursorState<T> getReferenceState(final List<KeyedMergeCursorState<T>> cursorStates) {
        return cursorStates.get(referencePlanIndex);
    }

    public int getReferencePlanIndex() {
        return referencePlanIndex;
    }

    private boolean compareAllStates(final List<KeyedMergeCursorState<T>> cursorStates) {
        final long startTime = System.nanoTime();

        List<Object> referenceKey = getReferenceState(cursorStates).getComparisonKey();
        for (KeyedMergeCursorState<T> cursorState : cursorStates) {
            // No point comparing the reference key to itself
            if (cursorState.getComparisonKey() == referenceKey) {
                continue;
            }
            int compare = KeyComparisons.KEY_COMPARATOR.compare(cursorState.getComparisonKey(), referenceKey);
            if (compare != 0) {
                logComparisonFailure(referenceKey, cursorState.getComparisonKey());
                return false;
            }

        }
        logCounters(cursorStates, startTime);

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

    private void logComparisonFailure(List<Object> expectedKey, List<Object> actualKey) {
        if (getTimer() != null) {
            getTimer().increment(mismatchesCounts, 1);
            // Only log comparison failure the first time we see it (per continuation)
            if (!errorLogged) {
                errorLogged = true;
                KeyValueLogMessage message = KeyValueLogMessage.build("Cursor Result Comparison Failed",
                        LogMessageKeys.EXPECTED, expectedKey,
                        LogMessageKeys.ACTUAL, actualKey,
                        LogMessageKeys.PLAN_HASH, planHashSupplier.get(),
                        LogMessageKeys.PLAN, planStringSupplier.get());
                logger.error(message.toString());
            }
        }
    }

    private void logTerminationFailure(KeyedMergeCursorState<T> referenceCursorState) {
        if (getTimer() != null) {
            getTimer().increment(mismatchesCounts, 1);
            // Only log failure the first time we see it (per continuation)
            if (!errorLogged) {
                errorLogged = true;
                KeyValueLogMessage message = KeyValueLogMessage.build("Not all cursors are exhausted",
                        LogMessageKeys.EXPECTED, isSourceExhausted(referenceCursorState),
                        LogMessageKeys.PLAN_HASH, planHashSupplier.get(),
                        LogMessageKeys.PLAN, planStringSupplier.get());
                logger.error(message.toString());
            }
        }
    }

    /**
     * Calculate and return the reason the cursor returns a no-next result. At this point, it is assumed that the
     * {@link #computeNextResultStates} method has finished and the cursor has stopped. This can be because all
     * sub-cursors are exhausted, or otherwise they all have non-exhaustion reason. Calculate the weakest reason
     * and return it so that continuation can pick up from that point.
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
                                                                           @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                                                                           int referencePlanIndex) {
        final List<KeyedMergeCursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final ComparatorCursorContinuation continuation = ComparatorCursorContinuation.from(byteContinuation, cursorFunctions.size(), referencePlanIndex);
        int i = 0;
        for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(KeyedMergeCursorState.from(cursorFunction, continuation.getContinuations().get(i), comparisonKeyFunction));
            i++;
        }
        return cursorStates;
    }
}
