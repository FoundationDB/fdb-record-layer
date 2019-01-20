/*
 * IntersectionCursorBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Common implementation code for performing an intersection shared between
 * the different intersection cursors. In particular, this base cursor
 * is agnostic to the type of result it returns to the users.
 *
 * @param <T> the type of elements returned by each child cursor
 * @param <U> the type of elements returned by this cursor
 */
abstract class IntersectionCursorBase<T, U> extends MergeCursor<T, U, KeyedMergeCursorState<T>> {
    @Nonnull
    private final Function<? super T, ? extends List<Object>> comparisonKeyFunction;
    private final boolean reverse;

    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_INTERSECTION);
    @Nonnull
    private static final Set<StoreTimer.Count> matchesCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_MATCHES);
    @Nonnull
    private static final Set<StoreTimer.Count> nonmatchesCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_NONMATCHES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    protected IntersectionCursorBase(@Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                                     boolean reverse, @Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                                     @Nullable FDBStoreTimer timer) {
        super(cursorStates, timer);
        this.comparisonKeyFunction = comparisonKeyFunction;
        this.reverse = reverse;
    }

    // Identify the list of maximal (and non-maximal) elements from the list of cursor states.
    private void findMaxStates(@Nonnull List<KeyedMergeCursorState<T>> maxStates, @Nonnull List<KeyedMergeCursorState<T>> nonMaxCursors) {
        final List<KeyedMergeCursorState<T>> cursorStates = getCursorStates();
        maxStates.add(cursorStates.get(0));
        List<Object> maxKey = cursorStates.get(0).getComparisonKey();
        for (KeyedMergeCursorState<T> cursorState : cursorStates.subList(1, cursorStates.size())) {
            int compare = KeyComparisons.KEY_COMPARATOR.compare(cursorState.getComparisonKey(), maxKey) * (reverse ? -1 : 1);
            if (compare == 0) {
                maxStates.add(cursorState);
            } else if (compare < 0) {
                // this new cursor is definitely not in the intersection
                nonMaxCursors.add(cursorState);
            } else {
                // the existing cursors in maxCursors are all not in the intersection
                // this new cursor is now the only max cursor
                nonMaxCursors.addAll(maxStates);
                maxStates.clear();
                maxKey = cursorState.getComparisonKey();
                maxStates.add(cursorState);
            }
        }
    }

    /**
     * Compute the next result states for the cursor based on the status of the existing states.
     * This should return a (not necessarily proper) sublist of this cursors cursor states.
     * By default, this assumes the cursors return results in a compatible order based on their
     * comparison key and loops until they all have matching values. Extenders of this class may
     * choose an alternative approach depending on the semantics of that cursor.
     *
     * <p>
     * To indicate that the intersection cursor should stop, this function should either return
     * an empty list of states or a list containing at least one state that does not have a next
     * item.
     * </p>
     *
     * @return the list of states included in the next result
     */
    @Override
    @Nonnull
    protected CompletableFuture<List<KeyedMergeCursorState<T>>> computeNextResultStates() {
        final List<KeyedMergeCursorState<T>> cursorStates = getCursorStates();
        return AsyncUtil.whileTrue(() -> whenAll(cursorStates).thenApply(vignore -> {
            // If any of the cursors do not have a next element, then we are done.
            if (cursorStates.stream().anyMatch(cursorState -> !cursorState.getResult().hasNext())) {
                return false;
            }

            // If everything compares equally, then we have a match and should return it.
            // Otherwise, everything except for the maximum iterator values is guaranteed
            // not to match, so null those values out and then move on.
            final long startTime = System.nanoTime();
            List<KeyedMergeCursorState<T>> maxCursors = new ArrayList<>(cursorStates.size());
            List<KeyedMergeCursorState<T>> nonMaxCursors = new ArrayList<>(cursorStates.size());
            findMaxStates(maxCursors, nonMaxCursors);
            logDuplicates(maxCursors, nonMaxCursors, startTime);
            if (!nonMaxCursors.isEmpty()) {
                // Any non-maximal cursor is definitely not in the intersection,
                // so we can consume those records (which updates their continuations).
                // Then we loop again to see if we pick any up the next go around.
                nonMaxCursors.forEach(KeyedMergeCursorState::consume);
            }
            return !nonMaxCursors.isEmpty();
        }), getExecutor()).thenApply(vignore -> {
            // This waits for all cursor states to return some result, so getResult will return
            // something non-null.
            if (cursorStates.stream().anyMatch(cursorState -> !cursorState.getResult().hasNext())) {
                return Collections.emptyList();
            } else {
                return cursorStates;
            }
        });
    }

    private void logDuplicates(@Nonnull List<?> maxStates, @Nonnull List<?> nonMaxStates, long startTime) {
        if (getTimer() != null) {
            if (nonMaxStates.isEmpty()) {
                // All of the cursors are in the intersection, so this will return a match.
                // Subtract one from the number of cursors in the matching set in
                // order to make it so that the metric represents the number of
                // records *not* returned because they have been matched away in
                // an intersection.
                getTimer().record(duringEvents, System.nanoTime() - startTime);
                getTimer().increment(matchesCounts, maxStates.size() - 1);
            } else {
                getTimer().record(duringEvents, System.nanoTime() - startTime);
                getTimer().increment(nonmatchesCounts, nonMaxStates.size());
            }
        }
    }

    @Nonnull
    protected Function<? super T, ? extends List<Object>> getComparisonKeyFunction() {
        return comparisonKeyFunction;
    }

    /**
     * Merges all of the cursors and whether they have stopped and returns the "weakest" reason for the result to stop.
     * It will return {@link com.apple.foundationdb.record.RecordCursor.NoNextReason#SOURCE_EXHAUSTED} if any
     * of the cursors are exhausted. If any of the cursors have stopped due to an in-band limit, it will return
     * an in-band limit as well. Finally, if all of the stopped cursors have done so due to hitting an out-of-band
     * limit, it will return an out-of-band limit as well. Note that, in practice, because an intersection cursor
     * will return <code>false</code> from <code>onHasNext</code> if any of its child cursors have stopped, it is
     * likely that there are only a small number (maybe one or two) cursors that have actually stopped when this
     * method is called (e.g., the first cursor to exhaust its source or the first cursor to hit a limit imposed
     * by the element scan limiter).
     * @return the weakest reason for stopping
     */
    @Override
    @Nonnull
    protected NoNextReason mergeNoNextReasons() {
        return getWeakestNoNextReason(getCursorStates());
    }

    @Override
    @Nonnull
    public IntersectionCursorContinuation getContinuationObject() {
        return IntersectionCursorContinuation.from(this);
    }

    @Nonnull
    protected static <T> List<KeyedMergeCursorState<T>> createCursorStates(@Nonnull Function<byte[], RecordCursor<T>> left,
                                                                           @Nonnull Function<byte[], RecordCursor<T>> right,
                                                                           @Nullable byte[] byteContinuation,
                                                                           @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction) {
        final IntersectionCursorContinuation continuation = IntersectionCursorContinuation.from(byteContinuation, 2);
        return ImmutableList.of(
                KeyedMergeCursorState.from(left, continuation.getContinuations().get(0), comparisonKeyFunction),
                KeyedMergeCursorState.from(right, continuation.getContinuations().get(1), comparisonKeyFunction));
    }

    @Nonnull
    protected static <T> List<KeyedMergeCursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
                                                                           @Nullable byte[] byteContinuation,
                                                                           @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction) {
        if (cursorFunctions.size() < 2) {
            throw new RecordCoreArgumentException("not enough child cursors provided to IntersectionCursor")
                    .addLogInfo(LogMessageKeys.CHILD_COUNT, cursorFunctions.size());
        }
        final List<KeyedMergeCursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final IntersectionCursorContinuation continuation = IntersectionCursorContinuation.from(byteContinuation, cursorFunctions.size());
        int i = 0;
        for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(KeyedMergeCursorState.from(cursorFunction, continuation.getContinuations().get(i), comparisonKeyFunction));
            i++;
        }
        return cursorStates;
    }
}
