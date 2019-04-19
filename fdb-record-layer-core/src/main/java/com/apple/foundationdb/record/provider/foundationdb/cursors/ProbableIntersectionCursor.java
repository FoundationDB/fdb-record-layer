/*
 * ProbableIntersectionCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A cursor that returns all results that are probably in all of its children. This differs from the
 * {@link IntersectionCursor} in that it does not require that its children produce results in a compatible order.
 * Just like the {@code IntersectionCursor}, this cursor does require a comparison key function for each entry, but it
 * only uses this comparison key for determining whether two results returned by child cursors are equal.
 *
 * <p>
 * This cursor makes very few guarantees about its results. In particular, just as with the {@link UnorderedUnionCursor},
 * results are returned as they come, so the exact ordering of returned results is determined only at runtime.
 * It also can produce duplicates if the same result appears multiple times in its child cursors.
 * Additionally, in order to support resuming this cursor using a continuation, Bloom filters are used internally
 * to remember which results the cursors have already seen. However, as Bloom filters can report false positives,
 * it is possible that this cursor can return a result that only appears in a proper subset of the child cursors'
 * result sets. The selectivity of the Bloom filter can be adjusted by setting the {@code expectedResults}
 * and {@code falsePositivePercentage} parameters at cursor creation time. These parameters are fed through to the
 * underlying Guava {@link com.google.common.hash.BloomFilter} initializer.
 * </p>
 *
 * <p>
 * However, this cursor does make the following guarantees:
 * </p>
 *
 * <ul>
 *     <li>All results that are actually in the intersection will be in this cursor's result set.</li>
 *     <li>Any result of this cursor is the result of at least one child cursor.</li>
 * </ul>
 *
 * <p>
 * This cursor can therefore be used to select a narrow candidate set of "probable" elements of the intersection
 * and then perform a (possibly more expensive) alternative filter to verify each result. For example, if each
 * child corresponds to satisfying some conjunct of an {@link com.apple.foundationdb.record.query.expressions.AndComponent "and"}
 * predicate by scanning an index, then one could intersect the results with this cursor and then evaluate
 * the predicate on each returned record as a residual filter.
 * </p>
 *
 * @param <T> the type of element returned by this cursor
 * @see com.google.common.hash.BloomFilter
 */
@API(API.Status.EXPERIMENTAL)
public class ProbableIntersectionCursor<T> extends MergeCursor<T, T, ProbableIntersectionCursorState<T>> {
    /**
     * The default number of results to expect to be read from each child cursor of this cursor.
     */
    public static final long DEFAULT_EXPECTED_RESULTS = 500L;
    /**
     * The default acceptable false positive percentage when evaluating whether an element is contained with a given cursor's result set.
     */
    public static final double DEFAULT_FALSE_POSITIVE_PERCENTAGE = 0.01;

    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_INTERSECTION);
    @Nonnull
    private static final Set<StoreTimer.Count> matchesCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_MATCHES);
    @Nonnull
    private static final Set<StoreTimer.Count> nonmatchesCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_NONMATCHES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    ProbableIntersectionCursor(@Nonnull List<ProbableIntersectionCursorState<T>> cursorStates, @Nullable FDBStoreTimer timer) {
        super(cursorStates, timer);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private boolean checkIfInRest(@Nonnull ProbableIntersectionCursorState<T> cursorState) {
        final List<ProbableIntersectionCursorState<T>> cursorStates = getCursorStates();
        final List<Object> key = cursorState.getComparisonKey();
        boolean allContain = true;
        for (ProbableIntersectionCursorState<T> otherCursorState : cursorStates) {
            // Check if all states (beside the source state) contain the element (at least according
            // to the Bloom filter)
            if (otherCursorState != cursorState && !otherCursorState.mightContain(key)) {
                allContain = false;
                break;
            }
        }
        return allContain;
    }

    @Override
    @Nonnull
    CompletableFuture<List<ProbableIntersectionCursorState<T>>> computeNextResultStates() {
        final long startComputingStateTime = System.currentTimeMillis();
        final AtomicReference<ProbableIntersectionCursorState<T>> resultStateRef = new AtomicReference<>();
        return AsyncUtil.whileTrue(() -> whenAny(getCursorStates()).thenApply(vignore -> {
            checkNextStateTimeout(startComputingStateTime);
            final long startTime = System.nanoTime();
            final List<ProbableIntersectionCursorState<T>> cursorStates = getCursorStates();
            boolean allDone = true;
            for (ProbableIntersectionCursorState<T> cursorState : cursorStates) {
                final CompletableFuture<RecordCursorResult<T>> onNextFuture = cursorState.getOnNextFuture();
                if (!MoreAsyncUtil.isCompletedNormally(onNextFuture)) {
                    allDone = false;
                    continue;
                }
                final RecordCursorResult<T> resultState = onNextFuture.join();
                if (resultState.hasNext()) {
                    allDone = false;
                    if (!cursorState.isDefiniteDuplicate() && checkIfInRest(cursorState)) {
                        // We've found an element that is in all of the other states.
                        // Exit immediately with "false" to indicate that the loop can stop.
                        resultStateRef.set(cursorState);
                        if (getTimer() != null) {
                            getTimer().increment(matchesCounts);
                            getTimer().record(duringEvents, System.nanoTime() - startTime);
                        }
                        return false;
                    } else {
                        // This element is missing from at least one child so will not be in the
                        // result set (at least not yet).
                        cursorState.consume();
                        if (getTimer() != null) {
                            getTimer().increment(nonmatchesCounts);
                        }
                    }
                }
            }
            if (getTimer() != null) {
                getTimer().record(duringEvents, System.nanoTime() - startTime);
            }
            return !allDone;
        }), getExecutor()).thenApply(vignore -> {
            if (resultStateRef.get() == null) {
                // All cursors have stopped. Signal this with the empty list.
                return Collections.emptyList();
            } else {
                // At least one cursor still has elements.
                return Collections.singletonList(resultStateRef.get());
            }
        });
    }

    @Override
    @Nonnull
    T getNextResult(@Nonnull List<ProbableIntersectionCursorState<T>> resultStates) {
        return resultStates.get(0).getResult().get();
    }

    @Override
    @Nonnull
    NoNextReason mergeNoNextReasons() {
        return getStrongestNoNextReason(getCursorStates());
    }

    @Override
    @Nonnull
    ProbableIntersectionCursorContinuation getContinuationObject() {
        return ProbableIntersectionCursorContinuation.from(this);
    }

    @Nonnull
    static <T> List<ProbableIntersectionCursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
                                                                           @Nullable byte[] byteContinuation,
                                                                           @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                                                                           long expectedInsertions, double falsePositiveRate) {
        final List<ProbableIntersectionCursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final ProbableIntersectionCursorContinuation continuation = ProbableIntersectionCursorContinuation.from(byteContinuation, cursorFunctions.size());
        int i = 0;
        for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(ProbableIntersectionCursorState.from(cursorFunction, continuation.getContinuations().get(i),
                    comparisonKeyFunction, expectedInsertions, falsePositiveRate));
            i++;
        }
        return cursorStates;
    }

    /**
     * Create a cursor merging the results of two or more cursors. This uses the default values for
     * <code>expectedResults</code> and <code>falsePositivePercentage</code>.
     *
     * @param comparisonKeyFunction the function evaluated to compare elements from different cursors
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by this cursor
     * @return a cursor containing any records from any child cursor
     * @see #create(Function, List, long, double, byte[], FDBStoreTimer)
     */
    @Nonnull
    public static <T> ProbableIntersectionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        return create(comparisonKeyFunction, cursorFunctions, DEFAULT_EXPECTED_RESULTS, DEFAULT_FALSE_POSITIVE_PERCENTAGE, continuation, timer);
    }

    /**
     * Create a cursor merging the results of two or more cursors. The resulting cursor will return all children
     * that are <i>probably</i> in all of the child cursors. Unlike the {@link IntersectionCursor}, this does not require
     * that results be returned in the same order as their values from the comparison key function. However, it
     * does require that if two elements evaluate to the same comparison key that they are equal.
     *
     * <p>
     * Every result from this cursor is guaranteed to be in at least one child, but some results may appear in only
     * a proper subset of the given cursors. This is because a probabilistic data structure is used internally to
     * allow for the cursor to be resumed across continuation boundaries. The caller can adjust the memory usage
     * as well as the false positive rate by tweaking the values of the {@code expectedResults} and
     * {@code falsePositivePercentage} parameters. Note that those parameters only matter if the continuation is <code>null</code>.
     * A non-null continuation will read those parameters based on serialized versions of the data structure that are
     * included as part of the continuation.
     * </p>
     *
     * @param comparisonKeyFunction the function evaluated to compare elements from different cursors
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param expectedResults the expected number of results from each child cursor
     * @param falsePositivePercentage an acceptable false positive percentage for each cursor
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by this cursor
     * @return a cursor containing any records from any child cursor
     */
    @Nonnull
    public static <T> ProbableIntersectionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            long expectedResults,
            double falsePositivePercentage,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        return new ProbableIntersectionCursor<>(createCursorStates(cursorFunctions, continuation, comparisonKeyFunction, expectedResults, falsePositivePercentage), timer);
    }
}
