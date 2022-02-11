/*
 * ComparatorCursor.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.protobuf.Message;
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
 * A cursor that implements a comparison of matching elements from a set of cursors all of whom are ordered compatibly.
 * For each record returned by the sub-cursors, this cursor will ensure that they are the same (by using the comparison
 * function). The cursor will log and continue if the records are not the same or if one of the sub-cursors ends
 * prematurely.
 * The cursor defines a "reference cursor" whose results are assumed to be correct. Values are compared to this
 * cursor's results, and the reference cursor's results are the ones that are actually returned.
 *
 * @param <T> the type of elements returned by the cursor
 */
@API(API.Status.EXPERIMENTAL)
public class ComparatorCursor<T> extends MergeCursor<T, T, KeyedMergeCursorState<T>> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ComparatorCursor.class);
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
    @Nonnull
    private final Supplier<String> planStringSupplier;
    @Nonnull
    private final Supplier<Integer> planHashSupplier;
    private boolean errorLogged = false;
    private final boolean abortOnComparisonFailure;

    private ComparatorCursor(@Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                             @Nullable FDBStoreTimer timer,
                             final int referencePlanIndex,
                             final boolean abortOnComparisonFailure,
                             @Nonnull final Supplier<String> planStringSupplier,
                             @Nonnull final Supplier<Integer> planHashSupplier) {
        super(cursorStates, timer);
        this.referencePlanIndex = referencePlanIndex;
        this.abortOnComparisonFailure = abortOnComparisonFailure;
        this.planStringSupplier = planStringSupplier;
        this.planHashSupplier = planHashSupplier;
    }

    /**
     * Create a comparator cursor from several compatibly-ordered cursors.
     * As its comparison key function, it will evaluate the provided comparison key
     * on each record from each cursor. This otherwise behaves exactly the same way
     * as the overload of this function that takes a function to extract a comparison
     * key.
     *
     * @param <M> the type of the Protobuf record elements of the cursor
     * @param <S> the type of record wrapping a record of type <code>M</code>
     * @param store record store from which records will be fetched
     * @param comparisonKey the key expression used to compare records from different cursors
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param referencePlanIndex the index of the reference cursor in the given list of sub-cursors
     * @param abortOnComparisonFailure whether to abort the execution and throw an exception when encountering a comparison failure
     * @param planStringSupplier function to return the cursor plan's hash (for logging purposes)
     * @param planHashSupplier function to return the cursor plan's string (for logging purposes)
     *
     * @return a cursor containing same records in all child cursors
     *
     * @see #create(Function, List, byte[], FDBStoreTimer, int, boolean, Supplier, Supplier)
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> ComparatorCursor<S> create(
            @Nonnull FDBRecordStoreBase<M> store,
            @Nonnull KeyExpression comparisonKey,
            @Nonnull List<Function<byte[], RecordCursor<S>>> cursorFunctions,
            @Nullable byte[] continuation,
            final int referencePlanIndex,
            final boolean abortOnComparisonFailure,
            @Nonnull final Supplier<String> planStringSupplier,
            @Nonnull final Supplier<Integer> planHashSupplier) {
        return create(
                (S rec) -> evaluateKey(comparisonKey, rec),
                cursorFunctions, continuation, store.getTimer(),
                referencePlanIndex, abortOnComparisonFailure,
                planStringSupplier, planHashSupplier);
    }

    /**
     * Create a comparator cursor from several compatibly-ordered cursors.
     * The returned cursor will return the reference results for the reference cursor, preserving order.
     * All cursors must return elements in the same order.
     * Additionally, if the comparison key function evaluates to the same value when applied
     * to two elements (possibly from different cursors), then those two elements
     * should be equal. In other words, the value of the comparison key should be the <i>only</i>
     * necessary data that need to be extracted from each element returned by the child cursors to
     * perform the comparison. Additionally, the provided comparison key function should not have
     * any side-effects and should produce the same output every time it is applied to the same input.
     *
     * <p>
     * The cursors are provided as a list of functions rather than a list of {@link RecordCursor}s.
     * These functions should create a new <code>RecordCursor</code> instance with a given
     * continuation appropriate for that cursor type. The value of that continuation will be determined
     * by this function from the <code>continuation</code> parameter for the
     * cursor as a whole.
     * </p>
     *
     * @param <T> the type of elements returned by this cursor
     * @param comparisonKeyFunction the function evaluated to compare elements from different cursors
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param referencePlanIndex the index of the reference cursor in the given list of sub-cursors
     * @param abortOnComparisonFailure whether to abort the execution and throw an exception when encountering a comparison failure
     * @param planStringSupplier function to return the cursor plan's hash (for logging purposes)
     * @param planHashSupplier function to return the cursor plan's string (for logging purposes)
     *
     * @return a cursor that contains the same records as the reference cursor (Note the side effect of logging comparison errors).
     */
    @Nonnull
    public static <T> ComparatorCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer,
            final int referencePlanIndex,
            final boolean abortOnComparisonFailure,
            @Nonnull final Supplier<String> planStringSupplier,
            @Nonnull final Supplier<Integer> planHashSupplier) {
        return new ComparatorCursor<>(createCursorStates(cursorFunctions, continuation, comparisonKeyFunction, referencePlanIndex), timer,
                referencePlanIndex, abortOnComparisonFailure, planStringSupplier, planHashSupplier);
    }

    /**
     * Calculate and return the reason the cursor returns a no-next result. At this point, it is assumed that the
     * {@link #computeNextResultStates} method has finished and the cursor has stopped. This can be because all
     * sub-cursors are exhausted; otherwise they will all have non-exhaustion reasons. Calculate the strongest reason
     * and return it so that continuation can pick up from that point.
     *
     * @return the strongest reason for stopping
     */
    @Override
    @Nonnull
    protected NoNextReason mergeNoNextReasons() {
        return getStrongestNoNextReason(getCursorStates());
    }

    @Override
    @Nonnull
    public ComparatorCursorContinuation getContinuationObject() {
        return ComparatorCursorContinuation.from(this);
    }

    public int getReferencePlanIndex() {
        return referencePlanIndex;
    }

    @Override
    @Nonnull
    protected T getNextResult(@Nonnull List<KeyedMergeCursorState<T>> cursorStates) {
        return getReferenceState(cursorStates).getResult().get();
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
            // some cursors have no next value. If any has out-of-band state - give them a chance to catch up
            if (cursorStates.stream().anyMatch(cursorState -> isOutOfBand(cursorState))) {
                return Collections.emptyList();
            }
            // all cursors are either exhausted or reached in-band limit
            // if all are exhausted, we are done
            long exhaustedCount = cursorStates.stream().filter(cursorState -> isSourceExhausted(cursorState)).count();
            if (exhaustedCount == cursorStates.size()) {
                // Success
                return Collections.emptyList();
            }
            // if any (but not all) are exhausted, we fail, since at least one of them is exhausted and at least one
            // has reached row limit (therefore comparison failed)
            if (exhaustedCount > 0) {
                logTerminationFailure(getReferenceState(cursorStates));
                if (abortOnComparisonFailure) {
                    throw new RecordCoreException("Not all cursors are exhausted")
                            .addLogInfo(LogMessageKeys.EXPECTED, isSourceExhausted(getReferenceState(cursorStates)))
                            .addLogInfo(LogMessageKeys.PLAN_HASH, planHashSupplier.get());
                }
            }
            if (!hasNext(getReferenceState(cursorStates))) {
                // Reference done - we are done
                return Collections.emptyList();
            } else {
                // Reference not done - continue the iteration
                return cursorStates;
            }
        });
    }

    @Nonnull
    protected KeyedMergeCursorState<T> getReferenceState(final List<KeyedMergeCursorState<T>> cursorStates) {
        return cursorStates.get(referencePlanIndex);
    }

    @Nonnull
    private static <T> List<KeyedMergeCursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
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

    /**
     * The evaluation function that evaluates the comparison key over a record to return the key value.
     * For the most part, this function evaluates to something similar to {@link KeyExpression#evaluateSingleton(FDBRecord)}.
     * IN the case of nested repeated fields, though, this function will return a value that will *always fail*
     * comparison.
     * The reason is, in the cases of nested repeated fields, we cannot yet perform proper comparison since we don't
     * yet have the ability to compare multiple keys for equality.
     *
     * @param comparisonKey the key to apply to the record
     * @param rec the record in question
     * @param <M> the type of encoding for the record
     * @param <S> the type of record
     *
     * @return the result of applying the key to the record, for the purpose of comparison to other records
     */
    @Nonnull
    private static <M extends Message, S extends FDBRecord<M>> List<Object> evaluateKey(final @Nonnull KeyExpression comparisonKey, final S rec) {
        final List<Key.Evaluated> keys = comparisonKey.evaluate(rec);
        if (keys.size() != 1) {
            return Collections.singletonList(new Unequal());
        } else {
            return keys.get(0).toTupleAppropriateList();
        }
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
                if (abortOnComparisonFailure) {
                    throw new RecordCoreException("Comparison of plans failed")
                            .addLogInfo(LogMessageKeys.EXPECTED, referenceKey)
                            .addLogInfo(LogMessageKeys.ACTUAL, cursorState.getComparisonKey())
                            .addLogInfo(LogMessageKeys.PLAN_HASH, planHashSupplier.get());
                } else {
                    return false;
                }
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

    private boolean isOutOfBand(final KeyedMergeCursorState<T> cursorState) {
        if (hasNext(cursorState)) {
            return false;
        } else {
            return cursorState.getResult().getNoNextReason().isOutOfBand();
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
     * An internal class that is never equal to anything (other than itself).
     */
    private static class Unequal implements Comparable<Object> {
        @Override
        public int compareTo(@Nonnull final Object o) {
            if (this == o) {
                return 0;
            } else {
                return 1;
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            return super.equals(o);
        }
    }
}
