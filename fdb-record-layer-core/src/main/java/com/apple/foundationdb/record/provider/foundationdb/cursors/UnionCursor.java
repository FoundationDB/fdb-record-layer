/*
 * UnionCursor.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A cursor that implements a union of all the records from a set of cursors, all of whom are ordered compatibly.
 * In this instance, "compatibly ordered" means that all cursors should return results sorted by the "comparison key".
 * For instance, a {@code UnionCursor} of type {@link com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord}
 * might get the primary key of each record as the comparison key function. This is legal as long as all its children
 * also return records ordered by primary key. This cursor removes any duplicates (relying solely on the comparison key
 * to indicate that two elements are equal), and it returns results ordered by the comparison key.
 *
 * <p>
 * If <i>any</i> child of this cursor stops because it hits a limit, then this cursor will also stop. This differs from the
 * behavior of the {@link UnorderedUnionCursor}. This cursor requires <i>all</i> of its children have returned a value
 * before it can determine which value to return next (as otherwise, it does not know what the minimum value is
 * according to the comparison key). If it were to continue returning results, there may be anomalies in the order of
 * returned results across continuation boundaries.
 * </p>
 *
 * @param <T> the type of elements returned by the cursor
 */
@API(API.Status.UNSTABLE)
public class UnionCursor<T> extends UnionCursorBase<T, KeyedMergeCursorState<T>> {
    private final boolean reverse;

    private UnionCursor(boolean reverse, @Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                        @Nullable FDBStoreTimer timer) {
        super(cursorStates, timer);
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    protected CompletableFuture<List<KeyedMergeCursorState<T>>> computeNextResultStates() {
        final List<KeyedMergeCursorState<T>> cursorStates = getCursorStates();
        return whenAll(cursorStates).thenApply(vignore -> {
            boolean anyHasNext = false;
            for (KeyedMergeCursorState<T> cursorState : cursorStates) {
                if (cursorState.getResult().hasNext()) {
                    anyHasNext = true;
                } else if (cursorState.getResult().getNoNextReason().isLimitReached()) {
                    // If any side stopped due to limit reached, need to stop completely,
                    // since might otherwise duplicate ones after that, if other side still available.
                    return Collections.emptyList();
                }
            }
            if (anyHasNext) {
                final long startTime = System.nanoTime();
                List<KeyedMergeCursorState<T>> chosenStates = new ArrayList<>(cursorStates.size());
                List<KeyedMergeCursorState<T>> otherStates = new ArrayList<>(cursorStates.size());
                chooseStates(cursorStates, chosenStates, otherStates);
                logDuplicates(chosenStates, startTime);
                return chosenStates;
            } else {
                return Collections.emptyList();
            }
        });
    }

    @SuppressWarnings("PMD.CloseResource")
    private void chooseStates(@Nonnull List<KeyedMergeCursorState<T>> allStates, @Nonnull List<KeyedMergeCursorState<T>> chosenStates, @Nonnull List<KeyedMergeCursorState<T>> otherStates) {
        List<Object> nextKey = null;
        for (KeyedMergeCursorState<T> cursorState : allStates) {
            final RecordCursorResult<T> result = cursorState.getResult();
            if (result.hasNext()) {
                int compare;
                final List<Object> resultKey = cursorState.getComparisonKey();
                if (nextKey == null) {
                    // This is the first key we've seen, so always chose it.
                    compare = -1;
                } else {
                    // Choose the minimum of the previous minimum key and this next one
                    // If doing a reverse scan, choose the maximum.
                    compare = KeyComparisons.KEY_COMPARATOR.compare(resultKey, nextKey) * (reverse ? -1 : 1);
                }
                if (compare < 0) {
                    // We have a new next key. Reset the book-keeping information.
                    otherStates.addAll(chosenStates);
                    chosenStates.clear();
                    nextKey = resultKey;
                }
                if (compare <= 0) {
                    chosenStates.add(cursorState);
                } else {
                    otherStates.add(cursorState);
                }
            } else {
                otherStates.add(cursorState);
            }
        }
    }

    private void logDuplicates(@Nonnull List<?> chosenStates, long startTime) {
        if (chosenStates.isEmpty()) {
            throw new RecordCoreException("union with additional items had no next states");
        }
        if (getTimer() != null) {
            if (chosenStates.size() == 1) {
                getTimer().increment(uniqueCounts);
            } else {
                // The number of duplicates is the number of minimum states
                // for this value except for the first one (hence the "- 1").
                getTimer().increment(duplicateCounts, chosenStates.size() - 1);
            }
            getTimer().record(duringEvents, System.nanoTime() - startTime);
        }
    }

    @Nonnull
    protected static <T> List<KeyedMergeCursorState<T>> createCursorStates(@Nonnull Function<byte[], RecordCursor<T>> left,
                                                                           @Nonnull Function<byte[], RecordCursor<T>> right,
                                                                           @Nullable byte[] byteContinuation,
                                                                           @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction) {
        final UnionCursorContinuation continuation = UnionCursorContinuation.from(byteContinuation, 2);
        return ImmutableList.of(
                KeyedMergeCursorState.from(left, continuation.getContinuations().get(0), comparisonKeyFunction),
                KeyedMergeCursorState.from(right, continuation.getContinuations().get(1), comparisonKeyFunction));
    }

    @Nonnull
    protected static <T> List<KeyedMergeCursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
                                                                           @Nullable byte[] byteContinuation,
                                                                           @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction) {
        final List<KeyedMergeCursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final UnionCursorContinuation continuation = UnionCursorContinuation.from(byteContinuation, cursorFunctions.size());
        //int i = 0;
        for (int i = 0; i < cursorFunctions.size(); i++) {
        //for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(KeyedMergeCursorState.from(cursorFunctions.get(i), continuation.getContinuations().get(i), comparisonKeyFunction));
            i++;
        }
        return cursorStates;
    }

    /**
     * Create a union cursor from two compatibly-ordered cursors. This cursor
     * is identical to the cursor that would be produced by calling the overload of
     * {@link #create(FDBRecordStoreBase, KeyExpression, boolean, List, byte[]) create()}
     * that takes a list of cursors.
     *
     * @param store record store from which records will be fetched
     * @param comparisonKey the key expression used to compare records from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param left a function to produce the first {@link RecordCursor} from a continuation
     * @param right a function to produce the second {@link RecordCursor} from a continuation
     * @param continuation any continuation from a previous scan
     * @param <M> the type of the Protobuf record elements of the cursor
     * @param <S> the type of record wrapping a record of type <code>M</code>
     * @return a cursor containing any records in any child cursors
     * @see #create(FDBRecordStoreBase, KeyExpression, boolean, List, byte[])
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> UnionCursor<S> create(
            @Nonnull FDBRecordStoreBase<M> store,
            @Nonnull KeyExpression comparisonKey, boolean reverse,
            @Nonnull Function<byte[], RecordCursor<S>> left,
            @Nonnull Function<byte[], RecordCursor<S>> right,
            @Nullable byte[] continuation) {
        return create(
                (S record) -> comparisonKey.evaluateSingleton(record).toTupleAppropriateList(),
                reverse, left, right, continuation, store.getTimer());
    }

    /**
     * Create a union cursor from two compatibly-ordered cursors. This cursor
     * is identical to the cursor that would be produced by calling the overload of
     * {@link #create(Function, boolean, List, byte[], FDBStoreTimer) create()}
     * that takes a list of cursors.
     *
     * @param comparisonKeyFunction the function expression used to compare elements from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param left a function to produce the first {@link RecordCursor} from a continuation
     * @param right a function to produce the second {@link RecordCursor} from a continuation
     * @param byteContinuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by the cursor
     * @return a cursor containing all elements in both child cursors
     */
    @Nonnull
    public static <T> UnionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            boolean reverse,
            @Nonnull Function<byte[], RecordCursor<T>> left,
            @Nonnull Function<byte[], RecordCursor<T>> right,
            @Nullable byte[] byteContinuation,
            @Nullable FDBStoreTimer timer) {
        final List<KeyedMergeCursorState<T>> cursorStates = createCursorStates(left, right, byteContinuation, comparisonKeyFunction);
        return new UnionCursor<>(reverse, cursorStates, timer);
    }

    /**
     * Create a union cursor from two or more compatibly-ordered cursors.
     * As its comparison key function, it will evaluate the provided comparison key
     * on each record from each cursor. This otherwise behaves exactly the same way
     * as the overload of this function that takes a function to extract a comparison
     * key.
     *
     * @param store record store from which records will be fetched
     * @param comparisonKey the key expression used to compare records from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param <M> the type of the Protobuf record elements of the cursor
     * @param <S> the type of record wrapping a record of type <code>M</code>
     * @return a cursor containing any records in any child cursors
     * @see #create(Function, boolean, List, byte[], FDBStoreTimer)
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> UnionCursor<S> create(
            @Nonnull FDBRecordStoreBase<M> store,
            @Nonnull KeyExpression comparisonKey, boolean reverse,
            @Nonnull List<Function<byte[], RecordCursor<S>>> cursorFunctions,
            @Nullable byte[] continuation) {
        return create(
                (S record) -> comparisonKey.evaluateSingleton(record).toTupleAppropriateList(),
                reverse, cursorFunctions, continuation, store.getTimer());
    }

    /**
     * Create a union cursor from two or more compatibly-ordered cursors.
     * Note that this will throw an error if the list of cursors does not have at least two elements.
     * The returned cursor will return any records that appear in any of the provided
     * cursors, preserving order. All cursors must return records in the same order,
     * and that order should be determined by the comparison key function, i.e., if <code>reverse</code>
     * is <code>false</code>, then the records should be returned in ascending order by that key,
     * and if <code>reverse</code> is <code>true</code>, they should be returned in descending
     * order. Additionally, if the comparison key function evaluates to the same value when applied
     * to two records (possibly from different cursors), then those two elements
     * should be equal. In other words, the value of the comparison key should be the <i>only</i>
     * necessary data that need to be extracted from each element returned by the child cursors to
     * perform the union. Additionally, the provided comparison key function should not have
     * any side-effects and should produce the same output every time it is applied to the same input.
     *
     * <p>
     * The cursors are provided as a list of functions rather than a list of {@link RecordCursor}s.
     * These functions should create a new <code>RecordCursor</code> instance with a given
     * continuation appropriate for that cursor type. The value of that continuation will be determined
     * by this function from the <code>continuation</code> parameter for the union
     * cursor as a whole.
     * </p>
     *
     * @param comparisonKeyFunction the function evaluated to compare elements from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param byteContinuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by this cursor
     * @return a cursor containing any records in any child cursors
     */
    @Nonnull
    public static <T> UnionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            boolean reverse,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] byteContinuation,
            @Nullable FDBStoreTimer timer) {
        if (cursorFunctions.size() < 2) {
            throw new RecordCoreArgumentException("not enough child cursors provided to UnionCursor")
                    .addLogInfo(LogMessageKeys.CHILD_COUNT, cursorFunctions.size());
        }
        final List<KeyedMergeCursorState<T>> cursorStates = createCursorStates(cursorFunctions, byteContinuation, comparisonKeyFunction);
        return new UnionCursor<>(reverse, cursorStates, timer);
    }
}
