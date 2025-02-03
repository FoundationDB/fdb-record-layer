/*
 * IntersectionCursor.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

/**
 * A cursor that implements an intersection of matching elements from a set of cursors all of whom are ordered compatibly.
 * For every matching set, this will return the element from the first child cursor. If each cursor returns something
 * slightly different and the use case requires accessing the different results from each child, then one should use
 * the {@link IntersectionMultiCursor} which returns a list of the child cursor values rather than just the first one.
 *
 * @param <T> the type of elements returned by the cursor
 * @see IntersectionMultiCursor
 */
@API(API.Status.UNSTABLE)
public class IntersectionCursor<T> extends IntersectionCursorBase<T, T> {

    private IntersectionCursor(@Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                               boolean reverse, @Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                               @Nullable FDBStoreTimer timer) {
        super(comparisonKeyFunction, reverse, cursorStates, timer);
    }

    @Override
    protected T getNextResult(@Nonnull List<KeyedMergeCursorState<T>> cursorStates) {
        return cursorStates.get(0).getResult().get();
    }

    /**
     * Create an intersection cursor from two compatibly-ordered cursors.
     * As its comparison key function, it will evaluate the provided comparison key
     * on each record from each cursor. This otherwise behaves exactly the same way
     * as the overload of this function that takes a function to extract a comparison
     * key.
     *
     * @param store record store from which records will be fetched
     * @param comparisonKey the key expression used to compare records from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param left a function to produce the first {@link RecordCursor} from a continuation
     * @param right a function to produce the second {@link RecordCursor} from a continuation
     * @param continuation any continuation from a previous scan
     * @param <M> the type of the Protobuf record elements of the cursor
     * @param <S> the type of record wrapping a record of type <code>M</code>
     * @return a cursor containing all records in both child cursors
     * @see #create(Function, boolean, Function, Function, byte[], FDBStoreTimer)
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> IntersectionCursor<S> create(
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
     * Create an intersection cursor from two compatibly-ordered cursors. This cursor
     * is identical to the cursor that would be produced by calling the overload of
     * {@link #create(Function, boolean, List, byte[], FDBStoreTimer) create()}
     * that takes a list of cursors.
     *
     * @param comparisonKeyFunction the function expression used to compare elements from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param left a function to produce the first {@link RecordCursor} from a continuation
     * @param right a function to produce the second {@link RecordCursor} from a continuation
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by the cursor
     * @return a cursor containing all elements in both child cursors
     * @see #create(Function, boolean, List, byte[], FDBStoreTimer)
     */
    @Nonnull
    public static <T> IntersectionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            boolean reverse,
            @Nonnull Function<byte[], RecordCursor<T>> left,
            @Nonnull Function<byte[], RecordCursor<T>> right,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        return new IntersectionCursor<>(comparisonKeyFunction, reverse, createCursorStates(left, right, continuation, comparisonKeyFunction), timer);
    }

    /**
     * Create an intersection cursor from two or more compatibly-ordered cursors.
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
     * @return a cursor containing all records in all child cursors
     * @see #create(Function, boolean, List, byte[], FDBStoreTimer)
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> IntersectionCursor<S> create(
            @Nonnull FDBRecordStoreBase<M> store,
            @Nonnull KeyExpression comparisonKey, boolean reverse,
            @Nonnull List<Function<byte[], RecordCursor<S>>> cursorFunctions,
            @Nullable byte[] continuation) {
        return create(
                (S record) -> comparisonKey.evaluateSingleton(record).toTupleAppropriateList(),
                reverse, cursorFunctions, continuation, store.getTimer());
    }

    /**
     * Create an intersection cursor from two or more compatibly-ordered cursors.
     * Note that this will throw an error if the list of cursors does not have at least two elements.
     * The returned cursor will return all elements that appear in all of the provided
     * cursors, preserving order. All cursors must return elements in the same order,
     * and that order should be determined by the comparison key function, i.e., if <code>reverse</code>
     * is <code>false</code>, then the elements should be returned in ascending order by that key,
     * and if <code>reverse</code> is <code>true</code>, they should be returned in descending
     * order. Additionally, if the comparison key function evaluates to the same value when applied
     * to two elements (possibly from different cursors), then those two elements
     * should be equal. In other words, the value of the comparison key should be the <i>only</i>
     * necessary data that need to be extracted from each element returned by the child cursors to
     * perform the intersection. Additionally, the provided comparison key function should not have
     * any side-effects and should produce the same output every time it is applied to the same input.
     *
     * <p>
     * The cursors are provided as a list of functions rather than a list of {@link RecordCursor}s.
     * These functions should create a new <code>RecordCursor</code> instance with a given
     * continuation appropriate for that cursor type. The value of that continuation will be determined
     * by this function from the <code>continuation</code> parameter for the intersection
     * cursor as a whole.
     * </p>
     *
     * @param comparisonKeyFunction the function evaluated to compare elements from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by this cursor
     * @return a cursor containing all records in all child cursors
     */
    @Nonnull
    public static <T> IntersectionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            boolean reverse,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        return new IntersectionCursor<>(comparisonKeyFunction, reverse, createCursorStates(cursorFunctions, continuation, comparisonKeyFunction), timer);
    }
}
