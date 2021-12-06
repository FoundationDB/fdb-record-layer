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
public class ComparatorCursor<T> extends ComparatorCursorBase<T, T> {

    private ComparatorCursor(@Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                             @Nullable FDBStoreTimer timer,
                             final int referencePlanIndex,
                             @Nonnull final Supplier<String> planStringSupplier,
                             @Nonnull final Supplier<Integer> planHashSupplier) {
        super(cursorStates, timer, referencePlanIndex, planStringSupplier, planHashSupplier);
    }

    @Override
    @Nonnull
    protected T getNextResult(@Nonnull List<KeyedMergeCursorState<T>> cursorStates) {
        return getReferenceState(cursorStates).getResult().get();
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
     * @param planStringSupplier function to return the cursor plan's hash (for logging purposes)
     * @param planHashSupplier function to return the cursor plan's string (for logging purposes)
     *
     * @return a cursor containing same records in all child cursors
     *
     * @see #create(Function, List, byte[], FDBStoreTimer, int, Supplier, Supplier)
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> ComparatorCursor<S> create(
            @Nonnull FDBRecordStoreBase<M> store,
            @Nonnull KeyExpression comparisonKey,
            @Nonnull List<Function<byte[], RecordCursor<S>>> cursorFunctions,
            @Nullable byte[] continuation,
            final int referencePlanIndex,
            @Nonnull final Supplier<String> planStringSupplier,
            @Nonnull final Supplier<Integer> planHashSupplier) {
        return create(
                (S rec) -> comparisonKey.evaluateSingleton(rec).toTupleAppropriateList(),
                cursorFunctions, continuation, store.getTimer(),
                referencePlanIndex, planStringSupplier, planHashSupplier);
    }

    /**
     * Create a comparator cursor from several compatibly-ordered cursors.
     * The returned cursor will the reference results for the reference cursor, preserving order.
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
     * @param planStringSupplier function to return the cursor plan's hash (for logging purposes)
     * @param planHashSupplier function to return the cursor plan's string (for logging purposes)
     *
     * @return a cursor containing all records in all child cursors
     */
    @Nonnull
    public static <T> ComparatorCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer,
            final int referencePlanIndex,
            @Nonnull final Supplier<String> planStringSupplier,
            @Nonnull final Supplier<Integer> planHashSupplier) {
        return new ComparatorCursor<>(createCursorStates(cursorFunctions, continuation, comparisonKeyFunction, referencePlanIndex), timer,
                referencePlanIndex, planStringSupplier, planHashSupplier);
    }
}
