/*
 * IntersectionMultiCursor.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A cursor that implements an intersection of matching elements from a set of cursors all of whom are ordered compatibly.
 * This operates like the {@link IntersectionCursor} except that for each set of matching results, this returns
 * a list of all of the elements from all of the child cursors rather than just one.
 *
 * @param <T> the type of elements returned by the child cursors
 * @see IntersectionCursor
 */
@API(API.Status.EXPERIMENTAL)
public class IntersectionMultiCursor<T> extends IntersectionCursorBase<T, List<T>> {

    private IntersectionMultiCursor(@Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                                    boolean reverse, @Nonnull List<KeyedMergeCursorState<T>> cursorStates,
                                    @Nullable FDBStoreTimer timer) {
        super(comparisonKeyFunction, reverse, cursorStates, timer);

    }

    @Override
    @Nonnull
    List<T> getNextResult(@Nonnull List<KeyedMergeCursorState<T>> cursorStates) {
        List<T> result = new ArrayList<>(cursorStates.size());
        cursorStates.forEach(cursorState -> result.add(cursorState.getResult().get()));
        return result;
    }

    /**
     * Create an intersection cursor from two or more compatibly-ordered cursors.
     * Note that this will throw an error if the list of cursors does not have at least two elements.
     * The returned cursor will return lists of elements, all of which compare equal
     * according to the comparison key function, though they might contain different
     * information outside of what the comparison key function examines. This is useful, for example,
     * if one needs to scan multiple segments of an index and then find all entries
     * that correspond to the same primary key (which can be done by setting a comparison
     * key function that identifies the primary key) and then also apply some filter
     * on all of the entries that come back.
     *
     * <p>
     * Otherwise, this function returns a cursor that is subject to all of the constraints
     * as an {@link IntersectionCursor} returned by its equivalent
     * {@link IntersectionCursor#create(Function, boolean, List, byte[], FDBStoreTimer) create()}
     * method.
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
    public static <T> IntersectionMultiCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            boolean reverse,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        return new IntersectionMultiCursor<>(comparisonKeyFunction, reverse,
                createCursorStates(cursorFunctions, continuation, comparisonKeyFunction), timer);
    }
}
