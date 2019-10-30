/*
 * CursorTestUtils.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Some utility functions for working with cursors that are useful for the various cursor tests.
 */
public class CursorTestUtils {

    /**
     * Create a list of functions for turning lists into {@link RecordCursor}s. This can then be used as
     * input into higher-level cursors. Each function will thread through the continuation its given to the
     * cursor it produces, so it can be used to correctly construct resumable cursors over multiple lists of input.
     *
     * @param lists a list of lists of items
     * @param <T> the type of element returned by each cursor and each list
     * @return a list of functions producing cursors over the original lists
     */
    @Nonnull
    public static <T> List<Function<byte[], RecordCursor<T>>> cursorFunctionsFromLists(@Nonnull List<List<T>> lists) {
        return lists.stream()
                .map(list -> (Function<byte[], RecordCursor<T>>)((byte[] continuation) -> RecordCursor.fromList(list, continuation)))
                .collect(Collectors.toList());
    }

    /**
     * Create a list of functions wrapping existing cursors. This can then be used as input into higher-level
     * cursors. <em>Warning:</em> this method ignores the continuation completely, so care must be taken
     * to make sure that the continuations given to the cursors given as input into this function match the
     * continuation that will eventually be provided to the function.
     *
     * @param cursors a list of cursors
     * @param <T> the type of element returned by each cursor
     * @return a list of functions wrapping the given cursors
     */
    @Nonnull
    public static <T> List<Function<byte[], RecordCursor<T>>> cursorFunctionsFromCursors(@Nonnull List<? extends RecordCursor<T>> cursors) {
        return cursors.stream()
                .map(cursor -> (Function<byte[], RecordCursor<T>>)((byte[] ignore) -> cursor))
                .collect(Collectors.toList());
    }

    @Nonnull
    private static <T> RecordCursorContinuation consumeFirstN(@Nonnull RecordCursor<T> cursor, @Nonnull List<T> dest, int n) {
        if (n <= 0) {
            throw new RecordCoreArgumentException("cannot consume a non-positive number of elements from a cursor");
        }
        RecordCursorContinuation continuation = null;
        for (int i = 0; i < n; i++) {
            RecordCursorResult<T> result = cursor.getNext();
            continuation = result.getContinuation();
            if (result.hasNext()) {
                dest.add(result.get());
            } else {
                break;
            }
        }
        return continuation;
    }

    /**
     * Make a cursor a list while consuming a given number of elements at a time. This will attempt to consume up
     * to {@code batchSize} elements of the cursor, after which it will discard that cursor and then resume from
     * the final result's continuation. If the cursor hits a limit during execution before returning {@code batchSize}
     * results, then the cursor will be likewise resumed using the continuation from the last result (i.e., the
     * result that did not have an element associated with it). The results from all of these cursors will be
     * returned as a list, with the elements of the list in the same order as returned by the cursors.
     *
     * @param cursorFunction a function for producing a cursor (resumed from a continuation)
     * @param batchSize the number of elements to consume between stopping and resuming the cursor
     * @param <T> the type of elements returned by the cursor
     * @return a list of elements returned by the cursor (possibly across multiple invocations)
     */
    @Nonnull
    public static <T> List<T> toListInBatches(@Nonnull Function<byte[], ? extends RecordCursor<T>> cursorFunction, int batchSize) {
        RecordCursorContinuation continuation = RecordCursorStartContinuation.START;
        List<T> dest = new ArrayList<>();
        while (!continuation.isEnd()) {
            try (RecordCursor<T> cursor = cursorFunction.apply(continuation.toBytes())) {
                continuation = consumeFirstN(cursor, dest, batchSize);
            }
        }
        return dest;
    }

    // Static utility class
    private CursorTestUtils() {
    }
}
