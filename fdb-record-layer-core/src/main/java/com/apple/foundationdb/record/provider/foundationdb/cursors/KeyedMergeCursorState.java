/*
 * KeyedMergeCursorState.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

/**
 * Adds a comparison key and a comparison function to the {@link MergeCursorState} class.
 * @param <T> the type of element returned by the underlying cursor
 */
class KeyedMergeCursorState<T> extends MergeCursorState<T> {
    @Nonnull
    private final Function<? super T, ? extends List<Object>> comparisonKeyFunction;
    @Nullable
    private List<Object> comparisonKey;

    KeyedMergeCursorState(@Nonnull RecordCursor<T> cursor, @Nonnull RecordCursorContinuation continuation,
                          @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction) {
        super(cursor, continuation);
        this.comparisonKeyFunction = comparisonKeyFunction;
    }

    @Override
    protected void handleNextCursorResult(@Nonnull RecordCursorResult<T> cursorResult) {
        super.handleNextCursorResult(cursorResult);
        if (cursorResult.hasNext()) {
            comparisonKey = comparisonKeyFunction.apply(cursorResult.get());
        }
    }

    @Nullable
    public List<Object> getComparisonKey() {
        return comparisonKey;
    }

    @Override
    public void consume() {
        super.consume();
        this.comparisonKey = null;
    }

    @Nonnull
    public static <T> KeyedMergeCursorState<T> from(
            @Nonnull Function<byte[], RecordCursor<T>> cursorFunction,
            @Nonnull RecordCursorContinuation continuation,
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction) {
        if (continuation.isEnd()) {
            return new KeyedMergeCursorState<>(RecordCursor.empty(), RecordCursorEndContinuation.END, comparisonKeyFunction);
        } else {
            return new KeyedMergeCursorState<>(cursorFunction.apply(continuation.toBytes()), continuation, comparisonKeyFunction);
        }
    }
}
