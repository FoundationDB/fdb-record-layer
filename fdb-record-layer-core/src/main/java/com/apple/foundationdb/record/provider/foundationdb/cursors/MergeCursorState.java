/*
 * MergeCursorState.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorResult;

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A holder for mutable state needed to track one of the children cursors of some merge operation. This includes
 * the cursor itself as well as information tracking the progress including continuation information. Subclasses
 * may add additional information such as a comparison key or value (for ordered cursors).
 *
 * @param <T> the type of elements returned by the underlying cursor
 */
@API(API.Status.INTERNAL)
public class MergeCursorState<T> implements AutoCloseable {
    private final RecordCursor<T> cursor;
    @Nullable
    private CompletableFuture<RecordCursorResult<T>> onNextFuture;
    private RecordCursorContinuation continuation;
    @Nullable
    private RecordCursorResult<T> result;

    protected MergeCursorState(RecordCursor<T> cursor, RecordCursorContinuation continuation) {
        this.cursor = cursor;
        this.continuation = continuation;
    }

    protected void handleNextCursorResult(RecordCursorResult<T> cursorResult) {
        result = cursorResult;
        if (!result.hasNext()) {
            continuation = result.getContinuation(); // no result, so we advanced the cached continuation
        }
    }

    public CompletableFuture<RecordCursorResult<T>> getOnNextFuture() {
        if (onNextFuture == null) {
            onNextFuture = cursor.onNext().thenApply(cursorResult -> {
                handleNextCursorResult(cursorResult);
                return cursorResult;
            });
        }
        return onNextFuture;
    }

    public void consume() {
        // after consuming a element from a cursor, we should never need to query it again,
        // so we update its continuation information now
        onNextFuture = null;
        continuation = Objects.requireNonNull(result, "result should be set before consume() is called").getContinuation();
    }

    /**
     * Return whether this cursor may return a result in the future. In particular, this will return {@code true}
     * if this cursor has either not returned its first result or if the most recent result had a next element.
     * @return whether the cursor might have more results
     */
    public boolean mightHaveNext() {
        return result == null || result.hasNext();
    }

    @Nullable
    public RecordCursorResult<T> getResult() {
        return result;
    }

    @Override
    public void close() {
        cursor.close();
    }

    public boolean isClosed() {
        return cursor.isClosed();
    }

    public Executor getExecutor() {
        return cursor.getExecutor();
    }

    public RecordCursor<T> getCursor() {
        return cursor;
    }

    public RecordCursorContinuation getContinuation() {
        return continuation;
    }

    @SuppressWarnings("NullAway") // NullAway/JSpecify does not currently track @Nullable on array (byte[]) parameters (a null continuation
                                   // intentionally means "start from the beginning").
    public static <T> MergeCursorState<T> from(
            Function<byte[], RecordCursor<T>> cursorFunction,
            RecordCursorContinuation continuation) {
        if (continuation.isEnd()) {
            return new MergeCursorState<>(RecordCursor.empty(), RecordCursorEndContinuation.END);
        } else {
            return new MergeCursorState<>(cursorFunction.apply(continuation.toBytes()), continuation);
        }
    }
}
