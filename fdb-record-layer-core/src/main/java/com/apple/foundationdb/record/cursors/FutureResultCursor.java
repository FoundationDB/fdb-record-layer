/*
 * FutureResultCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that returns the {@link RecordCursorResult} a future completes to. In contrast to {@link FutureCursor},
 * the future decides what the cursor returns, so it can produce no element at all and stop with a continuation and a
 * {@link RecordCursor.NoNextReason}, for example an out-of-band one that can be resumed later.
 *
 * <p>If the result has a value, the cursor is exhausted afterwards. Otherwise, the cursor keeps returning that result.
 *
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.INTERNAL)
public class FutureResultCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final CompletableFuture<RecordCursorResult<T>> future;
    private boolean closed;

    @Nullable
    private RecordCursorResult<T> nextResult;

    /**
     * Create a cursor.
     *
     * @param executor the executor to associate with this cursor
     * @param future a future that when completed will provide the first result of this cursor
     */
    public FutureResultCursor(@Nonnull final Executor executor,
                              @Nonnull final CompletableFuture<RecordCursorResult<T>> future) {
        this.executor = executor;
        this.future = future;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult == null) {
            return future.thenApply(result -> {
                nextResult = result;
                return result;
            });
        }
        if (nextResult.hasNext()) {
            nextResult = RecordCursorResult.exhausted();
        }
        return CompletableFuture.completedFuture(nextResult);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }
}
