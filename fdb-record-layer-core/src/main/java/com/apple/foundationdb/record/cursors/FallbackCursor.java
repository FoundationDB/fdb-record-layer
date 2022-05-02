/*
 * FallbackCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import java.util.function.Supplier;

/**
 * provide an alternative cursor in case the primary cursor fails. This cursor has an <code>inner</code> cursor that is
 * used to return the results in the sunny day scenarios, and an alternative provider of a <code>fallback</code> cursor
 * that will be used if the primary cursor encounters an error.
 * Note that there are business rules around when a fallback may happen. For example, because of the way records are
 * returned to the consumer, once a record is returned from the primary cursor, no fallback may be permitted: This is
 * done in order to prevent the case where a few records are returned, then a failure happens and the fallback cursor
 * starts again from the beginning, resulting in duplicate records being returned.
 * In practice, since many errors are observed when the request is sent to FDB (which coincide with the cursor's
 * first <code>onNext()</code> call, many such failures weill be caught by that first result.
 *
 * @param <T> the type of cursor result returned by the cursor
 */
@API(API.Status.MAINTAINED)
public class FallbackCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Supplier<RecordCursor<T>> fallbackCursorSupplier;
    @Nullable
    private final Executor executor;
    @Nonnull
    private RecordCursor<T> inner;
    @Nullable
    private RecordCursorResult<T> nextResult;
    @Nullable
    private CompletableFuture<RecordCursorResult<T>> nextResultFuture;

    private boolean alreadyFailed = false;
    private boolean allowedToFail = true;

    public FallbackCursor(@Nonnull RecordCursor<T> inner, @Nonnull Supplier<RecordCursor<T>> fallbackCursorSupplier) {
        this(inner, fallbackCursorSupplier, null);
    }

    /**
     * Creates a new fallback cursor.
     *
     * @param inner the primary (default) cursor to be used when results are successfully returned
     * @param fallbackCursorSupplier the fallback cursor provider to be used when the primary cursor fails
     * @param executor if not null, then this executor will be returned from {@link #getExecutor()}, otherwise
     * the executor from the <code>inner</code> cursor
     */
    public FallbackCursor(@Nonnull RecordCursor<T> inner, @Nonnull Supplier<RecordCursor<T>> fallbackCursorSupplier, @Nullable Executor executor) {
        this.inner = inner;
        this.fallbackCursorSupplier = fallbackCursorSupplier;
        this.executor = executor;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        // The first stage (whenComplete) will calculate the result of the operation if successful, or replace the inner
        // with the fallback cursor if failed, and store s future to the result in the atomic reference.
        // The second stage (thenCompose) will return the content of the atomic reference once the first stage is done.
        return inner.onNext().handle((result, throwable) -> {
            if (result != null) {
                nextResultFuture = CompletableFuture.completedFuture(result);
                // Cannot fail after the first result was delivered
                allowedToFail = false;
            } else if (throwable != null) {
                if (alreadyFailed || !allowedToFail) {
                    nextResultFuture = CompletableFuture.failedFuture(throwable);
                } else {
                    inner = fallbackCursorSupplier.get();
                    nextResultFuture = inner.onNext();
                }
                alreadyFailed = true;
            } else {
                nextResultFuture = CompletableFuture.completedFuture(null);
            }
            return null; // return value is ignored by next stage
        }).thenCompose(vignore -> nextResultFuture);
    }

    @Override
    public void close() {
        inner.close();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        if (this.executor == null) {
            return getInner().getExecutor();
        }
        return this.executor;
    }

    private RecordCursor<T> getInner() {
        return inner;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            getInner().accept(visitor);
        }
        return visitor.visitLeave(this);
    }
}
