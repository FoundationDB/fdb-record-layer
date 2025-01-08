/*
 * MapResultCursor.java
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
import java.util.function.Function;

/**
 * A cursor that applies a function to the elements of another cursor.
 * @param <T> the type of elements of the source cursor
 * @param <V> the type of elements of the cursor after applying the function
 */
@API(API.Status.UNSTABLE)
public class MapResultCursor<T, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<RecordCursorResult<T>, RecordCursorResult<V>> func;

    @Nullable
    private RecordCursorResult<V> nextResult;

    /**
     * Internal constructor. Adopters of the library should call {@link RecordCursor#mapResult(Function) inner.mapResult()}
     * or one of its variants instead.
     *
     * @param inner the inner cursor to apply the function on
     * @param func the function to apply
     * @see RecordCursor#map(Function)
     * @see RecordCursor#mapContinuation(Function, ContinuationConvertor, byte[])
     * @see RecordCursor#mapResult(Function)
     */
    @API(API.Status.INTERNAL)
    public MapResultCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<RecordCursorResult<T>, RecordCursorResult<V>> func) {
        this.inner = inner;
        this.func = func;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        return inner.onNext().thenApply(func)
                .thenApply(result -> {
                    nextResult = result;
                    return result;
                });
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return inner.isClosed();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return inner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inner.accept(visitor);
        }
        return visitor.visitLeave(this);
    }
}
