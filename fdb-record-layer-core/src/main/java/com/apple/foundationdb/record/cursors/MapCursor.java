/*
 * MapCursor.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that applies a function to the elements of another cursor.
 * @param <T> the type of elements of the source cursor
 * @param <V> the type of elements of the cursor after applying the function
 */
@API(API.Status.MAINTAINED)
public class MapCursor<T, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<T, V> func;

    @Nullable
    private CompletableFuture<Boolean> hasNextFuture;
    @Nullable
    private RecordCursorResult<V> nextResult;
    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public MapCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<T, V> func) {
        this.inner = inner;
        this.func = func;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        mayGetContinuation = false;
        return inner.onNext().thenApply(result -> result.map(func))
                .thenApply(result -> {
                    mayGetContinuation = !result.hasNext();
                    nextResult = result;
                    return result;
                });
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture == null) {
            hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return hasNextFuture;
    }

    @Nullable
    @Override
    @Deprecated
    public V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNextFuture = null;
        mayGetContinuation = true;
        return nextResult.get();
    }

    @Nullable
    @Override
    @Deprecated
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return nextResult.getContinuation().toBytes();
    }

    @Nonnull
    @Override
    @Deprecated
    public NoNextReason getNoNextReason() {
        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return inner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inner.accept(visitor);
        }
        return visitor.visitLeave(this);
    }
}
