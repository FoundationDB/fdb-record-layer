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

import com.apple.foundationdb.record.RecordCursor;
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
public class MapCursor<T, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<T, V> func;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public MapCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<T, V> func) {
        this.inner = inner;
        this.func = func;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        mayGetContinuation = false;
        return inner.onHasNext().thenApply(hasNext -> {
            mayGetContinuation = !hasNext;
            return hasNext;
        });
    }

    @Nullable
    @Override
    public V next() {
        mayGetContinuation = true;
        return func.apply(inner.next());
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return inner.getContinuation();
    }

    @Override
    public NoNextReason getNoNextReason() {
        return inner.getNoNextReason();
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
