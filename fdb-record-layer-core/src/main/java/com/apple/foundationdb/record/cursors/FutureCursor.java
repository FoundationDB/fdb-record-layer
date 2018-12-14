/*
 * FutureCursor.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that returns a single element when a future completes.
 * @param <T> the type of elements of the cursor
 */
public class FutureCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final CompletableFuture<T> future;
    private boolean done;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public FutureCursor(@Nonnull Executor executor, @Nonnull CompletableFuture<T> future) {
        this.executor = executor;
        this.future = future;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (done) {
            mayGetContinuation = true;
            return AsyncUtil.READY_FALSE;
        } else {
            mayGetContinuation = false;
            return future.thenApply(t -> {
                mayGetContinuation = false;
                return true;
            });
        }
    }

    @Override
    public boolean hasNext() {
        return !done;
    }

    @Nullable
    @Override
    public T next() {
        if (done) {
            throw new NoSuchElementException();
        } else {
            mayGetContinuation = true;
            done = true;
            return future.join();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return null;
    }

    @Override
    public NoNextReason getNoNextReason() {
        return NoNextReason.SOURCE_EXHAUSTED;
    }

    @Override
    public void close() {
        done = true;
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }
}
