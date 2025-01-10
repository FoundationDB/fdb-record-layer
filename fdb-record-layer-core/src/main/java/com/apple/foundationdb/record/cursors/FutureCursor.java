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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that returns a single element when a future completes.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.UNSTABLE)
public class FutureCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final CompletableFuture<T> future;
    private boolean done;

    @Nullable
    private RecordCursorResult<T> nextResult;

    @Nonnull
    private static final RecordCursorContinuation notDoneContinuation = ByteArrayContinuation.fromNullable(new byte[] {0});

    public FutureCursor(@Nonnull Executor executor, @Nonnull CompletableFuture<T> future) {
        this.executor = executor;
        this.future = future;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (done) {
            nextResult = RecordCursorResult.exhausted();
            return CompletableFuture.completedFuture(nextResult);
        }
        return future.thenApply(t -> {
            done = true;
            nextResult = RecordCursorResult.withNextValue(t, notDoneContinuation);
            return nextResult;
        });
    }

    @Override
    public void close() {
        done = true;
    }

    @Override
    public boolean isClosed() {
        return done;
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
