/*
 * EmptyCursor.java
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link RecordCursor} that always returns zero items.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.UNSTABLE)
public class EmptyCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Executor executor;
    private boolean closed = false;

    public EmptyCursor(@Nonnull Executor executor) {
        this.executor = executor;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
    }

    @Nonnull
    @Override
    public RecordCursorResult<T> getNext() {
        return RecordCursorResult.exhausted();
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
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }
}
