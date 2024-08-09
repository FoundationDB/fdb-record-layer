/*
 * AsyncLockCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.locking.AsyncLock;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A wrapper cursor that manages the locking of resources before operating on inner cursor. Implementation-wise, it
 * requests for the read/write lock from the current {@link FDBRecordContext} and composes the inner {@link RecordCursor#onNext()}
 * on top of the backlog tasks that are locking the resources.
 *
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.MAINTAINED)
public class AsyncLockCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final AsyncLock lock;
    @Nonnull
    private final RecordCursor<T> inner;
    private volatile boolean innerExhausted = false;

    public AsyncLockCursor(@Nonnull final AsyncLock lock, @Nonnull final RecordCursor<T> inner) {
        this.inner = inner;
        this.lock = lock;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (lock.isLockReleased()) {
            if (!innerExhausted && !isClosed()) {
                throw new RecordCoreException("AsyncLockCursor: lock released before the downstream cursor is exhausted or closed.");
            }
        }
        return inner.onNext().whenComplete((result, err) -> {
            if (err != null) {
                close();
            } else if (!result.hasNext()) {
                innerExhausted = true;
                close();
            }
        });
    }

    @Override
    public void close() {
        inner.close();
        lock.release();
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
