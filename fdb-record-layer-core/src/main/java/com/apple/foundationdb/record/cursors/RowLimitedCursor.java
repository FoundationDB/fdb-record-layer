/*
 * RowLimitedCursor.java
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that limits the number of elements that it allows through.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.MAINTAINED)
public class RowLimitedCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;

    private final int limit;
    private int soFar;

    @Nullable
    protected RecordCursorResult<T> nextResult;

    public RowLimitedCursor(@Nonnull RecordCursor<T> inner, int limit) {
        this.inner = inner;
        this.limit = limit;
        this.soFar = 0;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        if (limitReached()) {
            inner.close();
            NoNextReason reason = (!nextResult.hasNext() && nextResult.getContinuation().isEnd())
                                  ? nextResult.getNoNextReason() : NoNextReason.RETURN_LIMIT_REACHED;
            nextResult = RecordCursorResult.withoutNextValue(nextResult.getContinuation(), reason);
            return CompletableFuture.completedFuture(nextResult);
        } else {
            return inner.onNext().thenApply(result -> {
                soFar++;
                nextResult = result;
                return result;
            });
        }
    }

    protected boolean limitReached() {
        return soFar >= limit;
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
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inner.accept(visitor);
        }
        return visitor.visitLeave(this);
    }
}
