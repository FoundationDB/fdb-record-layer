/*
 * SkipCursor.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that skips a specified number of initial elements.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.UNSTABLE)
public class SkipCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;
    private int skipRemaining;

    @Nullable
    private RecordCursorResult<T> nextResult;

    public SkipCursor(@Nonnull RecordCursor<T> inner, int skip) {
        this.inner = inner;
        this.skipRemaining = skip;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        if (skipRemaining <= 0) {
            return inner.onNext().thenApply(result -> {
                nextResult = result;
                return result;
            });
        }
        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            nextResult = innerResult;
            if (innerResult.hasNext() && skipRemaining > 0) {
                skipRemaining--;
                return true;
            }
            return false; // Exhausted while skipping or found the result
        }), getExecutor()).thenApply(vignore -> nextResult);
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
