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
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that skips a specified number of initial elements.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.MAINTAINED)
public class SkipCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;
    private int skipRemaining;
    @Nullable
    private CompletableFuture<Boolean> nextFuture;

    @Nullable
    private RecordCursorResult<T> nextResult;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

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
                mayGetContinuation = !result.hasNext();
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
        }), getExecutor()).thenApply(vignore -> {
            mayGetContinuation = !nextResult.hasNext();
            return nextResult;
        });
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            mayGetContinuation = false;
            nextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return nextFuture;
    }


    @Nullable
    @Override
    @Deprecated
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        mayGetContinuation = true;
        nextFuture = null;
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
        if (nextFuture != null) {
            nextFuture.cancel(false);
            nextFuture = null;
        }
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
