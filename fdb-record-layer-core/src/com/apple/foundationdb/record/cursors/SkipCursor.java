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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
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
public class SkipCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;
    private int skipRemaining;
    private boolean hasNext;
    @Nullable
    private CompletableFuture<Boolean> nextFuture;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public SkipCursor(@Nonnull RecordCursor<T> inner, int skip) {
        this.inner = inner;
        this.skipRemaining = skip;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        mayGetContinuation = false;
        if (skipRemaining <= 0) {
            return inner.onHasNext().thenApply(has -> {
                mayGetContinuation = !has;
                return has;
            });
        }
        if (nextFuture == null) {
            nextFuture = AsyncUtil.whileTrue(() -> inner.onHasNext()
                    .thenApply(innerHasNext -> {
                        if (!innerHasNext) {
                            hasNext = false;
                            return false; // Exhausted while skipping
                        } else if (skipRemaining > 0) {
                            inner.next();
                            skipRemaining--;
                            return true;
                        } else {
                            hasNext = true;
                            return false;
                        }
                    }), getExecutor()).thenApply(vignore -> hasNext);
        }
        return nextFuture.thenApply(has -> {
            mayGetContinuation = !has;
            return has;
        });
    }


    @Nullable
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        mayGetContinuation = true;
        return inner.next();
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
