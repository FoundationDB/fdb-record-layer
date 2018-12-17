/*
 * OrElseCursor.java
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
import java.util.function.Function;

/**
 * A cursor that returns the elements of one cursor followed by the elements of another cursor if the first was empty.
 * @param <T> the type of elements of the cursor
 */
public class OrElseCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<Executor, RecordCursor<T>> func;
    private boolean first;
    @Nullable
    private CompletableFuture<Boolean> firstFuture;
    @Nullable
    private RecordCursor<T> other;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public OrElseCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<Executor, RecordCursor<T>> func) {
        this.inner = inner;
        this.func = func;
        this.first = true;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        mayGetContinuation = false;
        if (first) {
            if (firstFuture == null) {
                firstFuture = inner.onHasNext().thenCompose(hasNext -> {
                    if (hasNext) {
                        return AsyncUtil.READY_TRUE;
                    } else {
                        if (inner.getNoNextReason().isOutOfBand()) {
                            // Do not know whether to select else yet or not.
                            return AsyncUtil.READY_FALSE;
                        }
                        other = func.apply(getExecutor());
                        return other.onHasNext();
                    }
                });
            }
            return firstFuture.thenApply(hasNext -> {
                mayGetContinuation = !hasNext;
                return hasNext;
            });
        }
        if (other != null) {
            return other.onHasNext().thenApply(hasNext -> {
                mayGetContinuation = !hasNext;
                return hasNext;
            });
        }
        return inner.onHasNext().thenApply(hasNext -> {
            mayGetContinuation = !hasNext;
            return hasNext;
        });
    }

    @Nullable
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        mayGetContinuation = true;
        if (first) {
            firstFuture = null;
            first = false;
        }
        if (other != null) {
            return other.next();
        }
        return inner.next();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        if (other != null) {
            return other.getContinuation();
        }
        return inner.getContinuation();
    }

    @Override
    public NoNextReason getNoNextReason() {
        if (other != null) {
            return other.getNoNextReason();
        }
        return inner.getNoNextReason();
    }

    @Override
    public void close() {
        if (other != null) {
            other.close();
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
