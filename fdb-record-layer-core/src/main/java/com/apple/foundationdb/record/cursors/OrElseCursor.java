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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
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
@API(API.Status.MAINTAINED)
public class OrElseCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<Executor, RecordCursor<T>> func;
    private boolean first;
    @Nullable
    private RecordCursor<T> other;

    @Nullable
    private CompletableFuture<Boolean> hasNextFuture;
    @Nullable
    private RecordCursorResult<T> nextResult;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public OrElseCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<Executor, RecordCursor<T>> func) {
        this.inner = inner;
        this.func = func;
        this.first = true;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        if (first) {
            return inner.onNext().thenCompose(result -> {
                first = false;
                if (result.hasNext() || result.getNoNextReason().isOutOfBand()) {
                    // Either have result or do not know whether to select else yet or not.
                    return CompletableFuture.completedFuture(result);
                } else {
                    other = func.apply(getExecutor());
                    return other.onNext();
                }
            }).thenApply(this::postProcess);
        }
        if (other != null) {
            return other.onNext().thenApply(this::postProcess);
        }
        return inner.onNext().thenApply(this::postProcess);
    }

    // shim to support old continuation style
    @Nonnull
    private RecordCursorResult<T> postProcess(RecordCursorResult<T> result) {
        mayGetContinuation = !result.hasNext();
        nextResult = result;
        return result;
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture == null) {
            mayGetContinuation = false;
            hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return hasNextFuture;
    }

    @Nullable
    @Override
    @Deprecated
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        mayGetContinuation = true;
        hasNextFuture = null;
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
        if (other != null) {
            other.close();
        }
        inner.close();
        if (hasNextFuture != null) {
            hasNextFuture.cancel(false);
        }
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
