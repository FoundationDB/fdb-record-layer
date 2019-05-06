/*
 * FilterCursor.java
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
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that filters elements using a predicate.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.MAINTAINED)
public class FilterCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<T, Boolean> pred;
    private boolean hasNext;
    @Nullable
    private CompletableFuture<Boolean> nextFuture;
    @Nullable
    private RecordCursorResult<T> nextResult;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public FilterCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<T, Boolean> pred) {
        this.inner = inner;
        this.pred = pred;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        mayGetContinuation = false;
        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            nextResult = innerResult;
            hasNext = innerResult.hasNext() && (Boolean.TRUE.equals(pred.apply(innerResult.get()))); // relies on short circuiting
            return innerResult.hasNext() && !hasNext; // keep looping only if we might find more records and we filtered a record out
        }), getExecutor()).thenApply(vignore -> {
            mayGetContinuation = !hasNext;
            return nextResult;
        });
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
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
        nextFuture = null;
        mayGetContinuation = true;
        return nextResult.get();
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
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
