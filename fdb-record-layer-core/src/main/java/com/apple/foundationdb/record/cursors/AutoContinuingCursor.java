/*
 * AutoContinuingCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * A wrapper for a cursor to iterate across multiple transactions.
 * @param <T> the type of elements returned by this cursor
 */
@API(API.Status.EXPERIMENTAL)
public class AutoContinuingCursor<T> implements BaseCursor<T> {

    @Nonnull
    private FDBDatabaseRunner runner;
    @Nonnull
    private BiFunction<FDBRecordContext, byte[], RecordCursor<T>> nextCursorGenerator;

    @Nullable
    RecordCursor<T> currentCursor;
    @Nullable
    FDBRecordContext currentContext;

    @Nullable
    private CompletableFuture<Boolean> nextFuture;
    @Nullable
    private RecordCursorResult<T> lastResult;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public AutoContinuingCursor(@Nonnull FDBDatabaseRunner runner,
                                @Nonnull BiFunction<FDBRecordContext, byte[], RecordCursor<T>> nextCursorGenerator) {
        this.runner = runner;
        this.nextCursorGenerator = nextCursorGenerator;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (currentCursor == null) {
            openContextAndGenerateCursor(null);
        }
        return currentCursor.onNext().thenCompose(recordCursorResult -> {
            if (recordCursorResult.noNextButHasContinuation()) {
                currentContext.close();
                openContextAndGenerateCursor(lastResult.getContinuation().toBytes());
                return currentCursor.onNext().thenApply(finalRecordCursorResult -> {
                    if (finalRecordCursorResult.noNextButHasContinuation()) {
                        throw new RecordCoreException("failed to continue the cursor in a new context");
                    }
                    lastResult = finalRecordCursorResult;
                    return lastResult;
                });
            } else {
                lastResult = recordCursorResult;
                return CompletableFuture.completedFuture(lastResult);
            }
        });
    }

    private void openContextAndGenerateCursor(@Nullable byte[] continuation) {
        currentContext = runner.openContext();
        currentCursor = nextCursorGenerator.apply(currentContext, continuation);
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            mayGetContinuation = false;
            nextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return nextFuture;
    }

    @Nullable
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        nextFuture = null;
        mayGetContinuation = true;
        return lastResult.get();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return lastResult.getContinuation().toBytes();
    }

    @Override
    public NoNextReason getNoNextReason() {
        return lastResult.getNoNextReason();
    }

    @Override
    public void close() {
        if (nextFuture != null) {
            nextFuture.cancel(true);
        }
        if (currentContext != null) {
            currentContext.close();
        }
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return runner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }
}
