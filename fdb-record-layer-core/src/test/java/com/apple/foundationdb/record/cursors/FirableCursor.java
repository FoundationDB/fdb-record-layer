/*
 * FirableCursor.java
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
 * A cursor that wraps another cursor, but it returns an element only after the user calls the
 * "fire" command. This is useful for controlling the order in which cursors return results,
 * which can be used to simulate different scenarios deterministically.
 *
 * @param <T> the type of element returned by this cursor
 */
@API(API.Status.INTERNAL)
public class FirableCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> underlying;
    private boolean mayGetContinuation;
    @Nullable
    private CompletableFuture<Boolean> onHasNextFuture;
    @Nullable
    private RecordCursorResult<T> nextResult;
    @Nonnull
    private CompletableFuture<Void> fireSignal;
    private volatile boolean fireWhenReady;

    public FirableCursor(@Nonnull RecordCursor<T> underlying) {
        this.underlying = underlying;
        this.fireSignal = new CompletableFuture<>();
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        mayGetContinuation = false;
        return fireSignal.thenCompose(vignore -> underlying.onNext()).thenApply(result -> {
            mayGetContinuation = !result.hasNext();
            nextResult = result;
            if (result.hasNext()) {
                synchronized (this) {
                    if (!fireWhenReady) {
                        fireSignal = new CompletableFuture<>();
                    }
                }
            } else {
                // If the underlying cursor is exhausted, don't reset the fire signal
                // as this cursor won't be returning more elements any way.
                fireSignal = AsyncUtil.DONE;
            }
            return result;
        });
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (onHasNextFuture == null) {
            onHasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return onHasNextFuture;
    }

    @Nullable
    @Override
    @Deprecated
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        onHasNextFuture = null;
        mayGetContinuation = true;
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

    /**
     * Allow a single element to be emitted by the cursor.
     */
    public void fire() {
        fireSignal.complete(null);
    }

    /**
     * Allow the cursor to start emitting its child cursors elements
     * as they come.
     */
    public void fireAll() {
        fire();
        synchronized (this) {
            fireWhenReady = true;
            fireSignal = AsyncUtil.DONE;
        }
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return underlying.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            underlying.accept(visitor);
        }
        return visitor.visitLeave(this);
    }
}
