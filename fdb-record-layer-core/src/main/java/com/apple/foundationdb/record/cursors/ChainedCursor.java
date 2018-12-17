/*
 * ChainedCursor.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that produces items by applying a given function to the previous item.
 * @param <T> the type of elements of the cursor
 */
public class ChainedCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Function<Optional<T>, CompletableFuture<Optional<T>>> nextGenerator;
    @Nonnull
    private final Function<T, byte[]> getContinuation;
    @Nonnull
    private final Executor executor;
    @Nullable
    private CompletableFuture<Optional<T>> nextFuture;
    @Nonnull
    private Optional<T> lastValue;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public ChainedCursor(
            @Nonnull Function<Optional<T>, CompletableFuture<Optional<T>>> nextGenerator,
            @Nonnull Function<T, byte[]> getContinuation,
            @Nonnull Function<byte[], T> getContinueValue,
            @Nullable byte[] continuation,
            @Nonnull Executor executor) {
        this.nextGenerator = nextGenerator;
        this.getContinuation = getContinuation;
        this.executor = executor;
        if (continuation != null) {
            this.lastValue = Optional.of(getContinueValue.apply(continuation));
        } else {
            lastValue = Optional.empty();
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            mayGetContinuation = false;
            nextFuture = nextGenerator.apply(lastValue).thenApply(nextValue -> {
                if (!nextValue.isPresent()) {
                    lastValue = nextValue;
                }
                return nextValue;
            });
        }
        return nextFuture.thenApply(next -> {
            mayGetContinuation = !next.isPresent();
            return next.isPresent();
        });
    }

    @Nullable
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        lastValue = nextFuture.join();
        nextFuture = null;

        if (!lastValue.isPresent()) {
            throw new NoSuchElementException();
        }

        mayGetContinuation = true;
        return lastValue.get();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return lastValue.map(getContinuation).orElse(null);
    }

    @Override
    public NoNextReason getNoNextReason() {
        return NoNextReason.SOURCE_EXHAUSTED;
    }

    @Override
    public void close() {
        if (nextFuture != null) {
            nextFuture.cancel(true);
        }
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }
}
