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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
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
@API(API.Status.MAINTAINED)
public class ChainedCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Function<Optional<T>, CompletableFuture<Optional<T>>> nextGenerator;
    @Nonnull
    private final Function<T, byte[]> getContinuation;
    @Nonnull
    private final Executor executor;
    @Nullable
    private CompletableFuture<Boolean> nextFuture;
    @Nonnull
    private Optional<T> lastValue;
    @Nullable
    private RecordCursorResult<T> lastResult;

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
    @API(API.Status.EXPERIMENTAL)
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return nextGenerator.apply(lastValue).thenApply(nextValue -> {
            if (nextValue.isPresent()) {
                lastValue = nextValue;
                lastResult = RecordCursorResult.withNextValue(nextValue.get(), new Continuation<>(nextValue, getContinuation));
            } else {
                lastValue = nextValue;
                lastResult = RecordCursorResult.exhausted();
                mayGetContinuation = true;
            }
            return lastResult;
        });
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

    private static class Continuation<T> implements RecordCursorContinuation {
        @Nonnull
        private final Optional<T> lastValue;
        @Nonnull
        private final Function<T, byte[]> getContinuation;
        @Nullable
        private byte[] cachedBytes;

        public Continuation(@Nonnull Optional<T> lastValue, @Nonnull Function<T, byte[]> getContinuation) {
            this.lastValue = lastValue;
            this.getContinuation = getContinuation;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (cachedBytes == null) {
                cachedBytes = lastValue.map(getContinuation).orElse(null);
            }
            return cachedBytes;
        }

        @Override
        public boolean isEnd() {
            return toBytes() == null;
        }
    }
}
