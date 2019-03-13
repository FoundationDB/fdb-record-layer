/*
 * IteratorCursorBase.java
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
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base cursor for {@link IteratorCursor} and {@link AsyncIteratorCursor}.
 * @param <T> the type of elements of the cursor
 * @param <C> the type of {@link Iterator}
 */
@API(API.Status.INTERNAL)
public class IteratorCursorBase<T, C extends Iterator<T>> implements RecordCursor<T> {
    @Nonnull
    protected final Executor executor;
    @Nonnull
    protected final C iterator;
    protected int valuesSeen;

    @Nullable
    protected CompletableFuture<Boolean> hasNextFuture;
    @Nullable
    protected RecordCursorResult<T> nextResult;

    // for detecting incorrect cursor usage
    protected boolean mayGetContinuation = false;

    protected IteratorCursorBase(@Nonnull Executor executor, @Nonnull C iterator) {
        this.executor = executor;
        this.iterator = iterator;
        this.valuesSeen = 0;
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return CompletableFuture.completedFuture(computeNextResult(iterator.hasNext()));
    }

    protected RecordCursorResult<T> computeNextResult(boolean hasNext) {
        mayGetContinuation = !hasNext;
        if (hasNext) {
            valuesSeen++;
            nextResult = RecordCursorResult.withNextValue(iterator.next(), ByteArrayContinuation.fromInt(valuesSeen));
        } else {
            nextResult = RecordCursorResult.exhausted();
        }
        return nextResult;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture == null) {
            mayGetContinuation = false;
            hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return hasNextFuture;
    }

    @Nullable
    @Override
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
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return nextResult.getContinuation().toBytes();
    }

    @Override
    public NoNextReason getNoNextReason() {
        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RecordCoreException(ex.getMessage(), ex);
            }
        }
        if (hasNextFuture != null) {
            hasNextFuture.cancel(false);
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
