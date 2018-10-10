/*
 * IteratorCursor.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that returns the elements of an ordinary synchronous iterator.
 * @param <T> the type of elements of the cursor
 */
public class IteratorCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Executor executor;
    @Nonnull
    protected final Iterator<T> iterator;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public IteratorCursor(@Nonnull Executor executor, @Nonnull Iterator<T> iterator) {
        this.executor = executor;
        this.iterator = iterator;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        boolean hasNext = iterator.hasNext();
        mayGetContinuation = !hasNext;
        return CompletableFuture.completedFuture(hasNext);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Nullable
    @Override
    public T next() {
        mayGetContinuation = true;
        return iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return null;
    }

    @Override
    public NoNextReason getNoNextReason() {
        return NoNextReason.SOURCE_EXHAUSTED;
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
