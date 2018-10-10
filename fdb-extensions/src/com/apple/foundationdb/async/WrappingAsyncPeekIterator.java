/*
 * WrappingAsyncPeekIterator.java
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

package com.apple.foundationdb.async;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An implementation of the {@link AsyncPeekIterator} interface that wraps a regular
 * {@link AsyncPeekIterator}. It will return the same items as the iterator it wraps
 * in the same order, but it allows for peek semantics. Users should call the
 * {@link AsyncPeekIterator#wrap(AsyncIterator) wrap()} method of
 * <code>AsyncPeekIterator</code> instead of instantiating this class directly.
 *
 * @param <T> type of elements returned by this iterator
 */
class WrappingAsyncPeekIterator<T> implements AsyncPeekIterator<T> {
    @Nonnull private AsyncIterator<T> underlying;
    @Nullable private CompletableFuture<Boolean> hasNextFuture;
    private boolean done;
    private boolean hasCurrent;
    @Nullable private T nextItem;

    WrappingAsyncPeekIterator(@Nonnull AsyncIterator<T> underlying) {
        this.underlying = underlying;
        this.done = false;
        this.hasCurrent = false;
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (done) {
            return AsyncUtil.READY_FALSE;
        } else if (hasCurrent) {
            return AsyncUtil.READY_TRUE;
        }
        if (hasNextFuture == null) {
            hasNextFuture = underlying.onHasNext().thenApply(doesHaveNext -> {
                if (doesHaveNext) {
                    hasCurrent = true;
                    nextItem = underlying.next();
                } else {
                    done = true;
                }
                return doesHaveNext;
            });
        }
        return hasNextFuture;
    }

    @Override
    public boolean hasNext() {
        return onHasNext().join();
    }

    @Override
    public T peek() {
        if (done) {
            throw new NoSuchElementException();
        } else if (hasCurrent || hasNext()) {
            return nextItem;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public T next() {
        T val = peek();
        hasNextFuture = null;
        hasCurrent = false;
        return val;
    }

    @Override
    public void cancel() {
        underlying.cancel();
    }
}
