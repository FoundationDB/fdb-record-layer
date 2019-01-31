/*
 * AsyncPeekCallbackIterator.java
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

package com.apple.foundationdb.async;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

/**
 * An {@code AsyncPeekCallbackIterator} is an extension of the {@link AsyncPeekIterator} interface that can be given a
 * callback to call after each time it advances. Note that the {@code AsyncPeekCallbackIterator} is mostly a tag
 * interface and does not contain any logic for executing the callback after yielding each result; conforming
 * implementations must implement that logic.
 *
 * @param <T> type of elements returned by the scan
 */
public interface AsyncPeekCallbackIterator<T> extends AsyncPeekIterator<T> {
    /**
     * Wrap an {@link AsyncIterator} with an {@code AsyncPeekCallbackIterator}.
     * The returned iterator returns the same sequence of elements
     * as the supplied {@code AsyncIterator} instance in the same order.
     * The wrapping implementation is free to advance the underlying iterator,
     * so it is unsafe to modify <code>iterator</code> directly after calling
     * this method. The returned iterator is also not thread safe, so concurrent
     * calls to <code>onHasNext</code>, for example, may lead to unexpected behavior.
     *
     * @param <T> type of items returned by the scan
     * @param iterator {@link AsyncIterator} to wrap
     * @param callback a callback to call when {@link #next()} produces a result
     * @return an iterator over the same values as <code>iterator</code> that supports peek and callback semantics
     */
    static <T> AsyncPeekCallbackIterator<T> wrap(@Nonnull AsyncIterator<T> iterator, @Nonnull Consumer<T> callback) {
        if (iterator instanceof AsyncPeekCallbackIterator) {
            final Consumer<T> originalCallback = ((AsyncPeekCallbackIterator<T>)iterator).getCallback();
            ((AsyncPeekCallbackIterator<T>)iterator).setCallback(t -> {
                originalCallback.accept(t);
                callback.accept(t);
            });
            return (AsyncPeekCallbackIterator<T>) iterator;
        }
        return new WrappingAsyncPeekIterator<>(iterator, callback);
    }

    /**
     * Set the callback to the provided {@link Consumer}.
     * @param callback a consumer to call when a new result is produced by {@link #next()}
     */
    void setCallback(@Nonnull Consumer<T> callback);

    /**
     * Return the callback that this iterator calls before a new result is returned by {@link #next()}.
     * @return the callback that this iterator calls before a new result is returned by {@link #next()};W
     */
    @Nonnull
    Consumer<T> getCallback();
}
