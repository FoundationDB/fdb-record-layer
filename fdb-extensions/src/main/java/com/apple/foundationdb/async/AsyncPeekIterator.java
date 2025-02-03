/*
 * AsyncPeekIterator.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;

/**
 * A <code>AsyncPeekIterator</code> is an extension of the {@link AsyncIterator}
 * interface that adds peek semantics, i.e., viewing the next element of the iterator
 * without advancing it forward. A conforming implementation should, much like
 * a conforming {@link AsyncIterator} implementation, should not be blocking in the
 * case that one calls {@link AsyncIterator#onHasNext() onHasNext()} and waits
 * for that future to complete before calling either {@link AsyncIterator#next() next()}
 * or {@link #peek() peek()}. If one calls {@link AsyncIterator#next()} immediately
 * after a {@link #peek()}, both calls should return the same value.
 *
 * <p>
 * Conforming implementations do not necessarily need to be thread-safe.
 * </p>
 *
 * @param <T> type of elements returned by the scan
 */
@API(API.Status.UNSTABLE)
public interface AsyncPeekIterator<T> extends AsyncIterator<T> {
    /**
     * Get the next item of the scan without advancing it.
     * This item can be called multiple times, and it should return
     * the same value each time as long as there are no intervening
     * calls to {@link AsyncIterator#next() next()}.
     *
     * @return the next item that will be returned by {@link AsyncIterator#next() next()}
     * @throws NoSuchElementException if there are no items remaining in the scan
     */
    T peek() throws NoSuchElementException;

    /**
     * Wrap an {@link AsyncIterator} with an <code>AsyncPeekIterator</code>.
     * The returned scan iterator return the same sequence of elements
     * as the supplied <code>AsyncIterator</code> instance in the same order.
     * The wrapping implementation is free to advance the underlying iterator,
     * so it is unsafe to modify <code>iterator</code> directly after calling
     * this method. The returned <code>iterator</code> is also not thread safe,
     * so concurrent calls to <code>onHasNext</code>, for example, may lead to
     * unexpected behavior.
     *
     * @param iterator {@link AsyncIterator} to wrap
     * @param <T> type of items returned by the scan
     * @return an scan over the same values as <code>scan</code> that
     *         supports peek semantics
     */
    static <T> AsyncPeekIterator<T> wrap(@Nonnull AsyncIterator<T> iterator) {
        if (iterator instanceof AsyncPeekIterator) {
            // Don't actually wrap the iterator if it is already
            // a peek iterator.
            return (AsyncPeekIterator<T>)iterator;
        } else {
            return new WrappingAsyncPeekIterator<>(iterator);
        }
    }
}
