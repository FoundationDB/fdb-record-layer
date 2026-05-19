/*
 * TopK.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.async.AsyncUtil.forEachRemaining;

public class TopK<T> {
    @Nonnull
    private final PriorityQueue<T> queue;
    @SuppressWarnings("checkstyle:MemberName")
    final int k;

    private TopK(@Nonnull final Comparator<T> comparator, final int k) {
        this.queue = new PriorityQueue<>(comparator);
        this.k = k;
    }

    public boolean add(@Nonnull T item) {
        if (queue.size() < k) {
            return queue.add(item);
        }

        final Comparator<? super T> comparator = queue.comparator();
        final T currentWorst = queue.peek();

        if (comparator.compare(item, currentWorst) <= 0) {
            return false;
        }

        queue.poll();
        return queue.add(item);
    }

    @Nonnull
    public List<T> toUnsortedList() {
        return ImmutableList.copyOf(queue);
    }

    @SuppressWarnings("unchecked")
    public List<T> toSortedList() {
        final PriorityQueue<T> copy = new PriorityQueue<>(queue);
        final int size = copy.size();
        final T[] array = (T[]) new Object[size];

        for (int i = size - 1; i >= 0; i --) {
            array[i] = copy.poll();
        }

        return ImmutableList.copyOf(array);
    }

    @Nonnull
    public Optional<T> worstElement() {
        if (queue.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(queue.peek());
    }

    @Nonnull
    public CompletableFuture<List<T>> collect(final AsyncIterable<T> iterable,
                                              final Executor executor) {
        return collectRemaining(iterable.iterator(), executor);
    }

    /**
     * Iterates over a set of items and returns the remaining results as a list.
     *
     * @param iterator the source of data over which to iterate. This function will exhaust the iterator.
     * @param executor the {@link Executor} to use for asynchronous operations
     *
     * @return a {@code CompletableFuture} which will be set to the amalgamation of results
     *  from iteration.
     */
    public CompletableFuture<List<T>> collectRemaining(final AsyncIterator<T> iterator,
                                                       final Executor executor) {
        return forEachRemaining(iterator, this::add, executor).thenApply(ignored -> toSortedList());
    }

    @Nonnull
    public static <T> TopK<T> min(@Nonnull final Comparator<T> comparator, final int k) {
        return new TopK<>(comparator.reversed(), k);
    }

    @Nonnull
    public static <T> TopK<T> max(@Nonnull final Comparator<T> comparator, final int k) {
        return new TopK<>(comparator, k);
    }
}
