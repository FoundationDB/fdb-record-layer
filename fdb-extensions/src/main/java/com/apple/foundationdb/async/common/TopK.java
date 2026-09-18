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

package com.apple.foundationdb.async.common;

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

/**
 * A fixed-capacity collector that retains the top {@code k} elements offered, as ordered by a {@link Comparator}
 * (the {@code k} that compare greatest). Once at capacity, a new element is kept only if it compares strictly
 * greater than the current worst, which it then evicts. Unlike {@link DistinctTopK} this does not deduplicate, so
 * equal elements may be retained more than once. Backed by a bounded min-heap, giving {@code O(log k)}
 * {@link #add(Object)}.
 *
 * @param <T> the type of element collected
 */
public class TopK<T> {
    @Nonnull
    private final PriorityQueue<T> queue;
    @SuppressWarnings("checkstyle:MemberName")
    private final int k;

    private TopK(@Nonnull final Comparator<T> comparator, final int k) {
        this.queue = new PriorityQueue<>(comparator);
        this.k = k;
    }

    /**
     * Offers an element to this collector. The element is kept if fewer than {@code k} elements are currently held,
     * or if it compares strictly greater than the current worst, which it then evicts.
     *
     * @param item the element to offer
     *
     * @return {@code true} if the element was retained, {@code false} if it was rejected
     */
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

    /**
     * Returns the currently retained elements in no particular order.
     *
     * @return the retained elements, unordered
     */
    @Nonnull
    public List<T> toUnsortedList() {
        return ImmutableList.copyOf(queue);
    }

    /**
     * Returns the currently retained elements ordered from greatest to least, as ranked by the comparator.
     *
     * @return the retained elements, sorted from best (greatest) to worst (least)
     */
    public List<T> toSortedList() {
        final PriorityQueue<T> copy = new PriorityQueue<>(queue);
        final ImmutableList.Builder<T> builder = ImmutableList.builderWithExpectedSize(copy.size());
        while (!copy.isEmpty()) {
            builder.add(copy.poll());
        }
        // poll() drains the heap worst-first; ImmutableList.reverse() returns an O(1) reversed view (not a copy),
        // so the result reads best-to-worst. The right-sized builder lets build() reuse its array without copying.
        return builder.build().reverse();
    }

    /**
     * Returns the current worst retained element — the next one that would be evicted — or empty if none are held.
     *
     * @return the worst retained element, or {@link Optional#empty()} if the collector is empty
     */
    @Nonnull
    public Optional<T> worstElement() {
        if (queue.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(queue.peek());
    }

    /**
     * Offers every element produced by the given iterable to this collector and returns the retained top-K as a
     * sorted list.
     *
     * @param iterable the elements to offer
     * @param executor the executor to use for asynchronous iteration
     *
     * @return a future completing with the retained top-K elements, sorted from best to worst
     */
    @Nonnull
    public CompletableFuture<List<T>> collect(final AsyncIterable<T> iterable,
                                              final Executor executor) {
        return collectRemaining(iterable.iterator(), executor);
    }

    /**
     * Exhausts the iterator, offering every element to this top-K collector, and returns the retained
     * top-K elements as a sorted list.
     *
     * @param iterator the source of data over which to iterate. This function will exhaust the iterator.
     * @param executor the {@link Executor} to use for asynchronous operations
     *
     * @return a {@code CompletableFuture} completing with the retained top-K elements, sorted
     */
    public CompletableFuture<List<T>> collectRemaining(final AsyncIterator<T> iterator,
                                                       final Executor executor) {
        return forEachRemaining(iterator, this::add, executor).thenApply(ignored -> toSortedList());
    }

    /**
     * Creates a collector that retains the {@code k} <em>smallest</em> elements offered, as ordered by the given
     * comparator.
     *
     * @param comparator the comparator defining element order
     * @param k the maximum number of elements to retain
     * @param <T> the type of element collected
     *
     * @return a new collector retaining the k smallest elements
     */
    @Nonnull
    public static <T> TopK<T> min(@Nonnull final Comparator<T> comparator, final int k) {
        return new TopK<>(comparator.reversed(), k);
    }

    /**
     * Creates a collector that retains the {@code k} <em>largest</em> elements offered, as ordered by the given
     * comparator.
     *
     * @param comparator the comparator defining element order
     * @param k the maximum number of elements to retain
     * @param <T> the type of element collected
     *
     * @return a new collector retaining the k largest elements
     */
    @Nonnull
    public static <T> TopK<T> max(@Nonnull final Comparator<T> comparator, final int k) {
        return new TopK<>(comparator, k);
    }
}
