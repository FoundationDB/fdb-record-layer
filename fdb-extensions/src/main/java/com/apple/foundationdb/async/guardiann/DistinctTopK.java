/*
 * DistinctTopK.java
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
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.async.AsyncUtil.forEachRemaining;

public class DistinctTopK<T> {
    @Nonnull
    private final TreeSet<T> set;
    @SuppressWarnings("checkstyle:MemberName")
    final int k;

    private DistinctTopK(@Nonnull final Comparator<T> comparator, final int k) {
        this.set = Sets.newTreeSet(comparator);
        this.k = k;
    }

    public boolean add(@Nonnull T item) {
        if (set.contains(item)) {
            return false;
        }

        if (set.size() < k) {
            return set.add(item);
        }

        final Comparator<? super T> comparator = Objects.requireNonNull(set.comparator());
        final T currentWorst = Objects.requireNonNull(set.first());

        if (comparator.compare(item, currentWorst) < 0) {
            return false;
        }

        set.pollFirst();
        return set.add(item);
    }

    public List<T> toSortedList() {
        return ImmutableList.copyOf(set.descendingIterator());
    }

    @Nonnull
    public Optional<T> worstElement() {
        if (set.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(set.first());
    }

    @Nonnull
    public CompletableFuture<List<T>> collect(final AsyncIterable<T> iterable,
                                              final Executor executor) {
        return collectRemaining(iterable.iterator(), executor);
    }

    /**
     * Exhausts the iterator, offering every element to this distinct top-K collector, and returns the
     * retained distinct top-K elements as a sorted list.
     *
     * @param iterator the source of data over which to iterate. This function will exhaust the iterator.
     * @param executor the {@link Executor} to use for asynchronous operations
     *
     * @return a {@code CompletableFuture} completing with the retained distinct top-K elements, sorted
     */
    public CompletableFuture<List<T>> collectRemaining(final AsyncIterator<T> iterator,
                                                       final Executor executor) {
        return forEachRemaining(iterator, this::add, executor).thenApply(ignored -> toSortedList());
    }

    @Nonnull
    public static <T> DistinctTopK<T> min(@Nonnull final Comparator<T> comparator, final int k) {
        return new DistinctTopK<>(comparator.reversed(), k);
    }

    @Nonnull
    public static <T> DistinctTopK<T> max(@Nonnull final Comparator<T> comparator, final int k) {
        return new DistinctTopK<>(comparator, k);
    }
}
