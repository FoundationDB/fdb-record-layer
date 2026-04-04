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

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class TopK<T> {
    @Nonnull
    private final PriorityQueue<T> queue;
    @SuppressWarnings("checkstyle:MemberName")
    final int k;

    public TopK(@Nonnull final Comparator<T> comparator, final int k) {
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
}
