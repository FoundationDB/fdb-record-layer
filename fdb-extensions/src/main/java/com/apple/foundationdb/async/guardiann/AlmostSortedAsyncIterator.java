/*
 * AlmostSortedAsyncIterator.java
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

import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.async.MoreAsyncUtil;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

class AlmostSortedAsyncIterator<T> implements CloseableAsyncIterator<T> {
    @Nonnull
    private final AsyncIterator<T> in;
    private final int efSearch;
    @Nonnull
    private final PriorityQueue<T> out;

    @Nonnull
    private final Executor executor;

    private CompletableFuture<T> nextFuture;
    private boolean inDone;

    public AlmostSortedAsyncIterator(@Nonnull final AsyncIterator<T> in,
                                     @Nonnull Comparator<T> comparator,
                                     final int efSearch,
                                     @Nonnull final Executor executor) {
        this.in = in;
        this.efSearch = efSearch;
        this.out = new PriorityQueue<>(comparator);
        this.executor = executor;
        this.nextFuture = null;
        this.inDone = false;
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            nextFuture = computeNextRecord();
        }
        return nextFuture.thenApply(Objects::nonNull);
    }

    @Nonnull
    private CompletableFuture<T> computeNextRecord() {
        return AsyncUtil.whileTrue(() -> {
            if (inDone || out.size() >= efSearch) {
                return AsyncUtil.READY_FALSE; // break out of the loop
            }
            final var inOnHasNextFuture = in.onHasNext();
            if (MoreAsyncUtil.isCompletedNormally(inOnHasNextFuture)) {
                if (!inOnHasNextFuture.getNow(false)) {
                    inDone = true;
                } else {
                    out.add(in.next());
                }
            }
            return in.onHasNext()
                    .thenCompose(ignored -> AsyncUtil.READY_TRUE);
        }, executor).thenApply(ignored -> {
            if (out.isEmpty()) {
                return null;
            }
            return out.poll();
        });
    }

    @Override
    public boolean hasNext() {
        return onHasNext().join();
    }

    @Override
    public T next() {
        if (hasNext()) {
            // nextFuture has already completed
            final T next = Objects.requireNonNull(nextFuture).join();
            nextFuture = null;
            return next;
        }
        throw new NoSuchElementException("called next() on exhausted iterator");
    }

    @Override
    public void close() {
        MoreAsyncUtil.closeIterator(in);
    }
}
