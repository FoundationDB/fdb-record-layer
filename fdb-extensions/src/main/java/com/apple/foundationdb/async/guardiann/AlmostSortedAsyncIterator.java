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

/**
 * A {@link CloseableAsyncIterator} that re-orders its input into approximately sorted order. It keeps a bounded
 * look-ahead buffer of up to {@code maxQueueSize} elements drawn from the input and always emits the smallest buffered
 * element next, as ordered by the supplied {@link Comparator}. The ordering is only <em>approximate</em> because an
 * element more than the window size away from its fully sorted position cannot be pulled forward; this trades exact
 * ordering for bounded memory, which suits search pipelines that consume a roughly distance-ordered prefix.
 *
 * <p>
 * The first element is the expensive one: before anything can be emitted the buffer is filled to {@code maxQueueSize}
 * (or until the input is exhausted), so producing it draws a full window of elements from the input up front.
 * Therefore, its cost scales with {@code maxQueueSize} input pulls — though the wall-clock time is really governed by
 * the input's (unbounded, potentially I/O-bound) per-element cost, not by {@code maxQueueSize} itself. Thereafter each
 * element draws at most one further input element. Space is bounded by the at-most {@code maxQueueSize} buffered
 * elements.
 *
 * @param <T> the type of element produced
 */
class AlmostSortedAsyncIterator<T> implements CloseableAsyncIterator<T> {
    @Nonnull
    private final AsyncIterator<T> in;
    private final int maxQueueSize;
    @Nonnull
    private final PriorityQueue<T> out;

    @Nonnull
    private final Executor executor;

    private CompletableFuture<T> nextFuture;
    private boolean inDone;

    public AlmostSortedAsyncIterator(@Nonnull final AsyncIterator<T> in,
                                     @Nonnull Comparator<T> comparator,
                                     final int maxQueueSize,
                                     @Nonnull final Executor executor) {
        this.in = in;
        this.maxQueueSize = maxQueueSize;
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
            if (inDone || out.size() >= maxQueueSize) {
                return AsyncUtil.READY_FALSE; // break out of the loop
            }
            final CompletableFuture<Boolean> inHasNextFuture = in.onHasNext();
            if (!MoreAsyncUtil.isCompletedNormally(inHasNextFuture)) {
                // Whether there is a next input element isn't known yet: wait on this same future
                // (do not issue a second onHasNext()), then re-evaluate the loop on the next turn.
                return inHasNextFuture.thenApply(ignored -> true);
            }
            if (!inHasNextFuture.getNow(false)) {
                inDone = true;
            } else {
                out.add(in.next());
            }
            return AsyncUtil.READY_TRUE;
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
        cancelNextFuture();
        MoreAsyncUtil.closeIterator(in);
    }

    @Override
    public void cancel() {
        cancelNextFuture();
        in.cancel();
    }

    private void cancelNextFuture() {
        if (nextFuture != null) {
            nextFuture.cancel(false);
            nextFuture = null;
        }
    }
}
