/*
 * MapPipelinedCursor.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that applies an asynchronous function to the elements of another cursor.
 *
 * <p>
 * The cursor is <i>pipelined</i>, that is, it maintains up to a specified number of pending futures ahead of what it has returned,
 * so that work is done in parallel.
 * </p>
 * @param <T> the type of elements of the source cursor
 * @param <V> the type of elements of the cursor after applying the function and completing the future it returns
 */
public class MapPipelinedCursor<T, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<T, CompletableFuture<V>> func;
    private final int pipelineSize;
    @Nonnull
    private final Queue<PipelineQueueEntry<V>> pipeline;
    @Nullable
    private PipelineQueueEntry<V> lastEntry;
    @Nullable
    private CompletableFuture<Boolean> nextFuture;
    @Nullable
    private CompletableFuture<Boolean> innerFuture;
    @Nullable
    private byte[] continuation;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    public MapPipelinedCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<T, CompletableFuture<V>> func,
                              int pipelineSize) {
        this.inner = inner;
        this.func = func;
        this.pipelineSize = pipelineSize;
        this.pipeline = new ArrayDeque<>(pipelineSize);
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            mayGetContinuation = false;
            nextFuture = AsyncUtil.whileTrue(this::tryToFillPipeline, getExecutor()).thenApply(vignore -> {
                mayGetContinuation = pipeline.isEmpty();
                return !pipeline.isEmpty();
            });
        }
        return nextFuture;
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings(value = "EI2", justification = "copies are expensive")
    public V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        nextFuture = null;
        lastEntry = pipeline.remove();
        continuation = lastEntry.innerContinuation;
        mayGetContinuation = true;
        return lastEntry.future.join();
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings(value = "EI", justification = "copies are expensive")
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return continuation;
    }

    @Override
    public NoNextReason getNoNextReason() {
        return inner.getNoNextReason();
    }

    @Override
    public void close() {
        if (nextFuture != null) {
            nextFuture.cancel(false);
            nextFuture = null;
        }
        while (!pipeline.isEmpty()) {
            pipeline.remove().future.cancel(false);
        }
        if (innerFuture != null) {
            innerFuture.cancel(false);
            innerFuture = null;
        }
        inner.close();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return inner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inner.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    /**
     * Take items from inner cursor and put in pipeline until no more or a mapping result is available.
     * @return a future that will complete with {@code false} if an item is available or none will ever be, or with {@code true} if this method should be called to try again
     */
    protected CompletableFuture<Boolean> tryToFillPipeline() {
        CompletableFuture<Boolean> waitInnerFuture = null;
        while (pipeline.size() < pipelineSize) {
            if (innerFuture == null) {
                innerFuture = inner.onHasNext();
            }
            if (!innerFuture.isDone()) {
                waitInnerFuture = innerFuture;
                break;
            }

            if (!innerFuture.join()) {
                if (pipeline.isEmpty()) {
                    continuation = inner.getContinuation();
                }
                if (inner.getNoNextReason() == NoNextReason.TIME_LIMIT_REACHED && lastEntry != null) {
                    // Under time pressure, do not want to wait for any futures to complete.
                    // For other out-of-band reasons, still return results from the futures that were
                    // already started.
                    // Cannot do this for the very first entry, because do not have a continuation before that.
                    cancelPendingFutures();
                }
                break;
            }
            T n = inner.next();
            pipeline.add(new PipelineQueueEntry<>(func.apply(n),
                    inner.getContinuation()));
            innerFuture = null;
        }
        PipelineQueueEntry<V> nextEntry = pipeline.peek();
        if (nextEntry == null) {
            // Nothing more in pipeline.
            if (waitInnerFuture == null) {
                // Stop when source empty.
                return AsyncUtil.READY_FALSE;
            } else {
                // Loop to whileTrue to fill pipeline.
                return waitInnerFuture.thenApply(hasNext -> {
                    if (!hasNext) {
                        // Nothing in pipeline and nothing else to get, so the continuation might be stale
                        // once waitInnerFuture completes.
                        continuation = inner.getContinuation();
                    }
                    return hasNext;
                });
            }
        }
        if (nextEntry.future.isDone()) {
            // Stop because next mapped value ready.
            return AsyncUtil.READY_FALSE;
        }
        if (waitInnerFuture == null) {
            // Stop when value ready (pipeline is full).
            return nextEntry.future.thenApply(vignore -> false);
        }
        return CompletableFuture.anyOf(waitInnerFuture, nextEntry.future)
                // Recheck when either ready.
                .thenApply(vignore -> true);
    }

    private void cancelPendingFutures() {
        Iterator<PipelineQueueEntry<V>> iter = pipeline.iterator();
        while (iter.hasNext()) {
            PipelineQueueEntry<V> pendingEntry = iter.next();
            if (!pendingEntry.future.isDone()) {
                while (true) {
                    iter.remove();
                    pendingEntry.future.cancel(false);
                    if (!iter.hasNext()) {
                        return;
                    }
                    pendingEntry = iter.next();
                }
            }
        }
    }

    static class PipelineQueueEntry<V> {
        final CompletableFuture<V> future;
        final byte[] innerContinuation;

        public PipelineQueueEntry(CompletableFuture<V> future, byte[] innerContinuation) {
            this.future = future;
            this.innerContinuation = innerContinuation;
        }
    }
}
