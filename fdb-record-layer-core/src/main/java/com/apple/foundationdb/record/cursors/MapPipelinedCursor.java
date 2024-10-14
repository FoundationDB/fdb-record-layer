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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CancellationException;
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
@API(API.Status.MAINTAINED)
public class MapPipelinedCursor<T, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<T, CompletableFuture<V>> func;
    private final int pipelineSize;
    @Nonnull
    private final Queue<CompletableFuture<RecordCursorResult<V>>> pipeline;
    private boolean innerExhausted = false;
    private volatile boolean closing = false;

    @Nullable
    private CompletableFuture<RecordCursorResult<T>> waitInnerFuture = null;
    @Nullable
    private RecordCursorResult<V> nextResult = null;

    public MapPipelinedCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<T, CompletableFuture<V>> func,
                              int pipelineSize) {
        this.inner = inner;
        this.func = func;
        this.pipelineSize = pipelineSize;
        this.pipeline = new ArrayDeque<>(pipelineSize);
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        return AsyncUtil.whileTrue(this::tryToFillPipeline, getExecutor())
                // pipeline will necessarily contain something if we stopped looping, so pipeline.remove() is nonnull
                // future should already be (nearly) ready if we stopped looping
                // Unless, we are in the process of closing this, in which case there may not be any futures in the
                // pipeline, and so we should return a cancelled exception
                .thenCompose(vignore -> peekOrCancelled())
                .thenApply(result -> {
                    if (result.hasNext()) {
                        final CompletableFuture<RecordCursorResult<V>> inPipeline = pipeline.poll();
                        if (inPipeline == null) {
                            throw new CancellationException("Cursor has already been closed");
                        }
                    }
                    nextResult = result;
                    return result;
                });
    }

    @Nullable
    private CompletableFuture<RecordCursorResult<V>> peekOrCancelled() {
        final CompletableFuture<RecordCursorResult<V>> peek = pipeline.peek();
        if (peek == null) {
            // this could happen if the cursor is in the process of being closed
            CompletableFuture<RecordCursorResult<V>> future = new CompletableFuture<>();
            future.cancel(true);
            return future;
        }
        return peek;
    }

    @Override
    public void close() {
        closing = true;
        while (!pipeline.isEmpty()) {
            final CompletableFuture<RecordCursorResult<V>> outstanding = pipeline.poll();
            // outstanding here, could be null if an onNext future is also being processed, and it has just removed the
            // only future in the pipeline
            if (outstanding != null) {
                outstanding.cancel(false);
            }
        }
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return pipeline.isEmpty() && inner.isClosed();
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
        while (!innerExhausted && pipeline.size() < pipelineSize) {
            // try to add a future to the pipeline
            if (waitInnerFuture == null) {
                waitInnerFuture = inner.onNext();
            }

            if (!waitInnerFuture.isDone()) {
                // still waiting for inner future, check back once something has finished
                CompletableFuture<RecordCursorResult<V>> nextEntry = pipeline.peek();
                if (nextEntry == null) {
                    return waitInnerFuture.thenApply(vignore -> true); // loop back to process inner result
                } else {
                    // keep looping unless the next entry is done
                    return CompletableFuture.anyOf(waitInnerFuture, nextEntry).thenApply(vignore -> !nextEntry.isDone());
                }
            }

            final RecordCursorResult<T> innerResult = waitInnerFuture.join(); // future is ready, doesn't block
            pipeline.add(innerResult.mapAsync(func));

            if (innerResult.hasNext()) { // just added something to the pipeline, so pipeline will contain an entry
                waitInnerFuture = null; // done with this future, should advanced cursor next time
                if (pipeline.peek().isDone()) { //
                    return AsyncUtil.READY_FALSE; // next entry ready, don't loop
                }
                // otherwise, keep looping
            } else { // don't have next, and won't ever with this cursor
                innerExhausted = true;
                if (innerResult.getNoNextReason() == NoNextReason.TIME_LIMIT_REACHED && nextResult != null) {
                    // Under time pressure, do not want to wait for any futures to complete.
                    // For other out-of-band reasons, still return results from the futures that were
                    // already started.
                    // Cannot do this for the very first entry, because do not have a continuation before that.
                    RecordCursorContinuation lastFinishedContinuation = cancelPendingFutures();
                    pipeline.add(CompletableFuture.completedFuture(
                            RecordCursorResult.withoutNextValue(lastFinishedContinuation, NoNextReason.TIME_LIMIT_REACHED)));
                }
                // Wait for next entry, as if pipeline were full
                break;
            }
        }

        // just added something to the pipeline, so pipeline will contain an entry
        return pipeline.peek().thenApply(vignore -> false); // the next result is ready
    }

    @Nonnull
    private RecordCursorContinuation cancelPendingFutures() {
        Iterator<CompletableFuture<RecordCursorResult<V>>> iter = pipeline.iterator();
        // The earliest continuation we could need to start with is the one from the last returned result.
        // We may, however, return more results if they are already completed.
        RecordCursorContinuation continuation = nextResult.getContinuation();
        while (iter.hasNext()) {
            CompletableFuture<RecordCursorResult<V>> pendingEntry = iter.next();
            if (!pendingEntry.isDone()) {
                // Once we have found an entry that is not done, cancel that and all remaining
                // futures, remove them from the pipeline, and do *not* update the continuation.
                while (true) {
                    iter.remove();
                    pendingEntry.cancel(false);
                    if (!iter.hasNext()) {
                        return continuation;
                    }
                    pendingEntry = iter.next();
                }
            } else {
                // Entry is done, so this cursor will return this result. Keep the entry
                // in the pipeline, and update the continuation.
                continuation = pendingEntry.join().getContinuation();
            }
        }
        return continuation;
    }

}
