/*
 * FlatMapPipelinedCursor.java
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
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.google.protobuf.ByteString;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A cursor that maps elements to another cursor which is then flattened.
 *
 * <p>
 * The cursor is <i>pipelined</i>, that is, it maintains up to a specified number of open cursors ahead of what it has returned,
 * so that work is done in parallel.
 * </p>
 * @param <T> the type of elements of the source cursor
 * @param <V> the type of elements of the cursor produced by the function
 */
public class FlatMapPipelinedCursor<T, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<T> outerCursor;
    @Nonnull
    private final BiFunction<T, byte[], ? extends RecordCursor<V>> innerCursorFunction;
    @Nullable
    private final Function<T, byte[]> checkValueFunction;
    @Nullable
    private byte[] outerContinuation;
    @Nullable
    private final byte[] initialCheckValue;
    @Nullable
    private byte[] initialInnerContinuation;
    private final int pipelineSize;
    @Nonnull
    private final Queue<PipelineQueueEntry<V>> pipeline;
    @Nullable
    private PipelineQueueEntry<V> lastEntry;
    @Nullable
    private CompletableFuture<Boolean> nextFuture;
    @Nullable
    private CompletableFuture<Boolean> outerNextFuture;
    @Nullable
    private NoNextReason innerReason;
    private boolean outerExhausted = false;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public FlatMapPipelinedCursor(@Nonnull RecordCursor<T> outerCursor,
                                  @Nonnull BiFunction<T, byte[], ? extends RecordCursor<V>> innerCursorFunction,
                                  @Nullable Function<T, byte[]> checkValueFunction,
                                  @Nullable byte[] outerContinuation,
                                  @Nullable byte[] initialCheckValue,
                                  @Nullable byte[] initialInnerContinuation,
                                  int pipelineSize) {
        this.outerCursor = outerCursor;
        this.innerCursorFunction = innerCursorFunction;
        this.checkValueFunction = checkValueFunction;
        this.outerContinuation = outerContinuation;
        this.initialCheckValue = initialCheckValue;
        this.initialInnerContinuation = initialInnerContinuation;
        this.pipelineSize = pipelineSize;
        this.pipeline = new ArrayDeque<>(pipelineSize);
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            mayGetContinuation = false;
            nextFuture = AsyncUtil.whileTrue(this::tryToFillPipeline, getExecutor()).thenApply(vignore -> {
                boolean result = !pipeline.isEmpty()
                                 && (innerReason == null || innerReason.isSourceExhausted())
                                 && pipeline.peek().innerCursor != null;
                mayGetContinuation = !result;
                return result;
            });
        }
        return nextFuture;
    }

    @Nullable
    @Override
    public V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        nextFuture = null;
        innerReason = null;
        lastEntry = pipeline.peek();
        mayGetContinuation = true;
        return lastEntry.innerCursor.next();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        if (lastEntry == null) {
            return null;
        }
        final RecordCursorProto.FlatMapContinuation.Builder builder = RecordCursorProto.FlatMapContinuation.newBuilder();
        final byte[] innerContinuation = lastEntry.innerCursor != null ? lastEntry.innerCursor.getContinuation() : null;
        if (innerContinuation == null) {
            // This was the last of the inner cursor. Take continuation from outer after it.
            if (lastEntry.outerContinuation == null) {
                // Both exhausted.
                return null;
            }
            builder.setOuterContinuation(ByteString.copyFrom(lastEntry.outerContinuation));
        } else {
            // This was in the middle of the inner cursor. Take continuation from outer before it and arrange to skip to it.
            if (lastEntry.priorOuterContinuation != null) {
                builder.setOuterContinuation(ByteString.copyFrom(lastEntry.priorOuterContinuation));
            }
            if (lastEntry.outerCheckValue != null) {
                builder.setCheckValue(ByteString.copyFrom(lastEntry.outerCheckValue));
            }
            builder.setInnerContinuation(ByteString.copyFrom(innerContinuation));
        }
        return builder.build().toByteArray();
    }

    @Override
    public void close() {
        if (nextFuture != null) {
            nextFuture.cancel(false);
            nextFuture = null;
        }
        while (!pipeline.isEmpty()) {
            PipelineQueueEntry<V> pipelineEntry = pipeline.remove();
            if (pipelineEntry.innerCursor != null) {
                pipelineEntry.innerCursor.close();
            }
        }
        if (outerNextFuture != null) {
            outerNextFuture.cancel(false);
            outerNextFuture = null;
        }
        outerCursor.close();
    }

    @Override
    public NoNextReason getNoNextReason() {
        if (innerReason != null && !innerReason.isSourceExhausted()) {
            return innerReason;
        }
        return outerCursor.getNoNextReason();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            outerCursor.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return outerCursor.getExecutor();
    }

    /**
     * Take items from inner cursor and put in pipeline until no more or a mapped cursor item is available.
     * @return a future that will complete with {@code false} if an item is available or none will ever be, or with {@code true} if this method should be called to try again
     */
    protected CompletableFuture<Boolean> tryToFillPipeline() {
        CompletableFuture<Boolean> waitOuterNext = null;
        while (!outerExhausted && pipeline.size() < pipelineSize) {
            if (outerNextFuture == null) {
                outerNextFuture = outerCursor.onHasNext();
            }
            if (!outerNextFuture.isDone()) {
                waitOuterNext = outerNextFuture;
                break;
            }
            final RecordCursor<V> innerCursor;
            final byte[] outerCheckValue;
            if (outerNextFuture.join()) {
                final T outerValue = outerCursor.next();
                outerCheckValue = checkValueFunction == null ? null : checkValueFunction.apply(outerValue);
                byte[] innerContinuation = null;
                if (initialInnerContinuation != null) {
                    // If possible, ensure that we just positioned to the same place.
                    // Otherwise, take the whole inner cursor, rather applying initial continuation.
                    if (initialCheckValue == null || outerCheckValue == null || Arrays.equals(initialCheckValue, outerCheckValue)) {
                        innerContinuation = initialInnerContinuation;
                        initialInnerContinuation = null;
                    }
                }
                innerCursor = innerCursorFunction.apply(outerValue, innerContinuation);
            } else {
                // Add sentinel to the end of pipeline.
                innerCursor = null;
                outerCheckValue = null;
                outerExhausted = true;
            }
            byte[] priorOuterContinuation = outerContinuation;
            outerContinuation = outerCursor.getContinuation();
            pipeline.add(new PipelineQueueEntry<>(innerCursor, priorOuterContinuation, outerContinuation, outerCheckValue));
            outerNextFuture = null;
        }
        final PipelineQueueEntry<V> nextEntry = pipeline.peek();
        if (nextEntry == null) {
            // Nothing more in pipeline.
            if (waitOuterNext == null) {
                // Stop when source empty.
                return AsyncUtil.READY_FALSE;
            } else {
                // Loop to whileTrue to fill pipeline.
                return waitOuterNext;
            }
        } else if (nextEntry.innerCursor == null) {
            // Hit sentinel; outer cursor has no more values.
            lastEntry = nextEntry;
            innerReason = null; // we are stopping because of the outer reason--not the inner reason
            return AsyncUtil.READY_FALSE;
        }
        final CompletableFuture<Boolean> entryFuture = nextEntry.innerCursor.onHasNext();
        if (entryFuture.isDone()) {
            if (entryFuture.join()) {
                // Stop because next cursor has next.
                return AsyncUtil.READY_FALSE;
            }
            // Update the last entry to this entry. This will implicitly advance the continuation. This is safe
            // because we have already hit the limit of this inner cursor. If the cursor is not exhausted, the
            // whole cursor will stop and the advanced continuation will let it resume from where the cursor
            // hit the limit. If the cursor is exhausted, then every item from this cursor has been returned to
            // the user. That means that if the cursor is stopped right now, it will be resumed from the first
            // item of the next element of the outer cursor, which is fine.
            lastEntry = nextEntry;
            pipeline.remove();
            innerReason = nextEntry.innerCursor.getNoNextReason();
            if (!innerReason.isSourceExhausted()) {
                // Stop because inner cursor hit some limit.
                // Not valid to take from later in the pipeline, even if available already.
                return AsyncUtil.READY_FALSE;
            } else {
                return AsyncUtil.READY_TRUE;
            }
        }
        if (waitOuterNext == null) {
            // Recheck when cursor ready (pipeline is full).
            return entryFuture.thenApply(b -> true);
        }
        return CompletableFuture.anyOf(waitOuterNext, entryFuture)
                // Recheck when either ready.
                .thenApply(vignore -> true);
    }

    static class PipelineQueueEntry<V> {
        @Nullable
        final RecordCursor<V> innerCursor;
        @Nullable
        final byte[] priorOuterContinuation;
        @Nullable
        final byte[] outerContinuation;
        @Nullable
        final byte[] outerCheckValue;

        public PipelineQueueEntry(RecordCursor<V> innerCursor,
                                  byte[] priorOuterContinuation, byte[] outerContinuation, byte[] outerCheckValue) {
            this.innerCursor = innerCursor;
            this.priorOuterContinuation = priorOuterContinuation;
            this.outerContinuation = outerContinuation;
            this.outerCheckValue = outerCheckValue;
        }
    }
}
