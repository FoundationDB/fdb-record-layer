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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CancellationException;
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
@API(API.Status.UNSTABLE)
@SuppressWarnings("PMD.CloseResource")
public class FlatMapPipelinedCursor<T, V> implements RecordCursor<V> {

    private static final CompletableFuture<Boolean> ALREADY_CANCELLED = MoreAsyncUtil.alreadyCancelled();
    @Nonnull
    private final RecordCursor<T> outerCursor;
    @Nonnull
    private final BiFunction<T, byte[], ? extends RecordCursor<V>> innerCursorFunction;
    @Nullable
    private final Function<T, byte[]> checkValueFunction;
    @Nonnull
    private RecordCursorContinuation outerContinuation;
    @Nullable
    private final byte[] initialCheckValue;
    @Nullable
    private byte[] initialInnerContinuation;
    private final int pipelineSize;
    /**
     * The pipeline used to add some parallelism to reads. Note this queue is not thread safe, so access
     * should generally be mediated through one of the {@code synchronized} methods.
     */
    @Nonnull
    private final Queue<PipelineQueueEntry<V, T>> pipeline;
    /**
     * The next value to pull from the outer cursor. This value is cleared out by {@link #close()}, so
     * some care should be taken when accessing it. In general, it should be set via one of the {@code synchronized}
     * methods.
     */
    @Nullable
    private volatile CompletableFuture<RecordCursorResult<T>> outerNextFuture;
    private volatile boolean outerExhausted = false;
    private volatile boolean closed = false;

    @Nullable
    private RecordCursorResult<V> lastResult;

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
        if (outerContinuation == null) {
            // Because of the semantics of byte array continuations, ByteArrayContinuation.fromNullable(null) is the
            // end continuation, not the start continuation! This is a bit ugly, but it's temporary until we replace
            // byte array continuations entirely.
            this.outerContinuation = RecordCursorStartContinuation.START;
        } else {
            this.outerContinuation = ByteArrayContinuation.fromNullable(outerContinuation);
        }
        this.initialInnerContinuation = initialInnerContinuation;
        this.initialCheckValue = initialCheckValue;
        this.pipelineSize = pipelineSize;
        this.pipeline = new ArrayDeque<>(pipelineSize);
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (lastResult != null && !lastResult.hasNext()) {
            return CompletableFuture.completedFuture(lastResult);
        }
        return AsyncUtil.whileTrue(this::tryToFillPipeline, getExecutor()).thenApply(vignore -> {
            PipelineQueueEntry<V, T> peeked = peekPipeline();
            if (peeked == null) {
                throw new CancellationException("cursor closed while iterating");
            }
            lastResult = peeked.nextResult();
            return lastResult;
        });
    }

    private synchronized void markClosed() {
        closed = true;
        while (!pipeline.isEmpty()) {
            pipeline.remove().close();
        }
        if (outerNextFuture != null) {
            outerNextFuture.cancel(false);
            outerNextFuture = null;
        }
        outerCursor.close();
    }


    @Override
    public void close() {
        markClosed();
    }

    @Override
    public boolean isClosed() {
        return closed;
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
    @Nonnull
    protected CompletableFuture<Boolean> tryToFillPipeline() {
        if (closed) {
            return ALREADY_CANCELLED;
        }
        // Clear pipeline entries left behind by exhausted inner cursors.
        clearUnusedPipelineEntries();

        while (continueFillingPipeline()) {
            CompletableFuture<RecordCursorResult<T>> outerNext = ensureOuterCursorAdvanced();
            if (outerNext == null) {
                return ALREADY_CANCELLED;
            }

            if (!outerNext.isDone()) {
                // Still waiting for outer future. Check back when it has finished.
                final PipelineQueueEntry<V, T> nextEntry = peekPipeline();
                if (nextEntry == null) {
                    return outerNext.thenApply(vignore -> true); // loop back to process outer result
                } else {
                    // keep looping unless we get something from the next entry's inner cursor or the next cursor is ready
                    final CompletableFuture<PipelineQueueEntry<V, T>> innerPipelineFuture = nextEntry.getNextInnerPipelineFuture();
                    return CompletableFuture.anyOf(outerNext, innerPipelineFuture).thenApply(vignore ->
                        !innerPipelineFuture.isDone() || innerPipelineFuture.join().doesNotHaveReturnableResult());
                }
            }

            final RecordCursorResult<T> outerResult = outerNext.join();

            if (outerResult.hasNext()) {
                final RecordCursorContinuation priorOuterContinuation = outerContinuation;
                final T outerValue = outerResult.get();
                final byte[] outerCheckValue = checkValueFunction == null ? null : checkValueFunction.apply(outerValue);
                byte[] innerContinuation = null;
                if (initialInnerContinuation != null) {
                    // Check if the outer cursor is positioned to the same place as before, by comparing the outer
                    // check value to the initial check value used to build the cursor. If they match (or one is missing),
                    // use the given initial inner continuation. Otherwise, something about the outer cursor changed,
                    // so we should start the inner cursor from the beginning.
                    if (initialCheckValue == null || outerCheckValue == null || Arrays.equals(initialCheckValue, outerCheckValue)) {
                        innerContinuation = initialInnerContinuation;
                        initialInnerContinuation = null;
                    }
                }
                final RecordCursor<V> innerCursor = innerCursorFunction.apply(outerValue, innerContinuation);
                outerContinuation = outerResult.getContinuation();
                addEntryToPipeline(PipelineQueueEntry.newInstanceWithBackgroundComputationOfFirstResult(innerCursor, priorOuterContinuation, outerResult, outerCheckValue));
                outerNextFuture = null; // done with this future, advance outer cursor next time
                // keep looping to fill pipeline
            } else { // don't have next, and won't ever with this cursor
                // Add sentinel to end of pipeline
                addEntryToPipeline(PipelineQueueEntry.newSentinel(outerContinuation, outerResult));
                outerExhausted = true;
                // Wait for next entry, as if pipeline were full
                break;
            }
        }

        // One of the following holds:
        // 1) The pipeline is full.
        // 2) We just added something to it.
        // 3) The outer cursor is exhausted and so the last element in the pipeline is a sentinel that will never be removed.
        // 4) A concurrent operation cancelled this cursor and so the pipeline is empty
        // The only case where the element should be null is when the cursor has been closed, so return CANCELLED in
        // that case.
        PipelineQueueEntry<V, T> peeked = peekPipeline();
        if (peeked == null) {
            return ALREADY_CANCELLED;
        }
        return peeked.getNextInnerPipelineFuture().thenApply(PipelineQueueEntry::doesNotHaveReturnableResult);
    }

    private synchronized void clearUnusedPipelineEntries() {
        while (!pipeline.isEmpty() && pipeline.peek().doesNotHaveReturnableResult()) {
            pipeline.remove().close();
        }
    }

    @Nullable
    private synchronized CompletableFuture<RecordCursorResult<T>> ensureOuterCursorAdvanced() {
        if (closed) {
            return null;
        }
        if (outerNextFuture == null) {
            outerNextFuture = outerCursor.onNext();
        }
        return outerNextFuture;
    }

    private synchronized void addEntryToPipeline(PipelineQueueEntry<V, T> pipelineQueueEntry) {
        if (closed) {
            pipelineQueueEntry.close();
        }
        pipeline.add(pipelineQueueEntry);
    }

    private synchronized boolean continueFillingPipeline() {
        return !closed && !outerExhausted && pipeline.size() < pipelineSize;
    }

    private synchronized PipelineQueueEntry<V, T> peekPipeline() {
        // Not a lot in this method, but the underlying queue isn't thread safe so
        return pipeline.peek();
    }

    private static class PipelineQueueEntry<V, T> {
        final RecordCursor<V> innerCursor;
        final RecordCursorContinuation priorOuterContinuation;
        final RecordCursorResult<T> outerResult;
        final byte[] outerCheckValue;

        private CompletableFuture<RecordCursorResult<V>> innerFuture;

        private PipelineQueueEntry(RecordCursor<V> innerCursor,
                                  RecordCursorContinuation priorOuterContinuation,
                                  RecordCursorResult<T> outerResult,
                                  byte[] outerCheckValue) {
            this.innerCursor = innerCursor;
            this.priorOuterContinuation = priorOuterContinuation;
            this.outerResult = outerResult;
            this.outerCheckValue = outerCheckValue;
        }

        @Nonnull
        public CompletableFuture<PipelineQueueEntry<V, T>> getNextInnerPipelineFuture() {
            if (innerFuture == null) {
                setInnerFuture();
            }
            return innerFuture.thenApply(vignore -> this);
        }

        private void setInnerFuture() {
            if (innerCursor == null) {
                innerFuture = CompletableFuture.completedFuture(RecordCursorResult.exhausted());
            } else {
                innerFuture = innerCursor.onNext();
            }
        }

        public boolean doesNotHaveReturnableResult() {
            if (innerCursor == null ||       // Hit sentinel, so we have a returnable result
                    innerFuture == null ||   // Inner future hasn't been started yet.
                    !innerFuture.isDone()) { // No result yet. Don't know whether result will be returnable.
                return false;
            }

            final RecordCursorResult<V> innerResult = innerFuture.join();
            if (innerResult.hasNext()) {
                return false; // a result with a value is returnable by the cursor
            } else { // inner cursor exhausted
                // If the inner cursor is exhausted, we should return the first value from the next inner cursor.
                // If the inner cursor stopped for any other reason, it's not valid to take from later in the pipeline.
                return innerResult.getNoNextReason().isSourceExhausted();
            }
        }

        public void close() {
            if (innerFuture != null && innerFuture.cancel(false)) {
                innerCursor.close();
            }
        }

        @Nonnull
        public RecordCursorResult<V> nextResult() {
            // Only called after the future from getNextInnerPipelineFuture() has completed, so this join() is non-blocking.
            final RecordCursorResult<V> innerResult = innerFuture.join();
            final RecordCursorResult<V> result;
            if (innerResult.hasNext()) {
                result = RecordCursorResult.withNextValue(innerResult.get(), toContinuation());
            } else {
                NoNextReason reason;
                if (innerResult.getNoNextReason().isSourceExhausted()) {
                    // If the outer cursor had another result, we would have skipped over this exhausted result from
                    // the inner cursor and moved on to the next inner cursor (as indicated by
                    // doesNotHaveReturnableResult()). Thus, the outer cursor must be stopped.
                    reason = outerResult.getNoNextReason();
                } else {
                    reason = innerResult.getNoNextReason();
                }
                result = RecordCursorResult.withoutNextValue(toContinuation(), reason);
            }
            innerFuture = null;
            return result;
        }

        @Nonnull
        private Continuation<T, V> toContinuation() {
            return new Continuation<>(priorOuterContinuation, outerResult, outerCheckValue, innerFuture.join());
        }

        @Nonnull
        public static <V, T> PipelineQueueEntry<V, T> newInstance(RecordCursor<V> innerCursor,
                                                                  RecordCursorContinuation priorOuterContinuation,
                                                                  RecordCursorResult<T> outerResult,
                                                                  byte[] outerCheckValue) {
            return new PipelineQueueEntry<>(innerCursor, priorOuterContinuation, outerResult, outerCheckValue);
        }

        @Nonnull
        public static <V, T> PipelineQueueEntry<V, T> newInstanceWithBackgroundComputationOfFirstResult(RecordCursor<V> innerCursor,
                                                                                                        RecordCursorContinuation priorOuterContinuation,
                                                                                                        RecordCursorResult<T> outerResult,
                                                                                                        byte[] outerCheckValue) {
            final var result = newInstance(innerCursor, priorOuterContinuation, outerResult, outerCheckValue);
            result.setInnerFuture();
            return result;
        }

        @Nonnull
        public static <V, T> PipelineQueueEntry<V, T> newSentinel(RecordCursorContinuation priorOuterContinuation,
                                                                  RecordCursorResult<T> outerResult) {
            return new PipelineQueueEntry<>(null, priorOuterContinuation, outerResult, null);
        }
    }

    private static class Continuation<T, V> implements RecordCursorContinuation {
        @Nonnull
        private final RecordCursorContinuation priorOuterContinuation;
        @Nonnull
        private final RecordCursorResult<T> outerResult;
        @Nullable
        private final byte[] outerCheckValue;
        @Nonnull
        private final RecordCursorResult<V> innerResult;
        @Nullable
        private ByteString cachedByteString;
        @Nullable
        private byte[] cachedBytes;

        public Continuation(@Nonnull RecordCursorContinuation priorOuterContinuation,
                            @Nonnull RecordCursorResult<T> outerResult,
                            @Nullable byte[] outerCheckValue,
                            @Nonnull RecordCursorResult<V> innerResult) {
            this.priorOuterContinuation = priorOuterContinuation;
            this.outerResult = outerResult;
            this.outerCheckValue = outerCheckValue;
            this.innerResult = innerResult;
        }

        @Override
        public boolean isEnd() {
            return outerResult.getContinuation().isEnd() && innerResult.getContinuation().isEnd();
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (isEnd()) {
                return ByteString.EMPTY;
            }
            if (cachedByteString == null) {
                final RecordCursorProto.FlatMapContinuation.Builder builder = RecordCursorProto.FlatMapContinuation.newBuilder();
                final RecordCursorContinuation innerContinuation = innerResult.getContinuation();

                if (innerContinuation.isEnd()) {
                    // This was the last of the inner cursor. Take continuation from outer after it.
                    builder.setOuterContinuation(outerResult.getContinuation().toByteString());
                } else {
                    // This was in the middle of the inner cursor. Take continuation from outer before it and arrange to skip to it.
                    final ByteString priorOuterContinuationBytes = priorOuterContinuation.toByteString();
                    if (!priorOuterContinuationBytes.isEmpty()) { // isn't start or end continuation
                        builder.setOuterContinuation(priorOuterContinuationBytes);
                    }
                    if (outerCheckValue != null) {
                        builder.setCheckValue(ZeroCopyByteString.wrap(outerCheckValue));
                    }
                    builder.setInnerContinuation(innerContinuation.toByteString());
                }
                cachedByteString = builder.build().toByteString();
            }
            return cachedByteString;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            }
            if (cachedBytes == null) {
                cachedBytes = toByteString().toByteArray();
            }
            return cachedBytes;
        }
    }
}
