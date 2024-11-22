/*
 * RecursiveUnionCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A cursor that returns repeatedly executes its children until reaching a fix-point. Specifically, it returns the results
 * of an initial cursor as-is until it finishes, and then it repeatedly execute the other cursor recursively until it does
 * not produce any new results.
 * @param <T> The type of the cursor elements.
 */
public class RecursiveUnionCursor<T> implements RecordCursor<T> {

    @Nonnull
    private final RecordCursor<T> initialCursor;

    @Nonnull
    private final Supplier<RecordCursor<T>> recursiveCursorSupplier;

    @Nonnull
    private RecordCursor<T> recursiveCursor;

    @Nonnull
    private final Supplier<Boolean> isReadingFromInitialCursorSupplier;

    @Nonnull
    private final Supplier<Boolean> recursiveStepCompletionCallback;

    @Nonnull
    private final Executor executor;

    private boolean isInitialState;

    private RecursiveUnionCursor(@Nonnull final RecordCursor<T> initialCursor,
                                 @Nonnull final Supplier<RecordCursor<T>> recursiveCursorSupplier,
                                 @Nonnull final RecordCursor<T> recursiveCursor,
                                 @Nonnull final Executor executor,
                                 @Nonnull final Supplier<Boolean> isReadingFromInitialCursorSupplier,
                                 @Nonnull final Supplier<Boolean> recursiveStepCompletionCallback,
                                 boolean isInitialState) {
        this.initialCursor = initialCursor;
        this.recursiveCursorSupplier = recursiveCursorSupplier;
        this.recursiveCursor = recursiveCursor;
        this.isReadingFromInitialCursorSupplier = isReadingFromInitialCursorSupplier;
        this.isInitialState = isInitialState;
        this.recursiveStepCompletionCallback = recursiveStepCompletionCallback;
        this.executor = executor;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (isInitialState) {
            return initialCursor.onNext().thenCompose(recordCursorResult -> {
                if (!recordCursorResult.hasNext()) {
                    isInitialState = false;
                    if (recordCursorResult.getNoNextReason().isSourceExhausted()) {
                        recursiveCursor = recursiveCursorSupplier.get();
                        return onNextRecursive();
                    } else {
                        return wrapLastResult(recordCursorResult);
                    }
                } else {
                    return wrapNextResult(recordCursorResult);
                }
            });
        } else {
            return onNextRecursive();
        }
    }

    @Nonnull
    private CompletableFuture<RecordCursorResult<T>> onNextRecursive() {
        return recursiveCursor.onNext().thenCompose(recordCursorResult -> {
            if (!recordCursorResult.hasNext()) {
                if (recordCursorResult.getNoNextReason().isSourceExhausted()) {
                    return recurse();
                } else {
                    return wrapLastResult(recordCursorResult);
                }
            } else {
                return wrapNextResult(recordCursorResult);
            }
        });
    }

    @Nonnull
    private CompletableFuture<RecordCursorResult<T>> recurse() {
        if (recursiveStepCompletionCallback.get()) {
            // restart the cursor of the recursive state, without plugging in any continuation from previous recursive
            // state.
            recursiveCursor = recursiveCursorSupplier.get();
            return onNextRecursive();
        }
        return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
    }

    @Nonnull
    private CompletableFuture<RecordCursorResult<T>> wrapLastResult(@Nonnull RecordCursorResult<T> innerCursorResult) {
        return CompletableFuture.completedFuture(RecordCursorResult.withoutNextValue(
                new Continuation(isInitialState, isReadingFromInitialCursorSupplier,
                        innerCursorResult::getContinuation, () -> RecordCursorStartContinuation.START, false),
                innerCursorResult.getNoNextReason()));
    }

    @Nonnull
    private CompletableFuture<RecordCursorResult<T>> wrapNextResult(@Nonnull RecordCursorResult<T> innerCursorResult) {
        final var continuation = new Continuation(isInitialState, isReadingFromInitialCursorSupplier,
                innerCursorResult::getContinuation, () -> RecordCursorStartContinuation.START, false);
        return CompletableFuture.completedFuture(RecordCursorResult.withNextValue(innerCursorResult.get(), continuation));
    }

    @Override
    public void close() {
        initialCursor.close();
        recursiveCursor.close();
    }

    @Override
    public boolean isClosed() {
        return recursiveCursor.isClosed(); // todo: improve.
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            initialCursor.accept(visitor);
            recursiveCursor.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    @Nonnull
    public static <T> RecursiveUnionCursor<T> from(@Nullable final byte[] unparsed,
                                                   @Nonnull final Function<ByteString, RecordCursor<T>> initialCursorCreator,
                                                   @Nonnull final Function<ByteString, RecordCursor<T>> recursiveCursorCreator,
                                                   @Nonnull final Supplier<Boolean> isReadingFromInitialCursorSupplier,
                                                   @Nonnull final Consumer<Boolean> wasInInitialStateConsumer,
                                                   @Nonnull final Consumer<Boolean> wasReadingFromFirstTempTableConsumer,
                                                   @Nonnull final Supplier<Boolean> recursiveStepCompletionCallback,
                                                   @Nonnull Executor executor) {
        if (unparsed == null) {
            final var initialCursor = initialCursorCreator.apply(null);
            final var recursiveCursor = recursiveCursorCreator.apply(null);
            return new RecursiveUnionCursor<>(initialCursor, () -> recursiveCursorCreator.apply(null), recursiveCursor, executor, isReadingFromInitialCursorSupplier,
                    recursiveStepCompletionCallback, true);
        } else {
            RecordCursorProto.RecursiveCursorContinuation proto;
            try {
                proto = RecordCursorProto.RecursiveCursorContinuation.parseFrom(unparsed);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsed));
            }
            // resume the state.
            wasInInitialStateConsumer.accept(proto.getIsInitialState());
            wasReadingFromFirstTempTableConsumer.accept(proto.getIsReadingInitialCursor());
            final var initialCursor = initialCursorCreator.apply(proto.getInitialCursorContinuation());
            final var recursiveCursor = recursiveCursorCreator.apply(proto.getRecursiveCursorContinuation());
            return new RecursiveUnionCursor<>(initialCursor, () -> recursiveCursorCreator.apply(null), recursiveCursor, executor, isReadingFromInitialCursorSupplier,
                    recursiveStepCompletionCallback, proto.getIsReadingInitialCursor());
        }
    }

    private static class Continuation implements RecordCursorContinuation {

        private final boolean isInitialState;

        @Nonnull
        private final Supplier<Boolean> isReadingInitialCursorSupplier;

        @Nonnull
        private final Supplier<RecordCursorContinuation> initialCursorContinuation;

        @Nonnull
        private final Supplier<RecordCursorContinuation> recursiveCursorContinuation;

        private final boolean isEnd;

        Continuation(boolean isInitialState,
                     @Nonnull final Supplier<Boolean> isReadingInitialCursorSupplier,
                     @Nonnull final Supplier<RecordCursorContinuation> initialCursorContinuation,
                     @Nonnull final Supplier<RecordCursorContinuation> recursiveCursorContinuation,
                     boolean isEnd) {
            this.isInitialState = isInitialState;
            this.isReadingInitialCursorSupplier = isReadingInitialCursorSupplier;
            this.initialCursorContinuation = initialCursorContinuation;
            this.recursiveCursorContinuation = recursiveCursorContinuation;
            this.isEnd = isEnd;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd) {
                return null;
            }
            return toByteString().toByteArray();
        }

        @Override
        @Nonnull
        public ByteString toByteString() {
            if (isEnd()) {
                return ByteString.EMPTY;
            } else {
                return RecordCursorProto.RecursiveCursorContinuation.newBuilder()
                        .setInitialCursorContinuation(initialCursorContinuation.get().toByteString())
                        .setRecursiveCursorContinuation(recursiveCursorContinuation.get().toByteString())
                        .setIsReadingInitialCursor(isReadingInitialCursorSupplier.get())
                        .setIsInitialState(isInitialState)
                        .build().toByteString();
            }
        }

        @Override
        public boolean isEnd() {
            return isEnd;
        }
    }
}
