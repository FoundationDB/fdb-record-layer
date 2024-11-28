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

import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PTempTable;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that returns repeatedly executes its children until reaching a fix-point. Specifically, it returns the results
 * of an initial cursor as-is until it finishes, and then it repeatedly execute the other cursor recursively until it does
 * not produce any new results.
 * @param <T> The type of the cursor elements.
 */
public class RecursiveUnionCursor<T> implements RecordCursor<T> {

    /**
     * TODO.
     * @param <T> TODO.
     */
    public interface RecursiveStateManager<T> {

        void onCursorDone();

        boolean shouldContinue();

        RecordCursor<T> getActiveStateCursor();

        TempTable getRecursiveUnionTempTable();

        boolean isInitialState();

        boolean buffersAreFlipped();
    }

    @Nonnull
    private RecordCursor<T> activeStateCursor;

    @Nonnull
    private final Executor executor;

    @Nonnull
    private final RecursiveStateManager<T> recursiveStateManager;

    public RecursiveUnionCursor(@Nonnull final RecursiveStateManager<T> recursiveStateManager,
                                @Nonnull final Executor executor) {
        this.recursiveStateManager = recursiveStateManager;
        this.executor = executor;
        activeStateCursor = recursiveStateManager.getActiveStateCursor();
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return activeStateCursor.onNext().thenCompose(cursorResult -> {
            if (!cursorResult.hasNext()) {
                recursiveStateManager.onCursorDone();
                if (cursorResult.getNoNextReason().isSourceExhausted()) {
                    if (recursiveStateManager.shouldContinue()) {
                        activeStateCursor = recursiveStateManager.getActiveStateCursor();
                        return onNext();
                    } else {
                        return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
                    }
                } else {
                    return wrapLastResult(cursorResult);
                }
            } else {
                return wrapNextResult(cursorResult);
            }
        });
    }

    @Nonnull
    private CompletableFuture<RecordCursorResult<T>> wrapLastResult(@Nonnull RecordCursorResult<T> innerCursorResult) {
        return CompletableFuture.completedFuture(RecordCursorResult.withoutNextValue(
                new Continuation(recursiveStateManager.isInitialState(), innerCursorResult.getContinuation(), recursiveStateManager.getRecursiveUnionTempTable(), recursiveStateManager.buffersAreFlipped(), false),
                innerCursorResult.getNoNextReason()));
    }

    @Nonnull
    private CompletableFuture<RecordCursorResult<T>> wrapNextResult(@Nonnull RecordCursorResult<T> innerCursorResult) {
        final var continuation = new Continuation(recursiveStateManager.isInitialState(), innerCursorResult.getContinuation(), recursiveStateManager.getRecursiveUnionTempTable(), recursiveStateManager.buffersAreFlipped(), false);
        return CompletableFuture.completedFuture(RecordCursorResult.withNextValue(innerCursorResult.get(), continuation));
    }

    @Override
    public void close() {
        activeStateCursor.close();
    }

    @Override
    public boolean isClosed() {
        return activeStateCursor.isClosed(); // todo: improve.
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            activeStateCursor.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    /**
     * Continuation. TODO.
     */
    public static class Continuation implements RecordCursorContinuation {

        private final boolean isInitialState;

        @Nonnull
        private final RecordCursorContinuation activeStateContinuation;

        @Nonnull
        private final TempTable tempTable;

        private final boolean buffersAreFlipped;

        private final boolean isEnd;

        Continuation(boolean isInitialState,
                     @Nonnull final RecordCursorContinuation activeStateContinuation,
                     @Nonnull final TempTable tempTable,
                     boolean buffersAreFlipped,
                     boolean isEnd) {
            this.isInitialState = isInitialState;
            this.activeStateContinuation = activeStateContinuation;
            this.tempTable = tempTable;
            this.buffersAreFlipped = buffersAreFlipped;
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
                        .setIsInitialState(isInitialState())
                        .setTempTable(getTempTable().toProto().toByteString())
                        .setActiveStateContinuation(getActiveStateContinuation().toByteString())
                        .build().toByteString();
            }
        }

        @Override
        public boolean isEnd() {
            return isEnd;
        }

        @Nonnull
        public static Continuation from(@Nonnull RecordCursorProto.RecursiveCursorContinuation message, @Nonnull Function<PTempTable, TempTable> tempTableDeserializer) {
            final var childContinuation = message.hasActiveStateContinuation()
                                          ? ByteArrayContinuation.fromNullable(message.getActiveStateContinuation().toByteArray())
                                          : RecordCursorStartContinuation.START;
            PTempTable parsedTempTable;
            try {
                parsedTempTable = PTempTable.parseFrom(message.getTempTable());
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(message.toByteArray()));
            }
            final var buffersAreFlipped = message.getBuffersAreFlipped();
            return new Continuation(message.getIsInitialState(), childContinuation, tempTableDeserializer.apply(parsedTempTable), buffersAreFlipped, false);
        }

        @Nonnull
        public static Continuation from(@Nonnull byte[] unparsed, @Nonnull Function<PTempTable, TempTable> tempTableDeserializer) {
            try {
                final var parsed = RecordCursorProto.RecursiveCursorContinuation.parseFrom(unparsed);
                return from(parsed, tempTableDeserializer);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsed));
            }
        }

        public boolean isInitialState() {
            return isInitialState;
        }

        @Nonnull
        public RecordCursorContinuation getActiveStateContinuation() {
            return activeStateContinuation;
        }

        @Nonnull
        public TempTable getTempTable() {
            return tempTable;
        }


        public boolean isBuffersAreFlipped() {
            return buffersAreFlipped;
        }
    }
}
