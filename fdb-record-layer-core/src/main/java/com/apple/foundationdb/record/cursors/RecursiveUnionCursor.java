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
import com.apple.foundationdb.record.EvaluationContext;
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
import com.apple.foundationdb.record.query.plan.plans.RecursiveUnionQueryPlan;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that repeatedly executes its children until reaching a fix-point. Specifically, it returns the results
 * of an {@code initial} cursor as-is until it finishes, and then it repeatedly execute the other, {@code recursive} cursor
 * until it is done, in a way, it is similar to {@link ConcatCursor} but it is much more involved.
 * <br>
 * The union cursor has two children cursors: the {@code initial} cursor and the {@code recursive} cursor. The {@code initial}
 * cursor is used to seed the recursion (base case), while the {@code recursive} cursor is executed repeatedly until it
 * reaches a fix-point.
 * <br>
 * During execution, the cursor reads from a child cursor and writes into a {@link TempTable}, at
 * the end of the recursive step, its swaps that {@link TempTable} with another {@link TempTable} that is used by the
 * recursive scan to set up the <i>next</i> recursive step. The fix-point is reached by checking whether the
 * {@link TempTable}, that the recursive scan is supposed to read, is empty or not, in other words, whether recursive
 * step {@code n-1} produced any results for the current step {@code n}. If not, the recursion stops immediately.
 * <br>
 * The state management of this cursor is mostly handled one level above by the {@link RecursiveUnionQueryPlan}. For example,
 * the manipulation of the read and write {@link TempTable} is handled there through manipulation of the {@link EvaluationContext}.
 * A {@link RecursiveStateManager} represents all the aspects related to state management, this abstraction offers a well-defined API
 * that enables probing (e.g. for the purpose of creating a {@link Continuation}) and manipulating the recursive state
 * (e.g. when transitioning from {@code initial} to {@code recursive} cursor) without direct interaction between both
 * entities making it easier to test and reason about the recursive state in isolation.
 *
 * @param <T> The type of the cursor elements.
 */
public class RecursiveUnionCursor<T> implements RecordCursor<T> {

    /**
     * Interface for recursive state management, the caller is expected to invoke respective callbacks when certain
     * events occur so the internal state mutates accordingly, it also offers a set of methods that enable examining
     * current state.
     * @param <T> The type of the cursor elements.
     */
    public interface RecursiveStateManager<T> {

        /**
         * Callback notifying the manager that the current cursor is exhausted.
         */
        void notifyCursorIsExhausted();

        /**
         * Checks whether it is possible to transition from current recursive step {@code n} to next step {@code n + 1}.
         * @return {@code True} if it is possible to transition from current recursive step {@code n} to next step {@code n + 1},
         * otherwise {@code false}.
         */
        boolean canTransitionToNewStep();

        /**
         * Retrieve the currently active cursor, an active cursor is either the {@code initial} cursor or the {@code recursive} cursor.
         * @return The currently active cursor.
         */
        @Nonnull
        RecordCursor<T> getActiveStateCursor();

        /**
         * Retrieve the {@link TempTable} that is owned by the {@link RecursiveUnionQueryPlan}.
         * @return The {@link TempTable} that is owned by the {@link RecursiveUnionQueryPlan}.
         */
        @Nonnull
        TempTable getRecursiveUnionTempTable();

        /**
         * Checks whether the execution is still in the initial phase or not.
         * @return {@code True} if the execution is still in the initial phase, otherwise {@code false}, i.e. the execution
         * is in the recursive phase.
         */
        boolean isInitialState();

        /**
         * Checks whether read and write buffers ({@link TempTable}s) are swapped or not.
         * @return {@code True} if the read and write buffers are swapped, otherwise {@code false}.
         */
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
                if (cursorResult.getNoNextReason().isSourceExhausted()) {
                    recursiveStateManager.notifyCursorIsExhausted();
                    if (recursiveStateManager.canTransitionToNewStep()) {
                        activeStateCursor = recursiveStateManager.getActiveStateCursor();
                        // although this can take advantage of tail-recursion optimization, I am not certain it is done
                        // by the compiler. perhaps a better approach would be to use something like AsyncUtil.whileTrue
                        // instead of this recursive call.
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
                new Continuation(recursiveStateManager.isInitialState(), innerCursorResult.getContinuation(),
                        recursiveStateManager.getRecursiveUnionTempTable(), recursiveStateManager.buffersAreFlipped()),
                innerCursorResult.getNoNextReason()));
    }

    @Nonnull
    private CompletableFuture<RecordCursorResult<T>> wrapNextResult(@Nonnull RecordCursorResult<T> innerCursorResult) {
        final var continuation = new Continuation(recursiveStateManager.isInitialState(), innerCursorResult.getContinuation(),
                recursiveStateManager.getRecursiveUnionTempTable(), recursiveStateManager.buffersAreFlipped());
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
     * Continuation that captures the state of execution of a {@link RecursiveUnionCursor} that is orchestrated by
     * {@link RecursiveUnionQueryPlan} through a {@link RecursiveStateManager}.
     */
    public static class Continuation implements RecordCursorContinuation {

        // whether the execution is still in initial phase or not.
        private final boolean isInitialState;

        // the continuation of the currently active child cursor.
        @Nonnull
        private final RecordCursorContinuation activeStateContinuation;

        // the temp table owned by RecursiveUnionQueryPlan.
        @Nonnull
        private final TempTable tempTable;

        // whether the temp table is used for reading or writing.
        private final boolean buffersAreFlipped;

        Continuation(boolean isInitialState,
                     @Nonnull final RecordCursorContinuation activeStateContinuation,
                     @Nonnull final TempTable tempTable,
                     boolean buffersAreFlipped) {
            this.isInitialState = isInitialState;
            this.activeStateContinuation = activeStateContinuation;
            this.tempTable = tempTable;
            this.buffersAreFlipped = buffersAreFlipped;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            return toByteString().toByteArray();
        }

        @Override
        @Nonnull
        public ByteString toByteString() {
            return RecordCursorProto.RecursiveCursorContinuation.newBuilder()
                    .setIsInitialState(isInitialState())
                    .setTempTable(getTempTable().toProto())
                    .setActiveStateContinuation(getActiveStateContinuation().toByteString())
                    .build().toByteString();
        }

        @Override
        public boolean isEnd() {
            return false;
        }

        @Nonnull
        public static Continuation from(@Nonnull final RecordCursorProto.RecursiveCursorContinuation message,
                                        @Nonnull final Function<PTempTable, TempTable> tempTableDeserializer) {
            final var childContinuation = message.hasActiveStateContinuation()
                                          ? ByteArrayContinuation.fromNullable(message.getActiveStateContinuation().toByteArray())
                                          : RecordCursorStartContinuation.START;
            final PTempTable parsedTempTable;
            try {
                parsedTempTable = PTempTable.parseFrom(message.getTempTable().toByteString());
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(message.toByteArray()));
            }
            final var buffersAreFlipped = message.getBuffersAreFlipped();
            return new Continuation(message.getIsInitialState(), childContinuation, tempTableDeserializer.apply(parsedTempTable), buffersAreFlipped);
        }

        /**
         * Parses a {@code byte[]} using a given {@link TempTable} deserializer into a {@link Continuation}.
         *
         * @param unparsedContinuationBytes The bytes to parse.
         * @param tempTableDeserializer The {@link TempTable} deserializer.
         * @return a parsed {@link Continuation}.
         */
        @Nonnull
        public static Continuation from(@Nonnull byte[] unparsedContinuationBytes,
                                        @Nonnull final Function<PTempTable, TempTable> tempTableDeserializer) {
            try {
                final var parsed = RecordCursorProto.RecursiveCursorContinuation.parseFrom(unparsedContinuationBytes);
                return from(parsed, tempTableDeserializer);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsedContinuationBytes));
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

        public boolean buffersAreFlipped() {
            return buffersAreFlipped;
        }
    }
}
