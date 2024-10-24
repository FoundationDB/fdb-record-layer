/*
 * RecursiveCursor.java
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
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Recursive Cursor.
 */
public class DoubleBufferCursor implements RecordCursor<QueryResult> {

    @Nonnull
    private final Executor executor;

    @Nonnull
    private final InMemoryQueueCursor firstBuffer;

    @Nonnull
    private final InMemoryQueueCursor secondBuffer;

    private boolean readFromFirstBuffer;

    public DoubleBufferCursor(@Nonnull final Executor executor) {
        this(executor, null);
    }

    public DoubleBufferCursor(@Nonnull final Executor executor, @Nullable Continuation continuation) {
        this.executor = executor;
        firstBuffer = new InMemoryQueueCursor(executor, continuation != null ? continuation.firstBufferContinuation : null);
        secondBuffer = new InMemoryQueueCursor(executor, continuation != null ? continuation.secondBufferContinuation : null);
        readFromFirstBuffer = true;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
        // need to define continuation semantics

        return getReadCursor().onNext();
    }

    @Override
    public void close() {
        firstBuffer.close();
        secondBuffer.close();
    }

    @Override
    public boolean isClosed() {
        return firstBuffer.isClosed() && secondBuffer.isClosed();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Nonnull
    private InMemoryQueueCursor getReadCursor() {
        return readFromFirstBuffer ? firstBuffer : secondBuffer;
    }

    @Nonnull
    private InMemoryQueueCursor getWriteCursor() {
        return readFromFirstBuffer ? secondBuffer : firstBuffer;
    }

    /**
     * Add a new {@link QueryResult} element to the queue.
     * @param element the new element to be added.
     */
    public void add(@Nonnull QueryResult element) {
        getWriteCursor().add(element);
    }

    /**
     * Add a new {@link QueryResult} elements to the queue.
     * @param elements the new elements to be added.
     */
    public void add(@Nonnull QueryResult... elements) {
        Arrays.stream(elements).forEach(this::add);
    }

    public void flip() {
        readFromFirstBuffer = !readFromFirstBuffer;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    /**
     * TODO.
     */
    public static class Continuation implements RecordCursorContinuation {

        private final boolean firstIsReadBuffer;

        @Nonnull
        private final InMemoryQueueCursor.Continuation firstBufferContinuation;

        @Nonnull
        private final InMemoryQueueCursor.Continuation secondBufferContinuation;

        @Nullable
        private ByteString cachedByteString;

        @Nullable
        private byte[] cachedBytes;

        private Continuation(boolean firstIsReadBuffer,
                             @Nonnull InMemoryQueueCursor.Continuation firstBufferContinuation,
                             @Nonnull InMemoryQueueCursor.Continuation secondBufferContinuation) {
            this.firstIsReadBuffer = firstIsReadBuffer;
            this.firstBufferContinuation = firstBufferContinuation;
            this.secondBufferContinuation = secondBufferContinuation;
        }

        @Override
        public boolean isEnd() {
            return false;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (isEnd()) {
                return ByteString.EMPTY;
            }
            if (cachedByteString == null) {
                final var builder = RecordCursorProto.DoubleBufferContinuation.newBuilder();
                builder.setFirstIsReadBuffer(firstIsReadBuffer)
                        .setFirstContinuation(firstBufferContinuation.toByteString())
                        .setSecondContinuation(secondBufferContinuation.toByteString());
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

        @Nonnull
        public static Continuation from(@Nonnull byte[] bytes) {
            return fromInternal(null, bytes);
        }

        @Nonnull
        public static Continuation from(@Nonnull Descriptors.Descriptor descriptor, @Nonnull byte[] bytes) {
            return fromInternal(descriptor, bytes);
        }

        @Nonnull
        private static Continuation fromInternal(@Nullable Descriptors.Descriptor descriptor, @Nonnull byte[] bytes) {
            final RecordCursorProto.DoubleBufferContinuation doubleBufferContinuation;
            try {
                doubleBufferContinuation = RecordCursorProto.DoubleBufferContinuation.parseFrom(bytes);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
            }
            final var firstBufferContinuation = InMemoryQueueCursor.Continuation.from(descriptor, doubleBufferContinuation.getFirstContinuation());
            final var secondBufferContinuation = InMemoryQueueCursor.Continuation.from(descriptor, doubleBufferContinuation.getSecondContinuation());
            return new Continuation(doubleBufferContinuation.getFirstIsReadBuffer(), firstBufferContinuation, secondBufferContinuation);
        }
    }
}
