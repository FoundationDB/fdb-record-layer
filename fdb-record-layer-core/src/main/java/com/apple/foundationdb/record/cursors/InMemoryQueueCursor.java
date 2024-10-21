/*
 * InMemoryQueueCursor.java
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
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * A cursor that enables adding {@link QueryResult} elements to it. It stores these elements in a {@link Queue}
 * allowing the consumers to retrieve them in the same order.
 */
public class InMemoryQueueCursor implements RecordCursor<QueryResult> {

    @Nonnull
    private final Executor executor;

    @Nonnull
    private final Queue<CompletableFuture<QueryResult>> elements;

    @Nullable
    private CompletableFuture<QueryResult> itemInFlight;

    public InMemoryQueueCursor(@Nullable Continuation continuation) {
        this(ForkJoinPool.commonPool(), continuation);
    }

    public InMemoryQueueCursor(@Nonnull final Executor executor) {
        this(executor, null);
    }

    public InMemoryQueueCursor(@Nonnull final Executor executor, @Nullable Continuation continuation) {
        this.executor = executor;
        elements = new LinkedList<>();
        if (continuation != null) {
            elements.addAll(continuation.elements);
        }
    }

    /**
     * Add a new {@link QueryResult} element to the queue.
     * @param element the new element to be added.
     */
    public void add(@Nonnull QueryResult element) {
        elements.add(CompletableFuture.completedFuture(element));
    }

    /**
     * Add a new {@link QueryResult} elements to the queue.
     * @param elements the new elements to be added.
     */
    public void add(@Nonnull QueryResult... elements) {
        Arrays.stream(elements).forEach(element -> add(CompletableFuture.completedFuture(element)));
    }

    /**
     * Add a future that produces a new {@link QueryResult} element.
     * @param element a future {@link QueryResult}.
     */
    public void add(@Nonnull CompletableFuture<QueryResult> element) {
        elements.add(element);
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
        if (!elements.isEmpty()) {
            itemInFlight = elements.poll();
            return itemInFlight.thenApply(result -> RecordCursorResult.withNextValue(result, new Continuation(elements)));
        } else {
            return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
        }
    }

    @Override
    public void close() {
        // cancel items being processed.
        if (itemInFlight != null) {
            itemInFlight.cancel(false);
        }
        // and cancel any remaining items.
        while (!elements.isEmpty()) {
            elements.remove().cancel(false);
        }
    }

    @Override
    public boolean isClosed() {
        return elements.isEmpty() && (itemInFlight == null || itemInFlight.isDone());
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    /**
     * Continuation that saves the state of the {@link InMemoryQueueCursor}, i.e. the set of records that exist
     * in the underlying queue which are not consumed yet by the cursor reader.
     */
    public static class Continuation implements RecordCursorContinuation {
        @Nonnull
        private final Collection<CompletableFuture<QueryResult>> elements;

        @Nullable
        private ByteString cachedByteString;

        @Nullable
        private byte[] cachedBytes;

        private Continuation(@Nonnull Collection<CompletableFuture<QueryResult>> elements) {
            this.elements = elements;
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
                final var builder = RecordCursorProto.InMemoryQueueContinuation.newBuilder();
                for (final var element : elements) {
                    builder.addElements(ZeroCopyByteString.wrap(element.join().serialize()));
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
            final RecordCursorProto.InMemoryQueueContinuation inMemoryQueueContinuation;
            try {
                inMemoryQueueContinuation = RecordCursorProto.InMemoryQueueContinuation.parseFrom(bytes);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
            }
            return new Continuation(inMemoryQueueContinuation.getElementsList().stream()
                    .map(element -> CompletableFuture.completedFuture(QueryResult.deserialize(descriptor,
                            element.toByteArray()))).collect(ImmutableList.toImmutableList()));
        }
    }
}
