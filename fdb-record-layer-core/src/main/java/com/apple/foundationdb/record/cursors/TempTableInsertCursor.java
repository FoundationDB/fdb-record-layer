/*
 * TempTableInsertCursor.java
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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordSortingProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TempTableInsertCursor<T> implements RecordCursor<T> {
    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return null;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        return false;
    }

    private static class Continuation implements RecordCursorContinuation {

        @Nonnull
        private final Supplier<RecordMetaDataProto.PTempTable> tempTableSupplier;
        @Nonnull
        private final RecordCursorContinuation childContinuation;
        @Nullable
        private RecordCursorProto.TempTableInsertContinuation cachedProto;
        @Nullable
        private byte[] cachedBytes;

        private Continuation(@Nonnull final Supplier<RecordMetaDataProto.PTempTable> tempTableSupplier,
                             @Nonnull final RecordCursorContinuation childContinuation) {
            this.tempTableSupplier = tempTableSupplier;
            this.childContinuation = childContinuation;
        }

        @Nonnull
        RecordCursorProto.TempTableInsertContinuation toProto() {
            if (cachedProto == null) {
                RecordCursorProto.TempTableInsertContinuation.Builder builder =
                        RecordCursorProto.TempTableInsertContinuation.newBuilder();
                builder.setTempTable(tempTableSupplier.get().toByteString());
                ByteString childBytes = childContinuation.toByteString();
                if (!childBytes.isEmpty()) {
                    builder.setChildContinuation(childBytes);
                }
                cachedProto = builder.build();
            }
            return cachedProto;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            return toProto().toByteString();
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (cachedBytes == null) {
                cachedBytes = toByteString().toByteArray();
            }
            return cachedBytes;
        }

        @Override
        public boolean isEnd() {
            return childContinuation.isEnd();
        }

        @Nonnull
        static Continuation from(@Nonnull RecordCursorProto.TempTableInsertContinuation parsed,
                                 @Nonnull Supplier<RecordMetaDataProto.PTempTable> tempTableSupplier,
                                 @Nonnull Consumer<RecordMetaDataProto.PTempTable> tempTableConsumer) {
            // first, synchronize the state of the temp table through the consumer.
            if (parsed.hasTempTable()) {
                try {
                    tempTableConsumer.accept(RecordMetaDataProto.PTempTable.parseFrom(parsed.getTempTable()));
                } catch (InvalidProtocolBufferException ex) {
                    throw new RecordCoreException("invalid continuation", ex)
                            .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(parsed.getTempTable().toByteArray()));
                }
            }
            // second, construct the continuation.
            Continuation result = new Continuation();
            result.cachedProto = parsed;
            return result;
        }

        @Nonnull
        static Continuation from(@Nullable byte[] unparsed,
                                 @Nonnull Supplier<RecordMetaDataProto.PTempTable> tempTableSupplier,
                                 @Nonnull Consumer<RecordMetaDataProto.PTempTable> tempTableConsumer) {
            final Continuation result;
            if (unparsed == null) {
                result = new Continuation(tempTableSupplier, RecordCursorStartContinuation.START);
            } else {
                try {
                    result = Continuation.from(RecordCursorProto.TempTableInsertContinuation.parseFrom(unparsed),
                            tempTableSupplier, tempTableConsumer);
                } catch (InvalidProtocolBufferException ex) {
                    throw new RecordCoreException("invalid continuation", ex)
                            .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsed));
                }
                result.cachedBytes = unparsed;
            }
            return result;
        }
    }
}
