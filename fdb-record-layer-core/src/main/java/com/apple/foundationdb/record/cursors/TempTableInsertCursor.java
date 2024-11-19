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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.ProtoSerializable;
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
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that returns the items of a child cursor, and as a side effect, it adds the items to a designated
 * {@link TempTable}.
 */
@API(API.Status.EXPERIMENTAL)
public class TempTableInsertCursor implements RecordCursor<QueryResult> {

    @Nonnull
    private final RecordCursor<QueryResult> childCursor;
    @Nonnull
    private final TempTable tempTable;

    private TempTableInsertCursor(@Nonnull final RecordCursor<QueryResult> childCursor,
                                  @Nonnull final TempTable tempTable) {
        this.childCursor = childCursor;
        this.tempTable = tempTable;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
        return childCursor.onNext().thenApply(childCursorResult -> {
            if (!childCursorResult.hasNext()) {
                if (childCursorResult.getNoNextReason().isSourceExhausted()) {
                    return RecordCursorResult.exhausted();
                } else {
                    return RecordCursorResult.withoutNextValue(new Continuation(tempTable, childCursorResult.getContinuation()), childCursorResult.getNoNextReason());
                }
            } else {
                tempTable.add(Objects.requireNonNull(childCursorResult.get()));
                return RecordCursorResult.withNextValue(childCursorResult.get(), new Continuation(tempTable, childCursorResult.getContinuation()));
            }
        });
    }

    @Override
    public void close() {
        childCursor.close();
    }

    @Override
    public boolean isClosed() {
        return childCursor.isClosed();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return childCursor.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            childCursor.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    /**
     * Creates a new instance of {@link TempTableInsertCursor}.
     * @param unparsed An optional unparsed continuation, if {@code NULL} it is assumed that the cursor is at the beginning.
     * @param tempTableDeserializer A method that, given a serialized {@link TempTable} returns a runtime {@link TempTable},
     *                              note that the serialized {@link TempTable} can be {@code null}.
     * @param childCursorCreator A creator of the child cursor, using a nullable child continuation.
     *
     * @return a new {@link TempTableInsertCursor} that either resumes the execution according to the given continuation,
     *         or starts from the beginning.
     */
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public static TempTableInsertCursor from(@Nullable byte[] unparsed,
                                             @Nonnull Function<PTempTable, TempTable> tempTableDeserializer,
                                             @Nonnull Function<byte[], RecordCursor<QueryResult>> childCursorCreator) {
        final var continuation = Continuation.from(unparsed, tempTableDeserializer);
        final var childCursor = childCursorCreator.apply(continuation.getChildContinuation().toBytes());
        return new TempTableInsertCursor(childCursor, continuation.getTempTable());
    }

    private static class Continuation implements RecordCursorContinuation {

        @Nonnull
        private final TempTable tempTable;
        @Nonnull
        private final RecordCursorContinuation childContinuation;

        private Continuation(@Nonnull final TempTable tempTable,
                             @Nonnull final RecordCursorContinuation childContinuation) {
            this.tempTable = tempTable;
            this.childContinuation = childContinuation;
        }

        @Nonnull
        public RecordCursorContinuation getChildContinuation() {
            return childContinuation;
        }

        @Nonnull
        public TempTable getTempTable() {
            return tempTable;
        }

        @Nonnull
        private RecordCursorProto.TempTableInsertContinuation toProto() {
            RecordCursorProto.TempTableInsertContinuation.Builder builder =
                    RecordCursorProto.TempTableInsertContinuation.newBuilder();
            builder.setTempTable(tempTable.toProto().toByteString());
            ByteString childBytes = childContinuation.toByteString();
            if (!childBytes.isEmpty()) {
                builder.setChildContinuation(childBytes);
            }
            return builder.build();
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            return toProto().toByteString();
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            }
            return toByteString().toByteArray();
        }

        @Override
        public boolean isEnd() {
            return childContinuation.isEnd();
        }

        @Nonnull
        private static  Continuation from(@Nonnull final RecordCursorProto.TempTableInsertContinuation parsed,
                                          @Nullable final PTempTable parsedTempTable,
                                          @Nonnull final Function<PTempTable, TempTable> tempTableDeserializer) {
            final var tempTable = tempTableDeserializer.apply(parsedTempTable);
            final var childContinuation = parsed.hasChildContinuation()
                                          ? ByteArrayContinuation.fromNullable(parsed.getChildContinuation().toByteArray())
                                          : RecordCursorStartContinuation.START;
            return new Continuation(tempTable, childContinuation);
        }

        @Nonnull
        private static Continuation from(@Nullable final byte[] unparsed,
                                         @Nonnull final Function<PTempTable, TempTable> tempTableDeserializer) {
            if (unparsed == null) {
                return new Continuation(tempTableDeserializer.apply(null), RecordCursorStartContinuation.START);
            } else {
                try {
                    final var parsed = RecordCursorProto.TempTableInsertContinuation.parseFrom(unparsed);
                    final var parsedTempTable = parsed.hasTempTable() ? PTempTable.parseFrom(parsed.getTempTable()) : null;
                    return Continuation.from(parsed, parsedTempTable, tempTableDeserializer);
                } catch (InvalidProtocolBufferException ex) {
                    throw new RecordCoreException("invalid continuation", ex)
                            .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsed));
                }
            }
        }
    }
}
