/*
 * TempTable.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * A mutable, temporary, serializable, and in-memory buffer of {@link QueryResult}s. It is aimed to be used as a temporary
 * placeholder for computation results produced by some physical operator, but can be leveraged to represent, for example,
 * a SQL temporary tables as well.
 */
public class TempTable {

    @Nonnull
    private final Queue<QueryResult> underlyingBuffer;

    private TempTable() {
        this(new LinkedList<>());
    }

    private TempTable(@Nonnull Queue<QueryResult> buffer) {
        this.underlyingBuffer = buffer;
    }

    /**
     * Add a new {@link QueryResult} element to the queue.
     * @param element the new element to be added.
     */
    public void add(@Nonnull QueryResult element) {
        underlyingBuffer.add(element);
    }

    /**
     * Add a new {@link QueryResult} elements to the queue.
     * @param elements the new elements to be added.
     */
    public void add(@Nonnull QueryResult... elements) {
        Arrays.stream(elements).forEach(this::add);
    }


    @Nonnull
    public Queue<QueryResult> getReadBuffer() {
        return underlyingBuffer;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public RecordCursor<QueryResult> getReadCursor(@Nullable byte[] continuation) {
        return new ListCursor<>((List<QueryResult>)getReadBuffer(), continuation);
    }

    private void serializeBuffer(@Nonnull RecordMetaDataProto.PTempTable.Builder protoMessageBuilder) {
        for (final var element : underlyingBuffer) {
            final var elementByteString = element.toByteString();
            protoMessageBuilder.addBufferItems(elementByteString);
        }
    }

    @Nonnull
    public RecordMetaDataProto.PTempTable toProto() {
        final var builder = RecordMetaDataProto.PTempTable.newBuilder();
        serializeBuffer(builder);
        return builder.build();
    }

    @Nonnull
    public ByteString toByteString() {
        return toProto().toByteString();
    }

    @Nonnull
    public byte[] toBytes() {
        return toByteString().toByteArray();
    }

    @Nonnull
    public static TempTable deserialize(@Nullable Descriptors.Descriptor descriptor, @Nonnull byte[]bytes) {
        return deserialize(descriptor, ZeroCopyByteString.wrap(bytes));
    }

    @Nonnull
    public static TempTable deserialize(@Nullable Descriptors.Descriptor descriptor, @Nonnull ByteString byteString) {
        final RecordMetaDataProto.PTempTable tempTableProto;
        try {
            tempTableProto = RecordMetaDataProto.PTempTable.parseFrom(byteString);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid bytes", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(byteString.toByteArray()));
        }
        return fromProto(tempTableProto, descriptor);
    }

    @Nonnull
    public static TempTable fromProto(@Nonnull final RecordMetaDataProto.PTempTable tempTableProto,
                                      @Nullable Descriptors.Descriptor descriptor) {
        final var underlyingBuffer = new LinkedList<QueryResult>();
        for (final var element : tempTableProto.getBufferItemsList()) {
            underlyingBuffer.add(QueryResult.deserialize(descriptor, element));
        }
        return new TempTable(underlyingBuffer);
    }

    @Nonnull
    public static TempTable newInstance() {
        return new TempTable();
    }
}
