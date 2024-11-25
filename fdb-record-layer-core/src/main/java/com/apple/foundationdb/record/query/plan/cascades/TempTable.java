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

import com.apple.foundationdb.record.ProtoSerializable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PTempTable;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A mutable, temporary, serializable, and in-memory buffer of {@link QueryResult}s. It is aimed to be used as a temporary
 * placeholder for computation results produced by some physical operator (i.e. {@link com.apple.foundationdb.record.query.plan.plans.QueryPlan}),
 * but can be leveraged to represent, for example, SQL temporary tables as well.<br/>
 * The actual implementation leverages a synchronized list, however, similar to synchronized list, it does not synchronize
 * the returned {@link List} in {@link TempTable#getList()} method, leaving it to the user to decide whether they
 * want to lock the entire list of not.<br>
 * See {@link Collections#synchronizedList(List)} for more information. Moreover, it is unbounded leaving setting any upper
 * bound to the consumer.
 *
 */
public class TempTable implements ProtoSerializable {

    @Nonnull
    private final List<QueryResult> underlyingBuffer;

    @Nonnull
    private final PTempTable.Builder protoBuilder;

    @Nullable
    private Message cachedProto;

    private TempTable() {
        this(Collections.synchronizedList(new ArrayList<>()), PTempTable.newBuilder());
    }

    private TempTable(@Nonnull final List<QueryResult> buffer, @Nonnull final PTempTable.Builder protoBuilder) {
        this.underlyingBuffer = buffer;
        this.protoBuilder = protoBuilder;
        this.cachedProto = null;
    }

    /**
     * Add a new {@link QueryResult} element to the underlying buffer.
     * @param element the new element to be added.
     */
    public void add(@Nonnull QueryResult element) {
        underlyingBuffer.add(element);
        protoBuilder.addBufferItems(element.toProto().toByteString());
        cachedProto = null;
    }

    /**
     * Clears the underlying buffer.
     */
    public void clear() {
        underlyingBuffer.clear();
        protoBuilder.clearBufferItems();
        cachedProto = null;
    }

    /**
     * Returns a iterator of the underlying buffer, note that this iterator is not synchronized.
     * @return an iterator of the underlying buffer.
     */
    @Nonnull
    public Iterator<QueryResult> getIterator() {
        return underlyingBuffer.iterator();
    }

    /**
     * Returns the underlying buffer, note that this method is not synchronized.
     * @return The underlying buffer.
     */
    @Nonnull
    public List<QueryResult> getList() {
        return underlyingBuffer;
    }

    @Nonnull
    @Override
    public Message toProto() {
        if (cachedProto == null) {
            cachedProto = protoBuilder.build();
        }
        return cachedProto;
    }

    /**
     * Deserializes a byte buffer to a corresponding {@link TempTable}.
     *
     * @param bytes The byte buffer.
     * @param descriptor An optional descriptor of the temporary table elements.
     *
     * @return A deserialized {@link TempTable}.
     */
    @Nonnull
    public static TempTable from(@Nonnull final byte[] bytes, @Nullable final Descriptors.Descriptor descriptor) {
        return from(ZeroCopyByteString.wrap(bytes), descriptor);
    }

    /**
     * Deserializes a {@link PTempTable} message to a corresponding {@link TempTable}.
     *
     * @param byteString The byte string.
     * @param descriptor An optional descriptor of the temporary table elements.
     *
     * @return A deserialized {@link TempTable}.
     */
    @Nonnull
    public static TempTable from(@Nonnull final ByteString byteString, @Nullable final Descriptors.Descriptor descriptor) {
        final PTempTable tempTableProto;
        try {
            tempTableProto = PTempTable.parseFrom(byteString);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid bytes", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(byteString.toByteArray()));
        }
        return from(tempTableProto, descriptor);
    }

    /**
     * Deserializes a {@link PTempTable} message to a corresponding {@link TempTable}.
     * @param tempTableProto The serialized temporary table message.
     * @param descriptor An optional descriptor of the temporary table elements.
     * @return A deserialized {@link TempTable}.
     */
    @Nonnull
    public static TempTable from(@Nonnull final PTempTable tempTableProto,
                                 @Nullable final Descriptors.Descriptor descriptor) {
        final var underlyingBuffer = new LinkedList<QueryResult>();
        for (final var element : tempTableProto.getBufferItemsList()) {
            underlyingBuffer.add(QueryResult.from(descriptor, element));
        }
        return new TempTable(underlyingBuffer, tempTableProto.toBuilder());
    }

    /**
     * Creates a new instance of {@link TempTable} backed by a synchronized list.
     * @return a new instance of {@link TempTable}.
     */
    @Nonnull
    public static TempTable newInstance() {
        return new TempTable();
    }
}
