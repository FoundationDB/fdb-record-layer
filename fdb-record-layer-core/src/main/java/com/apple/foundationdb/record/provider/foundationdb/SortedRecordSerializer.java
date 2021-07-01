/*
 * SortedRecordSerializer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordSortingProto;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Serialize records during sorting, either in a continuation or in a file.
 * @param <M> type used to represent stored records
 */
@API(API.Status.EXPERIMENTAL)
public class SortedRecordSerializer<M extends Message> {
    @Nonnull
    private final RecordSerializer<M> serializer;
    @Nonnull
    private final RecordMetaData recordMetaData;
    @Nullable
    private final StoreTimer timer;
    
    public SortedRecordSerializer(@Nonnull final RecordSerializer<M> serializer, @Nonnull final RecordMetaData recordMetaData, @Nullable final StoreTimer timer) {
        this.serializer = serializer;
        this.recordMetaData = recordMetaData;
        this.timer = timer;
    }

    static class Sorted<M extends Message> extends FDBQueriedRecord.Stored<M> {
        Sorted(@Nonnull FDBStoredRecord<M> stored) {
            super(stored);
        }
    }

    public void write(@Nonnull FDBRecord<M> record, CodedOutputStream stream) throws IOException {
        stream.writeMessageNoTag(toProto(record));
    }

    @Nonnull
    public byte[] serialize(@Nonnull FDBRecord<M> record) {
        return toProto(record).toByteArray();
    }

    @Nonnull
    public RecordSortingProto.SortedRecord toProto(@Nonnull FDBRecord<M> record) {
        final RecordSortingProto.SortedRecord.Builder builder = RecordSortingProto.SortedRecord.newBuilder();
        builder.setPrimaryKey(ByteString.copyFrom(record.getPrimaryKey().pack()));
        builder.setMessage(ByteString.copyFrom(serializer.serialize(recordMetaData, record.getRecordType(), record.getRecord(), timer)));
        if (record.hasVersion()) {
            builder.setVersion(ByteString.copyFrom(record.getVersion().toBytes()));
        }
        return builder.build();
    }

    @Nonnull
    public FDBQueriedRecord<M> read(@Nonnull CodedInputStream stream) throws IOException {
        final RecordSortingProto.SortedRecord.Builder builder =  RecordSortingProto.SortedRecord.newBuilder();
        stream.readMessage(builder, ExtensionRegistryLite.getEmptyRegistry());
        return deserialize(builder.build());
    }

    @Nonnull
    public FDBQueriedRecord<M> deserialize(@Nonnull byte[] serialized) {
        final RecordSortingProto.SortedRecord sortedRecord;
        try {
            sortedRecord = RecordSortingProto.SortedRecord.parseFrom(serialized);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException(ex);
        }
        return deserialize(sortedRecord);
    }

    @Nonnull
    public FDBQueriedRecord<M> deserialize(@Nonnull RecordSortingProto.SortedRecord sortedRecord) {
        final SplitHelper.SizeInfo size = new SplitHelper.SizeInfo();
        final byte[] primaryKeyBytes = sortedRecord.getPrimaryKey().toByteArray();
        final Tuple primaryKey = Tuple.fromBytes(primaryKeyBytes);
        final byte[] recordBytes = sortedRecord.getMessage().toByteArray();
        final M record = serializer.deserialize(recordMetaData, primaryKey, recordBytes, timer);
        final RecordType recordType = recordMetaData.getRecordTypeForDescriptor(record.getDescriptorForType());
        size.set(primaryKeyBytes, recordBytes);
        final FDBRecordVersion version;
        if (sortedRecord.hasVersion()) {
            version = FDBRecordVersion.fromBytes(sortedRecord.getVersion().toByteArray());
        } else {
            version = null;
        }
        FDBStoredRecord<M> storedRecord = new FDBStoredRecord<>(primaryKey, recordType, record, size, version);
        return new Sorted<>(storedRecord);
    }

    public void writeSortKeyAndRecord(@Nonnull Tuple sortKey, @Nonnull FDBRecord<M> record, @Nonnull CodedOutputStream stream) throws IOException {
        stream.writeByteArrayNoTag(sortKey.pack());
        write(record, stream);
    }

    @Nonnull
    public Map.Entry<Tuple, FDBQueriedRecord<M>> readSortKeyAndRecord(@Nonnull CodedInputStream stream) throws IOException {
        return new AbstractMap.SimpleEntry<>(Tuple.fromBytes(stream.readByteArray()), read(stream));
    }

    @Nonnull
    public FDBQueriedRecord<M> skipSortKeyAndReadRecord(@Nonnull CodedInputStream stream) throws IOException {
        stream.skipRawBytes(stream.readRawVarint32());
        return read(stream);
    }

}
