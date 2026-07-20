/*
 * StandardIndexMaintainerWithQueue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecordBuilder;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Extend {@link StandardIndexMaintainer} with simple pending-write-queue support. This implementation does not support
 * synthetic records or non-idempotent indexes.
 *
 * <p>This is an abstract base. The serialization helpers are exposed (package-private, static) so that
 * other queue-capable maintainers that cannot extend this class directly can reuse the
 * exact same wire format.</p>
 */
@API(API.Status.EXPERIMENTAL)
public abstract class StandardIndexMaintainerWithQueue extends StandardIndexMaintainer {
    protected StandardIndexMaintainerWithQueue(@Nonnull final IndexMaintainerState state) {
        super(state);
    }

    @Override
    public boolean isPendingWriteQueueAllowed() {
        return isPendingWriteQueueAllowed(this, state);
    }

    static boolean isPendingWriteQueueAllowed(StandardIndexMaintainer maintainer, @Nonnull final IndexMaintainerState state) {
        return maintainer.isIdempotent() && !isSyntheticIndex(state);
    }

    @Override
    @Nonnull
    public <M extends Message> Any serializePendingWriteQueue(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                              @Nullable final FDBIndexableRecord<M> newRecord) {
        return serializePendingWrites(state, oldRecord, newRecord);
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> updateFromQueue(@Nonnull final Any data) {
        // Calling updateWhileWriteOnly explicitly, lest this update be re-pushed to the queue.
        final IndexBuildProto.OldAndNewRecords records = unpackPendingWrites(data);
        return updateWhileWriteOnly(
                records.hasOldRecords() ? deserializePendingRecord(state, records.getOldRecords()) : null,
                records.hasNewRecord() ? deserializePendingRecord(state, records.getNewRecord()) : null);
    }

    /**
     * Serialize an old/new record pair into the {@link Any}-packed payload deferred onto the pending write queue.
     * @param state the maintainer state whose store serializer is used
     * @param oldRecord the previous stored record, or {@code null} for an insert
     * @param newRecord the new record, or {@code null} for a delete
     * @param <M> type of message
     * @return the packed payload to enqueue
     */
    @Nonnull
    static <M extends Message> Any serializePendingWrites(@Nonnull final IndexMaintainerState state,
                                                          @Nullable final FDBIndexableRecord<M> oldRecord,
                                                          @Nullable final FDBIndexableRecord<M> newRecord) {
        final IndexBuildProto.OldAndNewRecords.Builder builder = IndexBuildProto.OldAndNewRecords.newBuilder();
        if (oldRecord != null) {
            builder.setOldRecords(serializePendingRecord(state, oldRecord));
        }
        if (newRecord != null) {
            builder.setNewRecord(serializePendingRecord(state, newRecord));
        }
        return Any.pack(builder.build());
    }

    /**
     * Unpack the {@link IndexBuildProto.OldAndNewRecords} payload from a queue entry's data.
     * @param data the {@link Any}-packed payload produced by {@link #serializePendingWrites}
     * @return the unpacked old/new records
     */
    @Nonnull
    static IndexBuildProto.OldAndNewRecords unpackPendingWrites(@Nonnull final Any data) {
        try {
            return data.unpack(IndexBuildProto.OldAndNewRecords.class);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("failed to parse pending write queue entry data", ex);
        }
    }

    @Nonnull
    static <M extends Message> ByteString serializePendingRecord(@Nonnull final IndexMaintainerState state,
                                                                 @Nonnull final FDBIndexableRecord<M> indexableRecord) {
        return ByteString.copyFrom(state.store.getSerializer().serialize(state.store.getRecordMetaData(),
                indexableRecord.getRecordType(), indexableRecord.getRecord(), state.store.getTimer()));
    }

    @Nonnull
    static FDBStoredRecord<Message> deserializePendingRecord(@Nonnull final IndexMaintainerState state,
                                                             @Nonnull final ByteString serialized) {
        final RecordMetaData metaData = state.store.getRecordMetaData();
        final Message rec = state.store.getSerializer()
                .deserialize(metaData, TupleHelpers.EMPTY, serialized.toByteArray(), state.store.getTimer());
        final RecordType recordType = metaData.getRecordTypeForDescriptor(rec.getDescriptorForType());
        final FDBStoredRecordBuilder<Message> builder = FDBStoredRecord.newBuilder(rec).setRecordType(recordType);
        builder.setPrimaryKey(recordType.getPrimaryKey().evaluateSingleton(builder).toTuple());
        return builder.build();
    }

    static boolean isSyntheticIndex(@Nonnull final IndexMaintainerState state) {
        return !state.store.getRecordMetaData().getSyntheticRecordTypes().isEmpty() &&
                state.store.getRecordMetaData().recordTypesForIndex(state.index).stream().anyMatch(RecordType::isSynthetic);
    }
}
