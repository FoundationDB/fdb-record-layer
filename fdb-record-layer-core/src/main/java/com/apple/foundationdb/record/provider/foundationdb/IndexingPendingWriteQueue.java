/*
 * IndexingPendingWriteQueue.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.CompletableFuture;

/**
 * Factory for the {@link PendingWritesQueue} used by the online indexer to defer index updates while an index is
 * being built in the {@link com.apple.foundationdb.record.IndexState#WRITE_ONLY_WITH_QUEUE} state. This keeps the
 * indexing-specific queue wiring (subspaces, payload type, capacity) in one place so producers (the index maintainer)
 * and consumers (the drainer) always agree on it.
 */
@ParametersAreNonnullByDefault
public final class IndexingPendingWriteQueue {
    // TODO: configurable maxQueueSize
    private static final int MAX_QUEUE_SIZE = 100_000;

    private IndexingPendingWriteQueue() {
    }

    @Nonnull
    public static PendingWritesQueue<IndexBuildProto.PendingWritesQueueEntry> getIndexingQueue(final FDBRecordStore store, final Index index) {
        return new PendingWritesQueue<>(
                IndexingSubspaces.indexPendingWriteQueueSubspace(store, index),
                IndexingSubspaces.indexPendingWriteQueueSizeSubspace(store, index),
                MAX_QUEUE_SIZE,
                IndexBuildProto.PendingWritesQueueEntry.class
        );
    }

    /**
     * Serialize an old/new record pair into a {@link IndexBuildProto.PendingWritesQueueEntry} and enqueue it for later
     * draining. Either record may be null: a null {@code oldRecord} represents an insert, and a null {@code newRecord}
     * represents a delete. This keeps the queue's payload construction next to its other wiring, so index maintainers
     * only decide whether to defer an update, not how it is encoded.
     * @param store the record store whose serializer, incarnation, and context are used
     * @param index the index whose queue the entry is appended to
     * @param oldRecord the record prior to the update, or null for an insert
     * @param newRecord the record after the update, or null for a delete
     * @param <M> the message type of the records
     * @return a future that completes when the entry has been enqueued
     */
    @Nonnull
    public static <M extends Message> CompletableFuture<Void> enqueueOldAndNewRecords(
            final FDBRecordStore store,
            final Index index,
            @Nullable final FDBIndexableRecord<M> oldRecord,
            @Nullable final FDBIndexableRecord<M> newRecord) {
        if (oldRecord == null && newRecord == null) {
            // This should never happen with the current maintainers
            return AsyncUtil.DONE;
        }
        final RecordSerializer<Message> serializer = store.getSerializer();
        final IndexBuildProto.PendingWritesQueueEntry.Builder builder = IndexBuildProto.PendingWritesQueueEntry.newBuilder();
        if (oldRecord != null) {
            builder.setOldRecords(serializeRecord(store, oldRecord, serializer));
        }
        if (newRecord != null) {
            builder.setNewRecord(serializeRecord(store, newRecord, serializer));
        }
        return getIndexingQueue(store, index).enqueue(store.getContext(), builder.build(), store.getIncarnation());
    }

    @Nonnull
    private static <M extends Message> ByteString serializeRecord(final FDBRecordStore store,
                                                                  final FDBIndexableRecord<M> indexableRecord,
                                                                  final RecordSerializer<Message> serializer) {
        return ByteString.copyFrom(serializer.serialize(store.getRecordMetaData(),
                indexableRecord.getRecordType(), indexableRecord.getRecord(), store.getTimer()));
    }

    @Nullable
    public static FDBStoredRecord<Message> getOldRecord(final FDBRecordStore store, IndexBuildProto.PendingWritesQueueEntry payload) {
        return payload.hasOldRecords() ? deserializeRecord(store, payload.getOldRecords()) : null;
    }

    @Nullable
    public static FDBStoredRecord<Message> getNewRecord(final FDBRecordStore store, IndexBuildProto.PendingWritesQueueEntry payload) {
        return payload.hasNewRecord() ? deserializeRecord(store, payload.getNewRecord()) : null;
    }

    /**
     * Rebuild a stored record from its serialized payload. The queue holds only the record bytes, so the primary key is
     * recomputed from the record's metadata, mirroring {@link FDBRecordStore#saveTypedRecord}.
     */
    @Nonnull
    private static FDBStoredRecord<Message> deserializeRecord(final FDBRecordStore store, final ByteString serialized) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final RecordSerializer<Message> serializer = store.getSerializer();
        final Message rec = serializer.deserialize(metaData, TupleHelpers.EMPTY, serialized.toByteArray(), store.getTimer());
        final RecordType recordType = metaData.getRecordTypeForDescriptor(rec.getDescriptorForType());
        final FDBStoredRecordBuilder<Message> builder = FDBStoredRecord.newBuilder(rec).setRecordType(recordType);
        builder.setPrimaryKey(recordType.getPrimaryKey().evaluateSingleton(builder).toTuple());
        return builder.build();
    }

}
