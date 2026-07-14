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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueueEntry;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.CursorFactory;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ThrottledRetryingIterator;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.util.CloseException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.Serial;
import java.util.concurrent.CompletableFuture;

/**
 * Use {@link PendingWritesQueue} to defer index updates while an index is being built.
 * Indexin maintainer: will use this module to push items to the queue
 * Online indexer: will use this module to drain the queue and update the index
 */
@ParametersAreNonnullByDefault
public final class IndexingPendingWriteQueue {
    // TODO: configurable maxQueueSize
    private static final int MAX_QUEUE_SIZE = 100_000;
    private static final int MAX_RECORDS_DELETE_PER_SECOND = 10_000;

    private final Index index;
    private final IndexingCommon common;

    public IndexingPendingWriteQueue(final Index index, final IndexingCommon common) {
        this.index = index;
        this.common = common;
    }

    CompletableFuture<Boolean> isQueueEmpty(FDBRecordStore store) {
        return getIndexingQueue(store, index).isQueueEmpty(store.getContext());
    }

    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Void> drainPendingQueue() {
        // Called by the indexer: update the index and then remove every queue item
        final FDBRecordContextConfig.Builder contextConfigBuilder =
                FDBRecordContextConfig.newBuilder().setTimer(common.getRunner().getTimer());
        final ThrottledRetryingIterator<PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry>> iterator =
                ThrottledRetryingIterator.builder(
                                common.getRunner().getDatabase(),
                                contextConfigBuilder,
                                cursorFactory(),
                                this::handleOneItem)
                        .withMaxRecordsDeletesPerSec(MAX_RECORDS_DELETE_PER_SECOND)
                        .build();
        return iterator.iterateAll(common.getRecordStoreBuilder().copyBuilder())
                .whenComplete((v, e) -> {
                    try {
                        iterator.close();
                    } catch (CloseException closeEx) {
                        throw new PendingWriteQueueDrainException(closeEx);
                    }
                });
    }

    @Nonnull
    private CursorFactory<PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry>> cursorFactory() {
        return (store, lastResult, rowLimit) -> {
            final byte[] continuation = lastResult == null ? null : lastResult.getContinuation().toBytes();
            final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(rowLimit));
            return getIndexingQueue(store, index).getQueueCursor(store.getContext(), scanProperties, continuation);
        };
    }

    @Nonnull
    private CompletableFuture<Void> handleOneItem(final FDBRecordStore store,
                                                  final RecordCursorResult<PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry>> lastResult,
                                                  final ThrottledRetryingIterator.QuotaManager quotaManager) {
        final PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry> entry = lastResult.get();
        if (entry == null) {
            return AsyncUtil.DONE;
        }
        final IndexBuildProto.PendingWritesQueueEntry payload = entry.getPayload();
        return store.getIndexMaintainer(index)
                // Calling updateWhileWriteOnly explicitly, lest this update will be re-pushed to the queue
                .updateWhileWriteOnly(
                        getOldRecord(store, payload),
                        getNewRecord(store, payload))
                .thenAccept(ignore -> {
                    quotaManager.deleteCountInc();
                    getIndexingQueue(store, index).clearEntry(store.getContext(), entry);
                });
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
     * Called by the index maintainer to enqueue data for a deferred index update.
     * @param store the record store whose incarnation and context are used
     * @param index the index whose queue the entry is appended to
     * @param entry the entry to enqueue
     * @return a future that completes when the entry has been enqueued
     */
    @Nonnull
    public static CompletableFuture<Void> enqueuePendingIndexUpdate(
            final FDBRecordStore store,
            final Index index,
            final IndexBuildProto.PendingWritesQueueEntry entry) {
        return getIndexingQueue(store, index).enqueue(store.getContext(), entry, store.getIncarnation());
    }

    @Nullable
    public static FDBStoredRecord<Message> getOldRecord(final FDBRecordStore store, IndexBuildProto.PendingWritesQueueEntry payload) {
        final IndexBuildProto.PendingWritesQueueEntry.OldAndNewRecords records = payload.getOldAndNewRecords();
        return records.hasOldRecords() ? deserializeRecord(store, records.getOldRecords()) : null;
    }

    @Nullable
    public static FDBStoredRecord<Message> getNewRecord(final FDBRecordStore store, IndexBuildProto.PendingWritesQueueEntry payload) {
        final IndexBuildProto.PendingWritesQueueEntry.OldAndNewRecords records = payload.getOldAndNewRecords();
        return records.hasNewRecord() ? deserializeRecord(store, records.getNewRecord()) : null;
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

    /**
     * thrown if pending queue drain had failed.
     */
    @SuppressWarnings("java:S110")
    public static class PendingWriteQueueDrainException extends RecordCoreException {
        @Serial
        private static final long serialVersionUID = 7;

        public PendingWriteQueueDrainException(final Throwable cause) {
            super("Pending write queue drain had failed", cause);
        }
    }
}
