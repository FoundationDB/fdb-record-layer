/*
 * LucenePendingWriteQueue.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Manages a persistent queue of pending Lucene write operations in FDB.
 * When any partition is locked during a merge, write operations (insert, update, delete)
 * are queued and later drained after the merge completes.
 *
 * <p>Each instance is scoped to a specific partition (groupingKey + partitionId).</p>
 *
 * <p>Queue structure in FDB (follows FDBDirectory subspace convention):</p>
 * <pre>
 * &lt;queueSubspace&gt; / &lt;versionstamp&gt;
 * </pre>
 * <p>The queueSubspace parameter should already include the grouping key and partition ID:</p>
 * <pre>
 * &lt;indexSubspace&gt; / &lt;groupingKey&gt; / &lt;partitionId&gt; / PARTITION_DATA_SUBSPACE(1) / PENDING_WRITE_QUEUE_SUBSPACE(8)
 * </pre>
 * <p>Uses FDB versionstamps for automatic ordering without explicit sequence counters.</p>
 */
@API(API.Status.EXPERIMENTAL)
public class LucenePendingWriteQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(LucenePendingWriteQueue.class);

    private final FDBRecordContext context;
    private final Subspace queueSubspace;

    /**
     * Create a pending write queue for a specific partition.
     *
     * @param context the FDB record context
     * @param queueSubspace the subspace for this partition's queue, should be:
     *                      &lt;indexSubspace&gt; / &lt;groupingKey&gt; / &lt;partitionId&gt; / PARTITION_DATA_SUBSPACE(1) / PENDING_WRITE_QUEUE_SUBSPACE(8)
     */
    public LucenePendingWriteQueue(@Nonnull FDBRecordContext context, @Nonnull Subspace queueSubspace) {
        this.context = context;
        this.queueSubspace = queueSubspace;
    }

    /**
     * Enqueue an INSERT operation.
     *
     * @param primaryKey the record's primary key
     * @param fields the document fields to index
     *
     * @return CompletableFuture that completes when operation is enqueued (versionstamp assigned by FDB)
     */
    @Nonnull
    public CompletableFuture<Void> enqueueInsert(
            @Nonnull Tuple primaryKey,
            @Nonnull List<LuceneDocumentFromRecord.DocumentField> fields) {

        return enqueueOperation(
                LucenePendingWriteQueueProto.QueuedOperation.OperationType.INSERT,
                primaryKey,
                fields
        );
    }

    /**
     * Enqueue an UPDATE operation.
     *
     * @param primaryKey the record's primary key
     * @param fields the new document fields to index
     *
     * @return CompletableFuture that completes when operation is enqueued (versionstamp assigned by FDB)
     */
    @Nonnull
    public CompletableFuture<Void> enqueueUpdate(
            @Nonnull Tuple primaryKey,
            @Nonnull List<LuceneDocumentFromRecord.DocumentField> fields) {

        return enqueueOperation(
                LucenePendingWriteQueueProto.QueuedOperation.OperationType.UPDATE,
                primaryKey,
                fields
        );
    }

    /**
     * Enqueue a DELETE operation.
     *
     * @param primaryKey the record's primary key to delete
     *
     * @return CompletableFuture that completes when operation is enqueued (versionstamp assigned by FDB)
     */
    @Nonnull
    public CompletableFuture<Void> enqueueDelete(@Nonnull Tuple primaryKey) {

        return enqueueOperation(
                LucenePendingWriteQueueProto.QueuedOperation.OperationType.DELETE,
                primaryKey,
                null
        );
    }

    /**
     * Core method to enqueue any operation type using versionstamped keys.
     */
    private CompletableFuture<Void> enqueueOperation(
            @Nonnull LucenePendingWriteQueueProto.QueuedOperation.OperationType operationType,
            @Nonnull Tuple primaryKey,
            @Nullable List<LuceneDocumentFromRecord.DocumentField> fields) {

        // Build queue entry protobuf
        LucenePendingWriteQueueProto.QueuedOperation.Builder builder =
                LucenePendingWriteQueueProto.QueuedOperation.newBuilder()
                        .setOperationType(operationType)
                        .setPrimaryKey(com.google.protobuf.ByteString.copyFrom(primaryKey.pack()))
                        .setEnqueueTimestamp(System.currentTimeMillis());

        // Add fields for INSERT and UPDATE operations
        if (fields != null && !fields.isEmpty()) {
            for (LuceneDocumentFromRecord.DocumentField field : fields) {
                builder.addFields(convertToProtoField(field));
            }
        }

        // Build key with incomplete versionstamp using Tuple API
        Tuple keyTuple = Tuple.from(Versionstamp.incomplete());
        byte[] queueKey = queueSubspace.packWithVersionstamp(keyTuple);
        byte[] value = builder.build().toByteArray();

        // Use addVersionMutation to let FDB assign the versionstamp
        context.addVersionMutation(
                com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY,
                queueKey,
                value);

        // Record metrics
        context.increment(LuceneEvents.Counts.LUCENE_BUFFER_QUEUE_WRITE);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Enqueued {} operation (versionstamp pending) in {} subspace",
                    operationType, Tuple.fromBytes(queueSubspace.pack()));
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Get all queued operations for this partition.
     *
     * @return CompletableFuture with list of queued operations
     */
    @Nonnull
    public CompletableFuture<List<QueuedOperationEntry>> getQueuedOperations() {

        Range range = queueSubspace.range();

        return context.ensureActive()
                .getRange(range)
                .asList()
                .thenApply(keyValues -> {
                    List<QueuedOperationEntry> entries = new ArrayList<>();
                    for (KeyValue kv : keyValues) {
                        try {
                            LucenePendingWriteQueueProto.QueuedOperation operation = LucenePendingWriteQueueProto.QueuedOperation.parseFrom(kv.getValue());
                            Versionstamp versionstamp = extractVersionstamp(kv.getKey());

                            entries.add(new QueuedOperationEntry(
                                    kv.getKey(),
                                    versionstamp,
                                    operation
                            ));
                        } catch (InvalidProtocolBufferException e) {
                            // todo: abort
                            LOGGER.warn("Corrupted queue entry at key {}, skipping",
                                    Tuple.fromBytes(kv.getKey()), e);
                        }
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Found {} entries in write pending queue for subspace {}", entries.size(), Tuple.fromBytes(queueSubspace.pack()));
                    }
                    return entries;
                });
    }

    /**
     * Delete a specific queued operation.
     *
     * @param queueKey the FDB key of the queue entry
     */
    public void deleteQueuedOperation(@Nonnull byte[] queueKey) {
        context.ensureActive().clear(queueKey);
    }

    /**
     * Clear all queued operations for this partition's queue.
     *
     * @return CompletableFuture that completes when the queue is cleared
     */
    @Nonnull
    public CompletableFuture<Void> clearQueue() {
        Range range = queueSubspace.range();
        context.ensureActive().clear(range);

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Get the count of queued operations for this partition.
     *
     * @return CompletableFuture with the count of queued operations
     */
    @Nonnull
    public CompletableFuture<Integer> getQueueSize() {
        return getQueuedOperations()
                .thenApply(List::size);
    }

    /**
     * Extract versionstamp from queue entry key.
     */
    private Versionstamp extractVersionstamp(byte[] queueKey) {
        // TODO: probably don't need
        // Unpack the key to get the Tuple, which should contain the versionstamp
        Tuple keyTuple = queueSubspace.unpack(queueKey);
        return keyTuple.getVersionstamp(0);
    }

    /**
     * Convert DocumentField to protobuf DocumentField.
     */
    private LucenePendingWriteQueueProto.DocumentField convertToProtoField(
            LuceneDocumentFromRecord.DocumentField field) {

        LucenePendingWriteQueueProto.DocumentField.Builder builder =
                LucenePendingWriteQueueProto.DocumentField.newBuilder()
                        .setFieldName(field.getFieldName());

        // Set the appropriate value type based on field type
        Object value = field.getValue();
        switch (field.getType()) {
            case TEXT:
            case STRING:
                // Both TEXT and STRING map to string_value (index definition controls tokenization)
                builder.setStringValue(value.toString());
                break;
            case INT:
                builder.setIntValue((Integer)value);
                break;
            case LONG:
                builder.setLongValue((Long)value);
                break;
            case DOUBLE:
                builder.setDoubleValue((Double)value);
                break;
            case BOOLEAN:
                builder.setBooleanValue((Boolean)value);
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + field.getType());
        }

        return builder.build();
    }

    /**
     * Wrapper class for a queued operation entry.
     */
    public static class QueuedOperationEntry {
        private final byte[] queueKey;
        private final Versionstamp versionstamp;
        private final LucenePendingWriteQueueProto.QueuedOperation operation;

        public QueuedOperationEntry(
                byte[] queueKey,
                Versionstamp versionstamp,
                LucenePendingWriteQueueProto.QueuedOperation operation) {
            this.queueKey = queueKey;
            this.versionstamp = versionstamp;
            this.operation = operation;
        }

        public byte[] getQueueKey() {
            return queueKey;
        }

        public Versionstamp getVersionstamp() {
            return versionstamp;
        }

        public LucenePendingWriteQueueProto.QueuedOperation getOperation() {
            return operation;
        }

        public Tuple getPrimaryKey() {
            return Tuple.fromBytes(operation.getPrimaryKey().toByteArray());
        }

        public LucenePendingWriteQueueProto.QueuedOperation.OperationType getOperationType() {
            return operation.getOperationType();
        }

        public long getEnqueueTimestamp() {
            return operation.getEnqueueTimestamp();
        }

        public List<LucenePendingWriteQueueProto.DocumentField> getFields() {
            return operation.getFieldsList();
        }
    }
}
