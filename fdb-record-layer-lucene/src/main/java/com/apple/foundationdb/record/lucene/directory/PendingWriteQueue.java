/*
 * PendingWriteQueue.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreInternalException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Manages a persistent queue of pending Lucene write operations in FDB.
 * When any partition is locked during a merge, write operations (insert, update, delete)
 * are queued and later moved into the index after the merge completes.
 * <p>
 * There are three flows that the queue supports:
 * <ul>
 *     <li>Enqueue - Add an item to the queue when a document change request arrives and the index is locked</li>
 *     <li>Replay - Ephemeral write of the queue items to an IndexWriter so the searcher can use them in a query</li>
 *     <li>Drain - Move items from the queue to the index once the merge lock is no longer taken</li>
 * </ul>
 *
 * <p>Each queue is scoped to a specific partition (groupingKey + partitionId).</p>
 * <p>The queueSubspace parameter should already include the grouping key and partition ID. The structure of the subspace
 * most generically looks like:</p>
 * <pre>
 * &lt;indexSubspace&gt; / &lt;groupingKey&gt; / &lt;partitionId&gt; / PARTITION_DATA_SUBSPACE(1) / PENDING_WRITE_QUEUE_SUBSPACE(8)
 * </pre>
 * <p>Although, non-grouped or non-partitioned indexes would not include all of these components.</p>
 * <p>The key of each K/V queue entry will be a versionStamp, to ensure uniqueness and ordering of the queue entries, which
 * should allow for conflict free insertion and removal of items. The value of the K/V will be an instance of PendingWriteItem proto.</p>
 */
public class PendingWriteQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(PendingWriteQueue.class);

    private final Subspace queueSubspace;

    /**
     * Create a pending write queue.
     *
     * @param queueSubspace the subspace for this partition's queue, should include the partition ID and grouping key,
     * as necessary
     */
    public PendingWriteQueue(@Nonnull Subspace queueSubspace) {
        this.queueSubspace = queueSubspace;
    }

    /**
     * Enqueue an INSERT operation.
     *
     * @param context the record context to use for the operation
     * @param primaryKey the record's primary key
     * @param fields the document fields to index
     */
    public void enqueueInsert(
            @Nonnull FDBRecordContext context,
            @Nonnull Tuple primaryKey,
            @Nonnull List<LuceneDocumentFromRecord.DocumentField> fields) {

        enqueueOperationInternal(
                context,
                LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                primaryKey,
                fields);
    }

    /**
     * Enqueue an UPDATE operation.
     *
     * @param context the record context to use for the operation
     * @param primaryKey the record's primary key
     * @param fields the new document fields to index
     */
    public void enqueueUpdate(
            @Nonnull FDBRecordContext context,
            @Nonnull Tuple primaryKey,
            @Nonnull List<LuceneDocumentFromRecord.DocumentField> fields) {

        enqueueOperationInternal(
                context,
                LucenePendingWriteQueueProto.PendingWriteItem.OperationType.UPDATE,
                primaryKey,
                fields);
    }

    /**
     * Enqueue a DELETE operation.
     *
     * @param context the record context to use for the operation
     * @param primaryKey the record's primary key to delete
     */
    public void enqueueDelete(
            @Nonnull FDBRecordContext context,
            @Nonnull Tuple primaryKey) {

        enqueueOperationInternal(
                context,
                LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                primaryKey,
                null);
    }

    /**
     * Return a cursor that iterates through the queue elements.
     *
     * @param context the record context to use
     * @param scanProperties the scan properties for the cursor
     * @param continuation the continuation to continue from (null means start from the beginning)
     *
     * @return a record cursor that iterates through the elements of the queue, in order
     */
    public RecordCursor<QueueEntry> iterateQueue(
            @Nonnull FDBRecordContext context,
            @Nonnull ScanProperties scanProperties,
            @Nullable byte[] continuation) {

        final KeyValueCursor cursor = KeyValueCursor.Builder.newBuilder(queueSubspace)
                .setContext(context)
                .setScanProperties(scanProperties)
                .setContinuation(continuation)
                .build();
        return cursor.map(this::toQueueEntry);
    }

    /**
     * Remove an entry from the queue.
     * The {@link QueueEntry} given to this method (likely returned from the {@link #iterateQueue(FDBRecordContext, ScanProperties, byte[])}
     * cursor) will be removed from the queue. Note that the entry has to have a <pre>Complete</pre> versionStamp
     * (that means, it has to have been committed to the DB) before it can be removed.
     *
     * @param context the context to use
     * @param entry the entry to remove
     */
    public void clearEntry(@Nonnull FDBRecordContext context, @Nonnull QueueEntry entry) {
        // for sanity,
        if (!entry.getVersionstamp().isComplete()) {
            throw new RecordCoreArgumentException("Queue item should have complete version stamp");
        }

        context.ensureActive().clear(queueSubspace.pack(entry.versionstamp));

        // Record metrics
        context.increment(LuceneEvents.Counts.LUCENE_PENDING_QUEUE_CLEAR);
    }

    private QueueEntry toQueueEntry(final KeyValue kv) {
        try {
            Tuple keyTuple = queueSubspace.unpack(kv.getKey());
            final Versionstamp versionstamp = keyTuple.getVersionstamp(0);
            LucenePendingWriteQueueProto.PendingWriteItem item = LucenePendingWriteQueueProto.PendingWriteItem.parseFrom(kv.getValue());
            return new QueueEntry(versionstamp, item);
        } catch (InvalidProtocolBufferException e) {
            throw new RecordCoreInternalException("Failed to parse queue item", e);
        }
    }

    private void enqueueOperationInternal(
            @Nonnull FDBRecordContext context,
            @Nonnull LucenePendingWriteQueueProto.PendingWriteItem.OperationType operationType,
            @Nonnull Tuple primaryKey,
            @Nullable List<LuceneDocumentFromRecord.DocumentField> fields) {

        // Build queue entry protobuf
        LucenePendingWriteQueueProto.PendingWriteItem.Builder builder =
                LucenePendingWriteQueueProto.PendingWriteItem.newBuilder()
                        .setOperationType(operationType)
                        .setPrimaryKey(ByteString.copyFrom(primaryKey.pack()))
                        .setEnqueueTimestamp(System.currentTimeMillis());

        // Add fields for INSERT and UPDATE operations
        if (fields != null && !fields.isEmpty()) {
            for (LuceneDocumentFromRecord.DocumentField field : fields) {
                builder.addFields(toProtoField(field));
            }
        }

        // Build key with incomplete versionStamp with a new local version
        FDBRecordVersion recordVersion = FDBRecordVersion.incomplete(context.claimLocalVersion());
        Tuple keyTuple = Tuple.from(recordVersion.toVersionstamp());
        byte[] queueKey = queueSubspace.packWithVersionstamp(keyTuple);
        byte[] value = builder.build().toByteArray();

        // Use addVersionMutation to let FDB assign the versionStamp
        final byte[] current = context.addVersionMutation(
                MutationType.SET_VERSIONSTAMPED_KEY,
                queueKey,
                value);

        if (current != null) {
            // This should never happen
            throw new RecordCoreInternalException("Pending queue item overwritten");
        }

        // Record metrics
        context.increment(LuceneEvents.Counts.LUCENE_PENDING_QUEUE_WRITE);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Enqueued {} operation", operationType);
        }
    }

    /**
     * Convert DocumentField to protobuf DocumentField.
     */
    private LucenePendingWriteQueueProto.DocumentField toProtoField(@Nonnull LuceneDocumentFromRecord.DocumentField field) {

        LucenePendingWriteQueueProto.DocumentField.Builder builder =
                LucenePendingWriteQueueProto.DocumentField.newBuilder()
                        .setFieldName(field.getFieldName())
                        .setStored(field.isStored())
                        .setSorted(field.isSorted());

        // Add field_configs map
        for (Map.Entry<String, Object> entry : field.getFieldConfigs().entrySet()) {
            LucenePendingWriteQueueProto.FieldConfigValue.Builder configBuilder =
                    LucenePendingWriteQueueProto.FieldConfigValue.newBuilder();

            Object configValue = entry.getValue();
            if (configValue instanceof String) {
                configBuilder.setStringValue((String)configValue);
            } else if (configValue instanceof Boolean) {
                configBuilder.setBooleanValue((Boolean)configValue);
            } else if (configValue instanceof Integer) {
                configBuilder.setIntValue((Integer)configValue);
            } else {
                throw new RecordCoreArgumentException("Unsupported field config type: " + configValue.getClass().getSimpleName() + " for field " + entry.getKey());
            }

            builder.putFieldConfigs(entry.getKey(), configBuilder.build());
        }

        // Set the appropriate value type based on field type
        Object value = field.getValue();
        switch (field.getType()) {
            case TEXT:
                builder.setTextValue((String)value);
                break;
            case STRING:
                builder.setStringValue((String)value);
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
                throw new IllegalArgumentException("Unsupported field type: " + field.getType() + " for field " + field.getFieldName());
        }

        return builder.build();
    }

    /**
     * Wrapper class for a queued entry.
     * Gives access to the queued item and its VersionStamp ID.
     */
    public static class QueueEntry {
        private final Versionstamp versionstamp;
        private final LucenePendingWriteQueueProto.PendingWriteItem item;

        public QueueEntry(
                Versionstamp versionstamp,
                LucenePendingWriteQueueProto.PendingWriteItem item) {
            this.versionstamp = versionstamp;
            this.item = item;
        }

        public Versionstamp getVersionstamp() {
            return versionstamp;
        }

        public LucenePendingWriteQueueProto.PendingWriteItem.OperationType getOperationType() {
            return item.getOperationType();
        }

        public byte[] getPrimaryKey() {
            return item.getPrimaryKey().toByteArray();
        }

        public long getEnqueuedTimeStamp() {
            return item.getEnqueueTimestamp();
        }

        public List<LucenePendingWriteQueueProto.DocumentField> getDocumentFields() {
            return item.getFieldsList();
        }
    }
}
