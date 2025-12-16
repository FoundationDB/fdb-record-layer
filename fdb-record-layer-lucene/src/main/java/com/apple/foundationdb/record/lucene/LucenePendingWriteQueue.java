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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
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
     * Replay all queued operations for a partition into the index.
     * This should be called before executing a query to ensure queued writes are visible.
     *
     * @param indexWriter the index writer to write to
     *
     * @return CompletableFuture that completes when all operations have been replayed
     */
    @Nonnull
    public CompletableFuture<Void> replayQueuedOperations(final IndexWriter indexWriter) {

        return getQueuedOperations()
                .thenAccept(entries -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Replaying {} queued operations",
                                entries.size());
                    }

                    // Process all operations
                    for (QueuedOperationEntry entry : entries) {
                        replayOperation(entry, indexWriter);
                    }
                });
    }

    /**
     * Replay a single queued operation directly to the IndexWriter.
     */
    private void replayOperation(@Nonnull QueuedOperationEntry entry, @Nonnull IndexWriter indexWriter) {
        Tuple primaryKey = entry.getPrimaryKey();
        LucenePendingWriteQueueProto.QueuedOperation.OperationType opType = entry.getOperationType();

        try {
            switch (opType) {
                case UPDATE:
                    // TODO: for update we should try to delete the doc first
                case INSERT:
                    // Get IndexWriter and write document directly
                    Document document = new Document();

                    // Add primary key fields (simplified - using packed bytes)
                    // todo: this should be reused from the index maintainer
                    BytesRef ref = new BytesRef(primaryKey.pack());
                    document.add(new StoredField(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME, ref));
                    document.add(new SortedDocValuesField(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, ref));

                    // Add document fields
                    List<LuceneDocumentFromRecord.DocumentField> fields = convertFromProtoFields(entry.getFields());
                    for (LuceneDocumentFromRecord.DocumentField field : fields) {
                        addFieldToDocument(field, document);
                    }

                    indexWriter.addDocument(document);
                    break;

                case DELETE:
                    // Use SortedDocValuesField query for deletion
                    // TODO: can we use the segment primary key index to delete the doc directly?
                    org.apache.lucene.search.Query deleteQuery = org.apache.lucene.document.SortedDocValuesField.newSlowExactQuery(
                            LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME,
                            new BytesRef(primaryKey.pack()));
                    indexWriter.deleteDocuments(deleteQuery);
                    break;

                default:
                    LOGGER.warn("Unknown operation type: {}", opType);
            }
        } catch (IOException ex) {
            throw LuceneExceptions.toRecordCoreException("failed to replay message on writer", ex);
        }
    }

    /**
     * Add a field to a Lucene document.
     * Simplified version of LuceneIndexMaintainer.insertField()
     */
    private void addFieldToDocument(LuceneDocumentFromRecord.DocumentField field, org.apache.lucene.document.Document document) {
        final String fieldName = field.getFieldName();
        final Object value = field.getValue();

        switch (field.getType()) {
            case TEXT:
                // For TEXT fields, use simple tokenized field (simplified - doesn't handle all field configs)
                document.add(new org.apache.lucene.document.TextField(fieldName, (String)value, org.apache.lucene.document.Field.Store.NO));
                break;
            case STRING:
                document.add(new org.apache.lucene.document.StringField(fieldName, (String)value, org.apache.lucene.document.Field.Store.NO));
                if (field.isSorted()) {
                    document.add(new org.apache.lucene.document.SortedDocValuesField(fieldName, new BytesRef((String)value)));
                }
                break;
            case INT:
                document.add(new org.apache.lucene.document.IntPoint(fieldName, (Integer)value));
                if (field.isSorted()) {
                    document.add(new org.apache.lucene.document.NumericDocValuesField(fieldName, (Integer)value));
                }
                if (field.isStored()) {
                    document.add(new org.apache.lucene.document.StoredField(fieldName, (Integer)value));
                }
                break;
            case LONG:
                document.add(new org.apache.lucene.document.LongPoint(fieldName, (Long)value));
                if (field.isSorted()) {
                    document.add(new org.apache.lucene.document.NumericDocValuesField(fieldName, (Long)value));
                }
                if (field.isStored()) {
                    document.add(new org.apache.lucene.document.StoredField(fieldName, (Long)value));
                }
                break;
            case DOUBLE:
                document.add(new org.apache.lucene.document.DoublePoint(fieldName, (Double)value));
                if (field.isSorted()) {
                    document.add(new org.apache.lucene.document.NumericDocValuesField(fieldName, org.apache.lucene.util.NumericUtils.doubleToSortableLong((Double)value)));
                }
                if (field.isStored()) {
                    document.add(new org.apache.lucene.document.StoredField(fieldName, (Double)value));
                }
                break;
            case BOOLEAN:
                byte[] bytes = Boolean.TRUE.equals(value) ? new byte[] {1} : new byte[] {0};  // Simplified
                document.add(new org.apache.lucene.document.BinaryPoint(fieldName, bytes));
                if (field.isSorted()) {
                    document.add(new org.apache.lucene.document.SortedDocValuesField(fieldName, new BytesRef(bytes)));
                }
                if (field.isStored()) {
                    document.add(new org.apache.lucene.document.StoredField(fieldName, bytes));
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + field.getType());
        }
    }

    /**
     * Convert protobuf DocumentField list back to LuceneDocumentFromRecord.DocumentField list.
     */
    private List<LuceneDocumentFromRecord.DocumentField> convertFromProtoFields(
            @Nonnull List<LucenePendingWriteQueueProto.DocumentField> protoFields) {

        List<LuceneDocumentFromRecord.DocumentField> fields = new ArrayList<>();
        for (LucenePendingWriteQueueProto.DocumentField protoField : protoFields) {
            String fieldName = protoField.getFieldName();
            Object value;
            LuceneIndexExpressions.DocumentFieldType fieldType;

            // Determine the value and type based on which field is set
            if (protoField.hasStringValue()) {
                value = protoField.getStringValue();
                // Default to TEXT for string values - the actual type will be determined by index definition
                fieldType = LuceneIndexExpressions.DocumentFieldType.STRING;
            } else if (protoField.hasTextValue()) {
                value = protoField.getTextValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.TEXT;
            } else if (protoField.hasIntValue()) {
                value = protoField.getIntValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.INT;
            } else if (protoField.hasLongValue()) {
                value = protoField.getLongValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.LONG;
            } else if (protoField.hasDoubleValue()) {
                value = protoField.getDoubleValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.DOUBLE;
            } else if (protoField.hasBooleanValue()) {
                value = protoField.getBooleanValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.BOOLEAN;
            } else {
                throw new IllegalStateException("DocumentField has no value set: " + fieldName);
            }

            // Create DocumentField - using basic constructor, will need to add field configuration if needed
            LuceneDocumentFromRecord.DocumentField field =
                    new LuceneDocumentFromRecord.DocumentField(fieldName, value, fieldType, false, false, null);
            fields.add(field);
        }

        return fields;
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
                builder.setTextValue(value.toString());
                break;
            case STRING:
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
