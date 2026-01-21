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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreInternalException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainerHelper;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
 * <p>The key of each K/V queue entry will be a versionStamp to ensure uniqueness and ordering of the queue entries, which
 * should allow for conflict free insertion and removal of items. The value of the K/V will be an instance of PendingWriteItem proto.</p>
 */
@API(API.Status.INTERNAL)
public class PendingWriteQueue {
    public static final int MAX_PENDING_ENTRIES_TO_REPLAY = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(PendingWriteQueue.class);

    /**
     * The maximum number of queue entries to replay into an {@link IndexWriter}.
     * In case the queue has more than that number of entries, then a replay attempt will fail. This is meant as a
     * protection
     * against a query attempt taking too long to replay a huge number of elements.
     * Default: 0 (unlimited)
     */
    private int maxEntriesToReplay;

    private final Subspace queueSubspace;

    /**
     * Create a pending write queue.
     *
     * @param queueSubspace the subspace for this partition's queue, should include the partition ID and grouping key,
     * as necessary
     */
    public PendingWriteQueue(@Nonnull Subspace queueSubspace) {
        this(queueSubspace, MAX_PENDING_ENTRIES_TO_REPLAY);
    }

    public PendingWriteQueue(@Nonnull Subspace queueSubspace, int maxEntriesToReplay) {
        this.queueSubspace = queueSubspace;
        this.maxEntriesToReplay = maxEntriesToReplay;
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
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<QueueEntry> getQueueCursor(
            @Nonnull FDBRecordContext context,
            @Nonnull ScanProperties scanProperties,
            @Nullable byte[] continuation) {

        final KeyValueCursor cursor = KeyValueCursor.Builder.newBuilder(queueSubspace)
                .setContext(context)
                .setScanProperties(scanProperties)
                .setContinuation(continuation)
                .build();
        return cursor.map(kv -> PendingWritesQueueHelper.toQueueEntry(queueSubspace, kv));
    }

    /**
     * Remove an entry from the queue.
     * The {@link QueueEntry} given to this method (likely returned from the {@link #getQueueCursor(FDBRecordContext, ScanProperties, byte[])}
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

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Cleared queue entry");
        }
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
    public CompletableFuture<Void> replayQueuedOperations(FDBRecordContext context, IndexWriter indexWriter) {
        ScanProperties scanProperties = ScanProperties.FORWARD_SCAN;
        if (maxEntriesToReplay > 0) {
            // create a limit for the cursor
            scanProperties = ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(maxEntriesToReplay)
                    .build()
                    .asScanProperties(false);
        }

        // Create a cursor over the allowed number of items, with no continuation.
        // There is no need to replay with a continuation as all the replayed items need to make it into the
        // current writer in the given transaction to be queried
        return getQueueCursor(context, scanProperties, null).forEachResult(entry -> {
            replayOperation(entry.get(), indexWriter);
        }).thenAccept(lastResult -> {
            if (lastResult.getNoNextReason().equals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED)) {
                // Reached the row limit
                LOGGER.error(KeyValueLogMessage.of("Too many entries to be replayed into IndexWriter",
                        LuceneLogMessageKeys.MAX_ENTRIES_TO_REPLAY, maxEntriesToReplay));
                throw new TooManyPendingWritesException("Too many entries to replay into IndexWriter");

                // TODO: Translate exception to Lucene?
            }
        });
    }

    /**
     * Replay a single queued operation directly to the IndexWriter.
     */
    @SuppressWarnings("fallthrough")
    private void replayOperation(@Nonnull QueueEntry entry, @Nonnull IndexWriter indexWriter) {
        Tuple primaryKey = entry.getPrimaryKey();
        LucenePendingWriteQueueProto.PendingWriteItem.OperationType opType = entry.getOperationType();

        try {
            switch (opType) {
                case UPDATE:
                    // TODO: can we use the segment primary key index to delete the doc directly?
                    Query updateDeleteQuery = SortedDocValuesField.newSlowExactQuery(
                            LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME,
                            new BytesRef(primaryKey.pack()));
                    indexWriter.deleteDocuments(updateDeleteQuery);
                    // Fallthrough to the next CASE

                case INSERT:
                    // Get IndexWriter and write document directly
                    Document document = new Document();

                    // Add primary key fields (simplified - using packed bytes)
                    // todo: this should be reused from the index maintainer
                    BytesRef ref = new BytesRef(primaryKey.pack());
                    document.add(new StoredField(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME, ref));
                    document.add(new SortedDocValuesField(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, ref));

                    // Add document fields
                    List<LuceneDocumentFromRecord.DocumentField> fields = PendingWritesQueueHelper.fromProtoFields(entry.getDocumentFields());
                    for (LuceneDocumentFromRecord.DocumentField field : fields) {
                        LuceneIndexMaintainerHelper.insertField(field, document);
                    }

                    indexWriter.addDocument(document);
                    break;

                case DELETE:
                    // Use SortedDocValuesField query for deletion
                    // TODO: can we use the segment primary key index to delete the doc directly?
                    Query deleteQuery = SortedDocValuesField.newSlowExactQuery(
                            LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME,
                            new BytesRef(primaryKey.pack()));
                    indexWriter.deleteDocuments(deleteQuery);
                    break;

                default:
                    LOGGER.warn("Unknown operation type: {}", opType);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Replayed {} operation", opType);
            }
        } catch (IOException ex) {
            throw LuceneExceptions.toRecordCoreException("failed to replay message on writer", ex);
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
                builder.addFields(PendingWritesQueueHelper.toProtoField(field));
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

        public Tuple getPrimaryKey() {
            return Tuple.fromBytes(item.getPrimaryKey().toByteArray());
        }

        public long getEnqueuedTimeStamp() {
            return item.getEnqueueTimestamp();
        }

        public List<LucenePendingWriteQueueProto.DocumentField> getDocumentFields() {
            return item.getFieldsList();
        }
    }

    @SuppressWarnings("serial")
    public static class TooManyPendingWritesException extends RecordCoreException {
        protected TooManyPendingWritesException(final String message) {
            super(message);
        }
    }
}
