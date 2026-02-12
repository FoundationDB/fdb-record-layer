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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneConcurrency;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainerHelper;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
 * should allow for conflict free insertion and removal of items. The value of the K/V will be an instance of
 * LucenePendingWriteQueueProto.PendingWriteItem.</p>
 * <p>The queue maintains a counter (in a separate subspace) for the number of entries it contains. The counter starts as
 * uninitialized and is atomically mutated upon enqueues and clear operations. The queue can (optionally) reject enqueues
 * if the number of entries is larger than (configurable) limit.</p>
 */
@API(API.Status.INTERNAL)
public class PendingWriteQueue {
    public static final int DEFAULT_MAX_PENDING_ENTRIES_TO_REPLAY = 0;
    public static final int DEFAULT_MAX_PENDING_QUEUE_SIZE = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(PendingWriteQueue.class);

    /**
     * The maximum number of queue entries to replay into an {@link IndexWriter}.
     * In case the queue has more than that number of entries, then a replay attempt will fail. This is meant as a
     * protection against a query attempt taking too long to replay a huge number of elements.
     * Default: 0 (unlimited)
     */
    private final int maxEntriesToReplay;
    /**
     * The maximum size of the Pending Writes queue.
     * In case more items than the maximum number allowed in the queue are attempted to get inserted, the operation
     * will fail.
     * This option is meant to limit the queue size such that in case of errors the queue won't grow boundlessly.
     * Default: 10,000. Value of 0 means unlimited.
     */
    private final long maxQueueSize;

    private final Subspace queueSubspace;
    private final Subspace queueSizeSubspace;

    /**
     * Create a pending write queue.
     *
     * @param queueSubspace the subspace for this partition's queue, should include the partition ID and grouping key,
     * as necessary
     * @param queueSizeSubspace the subspace for storing the queue size counter
     */
    public PendingWriteQueue(@Nonnull Subspace queueSubspace, @Nonnull Subspace queueSizeSubspace) {
        this(queueSubspace, queueSizeSubspace, DEFAULT_MAX_PENDING_ENTRIES_TO_REPLAY, DEFAULT_MAX_PENDING_QUEUE_SIZE);
    }

    public PendingWriteQueue(@Nonnull Subspace queueSubspace, @Nonnull Subspace queueSizeSubspace, int maxEntriesToReplay, int maxQueueSize) {
        this.queueSubspace = queueSubspace;
        this.queueSizeSubspace = queueSizeSubspace;
        this.maxEntriesToReplay = maxEntriesToReplay;
        this.maxQueueSize = maxQueueSize;
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
     * The {@link QueueEntry} given to this method (likely returned from the
     * {@link #getQueueCursor(FDBRecordContext, ScanProperties, byte[])}
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

        // Atomically decrement the queue size counter
        mutateQueueSizeCounter(context, -1);

        // Record metrics
        context.increment(LuceneEvents.Counts.LUCENE_PENDING_QUEUE_CLEAR);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getLogMessage("Cleared queue entry").toString());
        }
    }

    /**
     * Check if the pending write queue is empty.
     *
     * @param context the record context
     * @return a future that resolves to {@code true} if the queue is empty, {@code false} otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> isQueueEmpty(@Nonnull FDBRecordContext context) {
        // This is using direct count as it is still efficient, and is accurate in all cases, even when the counter is
        // uninitialized or has drifted
        // Return true if empty
        return context.ensureActive()
                .getRange(queueSubspace.range(), 1)
                .asList()
                .thenApply(List::isEmpty);
    }

    /**
     * Replay all queued operations into the index.
     * This should be called before executing a query to ensure queued writes are visible.
     *
     * @param indexWriter the index writer to write to
     *
     * @return CompletableFuture that completes when all operations have been replayed
     */
    @Nonnull
    public CompletableFuture<Void> replayQueuedOperations(FDBRecordContext context, IndexWriter indexWriter, Index index) {
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
            replayOperation(context, entry.get(), indexWriter, index);
        }).thenAccept(lastResult -> {
            if (lastResult.getNoNextReason().equals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED)) {
                // Reached the row limit
                throw new TooManyPendingWritesException("Too many entries to replay into IndexWriter", maxEntriesToReplay);
            }
        });
    }

    /**
     * Replay a single queued operation directly to the IndexWriter.
     */
    private void replayOperation(FDBRecordContext context, @Nonnull QueueEntry entry, @Nonnull IndexWriter indexWriter, @Nonnull Index index) {
        LucenePendingWriteQueueProto.PendingWriteItem.OperationType opType = entry.getOperationType();

        try {
            final Tuple primaryKey = entry.getPrimaryKeyParsed();
            switch (opType) {
                case INSERT:
                    List<LuceneDocumentFromRecord.DocumentField> fields = PendingWritesQueueHelper.fromProtoFields(entry.getDocumentFields());
                    LuceneIndexMaintainerHelper.writeDocument(context, indexWriter, index, primaryKey, fields);
                    break;

                case DELETE:
                    LuceneIndexMaintainerHelper.deleteDocument(indexWriter, primaryKey, index);
                    break;

                case OPERATION_TYPE_UNSPECIFIED:
                default:
                    throw new RecordCoreInternalException("Unknown queue operation type", LuceneLogMessageKeys.OPERATION_TYPE, opType);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(getLogMessage("Replayed operation")
                        .addKeyAndValue(LuceneLogMessageKeys.OPERATION_TYPE, opType)
                        .toString());
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

        // Check if queue size limit is exceeded (if maxQueueSize > 0)
        if (maxQueueSize > 0) {
            if (isQueueTooLarge(context, maxQueueSize)) {
                throw new PendingWritesQueueTooLarge("Queue size too large, cannot enqueue items", (int)maxQueueSize);
            }
        }

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

        // Atomically increment the queue size counter
        mutateQueueSizeCounter(context, 1);

        // Record metrics
        context.increment(LuceneEvents.Counts.LUCENE_PENDING_QUEUE_WRITE);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getLogMessage("Enqueued operation")
                    .addKeyAndValue(LuceneLogMessageKeys.OPERATION_TYPE, operationType)
                    .addKeyAndValue(LogMessageKeys.SUBSPACE, queueSubspace)
                    .toString());
        }
    }

    private KeyValueLogMessage getLogMessage(final @Nonnull String staticMsg) {
        return KeyValueLogMessage.build(staticMsg)
                .addKeyAndValue(LogMessageKeys.SUBSPACE, queueSubspace);
    }

    /**
     * Atomically mutate the queue size counter.
     * This would increment/decrement the queue size counter by the given amount. If the queue counter is not initialized
     * (key does not exist), FDB's ADD mutation treats it as 0, so the counter will be initialized to the given amount.
     * This is standard FDB behavior: ADD mutation on a non-existent key treats the original value as 0.
     * @param context the context to use
     * @param amount the amount by which to mutate the counter
     */
    private void mutateQueueSizeCounter(final @Nonnull FDBRecordContext context, final int amount) {
        context.ensureActive().mutate(MutationType.ADD, queueSizeSubspace.pack(), encodeQueueSize(amount));
    }

    /**
     * Return TRUE if the queue has no room for new entries.
     * This method uses the queue size counter to get the queue size, defaulting to FALSE when the counter does not exist.
     * @param context the context to use to read the counter
     * @param size the size to check
     * @return {@code true} is the queue size is larger than or equals the given size, {@code false} if smaller or not initialized
     */
    private boolean isQueueTooLarge(FDBRecordContext context, long size) {
        Long currentSize = LuceneConcurrency.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_QUEUE_SIZE, getQueueSize(context), context);
        // in case there is no value in the queue size subspace, we are not enforcing size yet (pre-initialization compatibility)
        return ((currentSize != null) && (currentSize >= size));
    }

    /**
     * Get the current size of the pending write queue.
     * Uses the atomic counter for efficiency. In case there is no counter available (e.g. the counter was not initialized),
     * null is returned.
     * This method uses a SNAPSHOT read to get the value in order to prevent conflicts on the counter in case another transaction
     * is mutating the value. This is OK in our case since pure serializability is not required (other than in the case
     * of {@link #isQueueEmpty(FDBRecordContext)}, in which case we use counting to see if the size is greater than 0).
     *
     * @param context the record context to use
     *
     * @return a future that resolves to the queue size, or {@code null} if the counter does not exist
     */
    @Nonnull
    public CompletableFuture<Long> getQueueSize(@Nonnull FDBRecordContext context) {
        return context.readTransaction(true).get(queueSizeSubspace.pack())
                .thenApply(size -> (size == null) ? null : decodeQueueSize(size));
    }

    /**
     * Initialize the queue size counter by counting the current number of entries in the queue.
     * This method should be called when transitioning from a non-enforced to enforced queue size limit,
     * or when the counter needs to be reset to match the actual queue size.
     * If maxQueueSize is configured (greater than 0), the scan will be limited to maxQueueSize entries.
     *
     * @param context the record context to use
     *
     * @return a future that resolves when the counter has been initialized
     */
    @Nonnull
    public CompletableFuture<Void> initializeQueueSizeCounter(@Nonnull FDBRecordContext context) {
        // Count the actual number of entries in the queue, limiting to maxQueueSize if configured
        // TODO: This may cause a skew of the count, that has no easy way to fix. Is this desired? Maybe just allow all?
        int scanLimit = maxQueueSize > 0 ? (int)maxQueueSize : 0;
        return context.ensureActive()
                .getRange(queueSubspace.range(), scanLimit)
                .asList()
                .thenAccept(keyValues -> {
                    long count = keyValues.size();
                    // Set the counter to the actual count
                    context.ensureActive().set(queueSizeSubspace.pack(), encodeQueueSize(count));

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(getLogMessage("Initialized queue size counter")
                                .addKeyAndValue(LuceneLogMessageKeys.COUNT, count)
                                .toString());
                    }
                });
    }

    private byte[] encodeQueueSize(long count) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(count).array();
    }

    private Long decodeQueueSize(@Nullable byte[] bytes) {
        return bytes == null ? null : ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
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

        public Tuple getPrimaryKeyParsed() {
            return Tuple.fromBytes(getPrimaryKey());
        }

        public long getEnqueuedTimeStamp() {
            return item.getEnqueueTimestamp();
        }

        public List<LucenePendingWriteQueueProto.DocumentField> getDocumentFields() {
            return item.getFieldsList();
        }

        public List<LuceneDocumentFromRecord.DocumentField> getDocumentFieldsParsed() {
            return PendingWritesQueueHelper.fromProtoFields(getDocumentFields());
        }
    }

    @SuppressWarnings("serial")
    public static final class TooManyPendingWritesException extends RecordCoreException {
        protected TooManyPendingWritesException(final String message, int itemCount) {
            super(message);
            addLogInfo(LuceneLogMessageKeys.MAX_ENTRIES_TO_REPLAY, itemCount);
        }
    }

    @SuppressWarnings("serial")
    public static class PendingWritesQueueTooLarge extends RecordCoreException {
        protected PendingWritesQueueTooLarge(final String message, long itemCount) {
            super(message);
            addLogInfo(LuceneLogMessageKeys.MAX_QUEUE_SIZE, itemCount);
        }
    }
}
