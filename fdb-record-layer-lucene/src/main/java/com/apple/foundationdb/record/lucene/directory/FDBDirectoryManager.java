/*
 * FDBDirectoryManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.KeyRange;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerCombinationProvider;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerRegistryImpl;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerType;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneIndexExpressions;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LucenePartitionInfoProto;
import com.apple.foundationdb.record.lucene.LucenePartitioner;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBTransactionPriority;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.CursorFactory;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ItemHandler;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ThrottledRetryingIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.util.CloseException;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A transaction-scoped manager of {@link FDBDirectory} objects. For a single transaction, all {@link FDBDirectory}
 * objects should be created through this manager. This allows for cached data (like the block cache or file
 * list cache) for a single directory to persist across different operations (e.g., different queries) conducted
 * in the same transaction.
 */
@API(API.Status.INTERNAL)
public class FDBDirectoryManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryManager.class);
    @Nonnull
    private final IndexMaintainerState state;
    @Nonnull
    private final Map<Tuple, FDBDirectoryWrapper> createdDirectories;
    private final int mergeDirectoryCount;
    @Nonnull
    protected final LuceneAnalyzerWrapper writerAnalyzer;
    @Nonnull
    private final LuceneAnalyzerCombinationProvider analyzerSelector;
    @Nullable
    protected final Exception exceptionAtCreation;

    protected FDBDirectoryManager(@Nonnull IndexMaintainerState state) {
        this.state = state;
        this.createdDirectories = new ConcurrentHashMap<>();
        this.mergeDirectoryCount = getMergeDirectoryCount(state);
        this.exceptionAtCreation = FDBTieredMergePolicy.usesCreationStack() ? new Exception() : null;
        final var fieldInfos = LuceneIndexExpressions.getDocumentFieldDerivations(state.index, state.store.getRecordMetaData());
        this.analyzerSelector = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.FULL_TEXT, fieldInfos);
        this.writerAnalyzer = analyzerSelector.provideIndexAnalyzer();
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public synchronized void close() throws IOException {
        for (FDBDirectoryWrapper directory : createdDirectories.values()) {
            directory.close();
        }
        createdDirectories.clear();
    }

    @Nonnull
    public LuceneAnalyzerCombinationProvider getAnalyzerSelector() {
        return analyzerSelector;
    }

    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Void> mergeIndex(@Nonnull LucenePartitioner partitioner) {
        // This function will iterate the grouping keys and explicitly merge each

        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(
                props -> props.clearState().setReturnedRowLimit(1));

        final Range range = state.indexSubspace.range();
        final KeyRange keyRange = new KeyRange(range.begin, range.end);
        final Subspace subspace = state.indexSubspace;
        final KeyExpression rootExpression = state.index.getRootExpression();

        // This agilityContext will be used to determine/iterate grouping keys and partitions. The time gap between calls might
        // be too long for a non-agile context.
        final AgilityContext agilityContext = getAgilityContext(true, false);

        if (! (rootExpression instanceof GroupingKeyExpression)) {
            // Here: empty grouping keys tuple
            return mergeIndex(TupleHelpers.EMPTY, partitioner, agilityContext)
                    .whenComplete((ignore, ex) -> closeOrAbortAgilityContext(agilityContext, ex));
        }
        // Here: iterate the grouping keys and merge each
        GroupingKeyExpression expression = (GroupingKeyExpression) rootExpression;
        final int groupingCount = expression.getGroupingCount();

        final RecordCursor<Tuple> cursor = new ChainedCursor<>(
                state.context,
                lastKey -> agilityContext.apply(context -> nextTuple(context, subspace, keyRange, lastKey, scanProperties, groupingCount)),
                Tuple::pack,
                Tuple::fromBytes,
                null,
                ScanProperties.FORWARD_SCAN);

        return cursor
                .map(tuple ->
                        Tuple.fromItems(tuple.getItems().subList(0, groupingCount)))
                // Use a pipeline size of 1. We don't want to be merging multiple different groups at a time
                // It may make sense in the future to make these concurrent, but there is enough complexity that it is
                // better to avoid the concurrent merges.
                // This also reduces the amount of load that a single store can cause on a system.
                .forEachAsync(groupingKey -> mergeIndex(groupingKey, partitioner, agilityContext),
                        1)
                .whenComplete((ignore, ex) -> closeOrAbortAgilityContext(agilityContext, ex));
    }

    private CompletableFuture<Void> mergeIndex(Tuple groupingKey,
                                               @Nonnull LucenePartitioner partitioner, final AgilityContext agileContext) {
        // Note: We always flush before calls to `mergeIndexNow` because we won't come back to get the next partition
        // or group until after the merge which could be many seconds later, in which case the current transaction would
        // no longer be valid. It may make sense to have AgilityContext.Agile commit periodically regardless of activity
        if (!partitioner.isPartitioningEnabled()) {
            agileContext.flush();
            mergeIndexNow(groupingKey, null);
            return AsyncUtil.DONE;
        } else {
            // Here: iterate the partition ids and merge each
            AtomicReference<LucenePartitionInfoProto.LucenePartitionInfo> lastPartitionInfo = new AtomicReference<>();
            return AsyncUtil.whileTrue(() -> getNextOlderPartitionInfo(groupingKey, agileContext, lastPartitionInfo)
                    .thenApply(partitionId -> {
                        if (partitionId == null) {
                            // partition list end
                            return false;
                        }
                        agileContext.flush();
                        mergeIndexNow(groupingKey, partitionId);
                        return true;
                    }));
        }
    }

    private void mergeIndexNow(Tuple groupingKey, @Nullable final Integer partitionId) {
        final AgilityContext agilityContext = getAgilityContext(true, true);
        try {
            mergeIndexWithContext(groupingKey, partitionId, agilityContext);
        } finally {
            // Here: drain this partition's queue and clear the "use queue" indicator
            drainPendingQueue(groupingKey, partitionId, agilityContext);

            // IndexWriter may release the file lock in a finally block in its own code, so if there is an error in its
            // code, we need to commit. We could optimize this a bit, and have it only flush if it has committed anything
            // but that should be rare.
            agilityContext.flushAndClose();
        }
    }

    public void mergeIndexWithContext(@Nonnull final Tuple groupingKey,
                                      @Nullable final Integer partitionId,
                                      @Nonnull final AgilityContext agilityContext) {
        try (FDBDirectoryWrapper directoryWrapper = createDirectoryWrapper(groupingKey, partitionId, agilityContext)) {
            try {
                directoryWrapper.setOngoingMergeIndicator();
                directoryWrapper.mergeIndex();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(KeyValueLogMessage.of("Lucene merge success",
                            LuceneLogMessageKeys.GROUP, groupingKey,
                            LuceneLogMessageKeys.INDEX_PARTITION, partitionId));
                }
            } catch (IOException e) {
                throw LuceneExceptions.toRecordCoreException("Lucene mergeIndex failed", e,
                        LuceneLogMessageKeys.GROUP, groupingKey,
                        LuceneLogMessageKeys.INDEX_PARTITION, partitionId);
            }
        } catch (IOException e) {
            // there was an IOException closing the index writer
            throw LuceneExceptions.toRecordCoreException("Lucene mergeIndex close failed", e,
                    LuceneLogMessageKeys.GROUP, groupingKey,
                    LuceneLogMessageKeys.INDEX_PARTITION, partitionId);
        }
    }

    public void drainPendingQueue(@Nonnull final Tuple groupingKey,
                                   @Nullable final Integer partitionId,
                                   @Nonnull final AgilityContext agilityContext) {
        try (FDBDirectoryWrapper directoryWrapper = createDirectoryWrapper(groupingKey, partitionId, agilityContext)) {
            drainPendingQueueWithRetries(groupingKey, partitionId, agilityContext, directoryWrapper);
        } catch (IOException e) {
            throw LuceneExceptions.toRecordCoreException("Drain pending queue failed", e,
                    LuceneLogMessageKeys.GROUP, groupingKey,
                    LuceneLogMessageKeys.INDEX_PARTITION, partitionId);
        }
    }

    private void drainPendingQueueWithRetries(@Nonnull final Tuple groupingKey,
                                              @Nullable final Integer partitionId,
                                              @Nonnull final AgilityContext agilityContext,
                                              FDBDirectoryWrapper directoryWrapper) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("Drain pending queue",
                    LogMessageKeys.GROUPING_KEY, groupingKey,
                    LogMessageKeys.PARTITION_ID, partitionId));
        }
        for (int retries = 0; retries < 10; retries ++) {
            try {
                agilityContext.flush(); // before potentially long drain
                drainPendingQueueNow(groupingKey, partitionId, agilityContext, directoryWrapper);
                directoryWrapper.clearOngoingMergeIndicator();
                agilityContext.flush(); // commit clear indicator
                return;
            } catch (RuntimeException ex) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("Failed to drain queue",
                            LogMessageKeys.RETRY_COUNT, retries,
                            LogMessageKeys.GROUPING_KEY, groupingKey,
                            LogMessageKeys.PARTITION_ID, partitionId
                    ), ex);
                }
            }
        }
        // Here: 10 failed clear queue attempts. What is the right thing to do?
    }

    private void drainPendingQueueNow(@Nonnull final Tuple groupingKey,
                                      @Nullable final Integer partitionId,
                                      @Nonnull final AgilityContext agilityContext,
                                      @Nonnull FDBDirectoryWrapper directoryWrapper) {
        // Note - since this directory wrapper was already created, agility context should be unused in the next line's path
        final PendingWriteQueue pendingWriteQueue = directoryWrapper.getPendingWriteQueue();
        try (ThrottledRetryingIterator<PendingWriteQueue.QueueEntry> iterator = ThrottledRetryingIterator.builder(
                        state.context.getDatabase(),
                        cursorFactory(pendingWriteQueue),
                        handleOneItemFactory(pendingWriteQueue, groupingKey, partitionId))
                .withMdcContext(MDC.getCopyOfContextMap())
                .withCommitWhenDone(true)
                .build()) {
            agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_DRAIN_PENDING_QUEUE,
                    iterator.iterateAll(state.store.asBuilder()));
        } catch (CloseException e) {
            throw new PendingQueueDrainException(e);
        }

    }

    private CursorFactory<PendingWriteQueue.QueueEntry> cursorFactory(PendingWriteQueue pendingWriteQueue) {
        return (@Nonnull FDBRecordStore store, @Nullable RecordCursorResult<PendingWriteQueue.QueueEntry> lastResult, int rowLimit) -> {
            byte[] continuation = lastResult == null ? null : lastResult.getContinuation().toBytes();
            ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(executeProperties -> executeProperties.setReturnedRowLimit(rowLimit));
            // Note: null could have been used instead of continuation as the preceding items should have been deleted. However,
            // using a continuation will prevent an infinite loop in case of a bug that doesn't clear items.
            return pendingWriteQueue.getQueueCursor(store.getContext(), scanProperties, continuation);
        };
    }

    private ItemHandler<PendingWriteQueue.QueueEntry> handleOneItemFactory(PendingWriteQueue pendingWriteQueue,
                                                                           @Nonnull final Tuple groupingKey,
                                                                           @Nullable final Integer partitionId) {
        return (store, lastResult, quotamanager) -> {
            final PendingWriteQueue.QueueEntry queueEntry = lastResult.get();
            if (queueEntry == null) {
                return AsyncUtil.DONE;
            }

            try {
                final LuceneIndexMaintainer maintainer = (LuceneIndexMaintainer)store.getIndexMaintainer(state.index);
                switch (queueEntry.getOperationType()) {
                    case UPDATE:
                    case INSERT:
                        if (store.isIndexWriteOnly(state.index)) {
                            // Here: covering a (rare) case of index becoming write-only after the item was queued.
                            // We could have been nice to avoid pushing the DELETE new record item in tryDeleteInWriteOnlyMode if queueing, but
                            // adding and processing this extra DELETE item in a background job seems easy enough.
                            maintainer.deleteDocumentBypassQueue(groupingKey, partitionId, queueEntry.getPrimaryKeyParsed());
                        }
                        maintainer.writeDocumentBypassQueue(groupingKey, partitionId, queueEntry.getPrimaryKeyParsed(), queueEntry.getDocumentFieldsParsed());
                        break;
                    case DELETE:
                        final int countDeleted = maintainer.deleteDocumentBypassQueue(groupingKey, partitionId, queueEntry.getPrimaryKeyParsed());
                        if (partitionId != null) {
                            state.context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_DECREMENT,
                                    maintainer.postDeleteUpdatePartitionCounter(groupingKey, partitionId, countDeleted));
                        }
                        break;
                    default:
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn(KeyValueLogMessage.of("bad queue entry",
                                    LogMessageKeys.PRIMARY_KEY, queueEntry.getPrimaryKey()));
                        }
                        break;
                }
                pendingWriteQueue.clearEntry(store.getContext(), queueEntry);
                return AsyncUtil.DONE;
            } catch (IOException ex) {
                throw new RecordCoreException("Lucene IOException", ex);
            }
        };
    }


    private static void closeOrAbortAgilityContext(AgilityContext agilityContext, Throwable ex) {
        if (ex == null) {
            agilityContext.flushAndClose();
        } else {
            agilityContext.abortAndClose();
        }
    }

    private CompletableFuture<Integer> getNextOlderPartitionInfo(final Tuple groupingKey, final AgilityContext agileContext,
                                                                 final AtomicReference<LucenePartitionInfoProto.LucenePartitionInfo> lastPartitionInfo) {
        return agileContext.apply(context -> LucenePartitioner.getNextOlderPartitionInfo(
                        context, groupingKey, lastPartitionInfo.get() == null ? null : LucenePartitioner.getPartitionKey(lastPartitionInfo.get()), state.indexSubspace)
                .thenApply(partitionInfo -> {
                    lastPartitionInfo.set(partitionInfo);
                    return partitionInfo == null ? null : partitionInfo.getId();
                }));
    }

    @SuppressWarnings("PMD.CloseResource")
    public static CompletableFuture<Optional<Tuple>> nextTuple(@Nonnull FDBRecordContext context,
                                                               @Nonnull Subspace subspace,
                                                               @Nonnull KeyRange range,
                                                               @Nonnull Optional<Tuple> lastTuple,
                                                               @Nonnull ScanProperties scanProperties,
                                                               int groupingCount) {
        KeyValueCursor.Builder cursorBuilder =
                KeyValueCursor.Builder.withSubspace(subspace)
                        .setContext(context)
                        .setContinuation(null)
                        .setScanProperties(scanProperties);

        if (lastTuple.isPresent()) {
            final byte[] lowKey = subspace.pack(Tuple.fromItems(lastTuple.get().getItems().subList(0, groupingCount)));
            cursorBuilder
                    .setLow(lowKey, EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(range.getHighKey(), range.getHighEndpoint());
        } else {
            cursorBuilder.setContext(context)
                    .setRange(range);
        }

        return cursorBuilder.build().onNext().thenApply(next -> {
            if (next.hasNext()) {
                KeyValue kv = next.get();
                if (kv != null) {
                    return Optional.of(subspace.unpack(kv.getKey()));
                }
            }
            return Optional.empty();
        });
    }

    /**
     * Invalidate directories from the cache if their grouping key begins with a specified prefix.
     * @param prefix the prefix of grouping keys to remove from the cache
     */
    public void invalidatePrefix(@Nonnull Tuple prefix) {
        final Iterator<Map.Entry<Tuple, FDBDirectoryWrapper>> iterator = createdDirectories.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Tuple, FDBDirectoryWrapper> item = iterator.next();
            if (TupleHelpers.isPrefix(prefix, item.getKey())) {
                try {
                    // Close the directory and associated readers/writers
                    item.getValue().close();
                } catch (IOException e) {
                    throw LuceneExceptions.toRecordCoreException("unable to close index writer", e);
                }
                iterator.remove();
            }
        }
    }

    public PendingWriteQueue getPendingWriteQueue(@Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        return getDirectoryWrapper(groupingKey, partitionId).getPendingWriteQueue();
    }

    @VisibleForTesting
    public FDBDirectoryWrapper getDirectoryWrapper(@Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        return getDirectoryWrapper(groupingKey, partitionId, getAgilityContext(false, false));
    }

    private FDBDirectoryWrapper getDirectoryWrapper(@Nullable Tuple groupingKey, @Nullable Integer partitionId, final AgilityContext agilityContext) {
        final Tuple mapKey = getDirectoryKey(groupingKey, partitionId);
        return createdDirectories.computeIfAbsent(mapKey, key -> createNewDirectoryWrapper(state, key, mergeDirectoryCount, agilityContext, getBlockCacheMaximumSize()));
    }

    private FDBDirectoryWrapper createDirectoryWrapper(@Nullable Tuple groupingKey, @Nullable Integer partitionId,
                                                      final AgilityContext agilityContext) {
        return createNewDirectoryWrapper(state, getDirectoryKey(groupingKey, partitionId), mergeDirectoryCount, agilityContext, getBlockCacheMaximumSize());
    }

    protected @Nonnull FDBDirectoryWrapper createNewDirectoryWrapper(final IndexMaintainerState state, final Tuple key, final int mergeDirectoryCount, final AgilityContext agilityContext, final int blockCacheMaximumSize) {
        return new FDBDirectoryWrapper(state, key, mergeDirectoryCount, agilityContext, blockCacheMaximumSize,
                writerAnalyzer, exceptionAtCreation);
    }

    private int getBlockCacheMaximumSize() {
        return state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_BLOCK_CACHE_MAXIMUM_SIZE);
    }

    private static Tuple getDirectoryKey(final @Nullable Tuple groupingKey, final @Nullable Integer partitionId) {
        Tuple mapKey = groupingKey == null ? TupleHelpers.EMPTY : groupingKey;
        if (partitionId != null) {
            mapKey = mapKey.add(LucenePartitioner.PARTITION_DATA_SUBSPACE).add(partitionId);
        }
        return mapKey;
    }

    private AgilityContext getAgilityContext(boolean useAgilityContext, boolean allowDefaultPriority) {
        final IndexDeferredMaintenanceControl deferredControl = state.store.getIndexDeferredMaintenanceControl();
        if (!useAgilityContext || Boolean.TRUE.equals(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AGILE_DISABLE_AGILITY_CONTEXT))) {
            // Avoid potential retries:
            deferredControl.setTimeQuotaMillis(0);
            deferredControl.setSizeQuotaBytes(0);
            return AgilityContext.nonAgile(state.context);
        }
        // Here: return an agile context
        long timeQuotaMillis = deferredControl.getTimeQuotaMillis();
        if (timeQuotaMillis <= 0) {
            timeQuotaMillis = Objects.requireNonNullElse(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA), 4000);
            deferredControl.setTimeQuotaMillis(timeQuotaMillis);
        }
        long sizeQuotaBytes = deferredControl.getSizeQuotaBytes();
        if (sizeQuotaBytes <= 0) {
            sizeQuotaBytes =  Objects.requireNonNullElse(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA), 900_000);
            deferredControl.setSizeQuotaBytes(sizeQuotaBytes);
        }
        boolean useDefaultPriorityDuringMerge = allowDefaultPriority && Objects.requireNonNullElse(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_USE_DEFAULT_PRIORITY_DURING_MERGE), true);
        if (useDefaultPriorityDuringMerge) {
            final FDBRecordContextConfig.Builder contextBuilder = state.context.getConfig().toBuilder();
            contextBuilder.setPriority(FDBTransactionPriority.DEFAULT);
            return AgilityContext.agile(state.context, contextBuilder, timeQuotaMillis, sizeQuotaBytes);
        } else {
            return AgilityContext.agile(state.context, timeQuotaMillis, sizeQuotaBytes);
        }
    }

    @Nonnull
    public FDBDirectory getDirectory(@Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        return getDirectoryWrapper(groupingKey, partitionId).getDirectory();
    }

    public IndexReader getIndexReader(@Nullable Tuple groupingKey, @Nullable Integer partitionId) throws IOException {
        return getDirectoryWrapper(groupingKey, partitionId).getReader();
    }

    @Nonnull
    public IndexWriter getIndexWriter(@Nullable Tuple groupingKey, @Nullable Integer partitionId) throws IOException {
        return getDirectoryWrapper(groupingKey, partitionId).getWriter();
    }

    public DirectoryReader getWriterReader(@Nullable Tuple groupingKey, @Nullable Integer partitionId) throws IOException {
        return getWriterReader(groupingKey, partitionId, false);
    }

    public DirectoryReader getWriterReader(@Nullable Tuple groupingKey, @Nullable Integer partititonId, boolean refresh) throws IOException {
        return getDirectoryWrapper(groupingKey, partititonId).getWriterReader(refresh);
    }

    @Nonnull
    public static FDBDirectoryManager getManager(@Nonnull IndexMaintainerState state) {
        return getOrCreateManager(state, () -> new FDBDirectoryManager(state));
    }

    @SuppressWarnings("PMD.CloseResource")
    protected static @Nonnull FDBDirectoryManager getOrCreateManager(final @Nonnull IndexMaintainerState state, Supplier<FDBDirectoryManager> managerSupplier) {
        synchronized (state.context) {
            FDBRecordContext context = state.context;
            FDBDirectoryManager existing = context.getInSession(state.indexSubspace, FDBDirectoryManager.class);
            if (existing != null) {
                return existing;
            }
            FDBDirectoryManager newManager = managerSupplier.get();
            context.putInSessionIfAbsent(state.indexSubspace, newManager);
            context.addCommitCheck(() -> {
                try {
                    newManager.close();
                } catch (IOException e) {
                    throw LuceneExceptions.toRecordCoreException("unable to close directories", e);
                }
                return AsyncUtil.DONE;
            });
            return newManager;
        }
    }

    private int getMergeDirectoryCount(@Nonnull IndexMaintainerState state) {
        return Math.toIntExact(state.store
                .getRecordMetaData()
                .getAllIndexes()
                .stream()
                .filter(i -> LuceneIndexTypes.LUCENE.equals(i.getType()))
                .count());
    }

    /**
     * thrown if pending queue drain had failed.
     */
    @SuppressWarnings("java:S110")
    public static class PendingQueueDrainException extends RecordCoreException {
        private static final long serialVersionUID = 10;

        public PendingQueueDrainException(final Throwable cause) {
            super("Pending queue drain had failed", cause);
        }
    }
}
