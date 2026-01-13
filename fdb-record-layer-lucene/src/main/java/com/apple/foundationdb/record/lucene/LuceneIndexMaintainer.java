/*
 * LuceneIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.AgilityContext;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.apple.foundationdb.record.lucene.idformat.LuceneIndexKeySerializer;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Index maintainer for Lucene Indexes backed by FDB.  The insert, update, and delete functionality
 * coupled with the scan functionality is implemented here.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneIndexMaintainer extends StandardIndexMaintainer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexMaintainer.class);

    @Nonnull
    private final FDBDirectoryManager directoryManager;
    private final LuceneAnalyzerCombinationProvider autoCompleteAnalyzerSelector;
    public static final String PRIMARY_KEY_FIELD_NAME = "_p";
    protected static final String PRIMARY_KEY_SEARCH_NAME = "_s";
    protected static final String PRIMARY_KEY_BINARY_POINT_NAME = "_b";
    private final Executor executor;
    LuceneIndexKeySerializer keySerializer;

    @Nonnull
    private final LucenePartitioner partitioner;

    public LuceneIndexMaintainer(@Nonnull final IndexMaintainerState state, @Nonnull Executor executor) {
        super(state);
        this.executor = executor;
        this.directoryManager = createDirectoryManager(state);
        final var fieldInfos = LuceneIndexExpressions.getDocumentFieldDerivations(state.index, state.store.getRecordMetaData());
        this.autoCompleteAnalyzerSelector = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.AUTO_COMPLETE, fieldInfos);
        String formatString = state.index.getOption(LuceneIndexOptions.PRIMARY_KEY_SERIALIZATION_FORMAT);
        keySerializer = LuceneIndexKeySerializer.fromStringFormat(formatString);
        partitioner = new LucenePartitioner(state);
    }

    public LuceneAnalyzerCombinationProvider getAutoCompleteAnalyzerSelector() {
        return autoCompleteAnalyzerSelector;
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanType scanType, @Nonnull final TupleRange range, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        throw new RecordCoreException("unsupported scan type for Lucene index: " + scanType);
    }

    /**
     * The scan takes Lucene a {@link Query} as scan bounds.
     *
     * @param scanBounds the {@link IndexScanType type} of Lucene scan and associated {@code Query}
     * @param continuation any continuation from a previous scan invocation
     * @param scanProperties skip, limit and other properties of the scan
     * @return RecordCursor of index entries reconstituted from Lucene documents
     */
    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        final IndexScanType scanType = scanBounds.getScanType();
        LOG.trace("scan scanType={}", scanType);


        if (scanType.equals(LuceneScanTypes.BY_LUCENE)) {
            LuceneScanQuery scanQuery = (LuceneScanQuery)scanBounds;
            // if partitioning is enabled, a non-null continuation will include the current partition info
            LucenePartitioner.PartitionedQueryHint partitionedQueryHint = continuation == null ? partitioner.selectQueryPartition(scanQuery.getGroupKey(), scanQuery) : null;
            if (partitionedQueryHint != null && !partitionedQueryHint.canHaveMatches) {
                return RecordCursor.empty(state.context.getExecutor());
            }
            LucenePartitionInfoProto.LucenePartitionInfo partitionInfo = partitionedQueryHint == null ? null : partitionedQueryHint.startPartition;

            return new LuceneRecordCursor(executor, state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_EXECUTOR_SERVICE),
                    partitioner,
                    state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE),
                    scanProperties, state, scanQuery.getQuery(), scanQuery.getSort(), continuation,
                    scanQuery.getGroupKey(), partitionInfo, scanQuery.getLuceneQueryHighlightParameters(), scanQuery.getTermMap(),
                    scanQuery.getStoredFields(), scanQuery.getStoredFieldTypes(), directoryManager.getAnalyzerSelector(), autoCompleteAnalyzerSelector);
        }

        if (scanType.equals(LuceneScanTypes.BY_LUCENE_SPELL_CHECK)) {
            if (continuation != null) {
                throw new RecordCoreArgumentException("Spellcheck does not currently support continuation scanning");
            }
            LuceneScanSpellCheck scanSpellcheck = (LuceneScanSpellCheck)scanBounds;
            return new LuceneSpellCheckRecordCursor(scanSpellcheck.getFields(), scanSpellcheck.getWord(),
                    executor, scanProperties, state, scanSpellcheck.getGroupKey(),
                    partitioner.selectQueryPartitionId(scanSpellcheck.getGroupKey()));
        }

        throw new RecordCoreException("unsupported scan type for Lucene index: " + scanType);
    }

    <M extends Message> void writeDocument(final FDBIndexableRecord<M> newRecord, final Map.Entry<Tuple, List<LuceneDocumentFromRecord.DocumentField>> entry, final Integer partitionId) {
        try {
            writeDocument(entry.getValue(), entry.getKey(), partitionId, newRecord.getPrimaryKey());
        } catch (IOException e) {
            throw LuceneExceptions.toRecordCoreException("Issue updating new index keys", e, "newRecord", newRecord.getPrimaryKey());
        }
    }

    private void writeDocument(@Nonnull List<LuceneDocumentFromRecord.DocumentField> fields,
                               Tuple groupingKey,
                               Integer partitionId,
                               Tuple primaryKey) throws IOException {
        LuceneIndexMaintainerHelper.writeDocument(state.context, directoryManager, state.index, groupingKey, partitionId,
                primaryKey, fields);
    }

    int deleteDocument(Tuple groupingKey, Integer partitionId, Tuple primaryKey) throws IOException {
        return LuceneIndexMaintainerHelper.deleteDocument(state.context, directoryManager, state.index, groupingKey, partitionId, primaryKey,
                state.store.isIndexWriteOnly(state.index));
    }

    @Override
    public CompletableFuture<Void> mergeIndex() {
        return rebalancePartitions()
                .thenCompose(ignored -> {
                    state.store.getIndexDeferredMaintenanceControl().setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
                    return directoryManager.mergeIndex(partitioner);
                });
    }

    @VisibleForTesting
    public void mergeIndexForTesting(@Nonnull final Tuple groupingKey,
                                     @Nullable final Integer partitionId,
                                     @Nonnull final AgilityContext agilityContext) throws IOException {
        directoryManager.mergeIndexWithContext(groupingKey, partitionId, agilityContext);
    }

    @Nonnull
    @VisibleForTesting
    public LucenePartitioner getPartitioner() {
        return partitioner;
    }

    @Nonnull
    private static LucenePartitioner getPartitioner(final Index index, final FDBRecordStore store) {
        final IndexMaintainer indexMaintainer = store.getIndexMaintainer(index);
        if (indexMaintainer instanceof LuceneIndexMaintainer) {
            return ((LuceneIndexMaintainer)indexMaintainer).partitioner;
        } else {
            throw new RecordCoreException("Index being repartitioned is no longer a lucene index")
                    .addLogInfo(LogMessageKeys.INDEX_NAME, index.getName(),
                            LogMessageKeys.INDEX_TYPE, index.getType());
        }
    }

    @SuppressWarnings("PMD.CloseResource") // the runner is closed in a whenComplete on the future returned
    public CompletableFuture<Void> rebalancePartitions() {
        if (!partitioner.isPartitioningEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
        mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.REPARTITION);
        int documentCount = mergeControl.getRepartitionDocumentCount();
        if (documentCount < 0) {
            // Skip re-balance
            return CompletableFuture.completedFuture(null);
        }
        if (documentCount == 0) {
            // Set default and broadcast it back to the caller
            documentCount = Objects.requireNonNull(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT));
            mergeControl.setRepartitionDocumentCount(documentCount);
        }

        final FDBRecordStore.Builder storeBuilder = state.store.asBuilder();
        FDBDatabaseRunner runner = state.context.newRunner();
        runner.setMaxAttempts(1); // retries (and throttling) are performed by the caller
        return rebalancePartitions(runner, storeBuilder, state, mergeControl)
                .whenComplete((result, error) -> {
                    runner.close();
                });
    }

    private static CompletableFuture<Void> rebalancePartitions(final FDBDatabaseRunner runner,
                                                               final FDBRecordStore.Builder storeBuilder,
                                                               final IndexMaintainerState state,
                                                               final IndexDeferredMaintenanceControl mergeControl) {
        FDBStoreTimer timer = state.context.getTimer();
        if (timer != null) {
            timer.increment(LuceneEvents.Counts.LUCENE_REPARTITION_CALLS);
        }
        AtomicReference<RecordCursorContinuation> continuation = new AtomicReference<>(RecordCursorStartContinuation.START);
        final int repartitionDocumentCount = mergeControl.getRepartitionDocumentCount();
        final int maxDocumentsToMove = Objects.requireNonNull(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING));
        AtomicInteger maxIterations = new AtomicInteger(Math.max(1, maxDocumentsToMove / repartitionDocumentCount));
        LucenePartitioner.RepartitioningLogMessages repartitioningLogMessages = new LucenePartitioner.RepartitioningLogMessages(-1, Tuple.from(), 0);
        return AsyncUtil.whileTrue(
                () -> runner.runAsync(
                        context -> context.instrument(LuceneEvents.Events.LUCENE_REBALANCE_PARTITION_TRANSACTION,
                                storeBuilder.setContext(context).openAsync().thenCompose(store ->
                                        getPartitioner(state.index, store).rebalancePartitions(continuation.get(), repartitionDocumentCount, repartitioningLogMessages)
                                                .thenApply(newContinuation -> {
                                                    if (newContinuation.isEnd() || maxIterations.decrementAndGet() == 0) {
                                                        mergeControl.setRepartitionCapped(!newContinuation.isEnd());
                                                        return false;
                                                    } else {
                                                        continuation.set(newContinuation);
                                                        return true;
                                                    }
                                                }))), repartitioningLogMessages.logMessages), state.context.getExecutor());
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
        return update(oldRecord, newRecord, null);
    }

    @Nonnull
    <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecordUnfiltered,
                                                       @Nullable FDBIndexableRecord<M> newRecordUnfiltered,
                                                       @Nullable Integer destinationPartitionIdHint) {
        FDBIndexableRecord<M> oldRecord = maybeFilterRecord(oldRecordUnfiltered);
        FDBIndexableRecord<M> newRecord = maybeFilterRecord(newRecordUnfiltered);

        LOG.trace("update oldRecord={}, newRecord={}", oldRecord, newRecord);

        // Extract information for grouping from old and new records
        final KeyExpression root = state.index.getRootExpression();
        final Map<Tuple, List<LuceneDocumentFromRecord.DocumentField>> oldRecordFields = LuceneDocumentFromRecord.getRecordFields(root, oldRecord);
        final Map<Tuple, List<LuceneDocumentFromRecord.DocumentField>> newRecordFields = LuceneDocumentFromRecord.getRecordFields(root, newRecord);

        final Set<Tuple> unchanged = new HashSet<>();
        for (Map.Entry<Tuple, List<LuceneDocumentFromRecord.DocumentField>> entry : oldRecordFields.entrySet()) {
            if (entry.getValue().equals(newRecordFields.get(entry.getKey()))) {
                unchanged.add(entry.getKey());
            }
        }
        for (Tuple t : unchanged) {
            newRecordFields.remove(t);
            oldRecordFields.remove(t);
        }

        LOG.trace("update oldFields={}, newFields{}", oldRecordFields, newRecordFields);

        return AsyncUtil.whenAll(oldRecordFields.keySet().stream()
                    // delete old
                    .map(groupingKey -> tryDelete(Objects.requireNonNull(oldRecord), groupingKey))
                    .collect(Collectors.toList()))
                .thenCompose(ignored ->
                    // update new
                    AsyncUtil.whenAll(newRecordFields.entrySet().stream()
                        .map(entry -> updateRecord(newRecord, destinationPartitionIdHint, entry))
                        .collect(Collectors.toList())));
    }

    /**
     * Internal utility to update a single record.
     * @param newRecord the new record to save
     * @param destinationPartitionIdHint partition ID
     * @param entry entry from the grouping key to the document fields
     */
    private <M extends Message> CompletableFuture<Void> updateRecord(
            final FDBIndexableRecord<M> newRecord,
            final Integer destinationPartitionIdHint,
            final Map.Entry<Tuple, List<LuceneDocumentFromRecord.DocumentField>> entry) {
        return tryDeleteInWriteOnlyMode(Objects.requireNonNull(newRecord), entry.getKey()).thenCompose(countDeleted ->
                partitioner.addToAndSavePartitionMetadata(newRecord, entry.getKey(), destinationPartitionIdHint)
                        .thenAccept(partitionId -> writeDocument(newRecord, entry, partitionId)));
    }

    @Nullable
    public <M extends Message> FDBIndexableRecord<M> maybeFilterRecord(FDBIndexableRecord<M> rec) {
        if (rec != null) {
            final IndexMaintenanceFilter.IndexValues filterType = getFilterTypeForRecord(rec);
            if (filterType == IndexMaintenanceFilter.IndexValues.NONE) {
                return null;
            } else if (filterType == IndexMaintenanceFilter.IndexValues.SOME) {
                throw new RecordCoreException("Lucene does not support this kind of filtering");
            }
        }
        return rec;
    }

    /**
     * convenience wrapper that calls {@link #tryDelete(FDBIndexableRecord, Tuple)} only if the index is in
     * {@code WriteOnly} mode.
     * This is usually needed when a record is inserted during an index build, but that
     * record had already been added due to an explicit update earlier.
     *
     * @param record record to try and delete
     * @param groupingKey grouping key
     * @param <M> message
     * @return count of deleted docs
     */
    private <M extends Message> CompletableFuture<Integer> tryDeleteInWriteOnlyMode(@Nonnull FDBIndexableRecord<M> record,
                                                                                    @Nonnull Tuple groupingKey) {
        if (!state.store.isIndexWriteOnly(state.index)) {
            // no op
            return CompletableFuture.completedFuture(0);
        }
        return tryDelete(record, groupingKey);
    }

    /**
     * Delete a given record if it is indexed.
     * The record may not necessarily exist in the index, or it may need to be deleted by query ({@link #deleteDocument(Tuple, Integer, Tuple)}).
     *
     * @param record record to be deleted
     * @param groupingKey grouping key
     * @param <M> record message
     * @return count of deleted docs: 1 indicates that the record has been deleted, 0 means that either no record was deleted or it was deleted by
     * query.
     */
    private <M extends Message> CompletableFuture<Integer> tryDelete(@Nonnull FDBIndexableRecord<M> record,
                                                                     @Nonnull Tuple groupingKey) {
        try {
            // non-partitioned
            if (!partitioner.isPartitioningEnabled()) {
                return CompletableFuture.completedFuture(deleteDocument(groupingKey, null, record.getPrimaryKey()));
            }
        } catch (IOException e) {
            throw LuceneExceptions.toRecordCoreException("Issue deleting", e, "record", Objects.requireNonNull(record).getPrimaryKey());
        }

        // partitioned
        return partitioner.tryGetPartitionInfo(record, groupingKey).thenCompose(partitionInfo -> {
            if (partitionInfo == null) {
                return CompletableFuture.completedFuture(0);
            }
            try {
                int countDeleted = deleteDocument(groupingKey, partitionInfo.getId(), record.getPrimaryKey());
                // this might be 0 when in writeOnly mode, but otherwise should not happen.
                if (countDeleted > 0) {
                    return partitioner.decrementCountAndSave(groupingKey, countDeleted, partitionInfo.getId())
                            .thenApply(vignore -> countDeleted);
                } else {
                    return CompletableFuture.completedFuture(countDeleted);
                }
            } catch (IOException e) {
                throw LuceneExceptions.toRecordCoreException("Issue deleting", e, "record", record.getPrimaryKey());
            }
        });
    }

    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation, @Nullable ScanProperties scanProperties) {
        LOG.trace("validateEntries");
        return RecordCursor.empty(executor);
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        LOG.trace("canEvaluateRecordFunction() function={}", function);
        return false;
    }

    @Nonnull
    @Override
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        LOG.warn("evaluateRecordFunction() function={}", function);
        return unsupportedRecordFunction(function);
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        LOG.trace("canEvaluateAggregateFunction() function={}", function);
        return false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        LOG.warn("evaluateAggregateFunction() function={}", function);
        return unsupportedAggregateFunction(function);
    }

    @Override
    public boolean isIdempotent() {
        LOG.trace("isIdempotent()");
        return true;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
        LOG.trace("addedRangeWithKey primaryKey={}", primaryKey);
        return AsyncUtil.READY_FALSE;
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        LOG.trace("canDeleteWhere matcher={}", matcher);
        return canDeleteGroup(matcher, evaluated);
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        LOG.trace("deleteWhere transaction={}, prefix={}", tr, prefix);
        directoryManager.invalidatePrefix(prefix);
        return super.deleteWhere(tr, prefix);
    }

    @Override
    @Nonnull
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        LOG.trace("performOperation operation={}", operation);
        if (operation instanceof LuceneGetMetadataInfo) {
            final LuceneGetMetadataInfo request = (LuceneGetMetadataInfo)operation;
            if (request.isJustPartitionInfo()) {
                // There isn't a more efficient way to get partition info by id than loading it all, and
                // if you can't load it all and one partition's lucene index in a single transaction,
                // you won't be able to repartition, so we always provide all the partition info.
                if (partitioner.isPartitioningEnabled()) {
                    return partitioner.getAllPartitionMetaInfo(request.getGroupingKey())
                            .thenApply(partitionInfos -> new LuceneMetadataInfo(partitionInfos, Map.of()));
                } else {
                    return CompletableFuture.completedFuture(new LuceneMetadataInfo(List.of(), Map.of()));
                }
            } else {
                if (partitioner.isPartitioningEnabled()) {
                    return getLuceneInfoForAllPartitions(request);
                } else {
                    return getLuceneInfo(request.getGroupingKey(), null)
                            .thenApply(luceneInfos -> new LuceneMetadataInfo(List.of(),
                                    Map.of(0, luceneInfos)));

                }
            }
        }

        return super.performOperation(operation);
    }

    private CompletableFuture<IndexOperationResult> getLuceneInfoForAllPartitions(final LuceneGetMetadataInfo request) {
        return partitioner.getAllPartitionMetaInfo(request.getGroupingKey())
                .thenCompose(partitionInfos -> {
                    Stream<LucenePartitionInfoProto.LucenePartitionInfo> infoStream = partitionInfos.stream();
                    // There isn't a more efficient way to get partition info by id than loading it all, and
                    // if you can't load it all and one partition's lucene index in a single transaction,
                    // you won't be able to repartition, so we always provide all the partition info.
                    if (request.getPartitionId() != null) {
                        infoStream = infoStream.filter(info -> info.getId() == request.getPartitionId());
                    }
                    final List<CompletableFuture<Map.Entry<Integer, LuceneMetadataInfo.LuceneInfo>>> luceneInfos =
                            infoStream.map(partitionInfo ->
                                            getLuceneInfo(request.getGroupingKey(), partitionInfo.getId())
                                                    .thenApply(luceneInfo -> Map.entry(partitionInfo.getId(), luceneInfo)))
                                    .collect(Collectors.toList());
                    return AsyncUtil.getAll(luceneInfos).thenApply(entries ->
                            new LuceneMetadataInfo(partitionInfos,
                                    entries.stream()
                                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
                });
    }


    @SuppressWarnings("PMD.CloseResource") // the indexReader and directory are closed by the directoryManager
    private CompletableFuture<LuceneMetadataInfo.LuceneInfo> getLuceneInfo(final Tuple groupingKey, final Integer partitionId) {
        try {
            try (IndexReader indexReader = directoryManager.getIndexReader(groupingKey, partitionId)) {
                final FDBDirectory directory = getDirectory(groupingKey, partitionId);
                final CompletableFuture<Integer> fieldInfosFuture = directory.getFieldInfosCount();
                return directory.getAllAsync()
                        .thenCombine(fieldInfosFuture, (fileList, fieldInfosCount) ->
                                new LuceneMetadataInfo.LuceneInfo(
                                        indexReader.numDocs(),
                                        fieldInfosCount,
                                        toLuceneFileInfo(fileList)));
            }
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Nonnull
    private static List<LuceneMetadataInfo.LuceneFileInfo> toLuceneFileInfo(final Map<String, FDBLuceneFileReference> fileList) {
        return fileList.entrySet().stream()
                .map(entry -> new LuceneMetadataInfo.LuceneFileInfo(
                        entry.getKey(),
                        entry.getValue().getId(),
                        entry.getValue().getSize()))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected FDBDirectory getDirectory(@Nonnull Tuple groupingKey, @Nullable Integer partitionId) {
        return directoryManager.getDirectory(groupingKey, partitionId);
    }

    @VisibleForTesting
    public FDBDirectoryManager getDirectoryManager() {
        return directoryManager;
    }

    @VisibleForTesting
    @Nonnull
    protected FDBDirectoryManager createDirectoryManager(final @Nonnull IndexMaintainerState state) {
        return FDBDirectoryManager.getManager(state);
    }

    @Nullable
    @Override
    public IndexScrubbingTools<?> getIndexScrubbingTools(final IndexScrubbingTools.ScrubbingType type) {
        switch (type) {
            case MISSING:
                final Map<String, String> options = state.index.getOptions();
                if (Boolean.parseBoolean(options.get(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED)) ||
                        Boolean.parseBoolean(options.get(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED))) {
                    return new LuceneIndexScrubbingToolsMissing(partitioner, directoryManager, this);
                }
                return null;
            default:
                return null;
        }
    }
}
