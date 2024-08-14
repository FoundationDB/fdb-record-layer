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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.AgilityContext;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.idformat.LuceneIndexKeySerializer;
import com.apple.foundationdb.record.lucene.idformat.RecordCoreFormatException;
import com.apple.foundationdb.record.lucene.search.BooleanPointsConfig;
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
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
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

    private final FDBDirectoryManager directoryManager;
    private final LuceneAnalyzerCombinationProvider indexAnalyzerSelector;
    private final LuceneAnalyzerCombinationProvider autoCompleteAnalyzerSelector;
    public static final String PRIMARY_KEY_FIELD_NAME = "_p";
    protected static final String PRIMARY_KEY_SEARCH_NAME = "_s";
    protected static final String PRIMARY_KEY_BINARY_POINT_NAME = "_b";
    private final Executor executor;
    LuceneIndexKeySerializer keySerializer;
    private boolean serializerErrorLogged = false;
    @Nonnull
    private final LucenePartitioner partitioner;

    public LuceneIndexMaintainer(@Nonnull final IndexMaintainerState state, @Nonnull Executor executor) {
        super(state);
        this.executor = executor;
        this.directoryManager = FDBDirectoryManager.getManager(state);
        final var fieldInfos = LuceneIndexExpressions.getDocumentFieldDerivations(state.index, state.store.getRecordMetaData());
        this.indexAnalyzerSelector = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.FULL_TEXT, fieldInfos);
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
                    scanQuery.getStoredFields(), scanQuery.getStoredFieldTypes(), indexAnalyzerSelector, autoCompleteAnalyzerSelector);
        }

        if (scanType.equals(LuceneScanTypes.BY_LUCENE_SPELL_CHECK)) {
            if (continuation != null) {
                throw new RecordCoreArgumentException("Spellcheck does not currently support continuation scanning");
            }
            LuceneScanSpellCheck scanSpellcheck = (LuceneScanSpellCheck)scanBounds;
            return new LuceneSpellCheckRecordCursor(scanSpellcheck.getFields(), scanSpellcheck.getWord(),
                    executor, scanProperties, state, scanSpellcheck.getGroupKey(), partitioner.selectQueryPartitionId(scanSpellcheck.getGroupKey()));
        }

        throw new RecordCoreException("unsupported scan type for Lucene index: " + scanType);
    }


    /**
     * Insert a field into the document and add a suggestion into the suggester if needed.
     */
    @SuppressWarnings("java:S3776")
    private void insertField(LuceneDocumentFromRecord.DocumentField field, final Document document) {
        final String fieldName = field.getFieldName();
        final Object value = field.getValue();
        final Field luceneField;
        final Field sortedField;
        final StoredField storedField;
        switch (field.getType()) {
            case TEXT:
                luceneField = new Field(fieldName, (String) value, getTextFieldType(field));
                sortedField = null;
                storedField = null;
                break;
            case STRING:
                luceneField = new StringField(fieldName, (String)value, field.isStored() ? Field.Store.YES : Field.Store.NO);
                sortedField = field.isSorted() ? new SortedDocValuesField(fieldName, new BytesRef((String)value)) : null;
                storedField = null;
                break;
            case INT:
                luceneField = new IntPoint(fieldName, (Integer)value);
                sortedField = field.isSorted() ? new NumericDocValuesField(fieldName, (Integer)value) : null;
                storedField = field.isStored() ? new StoredField(fieldName, (Integer)value) : null;
                break;
            case LONG:
                luceneField = new LongPoint(fieldName, (Long)value);
                sortedField = field.isSorted() ? new NumericDocValuesField(fieldName, (Long)value) : null;
                storedField = field.isStored() ? new StoredField(fieldName, (Long)value) : null;
                break;
            case DOUBLE:
                luceneField = new DoublePoint(fieldName, (Double)value);
                sortedField = field.isSorted() ? new NumericDocValuesField(fieldName, NumericUtils.doubleToSortableLong((Double)value)) : null;
                storedField = field.isStored() ? new StoredField(fieldName, (Double)value) : null;
                break;
            case BOOLEAN:
                byte[] bytes = Boolean.TRUE.equals(value) ? BooleanPointsConfig.TRUE_BYTES : BooleanPointsConfig.FALSE_BYTES;
                luceneField = new BinaryPoint(fieldName, bytes);
                storedField = field.isStored() ? new StoredField(fieldName, bytes) : null;
                sortedField = field.isSorted() ? new SortedDocValuesField(fieldName, new BytesRef(bytes)) : null;
                break;
            default:
                throw new RecordCoreArgumentException("Invalid type for lucene index field", "type", field.getType());
        }
        document.add(luceneField);
        if (sortedField != null) {
            document.add(sortedField);
        }
        if (storedField != null) {
            document.add(storedField);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private void writeDocument(@Nonnull List<LuceneDocumentFromRecord.DocumentField> fields,
                               Tuple groupingKey,
                               Integer partitionId,
                               Tuple primaryKey) throws IOException {
        final long startTime = System.nanoTime();
        final List<String> texts = fields.stream()
                .filter(f -> f.getType().equals(LuceneIndexExpressions.DocumentFieldType.TEXT))
                .map(f -> (String) f.getValue()).collect(Collectors.toList());
        Document document = new Document();
        final IndexWriter newWriter = directoryManager.getIndexWriter(groupingKey, partitionId, indexAnalyzerSelector.provideIndexAnalyzer(texts));

        BytesRef ref = new BytesRef(keySerializer.asPackedByteArray(primaryKey));
        // use packed Tuple for the Stored and Sorted fields
        document.add(new StoredField(PRIMARY_KEY_FIELD_NAME, ref));
        document.add(new SortedDocValuesField(PRIMARY_KEY_SEARCH_NAME, ref));
        if (keySerializer.hasFormat()) {
            try {
                // Use BinaryPoint for fast lookup of ID when enabled
                document.add(new BinaryPoint(PRIMARY_KEY_BINARY_POINT_NAME, keySerializer.asFormattedBinaryPoint(primaryKey)));
            } catch (RecordCoreFormatException ex) {
                // this can happen on format mismatch or encoding error
                // just don't write the field, but allow the document to continue
                logSerializationError("Failed to write using BinaryPoint encoded ID: {}", ex.getMessage());
            }
        }

        Map<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> indexOptionsToFieldsMap = getIndexOptionsToFieldsMap(fields);
        for (Map.Entry<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> entry : indexOptionsToFieldsMap.entrySet()) {
            for (LuceneDocumentFromRecord.DocumentField field : entry.getValue()) {
                insertField(field, document);
            }
        }
        newWriter.addDocument(document);
        state.context.record(LuceneEvents.Events.LUCENE_ADD_DOCUMENT, System.nanoTime() - startTime);
    }

    @Nonnull
    private Map<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> getIndexOptionsToFieldsMap(@Nonnull List<LuceneDocumentFromRecord.DocumentField> fields) {
        final Map<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> map = new EnumMap<>(IndexOptions.class);
        fields.stream().forEach(f -> {
            final IndexOptions indexOptions = getIndexOptions((String) Objects.requireNonNullElse(f.getConfig(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS),
                    LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name()));
            map.putIfAbsent(indexOptions, new ArrayList<>());
            map.get(indexOptions).add(f);
        });
        return map;
    }

    @SuppressWarnings({"PMD.CloseResource", "java:S2095"})
    int deleteDocument(Tuple groupingKey, Integer partitionId, Tuple primaryKey) throws IOException {
        final long startTime = System.nanoTime();
        final IndexWriter indexWriter = directoryManager.getIndexWriter(groupingKey, partitionId, indexAnalyzerSelector.provideIndexAnalyzer(""));
        @Nullable final LucenePrimaryKeySegmentIndex segmentIndex = directoryManager.getDirectory(groupingKey, partitionId).getPrimaryKeySegmentIndex();

        if (segmentIndex != null) {
            final DirectoryReader directoryReader = directoryManager.getDirectoryReader(groupingKey, partitionId);
            final LucenePrimaryKeySegmentIndex.DocumentIndexEntry documentIndexEntry = segmentIndex.findDocument(directoryReader, primaryKey);
            if (documentIndexEntry != null) {
                state.context.ensureActive().clear(documentIndexEntry.entryKey); // TODO: Only if valid?
                long valid = indexWriter.tryDeleteDocument(documentIndexEntry.indexReader, documentIndexEntry.docId);
                if (valid > 0) {
                    state.context.record(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY, System.nanoTime() - startTime);
                    return 1;
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug(KeyValueLogMessage.of("try delete document failed",
                            LuceneLogMessageKeys.GROUP, groupingKey,
                            LuceneLogMessageKeys.INDEX_PARTITION, partitionId,
                            LuceneLogMessageKeys.SEGMENT, documentIndexEntry.segmentName,
                            LuceneLogMessageKeys.DOC_ID, documentIndexEntry.docId,
                            LuceneLogMessageKeys.PRIMARY_KEY, primaryKey));
                }
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(KeyValueLogMessage.of("primary key segment index entry not found",
                        LuceneLogMessageKeys.GROUP, groupingKey,
                        LuceneLogMessageKeys.INDEX_PARTITION, partitionId,
                        LuceneLogMessageKeys.PRIMARY_KEY, primaryKey,
                        LuceneLogMessageKeys.SEGMENTS, segmentIndex.findSegments(primaryKey)));
            }
        }
        Query query;
        // null format means don't use BinaryPoint for the index primary key
        if (keySerializer.hasFormat()) {
            try {
                byte[][] binaryPoint = keySerializer.asFormattedBinaryPoint(primaryKey);
                query = BinaryPoint.newRangeQuery(PRIMARY_KEY_BINARY_POINT_NAME, binaryPoint, binaryPoint);
            } catch (RecordCoreFormatException ex) {
                // this can happen on format mismatch or encoding error
                // fallback to the old way (less efficient)
                query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(keySerializer.asPackedByteArray(primaryKey)));
                logSerializationError("Failed to delete using BinaryPoint encoded ID: {}", ex.getMessage());
            }
        } else {
            // fallback to the old way (less efficient)
            query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(keySerializer.asPackedByteArray(primaryKey)));
        }

        indexWriter.deleteDocuments(query);
        LuceneEvents.Events event = state.store.isIndexWriteOnly(state.index) ?
                                    LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY_IN_WRITE_ONLY_MODE :
                                    LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY;
        state.context.record(event, System.nanoTime() - startTime);

        // if we delete by query, we aren't certain whether the document was actually deleted (if, for instance, it wasn't in Lucene
        // to begin with)
        return 0;
    }

    @Override
    public CompletableFuture<Void> mergeIndex() {
        return rebalancePartitions()
                .thenCompose(ignored -> {
                    state.store.getIndexDeferredMaintenanceControl().setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
                    return directoryManager.mergeIndex(partitioner, indexAnalyzerSelector.provideIndexAnalyzer(""));
                });
    }

    @VisibleForTesting
    public void mergeIndexForTesting(@Nonnull final Tuple groupingKey,
                                     @Nullable final Integer partitionId,
                                     @Nonnull final AgilityContext agilityContext) throws IOException {
        directoryManager.mergeIndexWithContext(indexAnalyzerSelector.provideIndexAnalyzer(""),
                groupingKey, partitionId, agilityContext);
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
    <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                       @Nullable FDBIndexableRecord<M> newRecord,
                                                       @Nullable Integer destinationPartitionIdHint) {
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

        // delete old
        return AsyncUtil.whenAll(oldRecordFields.keySet().stream().map(t -> {
            try {
                return tryDelete(Objects.requireNonNull(oldRecord), t);
            } catch (IOException e) {
                throw LuceneExceptions.toRecordCoreException("Issue deleting", e, "record", Objects.requireNonNull(oldRecord).getPrimaryKey());
            }
        }).collect(Collectors.toList())).thenCompose(ignored ->
                // update new
                AsyncUtil.whenAll(newRecordFields.entrySet().stream().map(entry -> {
                    try {
                        return tryDeleteInWriteOnlyMode(Objects.requireNonNull(newRecord), entry.getKey()).thenCompose(countDeleted ->
                                partitioner.addToAndSavePartitionMetadata(newRecord, entry.getKey(), destinationPartitionIdHint).thenApply(partitionId -> {
                                    try {
                                        writeDocument(entry.getValue(), entry.getKey(), partitionId, newRecord.getPrimaryKey());
                                    } catch (IOException e) {
                                        throw LuceneExceptions.toRecordCoreException("Issue updating new index keys", e, "newRecord", newRecord.getPrimaryKey());
                                    }
                                    return null;
                                }));
                    } catch (IOException e) {
                        throw LuceneExceptions.toRecordCoreException("Issue updating", e, "record", Objects.requireNonNull(newRecord).getPrimaryKey());
                    }
                }).collect(Collectors.toList())));
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
     * @throws IOException propagated by {@link #tryDelete(FDBIndexableRecord, Tuple)}
     */
    private <M extends Message> CompletableFuture<Integer> tryDeleteInWriteOnlyMode(@Nonnull FDBIndexableRecord<M> record,
                                                                                    @Nonnull Tuple groupingKey) throws IOException {
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
     * @throws IOException propagated from {@link #deleteDocument(Tuple, Integer, Tuple)}
     */
    private <M extends Message> CompletableFuture<Integer> tryDelete(@Nonnull FDBIndexableRecord<M> record,
                                                                     @Nonnull Tuple groupingKey) throws IOException {
        // non-partitioned
        if (!partitioner.isPartitioningEnabled()) {
            return CompletableFuture.completedFuture(deleteDocument(groupingKey, null, record.getPrimaryKey()));
        }

        // partitioned
        return partitioner.tryGetPartitionInfo(record, groupingKey).thenApply(partitionInfo -> {
            if (partitionInfo != null) {
                try {
                    int countDeleted = deleteDocument(groupingKey, partitionInfo.getId(), record.getPrimaryKey());
                    if (countDeleted > 0) {
                        partitioner.decrementCountAndSave(groupingKey, partitionInfo, countDeleted);
                    }
                    return countDeleted;
                } catch (IOException e) {
                    throw LuceneExceptions.toRecordCoreException("Issue deleting", e, "record", record.getPrimaryKey());
                }
            }
            return 0;
        });
    }

    private FieldType getTextFieldType(LuceneDocumentFromRecord.DocumentField field) {
        FieldType ft = new FieldType();

        try {
            ft.setIndexOptions(getIndexOptions((String)Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS),
                    LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name())));
            ft.setTokenized(true);
            ft.setStored(field.isStored());
            ft.setStoreTermVectors((boolean)Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTORS), false));
            ft.setStoreTermVectorPositions((boolean)Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTOR_POSITIONS), false));
            ft.setOmitNorms(true);
            ft.freeze();
        } catch (ClassCastException ex) {
            throw new RecordCoreArgumentException("Invalid value type for Lucene field config", ex);
        }

        return ft;
    }

    private static IndexOptions getIndexOptions(@Nonnull String value) {
        try {
            return IndexOptions.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new RecordCoreArgumentException("Invalid enum value to parse for Lucene IndexOptions: " + value, ex);
        }
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        LOG.trace("scanUniquenessViolations");
        return RecordCursor.empty(executor);
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
            final IndexReader indexReader = directoryManager.getIndexReader(groupingKey, partitionId);
            final FDBDirectory directory = getDirectory(groupingKey, partitionId);
            final CompletableFuture<Integer> fieldInfosFuture = directory.getFieldInfosCount();
            return directory.listAllAsync()
                    .thenCombine(fieldInfosFuture, (fileList, fieldInfosCount) ->
                            new LuceneMetadataInfo.LuceneInfo(
                                    indexReader.numDocs(),
                                    fileList, fieldInfosCount));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @VisibleForTesting
    protected FDBDirectory getDirectory(@Nonnull Tuple groupingKey, @Nullable Integer partitionId) {
        return directoryManager.getDirectory(groupingKey, partitionId);
    }

    /**
     * Simple throttling mechanism for log messages.
     * Since the index writer may see many of these errors in quick succession, limit the number of log messages by ensuring
     * we only log once per transaction.
     * @param format the message format for the log
     * @param arguments teh message arguments
     */
    private void logSerializationError(String format, Object ... arguments) {
        if (LOG.isWarnEnabled()) {
            if (! serializerErrorLogged) {
                LOG.warn(format, arguments);
                // Not thread safe but OK as we may only log an extra message
                serializerErrorLogged = true;
            }
        }
    }
}
