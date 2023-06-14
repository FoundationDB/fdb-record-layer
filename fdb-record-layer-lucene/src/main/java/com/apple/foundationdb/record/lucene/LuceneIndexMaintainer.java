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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.idformat.LuceneIndexKeySerializer;
import com.apple.foundationdb.record.lucene.idformat.RecordCoreFormatException;
import com.apple.foundationdb.record.lucene.search.BooleanPointsConfig;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
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
import org.apache.lucene.index.IndexOptions;
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
import java.util.stream.Collectors;

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
    protected static final String PRIMARY_KEY_FIELD_NAME = "_p";
    protected static final String PRIMARY_KEY_SEARCH_NAME = "_s";
    protected static final String PRIMARY_KEY_BINARY_POINT_NAME = "_b";
    private final Executor executor;

    public LuceneIndexMaintainer(@Nonnull final IndexMaintainerState state, @Nonnull Executor executor) {
        super(state);
        this.executor = executor;
        this.directoryManager = FDBDirectoryManager.getManager(state);
        final var fieldInfos = LuceneIndexExpressions.getDocumentFieldDerivations(state.index, state.store.getRecordMetaData());
        this.indexAnalyzerSelector = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.FULL_TEXT, fieldInfos);
        this.autoCompleteAnalyzerSelector = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.AUTO_COMPLETE, fieldInfos);
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
            return new LuceneRecordCursor(executor, state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_EXECUTOR_SERVICE),
                    state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE),
                    scanProperties, state, scanQuery.getQuery(), scanQuery.getSort(), continuation,
                    scanQuery.getGroupKey(), scanQuery.getLuceneQueryHighlightParameters(),
                    scanQuery.getStoredFields(), scanQuery.getStoredFieldTypes(), indexAnalyzerSelector, autoCompleteAnalyzerSelector);
        }

        if (scanType.equals(LuceneScanTypes.BY_LUCENE_SPELL_CHECK)) {
            if (continuation != null) {
                throw new RecordCoreArgumentException("Spellcheck does not currently support continuation scanning");
            }
            LuceneScanSpellCheck scanSpellcheck = (LuceneScanSpellCheck)scanBounds;
            return new LuceneSpellCheckRecordCursor(scanSpellcheck.getFields(), scanSpellcheck.getWord(),
                    executor, scanProperties, state, scanSpellcheck.getGroupKey());
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
                               Tuple primaryKey) throws IOException {
        final List<String> texts = fields.stream()
                .filter(f -> f.getType().equals(LuceneIndexExpressions.DocumentFieldType.TEXT))
                .map(f -> (String) f.getValue()).collect(Collectors.toList());
        Document document = new Document();
        final IndexWriter newWriter = directoryManager.getIndexWriter(groupingKey, indexAnalyzerSelector.provideIndexAnalyzer(texts));

        // null format string means don't use BinaryPoint for the index primary key
        String formatString = state.index.getOption(LuceneIndexOptions.PRIMARY_KEY_SERIALIZATION_FORMAT);
        LuceneIndexKeySerializer ser = LuceneIndexKeySerializer.fromStringFormat(formatString, primaryKey);
        BytesRef ref = new BytesRef(ser.asPackedByteArray());
        // use packed Tuple for the Stored and Sorted fields
        document.add(new StoredField(PRIMARY_KEY_FIELD_NAME, ref));
        document.add(new SortedDocValuesField(PRIMARY_KEY_SEARCH_NAME, ref));
        if (formatString != null) {
            try {
                // Use BinaryPoint for fast lookup of ID when enabled
                document.add(new BinaryPoint(PRIMARY_KEY_BINARY_POINT_NAME, ser.asFormattedBinaryPoint()));
            } catch (RecordCoreFormatException ex) {
                // this can happen on format mismatch or encoding error
                // just don't write the field, but allow the document to continue
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to write using BinaryPoint encoded ID: {}", ex.getMessage());
                }
            }
        }

        Map<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> indexOptionsToFieldsMap = getIndexOptionsToFieldsMap(fields);
        for (Map.Entry<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> entry : indexOptionsToFieldsMap.entrySet()) {
            for (LuceneDocumentFromRecord.DocumentField field : entry.getValue()) {
                insertField(field, document);
            }
        }
        newWriter.addDocument(document);
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

    @SuppressWarnings("PMD.CloseResource")
    private void deleteDocument(Tuple groupingKey, Tuple primaryKey) throws IOException {
        final IndexWriter oldWriter = directoryManager.getIndexWriter(groupingKey, indexAnalyzerSelector.provideIndexAnalyzer(""));
        Query query;
        String formatString = state.index.getOption(LuceneIndexOptions.PRIMARY_KEY_SERIALIZATION_FORMAT);
        LuceneIndexKeySerializer ser = LuceneIndexKeySerializer.fromStringFormat(formatString, primaryKey);
        // null format string means don't use BinaryPoint for the index primary key
        if (formatString != null) {
            try {
                byte[][] binaryPoint = ser.asFormattedBinaryPoint();
                query = BinaryPoint.newRangeQuery(PRIMARY_KEY_BINARY_POINT_NAME, binaryPoint, binaryPoint);
            } catch (RecordCoreFormatException ex) {
                // this can happen on format mismatch or encoding error
                // fallback to the old way (less efficient)
                query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(ser.asPackedByteArray()));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to delete using BinaryPoint encoded ID: {}", ex.getMessage());
                }
            }
        } else {
            // fallback to the old way (less efficient)
            query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(ser.asPackedByteArray()));
        }
        oldWriter.deleteDocuments(query);
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
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
        try {
            for (Tuple t : oldRecordFields.keySet()) {
                // oldRecord cannot be null here since in that case the oldRecordFields would have been empty
                deleteDocument(t, Objects.requireNonNull(oldRecord).getPrimaryKey());
            }
        } catch (IOException e) {
            throw new RecordCoreException("Issue deleting old index keys", "oldRecord", oldRecord, e);
        }

        // update new
        try {
            for (Map.Entry<Tuple, List<LuceneDocumentFromRecord.DocumentField>> entry : newRecordFields.entrySet()) {
                // newRecord cannot be null here since in that case newRecordFields would have been empty
                writeDocument(entry.getValue(), entry.getKey(), Objects.requireNonNull(newRecord).getPrimaryKey());
            }
        } catch (IOException e) {
            throw new RecordCoreException("Issue updating new index keys", e)
                    .addLogInfo("newRecord", newRecord);
        }

        return AsyncUtil.DONE;
    }

    private FieldType getTextFieldType(LuceneDocumentFromRecord.DocumentField field) {
        FieldType ft = new FieldType();

        try {
            ft.setIndexOptions(getIndexOptions((String) Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS),
                            LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name())));
            ft.setTokenized(true);
            ft.setStored(field.isStored());
            ft.setStoreTermVectors((boolean) Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTORS), false));
            ft.setStoreTermVectorPositions((boolean) Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTOR_POSITIONS), false));
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
        return RecordCursor.empty();
    }

    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation, @Nullable ScanProperties scanProperties) {
        LOG.trace("validateEntries");
        return RecordCursor.empty();
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
        return CompletableFuture.completedFuture(new IndexOperationResult() {
        });
    }
}
