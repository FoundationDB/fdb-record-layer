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
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.record.lucene.IndexWriterCommitCheckAsync.getOrCreateIndexWriter;
import static com.apple.foundationdb.record.lucene.LuceneKeyExpression.normalize;
import static com.apple.foundationdb.record.lucene.LuceneKeyExpression.validateLucene;
import static com.apple.foundationdb.record.query.expressions.LuceneQueryComponent.FULL_TEXT_KEY_FIELD;

/**
 * Index maintainer for Lucene Indexes backed by FDB.  The insert, update, and delete functionality
 * coupled with the scan functionality is implemented here.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneIndexMaintainer extends StandardIndexMaintainer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexMaintainer.class);
    private final Analyzer analyzer;
    protected static final String PRIMARY_KEY_FIELD_NAME = "p"; // TODO: Need to find reserved names..
    private static final String PRIMARY_KEY_SEARCH_NAME = "s"; // TODO: Need to find reserved names..
    private List<ImmutablePair<String, LuceneKeyExpression>> fields = new ArrayList<>(2);
    private final Executor executor;

    public LuceneIndexMaintainer(@Nonnull final IndexMaintainerState state, @Nonnull Executor executor, @Nonnull Analyzer analyzer) {
        super(state);
        this.analyzer = analyzer;
        KeyExpression rootExpression = this.state.index.getRootExpression();
        validateLucene(rootExpression);
        fields = normalize(rootExpression);
        this.executor = executor;
    }

    /**
     * The scan uses the low element in the range to execute the
     * MultiFieldQueryParser.
     *
     * @param scanType the {@link IndexScanType type} of scan to perform
     * @param range the range to scan
     * @param continuation any continuation from a previous scan invocation
     * @param scanProperties skip, limit and other properties of the scan
     * @return RecordCursor
     */
    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        LOG.trace("scan scanType={}", scanType);
        Verify.verify(range.getLow() != null);
        Verify.verify(scanType == IndexScanType.BY_LUCENE);
        try {
            final QueryParser parser = new QueryParser(FULL_TEXT_KEY_FIELD, analyzer);
            Query query = parser.parse(range.getLow().getString(0));
            return new LuceneRecordCursor(executor, scanProperties, state, query, continuation, state.index.getRootExpression().normalizeKeyForPositions());
        } catch (Exception ioe) {
            throw new RecordCoreArgumentException("Unable to parse range given for query", "range", range,
                    "internalException", ioe);
        }
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
        LOG.trace("update oldRecord={}, newRecord{}", oldRecord, newRecord);
        return super.update(oldRecord, newRecord);
    }

    @Nonnull
    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        LOG.trace("updateIndexKeys savedRecord={}, remove=={}, indexEntries={}", savedRecord, remove, indexEntries);

        if (indexEntries.isEmpty()) {
            return AsyncUtil.DONE;
        }
        try {
            final IndexWriter writer = getOrCreateIndexWriter(state, analyzer, executor);
            Document document = new Document();
            byte[] primaryKey = savedRecord.getPrimaryKey().pack();
            BytesRef ref = new BytesRef(primaryKey);
            // TODO: explore expanding this out into stored fields so they are queryable
            document.add(new StoredField(PRIMARY_KEY_FIELD_NAME, ref));
            document.add(new SortedDocValuesField(PRIMARY_KEY_SEARCH_NAME, ref));
            List<String> fullText = Lists.newArrayList();
            indexEntries.forEach( (entry ) -> {
                try {
                    if (remove) {
                        Query query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(savedRecord.getPrimaryKey().pack()));
                        writer.deleteDocuments(query);
                    } else {
                        int offset = 0;
                        Tuple entryKey = entry.getKey();
                        //TODO I think this can be simplified for Fields to be a single
                        for (int i = 0; i < fields.size(); i++) {
                            ImmutablePair<String, LuceneKeyExpression> keyPair = fields.get(i);
                            LuceneKeyExpression expression = keyPair.right;
                            String prefix = keyPair.left;
                            if (expression instanceof LuceneThenKeyExpression) {
                                int prefixLocation = ((LuceneThenKeyExpression)expression).getPrimaryKeyPosition();
                                final Object o = entryKey.get(prefixLocation + i);
                                String subPrefix = o == null ? "" : o.toString();
                                prefix = prefix == null ? subPrefix : prefix.concat("_").concat(subPrefix);
                                List<LuceneFieldKeyExpression> children = ((LuceneThenKeyExpression)expression).getLuceneChildren();
                                for (int j = 0; j < children.size(); j++) {
                                    if (j != prefixLocation) {
                                        LuceneFieldKeyExpression child = children.get(j);
                                        Object value = entryKey.get(i + offset + j);
                                        if (value != null) {
                                            insertDocumentField(child, entryKey.get(i + offset + j), document, prefix);
                                            fullText.add(value.toString());
                                        }
                                    }
                                }
                                offset += children.size() - 1;
                            } else if (expression instanceof LuceneFieldKeyExpression) {
                                Object value = entryKey.get(i + offset);
                                if (value != null) {
                                    insertDocumentField((LuceneFieldKeyExpression)expression, value, document, null);
                                    fullText.add(value.toString());
                                }
                            }
                        }
                    }
                } catch (IOException ioe) {
                    throw new RecordCoreException("Issue updating index keys", "Key", entry, "IndexEntries", indexEntries,
                            "savedRecord", savedRecord, ioe);
                }
            });
            String fullTextString = "";
            for (String value : fullText) {
                fullTextString = fullTextString.concat(" ").concat(value);
            }
            document.add(new TextField(FULL_TEXT_KEY_FIELD, fullTextString, Field.Store.NO));
            writer.addDocument(document);
        } catch (Exception e) {
            throw new RecordCoreException(e);
        }
        return AsyncUtil.DONE;
    }

    private void insertDocumentField(final LuceneFieldKeyExpression expression, Object value,
                                     final Document document, String prefix) {
        if (!(value == null && expression.getType() != LuceneKeyExpression.FieldType.STRING)) {
            String fieldName = prefix == null ? expression.getFieldName() : prefix.concat("_").concat(expression.getFieldName());
            switch (expression.getType()) {
                case INT:
                    // Todo: figure out how to expand functionality to include storage, sorting etc.
                    document.add(new IntPoint(fieldName, (Integer)value));
                    break;
                case STRING:
                    document.add(new TextField(fieldName, value == null ? "" : (String)value, expression.isStored() ? Field.Store.YES : Field.Store.NO));
                    break;
                case LONG:
                    document.add(new LongPoint(fieldName, (long) value));
                    break;
                default:
                    throw new RecordCoreArgumentException("Invalid type for lucene index field", "type", expression.getType());
            }
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
        return false;
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        LOG.trace("deleteWhere transaction={}, prefix={}", tr, prefix);
        return AsyncUtil.DONE;
    }

    @Override
    @Nonnull
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        LOG.trace("performOperation operation={}", operation);
        return CompletableFuture.completedFuture(new IndexOperationResult() {
        });
    }


}
