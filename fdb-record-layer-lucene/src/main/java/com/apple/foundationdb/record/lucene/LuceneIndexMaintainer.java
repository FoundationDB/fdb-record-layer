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
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.record.lucene.IndexWriterCommitCheckAsync.getOrCreateIndexWriter;
import static com.apple.foundationdb.record.lucene.LuceneKeyExpression.listIndexFieldNames;
import static com.apple.foundationdb.record.lucene.LuceneKeyExpression.validateLucene;

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
    private final Executor executor;

    public LuceneIndexMaintainer(@Nonnull final IndexMaintainerState state, @Nonnull Executor executor, @Nonnull Analyzer analyzer) {
        super(state);
        this.analyzer = analyzer;
        KeyExpression rootExpression = this.state.index.getRootExpression();
        validateLucene(rootExpression);
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
        Verify.verify(scanType == IndexScanType.BY_LUCENE || scanType == IndexScanType.BY_LUCENE_FULL_TEXT);
        try {
            // This cannot work with nested documents the way that we currently use them. BlockJoin will be essential for this
            // functionality in this way.
            QueryParser parser;
            if (scanType == IndexScanType.BY_LUCENE_FULL_TEXT) {
                List<String> fieldNames = listIndexFieldNames(state.index.getRootExpression());
                parser = new MultiFieldQueryParser(fieldNames.toArray(new String[fieldNames.size()]), analyzer);
                parser.setDefaultOperator(QueryParser.Operator.OR);
            } else {
                // initialize default to scan primary key.
                parser = new QueryParser(PRIMARY_KEY_SEARCH_NAME, analyzer);
            }
            Query query = parser.parse(range.getLow().getString(0));
            return new LuceneRecordCursor(executor, scanProperties, state, query, continuation, state.index.getRootExpression().normalizeKeyForPositions());
        } catch (Exception ioe) {
            throw new RecordCoreArgumentException("Unable to parse range given for query", "range", range,
                    "internalException", ioe);
        }
    }

    protected static class DocumentEntry {
        final String fieldName;
        final Object value;
        final LuceneFieldKeyExpression expression;

        DocumentEntry(String name, Object value, LuceneFieldKeyExpression expression) {
            this.fieldName = name;
            this.value = value;
            this.expression = expression;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final DocumentEntry entry = (DocumentEntry)o;
            return fieldName.equals(entry.fieldName) && Objects.equals(value, entry.value) && expression.equals(entry.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, value, expression);
        }

    }

    public <M extends Message> List<DocumentEntry> getFields(KeyExpression expression,
                                                                             FDBIndexableRecord<M> record, Message message) {
        if (expression instanceof LuceneThenKeyExpression) {
            List<DocumentEntry> evaluatedList = Lists.newArrayList();
            for (KeyExpression child : ((LuceneThenKeyExpression)expression).getChildren()) {
                evaluatedList.addAll(getFields(child, record, message));
            }
            if (((LuceneThenKeyExpression)expression).getPrimaryKey() == null) {
                return evaluatedList;
            }
            DocumentEntry primaryKey = evaluatedList.get(((LuceneThenKeyExpression)expression).getPrimaryKeyPosition());
            evaluatedList.remove(primaryKey);
            List<DocumentEntry> result = Lists.newArrayList();
            String prefix = primaryKey.value.toString().concat("_");
            for (DocumentEntry entry : evaluatedList) {
                result.add(new DocumentEntry(prefix.concat(entry.fieldName), entry.value, entry.expression));
            }
            return result;
        }
        if (expression instanceof NestingKeyExpression) {
            final List<Key.Evaluated> parentKeys = ((NestingKeyExpression)expression).getParent().evaluateMessage(record, message);
            List<DocumentEntry> result = new ArrayList<>();
            for (Key.Evaluated value : parentKeys) {
                //TODO we may have to make this act like the NestingKeyExpression logic which takes only the first in the message list
                for (DocumentEntry entry : getFields(((NestingKeyExpression)expression).getChild(), record, (Message)value.toList().get(0))) {
                    if (((NestingKeyExpression)expression).getParent().getFanType().equals(KeyExpression.FanType.None)) {
                        result.add(new DocumentEntry(((NestingKeyExpression)expression).getParent().getFieldName().concat("_").concat(entry.fieldName), entry.value, entry.expression));
                    } else {
                        result.add(entry);
                    }
                }
            }
            return result;
        }
        if (expression instanceof GroupingKeyExpression) {
            return getFields(((GroupingKeyExpression)expression).getWholeKey(), record, message);
        }
        if (expression instanceof LuceneFieldKeyExpression) {
            List<DocumentEntry> evaluatedKeys = Lists.newArrayList();
            for (Key.Evaluated key : expression.evaluateMessage(record, message)) {
                Object value = key.values().get(0);
                if (key.containsNonUniqueNull()) {
                    value = null;
                }
                evaluatedKeys.add(new DocumentEntry(((LuceneFieldKeyExpression)expression).getFieldName(), value, (LuceneFieldKeyExpression) expression));
            }
            return evaluatedKeys;
        }
        return Lists.newArrayList();
    }

    private void insertField(DocumentEntry entry, final Document document) {

        Object value = entry.value;
        String fieldName = entry.fieldName;
        LuceneFieldKeyExpression expression = entry.expression;
        switch (expression.getType()) {
            case INT:
                // Todo: figure out how to expand functionality to include storage, sorting etc.
                document.add(new IntPoint(fieldName, (Integer)value));
                break;
            case STRING:
                document.add(new TextField(fieldName, value == null ? "" : value.toString(), expression.isStored() ? Field.Store.YES : Field.Store.NO));
                break;
            case LONG:
                document.add(new LongPoint(fieldName, (long)value));
                break;
            default:
                throw new RecordCoreArgumentException("Invalid type for lucene index field", "type", entry.expression.getType());
        }
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
        LOG.trace("update oldRecord={}, newRecord{}", oldRecord, newRecord);

        List<DocumentEntry> oldRecordFields = Lists.newArrayList();
        KeyExpression root = state.index.getRootExpression();
        List<DocumentEntry> newRecordFields = Lists.newArrayList();

        // evaluate fields from records provided
        if (newRecord != null) {
            newRecordFields.addAll(getFields(root, newRecord, newRecord.getRecord()));
        }
        if (oldRecord != null) {
            oldRecordFields.addAll(getFields(root, oldRecord, oldRecord.getRecord()));
        }

        // if no relevant fields are changed then nothing needs to be done.
        if (newRecordFields.equals(oldRecordFields)) {
            return AsyncUtil.DONE;
        }

        try {
            // create writer and document
            final IndexWriter writer = getOrCreateIndexWriter(state, analyzer, executor);
            Document document = new Document();

            // if old record has fields in the index and there are changes then delete old record
            if (!oldRecordFields.isEmpty()) {
                Query query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(oldRecord.getPrimaryKey().pack()));
                writer.deleteDocuments(query);
            }

            // if there are changes then update the index with the new document.
            if (!newRecordFields.isEmpty()) {
                byte[] primaryKey = newRecord.getPrimaryKey().pack();
                BytesRef ref = new BytesRef(primaryKey);
                document.add(new StoredField(PRIMARY_KEY_FIELD_NAME, ref));
                document.add(new SortedDocValuesField(PRIMARY_KEY_SEARCH_NAME, ref));
                for (DocumentEntry entry : newRecordFields) {
                    insertField(entry, document);
                }
                writer.addDocument(document);
            }
        } catch (IOException e) {
            throw new RecordCoreException("Issue updating index keys", "oldRecord", oldRecord, "newRecord", newRecord, e);
        }
        return AsyncUtil.DONE;
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
