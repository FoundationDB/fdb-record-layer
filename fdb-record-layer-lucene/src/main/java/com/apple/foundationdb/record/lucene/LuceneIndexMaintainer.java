/*
 * LuceneIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.IteratorCursor;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene86.Lucene86Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Index maintainer for Lucene Indexes backed by FDB.  The insert, update, and delete functionality
 * coupled with the scan functionality is implemented here.
 *
 */
public class LuceneIndexMaintainer extends StandardIndexMaintainer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexMaintainer.class);
    private DirectoryCheck directoryCheck;
    private final Analyzer analyzer;
    private static final String PRIMARY_KEY_FIELD_NAME = "p"; // Need to find reserved names..
    private static final String PRIMARY_KEY_SEARCH_NAME = "s"; // Need to find reserved names..
    private final List<String> fieldNames;
    private final String writer;

    public LuceneIndexMaintainer(final IndexMaintainerState state) {
        super(state);
        this.writer = "writer$" + state.index.getName();
        String directoryName = "directory$" + state.index.getName();
        this.directoryCheck = state.context.getCommitCheck(directoryName, DirectoryCheck.class);
        if (this.directoryCheck == null) {
            this.directoryCheck = new DirectoryCheck(state.indexSubspace, state.transaction);
            this.state.context.addCommitCheck(directoryName, directoryCheck);
        }
        this.analyzer = new StandardAnalyzer();
        this.fieldNames = new ArrayList<>(2);
        this.state.index.getRootExpression()
                .normalizeKeyForPositions().forEach( (fke) -> this.fieldNames.add( ((FieldKeyExpression) fke).getFieldName()));
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        LOG.trace("scan scanType={}", scanType);
        assert range.getLow() != null;
        try {
            final IndexReader indexReader = getReader();
            final MultiFieldQueryParser parser = new MultiFieldQueryParser(fieldNames.toArray(new String[0]) , analyzer);
            Query query = parser.parse(range.getLow().getString(0));
            IndexSearcher searcher = new IndexSearcher(indexReader);
            TopDocs topDocs = searcher.search(query, scanProperties.getExecuteProperties().getScannedRecordsLimit());
            return new IteratorCursor<>(getExecutor(),
                    Arrays.stream(topDocs.scoreDocs).map(scoreDoc -> {
                        try {
                            Document document = searcher.doc(scoreDoc.doc);
                            IndexableField primaryKey = document.getField(PRIMARY_KEY_FIELD_NAME);
                            BytesRef pk = primaryKey.binaryValue();
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("document={}", document);
                                LOG.trace("primary key read={}", Tuple.fromBytes(pk.bytes, pk.offset, pk.length));
                            }
                            Tuple tuple = Tuple.fromList(fieldNames);
                            return new IndexEntry(state.index, tuple.addAll(Tuple.fromBytes(pk.bytes, pk.offset, pk.length)), null);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).iterator(), indexReader);
        } catch (Exception ioe) {
            throw new RuntimeException(ioe);
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
            final IndexWriter writer = getWriter();
            indexEntries.forEach( (entry ) -> {
                try {
                    if (remove) {
                        Query query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(savedRecord.getPrimaryKey().pack()));
                        writer.deleteDocuments(query);
                    } else {
                        Document document = new Document();
                        byte[] primaryKey = savedRecord.getPrimaryKey().pack();
                        BytesRef ref = new BytesRef(primaryKey);
                        document.add(new StoredField(PRIMARY_KEY_FIELD_NAME, ref));
                        document.add(new SortedDocValuesField(PRIMARY_KEY_SEARCH_NAME, ref));
                        for (int i = 0; i < fieldNames.size(); i++) {
                            document.add(new TextField(fieldNames.get(i), entry.getKey().getString(i), Field.Store.NO));
                        }
                        writer.addDocument(document);
                    }
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return AsyncUtil.DONE;
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        LOG.debug("scanUniquenessViolations");
        return RecordCursor.empty();
    }

    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation, @Nullable ScanProperties scanProperties) {
        LOG.debug("validateEntries");
        return RecordCursor.empty();
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        LOG.debug("canEvaluateRecordFunction() function={}", function);
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
        return false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
        LOG.debug("addedRangeWithKey primaryKey={}", primaryKey);
        return AsyncUtil.READY_FALSE;
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        LOG.debug("canDeleteWhere matcher={}", matcher);
        return true;
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        LOG.debug("deleteWhere transaction={}, prefix={}", tr, prefix);
        return AsyncUtil.DONE;
    }

    @Override
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        LOG.debug("performOperation operation={}", operation);
        return CompletableFuture.completedFuture(new IndexOperationResult() {
        });
    }

    private static class DirectoryCheck implements FDBRecordContext.CommitCheckAsync {
        private final Directory directory;

        public DirectoryCheck(Subspace subspace, Transaction txn) {
            this.directory = new FDBDirectory(subspace, txn);
        }

        @Nonnull
        @Override
        public CompletableFuture<Void> checkAsync() {
            LOG.trace("closing directory check");
            return CompletableFuture.runAsync(() -> IOUtils.closeWhileHandlingException(directory));
        }

    }

    private class WriterCheck implements FDBRecordContext.CommitCheckAsync {
        private final IndexWriter indexWriter;

        public WriterCheck() throws IOException {
            TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
            tieredMergePolicy.setMaxMergedSegmentMB(5.00);
            tieredMergePolicy.setMaxMergeAtOnceExplicit(2);
            tieredMergePolicy.setNoCFSRatio(1.00);
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setUseCompoundFile(true);
            indexWriterConfig.setMergePolicy(tieredMergePolicy);
            indexWriterConfig.setMergeScheduler(new ConcurrentMergeScheduler() {
                @Override
                protected void doMerge(final MergeSource mergeSource, final MergePolicy.OneMerge merge) throws IOException {
                    merge.segments.forEach( (segmentCommitInfo) -> LOG.debug("segmentInfo={}", segmentCommitInfo.info.name));
                    super.doMerge(mergeSource, merge);
                }
            });
            indexWriterConfig.setCodec(new Lucene86Codec(Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION));
            this.indexWriter = new IndexWriter(directoryCheck.directory, indexWriterConfig);
        }

        @Nonnull
        @Override
        public CompletableFuture<Void> checkAsync() {
            LOG.debug("closing writer check");
            return indexWriter.isOpen() ?
                   CompletableFuture.runAsync(() -> IOUtils.closeWhileHandlingException(indexWriter)) :
                   AsyncUtil.DONE;
        }

    }

    private synchronized IndexReader getReader() throws IOException {
        WriterCheck writerCheck = this.state.context.getCommitCheck(writer, WriterCheck.class);
        return writerCheck == null ? DirectoryReader.open(directoryCheck.directory) : DirectoryReader.open(writerCheck.indexWriter);
    }

    private synchronized IndexWriter getWriter() throws IOException {
        WriterCheck writerCheck = this.state.context.getCommitCheck(writer, WriterCheck.class);
        if (writerCheck == null) {
            writerCheck = new WriterCheck();
            this.state.context.addCommitCheck(writer, writerCheck);
        }
        return writerCheck.indexWriter;
    }

}
