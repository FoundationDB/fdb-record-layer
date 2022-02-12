/*
 * LuceneRecordCursor.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.apple.foundationdb.record.lucene.DirectoryCommitCheckAsync.getOrCreateDirectoryCommitCheckAsync;
import static com.apple.foundationdb.record.lucene.IndexWriterCommitCheckAsync.getIndexWriterCommitCheckAsync;

/**
 * This class is a Record Cursor implementation for Lucene queries.
 *
 */
@API(API.Status.EXPERIMENTAL)
class LuceneRecordCursor implements BaseCursor<IndexEntry> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneRecordCursor.class);
    // pagination within single instance of record cursor for lucene queries.
    private static final int MAX_PAGE_SIZE = 201;
    @Nonnull
    private final Executor executor;
    @Nullable
    private final ExecutorService executorService;
    @Nonnull
    private final CursorLimitManager limitManager;
    @Nullable
    private final FDBStoreTimer timer;
    private int limitRemaining;
    @Nullable
    private RecordCursorResult<IndexEntry> nextResult;
    final IndexMaintainerState state;
    private IndexReader indexReader;
    private final Query query;
    private IndexSearcher searcher;
    private TopDocs topDocs;
    private int currentPosition;
    private final List<KeyExpression> fields;
    private Sort sort = null;
    private ScoreDoc searchAfter = null;
    private boolean exhausted = false;
    @Nullable
    private final Tuple groupingKey;

    //TODO: once we fix the available fields logic for lucene to take into account which fields are
    // stored there should be no need to pass in a list of fields, or we could only pass in the store field values.
    LuceneRecordCursor(@Nonnull Executor executor,
                       @Nullable ExecutorService executorService,
                       @Nonnull ScanProperties scanProperties,
                       @Nonnull final IndexMaintainerState state, Query query,
                       byte[] continuation,
                       @Nullable Tuple groupingKey) {
        this.state = state;
        this.executor = executor;
        this.executorService = executorService;
        this.limitManager = new CursorLimitManager(state.context, scanProperties);
        this.limitRemaining = scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();
        this.timer = state.context.getTimer();
        this.query = query;
        if (continuation != null) {
            try {
                LuceneContinuationProto.LuceneIndexContinuation luceneIndexContinuation = LuceneContinuationProto.LuceneIndexContinuation.parseFrom(continuation);
                searchAfter = new ScoreDoc((int)luceneIndexContinuation.getDoc(), luceneIndexContinuation.getScore(), (int)luceneIndexContinuation.getShard());
            } catch (Exception e) {
                throw new RecordCoreException("Invalid continuation for Lucene index", "ContinuationValues", continuation, e);
            }
        }
        this.currentPosition = 0;
        if (scanProperties.getExecuteProperties().getSkip() > 0) {
            this.currentPosition += scanProperties.getExecuteProperties().getSkip();
        }
        this.fields = state.index.getRootExpression().normalizeKeyForPositions();
        this.groupingKey = groupingKey;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            // Like the KeyValueCursor, it is necessary to memoize and return the first result where
            // hasNext is false to avoid the NoNextReason changing.
            return CompletableFuture.completedFuture(nextResult);
        }
        if (topDocs == null) {
            try {
                performScan();
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }
        if (topDocs.scoreDocs.length - 1 < currentPosition && limitRemaining > 0 && !exhausted) {
            try {
                performScan();
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
            currentPosition = Math.max(currentPosition - MAX_PAGE_SIZE, 0);
        }
        if (limitRemaining > 0 && currentPosition < topDocs.scoreDocs.length && limitManager.tryRecordScan()) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Document document = searcher.doc(topDocs.scoreDocs[currentPosition].doc);
                    searchAfter = topDocs.scoreDocs[currentPosition];
                    IndexableField primaryKey = document.getField(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME);
                    BytesRef pk = primaryKey.binaryValue();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("document={}", document);
                        LOGGER.trace("primary key read={}", Tuple.fromBytes(pk.bytes, pk.offset, pk.length));
                    }
                    if (timer != null) {
                        timer.increment(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY);
                    }
                    if (limitRemaining != Integer.MAX_VALUE) {
                        limitRemaining--;
                    }
                    Tuple setPrimaryKey = Tuple.fromBytes(pk.bytes);
                    // Initialized with values that aren't really legal in a Tuple to find offset bugs.
                    List<Object> fieldValues = Lists.newArrayList(fields);
                    if (groupingKey != null) {
                        // Grouping keys are at the front.
                        for (int i = 0; i < groupingKey.size(); i++) {
                            fieldValues.set(i, groupingKey.get(i));
                        }
                    }
                    int[] keyPos = state.index.getPrimaryKeyComponentPositions();
                    Tuple tuple;
                    if (keyPos != null) {
                        List<Object> leftovers = Lists.newArrayList();
                        for (int i = 0; i < keyPos.length; i++) {
                            if (keyPos[i] > -1) {
                                fieldValues.set(keyPos[i], setPrimaryKey.get(i));
                            } else {
                                leftovers.add(setPrimaryKey.get(i));
                            }
                        }
                        tuple = Tuple.fromList(fieldValues).addAll(leftovers);
                    } else {
                        tuple = Tuple.fromList(fieldValues).addAll(setPrimaryKey);
                    }

                    nextResult = RecordCursorResult.withNextValue(new IndexEntry(state.index,
                            tuple, null), continuationHelper());
                    currentPosition++;
                    return nextResult;
                } catch (Exception e) {
                    throw new RecordCoreException("Failed to get document", "currentPosition", currentPosition, "exception", e);
                }
            }, executor);
        } else { // a limit was exceeded
            if (limitRemaining <= 0) {
                nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), NoNextReason.RETURN_LIMIT_REACHED);
            } else if (currentPosition >= topDocs.scoreDocs.length) {
                nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), NoNextReason.SOURCE_EXHAUSTED);
            } else {
                final Optional<NoNextReason> stoppedReason = limitManager.getStoppedReason();
                if (!stoppedReason.isPresent()) {
                    throw new RecordCoreException("limit manager stopped LuceneRecordCursor but did not report a reason");
                } else {
                    nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), stoppedReason.get());
                }
            }
            return CompletableFuture.completedFuture(nextResult);
        }
    }

    @Nonnull
    private RecordCursorContinuation continuationHelper() {
        if (currentPosition >= topDocs.scoreDocs.length && limitRemaining > 0) {
            return ByteArrayContinuation.fromNullable(null);
        } else {
            LuceneContinuationProto.LuceneIndexContinuation continuation = LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(searchAfter.doc)
                    .setScore(searchAfter.score)
                    .setShard(searchAfter.shardIndex)
                    .build();
            return ByteArrayContinuation.fromNullable(continuation.toByteArray());
        }
    }

    @Override
    public void close() {
        if (indexReader != null) {
            IOUtils.closeWhileHandlingException(indexReader);
        }
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    private synchronized IndexReader getIndexReader() throws IOException {
        IndexWriterCommitCheckAsync writerCheck = getIndexWriterCommitCheckAsync(state, groupingKey);
        return writerCheck == null ? DirectoryReader.open(getOrCreateDirectoryCommitCheckAsync(state, groupingKey).getDirectory()) : DirectoryReader.open(writerCheck.indexWriter);
    }

    private void performScan() throws IOException {
        long startTime = System.nanoTime();
        indexReader = getIndexReader();
        searcher = new IndexSearcher(indexReader, executorService);
        int limit = Math.min(limitRemaining, MAX_PAGE_SIZE);
        TopDocs newTopDocs;
        if (searchAfter != null && sort != null) {
            newTopDocs = searcher.searchAfter(searchAfter, query, limit, sort);
        } else if (searchAfter != null) {
            newTopDocs =  searcher.searchAfter(searchAfter, query, limit);
        } else if (sort != null) {
            newTopDocs = searcher.search(query,  limit, sort);
        } else {
            newTopDocs = searcher.search(query, limit);
        }
        if (newTopDocs.scoreDocs.length < limit) {
            exhausted = true;
        }
        topDocs = newTopDocs;
        if (topDocs.scoreDocs.length != 0) {
            searchAfter = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
        }
        if (timer != null) {
            timer.recordSinceNanoTime(LuceneEvents.Events.LUCENE_INDEX_SCAN, startTime);
            timer.increment(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS, topDocs.scoreDocs.length);
        }
    }
}
