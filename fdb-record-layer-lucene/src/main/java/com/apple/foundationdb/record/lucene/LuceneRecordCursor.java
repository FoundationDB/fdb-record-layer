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
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Ints;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
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

import static com.apple.foundationdb.record.lucene.DirectoryCommitCheckAsync.getOrCreateDirectoryCommitCheckAsync;
import static com.apple.foundationdb.record.lucene.IndexWriterCommitCheckAsync.getIndexWriterCommitCheckAsync;

/**
 * This class is a Record Cursor implementation for Lucene queries.
 *
 */
@API(API.Status.EXPERIMENTAL)
class LuceneRecordCursor implements BaseCursor<IndexEntry> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneRecordCursor.class);
    @Nonnull
    private final Executor executor;
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
    private final List<String> fieldNames;

    LuceneRecordCursor(@Nonnull Executor executor,
                       @Nonnull ScanProperties scanProperties,
                       @Nonnull final IndexMaintainerState state, Query query,
                       byte[] continuation, List<String> fieldNames) {
        this.state = state;
        this.executor = executor;
        this.limitManager = new CursorLimitManager(state.context, scanProperties);
        this.limitRemaining = scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();
        this.timer = state.context.getTimer();
        this.query = query;
        this.currentPosition = continuation == null ? 0 : Ints.fromByteArray(continuation);
        if (scanProperties.getExecuteProperties().getSkip() > 0) {
            this.currentPosition += scanProperties.getExecuteProperties().getSkip();
        }
        this.fieldNames = fieldNames;
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
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        if (limitRemaining > 0 && currentPosition < topDocs.scoreDocs.length && limitManager.tryRecordScan()) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Document document = searcher.doc(topDocs.scoreDocs[currentPosition].doc);
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
                    Tuple tuple = Tuple.fromList(fieldNames);
                    nextResult = RecordCursorResult.withNextValue(new IndexEntry(state.index,
                            tuple.addAll(Tuple.fromBytes(pk.bytes, pk.offset, pk.length)), null), continuationHelper());
                    currentPosition++;
                    return nextResult;
                } catch (Exception e) {
                    throw new RecordCoreException("Failed to get document", "currentPosition", currentPosition, e);
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
            return ByteArrayContinuation.fromNullable(Ints.toByteArray(currentPosition));
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
        IndexWriterCommitCheckAsync writerCheck = getIndexWriterCommitCheckAsync(state);
        return writerCheck == null ? DirectoryReader.open(getOrCreateDirectoryCommitCheckAsync(state).getDirectory()) : DirectoryReader.open(writerCheck.indexWriter);
    }

    private void performScan() throws IOException {
        indexReader = getIndexReader();
        searcher = new IndexSearcher(indexReader);
        topDocs = searcher.search(query, limitRemaining);
    }
}
