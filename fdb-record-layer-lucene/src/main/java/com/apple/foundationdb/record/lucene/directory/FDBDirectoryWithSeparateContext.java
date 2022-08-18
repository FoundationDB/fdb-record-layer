/*
 * FDBDirectoryWithSeparateContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCodec;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.subspace.Subspace;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An variant of {@link FDBDirectory} that has its own transactionality and can therefore operate on larger datasets for longer.
 */
@API(API.Status.EXPERIMENTAL)
public class FDBDirectoryWithSeparateContext extends FDBDirectory implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryWithSeparateContext.class);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Deque<CompletableFuture<?>> pendingReads = new ConcurrentLinkedDeque<>();

    private long commitAfterBytesLimit;
    private long commitAfterMillisLimit;
    private long contextStartMillis;
    private int pendingBytesCount;

    @Nullable
    IndexWriter indexWriter;

    public FDBDirectoryWithSeparateContext(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context) {
        super(subspace, openLikeContext(context));
        contextStartMillis = System.currentTimeMillis();
    }

    public void setCommitAfterLimits(long commitAfterBytesLimit, long commitAfterMillisLimit) {
        this.commitAfterBytesLimit = commitAfterBytesLimit;
        this.commitAfterMillisLimit = commitAfterMillisLimit;
    }

    @Override
    public void close() {
        if (indexWriter != null) {
            try {
                indexWriter.close();
            } catch (IOException ex) {
                throw new RecordCoreException("error closing writer", ex);
            }
        }
        if (context != null) {
            context.commit();
        }
        super.close();
    }

    @Override
    protected <T> CompletableFuture<T> readTransaction(Function<ReadTransaction, CompletableFuture<T>> reader) {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            CompletableFuture<T> result = super.readTransaction(reader);
            pendingReads.removeIf(CompletableFuture::isDone);
            if (!result.isDone()) {
                pendingReads.addLast(result);
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void writeTransaction(Consumer<Transaction> writer) {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            super.writeTransaction(writer);
            if ((commitAfterBytesLimit > 0 && pendingBytesCount > commitAfterBytesLimit) ||
                    (commitAfterMillisLimit > 0 && System.currentTimeMillis() - contextStartMillis > commitAfterMillisLimit)) {
                syncTransaction();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void countWriteBytes(long count) {
        pendingBytesCount += count;
    }

    @Override
    @SuppressWarnings("PMD.EmptyCatchBlock")
    protected void syncTransaction() {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            super.syncTransaction();
            while (!pendingReads.isEmpty()) {
                CompletableFuture<?> future = pendingReads.removeFirst();
                if (!future.isDone()) {
                    try {
                        future.get();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException ex) {
                        // Swallow exception
                    }
                }
            }
            context.commit();
            openNewContext();
            contextStartMillis = System.currentTimeMillis();
            pendingBytesCount = 0;
            fileSequenceCounter.set(-1);    // Fetch again in new transaction.
        } finally {
            lock.unlock();
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public IndexWriter getIndexWriter(@Nonnull RecordLayerPropertyStorage propertyStorage, @Nonnull LuceneAnalyzerWrapper analyzerWrapper) throws IOException {
        if (indexWriter == null) {
            TieredMergePolicy tieredMergePolicy = new TieredMergePolicy()
                    .setMaxMergedSegmentMB(Math.max(0.0, propertyStorage.getPropertyValue(LuceneRecordContextProperties.LUCENE_MERGE_MAX_SIZE)));
            tieredMergePolicy.setNoCFSRatio(1.00);
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzerWrapper.getAnalyzer())
                    .setUseCompoundFile(true)
                    .setMergePolicy(tieredMergePolicy)
                    .setMergeScheduler(new ConcurrentMergeScheduler())
                    .setCodec(new LuceneOptimizedCodec())
                    .setInfoStream(new LuceneLoggerInfoStream(LOGGER));
            indexWriter = new IndexWriter(this, indexWriterConfig);
        }
        return indexWriter;
    }

    protected void openNewContext() {
        context = openLikeContext(context);
    }

    @Nonnull
    private static FDBRecordContext openLikeContext(@Nonnull FDBRecordContext context) {
        return context.getDatabase().openContext(context.getConfig());
    }
}
