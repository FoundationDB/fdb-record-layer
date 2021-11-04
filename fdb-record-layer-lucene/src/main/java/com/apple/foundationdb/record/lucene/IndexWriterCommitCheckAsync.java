/*
 * IndexWriterCommitCheckAsync.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCodec;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.record.lucene.DirectoryCommitCheckAsync.getOrCreateDirectoryCommitCheckAsync;

/**
 * This class closes the writer after commit.  The goal here is to only flush the results
 * at the end of the commit.
 */
@API(API.Status.EXPERIMENTAL)
public class IndexWriterCommitCheckAsync implements FDBRecordContext.CommitCheckAsync {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexMaintainer.class);
    protected final IndexWriter indexWriter;
    protected final Executor executor;


    /**
     * Creates an index writer config with merge configurations that limit the amount of data in a segment.
     *
     * @param analyzer analyzer
     * @param directoryCommitCheckAsync directoryCommitCheckAsync
     * @param executor executor
     * @throws IOException exception
     */
    public IndexWriterCommitCheckAsync(@Nonnull Analyzer analyzer, @Nonnull DirectoryCommitCheckAsync directoryCommitCheckAsync, Executor executor) throws IOException {
        TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
        tieredMergePolicy.setMaxMergedSegmentMB(5.00);
        tieredMergePolicy.setMaxMergeAtOnceExplicit(2);
        tieredMergePolicy.setNoCFSRatio(1.00);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        indexWriterConfig.setUseCompoundFile(true);
        indexWriterConfig.setMergePolicy(tieredMergePolicy);
        indexWriterConfig.setMergeScheduler(new ConcurrentMergeScheduler() {
            @Override
            protected void doMerge(final IndexWriter writer, final MergePolicy.OneMerge merge) throws IOException {
                merge.segments.forEach( (segmentCommitInfo) -> LOGGER.trace("segmentInfo={}", segmentCommitInfo.info.name));
                super.doMerge(writer, merge);
            }
        });
        indexWriterConfig.setCodec(new LuceneOptimizedCodec(Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION));
        this.indexWriter = new IndexWriter(directoryCommitCheckAsync.getDirectory(), indexWriterConfig);
        this.executor = executor;
    }

    /**
     * Close IndexWriter.
     *
     * @return CompletableFuture
     */
    @Nonnull
    @Override
    public CompletableFuture<Void> checkAsync() {
        LOGGER.trace("closing writer check");
        if (indexWriter.isOpen()) {
            return CompletableFuture.runAsync(() -> {
                try {
                    IOUtils.close(indexWriter);
                } catch (IOException ioe) {
                    LOGGER.error("IndexWriterCommitCheckSync Failed", ioe);
                    throw new RecordCoreStorageException("IndexWriterCommitCheckSync Failed", ioe);
                }
            },
            executor);
        } else {
            return AsyncUtil.DONE;
        }
    }

    @Nullable
    protected static IndexWriterCommitCheckAsync getIndexWriterCommitCheckAsync(@Nonnull final IndexMaintainerState state, @Nullable final Tuple groupingKey) {
        return state.context.getInSession(getWriterSubspace(state, groupingKey), IndexWriterCommitCheckAsync.class);
    }

    @Nonnull
    protected static IndexWriter getOrCreateIndexWriter(@Nonnull final IndexMaintainerState state, @Nonnull Analyzer analyzer, @Nonnull Executor executor, @Nullable final Tuple groupingKey) throws IOException {
        synchronized (state.context) {
            IndexWriterCommitCheckAsync writerCheck = getIndexWriterCommitCheckAsync(state, groupingKey);
            if (writerCheck == null) {
                writerCheck = new IndexWriterCommitCheckAsync(analyzer, getOrCreateDirectoryCommitCheckAsync(state, groupingKey), executor);
                state.context.addCommitCheck(writerCheck);
                state.context.putInSessionIfAbsent(getWriterSubspace(state, groupingKey), writerCheck);
            }
            return writerCheck.indexWriter;
        }
    }

    @Nonnull
    private static Subspace getWriterSubspace(@Nonnull final IndexMaintainerState state, @Nullable final Tuple groupingKey) {
        return state.indexSubspace.subspace(groupingKey).subspace(Tuple.from("w"));
    }

}
