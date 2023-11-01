/*
 * FDBDirectoryWrapper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCodec;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenancePolicy;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.StandardDirectoryReaderOptimization;
import org.apache.lucene.index.TieredMergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Wrapper containing an {@link FDBDirectory} and cached accessor objects (like {@link IndexWriter}s). This object
 * is designed to be held by the {@link FDBDirectoryManager}, with one object cached per open directory. Because the
 * {@link FDBDirectory} contains cached information from FDB, it is important for cache coherency that all writers
 * (etc.) accessing that directory go through the same wrapper object so that they share a common cache.
 */
class FDBDirectoryWrapper implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryWrapper.class);

    // Lucene Optimized Codec Singleton
    private static final Codec CODEC = new LuceneOptimizedCodec();

    private final IndexMaintainerState state;
    private final FDBDirectory directory;
    private final int mergeDirectoryCount;
    @SuppressWarnings({"squid:S3077"}) // object is thread safe, so use of volatile to control instance creation is correct
    private volatile IndexWriter writer;
    @SuppressWarnings({"squid:S3077"}) // object is thread safe, so use of volatile to control instance creation is correct
    private volatile String writerAnalyzerId;

    FDBDirectoryWrapper(IndexMaintainerState state, Tuple key, int mergeDirectoryCount) {
        final Subspace subspace = state.indexSubspace.subspace(key);
        final FDBDirectorySharedCacheManager sharedCacheManager = FDBDirectorySharedCacheManager.forContext(state.context);
        final Tuple sharedCacheKey = sharedCacheManager == null ? null :
                                     (sharedCacheManager.getSubspace() == null ? state.store.getSubspace() : sharedCacheManager.getSubspace()).unpack(subspace.pack());

        this.state = state;
        this.directory = new FDBDirectory(subspace, state.context, sharedCacheManager, sharedCacheKey,
                state.index.getBooleanOption(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, false));
        this.mergeDirectoryCount = mergeDirectoryCount;
    }

    public FDBDirectory getDirectory() {
        return directory;
    }

    @SuppressWarnings("PMD.CloseResource")
    public IndexReader getReader() throws IOException {
        IndexWriter indexWriter = writer;
        if (writer == null) {
            return StandardDirectoryReaderOptimization.open(directory, null, null,
                    state.context.getExecutor(),
                    state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_OPEN_PARALLELISM));
        } else {
            return DirectoryReader.open(indexWriter);
        }
    }

    private static class FDBDirectoryMergeScheduler extends ConcurrentMergeScheduler {
        @Nonnull
        private final IndexMaintainerState state;
        private final int mergeDirectoryCount;

        private FDBDirectoryMergeScheduler(@Nonnull IndexMaintainerState state, int mergeDirectoryCount) {
            this.state = state;
            this.mergeDirectoryCount = mergeDirectoryCount;
        }

        @SuppressWarnings({
                "squid:S2245", // ThreadLocalRandom not used in case where cryptographic security is needed
                "squid:S3776", // Cognitive complexity is too high. Candidate for later refactoring
        })
        @Override
        public synchronized void merge(final MergeSource mergeSource, final MergeTrigger trigger) throws IOException {
            long startTime = System.nanoTime();
            if (state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_MULTIPLE_MERGE_OPTIMIZATION_ENABLED) && trigger == MergeTrigger.FULL_FLUSH) {
                if (ThreadLocalRandom.current().nextInt(mergeDirectoryCount) == 0) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(FDBDirectoryManager.getMergeLogMessage(mergeSource, trigger, state, "Basic Lucene index merge based on probability"));
                    }
                    super.merge(mergeSource, trigger);
                } else {
                    skipMerge(mergeSource, trigger, "probability optimization");
                }
            } else {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(FDBDirectoryManager.getMergeLogMessage(mergeSource, trigger, state, "Basic Lucene index merge"));
                }
                super.merge(mergeSource, trigger);
            }
            state.context.record(LuceneEvents.Events.LUCENE_MERGE, System.nanoTime() - startTime);
        }

        private void skipMerge(final MergeSource mergeSource, final MergeTrigger trigger, String reason) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(FDBDirectoryManager.getMergeLogMessage(mergeSource, trigger, state, "Lucene index skipped. Reason: " + reason));
            }
            synchronized (this) {
                MergePolicy.OneMerge nextMerge = mergeSource.getNextMerge();
                while (nextMerge != null) {
                    nextMerge.setAborted();
                    mergeSource.onMergeFinished(nextMerge);
                    nextMerge = mergeSource.getNextMerge();
                }
            }
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public IndexWriter getWriter(LuceneAnalyzerWrapper analyzerWrapper) throws IOException {
        if (writer == null || !writerAnalyzerId.equals(analyzerWrapper.getUniqueIdentifier())) {
            synchronized (this) {
                if (writer == null || !writerAnalyzerId.equals(analyzerWrapper.getUniqueIdentifier())) {
                    final IndexDeferredMaintenancePolicy deferredMergePolicy = state.store.getIndexDeferredMaintenancePolicy();
                    TieredMergePolicy tieredMergePolicy = new FDBTieredMergePolicy(deferredMergePolicy.shouldAutoMergeDuringCommit(), state.context)
                            .setMaxMergedSegmentMB(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_MERGE_MAX_SIZE))
                            .setSegmentsPerTier(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER));
                    tieredMergePolicy.setNoCFSRatio(1.00);
                    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzerWrapper.getAnalyzer())
                            .setUseCompoundFile(true)
                            .setMergePolicy(tieredMergePolicy)
                            .setMergeScheduler(new FDBDirectoryMergeScheduler(state, mergeDirectoryCount))
                            .setCodec(CODEC)
                            .setInfoStream(new LuceneLoggerInfoStream(LOGGER));

                    IndexWriter oldWriter = writer;
                    if (oldWriter != null) {
                        oldWriter.close();
                    }
                    writer = new IndexWriter(directory, indexWriterConfig);
                    writerAnalyzerId = analyzerWrapper.getUniqueIdentifier();
                    // Merge is required when creating an index writer (do we have a better indicator for a required merge?)
                    deferredMergePolicy.setMergeRequiredIndexes(state.index);
                }
            }
        }
        return writer;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public synchronized void close() throws IOException {
        IndexWriter indexWriter = writer;
        if (indexWriter != null) {
            indexWriter.close();
            writer = null;
            writerAnalyzerId = null;
        }
        directory.close();
    }

    public void mergeIndex(LuceneAnalyzerWrapper analyzerWrapper) throws IOException {
        getWriter(analyzerWrapper).maybeMerge();
    }
}
