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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.codec.LazyCloseable;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCodec;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.CloseException;
import com.apple.foundationdb.util.CloseableUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.StandardDirectoryReaderOptimization;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Wrapper containing an {@link FDBDirectory} and cached accessor objects (like {@link IndexWriter}s). This object
 * is designed to be held by the {@link FDBDirectoryManager}, with one object cached per open directory. Because the
 * {@link FDBDirectory} contains cached information from FDB, it is important for cache coherency that all writers
 * (etc.) accessing that directory go through the same wrapper object so that they share a common cache.
 */
@API(API.Status.INTERNAL)
public class FDBDirectoryWrapper implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryWrapper.class);

    // Lucene Optimized Codec Singleton
    private static final Codec CODEC = LuceneOptimizedCodec.CODEC;
    public static final boolean USE_COMPOUND_FILE = true;

    private final IndexMaintainerState state;
    private final FDBDirectory directory;
    private final int mergeDirectoryCount;
    private final AgilityContext agilityContext;
    private final Tuple key;
    @Nonnull
    private final LuceneAnalyzerWrapper analyzerWrapper;
    /**
     * When we fetch the reader via {@link #getReader()}, if the {@link #writer} has been instantiated, we want to
     * create a NRT reader based of that, so that you can read your own writes. If we haven't interacted with the
     * writer though, we don't want to cause one to be created to do reads.
     * This field is set to {@code true} when the {@link #writer} is instantiated, to make sure that flow works
     * correctly.
     */
    private volatile boolean useWriter = false;
    /**
     * The cached {@link IndexWriter} on this directory, for any mutations. This is loaded lazily, so that read-only
     * operations do not have to go through the expensive operation of writing.
     */
    private final LazyCloseable<IndexWriter> writer;
    /**
     * A cached, unflushed NRT reader based on the {@link #writer}. The lifecycle of this is managed by this class
     * (which is different from the {@link #getReader()} result), and it is not reloaded at any point. This is
     * predominately used by the {@link com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndex} to find the
     * segments associated with documents being deleted.
     */
    private LazyCloseable<DirectoryReader> writerReader;
    /**
     * WriterReaders that were created by this instance, and should be closed once the wrapper is closed.
     * These readers should all be closed, but they may still be in use while this class is in circulation, so their
     * closure is postponed until this class' {@link #close()} call.
     */
    private Queue<LazyCloseable<DirectoryReader>> readersToClose;

    FDBDirectoryWrapper(@Nonnull final IndexMaintainerState state,
                        @Nonnull final Tuple key,
                        int mergeDirectoryCount,
                        @Nonnull final AgilityContext agilityContext,
                        final int blockCacheMaximumSize,
                        @Nonnull final LuceneAnalyzerWrapper analyzerWrapper,
                        @Nullable final Exception exceptionAtCreation) {
        this.state = state;
        this.key = key;
        this.directory = createFDBDirectory(state, key, agilityContext, blockCacheMaximumSize);
        this.agilityContext = agilityContext;
        this.mergeDirectoryCount = mergeDirectoryCount;
        this.analyzerWrapper = analyzerWrapper;
        writer = LazyCloseable.supply(() -> createIndexWriter(exceptionAtCreation));
        writerReader = LazyCloseable.supply(() -> DirectoryReader.open(writer.get()));
        readersToClose = new ConcurrentLinkedQueue<>();
        readersToClose.add(writerReader);
    }

    @VisibleForTesting
    public FDBDirectoryWrapper(@Nonnull final IndexMaintainerState state,
                               @Nonnull final FDBDirectory directory,
                               @Nonnull final Tuple key,
                               int mergeDirectoryCount,
                               @Nonnull final AgilityContext agilityContext,
                               @Nonnull final LuceneAnalyzerWrapper analyzerWrapper,
                               @Nullable final Exception exceptionAtCreation) {
        this.state = state;
        this.key = key;
        this.directory = directory;
        this.agilityContext = agilityContext;
        this.mergeDirectoryCount = mergeDirectoryCount;
        this.analyzerWrapper = analyzerWrapper;
        writer = LazyCloseable.supply(() -> createIndexWriter(exceptionAtCreation));
        writerReader = LazyCloseable.supply(() -> DirectoryReader.open(writer.get()));
        readersToClose = new ConcurrentLinkedQueue<>();
        readersToClose.add(writerReader);
    }

    @Nonnull
    private IndexWriter createIndexWriter(final Exception exceptionAtCreation) throws IOException {
        useWriter = true;
        final IndexDeferredMaintenanceControl mergeControl = this.state.store.getIndexDeferredMaintenanceControl();
        TieredMergePolicy tieredMergePolicy = new FDBTieredMergePolicy(mergeControl, this.agilityContext,
                this.state.indexSubspace, this.key, exceptionAtCreation)
                .setMaxMergedSegmentMB(this.state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_MERGE_MAX_SIZE))
                .setSegmentsPerTier(this.state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER));
        tieredMergePolicy.setNoCFSRatio(1.00);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(this.analyzerWrapper.getAnalyzer())
                .setUseCompoundFile(USE_COMPOUND_FILE)
                .setMergePolicy(tieredMergePolicy)
                .setMergeScheduler(getMergeScheduler(this.state, this.mergeDirectoryCount, this.agilityContext, this.key))
                .setCodec(CODEC)
                .setInfoStream(new LuceneLoggerInfoStream(LOGGER));

        // Merge is required when creating an index writer (do we have a better indicator for a required merge?)
        mergeControl.setMergeRequiredIndexes(this.state.index);
        return new IndexWriter(this.directory, indexWriterConfig);
    }

    @Nonnull
    protected FDBDirectory createFDBDirectory(final IndexMaintainerState state,
                                              final Tuple key,
                                              final AgilityContext agilityContext,
                                              final int blockCacheMaximumSize) {
        final Subspace subspace = state.indexSubspace.subspace(key);
        final FDBDirectorySharedCacheManager sharedCacheManager = FDBDirectorySharedCacheManager.forContext(state.context);
        final Tuple sharedCacheKey;
        if (sharedCacheManager == null) {
            sharedCacheKey = null;
        } else {
            if (sharedCacheManager.getSubspace() == null) {
                sharedCacheKey = state.store.getSubspace().unpack(subspace.pack());
            } else {
                sharedCacheKey = sharedCacheManager.getSubspace().unpack(subspace.pack());
            }
        }
        return createFDBDirectory(subspace, state.index.getOptions(), sharedCacheManager, sharedCacheKey, USE_COMPOUND_FILE, agilityContext, blockCacheMaximumSize);
    }

    protected @Nonnull FDBDirectory createFDBDirectory(final Subspace subspace,
                                                       final Map<String, String> options,
                                                       final FDBDirectorySharedCacheManager sharedCacheManager,
                                                       final Tuple sharedCacheKey,
                                                       final boolean useCompoundFile, final AgilityContext agilityContext,
                                                       final int blockCacheMaximumSize) {
        return new FDBDirectory(subspace, options, sharedCacheManager, sharedCacheKey, useCompoundFile, agilityContext, blockCacheMaximumSize);
    }

    public FDBDirectory getDirectory() {
        return directory;
    }

    /**
     * An {@link IndexReader} for searching this directory. This will wrap {@link #getWriter()} if the writer has
     * already been instantiated, but otherwise, will just create a reader against this directory.
     * This object should be closed by callers.
     */
    @SuppressWarnings("PMD.CloseResource")
    public IndexReader getReader() throws IOException {
        if (useWriter) {
            return DirectoryReader.open(writer.get());
        } else {
            return StandardDirectoryReaderOptimization.open(directory, null, null,
                    state.context.getExecutor(),
                    state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_OPEN_PARALLELISM));
        }
    }

    /**
     * Get a {@link DirectoryReader} wrapped around the {@link #getWriter()} to be able to get segments associated with
     * documents. This resource will be closed when {@code this} is closed, and should not be closed by callers
     * @param refresh if TRUE will try to refresh the reader data from the writer
     */
    @SuppressWarnings("PMD.CloseResource")
    public DirectoryReader getWriterReader(boolean refresh) throws IOException {
        if (refresh) {
            final DirectoryReader newReader = DirectoryReader.openIfChanged(writerReader.get());
            if (newReader != null) {
                // previous reader instantiated but then writer changed
                final LazyCloseable<DirectoryReader> newLazyReader = LazyCloseable.supply(() -> newReader);
                readersToClose.add(newLazyReader);
                writerReader = newLazyReader;
            }
        }
        return writerReader.get();
    }

    private static class FDBDirectorySerialMergeScheduler extends MergeScheduler {
        /**
         * This class is temporarily duplicating FDBDirectoryMergeScheduler.
         * TODO: After verified, either delete FDBDirectoryMergeScheduler or delete FDBDirectorySerialMergeScheduler.
         */
        @Nonnull
        private final IndexMaintainerState state;
        private final int mergeDirectoryCount;
        @Nonnull
        private final AgilityContext agilityContext;
        @Nonnull
        private final Tuple key;

        private FDBDirectorySerialMergeScheduler(@Nonnull IndexMaintainerState state, int mergeDirectoryCount,
                                           @Nonnull final AgilityContext agilityContext,
                                           @Nonnull final Tuple key) {
            this.state = state;
            this.mergeDirectoryCount = mergeDirectoryCount;
            this.agilityContext = agilityContext;
            this.key = key;
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
                    if (mergeSource.hasPendingMerges()) {
                        MergeUtils.logExecutingMerge(LOGGER, "Executing Merge based on probability", agilityContext, state.indexSubspace, key, trigger);
                    }
                    serialMerge(mergeSource, trigger);
                } else {
                    skipMerge(mergeSource);
                }
            } else {
                if (mergeSource.hasPendingMerges()) {
                    MergeUtils.logExecutingMerge(LOGGER, "Executing Merge", agilityContext, state.indexSubspace, key, trigger);
                }
                serialMerge(mergeSource, trigger);
            }
            state.context.record(LuceneEvents.Events.LUCENE_MERGE, System.nanoTime() - startTime);
        }

        private void skipMerge(final MergeSource mergeSource) {
            synchronized (this) {
                MergePolicy.OneMerge nextMerge = mergeSource.getNextMerge();
                while (nextMerge != null) {
                    nextMerge.setAborted();
                    mergeSource.onMergeFinished(nextMerge);
                    nextMerge = mergeSource.getNextMerge();
                }
            }
        }

        // This is copied form SerialMergeScheduler::merge, but is called as a non-synchronized function.
        void serialMerge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
            while (true) {
                MergePolicy.OneMerge merge = mergeSource.getNextMerge();
                if (merge == null) {
                    break;
                }
                mergeSource.merge(merge);
            }
        }

        @Override
        public void close() {
            // Nothing to do
        }
    }

    private static class FDBDirectoryMergeScheduler extends ConcurrentMergeScheduler {
        @Nonnull
        private final IndexMaintainerState state;
        private final int mergeDirectoryCount;
        @Nonnull
        private final AgilityContext agilityContext;
        @Nonnull
        private final Tuple key;

        private FDBDirectoryMergeScheduler(@Nonnull IndexMaintainerState state, int mergeDirectoryCount,
                                           @Nonnull final AgilityContext agilityContext,
                                           @Nonnull final Tuple key) {
            this.state = state;
            this.mergeDirectoryCount = mergeDirectoryCount;
            this.agilityContext = agilityContext;
            this.key = key;
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
                    if (mergeSource.hasPendingMerges()) {
                        MergeUtils.logExecutingMerge(LOGGER, "Executing Merge Concurrently based on probability", agilityContext, state.indexSubspace, key, trigger);
                    }
                    super.merge(mergeSource, trigger);
                } else {
                    skipMerge(mergeSource);
                }
            } else {
                if (mergeSource.hasPendingMerges()) {
                    MergeUtils.logExecutingMerge(LOGGER, "Executing Merge Concurrently", agilityContext, state.indexSubspace, key, trigger);
                }
                super.merge(mergeSource, trigger);
            }
            state.context.record(LuceneEvents.Events.LUCENE_MERGE, System.nanoTime() - startTime);
        }

        private void skipMerge(final MergeSource mergeSource) {
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

    private MergeScheduler getMergeScheduler(@Nonnull IndexMaintainerState state,
                                             int mergeDirectoryCount,
                                             @Nonnull final AgilityContext agilityContext,
                                             @Nonnull final Tuple key) {
        final Boolean useConcurrent = state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_USE_CONCURRENT_MERGE_SCHEDULER);
        return Boolean.TRUE.equals(useConcurrent) ?
               new FDBDirectoryMergeScheduler(state, mergeDirectoryCount, agilityContext, key) :
               new FDBDirectorySerialMergeScheduler(state, mergeDirectoryCount, agilityContext, key);

    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public IndexWriter getWriter() throws IOException {
        return writer.get();
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public synchronized void close() throws IOException {
        IOUtils.close(writer, directory);
        try {
            CloseableUtils.closeAll(readersToClose.toArray(new LazyCloseable<?>[0]));
        } catch (CloseException e) {
            throw new IOException(e);
        }
    }

    public void mergeIndex() throws IOException {
        getWriter().maybeMerge();
    }

    @VisibleForTesting
    public Queue<LazyCloseable<DirectoryReader>> getReadersToClose() {
        return readersToClose;
    }
}
