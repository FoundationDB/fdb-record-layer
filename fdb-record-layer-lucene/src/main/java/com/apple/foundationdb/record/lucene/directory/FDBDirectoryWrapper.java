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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneConcurrency;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.codec.LazyCloseable;
import com.apple.foundationdb.record.lucene.codec.LazyOpener;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCodec;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.CursorFactory;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ItemHandler;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ThrottledRetryingIterator;
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
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
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
    private final int blockCacheMaximumSize;
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
    /**
     * The cached version of the {@link IndexWriter} that contains the replayed {@link PendingWriteQueue} entries.
     * This writer has all the queue elements replayed, so any reader created from it can find them through a query.
     * This writer is lazily initialized, and is closed once this directory wrapper is closed.
     */
    private LazyCloseable<IndexWriter> replayedQueueIndexWriter;
    /**
     * The cached context used by the {@link #replayedQueueIndexWriter}.
     * Since the {@link #replayedQueueIndexWriter} cannot write anything to disk (the queue replay is used only for the
     * query - other mechanisms will drain the queue into the index), we use a read-only {@link AgilityContext} for the
     * writer.
     * This context is lazily initialized, and is closed once this directory wrapper is closed.
     */
    private LazyCloseable<CloseableReadOnlyAgilityContext> replayedQueueContext;
    /**
     * The instance of the pending writes queue.
     */
    private LazyOpener<PendingWriteQueue> pendingWriteQueue;

    FDBDirectoryWrapper(@Nonnull final IndexMaintainerState state,
                        @Nonnull final Tuple key,
                        int mergeDirectoryCount,
                        @Nonnull final AgilityContext agilityContext,
                        final int blockCacheMaximumSize,
                        @Nonnull final LuceneAnalyzerWrapper analyzerWrapper,
                        @Nullable final Exception exceptionAtCreation) {
        this.state = state;
        this.key = key;
        this.directory = createFDBDirectory(state, key, agilityContext, null, blockCacheMaximumSize);
        this.agilityContext = agilityContext;
        this.blockCacheMaximumSize = blockCacheMaximumSize;
        this.mergeDirectoryCount = mergeDirectoryCount;
        this.analyzerWrapper = analyzerWrapper;
        writer = LazyCloseable.supply(() -> createIndexWriter(exceptionAtCreation));
        writerReader = LazyCloseable.supply(() -> DirectoryReader.open(writer.get()));
        readersToClose = new ConcurrentLinkedQueue<>();
        readersToClose.add(writerReader);
        replayedQueueContext = LazyCloseable.supply(() -> createReadOnlyContext());
        replayedQueueIndexWriter = LazyCloseable.supply(() -> createIndexWriterWithReplayedQueue());
        pendingWriteQueue = LazyOpener.supply(() -> directory.createPendingWritesQueue());
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
        this.blockCacheMaximumSize = directory.getBlockCacheMaximumSise();
        this.agilityContext = agilityContext;
        this.mergeDirectoryCount = mergeDirectoryCount;
        this.analyzerWrapper = analyzerWrapper;
        writer = LazyCloseable.supply(() -> createIndexWriter(exceptionAtCreation));
        writerReader = LazyCloseable.supply(() -> DirectoryReader.open(writer.get()));
        readersToClose = new ConcurrentLinkedQueue<>();
        readersToClose.add(writerReader);
        replayedQueueContext = LazyCloseable.supply(() -> createReadOnlyContext());
        replayedQueueIndexWriter = LazyCloseable.supply(() -> createIndexWriterWithReplayedQueue());
        pendingWriteQueue = LazyOpener.supply(() -> directory.createPendingWritesQueue());
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
                                              final @Nullable LockFactory lockFactory,
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
        return createFDBDirectory(
                subspace, state.index.getOptions(), sharedCacheManager, sharedCacheKey, USE_COMPOUND_FILE,
                agilityContext, lockFactory, blockCacheMaximumSize);
    }

    protected @Nonnull FDBDirectory createFDBDirectory(final Subspace subspace,
                                                       final Map<String, String> options,
                                                       final FDBDirectorySharedCacheManager sharedCacheManager,
                                                       final Tuple sharedCacheKey,
                                                       final boolean useCompoundFile, final AgilityContext agilityContext,
                                                       final @Nullable LockFactory lockFactory,
                                                       final int blockCacheMaximumSize) {
        return new FDBDirectory(subspace, options, sharedCacheManager, sharedCacheKey, useCompoundFile, agilityContext, lockFactory, blockCacheMaximumSize);
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
     * An {@link IndexReader} for querying this directory.
     * In the case that the writer has already been created (which means that the index has been written to), that same writer
     * will be used to create the reader, ensuring that written docs are visible to the query.
     * In the case there were no writes and the pending writes queue is empty (there are no pending writes), then the reader
     * is created from the current directory and can search all existing docs.
     * In case there are entries in the pending writes queue, they will be replayed so that they are visible to the reader,
     * and will be rolled back once the directory is closed.
     * This object should be closed by callers.
     */
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public IndexReader getIndexReaderWithReplayedQueue() throws IOException {
        if (useWriter) {
            // In case the current directory has writes, prioritize them over the pending writes queue.
            return DirectoryReader.open(writer.get());
        } else {
            PendingWriteQueue queue = getPendingWriteQueue();
            // Use the regular context to find out if the queue has anything
            final Boolean queueHasItems = LuceneConcurrency.asyncToSync(
                    LuceneEvents.Waits.WAIT_LUCENE_READ_PENDING_QUEUE,
                    queue.isQueueEmpty(state.context),
                    state.context);
            if (!queueHasItems) {
                // Use the regular reader in case there is nothing in the queue
                return StandardDirectoryReaderOptimization.open(directory, null, null,
                        state.context.getExecutor(),
                        state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_OPEN_PARALLELISM));
            } else {
                // create a reader from a writer that has the queue elements replayed
                return DirectoryReader.open(replayedQueueIndexWriter.get());
            }
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    private IndexWriter createIndexWriterWithReplayedQueue() throws IOException {
        final AgilityContext readOnlyContext = replayedQueueContext.get().getAgilityContext();
        // Create a read-only directory with read-only context and no-op lock factory
        final FDBDirectory readOnlyDirectory = createFDBDirectory(state, key, readOnlyContext, NoOpLockFactory.INSTANCE, blockCacheMaximumSize);
        // Create a minimal IndexWriterConfig for the read-only writer
        IndexWriterConfig config = new IndexWriterConfig(this.analyzerWrapper.getAnalyzer())
                .setCodec(CODEC)
                .setUseCompoundFile(USE_COMPOUND_FILE)
                .setCommitOnClose(false);
        IndexWriter readOnlyIndexWriter = new IndexWriter(readOnlyDirectory, config);

        // Get the queue and apply changes
        // This is using the read-only agility context as this is the one to be used for the query that is to be executed
        readOnlyContext.apply(context -> {
            return CompletableFuture.completedFuture(LuceneConcurrency.asyncToSync(
                    LuceneEvents.Waits.WAIT_LUCENE_REPLAY_QUEUE,
                    getPendingWriteQueue().replayQueuedOperations(context, readOnlyIndexWriter),
                    context));
            // Block until replay completes: Since the returned future is completed with the result of the asyncToSync, the
            // join() will return immediately.
        }).join();
        return readOnlyIndexWriter;
    }

    @Nonnull
    private CloseableReadOnlyAgilityContext createReadOnlyContext() {
        AgilityContext readOnlyContext = AgilityContext.readOnlyNonAgile(agilityContext.getCallerContext(), null);
        return new CloseableReadOnlyAgilityContext(readOnlyContext);
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

    /**
     * Return the {@link PendingWriteQueue} instance to use to read documents queued while the directory was locked.
     * @return the queue instance
     */
    public PendingWriteQueue getPendingWriteQueue() {
        return pendingWriteQueue.getUnchecked();
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
        // Note that we are not closing the replayedQueueIndexWriter, since it is wasteful to roll back in memory changes
        // as the actual transaction is rolled back anyway
        IOUtils.close(writer, directory, replayedQueueContext);
        try {
            CloseableUtils.closeAll(readersToClose.toArray(new LazyCloseable<?>[0]));
        } catch (CloseException e) {
            throw new IOException(e);
        }
    }

    @VisibleForTesting
    public Queue<LazyCloseable<DirectoryReader>> getReadersToClose() {
        return readersToClose;
    }

    public void setOngoingMergeIndicator() {
        getDirectory().setOngoingMergeIndicator();
    }

    public void mergeIndex() throws IOException {
        getWriter().maybeMerge();
    }

    public void clearOngoingMergeIndicator() {
        getDirectory().clearOngoingMergeIndicatorButFailIfNonEmpty();
    }

    public void drainPendingQueue(@Nonnull final Tuple groupingKey,
                                  @Nullable final Integer partitionId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("Drain pending queue",
                    LogMessageKeys.GROUPING_KEY, groupingKey,
                    LogMessageKeys.PARTITION_ID, partitionId));
        }
        final int maxRetries = 20;
        for (int retries = 0; ; retries ++) {
            // while draining the pending queue new updates may "refill" it and cause the clearance of the ongoing merge
            // indicator to fail. Hence, the draining/indicator clearing process should be retried.
            try {
                agilityContext.flush(); // before potentially long drain (Note that the drain iteration does not use agilityContext)
                drainPendingQueueNow(groupingKey, partitionId);
                clearOngoingMergeIndicator();
                agilityContext.flush(); // commit clear indicator
                return;
            } catch (RuntimeException ex) {
                if (retries >= maxRetries) {
                    throw new PendingQueueDrainException(ex);
                }
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("Failed to drain queue, retrying",
                            LogMessageKeys.RETRY_COUNT, retries,
                            LogMessageKeys.GROUPING_KEY, groupingKey,
                            LogMessageKeys.PARTITION_ID, partitionId
                    ), ex);
                }
            }
        }
    }

    private void drainPendingQueueNow(@Nonnull final Tuple groupingKey,
                                      @Nullable final Integer partitionId) {
        // Note - since this directory wrapper was already created, agility context should be unused in the next line's path
        final PendingWriteQueue writeQueue = getPendingWriteQueue();
        try (ThrottledRetryingIterator<PendingWriteQueue.QueueEntry> iterator = ThrottledRetryingIterator.builder(
                        agilityContext.getCallerContext().getDatabase(),
                        cursorFactory(writeQueue),
                        handleOneItemFactory(writeQueue, groupingKey, partitionId))
                .withMdcContext(MDC.getCopyOfContextMap())
                .withCommitWhenDone(true)
                .build()) {
            agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_DRAIN_PENDING_QUEUE,
                    iterator.iterateAll(state.store.asBuilder()));
        } catch (CloseException e) {
            throw new PendingQueueDrainException(e);
        }

    }

    private CursorFactory<PendingWriteQueue.QueueEntry> cursorFactory(PendingWriteQueue pendingWriteQueue) {
        return (@Nonnull FDBRecordStore store, @Nullable RecordCursorResult<PendingWriteQueue.QueueEntry> lastResult, int rowLimit) -> {
            byte[] continuation = lastResult == null ? null : lastResult.getContinuation().toBytes();
            ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(executeProperties -> executeProperties.setReturnedRowLimit(rowLimit));
            // Note: null could have been used instead of continuation as the preceding items should have been deleted. However,
            // using a continuation will prevent an infinite loop in case of a bug that doesn't clear items.
            return pendingWriteQueue.getQueueCursor(store.getContext(), scanProperties, continuation);
        };
    }

    private ItemHandler<PendingWriteQueue.QueueEntry> handleOneItemFactory(PendingWriteQueue pendingWriteQueue,
                                                                           @Nonnull final Tuple groupingKey,
                                                                           @Nullable final Integer partitionId) {
        return (store, lastResult, quotamanager) -> {
            final PendingWriteQueue.QueueEntry queueEntry = lastResult.get();
            if (queueEntry == null) {
                return AsyncUtil.DONE;
            }

            try {
                final LuceneIndexMaintainer maintainer = (LuceneIndexMaintainer)store.getIndexMaintainer(state.index);
                CompletableFuture<Void> oneItemFuture = AsyncUtil.DONE;
                switch (queueEntry.getOperationType()) {
                    case UPDATE:
                    case INSERT:
                        maintainer.writeDocumentBypassQueue(groupingKey, partitionId, queueEntry.getPrimaryKeyParsed(), queueEntry.getDocumentFieldsParsed());
                        break;
                    case DELETE:
                        final int countDeleted = maintainer.deleteDocumentBypassQueue(groupingKey, partitionId, queueEntry.getPrimaryKeyParsed());
                        if (partitionId != null) {
                            oneItemFuture = maintainer.postDeleteUpdatePartitionCounter(groupingKey, partitionId, countDeleted)
                                    .thenApply(ignore -> null);
                        }
                        break;
                    default:
                        throw new PendingQueueDrainException("Unknown operation type",
                                LogMessageKeys.GROUPING_KEY, groupingKey,
                                LogMessageKeys.PARTITION_ID, partitionId,
                                LogMessageKeys.CODE, queueEntry.getOperationType());
                }
                pendingWriteQueue.clearEntry(store.getContext(), queueEntry);
                return oneItemFuture;
            } catch (IOException ex) {
                throw new RecordCoreException("Lucene IOException", ex);
            }
        };
    }

    /**
     * thrown if pending queue drain had failed.
     */
    @SuppressWarnings("java:S110")
    public static class PendingQueueDrainException extends RecordCoreException {
        private static final long serialVersionUID = 10;

        public PendingQueueDrainException(final Throwable cause) {
            super("Pending queue drain had failed", cause);
        }

        public PendingQueueDrainException(@Nonnull final String msg, @Nullable final Object... keyValues) {
            super(msg, keyValues);
        }
    }


    private static class CloseableReadOnlyAgilityContext implements Closeable {
        private AgilityContext agilityContext;

        private CloseableReadOnlyAgilityContext(final AgilityContext agilityContext) {
            this.agilityContext = agilityContext;
        }

        public AgilityContext getAgilityContext() {
            return agilityContext;
        }

        @Override
        public void close() throws IOException {
            agilityContext.abortAndClose();
        }
    }
}
