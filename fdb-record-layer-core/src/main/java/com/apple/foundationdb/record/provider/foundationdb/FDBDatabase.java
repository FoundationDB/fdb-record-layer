/*
 * FDBDatabase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.AsyncLoadingCache;
import com.apple.foundationdb.record.LoggableTimeoutException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedValue;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.provider.foundationdb.storestate.PassThroughRecordStoreStateCache;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A known FDB {@link Database}, associated with a cluster file location.
 *
 * <p>
 * All reads and writes to the database are transactional: an open {@link FDBRecordContext} is needed.
 * An {@code FDBDatabase} is needed to open an {@code FDBRecordContext}.
 * </p>
 *
 * <pre><code>
 * final FDBDatabase fdb = FDBDatabaseFactory.instance().getDatabase();
 * try (FDBRecordContext ctx = fdb.openContext()) {
 *     ...
 * }
 * </code></pre>
 *
 * @see FDBDatabaseFactory
 */
@API(API.Status.STABLE)
public class FDBDatabase {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabase.class);

    /**
     * Text of message that is logged or exception that is thrown when a blocking API call
     * (<code>asyncToSync()</code>, {@link #join(CompletableFuture)}, or {@link #get(CompletableFuture)}) is
     * called from within a <code>CompletableFuture</code> completion state.
     */
    protected static final String BLOCKING_IN_ASYNC_CONTEXT_MESSAGE = "Blocking in an asynchronous context";

    /**
     * Message that is logged when it is detected that a blocking call is being made in a method that may be producing
     * a future (specifically a method that ends in "<code>Async</code>").
     */
    protected static final String BLOCKING_RETURNING_ASYNC_MESSAGE = "Blocking in future producing call";

    /**
     * Message that is logged when one joins on a future and expects it to be completed, but it is not yet done.
     */
    protected static final String BLOCKING_FOR_FUTURE_MESSAGE = "Blocking on a future that should be completed";

    @Nonnull
    private final FDBDatabaseFactory factory;
    @Nullable
    private final String clusterFile;
    /* Null until openFDB is called. */
    @Nullable
    private Database database;
    @Nullable
    private Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> asyncToSyncTimeout;
    @Nonnull
    private ExceptionMapper asyncToSyncExceptionMapper;
    @Nonnull
    private AsyncLoadingCache<LocatableResolver, ResolverStateProto.State> resolverStateCache;
    @Nonnull
    private Cache<ScopedValue<String>, ResolverResult> directoryCache;
    // Version that the current directory cache was initialized with. A version counter is kept in the directory layer
    // state. Major changes to the directory layer will increment the version stored in the database, when that version
    // moves past directoryCacheVersion we invalidate the current directoryCache and update directoryCacheVersion.
    @Nonnull
    private AtomicInteger directoryCacheVersion = new AtomicInteger();
    @Nonnull
    private Cache<ScopedValue<Long>, String> reverseDirectoryInMemoryCache;
    private boolean opened;
    private final Object reverseDirectoryCacheLock = new Object();
    private volatile FDBReverseDirectoryCache reverseDirectoryCache;
    private final int reverseDirectoryMaxRowsPerTransaction;
    private final long reverseDirectoryMaxMillisPerTransaction;
    @Nonnull
    private FDBRecordStoreStateCache storeStateCache = PassThroughRecordStoreStateCache.instance();
    private final Supplier<Boolean> transactionIsTracedSupplier;
    private final long warnAndCloseOpenContextsAfterSeconds;
    // The number of cache entries to maintain in memory
    public static final int DEFAULT_MAX_REVERSE_CACHE_ENTRIES = 5000;
    // public for javadoc purposes
    public static final int DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS = 30;

    private boolean trackLastSeenVersionOnRead = false;
    private boolean trackLastSeenVersionOnCommit = false;

    @Nonnull
    private final Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier;

    @Nonnull
    private final Function<FDBLatencySource, Long> latencyInjector;

    private String datacenterId;

    @Nonnull
    private FDBLocalityProvider localityProvider;

    @Nonnull
    private static ImmutablePair<Long, Long> initialVersionPair = new ImmutablePair<>(null, null);
    @Nonnull
    private AtomicReference<ImmutablePair<Long, Long>> lastSeenFDBVersion = new AtomicReference<>(initialVersionPair);

    private final NavigableMap<Long, FDBRecordContext> trackedOpenContexts = new ConcurrentSkipListMap<>();

    @VisibleForTesting
    public FDBDatabase(@Nonnull FDBDatabaseFactory factory, @Nullable String clusterFile) {
        this.factory = factory;
        this.clusterFile = clusterFile;
        this.asyncToSyncExceptionMapper = (ex, ev) -> FDBExceptions.wrapException(ex);
        this.reverseDirectoryMaxRowsPerTransaction = factory.getReverseDirectoryRowsPerTransaction();
        this.reverseDirectoryMaxMillisPerTransaction = factory.getReverseDirectoryMaxMillisPerTransaction();
        this.transactionIsTracedSupplier = factory.getTransactionIsTracedSupplier();
        this.warnAndCloseOpenContextsAfterSeconds = factory.getWarnAndCloseOpenContextsAfterSeconds();
        this.blockingInAsyncDetectionSupplier = factory.getBlockingInAsyncDetectionSupplier();
        this.reverseDirectoryInMemoryCache = CacheBuilder.newBuilder()
                .maximumSize(DEFAULT_MAX_REVERSE_CACHE_ENTRIES)
                .recordStats()
                .build();
        this.directoryCache = CacheBuilder.newBuilder()
                .maximumSize(factory.getDirectoryCacheSize())
                .recordStats()
                .build();
        this.resolverStateCache = new AsyncLoadingCache<>(factory.getStateRefreshTimeMillis());
        this.latencyInjector = factory.getLatencyInjector();
        this.datacenterId = factory.getDatacenterId();
        this.localityProvider = factory.getLocalityProvider();
    }

    /**
     * Function for mapping an underlying exception to a synchronous failure.
     *
     * It is possible for this function to be called with the result of calling it previously.
     * Therefore, if wrapping exceptions with some application-specific exception class, it is best
     * to check for being passed an {@code ex} that is already of that class and in that case just return it.
     * @see #setAsyncToSyncExceptionMapper
     * @see #asyncToSync
     * @see FDBExceptions#wrapException(Throwable)
     */
    @FunctionalInterface
    public interface ExceptionMapper {
        RuntimeException apply(@Nonnull Throwable ex, @Nullable FDBStoreTimer.Event event);
    }

    protected synchronized void openFDB() {
        if (!opened) {
            final FDB fdb = factory.initFDB();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Opening FDB", LogMessageKeys.CLUSTER, clusterFile));
            }
            database = fdb.open(clusterFile);
            setDirectoryCacheSize(factory.getDirectoryCacheSize());
            opened = true;
        }
    }

    public synchronized void setDirectoryCacheSize(int size) {
        int maxSize = (size > 0) ? size : 0;
        directoryCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(maxSize)
                .build();
    }

    public synchronized void setDatacenterId(String datacenterId) {
        this.datacenterId = datacenterId;
        database().options().setDatacenterId(datacenterId);
    }

    public synchronized String getDatacenterId() {
        return datacenterId;
    }

    /**
     * Get the locality provider that is used to discover the server location of the keys.
     * @return the locality provider
     */
    @Nonnull
    public synchronized FDBLocalityProvider getLocalityProvider() {
        return localityProvider;
    }

    public synchronized void setTrackLastSeenVersionOnRead(boolean trackLastSeenVersion) {
        this.trackLastSeenVersionOnRead = trackLastSeenVersion;
    }

    public synchronized boolean isTrackLastSeenVersionOnRead() {
        return trackLastSeenVersionOnRead;
    }

    public synchronized void setTrackLastSeenVersionOnCommit(boolean trackLastSeenVersion) {
        this.trackLastSeenVersionOnCommit = trackLastSeenVersion;
    }

    public synchronized boolean isTrackLastSeenVersionOnCommit() {
        return trackLastSeenVersionOnCommit;
    }

    public synchronized void setTrackLastSeenVersion(boolean trackLastSeenVersion) {
        this.trackLastSeenVersionOnRead = trackLastSeenVersion;
        this.trackLastSeenVersionOnCommit = trackLastSeenVersion;
    }

    public synchronized boolean isTrackLastSeenVersion() {
        return trackLastSeenVersionOnRead || trackLastSeenVersionOnCommit;
    }

    /**
     * Get the path to the cluster file that this database was created with. May return <code>null</code> if using the
     * default cluster file.
     * @return The path to the cluster file.
     */
    @Nullable
    public String getClusterFile() {
        return clusterFile;
    }

    /**
     * Get the factory that produced this database.
     * @return the database factory
     */
    @Nonnull
    protected FDBDatabaseFactory getFactory() {
        return factory;
    }

    /**
     * Get the underlying FDB database.
     * @return the FDB database
     */
    @Nonnull
    public Database database() {
        openFDB();
        return database;
    }

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     * @return a new record context
     * @see Database#createTransaction
     */
    @Nonnull
    public FDBRecordContext openContext() {
        return openContext(null, null);
    }

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the logger context includes "uuid" as a key, the value associated with that key will be associated
     * with the returned transaction as its debug identifier. See
     * {@link #openContext(Map, FDBStoreTimer, WeakReadSemantics, FDBTransactionPriority, String)} for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @return a new record context
     * @see Database#createTransaction
     */
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer) {
        return openContext(mdcContext, timer, null);
    }

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the logger context includes "uuid" as a key, the value associated with that key will be associated
     * with the returned transaction as its debug identifier. See
     * {@link #openContext(Map, FDBStoreTimer, WeakReadSemantics, FDBTransactionPriority, String)} for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @return a new record context
     * @see Database#createTransaction()
     */
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer,
                                        @Nullable WeakReadSemantics weakReadSemantics) {
        return openContext(mdcContext, timer, weakReadSemantics, FDBTransactionPriority.DEFAULT);
    }

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the logger context includes "uuid" as a key, the value associated with that key will be associated
     * with the returned transaction as its debug identifier. See
     * {@link #openContext(Map, FDBStoreTimer, WeakReadSemantics, FDBTransactionPriority, String)} for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @param priority the priority of the transaction being created
     * @return a new record context
     * @see Database#createTransaction
     */
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer,
                                        @Nullable WeakReadSemantics weakReadSemantics,
                                        @Nonnull FDBTransactionPriority priority) {
        return openContext(mdcContext, timer, weakReadSemantics, priority, null);
    }


    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the passed {@code transactionId} is {@code null}, then this method will set the transaction ID
     * of the given transaction to the value of the "uuid" key in the MDC context if present. The transaction ID
     * should typically consist solely of printable ASCII characters and should not exceed 100 bytes. The ID may
     * be truncated or dropped if the ID will not fit in 100 bytes. See {@link FDBRecordContext#getTransactionId()}
     * for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @param priority the priority of the transaction being created
     * @param transactionId the transaction ID to associate with this transaction
     * @return a new record context
     * @see Database#createTransaction
     */
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer,
                                        @Nullable WeakReadSemantics weakReadSemantics,
                                        @Nonnull FDBTransactionPriority priority,
                                        @Nullable String transactionId) {
        FDBRecordContextConfig contextConfig = FDBRecordContextConfig.newBuilder()
                .setMdcContext(mdcContext)
                .setTimer(timer)
                .setWeakReadSemantics(weakReadSemantics)
                .setPriority(priority)
                .setTransactionId(transactionId)
                .build();
        return openContext(contextConfig);
    }

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database. Various
     * options on the transaction will be informed based on the passed context.
     *
     * @param contextConfig a configuration object specifying various options on the returned context
     * @return a new record context
     * @see Database#createTransaction
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public FDBRecordContext openContext(@Nonnull FDBRecordContextConfig contextConfig) {
        openFDB();
        final Executor executor = newContextExecutor(contextConfig.getMdcContext());
        final Transaction transaction = createTransaction(contextConfig, executor);

        // TODO: Compatibility with STABLE API.
        if (transactionIsTracedSupplier.get()) {
            contextConfig = contextConfig.toBuilder()
                    .setTrackOpen(true)
                    .setLogTransaction(true)
                    .setSaveOpenStackTrace(true)
                    .build();
        }

        FDBRecordContext context = new FDBRecordContext(this, transaction, contextConfig);
        final WeakReadSemantics weakReadSemantics = context.getWeakReadSemantics();
        if (isTrackLastSeenVersion() && (weakReadSemantics != null)) {
            Pair<Long, Long> pair = lastSeenFDBVersion.get();
            if (pair != initialVersionPair) {
                long version = pair.getLeft();
                long versionTimeMillis = pair.getRight();
                // If the following condition holds, a subsequent getReadVersion (on this transaction) returns version,
                // otherwise getReadVersion does not use the cached value and results in a GRV call to FDB
                if (version >= weakReadSemantics.getMinVersion() &&
                        (System.currentTimeMillis() - versionTimeMillis) <= weakReadSemantics.getStalenessBoundMillis()) {
                    context.setReadVersion(version);
                    context.increment(FDBStoreTimer.Counts.SET_READ_VERSION_TO_LAST_SEEN);
                }
            }
        }

        if (warnAndCloseOpenContextsAfterSeconds > 0) {
            warnAndCloseOldTrackedOpenContexts(warnAndCloseOpenContextsAfterSeconds);
        }
        if (contextConfig.isTrackOpen()) {
            trackOpenContext(context);
        }

        return context;
    }

    private void logNoOpFailure(@Nonnull Throwable err) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error(KeyValueLogMessage.of("unable to perform no-op operation against fdb",
                    LogMessageKeys.CLUSTER, getClusterFile()), err);
        }
    }

    /**
     * Perform a no-op against FDB to check network thread liveness. See {@link #performNoOp(Map, FDBStoreTimer)}
     * for more information. This will use the default MDC for running threads and will not instrument the
     * operation.
     *
     * @return a future that will complete after being run by the FDB network thread
     * @see #performNoOp(Map, FDBStoreTimer)
     */
    @Nonnull
    public CompletableFuture<Void> performNoOpAsync() {
        return performNoOpAsync(null);
    }

    /**
     * Perform a no-op against FDB to check network thread liveness. See {@link #performNoOp(Map, FDBStoreTimer)}
     * for more information. This will use the default MDC for running threads.
     *
     * @param timer the timer to use for instrumentation
     * @return a future that will complete after being run by the FDB network thread
     * @see #performNoOp(Map, FDBStoreTimer)
     */
    @Nonnull
    public CompletableFuture<Void> performNoOpAsync(@Nullable FDBStoreTimer timer) {
        return performNoOpAsync(null, timer);
    }

    /**
     * Perform a no-op against FDB to check network thread liveness. This operation will not change the underlying data
     * in any way, nor will it perform any I/O against the FDB cluster. However, it will schedule some amount of work
     * onto the FDB client and wait for it to complete. The FoundationDB client operates by scheduling onto an event
     * queue that is then processed by a single thread (the "network thread"). This method can be used to determine if
     * the network thread has entered a state where it is no longer processing requests or if its time to process
     * requests has increased. If the network thread is busy, this operation may take some amount of time to complete,
     * which is why this operation returns a future.
     *
     * <p>
     * If the provided {@link FDBStoreTimer} is not {@code null}, then this will update the {@link FDBStoreTimer.Events#PERFORM_NO_OP}
     * operation with related timing information. This can then be monitored to detect instances of client saturation
     * where the performance bottleneck lies in scheduling work to run on the FDB event queue.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @return a future that will complete after being run by the FDB network thread
     */
    @Nonnull
    public CompletableFuture<Void> performNoOpAsync(@Nullable Map<String, String> mdcContext,
                                                    @Nullable FDBStoreTimer timer) {
        final FDBRecordContext context = openContext(mdcContext, timer);
        boolean futureStarted = false;

        try {
            // Set the read version of the transaction, then read it back. This requires no I/O, but it does
            // require the network thread be running. The exact value used for the read version is unimportant.
            // Note that this calls setReadVersion and getReadVersion on the Transaction object, *not* on the
            // FDBRecordContext. This is because the FDBRecordContext will cache the value of setReadVersion to
            // avoid having to go back to the FDB network thread, but we do not want that for instrumentation.
            final Transaction tr = context.ensureActive();
            final long startTime = System.nanoTime();
            tr.setReadVersion(1066L);
            CompletableFuture<Long> future = tr.getReadVersion();
            if (timer != null) {
                future = context.instrument(FDBStoreTimer.Events.PERFORM_NO_OP, future, startTime);
            }
            futureStarted = true;
            return future.thenAccept(ignore -> { }).whenComplete((vignore, err) -> {
                context.close();
                if (err != null) {
                    logNoOpFailure(err);
                }
            });
        } catch (RuntimeException e) {
            logNoOpFailure(e);
            CompletableFuture<Void> errFuture = new CompletableFuture<>();
            errFuture.completeExceptionally(e);
            return errFuture;
        } finally {
            if (!futureStarted) {
                context.close();
            }
        }
    }

    /**
     * Perform a no-op against FDB to check network thread liveness. This is a blocking version of {@link #performNoOpAsync()}.
     * Note that the future is expected to complete relatively quickly in most circumstances, but it may take a long
     * time if the network thread has been saturated, e.g., if there is a lot of work being scheduled to run in FDB.
     *
     * @see #performNoOpAsync()
     */
    public void performNoOp() {
        performNoOp(null);
    }

    /**
     * Perform a no-op against FDB to check network thread liveness. This is a blocking version of {@link #performNoOpAsync(FDBStoreTimer)}.
     * Note that the future is expected to complete relatively quickly in most circumstances, but it may take a long
     * time if the network thread has been saturated, e.g., if there is a lot of work being scheduled to run in FDB.
     *
     * @param timer the timer to use for instrumentation
     * @see #performNoOpAsync(FDBStoreTimer)
     */
    public void performNoOp(@Nullable FDBStoreTimer timer) {
        performNoOp(null, timer);
    }

    /**
     * Perform a no-op against FDB to check network thread liveness. This is a blocking version of {@link #performNoOpAsync(Map, FDBStoreTimer)}.
     * Note that the future is expected to complete relatively quickly in most circumstances, but it may take a long
     * time if the network thread has been saturated, e.g., if there is a lot of work being scheduled to run in FDB.
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @see #performNoOpAsync(Map, FDBStoreTimer)
     */
    public void performNoOp(@Nullable Map<String, String> mdcContext, @Nullable FDBStoreTimer timer) {
        asyncToSync(timer, FDBStoreTimer.Waits.WAIT_PERFORM_NO_OP, performNoOpAsync(mdcContext, timer));
    }

    private long versionTimeEstimate(long startMillis) {
        return startMillis + (System.currentTimeMillis() - startMillis) / 2;
    }

    public long getResolverStateCacheRefreshTime() {
        return resolverStateCache.getRefreshTimeSeconds();
    }

    @VisibleForTesting
    public void setResolverStateRefreshTimeMillis(long resolverStateRefreshTimeMillis) {
        resolverStateCache.clear();
        resolverStateCache = new AsyncLoadingCache<>(resolverStateRefreshTimeMillis);
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    public CompletableFuture<ResolverStateProto.State> getStateForResolver(@Nonnull LocatableResolver resolver,
                                                                           @Nonnull Supplier<CompletableFuture<ResolverStateProto.State>> loader) {
        return resolverStateCache.orElseGet(resolver, loader);
    }

    /**
     * Get the read version (GRV) for the given context.
     * An explicit get read version is no more expensive than the implicit one that every operation will entail.
     * Measuring it explicitly gives an indication of the cluster's GRV latency, which is driven by its rate keeping.
     * @param context transaction to use to access the database
     * @return a future that will be completed with the read version of the current transaction
     * @deprecated use {@link FDBRecordContext#getReadVersionAsync()} instead
     */
    @Deprecated
    @Nonnull
    public CompletableFuture<Long> getReadVersion(@Nonnull FDBRecordContext context) {
        return context.getReadVersionAsync();
    }

    // Update lastSeenFDBVersion if readVersion is newer
    @API(API.Status.INTERNAL)
    public void updateLastSeenFDBVersion(long startTime, long readVersion) {
        lastSeenFDBVersion.updateAndGet(pair ->
                (pair.getLeft() == null || readVersion > pair.getLeft()) ?
                        new ImmutablePair<>(readVersion, versionTimeEstimate(startTime)) : pair);
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    public FDBReverseDirectoryCache getReverseDirectoryCache() {
        if (reverseDirectoryCache == null) {
            synchronized (reverseDirectoryCacheLock) {
                if (reverseDirectoryCache == null) {
                    reverseDirectoryCache = new FDBReverseDirectoryCache(
                            this,
                            reverseDirectoryMaxRowsPerTransaction,
                            reverseDirectoryMaxMillisPerTransaction);
                }
            }
        }
        return reverseDirectoryCache;
    }

    private void setDirectoryCacheVersion(int version) {
        directoryCacheVersion.set(version);
    }

    @API(API.Status.INTERNAL)
    public int getDirectoryCacheVersion() {
        return directoryCacheVersion.get();
    }

    public CacheStats getDirectoryCacheStats() {
        return directoryCache.stats();
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    public Cache<ScopedValue<String>, ResolverResult> getDirectoryCache(int atVersion) {
        if (atVersion > getDirectoryCacheVersion()) {
            synchronized (this) {
                if (atVersion > getDirectoryCacheVersion()) {
                    directoryCache = CacheBuilder.newBuilder()
                            .recordStats()
                            .maximumSize(factory.getDirectoryCacheSize())
                            .build();
                    setDirectoryCacheVersion(atVersion);
                }
            }
        }
        return directoryCache;
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    public Cache<ScopedValue<Long>, String> getReverseDirectoryInMemoryCache() {
        return reverseDirectoryInMemoryCache;
    }

    @API(API.Status.INTERNAL)
    public void clearForwardDirectoryCache() {
        directoryCache.invalidateAll();
    }

    @VisibleForTesting
    @API(API.Status.INTERNAL)
    public void clearReverseDirectoryCache() {
        synchronized (reverseDirectoryCacheLock) {
            reverseDirectoryCache = null;
            reverseDirectoryInMemoryCache.invalidateAll();
        }
    }

    /**
     * Get the store state cache for this database. This cache will be used when initializing record stores associated
     * with this database.
     *
     * @return the store state cache for this database
     * @see FDBRecordStoreStateCache
     */
    @Nonnull
    public FDBRecordStoreStateCache getStoreStateCache() {
        return storeStateCache;
    }

    /**
     * Set the store state cache for this database. The provided cache will be used when initializing record stores
     * with this database. Note that the store state cache should <em>not</em> be set with a store state cache
     * that is used by a different database.
     *
     * @param storeStateCache the store state cache
     */
    public void setStoreStateCache(@Nonnull FDBRecordStoreStateCache storeStateCache) {
        storeStateCache.validateDatabase(this);
        this.storeStateCache = storeStateCache;
    }

    @VisibleForTesting
    @API(API.Status.INTERNAL)
    public void clearCaches() {
        resolverStateCache.clear();
        clearForwardDirectoryCache();
        clearReverseDirectoryCache();
        storeStateCache.clear();
    }

    public synchronized void close() {
        if (opened) {
            database.close();
            database = null;
            opened = false;
            directoryCacheVersion.set(0);
            clearCaches();
            reverseDirectoryInMemoryCache.invalidateAll();
        }
    }

    @Nonnull
    public Executor getExecutor() {
        return factory.getExecutor();
    }

    protected Executor newContextExecutor(@Nullable Map<String, String> mdcContext) {
        return factory.newContextExecutor(mdcContext);
    }

    /**
     * Creates a new transaction against the database.
     *
     * @param executor the executor to be used for asynchronous operations
     * @param mdcContext if not [@code null} and tracing is enabled, information in the context will be included
     *      in tracing log messages
     * @param transactionIsTraced unused
     * @return newly created transaction
     * @deprecated use {@link #openContext()} instead
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    public Transaction createTransaction(Executor executor, @Nullable Map<String, String> mdcContext, boolean transactionIsTraced) {
        return createTransaction(
                FDBRecordContextConfig.newBuilder()
                        .setMdcContext(mdcContext)
                        .build(),
                executor);
    }

    /**
     * Creates a new transaction against the database.
     *
     * @param executor the executor to be used for asynchronous operations
     * @return newly created transaction
     */
    private Transaction createTransaction(@Nonnull FDBRecordContextConfig config, @Nonnull Executor executor) {
        Transaction transaction = database.createTransaction(executor);

        if (config.getTimer() != null) {
            transaction = new InstrumentedTransaction(config.getTimer(), transaction, config.areAssertionsEnabled());
        } else if (config.areAssertionsEnabled()) {
            transaction = new InstrumentedTransaction(null, transaction, true);
        }

        return transaction;
    }

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     * Changes made to {@code contextConfigBuilder} subsequently will continue to be reflected in contexts opened
     * by the runner.
     * @param contextConfigBuilder options for contexts opened by the new runner
     * @return a new runner
     */
    @Nonnull
    public FDBDatabaseRunner newRunner(@Nonnull FDBRecordContextConfig.Builder contextConfigBuilder) {
        return new FDBDatabaseRunnerImpl(this, contextConfigBuilder);
    }

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     * @return a new runner
     */
    @Nonnull
    public FDBDatabaseRunner newRunner() {
        return newRunner(FDBRecordContextConfig.newBuilder());
    }

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @return a new runner
     */
    @Nonnull
    public FDBDatabaseRunner newRunner(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext) {
        return newRunner(FDBRecordContextConfig.newBuilder().setTimer(timer).setMdcContext(mdcContext));
    }

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @return a new runner
     */
    @Nonnull
    public FDBDatabaseRunner newRunner(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                       @Nullable WeakReadSemantics weakReadSemantics) {
        return newRunner(FDBRecordContextConfig.newBuilder().setTimer(timer).setMdcContext(mdcContext).setWeakReadSemantics(weakReadSemantics));
    }

    /**
     * Runs a transactional function against this <code>FDBDatabase</code> with retry logic.
     *
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return result of function after successful run and commit
     * @see #newRunner()
     * @see FDBDatabaseRunner#run
     */
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        try (FDBDatabaseRunner runner = newRunner()) {
            return runner.run(retriable);
        }
    }

    /**
     * Runs a transactional function against this <code>FDBDatabase</code> with retry logic.
     *
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return result of function after successful run and commit
     * @see #newRunner(FDBStoreTimer, Map)
     * @see FDBDatabaseRunner#run
     */
    public <T> T run(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                     @Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        try (FDBDatabaseRunner runner = newRunner(timer, mdcContext)) {
            return runner.run(retriable);
        }
    }

    /**
     * Runs a transactional function against this <code>FDBDatabase</code> with retry logic.
     * This will run the function and commit the transaction associated with it until the
     * function either completes successfully or encounters a non-retriable error. An error
     * is considered retriable if it is a {@link RecordCoreRetriableTransactionException},
     * a retriable {@link FDBException}, or is an exception caused by a retriable error.
     * The function will not be run more than the number of times specified by
     * {@link FDBDatabaseFactory#getMaxAttempts() FDBDatabaseFactory.getMaxAttempts()}.
     * It also important that the function provided is idempotent as the function
     * may be applied multiple times successfully if the transaction commit returns
     * a <code>commit_unknown_result</code> error.
     *
     * <p>
     * If this <code>FDBDatabase</code> is configured to cache read versions, one
     * can specify that this function should use the cached version by supplying
     * a non-<code>null</code> {@link WeakReadSemantics} object to the
     * <code>weakReadSemantics</code> parameter. Each time that the function is
     * retried, the cached read version is checked again, so each retry
     * might get different read versions.
     * </p>
     *
     * <p>
     * This is a blocking call, and this function will not return until the database
     * has synchronously returned a response as to the success or failure of this
     * operation. If one wishes to achieve the same functionality in a non-blocking
     * manner, see {@link #runAsync(FDBStoreTimer, Map, WeakReadSemantics, Function) runAsync()}.
     * </p>
     *
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param weakReadSemantics allowable staleness parameters if caching read versions
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return result of function after successful run and commit
     * @see #newRunner(FDBStoreTimer, Map, WeakReadSemantics)
     * @see FDBDatabaseRunner#run
     */
    public <T> T run(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext, @Nullable WeakReadSemantics weakReadSemantics,
                     @Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        try (FDBDatabaseRunner runner = newRunner(timer, mdcContext, weakReadSemantics)) {
            return runner.run(retriable);
        }
    }

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     *
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return future that will contain the result of function after successful run and commit
     * @see #newRunner()
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.MAINTAINED)
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        final FDBDatabaseRunner runner = newRunner();
        return runner.runAsync(retriable).whenComplete((t, e) -> runner.close());
    }

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     *
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return future that will contain the result of function after successful run and commit
     * @see #newRunner(FDBStoreTimer, Map)
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.MAINTAINED)
    public <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                             @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        final FDBDatabaseRunner runner = newRunner(timer, mdcContext);
        return runner.runAsync(retriable).whenComplete((t, e) -> runner.close());
    }

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     * This will run the function and commit the transaction associated with it until the
     * function either completes successfully or encounters a non-retriable error. An error
     * is considered retriable if it is a {@link RecordCoreRetriableTransactionException},
     * a retriable {@link FDBException}, or is an exception caused by a retriable error.
     * The function will not be run more than the number of times specified by
     * {@link FDBDatabaseFactory#getMaxAttempts() FDBDatabaseFactory.getMaxAttempts()}.
     * It also important that the function provided is idempotent as the function
     * may be applied multiple times successfully if the transaction commit returns
     * a <code>commit_uknown_result</code> error.
     *
     * <p>
     * If this <code>FDBDatabase</code> is configured to cache read versions, one
     * can specify that this function should use the cached version by supplying
     * a non-<code>null</code> {@link WeakReadSemantics} object to the
     * <code>weakReadSemantics</code> parameter. Each time that the function is
     * retried, the cached read version is checked again, so each retry
     * might get different read versions.
     * </p>
     *
     * <p>
     * This is a non-blocking call, and this function will return immediately with
     * a future that will not be ready until the database has returned a response as
     * to the success or failure of this operation. If one wishes to achieve the same
     * functionality in a blocking manner, see {@link #run(FDBStoreTimer, Map, WeakReadSemantics, Function) run()}.
     * </p>
     *
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return future that will contain the result of function after successful run and commit
     * @see #newRunner(FDBStoreTimer, Map, WeakReadSemantics)
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.MAINTAINED)
    public <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext, @Nullable WeakReadSemantics weakReadSemantics,
                                             @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        final FDBDatabaseRunner runner = newRunner(timer, mdcContext, weakReadSemantics);
        return runner.runAsync(retriable).whenComplete((t, e) -> runner.close());
    }

    public boolean hasAsyncToSyncTimeout() {
        return asyncToSyncTimeout != null;
    }

    @Nullable
    public Pair<Long, TimeUnit> getAsyncToSyncTimeout(FDBStoreTimer.Wait event) {
        if (asyncToSyncTimeout == null) {
            return null;
        } else {
            return asyncToSyncTimeout.apply(event);
        }
    }

    @Nullable
    public Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> getAsyncToSyncTimeout() {
        return asyncToSyncTimeout;
    }

    public void setAsyncToSyncTimeout(@Nullable Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> asyncToSyncTimeout) {
        this.asyncToSyncTimeout = asyncToSyncTimeout;
    }

    public void setAsyncToSyncTimeout(long asyncToSyncTimeout, @Nonnull TimeUnit asyncToSyncTimeoutUnit) {
        setAsyncToSyncTimeout(event -> new ImmutablePair<>(asyncToSyncTimeout, asyncToSyncTimeoutUnit));
    }

    public void clearAsyncToSyncTimeout() {
        asyncToSyncTimeout = null;
    }

    public void setAsyncToSyncExceptionMapper(@Nonnull ExceptionMapper asyncToSyncExceptionMapper) {
        this.asyncToSyncExceptionMapper = asyncToSyncExceptionMapper;
    }

    protected RuntimeException mapAsyncToSyncException(@Nonnull Throwable ex) {
        return asyncToSyncExceptionMapper.apply(ex, null);
    }

    @Nullable
    public <T> T asyncToSync(@Nullable FDBStoreTimer timer, FDBStoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        checkIfBlockingInFuture(async);
        if (async.isDone()) {
            try {
                return async.get();
            } catch (ExecutionException ex) {
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw asyncToSyncExceptionMapper.apply(ex, event);
            }
        } else {

            final Pair<Long, TimeUnit> timeout = getAsyncToSyncTimeout(event);
            final long startTime = System.nanoTime();
            try {
                if (timeout != null) {
                    return async.get(timeout.getLeft(), timeout.getRight());
                } else {
                    return async.get();
                }
            } catch (TimeoutException ex) {
                if (timer != null) {
                    timer.recordTimeout(event, startTime);
                    throw asyncToSyncExceptionMapper.apply(new LoggableTimeoutException(ex, LogMessageKeys.TIME_LIMIT.toString(), timeout.getLeft(), LogMessageKeys.TIME_UNIT.toString(), timeout.getRight()), event);
                }
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } catch (ExecutionException ex) {
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } finally {
                if (timer != null) {
                    timer.recordSinceNanoTime(event, startTime);
                }
            }
        }
    }

    /**
     * Join a future, following the same logic that <code>asyncToSync()</code> uses to validate that the operation
     * isn't blocking in an asynchronous context.
     *
     * @param future the future to be completed
     * @param <T> the type of the value produced by the future
     * @return the result value
     */
    public <T> T join(CompletableFuture<T> future) {
        checkIfBlockingInFuture(future);
        return future.join();
    }

    /**
     * Join a future but validate that the future is already completed. This can be used to unwrap a completed
     * future while allowing for bugs caused by inadvertently waiting on incomplete futures to be caught.
     * In particular, this will throw an exception if the {@link BlockingInAsyncDetection} behavior is set
     * to throw an exception on incomplete futures and otherwise just log that future was waited on.
     *
     * @param future the future that should already be completed
     * @param <T> the type of the value produced by the future
     * @return the result value
     */
    public <T> T joinNow(CompletableFuture<T> future) {
        if (future.isDone()) {
            return future.join();
        }
        final BlockingInAsyncDetection behavior = getBlockingInAsyncDetection();
        final StackTraceElement caller = Thread.currentThread().getStackTrace()[1];
        logOrThrowBlockingInAsync(behavior, false, caller, BLOCKING_FOR_FUTURE_MESSAGE);

        // If the behavior only logs, we still need to block to return a value.
        // If the behavior throws an exception, we won't reach here.
        return future.join();
    }

    /**
     * Get a future, following the same logic that <code>asyncToSync()</code> uses to validate that the operation
     * isn't blocking in an asynchronous context.
     *
     * @param future the future to be completed
     * @param <T> the type of the value produced by the future
     * @return the result value
     *
     * @throws java.util.concurrent.CancellationException if the future was cancelled
     * @throws ExecutionException if the future completed exceptionally
     * @throws InterruptedException if the current thread was interrupted
     */
    public <T> T get(CompletableFuture<T> future) throws InterruptedException, ExecutionException {
        checkIfBlockingInFuture(future);
        return future.get();
    }

    /**
     * Log warning and close tracked contexts that have been open for too long.
     * @param minAgeSeconds number of seconds above which to warn
     * @return number of such contexts found
     */
    @VisibleForTesting
    public int warnAndCloseOldTrackedOpenContexts(long minAgeSeconds) {
        long nanoTime = System.nanoTime() - TimeUnit.SECONDS.toNanos(minAgeSeconds);
        if (trackedOpenContexts.isEmpty()) {
            return 0;
        }
        try {
            if (trackedOpenContexts.firstKey() > nanoTime) {
                return 0;
            }
        } catch (NoSuchElementException ex) {
            return 0;
        }
        int count = 0;
        for (FDBRecordContext context : trackedOpenContexts.headMap(nanoTime, true).values()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("context not closed",
                    LogMessageKeys.AGE_SECONDS, TimeUnit.NANOSECONDS.toSeconds(nanoTime - context.getTrackOpenTimeNanos()),
                    LogMessageKeys.TRANSACTION_ID, context.getTransactionId());
            if (context.getOpenStackTrace() != null) {
                LOGGER.warn(msg.toString(), context.getOpenStackTrace());
            } else {
                LOGGER.warn(msg.toString());
            }
            context.closeTransaction(true);
            count++;
        }
        return count;
    }

    protected void trackOpenContext(FDBRecordContext context) {
        long key = System.nanoTime();
        while (key == 0 || trackedOpenContexts.putIfAbsent(key, context) != null) {
            key++;  // Might not have nanosecond resolution and need something non-zero and unique.
        }
        context.setTrackOpenTimeNanos(key);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    protected void untrackOpenContext(FDBRecordContext context) {
        FDBRecordContext found = trackedOpenContexts.remove(context.getTrackOpenTimeNanos());
        if (found != context) {
            throw new RecordCoreException("tracked context does not match");
        }
    }

    @API(API.Status.INTERNAL)
    public BlockingInAsyncDetection getBlockingInAsyncDetection() {
        return blockingInAsyncDetectionSupplier.get();
    }

    /**
     * Given a specific FDB API call, computes an amount of latency (in milliseconds) that should be injected
     * prior to performing the actual operation.  This latency is computed via an installed latency injector
     * via {@link FDBDatabaseFactory#setLatencyInjector(Function)}.
     *
     * @param fdbLatencySource the call for which the latency is to be computed
     * @return the amount of delay, in milliseconds, to inject for the provided operation
     */
    protected long getLatencyToInject(FDBLatencySource fdbLatencySource) {
        return latencyInjector.apply(fdbLatencySource);
    }

    private void checkIfBlockingInFuture(CompletableFuture<?> future) {
        if (1 != 2)
            return;
        BlockingInAsyncDetection behavior = getBlockingInAsyncDetection();
        if (behavior == BlockingInAsyncDetection.DISABLED) {
            return;
        }

        final boolean isComplete = future.isDone();
        if (isComplete && behavior.ignoreComplete()) {
            return;
        }

        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();

        // If, during our traversal of the stack looking for blocking calls, we discover that one of our
        // callers may have been a method that was producing a CompletableFuture (as indicated by a method name
        // ending in "Async"), we will keep track of where this happened. What this may indicate is that some
        // poor, otherwise well-intentioned individual, may be doing something like:
        //
        // @Nonnull
        // public CompletableFuture<Void> doSomethingAsync(@Nonnull FDBRecordStore store) {
        //    Message record = store.loadRecord(Tuple.from(1066L));
        //    return AsyncUtil.DONE;
        // }
        //
        // There are possibly legitimate situations in which this might occur, but those are probably rare, so
        // we will simply log the fact that this has taken place and where it has taken place here.
        StackTraceElement possiblyAsyncReturningLocation = null;

        for (StackTraceElement stackElement : stack) {
            if (stackElement.getClassName().startsWith(CompletableFuture.class.getName())) {
                logOrThrowBlockingInAsync(behavior, isComplete, stackElement, BLOCKING_IN_ASYNC_CONTEXT_MESSAGE);
            } else if (stackElement.getMethodName().endsWith("Async")) {
                possiblyAsyncReturningLocation = stackElement;
            }
        }

        if (possiblyAsyncReturningLocation != null && !isComplete) {
            // Maybe one day this will be configurable, but for now we will only allow this situation to log
            logOrThrowBlockingInAsync(BlockingInAsyncDetection.IGNORE_COMPLETE_WARN_BLOCKING, isComplete,
                    possiblyAsyncReturningLocation, BLOCKING_RETURNING_ASYNC_MESSAGE);
        }
    }

    private void logOrThrowBlockingInAsync(@Nonnull BlockingInAsyncDetection behavior,
                                           boolean isComplete,
                                           @Nonnull StackTraceElement stackElement,
                                           @Nonnull String title) {
        final RuntimeException exception = new BlockingInAsyncException(title)
                .addLogInfo(
                        LogMessageKeys.FUTURE_COMPLETED, isComplete,
                        LogMessageKeys.CALLING_CLASS, stackElement.getClassName(),
                        LogMessageKeys.CALLING_METHOD, stackElement.getMethodName(),
                        LogMessageKeys.CALLING_LINE, stackElement.getLineNumber());

        if (!isComplete && behavior.throwExceptionOnBlocking()) {
            throw exception;
        } else {
            LOGGER.warn(KeyValueLogMessage.of(title,
                    LogMessageKeys.FUTURE_COMPLETED, isComplete,
                    LogMessageKeys.CALLING_CLASS, stackElement.getClassName(),
                    LogMessageKeys.CALLING_METHOD, stackElement.getMethodName(),
                    LogMessageKeys.CALLING_LINE, stackElement.getLineNumber()),
                    exception);
        }
    }

    public CompletableFuture<Tuple> loadBoundaryKeys(@Nonnull FDBTransactionContext context, Tuple key) {
        CompletableFuture<Tuple> result = context.ensureActive().get(key.pack())
                .thenApply(bytes -> bytes == null ? null : Tuple.fromBytes(bytes));
        return context.instrument(FDBStoreTimer.Events.LOAD_BOUNDARY_KEYS, result);
    }

    /**
     * 1. Bounds for stale reads; and 2. indication whether FDB's strict causal consistency guarantee isn't required.
     *
     * <p>
     * Stale reads never cause inconsistency <em>within</em> the database. If something had changed beforehand but
     * was not seen, the commit will conflict, while it would have succeeded if the read has been current.
     * </p>
     *
     * <p>
     * It is still possible to have inconsistency <em>external</em> to the database. For example, if something writes
     * a record and then sends you the id of that record via some means other than in the same database, you might
     * receive it but then access the database from before it committed and miss what they wanted you to see.
     * </p>
     *
     * <p>
     * Setting the causal read risky flag leads to a lower latency at the cost of weaker semantics in case of failures.
     * The read version of the transaction will be a committed version, and usually will be the latest committed,
     * but it might be an older version in the event of a fault on network partition.
     * </p>
     */
    public static class WeakReadSemantics {
        // Minimum version at which the read should be performed (usually the last version seen by the client)
        private long minVersion;

        // How stale a cached read version can be
        private long stalenessBoundMillis;

        // Whether the transaction should be set with a causal read risky flag.
        private boolean isCausalReadRisky;

        @Deprecated
        public WeakReadSemantics(long minVersion, long stalenessBoundMillis) {
            this(minVersion, stalenessBoundMillis, false);
        }

        public WeakReadSemantics(long minVersion, long stalenessBoundMillis, boolean isCausalReadRisky) {
            this.minVersion = minVersion;
            this.stalenessBoundMillis = stalenessBoundMillis;
            this.isCausalReadRisky = isCausalReadRisky;
        }

        public long getMinVersion() {
            return minVersion;
        }

        public long getStalenessBoundMillis() {
            return stalenessBoundMillis;
        }

        public boolean isCausalReadRisky() {
            return isCausalReadRisky;
        }
    }
}
