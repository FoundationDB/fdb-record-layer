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

import com.apple.foundationdb.API;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.record.AsyncLoadingCache;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedValue;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
     * (<code>asyncToSync(FDBStoreTimer, FDBStoreTimer.Wait, CompletableFuture)</code>, {@link #join(CompletableFuture)},
     * or {@link #get(CompletableFuture)}) is called from within a <code>CompletableFuture</code> completion state.
     */
    protected static final String BLOCKING_IN_ASYNC_CONTEXT_MESSAGE = "Blocking in an asynchronous context";

    /**
     * Message that logged when it is detected that a blocking call is being made in a method that may be producing
     * a future (specifically a method that ends in "<code>Async</code>".
     */
    protected static final String BLOCKING_RETURNING_ASYNC_MESSAGE = "Blocking in future producing call";

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
    private final Supplier<Boolean> transactionIsTracedSupplier;
    /// The number of cache entries to maintain in memory
    public static final int DEFAULT_MAX_REVERSE_CACHE_ENTRIES = 5000;
    // public for javadoc purposes
    public static final int DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS = 30;

    private boolean trackLastSeenVersionOnRead = false;
    private boolean trackLastSeenVersionOnCommit = false;

    @Nonnull
    private final Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier;

    private String datacenterId;

    @Nonnull
    private static ImmutablePair<Long, Long> initialVersionPair = new ImmutablePair<>(null, null);
    @Nonnull
    private AtomicReference<ImmutablePair<Long, Long>> lastSeenFDBVersion = new AtomicReference<>(initialVersionPair);

    @VisibleForTesting
    public FDBDatabase(@Nonnull FDBDatabaseFactory factory, @Nullable String clusterFile) {
        this.factory = factory;
        this.clusterFile = clusterFile;
        this.asyncToSyncExceptionMapper = (ex, ev) -> FDBExceptions.wrapException(ex);
        this.reverseDirectoryMaxRowsPerTransaction = factory.getReverseDirectoryRowsPerTransaction();
        this.reverseDirectoryMaxMillisPerTransaction = factory.getReverseDirectoryMaxMillisPerTransaction();
        this.transactionIsTracedSupplier = factory.getTransactionIsTracedSupplier();
        this.blockingInAsyncDetectionSupplier = factory.getBlockingInAsyncDetectionSupplier();
        this.reverseDirectoryInMemoryCache = CacheBuilder.newBuilder()
                .maximumSize(DEFAULT_MAX_REVERSE_CACHE_ENTRIES)
                .recordStats()
                .build();
        this.directoryCache = CacheBuilder.newBuilder()
                .maximumSize(0)
                .recordStats()
                .build();
        this.resolverStateCache = new AsyncLoadingCache<>(factory.getStateRefreshTimeMillis());
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
                LOGGER.debug(KeyValueLogMessage.of("Opening FDB", "cluster", clusterFile));
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
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
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
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @return a new record context
     * @see Database#createTransaction
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer,
                                        @Nullable WeakReadSemantics weakReadSemantics) {
        openFDB();
        FDBRecordContext context = new FDBRecordContext(this, mdcContext, transactionIsTracedSupplier.get(), weakReadSemantics);
        if (timer != null) {
            context.setTimer(timer);
            timer.increment(FDBStoreTimer.Counts.OPEN_CONTEXT);
        }
        if (isTrackLastSeenVersion() && (weakReadSemantics != null)) {
            Pair<Long, Long> pair = lastSeenFDBVersion.get();
            if (pair != initialVersionPair) {
                long version = pair.getLeft();
                long versionTimeMillis = pair.getRight();
                // If the following condition holds, a subsequent getReadVersion (on this transaction) returns version,
                // otherwise getReadVersion does not use the cached value and results in a GRV call to FDB
                if (version >= weakReadSemantics.getMinVersion() &&
                        (System.currentTimeMillis() - versionTimeMillis) <= weakReadSemantics.getStalenessBoundMillis()) {
                    context.ensureActive().setReadVersion(version);
                    if (timer != null) {
                        timer.increment(FDBStoreTimer.Counts.SET_READ_VERSION_TO_LAST_SEEN);
                    }
                }
            }
        }
        return context;
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
     */
    public CompletableFuture<Long> getReadVersion(@Nonnull FDBRecordContext context) {
        CompletableFuture<Long> readVersionFuture = context.ensureActive().getReadVersion();
        if (!isTrackLastSeenVersionOnRead()) {
            return readVersionFuture;
        }
        long startTime = System.currentTimeMillis();
        return readVersionFuture.thenApply((Long readVersion) -> {
            updateLastSeenFDBVersion(startTime, readVersion);
            return readVersion;
        });
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

    @VisibleForTesting
    @API(API.Status.INTERNAL)
    public void clearCaches() {
        resolverStateCache.clear();
        clearForwardDirectoryCache();
        clearReverseDirectoryCache();
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

    public Transaction createTransaction(Executor executor, boolean transactionIsTraced) {
        Transaction transaction = database.createTransaction(executor);
        if (transactionIsTraced) {
            return new TracedTransaction(transaction);
        } else {
            return transaction;
        }
    }
    
    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     * @return a new runner
     */
    @Nonnull
    public FDBDatabaseRunner newRunner() {
        return new FDBDatabaseRunner(this);
    }

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @return a new runner
     */
    @Nonnull
    public FDBDatabaseRunner newRunner(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext) {
        return new FDBDatabaseRunner(this, timer, mdcContext);
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
        return new FDBDatabaseRunner(this, timer, mdcContext, weakReadSemantics);
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
     * Join a future, following the same logic that <code>asyncToSync(FDBStoreTimer, FDBStoreTimer.Wait, CompletableFuture)</code>
     * uses to validate that the operation isn't blocking in an asynchronous context.
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
     * Get a future, following the same logic that <code>asyncToSync(FDBStoreTimer, FDBStoreTimer.Wait, CompletableFuture)</code>
     * uses to validate that the operation isn't blocking in an asynchronous context.
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

    @API(API.Status.INTERNAL)
    public BlockingInAsyncDetection getBlockingInAsyncDetection() {
        return blockingInAsyncDetectionSupplier.get();
    }

    private void checkIfBlockingInFuture(CompletableFuture<?> future) {
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
        // just we will log the fact that this has taken place and where it has taken place here.
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

    /**
     * Get key tuples that are more or less evenly distributed in the key-value space.
     * This keys can be used to store multiple copies of a value that does not change very
     * often but is accessed so frequently that it would overload the single FDB storage
     * server that has the authoritative value. For example, {@link MetaDataCache#setCurrentVersion}.
     * @param context context to use for reading the database
     * @param prefix prefix key tuple beneath which keys spread out reasonably well
     * @param size number of items needed in a tuple to get the spread
     * @param count maximum number of keys to return
     * @return future for list of boundary key tuples
     */
    @API(API.Status.INTERNAL)
    public CompletableFuture<List<Tuple>> computeBoundaryKeys(@Nonnull FDBTransactionContext context, Tuple prefix,
                                                               int size, int count) {
        byte[] prefixBytes = prefix.pack();
        CloseableAsyncIterator<byte[]> iter = LocalityUtil.getBoundaryKeys(context.ensureActive(),
                                                                           prefixBytes, ByteArrayUtil.strinc(prefixBytes));
        List<Tuple> tuples = new ArrayList<>();
        CompletableFuture<List<Tuple>> result = AsyncUtil.whileTrue(() -> iter.onHasNext().thenApply(hasNext -> {
            if (hasNext) {
                byte[] key = iter.next();
                Tuple item = tryGetNextBoundaryTuple(key, prefixBytes.length, size);
                if (item != null && (tuples.isEmpty() || !item.equals(tuples.get(tuples.size() - 1)))) {
                    tuples.add(item);
                }
            } else {
                iter.close();
            }
            return hasNext;
        }), getExecutor()).thenApply(vignore -> {
            if (count >= tuples.size()) {
                if (tuples.isEmpty()) {
                    return Collections.singletonList(TupleHelpers.EMPTY);
                } else {
                    return tuples;
                }
            } else {
                List<Tuple> selected = new ArrayList<>(count);
                for (int i = 1; i < count + 1; i++) {
                    selected.add(tuples.get(((tuples.size() + 1) * i) / (count + 1) - 1));
                }
                return selected;
            }
        });
        return context.instrument(FDBStoreTimer.Events.COMPUTE_BOUNDARY_KEYS, result);
    }

    @SuppressWarnings("PMD.EmptyCatchBlock")
    private static Tuple tryGetNextBoundaryTuple(byte[] key, int offset, int size) {
        // A boundary may occasionally be in the middle of a tuple item, so back off until parse successfully.
        // TODO: It is hard to do this without try/catch the way the Tuple[2] code is currently structured.
        for (int limit = key.length; limit > offset; limit--) {
            try {
                Tuple t = Tuple.fromBytes(key, offset, limit);
                if (t.size() < size) {
                    return null;
                }
                return Tuple.fromList(t.getItems().subList(0, size));
            } catch (Exception ex) {
                // Keep trying.
            }
        }
        return null;
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
