/*
 * FDBDatabaseFactory.java
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

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCacheFactory;
import com.apple.foundationdb.record.provider.foundationdb.storestate.PassThroughRecordStoreStateCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A singleton maintaining a list of {@link FDBDatabase} instances, indexed by their cluster file location.
 */
@API(API.Status.STABLE)
public class FDBDatabaseFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseFactory.class);

    protected static final Function<FDBLatencySource, Long> DEFAULT_LATENCY_INJECTOR = api -> 0L;

    /**
     * The default number of entries that is to be cached, per database, from
     * {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver} retrieval requests.
     */
    public static final int DEFAULT_DIRECTORY_CACHE_SIZE = 5000;
    private static final int API_VERSION = 600;

    @Nonnull
    private static final FDBDatabaseFactory INSTANCE = new FDBDatabaseFactory();

    @Nonnull
    private FDBLocalityProvider localityProvider = FDBLocalityUtil.instance();

    /* Next few null until initFDB is called */

    @Nullable
    private Executor networkExecutor = null;
    private Executor executor = ForkJoinPool.commonPool();

    @Nullable
    private FDB fdb;
    private boolean inited;

    private boolean unclosedWarning = true;
    @Nullable
    private String traceDirectory = null;
    @Nullable
    private String traceLogGroup = null;
    private int directoryCacheSize = DEFAULT_DIRECTORY_CACHE_SIZE;
    private boolean trackLastSeenVersion;
    private String datacenterId;

    private int maxAttempts = 10;
    private long maxDelayMillis = 1000;
    private long initialDelayMillis = 10;
    private int reverseDirectoryRowsPerTransaction = FDBReverseDirectoryCache.MAX_ROWS_PER_TRANSACTION;
    private long reverseDirectoryMaxMillisPerTransaction = FDBReverseDirectoryCache.MAX_MILLIS_PER_TRANSACTION;
    private long stateRefreshTimeMillis = TimeUnit.SECONDS.toMillis(FDBDatabase.DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS);
    /**
     * The default is a log-based predicate, which can also be used to enable tracing on a more granular level
     * (such as by request) using {@link #setTransactionIsTracedSupplier(Supplier)}.
     */
    @Nonnull
    private Supplier<Boolean> transactionIsTracedSupplier = LOGGER::isTraceEnabled;
    @Nonnull
    private Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier = () -> BlockingInAsyncDetection.DISABLED;
    @Nonnull
    private FDBRecordStoreStateCacheFactory storeStateCacheFactory = PassThroughRecordStoreStateCacheFactory.instance();

    @Nonnull
    private Function<FDBLatencySource, Long> latencyInjector = DEFAULT_LATENCY_INJECTOR;

    private final Map<String, FDBDatabase> databases = new HashMap<>();


    @Nonnull
    public static FDBDatabaseFactory instance() {
        return INSTANCE;
    }

    protected synchronized FDB initFDB() {
        if (!inited) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Starting FDB"));
            }
            fdb = FDB.selectAPIVersion(API_VERSION);
            fdb.setUnclosedWarning(unclosedWarning);
            NetworkOptions options = fdb.options();
            if (traceDirectory != null) {
                options.setTraceEnable(traceDirectory);
            }
            if (traceLogGroup != null) {
                options.setTraceLogGroup(traceLogGroup);
            }
            if (networkExecutor == null) {
                fdb.startNetwork();
            } else {
                fdb.startNetwork(networkExecutor);
            }
            inited = true;
        }
        return fdb;
    }

    @Nonnull
    public Executor getNetworkExecutor() {
        return networkExecutor;
    }

    public void setNetworkExecutor(@Nonnull Executor networkExecutor) {
        this.networkExecutor = networkExecutor;
    }

    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(@Nonnull Executor executor) {
        this.executor = executor;
    }

    public synchronized void shutdown() {
        if (inited) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Shutting down FDB"));
            }
            for (FDBDatabase database : databases.values()) {
                database.close();
            }
            // TODO: Does this do the right thing yet?
            fdb.stopNetwork();
            inited = false;
        }
    }

    public synchronized void clear() {
        for (FDBDatabase database : databases.values()) {
            database.close();
        }
        databases.clear();
    }

    public boolean isUnclosedWarning() {
        return unclosedWarning;
    }

    public void setUnclosedWarning(boolean unclosedWarning) {
        this.unclosedWarning = unclosedWarning;
    }

    @SpotBugsSuppressWarnings("IS2_INCONSISTENT_SYNC")
    public void setTrace(@Nullable String traceDirectory, @Nullable String traceLogGroup) {
        this.traceDirectory = traceDirectory;
        this.traceLogGroup = traceLogGroup;
    }

    public synchronized int getDirectoryCacheSize() {
        return directoryCacheSize;
    }

    @SuppressWarnings("PMD.BooleanGetMethodName")
    public synchronized boolean getTrackLastSeenVersion() {
        return trackLastSeenVersion;
    }

    public synchronized String getDatacenterId() {
        return datacenterId;
    }

    /**
     * Sets the number of directory layer entries that will be cached for each database that is produced by the factory.
     * Changing this value after databases have been created will result in each database having its existing entries
     * discarded and the cache size adjusted to the provided value.
     *
     * <p>Each {@link FDBDatabase} maintains a cache of entries that have been retrieved by any instance of a
     * {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver} using that database.
     * Thus, this cache is shared <i>across</i> all resolvers (it should be noted entries in this cache are
     * segregated by resolver and treated as distinct, ensuring that the value for a directory entry from one
     * resolver will not be returned to another resolver even if they share the same key).
     *
     * @param directoryCacheSize the new directory cache size
     */
    public synchronized void setDirectoryCacheSize(int directoryCacheSize) {
        this.directoryCacheSize = directoryCacheSize;
        for (FDBDatabase database : databases.values()) {
            database.setDirectoryCacheSize(directoryCacheSize);
        }
    }

    public synchronized void setTrackLastSeenVersion(boolean trackLastSeenVersion) {
        this.trackLastSeenVersion = trackLastSeenVersion;
        for (FDBDatabase database : databases.values()) {
            database.setTrackLastSeenVersion(trackLastSeenVersion);
        }
    }

    public synchronized void setDatacenterId(String datacenterId) {
        this.datacenterId = datacenterId;
        for (FDBDatabase database : databases.values()) {
            database.setDatacenterId(datacenterId);
        }
    }

    /**
     * Gets the maximum number of attempts for a database to make when running a
     * retriable transactional operation. This is used by {@link FDBDatabase#run(java.util.function.Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(java.util.function.Function) FDBDatabase.runAsync()} to limit the number of
     * attempts that an operation is retried. The default value is 10.
     * @return the maximum number of times to run a transactional database operation
     */
    public int getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * Sets the maximum number of attempts for a database to make when running a
     * retriable transactional operation. This is used by {@link FDBDatabase#run(java.util.function.Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(java.util.function.Function) FDBDatabase.runAsync()} to limit the number of
     * attempts that an operation is retried. The default value is 10.
     * @param maxAttempts the maximum number of times to run a transactional database operation
     * @throws IllegalArgumentException if a non-positive number is given
     */
    public void setMaxAttempts(int maxAttempts) {
        if (maxAttempts <= 0) {
            throw new RecordCoreException("Cannot set maximum number of attempts to less than or equal to zero");
        }
        this.maxAttempts = maxAttempts;
    }

    /**
     * Gets the maximum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link FDBDatabase#run(java.util.function.Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(java.util.function.Function) FDBDatabase.runAsync()} to limit the time spent
     * between successive attempts at completing a database operation. The default value is 1000 so that
     * there will not be more than 1 second between attempts.
     * @return the maximum delay between attempts when retrying operations
     */
    public long getMaxDelayMillis() {
        return maxDelayMillis;
    }

    /**
     * Sets the maximum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link FDBDatabase#run(java.util.function.Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(java.util.function.Function) FDBDatabase.runAsync()} to limit the time spent
     * between successive attempts at completing a database operation. The default is 1000 so that
     * there will not be more than 1 second between attempts.
     * @param maxDelayMillis the maximum delay between attempts when retrying operations
     * @throws IllegalArgumentException if the value is negative or less than the minimum delay
     */
    public void setMaxDelayMillis(long maxDelayMillis) {
        if (maxDelayMillis < 0) {
            throw new RecordCoreException("Cannot set maximum delay milliseconds to less than or equal to zero");
        } else if (maxDelayMillis < initialDelayMillis) {
            throw new RecordCoreException("Cannot set maximum delay to less than minimum delay");
        }
        this.maxDelayMillis = maxDelayMillis;
    }

    /**
     * Gets the delay ceiling (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link FDBDatabase#run(java.util.function.Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(java.util.function.Function) FDBDatabase.runAsync()} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initial delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     * @return the delay ceiling between the first and second attempts at running a database operation
     */
    public long getInitialDelayMillis() {
        return initialDelayMillis;
    }

    /**
     * Sets the delay ceiling (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link FDBDatabase#run(java.util.function.Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(java.util.function.Function) FDBDatabase.runAsync()} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initial delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     * @param initialDelayMillis the delay ceiling between the first and second attempts at running a database operation
     * @throws IllegalArgumentException if the value is negative or greater than the maximum delay
     */
    public void setInitialDelayMillis(long initialDelayMillis) {
        if (initialDelayMillis < 0) {
            throw new RecordCoreException("Cannot set initial delay milleseconds to less than zero");
        } else if (initialDelayMillis > maxDelayMillis) {
            throw new RecordCoreException("Cannot set initial delay to greater than maximum delay");
        }
        this.initialDelayMillis = initialDelayMillis;
    }

    /**
     * When a reverse directory lookup is performed from a {@link FDBReverseDirectoryCache} and an entry is not found
     * in the cache and, thus, the directory layer must be scanned to find it, this property determines how many rows
     * will be scanned within the context of a single transaction, before the transaction is closed and re-opened
     * in order to avoid a <code>past_version</code>.
     * @param rowsPerTransaction the number of rows to scan within the context of a single transaction
     */
    public void setReverseDirectoryRowsPerTransaction(int rowsPerTransaction) {
        this.reverseDirectoryRowsPerTransaction = rowsPerTransaction;
    }

    public int getReverseDirectoryRowsPerTransaction() {
        return reverseDirectoryRowsPerTransaction;
    }

    /**
     * When a reverse directory lookup is performed from a {@link FDBReverseDirectoryCache} and an entry is not found
     * in the cache and, thus, the directory layer must be scanned to find it, this property determines how many
     * milliseconds may be spent scanning the cache within the context of a single transaction, before the transaction
     * is closed and re-opened in order to avoid a <code>past_version</code>.
     * @param millisPerTransaction the number of miliseconds to spend scanning
     */
    public void setReverseDirectoryMaxMillisPerTransaction(long millisPerTransaction) {
        this.reverseDirectoryMaxMillisPerTransaction = millisPerTransaction;
    }

    public long getReverseDirectoryMaxMillisPerTransaction() {
        return reverseDirectoryMaxMillisPerTransaction;
    }

    /**
     * Use transactionIsTracedSupplier to control whether a newly created transaction should be traced or not. Traced
     * transactions are used to identify and profile unclosed transactions. In order to trace the source of a leaked
     * transaction, it is necessary to capture a stack trace at the point the transaction is created which is a rather
     * expensive operation, so this should either be disabled in a production environment or the supplier to return
     * true for only a very small subset of transactions.
     * @param transactionIsTracedSupplier a supplier which should return <code>true</code> for creating {@link
     * TracedTransaction}s or <code>false</code> for creating normal {@link com.apple.foundationdb.Transaction}s
     */
    public void setTransactionIsTracedSupplier(Supplier<Boolean> transactionIsTracedSupplier) {
        this.transactionIsTracedSupplier = transactionIsTracedSupplier;
    }

    public Supplier<Boolean> getTransactionIsTracedSupplier() {
        return transactionIsTracedSupplier;
    }

    /**
     * Controls if calls to <code>FDBDatabase#asyncToSync(FDBStoreTimer, FDBStoreTimer.Wait, CompletableFuture)</code>
     * or <code>FDBRecordContext#asyncToSync(FDBStoreTimer.Wait, CompletableFuture)</code> will attempt to detect
     * when they are being called from within an asynchronous context and how they should react to this fact
     * when they are. Note that the process of performing this detection is quite expensive, so running with
     * detection enabled in not recommended for environments other than testing.
     *
     * @param behavior the blocking desired blocking detection behavior
     * (see {@link BlockingInAsyncDetection})
     */
    public void setBlockingInAsyncDetection(@Nonnull BlockingInAsyncDetection behavior) {
        setBlockingInAsyncDetection(() -> behavior);
    }

    /**
     * Provides a supplier that controls if calls to <code>FDBDatabase#asyncToSync(FDBStoreTimer, FDBStoreTimer.Wait, CompletableFuture)</code>
     * or <code>FDBRecordContext#asyncToSync(FDBStoreTimer.Wait, CompletableFuture)</code> will attempt to detect
     * when they are being called from within an asynchronous context and how they should react to this fact
     * when they are.  Because such detection is quite expensive, it is suggested that it is either
     * {@link BlockingInAsyncDetection#DISABLED} for anything other than testing environments or that the
     * supplier randomly chooses a small sample rate in which detection should be enabled.
     *
     * @param supplier a supplier that produces the blocking desired blocking detection behavior
     * (see {@link BlockingInAsyncDetection})
     */
    public void setBlockingInAsyncDetection(@Nonnull Supplier<BlockingInAsyncDetection> supplier) {
        this.blockingInAsyncDetectionSupplier = supplier;
    }

    @Nonnull
    protected Supplier<BlockingInAsyncDetection> getBlockingInAsyncDetectionSupplier() {
        return this.blockingInAsyncDetectionSupplier;
    }

    /**
     * Provides a function that computes a latency that should be injected into a specific FDB operation.  The
     * provided function takes a {@link FDBLatencySource} as input and returns the number of milliseconds delay that should
     * be injected before the operation completes.  Returning a value of zero or less indicates that no delay should
     * be injected.
     *
     * <p>Latency injection can be useful for simulating environments in which FDB is under stress or in a
     * configuration in which latency is inherent in its operation.
     *
     * @param latencyInjector a function computing the latency to be injected into an operation
     */
    public void setLatencyInjector(@Nonnull Function<FDBLatencySource, Long> latencyInjector) {
        this.latencyInjector = latencyInjector;
    }

    /**
     * Returns the current latency injector.
     *
     * @return the current latency injector
     */
    public Function<FDBLatencySource, Long> getLatencyInjector() {
        return latencyInjector;
    }

    /**
     * Removes any previously installed latency injector.
     */
    public void clearLatencyInjector() {
        this.latencyInjector = DEFAULT_LATENCY_INJECTOR;
    }

    public long getStateRefreshTimeMillis() {
        return stateRefreshTimeMillis;
    }

    /**
     * Set the refresh time for the cached {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver}
     * state. Defaults to {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS} seconds.
     * @param stateRefreshTimeMillis time to set, in milliseconds
     */
    public void setStateRefreshTimeMillis(long stateRefreshTimeMillis) {
        this.stateRefreshTimeMillis = stateRefreshTimeMillis;
    }

    /**
     * Get the store state cache factory. Each {@link FDBDatabase} produced by this {@code FDBDatabaseFactory} will be
     * initialized with an {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache FDBRecordStoreStateCache}
     * from this cache factory. By default, the factory is a {@link PassThroughRecordStoreStateCacheFactory} which means
     * that the record store state information is never cached.
     *
     * @return the factory of {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache FDBRecordStoreStateCache}s
     *      used when initializing {@link FDBDatabase}s
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public FDBRecordStoreStateCacheFactory getStoreStateCacheFactory() {
        return storeStateCacheFactory;
    }

    /**
     * Set the store state cache factory. Each {@link FDBDatabase} produced by this {@code FDBDatabaseFactory} will be
     * initialized with an {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache FDBRecordStoreStateCache}
     * from the cache factory provided.
     *
     * @param storeStateCacheFactory a factory of {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache FDBRecordStoreStateCache}s
     *      to use when initializing {@link FDBDatabase}s
     * @see com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache
     */
    @API(API.Status.EXPERIMENTAL)
    public void setStoreStateCacheFactory(@Nonnull FDBRecordStoreStateCacheFactory storeStateCacheFactory) {
        this.storeStateCacheFactory = storeStateCacheFactory;
    }

    @Nonnull
    public synchronized FDBDatabase getDatabase(@Nullable String clusterFile) {
        FDBDatabase database = databases.get(clusterFile);
        if (database == null) {
            database = new FDBDatabase(this, clusterFile);
            database.setDirectoryCacheSize(getDirectoryCacheSize());
            database.setTrackLastSeenVersion(getTrackLastSeenVersion());
            database.setResolverStateRefreshTimeMillis(getStateRefreshTimeMillis());
            database.setDatacenterId(getDatacenterId());
            database.setStoreStateCache(storeStateCacheFactory.getCache(database));
            databases.put(clusterFile, database);
        }
        return database;
    }

    @Nonnull
    public synchronized FDBDatabase getDatabase() {
        return getDatabase(null);
    }

    /**
     * Get the locality provider that is used to discover the server location of the keys.
     * @return the installed locality provider
     */
    @Nonnull
    public FDBLocalityProvider getLocalityProvider() {
        return localityProvider;
    }

    /**
     * Set the locality provider that is used to discover the server location of the keys.
     * @param localityProvider the locality provider
     * @see FDBLocalityUtil
     */
    public void setLocalityProvider(@Nonnull FDBLocalityProvider localityProvider) {
        this.localityProvider = localityProvider;
    }
}
