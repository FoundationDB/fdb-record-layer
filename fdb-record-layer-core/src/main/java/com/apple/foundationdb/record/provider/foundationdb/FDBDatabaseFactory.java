/*
 * FDBDatabaseFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCacheFactory;
import com.apple.foundationdb.record.provider.foundationdb.storestate.PassThroughRecordStoreStateCacheFactory;

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
 * Abstract class for a factory that manages {@link FDBDatabase} instances.
 */
public abstract class FDBDatabaseFactory {

    /**
     * The default number of entries that is to be cached, per database, from
     * {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver} retrieval requests.
     */
    public static final int DEFAULT_DIRECTORY_CACHE_SIZE = 5000;
    /**
     * Special value to set the transaction timeout to indicate that transactions should use the system
     * default.
     *
     * @see #setTransactionTimeoutMillis(long)
     */
    public static final long DEFAULT_TR_TIMEOUT_MILLIS = -1L;
    /**
     * Special value to set the transaction timeout to to indicate that transactions should not have any
     * timeout set at all.
     *
     * @see #setTransactionTimeoutMillis(long)
     */
    public static final long UNLIMITED_TR_TIMEOUT_MILLIS = 0L;

    protected static final Function<FDBLatencySource, Long> DEFAULT_LATENCY_INJECTOR = api -> 0L;

    protected final Map<String, FDBDatabase> databases = new HashMap<>();

    @Nullable
    protected volatile Executor networkExecutor = null;
    @Nonnull
    protected Function<Executor, Executor> contextExecutor = Function.identity();
    protected boolean unclosedWarning = true;
    @Nonnull
    protected Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier = () -> BlockingInAsyncDetection.DISABLED;
    @Nonnull
    protected FDBRecordStoreStateCacheFactory storeStateCacheFactory = PassThroughRecordStoreStateCacheFactory.instance();
    @Nonnull
    private Executor executor = ForkJoinPool.commonPool();
    private int directoryCacheSize = DEFAULT_DIRECTORY_CACHE_SIZE;
    private boolean trackLastSeenVersion;
    private String datacenterId;
    private int maxAttempts = 10;
    private long maxDelayMillis = 1000;
    private long initialDelayMillis = 10;
    private int reverseDirectoryRowsPerTransaction = FDBReverseDirectoryCache.MAX_ROWS_PER_TRANSACTION;
    private long reverseDirectoryMaxMillisPerTransaction = FDBReverseDirectoryCache.MAX_MILLIS_PER_TRANSACTION;
    private long stateRefreshTimeMillis = TimeUnit.SECONDS.toMillis(FDBDatabase.DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS);
    private long transactionTimeoutMillis = DEFAULT_TR_TIMEOUT_MILLIS;
    private long warnAndCloseOpenContextsAfterSeconds;
    @Nullable
    private TransactionListener transactionListener;

    private Function<FDBLatencySource, Long> latencyInjector = DEFAULT_LATENCY_INJECTOR;

    /**
     * Returns the default implementation of {@code FDBDatabaseFactory} (specifically an instance of
     * {@link FDBDatabaseFactoryImpl#instance()}.  Note that callers wishing to use alternative implementations
     * should call the appropriate {@code instance()} method for the implementation.
     *
     * @return singleton instance of {@link FDBDatabaseFactoryImpl}.
     */
    @Nonnull
    public static FDBDatabaseFactoryImpl instance() {
        return FDBDatabaseFactoryImpl.instance();
    }

    @Nullable
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

    /**
     * Sets the executor that will be used for all asynchronous tasks that are produced from operations initiated
     * from databases produced from this factory.
     *
     * @param executor the executor to be used for asynchronous task completion
     */
    public void setExecutor(@Nonnull Executor executor) {
        this.executor = executor;
    }

    /**
     * Provides a function that will be invoked when a {@link FDBRecordContext} is created, taking as input the
     * {@code Executor} that is configured for the database, returning the {@code Executor} that will be used
     * to execute all asynchronous completions produced from the {@code FDBRecordContext}. An example use case
     * for this function is to ensure that {@code ThreadLocal} variables that are present in the thread that
     * creates the {@code FDBRecordContext} will be made present in the executor threads that are executing tasks.
     *
     * @param contextExecutor function to produce an executor to be used for all tasks executed on behalf of a
     * specific record context
     */
    public void setContextExecutor(@Nonnull Function<Executor, Executor> contextExecutor) {
        this.contextExecutor = contextExecutor;
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
     * retriable transactional operation. This is used by {@link FDBDatabase#run(Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(Function) FDBDatabase.runAsync()} to limit the number of
     * attempts that an operation is retried. The default value is 10.
     *
     * @return the maximum number of times to run a transactional database operation
     */
    public int getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * Sets the maximum number of attempts for a database to make when running a
     * retriable transactional operation. This is used by {@link FDBDatabase#run(Function) FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(Function) FDBDatabase.runAsync()} to limit the number of
     * attempts that an operation is retried. The default value is 10.
     *
     * @param maxAttempts the maximum number of times to run a transactional database operation
     *
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
     * run a transactional database operation. This is used within {@link FDBDatabase#run(Function)
     * FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(Function) FDBDatabase.runAsync()} to limit the time spent
     * between successive attempts at completing a database operation. The default value is 1000 so that
     * there will not be more than 1 second between attempts.
     *
     * @return the maximum delay between attempts when retrying operations
     */
    public long getMaxDelayMillis() {
        return maxDelayMillis;
    }

    /**
     * Sets the maximum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link FDBDatabase#run(Function)
     * FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(Function) FDBDatabase.runAsync()} to limit the time spent
     * between successive attempts at completing a database operation. The default is 1000 so that
     * there will not be more than 1 second between attempts.
     *
     * @param maxDelayMillis the maximum delay between attempts when retrying operations
     *
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
     * run a transactional database operation. This is used within {@link FDBDatabase#run(Function)
     * FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(Function) FDBDatabase.runAsync()} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initial delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     *
     * @return the delay ceiling between the first and second attempts at running a database operation
     */
    public long getInitialDelayMillis() {
        return initialDelayMillis;
    }

    /**
     * Sets the delay ceiling (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link FDBDatabase#run(Function)
     * FDBDatabase.run()}
     * and {@link FDBDatabase#runAsync(Function) FDBDatabase.runAsync()} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initial delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     *
     * @param initialDelayMillis the delay ceiling between the first and second attempts at running a database operation
     *
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
     *
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
     *
     * @param millisPerTransaction the number of miliseconds to spend scanning
     */
    public void setReverseDirectoryMaxMillisPerTransaction(long millisPerTransaction) {
        this.reverseDirectoryMaxMillisPerTransaction = millisPerTransaction;
    }

    public long getReverseDirectoryMaxMillisPerTransaction() {
        return reverseDirectoryMaxMillisPerTransaction;
    }

    /**
     * Get whether to warn and close record contexts open for too long.
     *
     * @return number of seconds after which a context and be closed and a warning logged
     */
    public long getWarnAndCloseOpenContextsAfterSeconds() {
        return warnAndCloseOpenContextsAfterSeconds;
    }

    /**
     * Set whether to warn and close record contexts open for too long.
     *
     * @param warnAndCloseOpenContextsAfterSeconds number of seconds after which a context and be closed and a warning
     * logged
     */
    public void setWarnAndCloseOpenContextsAfterSeconds(long warnAndCloseOpenContextsAfterSeconds) {
        this.warnAndCloseOpenContextsAfterSeconds = warnAndCloseOpenContextsAfterSeconds;
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
     * Provides a supplier that controls if calls to <code>FDBDatabase#asyncToSync(FDBStoreTimer, FDBStoreTimer.Wait,
     * CompletableFuture)</code>
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

    /**
     * Provides a function that computes a latency that should be injected into a specific FDB operation.  The
     * provided function takes a {@link FDBLatencySource} as input and returns the number of milliseconds delay that
     * should
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
    @Nonnull
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
     *
     * @param stateRefreshTimeMillis time to set, in milliseconds
     */
    public void setStateRefreshTimeMillis(long stateRefreshTimeMillis) {
        this.stateRefreshTimeMillis = stateRefreshTimeMillis;
    }

    /**
     * Set the transaction timeout time in milliseconds. Databases created by this factory will use this value when
     * they create transactions. If the timeout is reached, the transaction will fail with an
     * {@link FDBExceptions.FDBStoreTransactionTimeoutException}
     * and will not be retried. Any outstanding work from the transaction will be cancelled, though the
     * user should still close the {@link FDBRecordContext} to free any native memory used by the transaction.
     *
     * <p>
     * If set to {@link #DEFAULT_TR_TIMEOUT_MILLIS}, then the transaction's timeout will default to the system default,
     * which is the value set by {@link com.apple.foundationdb.DatabaseOptions#setTransactionTimeout(long)}. If that
     * option is not set, then no timeout will be imposed on the transaction. Note also that this is the
     * default value
     * </p>
     *
     * <p>
     * If set to {@link #UNLIMITED_TR_TIMEOUT_MILLIS}, then the no timeout will be imposed on the transaction. This
     * will override the system default if one is set.
     * </p>
     *
     * @param transactionTimeoutMillis the amount of time in milliseconds before a transaction should timeout
     */
    public void setTransactionTimeoutMillis(long transactionTimeoutMillis) {
        if (transactionTimeoutMillis < DEFAULT_TR_TIMEOUT_MILLIS) {
            throw new RecordCoreArgumentException("cannot set transaction timeout millis to " + transactionTimeoutMillis);
        }
        this.transactionTimeoutMillis = transactionTimeoutMillis;
    }

    /**
     * Get the transaction timeout time in milliseconds. See {@link #setTransactionTimeoutMillis(long)} for more
     * information, especially for the meaning of the special values {@link #DEFAULT_TR_TIMEOUT_MILLIS} and
     * {@link #UNLIMITED_TR_TIMEOUT_MILLIS}.
     *
     * @return the transaction timeout time in milliseconds
     *
     * @see #setTransactionTimeoutMillis(long)
     */
    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    /**
     * Get the store state cache factory. Each {@link FDBDatabase} produced by this {@code FDBDatabaseFactory} will
     * be
     * initialized with an {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache
     * FDBRecordStoreStateCache}
     * from this cache factory. By default, the factory is a {@link PassThroughRecordStoreStateCacheFactory} which
     * means
     * that the record store state information is never cached.
     *
     * @return the factory of {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache
     * FDBRecordStoreStateCache}s
     * used when initializing {@link FDBDatabase}s
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public FDBRecordStoreStateCacheFactory getStoreStateCacheFactory() {
        return storeStateCacheFactory;
    }

    /**
     * Set the store state cache factory. Each {@link FDBDatabase} produced by this {@code FDBDatabaseFactory} will
     * be
     * initialized with an {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache
     * FDBRecordStoreStateCache}
     * from the cache factory provided.
     *
     * @param storeStateCacheFactory a factory of {@link com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache
     * FDBRecordStoreStateCache}s
     * to use when initializing {@link FDBDatabase}s
     *
     * @see com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache
     */
    @API(API.Status.EXPERIMENTAL)
    public void setStoreStateCacheFactory(@Nonnull FDBRecordStoreStateCacheFactory storeStateCacheFactory) {
        this.storeStateCacheFactory = storeStateCacheFactory;
    }

    /**
     * Creates a new {@code Executor} for use by a specific {@code FDBRecordContext}. If {@code mdcContext}
     * is not {@code null}, the executor will ensure that the provided MDC present within the context of the
     * executor thread.
     *
     * @param mdcContext if present, the MDC context to be made available within the executors threads
     *
     * @return a new executor to be used by a {@code FDBRecordContext}
     */
    @Nonnull
    public Executor newContextExecutor(@Nullable Map<String, String> mdcContext) {
        Executor newExecutor = contextExecutor.apply(getExecutor());
        if (mdcContext != null) {
            newExecutor = new ContextRestoringExecutor(newExecutor, mdcContext);
        }
        return newExecutor;
    }

    /**
     * Set the number of threads per FDB client version. The default value is 1.
     *
     * @param threadsPerClientV the number of threads per client version. Cannot be less than 1.
     *
     * @deprecated Call directly on {@link FDBDatabaseFactoryImpl}. This will be removed in a future release.
     */
    @Deprecated
    public static void setThreadsPerClientVersion(int threadsPerClientV) {
        FDBDatabaseFactoryImpl.setThreadsPerClientVersion(threadsPerClientV);
    }

    /**
     * Get the number of threads per FDB client version. The default value is 1.
     *
     * @return the number of threads per client version to use.
     *
     * @deprecated Call directly on {@link FDBDatabaseFactoryImpl}. This will be removed in a future release.
     */
    @Deprecated
    public static int getThreadsPerClientVersion() {
        return FDBDatabaseFactoryImpl.getThreadsPerClientVersion();
    }

    @Nonnull
    public Supplier<BlockingInAsyncDetection> getBlockingInAsyncDetectionSupplier() {
        return this.blockingInAsyncDetectionSupplier;
    }

    /**
     * Installs a listener to be notified of transaction events, such as creation, commit, and close.
     * This listener is in the path of the completion of every transaction. Implementations should take
     * care to ensure that they are thread safe and efficiently process the provided metrics before returning.
     *
     * @param listener the listener to install
     *
     * @return this factory
     */
    @Nonnull
    public FDBDatabaseFactory setTransactionListener(@Nullable final TransactionListener listener) {
        this.transactionListener = listener;
        return this;
    }

    @Nullable
    public TransactionListener getTransactionListener() {
        return transactionListener;
    }

    public abstract void shutdown();

    /**
     * Configure the client trace directory and log group. If set, this will configure the native client to
     * emit trace logs with important metrics and instrumentation. As this information is useful for monitoring
     * FoundationDB client behavior, it is generally recommended that the user set this option in production
     * environments.
     *
     * <p>
     * The logs will be placed in the directory on the local filesystem specified by the {@code traceDirectory}
     * parameter. If the {@code traceLogGroup} is set to a non-null value, each log message will have a
     * {@code LogGroup} field associated with it that is set to the parameter's value. This can be used to associate
     * log messages from related processes together.
     * </p>
     *
     * <p>
     * This method should be called prior to the first time this factory is used to produce an {@link FDBDatabase}.
     * The factory will configure the client in a manner consistent with the passed parameters the first
     * time a database is needed, and subsequent calls to this method will have no effect.
     * </p>
     *
     * @param traceDirectory the directory in which to write trace log files or {@code null} to disable writing logs
     * @param traceLogGroup the value to set the log group field to in each message of {@code null} to set no group
     */
    @SpotBugsSuppressWarnings("IS2_INCONSISTENT_SYNC")
    public abstract void setTrace(@Nullable String traceDirectory, @Nullable String traceLogGroup);

    /**
     * Set the output format for the client trace logs. This only will have any effect if
     * {@link #setTrace(String, String)} is also called. If that method is called (i.e., if trace logs are enabled),
     * then this will be used to configure what the output format of trace log files should be. See
     * {@link FDBTraceFormat} for more details on what options are available.
     *
     * <p>
     * This method should be called prior to the first time this factory is used to produce an {@link FDBDatabase}.
     * The factory will configure the client in a manner consistent with the passed parameters the first
     * time a database is needed, and subsequent calls to this method will have no effect.
     * </p>
     *
     * @param traceFormat the output format for client trace logs
     *
     * @see #setTrace(String, String)
     * @see FDBTraceFormat
     */
    public abstract void setTraceFormat(@Nonnull FDBTraceFormat traceFormat);

    /**
     * Set whether additional run-loop profiling of the FDB client is enabled. This can be useful for debugging
     * certain performance problems, but the profiling is also fairly heavy-weight, and so it is not generally
     * recommended when performance is critical. This method should be set prior to the first {@link FDBDatabase}
     * is returned by this factory, as otherwise, it will have no effect (i.e., run-loop profiling will not actually
     * be enabled). Enabling run-loop profiling also places its output in the FDB client trace logs, so it only makes
     * sense to call this method if one also calls {@link #setTrace(String, String)}.
     *
     * @param runLoopProfilingEnabled whether run-loop profiling should be enabled
     *
     * @see NetworkOptions#setEnableSlowTaskProfiling()
     */
    public abstract void setRunLoopProfilingEnabled(boolean runLoopProfilingEnabled);

    /**
     * Get whether additional run-loop profiling has been been enabled.
     *
     * @return whether additional run-loop profiling has been enabled
     *
     * @see #setRunLoopProfilingEnabled(boolean)
     */
    public abstract boolean isRunLoopProfilingEnabled();

    /**
     * Use transactionIsTracedSupplier to control whether a newly created transaction should be traced or not. Traced
     * transactions are used to identify and profile unclosed transactions. In order to trace the source of a leaked
     * transaction, it is necessary to capture a stack trace at the point the transaction is created which is a rather
     * expensive operation, so this should either be disabled in a production environment or the supplier to return
     * true for only a very small subset of transactions.
     *
     * @param transactionIsTracedSupplier a supplier which should return <code>true</code> for creating traced
     * transactions
     */
    public abstract void setTransactionIsTracedSupplier(Supplier<Boolean> transactionIsTracedSupplier);

    public abstract Supplier<Boolean> getTransactionIsTracedSupplier();

    /**
     * Set the API version for this database factory. This value will be used to determine what FDB APIs are
     * available.
     *
     * <p>
     * Note that once an API version has been set, the client will not be able to talk to any FDB cluster
     * that is running an older associated server version. It is therefore important that the adopter sets
     * this value so that it does not exceed the smallest FDB server version that this client will be
     * expected to connect to. However, if there are features that are only available on a subset of
     * supported FDB API versions, the Record Layer will attempt to guard those features so that they are
     * only used if the configured API version supports it.
     * </p>
     *
     * <p>
     * By default, this will be set to {@link APIVersion#getDefault()}. Note that setting the API version
     * to a non-default value is currently experimental as additional testing and validation of various
     * API versions is developed.
     * </p>
     *
     * @param apiVersion the API version to use when initializing the FDB client
     * @see APIVersion
     */
    @API(API.Status.EXPERIMENTAL)
    public abstract void setAPIVersion(@Nonnull APIVersion apiVersion);

    /**
     * Get the configured FDB API version. This is an internal method to indicate what value was configured
     * on this database factory, as other Record Layer APIs should hide API-version compatibility code so
     * that adopters do not need to worry about setting this value except possibly during client initialization.
     *
     * @return the configured FDB API version
     * @see #setAPIVersion(APIVersion)
     */
    @API(API.Status.INTERNAL)
    public abstract APIVersion getAPIVersion();

    @Nonnull
    public abstract FDBDatabase getDatabase(@Nullable String clusterFile);

    @Nonnull
    public FDBDatabase getDatabase() {
        return getDatabase(null);
    }

    /**
     * Get the locality provider that is used to discover the server location of the keys.
     *
     * @return the installed locality provider
     */
    @Nonnull
    public abstract FDBLocalityProvider getLocalityProvider();

    /**
     * Set the locality provider that is used to discover the server location of the keys.
     *
     * @param localityProvider the locality provider
     *
     * @see FDBLocalityUtil
     */
    public abstract void setLocalityProvider(@Nonnull FDBLocalityProvider localityProvider);

    /**
     * Return a {@link Database} object from the factory.
     *
     * @param clusterFile Cluster file.
     *
     * @return FDB Database object.
     */
    @Nonnull
    public abstract Database open(String clusterFile);
}
