/*
 * OnlineIndexerBaseBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Base class that the {@link OnlineIndexer.Builder} and the {@link OnlineIndexScrubber.Builder} can both inherit
 * from. Building and scrubbing indexes contains a fair amount of common configuration. This consolidates the
 * common configuration. Note that each method returns the type specified as the concrete type of the resulting
 * builder, so each implementation should be able to use this class fluently without casting.
 *
 * @param <B> concrete type of this builder
 */
public abstract class OnlineIndexOperationBaseBuilder<B extends OnlineIndexOperationBaseBuilder<B>> {
    @Nullable
    private FDBDatabaseRunner runner;
    @Nullable
    private FDBRecordStore.Builder recordStoreBuilder;
    @Nullable
    private UnaryOperator<OnlineIndexOperationConfig> configLoader = null;
    @Nonnull
    private final OnlineIndexOperationConfig.Builder configBuilder = OnlineIndexOperationConfig.newBuilder();
    // Maybe the performance impact of this is low enough to be always enabled?
    private boolean trackProgress = true;


    protected OnlineIndexOperationBaseBuilder() {
    }

    /**
     * Returns {@code this}. Used to allow the type checker to validate that each subclass returns an
     * instance of the concrete builder, which is important for using the builder in fluent situations.
     *
     * @return {@code this}
     */
    abstract B self();

    /**
     * Get the runner that will be used to call into the database.
     * @return the runner that connects to the target database
     */
    @Nullable
    public FDBDatabaseRunner getRunner() {
        return runner;
    }

    /**
     * Set the runner that will be used to call into the database.
     *
     * Normally the runner is gotten from {@link #setDatabase} or {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param runner the runner that connects to the target database
     * @return this builder
     */
    public B setRunner(@Nullable FDBDatabaseRunner runner) {
        this.runner = runner;
        return self();
    }

    private void setRunnerDefaults() {
        setPriority(FDBTransactionPriority.BATCH);
    }

    /**
     * Set the database in which to run the indexing.
     *
     * Normally the database is gotten from {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param database the target database
     * @return this builder
     */
    public B setDatabase(@Nonnull FDBDatabase database) {
        this.runner = database.newRunner();
        setRunnerDefaults();
        return self();
    }

    /**
     * Get the record store builder that will be used to open record store instances for indexing.
     * @return the record store builder
     */
    @Nullable
    @SuppressWarnings("squid:S1452")
    public FDBRecordStore.Builder getRecordStoreBuilder() {
        return recordStoreBuilder;
    }

    /**
     * Set the record store builder that will be used to open record store instances for indexing.
     * @param recordStoreBuilder the record store builder
     * @return this builder
     * @see #setRecordStore
     */
    public B setRecordStoreBuilder(@Nonnull FDBRecordStore.Builder recordStoreBuilder) {
        this.recordStoreBuilder = recordStoreBuilder.copyBuilder().setContext(null);
        if (runner == null && recordStoreBuilder.getContext() != null) {
            runner = recordStoreBuilder.getContext().newRunner();
            setRunnerDefaults();
        }
        return self();
    }

    /**
     * Set the record store that will be used as a template to open record store instances for indexing.
     * @param recordStore the target record store
     * @return this builder
     */
    public B setRecordStore(@Nonnull FDBRecordStore recordStore) {
        recordStoreBuilder = recordStore.asBuilder().setContext(null);
        if (runner == null) {
            runner = recordStore.getRecordContext().newRunner();
            setRunnerDefaults();
        }
        return self();
    }

    /**
     * Get the function used by the online indexer to load the config parameters on fly.
     * @return the function
     */
    @Nullable
    public UnaryOperator<OnlineIndexOperationConfig> getConfigLoader() {
        return configLoader;
    }

    /**
     * Set the function used by the online indexer to load the mutable configuration parameters on fly.
     *
     * <p>
     * The loader is given the current configuration as input at the beginning of each transaction and
     * should produce the configuration to use in the next transaction.
     * </p>
     * @param configLoader the function
     * @return this builder
     */
    @Nonnull
    public B setConfigLoader(@Nonnull UnaryOperator<OnlineIndexOperationConfig> configLoader) {
        this.configLoader = configLoader;
        return self();
    }

    /**
     * Get the maximum number of records to process in one transaction.
     * @return the maximum number of records to process in one transaction
     */
    public int getLimit() {
        return configBuilder.getMaxLimit();
    }

    /**
     * Set the maximum number of records to process in one transaction.
     *
     * The default limit is {@link OnlineIndexOperationConfig#DEFAULT_LIMIT} = {@value OnlineIndexOperationConfig#DEFAULT_LIMIT}.
     * Note {@link #setConfigLoader(UnaryOperator)} is the recommended way of loading online index builder's parameters
     * and the values set by this method will be overwritten if the supplier is set.
     * @param limit the maximum number of records to process in one transaction
     * @return this builder
     */
    @Nonnull
    public B setLimit(int limit) {
        configBuilder.setMaxLimit(limit);
        return self();
    }

    /**
     * Set the initial number of records to process in one transaction. This can be useful to avoid
     * starting the indexing with the max limit (set by {@link #setLimit(int)}), which may cause timeouts.
     * a non-positive value (default) or a value that is bigger than the max limit will initial the limit at the max limit.
     * @param limit the initial number of records to process in one transaction
     * @return this builder
     */
    @Nonnull
    public B setInitialLimit(int limit) {
        configBuilder.setInitialLimit(limit);
        return self();
    }

    /**
     * Get the approximate maximum transaction write size. Note that the actual write size might be up to one
     * record bigger than this value - transactions started as part of the index build will be committed after
     * they exceed this size, and a new transaction will be started.
     * @return the max write size
     */
    public int getMaxWriteLimitBytes() {
        return configBuilder.getWriteLimitBytes();
    }

    /**
     * Set the approximate maximum transaction write size. Note that the actual size might be up to one record
     * bigger than this value - transactions started as part of the index build will be committed after
     * they exceed this size, and a new transaction will be started. A non-positive value implies unlimited.
     * the default limit is {@link OnlineIndexOperationConfig#DEFAULT_WRITE_LIMIT_BYTES} = {@value OnlineIndexOperationConfig#DEFAULT_WRITE_LIMIT_BYTES}.
     * @param max the desired max write size
     * @return this builder
     */
    @Nonnull
    public B setMaxWriteLimitBytes(int max) {
        configBuilder.setWriteLimitBytes(max);
        return self();
    }

    /**
     * Get the maximum number of times to retry a single range rebuild.
     * This retry is on top of the retries caused by {@link #getMaxAttempts()}, and it will also retry for other error
     * codes, such as {@code transaction_too_large}.
     * @return the maximum number of times to retry a single range rebuild
     */
    public int getMaxRetries() {
        return configBuilder.getMaxRetries();
    }

    /**
     * Set the maximum number of times to retry a single range rebuild.
     * This retry is on top of the retries caused by {@link #getMaxAttempts()}, it and will also retry for other error
     * codes, such as {@code transaction_too_large}.
     *
     * The default number of retries is {@link OnlineIndexOperationConfig#DEFAULT_MAX_RETRIES} = {@value OnlineIndexOperationConfig#DEFAULT_MAX_RETRIES}.
     * Note {@link #setConfigLoader(UnaryOperator)} is the recommended way of loading online index builder's parameters
     * and the values set by this method will be overwritten if the supplier is set.
     * @param maxRetries the maximum number of times to retry a single range rebuild
     * @return this builder
     */
    @Nonnull
    public B setMaxRetries(int maxRetries) {
        configBuilder.setMaxRetries(maxRetries);
        return self();
    }

    /**
     * Get the maximum number of records to process in a single second.
     * @return the maximum number of records to process in a single second
     */
    public int getRecordsPerSecond() {
        return configBuilder.getRecordsPerSecond();
    }

    /**
     * Set the maximum number of records to process in a single second.
     *
     * The default number of retries is {@link OnlineIndexOperationConfig#DEFAULT_RECORDS_PER_SECOND} = {@value OnlineIndexOperationConfig#DEFAULT_RECORDS_PER_SECOND}.
     * Note {@link #setConfigLoader(UnaryOperator)} is the recommended way of loading online index builder's parameters
     * and the values set by this method will be overwritten if the supplier is set.
     * @param recordsPerSecond the maximum number of records to process in a single second.
     * @return this builder
     */
    @Nonnull
    public B setRecordsPerSecond(int recordsPerSecond) {
        configBuilder.setRecordsPerSecond(recordsPerSecond);
        return self();
    }

    /**
     * Get the timer used during the online index operation.
     * @return the timer or <code>null</code> if none is set
     */
    @Nullable
    public FDBStoreTimer getTimer() {
        if (runner == null) {
            throw new MetaDataException("timer is only known after runner has been set");
        }
        return runner.getTimer();
    }

    /**
     * Set the timer used during the online index operation.
     * @param timer timer to use
     * @return this builder
     */
    @Nonnull
    public B setTimer(@Nullable FDBStoreTimer timer) {
        if (runner == null) {
            throw new MetaDataException("timer can only be set after runner has been set");
        }
        runner.setTimer(timer);
        return self();
    }

    /**
     * Get the logging context used during the online index operation.
     * @return the logging context of <code>null</code> if none is set
     */
    @Nullable
    public Map<String, String> getMdcContext() {
        if (runner == null) {
            throw new MetaDataException("logging context is only known after runner has been set");
        }
        return runner.getMdcContext();
    }

    /**
     * Set the logging context used during the online index operation.
     * @param mdcContext the logging context to set while running
     * @return this builder
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    @Nonnull
    public B setMdcContext(@Nullable Map<String, String> mdcContext) {
        if (runner == null) {
            throw new MetaDataException("logging context can only be set after runner has been set");
        }
        runner.setMdcContext(mdcContext);
        return self();
    }

    /**
     * Get the acceptable staleness bounds for transactions used by this build. By default, this
     * is set to {@code null}, which indicates that the transaction should not used any cached version
     * at all.
     * @return the acceptable staleness bounds for transactions used by this build
     * @see FDBRecordContext#getWeakReadSemantics()
     */
    @Nullable
    public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        if (runner == null) {
            throw new MetaDataException("weak read semantics is only known after runner has been set");
        }
        return runner.getWeakReadSemantics();
    }

    /**
     * Set the acceptable staleness bounds for transactions used by this build. For index builds, essentially
     * all operations will read and write data in the same transaction, so it is safe to set this value
     * to use potentially stale read versions, though that can potentially result in more transaction conflicts.
     * For performance reasons, it is generally advised that this only be provided an acceptable staleness bound
     * that might use a cached commit if the database tracks the latest commit version in addition to the read
     * version. This is to ensure that the online indexer see its own commits, and it should not be required
     * for correctness, but the online indexer may perform additional work if this is not set.
     *
     * @param weakReadSemantics the acceptable staleness bounds for transactions used by this build
     * @return this builder
     * @see FDBRecordContext#getWeakReadSemantics()
     * @see FDBDatabase#setTrackLastSeenVersion(boolean)
     */
    @Nonnull
    public B setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        if (runner == null) {
            throw new MetaDataException("weak read semantics can only be set after runner has been set");
        }
        runner.setWeakReadSemantics(weakReadSemantics);
        return self();
    }

    /**
     * Get the priority of transactions used for this index build. By default, this will be
     * {@link FDBTransactionPriority#BATCH}.
     * @return the priority of transactions used for this index build
     * @see FDBRecordContext#getPriority()
     */
    @Nonnull
    public FDBTransactionPriority getPriority() {
        if (runner == null) {
            throw new MetaDataException("transaction priority is only known after runner has been set");
        }
        return runner.getPriority();
    }

    /**
     * Set the priority of transactions used for this index build. In general, index builds should run
     * using the {@link FDBTransactionPriority#BATCH BATCH} priority level as their work is generally
     * discretionary and not time sensitive. However, in certain circumstances, it may be
     * necessary to run at the higher {@link FDBTransactionPriority#DEFAULT DEFAULT} priority level.
     * For example, if a missing index is causing some queries to perform additional, unnecessary work that
     * is overwhelming the database, it may be necessary to build the index at {@code DEFAULT} priority
     * in order to lessen the load induced by those queries on the cluster.
     *
     * @param priority the priority of transactions used for this index build
     * @return this builder
     * @see FDBRecordContext#getPriority()
     */
    @Nonnull
    public B setPriority(@Nonnull FDBTransactionPriority priority) {
        if (runner == null) {
            throw new MetaDataException("transaction priority can only be set after runner has been set");
        }
        runner.setPriority(priority);
        return self();
    }

    /**
     * Get the maximum number of transaction retry attempts.
     * This is the number of times that it will retry a given transaction that throws
     * {@link com.apple.foundationdb.record.RecordCoreRetriableTransactionException}.
     * @return the maximum number of attempts
     * @see FDBDatabaseRunner#getMaxAttempts
     */
    public int getMaxAttempts() {
        if (runner == null) {
            throw new MetaDataException("maximum attempts is only known after runner has been set");
        }
        return runner.getMaxAttempts();
    }

    /**
     * Set the maximum number of transaction retry attempts.
     * This is the number of times that it will retry a given transaction that throws
     * {@link com.apple.foundationdb.record.RecordCoreRetriableTransactionException}.
     * @param maxAttempts the maximum number of attempts
     * @return this builder
     * @see FDBDatabaseRunner#setMaxAttempts
     */
    public B setMaxAttempts(int maxAttempts) {
        if (runner == null) {
            throw new MetaDataException("maximum attempts can only be set after runner has been set");
        }
        runner.setMaxAttempts(maxAttempts);
        return self();
    }

    /**
     * Set the number of successful range builds before re-increasing the number of records to process in a single
     * transaction. The number of records to process in a single transaction will never go above {@link #getLimit()}.
     * By default, this is {@link OnlineIndexOperationConfig#DO_NOT_RE_INCREASE_LIMIT}, which means it will not re-increase after successes.
     * <p>
     * Note {@link #setConfigLoader(UnaryOperator)} is the recommended way of loading online index builder's parameters
     * and the values set by this method will be overwritten if the supplier is set.
     * </p>
     * @param increaseLimitAfter the number of successful range builds before increasing the number of records
     * processed in a single transaction
     * @return this builder
     */
    public B setIncreaseLimitAfter(int increaseLimitAfter) {
        configBuilder.setIncreaseLimitAfter(increaseLimitAfter);
        return self();
    }

    /**
     * Get the number of successful range builds before re-increasing the number of records to process in a single
     * transaction.
     * By default this is {@link OnlineIndexOperationConfig#DO_NOT_RE_INCREASE_LIMIT}, which means it will not re-increase after successes.
     * @return the number of successful range builds before increasing the number of records processed in a single
     * transaction
     * @see #getLimit()
     */
    public int getIncreaseLimitAfter() {
        return configBuilder.getIncreaseLimitAfter();
    }

    /**
     * Get the maximum delay between transaction retry attempts.
     * @return the maximum delay
     * @see FDBDatabaseRunner#getMaxDelayMillis
     */
    public long getMaxDelayMillis() {
        if (runner == null) {
            throw new MetaDataException("maximum delay is only known after runner has been set");
        }
        return runner.getMaxDelayMillis();
    }

    /**
     * Set the maximum delay between transaction retry attempts.
     * @param maxDelayMillis the maximum delay
     * @return this builder
     * @see FDBDatabaseRunner#setMaxDelayMillis
     */
    public B setMaxDelayMillis(long maxDelayMillis) {
        if (runner == null) {
            throw new MetaDataException("maximum delay can only be set after runner has been set");
        }
        runner.setMaxDelayMillis(maxDelayMillis);
        return self();
    }

    /**
     * Get the initial delay between transaction retry attempts.
     * @return the initial delay
     * @see FDBDatabaseRunner#getInitialDelayMillis
     */
    public long getInitialDelayMillis() {
        if (runner == null) {
            throw new MetaDataException("initial delay is only known after runner has been set");
        }
        return runner.getInitialDelayMillis();
    }

    /**
     * Set the initial delay between transaction retry attempts.
     * @param initialDelayMillis the initial delay
     * @return this builder
     * @see FDBDatabaseRunner#setInitialDelayMillis
     */
    public B setInitialDelayMillis(long initialDelayMillis) {
        if (runner == null) {
            throw new MetaDataException("initial delay can only be set after runner has been set");
        }
        runner.setInitialDelayMillis(initialDelayMillis);
        return self();
    }

    /**
     * Get the minimum time between successful progress logs when building across transactions.
     * Negative will not log at all, 0 will log after every commit.
     * @return the minimum time between successful progress logs in milliseconds
     * @see #setProgressLogIntervalMillis(long) for more information on the format of the log
     */
    public long getProgressLogIntervalMillis() {
        return configBuilder.getProgressLogIntervalMillis();
    }

    /**
     * Set the minimum time between successful progress logs when building across transactions.
     * Negative will not log at all, 0 will log after every commit.
     * This log will contain the following information:
     * <ul>
     *     <li>startTuple - the first primaryKey scanned as part of this range</li>
     *     <li>endTuple - the desired primaryKey that is the end of this range</li>
     *     <li>realEnd - the tuple that was successfully scanned to (always before endTuple)</li>
     *     <li>recordsScanned - the number of records successfully scanned and processed
     *     <p>
     *         This is the count of records scanned as part of successful transactions used by the
     *         multi-transaction methods (e.g. {@link OnlineIndexer#buildIndexAsync()} or
     *         {@link OnlineIndexer#buildRange(Key.Evaluated, Key.Evaluated)}). The transactional methods (i.e., the methods that
     *         take a store) do not count towards this value. Since only successful transactions are included,
     *         transactions that get {@code commit_unknown_result} will not get counted towards this value,
     *         so this may be short by the number of records scanned in those transactions if they actually
     *         succeeded. In contrast, the timer count:
     *         {@link FDBStoreTimer.Counts#ONLINE_INDEX_BUILDER_RECORDS_SCANNED}, includes all records scanned,
     *         regardless of whether the associated transaction was successful or not.
     *     </p></li>
     * </ul>
     *
     * <p>
     * Note {@link #setConfigLoader(UnaryOperator)} is the recommended way of loading online index builder's parameters
     * and the values set by this method will be overwritten if the supplier is set.
     * </p>
     *
     * @param millis the number of milliseconds to wait between successful logs
     * @return this builder
     */
    public B setProgressLogIntervalMillis(long millis) {
        configBuilder.setProgressLogIntervalMillis(millis);
        return self();
    }

    public boolean isTrackProgress() {
        return trackProgress;
    }

    /**
     * Set whether or not to track the index build progress by updating the number of records successfully scanned
     * and processed. The progress is persisted in {@link OnlineIndexer#indexBuildScannedRecordsSubspace(FDBRecordStoreBase, Index)}
     * which can be accessed by {@link IndexBuildState#loadIndexBuildStateAsync(FDBRecordStoreBase, Index)}.
     * <p>
     * This setting does not affect the setting at {@link #setProgressLogIntervalMillis(long)}.
     * </p>
     * @param trackProgress track progress if true, otherwise false
     * @return this builder
     */
    public B setTrackProgress(boolean trackProgress) {
        this.trackProgress = trackProgress;
        return self();
    }

    /**
     * Set the {@link IndexMaintenanceFilter} to use while building the index.
     *
     * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param indexMaintenanceFilter the index filter to use
     * @return this builder
     */
    public B setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("index filter can only be set after record store builder has been set");
        }
        recordStoreBuilder.setIndexMaintenanceFilter(indexMaintenanceFilter);
        return self();
    }

    /**
     * Set the {@link RecordSerializer} to use while building the index.
     *
     * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param serializer the serializer to use
     * @return this builder
     */
    public B setSerializer(@Nonnull RecordSerializer<Message> serializer) {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("serializer can only be set after record store builder has been set");
        }
        recordStoreBuilder.setSerializer(serializer);
        return self();
    }

    /**
     * Set the store format version to use while building the index.
     *
     * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param formatVersion the format version to use
     * @return this builder
     */
    public B setFormatVersion(int formatVersion) {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("format version can only be set after record store builder has been set");
        }
        recordStoreBuilder.setFormatVersion(formatVersion);
        return self();
    }

    @Nonnull
    protected RecordMetaData getRecordMetaData() {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("record store must be set");
        }
        if (recordStoreBuilder.getMetaDataProvider() == null) {
            throw new MetaDataException("record store builder must include metadata");
        }
        return recordStoreBuilder.getMetaDataProvider().getRecordMetaData();
    }

    /**
     * Set the meta-data to use when indexing.
     * @param metaDataProvider meta-data to use
     * @return this builder
     */
    public B setMetaData(@Nonnull RecordMetaDataProvider metaDataProvider) {
        if (recordStoreBuilder == null) {
            recordStoreBuilder = FDBRecordStore.newBuilder();
        }
        recordStoreBuilder.setMetaDataProvider(metaDataProvider);
        return self();
    }

    /**
     * Set the subspace of the record store in which to build the index.
     * @param subspaceProvider subspace to use
     * @return this builder
     */
    public B setSubspaceProvider(@Nonnull SubspaceProvider subspaceProvider) {
        if (recordStoreBuilder == null) {
            recordStoreBuilder = FDBRecordStore.newBuilder();
        }
        recordStoreBuilder.setSubspaceProvider(subspaceProvider);
        return self();
    }

    /**
     * Set the subspace of the record store in which to build the index.
     * @param subspace subspace to use
     * @return this builder
     */
    public B setSubspace(@Nonnull Subspace subspace) {
        if (recordStoreBuilder == null) {
            recordStoreBuilder = FDBRecordStore.newBuilder();
        }
        recordStoreBuilder.setSubspace(subspace);
        return self();
    }

    /**
     * Set the time limit. The indexer will exit with a proper exception if this time is exceeded after a non-final
     * transaction.
     * @param timeLimitMilliseconds the time limit in milliseconds
     * @return this builder
     */
    @Nonnull
    public B setTimeLimitMilliseconds(long timeLimitMilliseconds) {
        configBuilder.setTimeLimitMilliseconds(timeLimitMilliseconds);
        return self();
    }

    /**
     * Set the time limit for a single transaction. If this limit is exceeded, the indexer will commit the
     * transaction and start a new one. This can be useful to avoid timeouts while scanning many records
     * in each transaction.
     * A non-positive value implies unlimited.
     * Note that this limit, if reached, will be exceeded by an Order(1) overhead time. Keeping some margins might be
     * a good idea.
     * The default value is 4,000 (4 seconds), matches fdb's default 5 seconds transaction limit.
     * @param timeLimitMilliseconds the time limit, per transaction, in milliseconds
     * @return this builder
     */
    @Nonnull
    public B setTransactionTimeLimitMilliseconds(long timeLimitMilliseconds) {
        configBuilder.setTransactionTimeLimitMilliseconds(timeLimitMilliseconds);
        return self();
    }

    /**
     * Set the use of a synchronized session during the index operation. Synchronized sessions help performing
     * the multiple transactions operations in a resource efficient way.
     * Normally this should be {@code true}.
     *
     * @see SynchronizedSessionRunner
     * @param useSynchronizedSession use synchronize session if true, otherwise false
     * @return this builder
     */
    public B setUseSynchronizedSession(boolean useSynchronizedSession) {
        configBuilder.setUseSynchronizedSession(useSynchronizedSession);
        return self();
    }

    /**
     * Set the lease length in milliseconds if the synchronized session is used. The default value is {@link OnlineIndexOperationConfig#DEFAULT_LEASE_LENGTH_MILLIS}.
     * @see #setUseSynchronizedSession(boolean)
     * @see com.apple.foundationdb.synchronizedsession.SynchronizedSession
     * @param leaseLengthMillis length between last access and lease's end time in milliseconds
     * @return this builder
     */
    public B setLeaseLengthMillis(long leaseLengthMillis) {
        configBuilder.setLeaseLengthMillis(leaseLengthMillis);
        return self();
    }


    @Nonnull
    protected OnlineIndexOperationConfig getConfig() {
        return configBuilder.build();
    }

    protected void validateLimits() {
        checkPositive(getMaxRetries(), "maximum retries");
        checkPositive(getLimit(), "record limit");
        checkPositive(getRecordsPerSecond(), "records per second value");
    }

    private static void checkPositive(int value, String desc) {
        if (value <= 0) {
            throw new RecordCoreException("Non-positive value " + value + " given for " + desc);
        }
    }
}
