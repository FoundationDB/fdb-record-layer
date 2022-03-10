/*
 * OnlineScrubber.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@API(API.Status.UNSTABLE)
public class OnlineIndexScrubber implements AutoCloseable {

    @Nonnull private final IndexingCommon common;
    @Nonnull private final FDBDatabaseRunner runner;
    @Nonnull private final ScrubbingPolicy scrubbingPolicy;

    public enum ScrubbingType {
        DANGLING,
        MISSING
    }

    @SuppressWarnings("squid:S00107")
    OnlineIndexScrubber(@Nonnull FDBDatabaseRunner runner,
                        @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                        @Nonnull Index index,
                        @Nonnull Collection<RecordType> recordTypes,
                        @Nonnull Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader,
                        @Nonnull OnlineIndexer.Config config,
                        long leaseLengthMillis,
                        boolean trackProgress,
                        @Nonnull OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy) {

        this.runner = runner;
        this.scrubbingPolicy = scrubbingPolicy;
        this.common = new IndexingCommon(runner, recordStoreBuilder,
                Collections.singletonList(index), recordTypes, configLoader, config,
                trackProgress,
                true, // always use synchronized session
                leaseLengthMillis);
    }

    @Override
    public void close() {
        common.close();
    }

    private IndexingBase getScrubber(ScrubbingType type, AtomicLong count) {
        switch (type) {
            case DANGLING:
                return new IndexingScrubDangling(common, OnlineIndexer.IndexingPolicy.DEFAULT, scrubbingPolicy, count);

            case MISSING:
                return new IndexingScrubMissing(common, OnlineIndexer.IndexingPolicy.DEFAULT, scrubbingPolicy, count);

            default:
                throw new MetaDataException("bad type");
        }
    }

    @VisibleForTesting
    @Nonnull
    CompletableFuture<Void> scrubIndexAsync(ScrubbingType type, AtomicLong count) {
        return AsyncUtil.composeHandle(
                getScrubber(type, count).buildIndexAsync(false),
                (ignore, ex) -> {
                    if (ex != null) {
                        throw FDBExceptions.wrapException(ex);
                    }
                    return AsyncUtil.DONE;
                });
    }

    /**
     * Scrub the index, find and repair dangling entries.
     * Synchronous version of {@link #scrubIndexAsync}.
     * @return found dangling index entries count.
     */
    public long scrubDanglingIndexEntries() {
        final AtomicLong danglingCount = new AtomicLong(0);
        runner.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, scrubIndexAsync(ScrubbingType.DANGLING, danglingCount));
        return danglingCount.get();
    }

    /**
     * Scrub the index, find and repair missing entries.
     * Synchronous version of {@link #scrubIndexAsync}.
     * @return found missing index entries count.
     */
    public long scrubMissingIndexEntries() {
        final AtomicLong missingCount = new AtomicLong(0);
        runner.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, scrubIndexAsync(ScrubbingType.MISSING, missingCount));
        return missingCount.get();
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the scrubbing policy.
     */
    public static class ScrubbingPolicy {
        public static final ScrubbingPolicy DEFAULT = new ScrubbingPolicy(1000, true, 0, false);
        private final int logWarningsLimit;
        private final boolean allowRepair;
        private final long entriesScanLimit;
        private final boolean ignoreIndexTypeCheck;

        public ScrubbingPolicy(int logWarningsLimit, boolean allowRepair, long entriesScanLimit,
                               boolean ignoreIndexTypeCheck) {

            this.logWarningsLimit = logWarningsLimit;
            this.allowRepair = allowRepair;
            this.entriesScanLimit = entriesScanLimit;
            this.ignoreIndexTypeCheck = ignoreIndexTypeCheck;
        }

        boolean allowRepair() {
            return allowRepair;
        }

        boolean ignoreIndexTypeCheck() {
            return ignoreIndexTypeCheck;
        }

        long getEntriesScanLimit() {
            return entriesScanLimit;
        }

        public int getLogWarningsLimit() {
            return logWarningsLimit;
        }

        /**
         * Create an scrubbing policy builder.
         * @return a new {@link ScrubbingPolicy} builder
         */
        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Builder for {@link ScrubbingPolicy}.
         *
         * <pre><code>
         * OnlineScrubber.ScrubbingPolicy.newBuilder().setLogWarningsLimit(100).setAllowRepair(true).build()
         * </code></pre>
         *
         */
        @API(API.Status.UNSTABLE)
        public static class Builder {
            int logWarningsLimit = 1000;
            boolean allowRepair = true;
            long entriesScanLimit = 0;
            boolean ignoreIndexTypeCheck = false;

            protected Builder() {
            }

            /**
             * Set a rigid limit on the max number of warnings to log..
             * If never called, the default is allowing (up to) 1000 warnings.
             * @param logWarningsLimit the max number of warnings to log.
             * @return this builder.
             */
            public Builder setLogWarningsLimit(final int logWarningsLimit) {
                this.logWarningsLimit = logWarningsLimit;
                return this;
            }


            /**
             * Set whether the scrubber, if it finds an error, will repair it.
             * @param val - if false, always report errors but do not repair.
             *            - if true (default), report errors up to requested limits, but always repair.
             * @return this builder.
             */
            public Builder setAllowRepair(boolean val) {
                this.allowRepair = val;
                return this;
            }

            /**
             * Set records/index entries scan limit.
             * Note that for efficiency, the scrubber reads and processes batches of entries (either index entries or records)
             * and will only check this limit after processing a batch. The scrubber will either stop when the number of
             * checked entries exceeds this limit or when it covered the whole range.
             * If stopped by limit, the next call will skip the ranges that already been checked (whether new entries were
             * written in these ranges or not). If the previous scrubber call had finished covering the whole range, the current
             * call will start a fresh scan.
             * @param entriesScanLimit - if 0 (default) or less, unlimited. Else return after scanned records count exceeds this limit.
             * @return this builder.
             */
            public Builder setEntriesScanLimit(long entriesScanLimit) {
                this.entriesScanLimit = entriesScanLimit;
                return this;
            }

            /**
             * Declare that the index to be scrubbed is valid for scrubbing, regardless of its type's name.
             *
             * Typically, this function is called to allow scrubbing of an index with a user-defined index type. If called,
             * it is the caller's responsibility to verify that the scrubbed index matches the required criteria, which are:
             * 1. For the dangling scrubber job, every index entry needs to contain the primary key of the record that
             *    generated it so that we can detect if that record is present.
             * 2. For the missing entry scrubber, the index key for the record needs to be present in the index.
             */
            public Builder ignoreIndexTypeCheck() {
                ignoreIndexTypeCheck = true;
                return this;
            }

            public ScrubbingPolicy build() {
                return new ScrubbingPolicy(logWarningsLimit, allowRepair, entriesScanLimit, ignoreIndexTypeCheck);
            }
        }
    }

    /**
     * Builder for {@link OnlineIndexScrubber}.
     *
     * <pre><code>
     * OnlineScrubber.newBuilder().setRecordStoreBuilder(recordStoreBuilder).setIndex(index).build()
     * </code></pre>
     *
     * <pre><code>
     * OnlineScrubber.newBuilder().setDatabase(fdb).setMetaData(metaData).setSubspace(subspace).setIndex(index).build()
     * </code></pre>
     *
     */
    @API(API.Status.UNSTABLE)
    public static class Builder {
        @Nullable
        protected FDBDatabaseRunner runner;
        @Nullable
        protected FDBRecordStore.Builder recordStoreBuilder;
        @Nullable
        protected Index index;
        @Nullable
        protected Collection<RecordType> recordTypes;
        @Nonnull
        protected Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader = old -> old;

        ScrubbingPolicy scrubbingPolicy = null;
        ScrubbingPolicy.Builder scrubbingPolicyBuilder = null;


        protected int limit = 2000;
        protected int maxWriteLimitBytes = OnlineIndexer.DEFAULT_WRITE_LIMIT_BYTES;
        protected int maxRetries = OnlineIndexer.DEFAULT_MAX_RETRIES;
        protected int recordsPerSecond = OnlineIndexer.DEFAULT_RECORDS_PER_SECOND;
        private long progressLogIntervalMillis = OnlineIndexer.DEFAULT_PROGRESS_LOG_INTERVAL;
        // Maybe the performance impact of this is low enough to be always enabled?
        private boolean trackProgress = true;
        private int increaseLimitAfter = OnlineIndexer.DO_NOT_RE_INCREASE_LIMIT;
        private long leaseLengthMillis = OnlineIndexer.DEFAULT_LEASE_LENGTH_MILLIS;

        protected Builder() {
        }

        /**
         * Set the runner that will be used to call into the database.
         *
         * Normally the runner is gotten from {@link #setDatabase} or {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
         * @param runner the runner that connects to the target database
         * @return this builder
         */
        public Builder setRunner(@Nullable FDBDatabaseRunner runner) {
            this.runner = runner;
            return this;
        }

        private void setRunnerDefaults() {
            setPriority(FDBTransactionPriority.BATCH);
        }

        /**
         * Set the database in which to run the scrubbing.
         *
         * Normally the database is gotten from {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
         * @param database the target database
         * @return this builder
         */
        public Builder setDatabase(@Nonnull FDBDatabase database) {
            this.runner = database.newRunner();
            setRunnerDefaults();
            return this;
        }

        /**
         * Set the record store builder that will be used to open record store instances for scrubbing.
         * @param recordStoreBuilder the record store builder
         * @return this builder
         * @see #setRecordStore
         */
        public Builder setRecordStoreBuilder(@Nonnull FDBRecordStore.Builder recordStoreBuilder) {
            this.recordStoreBuilder = recordStoreBuilder.copyBuilder().setContext(null);
            if (runner == null && recordStoreBuilder.getContext() != null) {
                runner = recordStoreBuilder.getContext().newRunner();
                setRunnerDefaults();
            }
            return this;
        }

        /**
         * Set the record store that will be used as a template to open record store instances for scrubbing.
         * @param recordStore the target record store
         * @return this builder
         */
        public Builder setRecordStore(@Nonnull FDBRecordStore recordStore) {
            recordStoreBuilder = recordStore.asBuilder().setContext(null);
            if (runner == null) {
                runner = recordStore.getRecordContext().newRunner();
                setRunnerDefaults();
            }
            return this;
        }

        /**
         * Set the index to be scrubbed.
         * @param index the index to be scrubbed
         * @return this builder
         */
        @Nonnull
        public Builder setIndex(@Nullable Index index) {
            this.index = index;
            return this;
        }

        /**
         * Set the index to be scrubbed.
         * @param indexName the index to be scrubbed
         * @return this builder
         */
        @Nonnull
        public Builder setIndex(@Nonnull String indexName) {
            this.index = getRecordMetaData().getIndex(indexName);
            return this;
        }

        /**
         * Set the explicit set of record types to be scrubbed.
         *
         * Normally, record types are inferred from {@link #setIndex}.
         * @param recordTypes the record types to be indexed or {@code null} to infer from the index
         * @return this builder
         */
        @Nonnull
        public Builder setRecordTypes(@Nullable Collection<RecordType> recordTypes) {
            this.recordTypes = recordTypes;
            return this;
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
        public Builder setConfigLoader(@Nonnull Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader) {
            this.configLoader = configLoader;
            return this;
        }

        /**
         * Set the maximum number of records to process in one transaction.
         *
         * The default limit is {@link OnlineIndexer#DEFAULT_LIMIT} = {@value OnlineIndexer#DEFAULT_LIMIT}.
         * Note {@link #setConfigLoader(Function)} is the recommended way of loading online index builder's parameters
         * and the values set by this method will be overwritten if the supplier is set.
         * @param limit the maximum number of records to process in one transaction
         * @return this builder
         */
        @Nonnull
        public Builder setLimit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Set the approximate maximum transaction write size. Note that the actual size might be up to one record
         * bigger than this value - transactions started as part of adding missing index entries will be committed after
         * they exceed this size, and a new transaction will be started.
         * he default limit is {@link OnlineIndexer#DEFAULT_WRITE_LIMIT_BYTES} = {@value OnlineIndexer#DEFAULT_WRITE_LIMIT_BYTES}.
         * @param max the desired max write size
         * @return this builder
         */
        @Nonnull
        public Builder setMaxWriteLimitBytes(int max) {
            this.maxWriteLimitBytes = max;
            return this;
        }

        /**
         * Set the maximum number of times to retry a single range rebuild.
         * This retry is on top of the retries caused by {@link #getMaxAttempts()}, it and will also retry for other error
         * codes, such as {@code transaction_too_large}.
         *
         * The default number of retries is {@link OnlineIndexer#DEFAULT_MAX_RETRIES} = {@value OnlineIndexer#DEFAULT_MAX_RETRIES}.
         * Note {@link #setConfigLoader(Function)} is the recommended way of loading online index builder's parameters
         * and the values set by this method will be overwritten if the supplier is set.
         * @param maxRetries the maximum number of times to retry a single range rebuild
         * @return this builder
         */
        @Nonnull
        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Set the maximum number of records to process in a single second.
         *
         * The default number of retries is {@link OnlineIndexer#DEFAULT_RECORDS_PER_SECOND} = {@value OnlineIndexer#DEFAULT_RECORDS_PER_SECOND}.
         * Note {@link #setConfigLoader(Function)} is the recommended way of loading online index builder's parameters
         * and the values set by this method will be overwritten if the supplier is set.
         * @param recordsPerSecond the maximum number of records to process in a single second.
         * @return this builder
         */
        @Nonnull
        public Builder setRecordsPerSecond(int recordsPerSecond) {
            this.recordsPerSecond = recordsPerSecond;
            return this;
        }

        /**
         * Get the timer used in {@link #scrubIndexAsync}.
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
         * Set the timer used in {@link #scrubIndexAsync}.
         * @param timer timer to use
         * @return this builder
         */
        @Nonnull
        public Builder setTimer(@Nullable FDBStoreTimer timer) {
            if (runner == null) {
                throw new MetaDataException("timer can only be set after runner has been set");
            }
            runner.setTimer(timer);
            return this;
        }

        /**
         * Get the logging context used in {@link #scrubIndexAsync}.
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
         * Set the logging context used in {@link #scrubIndexAsync}.
         * @param mdcContext the logging context to set while running
         * @return this builder
         * @see FDBDatabase#openContext(Map,FDBStoreTimer)
         */
        @Nonnull
        public Builder setMdcContext(@Nullable Map<String, String> mdcContext) {
            if (runner == null) {
                throw new MetaDataException("logging context can only be set after runner has been set");
            }
            runner.setMdcContext(mdcContext);
            return this;
        }

        /**
         * Get the acceptable staleness bounds for transactions used by this scrub/fix. By default, this
         * is set to {@code null}, which indicates that the transaction should not used any cached version
         * at all.
         * @return the acceptable staleness bounds for transactions used by this scrub/fix
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
         * Set the acceptable staleness bounds for transactions used by this scrub/fix. For index scrub, essentially
         * all operations will read and write data in the same transaction, so it is safe to set this value
         * to use potentially stale read versions, though that can potentially result in more transaction conflicts.
         * For performance reasons, it is generally advised that this only be provided an acceptable staleness bound
         * that might use a cached commit if the database tracks the latest commit version in addition to the read
         * version. This is to ensure that the online indexer see its own commits, and it should not be required
         * for correctness, but the online indexer may perform additional work if this is not set.
         *
         * @param weakReadSemantics the acceptable staleness bounds for transactions used by this scrub
         * @return this builder
         * @see FDBRecordContext#getWeakReadSemantics()
         * @see FDBDatabase#setTrackLastSeenVersion(boolean)
         */
        @Nonnull
        public Builder setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
            if (runner == null) {
                throw new MetaDataException("weak read semantics can only be set after runner has been set");
            }
            runner.setWeakReadSemantics(weakReadSemantics);
            return this;
        }

        /**
         * Get the priority of transactions used for this index scrub. By default, this will be
         * {@link FDBTransactionPriority#BATCH}.
         * @return the priority of transactions used for this index scrub
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
         * Set the priority of transactions used for this index scrubbing. In general, index scrubbing should run
         * using the {@link FDBTransactionPriority#BATCH BATCH} priority level as their work is generally
         * discretionary and not time sensitive. However, in certain circumstances, it may be
         * necessary to run at the higher {@link FDBTransactionPriority#DEFAULT DEFAULT} priority level.
         * For example, if a missing index is causing some queries to perform additional, unnecessary work that
         * is overwhelming the database, it may be necessary to scrub the index at {@code DEFAULT} priority
         * in order to lessen the load induced by those queries on the cluster.
         *
         * @param priority the priority of transactions used for this index scrub
         * @return this builder
         * @see FDBRecordContext#getPriority()
         */
        @Nonnull
        public Builder setPriority(@Nonnull FDBTransactionPriority priority) {
            if (runner == null) {
                throw new MetaDataException("transaction priority can only be set after runner has been set");
            }
            runner.setPriority(priority);
            return this;
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
        public Builder setMaxAttempts(int maxAttempts) {
            if (runner == null) {
                throw new MetaDataException("maximum attempts can only be set after runner has been set");
            }
            runner.setMaxAttempts(maxAttempts);
            return this;
        }

        /**
         * Set the number of successful range scrubs before re-increasing the number of records to process in a single
         * transaction. The number of records to process in a single transaction will never go above {@link #limit}.
         * By default this is -1}, which means it will not re-increase after successes.
         * <p>
         * Note {@link #setConfigLoader(Function)} is the recommended way of loading online index builder's parameters
         * and the values set by this method will be overwritten if the supplier is set.
         * </p>
         * @param increaseLimitAfter the number of successful range scrubbed before increasing the number of records
         * processed in a single transaction
         * @return this builder
         */
        public Builder setIncreaseLimitAfter(int increaseLimitAfter) {
            this.increaseLimitAfter = increaseLimitAfter;
            return this;
        }

        /**
         * Get the number of successful range scrubbed before re-increasing the number of records to process in a single
         * transaction.
         * By default this is -1, which means it will not re-increase after successes.
         * @return the number of successful range scrubbed before increasing the number of records processed in a single
         * transaction
         * @see #limit
         */
        public int getIncreaseLimitAfter() {
            return increaseLimitAfter;
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
        public Builder setMaxDelayMillis(long maxDelayMillis) {
            if (runner == null) {
                throw new MetaDataException("maximum delay can only be set after runner has been set");
            }
            runner.setMaxDelayMillis(maxDelayMillis);
            return this;
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
        public Builder setInitialDelayMillis(long initialDelayMillis) {
            if (runner == null) {
                throw new MetaDataException("initial delay can only be set after runner has been set");
            }
            runner.setInitialDelayMillis(initialDelayMillis);
            return this;
        }

        /**
         * Get the minimum time between successful progress logs when building across transactions.
         * Negative will not log at all, 0 will log after every commit.
         * @return the minimum time between successful progress logs in milliseconds
         * @see #setProgressLogIntervalMillis(long) for more information on the format of the log
         */
        public long getProgressLogIntervalMillis() {
            return progressLogIntervalMillis;
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
         *         multi-transaction methods (e.g. {@link #scrubIndexAsync} ()}. The transactional methods (i.e., the methods that
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
         * Note {@link #setConfigLoader(Function)} is the recommended way of loading online index builder's parameters
         * and the values set by this method will be overwritten if the supplier is set.
         * </p>
         *
         * @param millis the number of milliseconds to wait between successful logs
         * @return this builder
         */
        public Builder setProgressLogIntervalMillis(long millis) {
            progressLogIntervalMillis = millis;
            return this;
        }

        /**
         * Set whether or not to track the index scrubbed progress by updating the number of records successfully scanned
         * and processed. The progress is persisted and can be accessed by {@link IndexBuildState#loadIndexBuildStateAsync(FDBRecordStoreBase, Index)}.
         * <p>
         * This setting does not affect the setting at {@link #setProgressLogIntervalMillis(long)}.
         * </p>
         * @param trackProgress track progress if true, otherwise false
         * @return this builder
         */
        public Builder setTrackProgress(boolean trackProgress) {
            this.trackProgress = trackProgress;
            return this;
        }

        /**
         * Set the {@link IndexMaintenanceFilter} to use while building the index.
         *
         * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
         * @param indexMaintenanceFilter the index filter to use
         * @return this builder
         */
        public Builder setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
            if (recordStoreBuilder == null) {
                throw new MetaDataException("index filter can only be set after record store builder has been set");
            }
            recordStoreBuilder.setIndexMaintenanceFilter(indexMaintenanceFilter);
            return this;
        }

        /**
         * Set the {@link RecordSerializer} to use while building the index.
         *
         * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
         * @param serializer the serializer to use
         * @return this builder
         */
        public Builder setSerializer(@Nonnull RecordSerializer<Message> serializer) {
            if (recordStoreBuilder == null) {
                throw new MetaDataException("serializer can only be set after record store builder has been set");
            }
            recordStoreBuilder.setSerializer(serializer);
            return this;
        }

        /**
         * Set the store format version to use while building the index.
         *
         * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
         * @param formatVersion the format version to use
         * @return this builder
         */
        public Builder setFormatVersion(int formatVersion) {
            if (recordStoreBuilder == null) {
                throw new MetaDataException("format version can only be set after record store builder has been set");
            }
            recordStoreBuilder.setFormatVersion(formatVersion);
            return this;
        }

        @Nonnull
        private RecordMetaData getRecordMetaData() {
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
        public Builder setMetaData(@Nonnull RecordMetaDataProvider metaDataProvider) {
            if (recordStoreBuilder == null) {
                recordStoreBuilder = FDBRecordStore.newBuilder();
            }
            recordStoreBuilder.setMetaDataProvider(metaDataProvider);
            return this;
        }

        /**
         * Set the subspace of the record store in which to scrub the index.
         * @param subspaceProvider subspace to use
         * @return this builder
         */
        public Builder setSubspaceProvider(@Nonnull SubspaceProvider subspaceProvider) {
            if (recordStoreBuilder == null) {
                recordStoreBuilder = FDBRecordStore.newBuilder();
            }
            recordStoreBuilder.setSubspaceProvider(subspaceProvider);
            return this;
        }

        /**
         * Set the subspace of the record store in which to scrub the index.
         * @param subspace subspace to use
         * @return this builder
         */
        public Builder setSubspace(@Nonnull Subspace subspace) {
            if (recordStoreBuilder == null) {
                recordStoreBuilder = FDBRecordStore.newBuilder();
            }
            recordStoreBuilder.setSubspace(subspace);
            return this;
        }

        /**
         * Set the lease length in milliseconds if the synchronized session is used. By default this is 10_000.
         * @param leaseLengthMillis length between last access and lease's end time in milliseconds
         * @return this builder
         */
        public Builder setLeaseLengthMillis(long leaseLengthMillis) {
            this.leaseLengthMillis = leaseLengthMillis;
            return this;
        }

        /**
         * Add a {@link ScrubbingPolicy} policy.
         * A scrubbing job will validate (and fix, if applicable) index entries of readable indexes. It is designed to support an ongoing
         * index consistency verification.
         * @param scrubbingPolicy see {@link ScrubbingPolicy}
         * @return this Builder
         */
        public Builder setScrubbingPolicy(@Nonnull final ScrubbingPolicy scrubbingPolicy) {
            this.scrubbingPolicyBuilder = null;
            this.scrubbingPolicy = scrubbingPolicy;
            return this;
        }

        /**
         * Add a {@link ScrubbingPolicy.Builder} policy builder.
         * A scrubbing job will validate (and fix, if applicable) index entries of readable indexes. It is designed to support an ongoing
         * index consistency verification.
         * @param scrubbingPolicyBuilder see {@link ScrubbingPolicy.Builder}
         * @return this Builder
         */
        public Builder setScrubbingPolicy(@Nonnull final ScrubbingPolicy.Builder scrubbingPolicyBuilder) {
            this.scrubbingPolicy = null;
            this.scrubbingPolicyBuilder = scrubbingPolicyBuilder;
            return this;
        }

        /**
         * Build an {@link OnlineIndexScrubber}.
         * @return a new online indexer
         */
        public OnlineIndexScrubber build() {
            validate();
            OnlineIndexer.Config conf = new OnlineIndexer.Config(limit, maxRetries, recordsPerSecond,
                    progressLogIntervalMillis, increaseLimitAfter, maxWriteLimitBytes, OnlineIndexer.Config.UNLIMITED_TIME);
            if (scrubbingPolicyBuilder != null) {
                scrubbingPolicy = scrubbingPolicyBuilder.build();
            }
            if (scrubbingPolicy == null) {
                scrubbingPolicy = ScrubbingPolicy.DEFAULT;
            }
            return new OnlineIndexScrubber(runner, recordStoreBuilder, index, recordTypes,
                    configLoader, conf,
                    leaseLengthMillis, trackProgress,
                    scrubbingPolicy);
        }

        protected void validate() {
            validateIndex();
            validateLimits();
        }

        // Check pointer equality to make sure other objects really came from given metaData.
        // Also resolve record types to use if not specified.
        private void validateIndex() {
            if (index == null) {
                throw new MetaDataException("index must be set");
            }
            final RecordMetaData metaData = getRecordMetaData();
            if (!metaData.hasIndex(index.getName()) || index != metaData.getIndex(index.getName())) {
                throw new MetaDataException("Index " + index.getName() + " not contained within specified metadata");
            }
            if (recordTypes == null) {
                recordTypes = metaData.recordTypesForIndex(index);
            } else {
                for (RecordType recordType : recordTypes) {
                    if (recordType != metaData.getIndexableRecordType(recordType.getName())) {
                        throw new MetaDataException("Record type " + recordType.getName() + " not contained within specified metadata");
                    }
                }
            }
            if (recordTypes.stream().anyMatch(RecordType::isSynthetic)) {
                // The (stored) types to scan, not the (synthetic) types that are indexed.
                recordTypes = new SyntheticRecordPlanner(metaData, new RecordStoreState(null, null))
                        .storedRecordTypesForIndex(index, recordTypes);
            }
        }

        private void validateLimits() {
            checkPositive(maxRetries, "maximum retries");
            checkPositive(limit, "record limit");
            checkPositive(recordsPerSecond, "records per second value");
        }

        private static void checkPositive(int value, String desc) {
            if (value <= 0) {
                throw new RecordCoreException("Non-positive value " + value + " given for " + desc);
            }
        }
    }
}
