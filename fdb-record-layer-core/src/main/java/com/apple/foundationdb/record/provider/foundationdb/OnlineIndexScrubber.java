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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

/**
 * Scan indexes for problems and optionally report or repair.
 */
@API(API.Status.UNSTABLE)
public class OnlineIndexScrubber implements AutoCloseable {

    @Nonnull private final IndexingCommon common;
    @Nonnull private final FDBDatabaseRunner runner;
    @Nonnull private final ScrubbingPolicy scrubbingPolicy;

    /**
     * The type of problem to scan for.
     */
    public enum ScrubbingType {
        DANGLING,
        MISSING
    }

    @SuppressWarnings("squid:S00107")
    OnlineIndexScrubber(@Nonnull FDBDatabaseRunner runner,
                        @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                        @Nonnull Index index,
                        @Nonnull Collection<RecordType> recordTypes,
                        @Nonnull UnaryOperator<OnlineIndexOperationConfig> configLoader,
                        @Nonnull OnlineIndexOperationConfig config,
                        boolean trackProgress,
                        @Nonnull OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy) {

        this.runner = runner;
        this.scrubbingPolicy = scrubbingPolicy;
        this.common = new IndexingCommon(runner, recordStoreBuilder,
                Collections.singletonList(index), recordTypes, configLoader, config,
                trackProgress);
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
                getScrubber(type, count).buildIndexAsync(false, common.config.shouldUseSynchronizedSession()),
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
             * @return this builder
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
    public static class Builder extends OnlineIndexOperationBaseBuilder<Builder> {
        @Nullable
        protected Index index;
        @Nullable
        protected Collection<RecordType> recordTypes;

        ScrubbingPolicy scrubbingPolicy = null;
        ScrubbingPolicy.Builder scrubbingPolicyBuilder = null;

        protected Builder() {
            setLimit(2000);
        }

        @Override
        Builder self() {
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
            OnlineIndexOperationConfig conf = getConfig();
            if (scrubbingPolicyBuilder != null) {
                scrubbingPolicy = scrubbingPolicyBuilder.build();
            }
            if (scrubbingPolicy == null) {
                scrubbingPolicy = ScrubbingPolicy.DEFAULT;
            }
            return new OnlineIndexScrubber(getRunner(), getRecordStoreBuilder(), index, recordTypes,
                    getConfigLoader(), conf, isTrackProgress(), scrubbingPolicy);
        }

        protected void validate() {
            validateIndex();
            validateLimits();
        }

        // Check pointer equality to make sure other objects really came from given metaData.
        // Also resolve record types to use if not specified.
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        private void validateIndex() {
            if (index == null) {
                throw new MetaDataException("index must be set");
            }
            final RecordMetaData metaData = getRecordMetaData();
            if (!metaData.hasIndex(index.getName()) || index != metaData.getIndex(index.getName())) {
                throw new MetaDataException("Index " + index.getName() + " not contained within specified metadata");
            }
            if (recordTypes != null) {
                for (RecordType recordType : recordTypes) {
                    if (recordType != metaData.getIndexableRecordType(recordType.getName())) {
                        throw new MetaDataException("Record type " + recordType.getName() + " not contained within specified metadata");
                    }
                }
            }
        }
    }
}
