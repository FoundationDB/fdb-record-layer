/*
 * OnlineIndexOperationConfig.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;

import javax.annotation.Nonnull;

/**
 * A holder for the mutable configuration parameters needed to rebuild an online index. These parameters are
 * designed to be safe to be updated while a build is running.
 */
@API(API.Status.UNSTABLE)
public class OnlineIndexOperationConfig {
    /**
     * Default number of records to attempt to run in a single transaction.
     */
    public static final int DEFAULT_LIMIT = 100;
    /**
     * Default transaction write size limit. Note that the actual write might be "a little" bigger.
     */
    public static final int DEFAULT_WRITE_LIMIT_BYTES = 900_000;
    /**
     * Default limit to the number of records to attempt in a single second.
     */
    public static final int DEFAULT_RECORDS_PER_SECOND = 10_000;
    /**
     * Default number of times to retry a single range rebuild.
     */
    public static final int DEFAULT_MAX_RETRIES = 100;
    /**
     * Default interval to be logging successful progress in millis when building across transactions.
     * {@code -1} means it will not log.
     */
    public static final int DEFAULT_PROGRESS_LOG_INTERVAL = -1;
    /**
     * Default synchronized session lease time in milliseconds. This allows a lock expiration, if the online operation stops unexpectedly.
     */
    public static final long DEFAULT_LEASE_LENGTH_MILLIS = 10_000;

    /**
     * If/When the index operation exceeds this transaction time, it would attempt commiting and continuing the iteration in the following transactions.
     */
    public static final long DEFAULT_TRANSACTION_TIME_LIMIT = 4_000;
    /**
     * If {@link OnlineIndexer.Builder#getIncreaseLimitAfter()} is this value, the limit will not go back up, no matter how many
     * successes there are.
     * This is the default value.
     */
    public static final int DO_NOT_RE_INCREASE_LIMIT = -1;

    private final int maxLimit;
    private final int initialLimit;
    private final int maxWriteLimitBytes;
    private final int maxRetries;
    private final int recordsPerSecond;
    private final long progressLogIntervalMillis;
    private final int increaseLimitAfter;
    private final long timeLimitMilliseconds;
    private final long transactionTimeLimitMilliseconds;
    private final boolean useSynchronizedSession;
    private final long leaseLengthMillis;

    public static final long UNLIMITED_TIME = 0;

    OnlineIndexOperationConfig(int maxLimit, int initialLimit, int maxRetries, int recordsPerSecond, long progressLogIntervalMillis, int increaseLimitAfter,
                               int maxWriteLimitBytes, long timeLimitMilliseconds, long transactionTimeLimitMilliseconds,
                               boolean useSynchronizedSession, long leaseLengthMillis) {
        this.maxLimit = maxLimit;
        this.initialLimit = initialLimit;
        this.maxRetries = maxRetries;
        this.recordsPerSecond = recordsPerSecond;
        this.progressLogIntervalMillis = progressLogIntervalMillis;
        this.increaseLimitAfter = increaseLimitAfter;
        this.maxWriteLimitBytes = maxWriteLimitBytes;
        this.timeLimitMilliseconds = timeLimitMilliseconds;
        this.transactionTimeLimitMilliseconds = transactionTimeLimitMilliseconds;
        this.useSynchronizedSession = useSynchronizedSession;
        this.leaseLengthMillis = leaseLengthMillis;
    }

    /**
     * Get the maximum number of records to process in one transaction.
     *
     * @return the maximum number of records to process in one transaction
     */
    public int getMaxLimit() {
        return maxLimit;
    }

    /**
     * Get the initial number of records to process in one transaction.
     *
     * @return the initial number of records to process in one transaction
     */
    public int getInitialLimit() {
        return initialLimit > 0 ? Math.min(initialLimit, maxLimit) : maxLimit;
    }

    /**
     * Get the maximum number of times to retry a single range rebuild.
     *
     * @return the maximum number of times to retry a single range rebuild
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Get the maximum number of records to process in a single second.
     *
     * @return the maximum number of records to process in a single second
     */
    public int getRecordsPerSecond() {
        return recordsPerSecond;
    }

    /**
     * Get the minimum time between successful progress logs when building across transactions.
     * Negative will not log at all, 0 will log after every commit.
     *
     * @return the minimum time between successful progress logs in milliseconds
     */
    public long getProgressLogIntervalMillis() {
        return progressLogIntervalMillis;
    }

    /**
     * Get the number of successful range builds before re-increasing the number of records to process in a single
     * transaction.
     * By default this is {@link #DO_NOT_RE_INCREASE_LIMIT}, which means it will not re-increase after successes.
     *
     * @return the number of successful range builds before increasing the number of records processed in a single
     * transaction
     */
    public int getIncreaseLimitAfter() {
        return increaseLimitAfter;
    }

    /**
     * Stop scanning if the write size (bytes) becomes bigger that this value.
     *
     * @return max write quota for a single transaction
     */
    public long getMaxWriteLimitBytes() {
        return maxWriteLimitBytes;
    }

    /**
     * Exit with exception if this limit is exceeded (checked after each non-final transaction).
     *
     * @return time limit in millisecond
     */
    public long getTimeLimitMilliseconds() {
        return timeLimitMilliseconds;
    }

    /**
     * Get the time quota for a single transaction.
     *
     * @return the time quota for a single transaction
     */
    public long getTransactionTimeLimitMilliseconds() {
        return transactionTimeLimitMilliseconds;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    public boolean shouldUseSynchronizedSession() {
        return useSynchronizedSession;
    }

    public long getLeaseLengthMillis() {
        return leaseLengthMillis;
    }

    /**
     * To create a builder for the given config.
     *
     * @return a {@link Builder}
     */
    @Nonnull
    public Builder toBuilder() {
        return OnlineIndexOperationConfig.newBuilder()
                .setMaxLimit(this.maxLimit)
                .setInitialLimit(this.initialLimit)
                .setWriteLimitBytes(this.maxWriteLimitBytes)
                .setIncreaseLimitAfter(this.increaseLimitAfter)
                .setProgressLogIntervalMillis(this.progressLogIntervalMillis)
                .setRecordsPerSecond(this.recordsPerSecond)
                .setMaxRetries(this.maxRetries)
                .setTimeLimitMilliseconds(timeLimitMilliseconds)
                .setTransactionTimeLimitMilliseconds(this.transactionTimeLimitMilliseconds)
                .setUseSynchronizedSession(useSynchronizedSession)
                .setLeaseLengthMillis(leaseLengthMillis);
    }

    /**
     * A builder for {@link OnlineIndexOperationConfig}. These are the mutable configuration parameters used while
     * building indexes and are
     * designed to be safe to be updated while a build is running.
     */
    @API(API.Status.UNSTABLE)
    public static class Builder {
        private int maxLimit = DEFAULT_LIMIT;
        private int initialLimit = 0;
        private int maxWriteLimitBytes = DEFAULT_WRITE_LIMIT_BYTES;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private int recordsPerSecond = DEFAULT_RECORDS_PER_SECOND;
        private long progressLogIntervalMillis = DEFAULT_PROGRESS_LOG_INTERVAL;
        private int increaseLimitAfter = DO_NOT_RE_INCREASE_LIMIT;
        private long timeLimitMilliseconds = UNLIMITED_TIME;
        private long transactionTimeLimitMilliseconds = DEFAULT_TRANSACTION_TIME_LIMIT;
        private long leaseLengthMillis = DEFAULT_LEASE_LENGTH_MILLIS;
        private boolean useSynchronizedSession = true;

        protected Builder() {

        }

        /**
         * Get the maximum number of records to process in one transaction.
         *
         * @return the maximum number of records to process in one transaction
         *
         * @see #setMaxLimit(int)
         */
        public int getMaxLimit() {
            return maxLimit;
        }

        /**
         * Set the maximum number of records to process in one transaction.
         * The default limit is {@link #DEFAULT_LIMIT} = {@value #DEFAULT_LIMIT}.
         *
         * @param limit the maximum number of records to process in one transaction
         *
         * @return this builder
         */
        @Nonnull
        public Builder setMaxLimit(int limit) {
            this.maxLimit = limit;
            return this;
        }

        /**
         * Get the initial number of records to process in one transaction.
         *
         * @return the initial number of records to process in one transaction
         *
         * @see #setInitialLimit(int)
         */
        public int getInitialLimit() {
            return initialLimit;
        }

        /**
         * Set the initial number of records to process in one transaction. This can be useful to avoid
         * starting the indexing with the maximum limit (set by {@link #setMaxLimit(int)}), which may cause timeouts.
         * The default initial limit is {@link #DEFAULT_LIMIT} = {@value #DEFAULT_LIMIT}.
         *
         * @param limit the initial number of records to process in one transaction
         *
         * @return this builder
         */
        @Nonnull
        public Builder setInitialLimit(int limit) {
            this.initialLimit = limit;
            return this;
        }

        /**
         * Stop scanning if the write size (bytes) becomes bigger that this value.
         *
         * @return max write quota for a single transaction
         *
         * @see #setWriteLimitBytes(int)
         */
        public int getWriteLimitBytes() {
            return maxWriteLimitBytes;
        }

        /**
         * Set the maximum transaction size in a single transaction.
         * The default limit is {@link #DEFAULT_WRITE_LIMIT_BYTES} = {@value #DEFAULT_WRITE_LIMIT_BYTES}.
         *
         * @param limit the approximate maximum write size in one transaction
         *
         * @return this builder
         */
        @Nonnull
        public Builder setWriteLimitBytes(int limit) {
            this.maxWriteLimitBytes = limit;
            return this;
        }

        /**
         * Get the maximum number of times to retry a single range rebuild.
         *
         * @return the maximum number of times to retry a single range rebuild
         *
         * @see #setMaxRetries(int)
         */
        public int getMaxRetries() {
            return maxRetries;
        }

        /**
         * Set the maximum number of times to retry a single range rebuild.
         * <p>
         * The default number of retries is {@link #DEFAULT_MAX_RETRIES} = {@value #DEFAULT_MAX_RETRIES}.
         *
         * @param maxRetries the maximum number of times to retry a single range rebuild
         *
         * @return this builder
         */
        @Nonnull
        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Get the maximum number of records to process in a single second.
         *
         * @return the maximum number of records to process in a single second
         *
         * @see #setRecordsPerSecond(int)
         */
        public int getRecordsPerSecond() {
            return recordsPerSecond;
        }

        /**
         * Set the maximum number of records to process in a single second.
         * <p>
         * The default number of retries is {@link #DEFAULT_RECORDS_PER_SECOND} = {@value #DEFAULT_RECORDS_PER_SECOND}.
         *
         * @param recordsPerSecond the maximum number of records to process in a single second
         *
         * @return this builder
         */
        @Nonnull
        public Builder setRecordsPerSecond(int recordsPerSecond) {
            this.recordsPerSecond = recordsPerSecond;
            return this;
        }

        /**
         * Get the minimum time between successful progress logs when building across transactions.
         * Negative will not log at all, 0 will log after every commit.
         *
         * @return the minimum time between successful progress logs in milliseconds
         *
         * @see #setProgressLogIntervalMillis(long)
         */
        public long getProgressLogIntervalMillis() {
            return progressLogIntervalMillis;
        }

        /**
         * Set the minimum time between successful progress logs when building across transactions.
         * Negative will not log at all, 0 will log after every commit.
         *
         * @param progressLogIntervalMillis the number of milliseconds to wait between successful logs
         *
         * @return this builder
         */
        @Nonnull
        public Builder setProgressLogIntervalMillis(long progressLogIntervalMillis) {
            this.progressLogIntervalMillis = progressLogIntervalMillis;
            return this;
        }

        /**
         * Get the number of successful range builds before re-increasing the number of records to process in a single
         * transaction.
         * By default, this is {@link #DO_NOT_RE_INCREASE_LIMIT}, which means it will not re-increase after successes.
         *
         * @return the number of successful range builds before increasing the number of records processed in a single
         * transaction
         *
         * @see #setIncreaseLimitAfter(int)
         */
        public int getIncreaseLimitAfter() {
            return increaseLimitAfter;
        }

        /**
         * Set the number of successful range builds before re-increasing the number of records to process in a single
         * transaction. The number of records to process in a single transaction will never go above
         * {@link #getMaxLimit()}.
         * By default this is {@link #DO_NOT_RE_INCREASE_LIMIT}, which means it will not re-increase after successes.
         *
         * @param increaseLimitAfter the number of successful range builds before increasing the number of records
         * processed in a single transaction
         *
         * @return this builder
         */
        @Nonnull
        public Builder setIncreaseLimitAfter(int increaseLimitAfter) {
            this.increaseLimitAfter = increaseLimitAfter;
            return this;
        }

        /**
         * Exit with exception if this limit is exceeded (checked after each non-final transaction).
         *
         * @return time limit in milliseconds
         *
         * @see #setTimeLimitMilliseconds(long)
         */
        public long getTimeLimitMilliseconds() {
            return timeLimitMilliseconds;
        }

        /**
         * Set the time limit. The indexer will exit with a proper exception if this time is exceeded after a
         * non-final transaction.
         *
         * @param timeLimitMilliseconds the time limit in milliseconds
         *
         * @return this builder
         */
        @Nonnull
        public Builder setTimeLimitMilliseconds(long timeLimitMilliseconds) {
            if (timeLimitMilliseconds < 0) {
                timeLimitMilliseconds = UNLIMITED_TIME;
            }
            this.timeLimitMilliseconds = timeLimitMilliseconds;
            return this;
        }

        /**
         * Get the time quota for a single transaction.
         *
         * @return the time quota for a single transaction
         *
         * @see #setTransactionTimeLimitMilliseconds(long)
         */
        public long getTransactionTimeLimitMilliseconds() {
            return transactionTimeLimitMilliseconds;
        }

        /**
         * Set the time limit for a single transaction. If this limit is exceeded, the indexer will commit the
         * transaction and start a new one. This can be useful to avoid timeouts while scanning many records
         * in each transaction.
         * A non-positive value implies unlimited.
         * Note that this limit, if reached, will be exceeded by an Order(1) overhead time. Keeping some margins might
         * be
         * a good idea.
         * The default value is 4,000 (4 seconds), matches fdb's default 5 seconds transaction limit.
         *
         * @param timeLimitMilliseconds the time limit, per transaction, in milliseconds
         *
         * @return this builder
         */
        @Nonnull
        public Builder setTransactionTimeLimitMilliseconds(long timeLimitMilliseconds) {
            this.transactionTimeLimitMilliseconds = timeLimitMilliseconds;
            return this;
        }

        /**
         * Set the use of a synchronized session during the index operation. Synchronized sessions help performing
         * the multiple transactions operation in a resource efficient way.
         * Normally this should be {@code true}.
         *
         * @see SynchronizedSessionRunner
         * @param useSynchronizedSession use synchronize session if true, otherwise false
         * @return this builder
         */
        public Builder setUseSynchronizedSession(boolean useSynchronizedSession) {
            this.useSynchronizedSession = useSynchronizedSession;
            return this;
        }

        /**
         * Set the lease length in milliseconds if the synchronized session is used. By default this is {@link #DEFAULT_LEASE_LENGTH_MILLIS}.
         * @see #setUseSynchronizedSession(boolean)
         * @see com.apple.foundationdb.synchronizedsession.SynchronizedSession
         * @param leaseLengthMillis length between last access and lease's end time in milliseconds
         * @return this builder
         */
        public Builder setLeaseLengthMillis(long leaseLengthMillis) {
            this.leaseLengthMillis = leaseLengthMillis;
            return this;
        }

        /**
         * Build a {@link OnlineIndexOperationConfig}.
         *
         * @return a new Config object needed by {@link OnlineIndexer}
         */
        @Nonnull
        public OnlineIndexOperationConfig build() {
            return new OnlineIndexOperationConfig(maxLimit, initialLimit, maxRetries, recordsPerSecond, progressLogIntervalMillis, increaseLimitAfter,
                    maxWriteLimitBytes, timeLimitMilliseconds, transactionTimeLimitMilliseconds,
                    useSynchronizedSession, leaseLengthMillis);
        }
    }
}
