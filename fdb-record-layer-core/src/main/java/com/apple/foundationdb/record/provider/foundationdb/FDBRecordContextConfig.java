/*
 * FDBRecordContextConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreArgumentException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * A configuration struct that can be used to set various options on an {@link FDBRecordContext}. Instances
 * of this configuration object can be passed to {@link FDBDatabase#openContext(FDBRecordContextConfig)}
 * to create a new transaction with various parameters set according to the values specified here.
 */
public class FDBRecordContextConfig {
    @Nullable
    private final Map<String, String> mdcContext;
    @Nullable
    private final FDBStoreTimer timer;
    @Nullable
    private final FDBDatabase.WeakReadSemantics weakReadSemantics;
    @Nonnull
    private final FDBTransactionPriority priority;
    @Nullable
    private final String transactionId;
    private final long transactionTimeoutMillis;
    private final boolean enableAssertions;

    private FDBRecordContextConfig(@Nonnull Builder builder) {
        this.mdcContext = builder.mdcContext;
        this.timer = builder.timer;
        this.weakReadSemantics = builder.weakReadSemantics;
        this.priority = builder.priority;
        this.transactionId = builder.transactionId;
        this.transactionTimeoutMillis = builder.transactionTimeoutMillis;
        this.enableAssertions = builder.enableAssertions;
    }

    /**
     * Get the MDC context used to set additional keys and values when logging.
     *
     * @return the MDC context to use when logging
     */
    @Nullable
    public Map<String, String> getMdcContext() {
        return mdcContext;
    }

    /**
     * Get the timer to use to instrument events. This is especially useful for tracking
     * and timing operations that interact with the database.
     *
     * @return the timer to use to instrument events
     */
    @Nullable
    public FDBStoreTimer getTimer() {
        return timer;
    }

    /**
     * Get the {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics}
     * configuration used when creating the transaction. This is used to determine whether this
     * transaction should be created with a cached read version and whether this transaction should
     * set the {@link com.apple.foundationdb.TransactionOptions#setCausalReadRisky()} option.
     *
     * @return the {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics} to use when creating the transaction
     */
    @Nullable
    public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        return weakReadSemantics;
    }

    /**
     * Get the priority for the created transaction. For more details on that option, see
     * {@link FDBTransactionPriority}.
     *
     * @return the priority for the created transaction
     */
    @Nonnull
    public FDBTransactionPriority getPriority() {
        return priority;
    }

    /**
     * Get the ID to use for the transaction in FDB logs. See {@link FDBRecordContext}
     * for more details.
     *
     * @return the ID to use for the transaction in FDB logs
     */
    @Nullable
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Get the configured transaction timeout time in milliseconds. If set to {@link FDBDatabaseFactory#DEFAULT_TR_TIMEOUT_MILLIS},
     * then the created transaction will use the default from the {@link FDBDatabaseFactory}.
     *
     * @return the transaction timeout time in milliseconds
     */
    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    /**
     * Returns whether or not internal correctness assertions are enabled.
     * @return whether or not internal correctness assertions are enabled
     */
    public boolean areAssertionsEnabled() {
        return enableAssertions;
    }

    /**
     * Get a new builder for this class.
     *
     * @return a new builder for this class
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Convert the current configuration to a builder. This will set all options in the builder to their
     * current values in this configuration object.
     *
     * @return a new builder based on this configuration object
     */
    @Nonnull
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * A builder of {@link FDBRecordContextConfig}s using the standard builder pattern.
     */
    public static class Builder {
        @Nullable
        private Map<String, String> mdcContext = null;
        @Nullable
        private FDBStoreTimer timer = null;
        @Nullable
        private FDBDatabase.WeakReadSemantics weakReadSemantics = null;
        @Nonnull
        private FDBTransactionPriority priority = FDBTransactionPriority.DEFAULT;
        @Nullable
        private String transactionId = null;
        private long transactionTimeoutMillis = FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS;
        private boolean enableAssertions = false;

        private Builder() {
        }

        private Builder(@Nonnull FDBRecordContextConfig config) {
            this.mdcContext = config.mdcContext;
            this.timer = config.timer;
            this.weakReadSemantics = config.weakReadSemantics;
            this.priority = config.priority;
            this.transactionId = config.transactionId;
            this.transactionTimeoutMillis = config.transactionTimeoutMillis;
            this.enableAssertions = config.enableAssertions;
        }

        private Builder(@Nonnull Builder config) {
            this.mdcContext = config.mdcContext;
            this.timer = config.timer;
            this.weakReadSemantics = config.weakReadSemantics;
            this.priority = config.priority;
            this.transactionId = config.transactionId;
            this.transactionTimeoutMillis = config.transactionTimeoutMillis;
            this.enableAssertions = config.enableAssertions;
        }

        /**
         * Set the MDC context. By default, this will be set to {@code null}, which does not add any additional
         * keys or values to the logs. Additionally, if the "uuid" key of this parameter is set and the
         * transaction ID parameter is <em>not</em> set, then the transaction will set its logging ID based
         * on the value of that key from the MDC context.
         *
         * @param mdcContext the MDC context to use when logging
         * @return this builder
         * @see FDBRecordContextConfig#getMdcContext()
         * @see FDBRecordContextConfig.Builder#setTransactionId(String)
         */
        @Nonnull
        public Builder setMdcContext(@Nullable Map<String, String> mdcContext) {
            this.mdcContext = mdcContext;
            return this;
        }

        /**
         * Get the MDC context.
         *
         * @return the MDC context
         * @see FDBRecordContextConfig#getMdcContext()
         */
        @Nullable
        public Map<String, String> getMdcContext() {
            return mdcContext;
        }

        /**
         * Set the timer to use when instrumenting events. By default, this will be set to
         * {@code null}, when means that events will not be instrumented.
         *
         * @param timer the timer to use to instrument events
         * @return this builder
         * @see FDBRecordContextConfig#getTimer()
         */
        @Nonnull
        public Builder setTimer(@Nullable FDBStoreTimer timer) {
            this.timer = timer;
            return this;
        }

        /**
         * Get the timer to use to instrument events.
         *
         * @return the current timer
         * @see FDBRecordContextConfig#getTimer()
         */
        @Nullable
        public FDBStoreTimer getTimer() {
            return timer;
        }

        /**
         * Set the {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics} to use
         * when creating a transaction. The default value is {@code null}, which indicates that the
         * transaction should not use a cached read version and will not set
         * {@link com.apple.foundationdb.TransactionOptions#setCausalReadRisky()}. This guarantees that the
         * transaction will be linearizable, i.e., it will see all commits from all transactions that
         * have committed before it.
         *
         * @param weakReadSemantics the {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics} to use when creating the transaction
         * @return this builder
         * @see FDBRecordContextConfig#getWeakReadSemantics()
         */
        @Nonnull
        public Builder setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
            this.weakReadSemantics = weakReadSemantics;
            return this;
        }

        /**
         * Get the {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics} from this
         * configuration.
         *
         * @return the {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics}
         * @see FDBRecordContextConfig#getWeakReadSemantics()
         */
        @Nullable
        public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
            return weakReadSemantics;
        }

        /**
         * Set the {@link FDBTransactionPriority} to use when creating a transaction. By default, this
         * will be set to {@link FDBTransactionPriority#DEFAULT}. For more details on what this value
         * means, see {@link FDBTransactionPriority}.
         *
         * @param priority the priority to use when creating a transaction
         * @return the transaction priority
         * @see FDBTransactionPriority
         * @see FDBRecordContextConfig#getPriority()
         */
        @Nonnull
        public Builder setPriority(@Nonnull FDBTransactionPriority priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Get the {@link FDBTransactionPriority}.
         *
         * @return the transaction priority
         * @see FDBRecordContextConfig#getPriority()
         */
        @Nonnull
        public FDBTransactionPriority getPriority() {
            return priority;
        }

        /**
         * Set the transaction ID to use within FDB logs. The default value of this parameter is
         * {@code null}, which indicates that the transaction should look at the "uuid" key of the
         * MDC context (if set) to set the transaction ID.
         *
         * <p>
         * The transaction ID should typically be set to a string of entirely ASCII characters, and
         * it should not exceed 100 bytes in length. If the string is longer than 100 bytes, then the
         * ID may be truncated or dropped. See {@link FDBRecordContext#getTransactionId()}.
         * </p>
         *
         * @param transactionId the ID to use for the transaction in FDB logs
         * @return this builder
         * @see FDBRecordContextConfig#getTransactionId()
         * @see FDBRecordContext#getTransactionId()
         */
        @Nonnull
        public Builder setTransactionId(@Nullable String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        /**
         * Get the ID to use for the transaction within FDB logs.
         *
         * @return the ID to use for the transaction within FDB logs
         * @see FDBRecordContextConfig#getTransactionId()
         */
        @Nullable
        public String getTransactionId() {
            return transactionId;
        }

        /**
         * Set the transaction timeout time in milliseconds. The default is {@link FDBDatabaseFactory#DEFAULT_TR_TIMEOUT_MILLIS},
         * which indicates that a created transaction should inherit its default from the {@link FDBDatabaseFactory}
         * used to create it. If set to {@link FDBDatabaseFactory#UNLIMITED_TR_TIMEOUT_MILLIS}, then no timeout will
         * be imposed on the transaction. Otherwise, the transaction will be configured to automatically cancel
         * itself after the configured number of milliseconds.
         *
         * @param transactionTimeoutMillis the timeout time in milliseconds
         * @return this builder
         * @see FDBRecordContextConfig#getTransactionTimeoutMillis()
         * @see FDBDatabaseFactory#setTransactionTimeoutMillis(long)
         */
        @Nonnull
        public Builder setTransactionTimeoutMillis(long transactionTimeoutMillis) {
            if (transactionTimeoutMillis < FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS) {
                throw new RecordCoreArgumentException("cannot set transaction timeout to " + transactionTimeoutMillis);
            }
            this.transactionTimeoutMillis = transactionTimeoutMillis;
            return this;
        }

        /**
         * Set the transaction timeout time in milliseconds.
         * A value of {@link FDBDatabaseFactory#DEFAULT_TR_TIMEOUT_MILLIS} indicates that a created transaction should inherit its default from the {@link FDBDatabaseFactory}
         * used to create it. A value of {@link FDBDatabaseFactory#UNLIMITED_TR_TIMEOUT_MILLIS} indicates that no timeout will be imposed on the transaction.
         * @return the timeout time in milliseconds
         */
        public long getTransactionTimeoutMillis() {
            return transactionTimeoutMillis;
        }


        /**
         * Enables or disables internal correctness assertions for the context, such as validating maximum key and
         * value lengths for all database requests.
         *
         * @param enableAssertions whether or not assertions are enabled
         * @return this builder
         */
        public Builder setEnableAssertions(boolean enableAssertions) {
            this.enableAssertions = enableAssertions;
            return this;
        }

        /**
         * Return whether or not correctness assertions will enabled for the context.
         * @return {@code true} if correctness assertions are to be enabled for the context
         */
        public boolean areAssertionsEnabled() {
            return enableAssertions;
        }

        /**
         * Create an {@link FDBRecordContextConfig} from this builder.
         *
         * @return an {@link FDBRecordContextConfig} with its values set based on this builder
         */
        @Nonnull
        public FDBRecordContextConfig build() {
            return new FDBRecordContextConfig(this);
        }

        /**
         * Make a copy of this builder.
         * @return a new builder with the same values as this builder
         */
        public Builder copyBuilder() {
            return new Builder(this);
        }
    }
}
