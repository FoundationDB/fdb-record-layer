/*
 * TransactionConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TransactionConfig {
    @Nullable
    private final String transactionId;
    @Nullable
    private final Map<String, String> loggingContext;
    @Nullable
    private final WeakReadSemantics weakReadSemantics;
    @Nonnull
    private final Priority priority;
    private final long transactionTimeoutMillis;
    private final boolean enableAssertions;
    private final boolean logTransaction;
    private final boolean trackOpen;
    private final boolean saveOpenStackTrace;

    public static final TransactionConfig DEFAULT = newBuilder().build();

    TransactionConfig(String transactionId, Map<String, String> loggingContext, WeakReadSemantics weakReadSemantics,
                      Priority priority, long transactionTimeoutMillis,
                      boolean enableAssertions, boolean logTransaction,
                      boolean trackOpen, boolean saveOpenStackTrace) {
        this.transactionId = transactionId;
        this.loggingContext = loggingContext;
        this.weakReadSemantics = weakReadSemantics;
        this.priority = priority;
        this.transactionTimeoutMillis = transactionTimeoutMillis;
        this.enableAssertions = enableAssertions;
        this.logTransaction = logTransaction;
        this.trackOpen = trackOpen;
        this.saveOpenStackTrace = saveOpenStackTrace;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Map<String, String> getLoggingContext() {
        return loggingContext;
    }

    public WeakReadSemantics getWeakReadSemantics() {
        return weakReadSemantics;
    }

    public Priority getTransactionPriority() {
        return priority;
    }

    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    public boolean isEnableAssertions() {
        return enableAssertions;
    }

    public boolean isLogTransaction() {
        return logTransaction;
    }

    public boolean isTrackOpen() {
        return trackOpen;
    }

    public boolean isSaveOpenStackTrace() {
        return saveOpenStackTrace;
    }

    public static class Builder {
        @Nullable
        private String transactionId;
        @Nullable
        private Map<String, String> loggingContext;
        @Nullable
        private WeakReadSemantics weakReadSemantics;
        @Nonnull
        private Priority priority = Priority.DEFAULT;
        private long transactionTimeoutMillis = -1L;
        private boolean enableAssertions;
        private boolean logTransaction;
        private boolean trackOpen;
        private boolean saveOpenStackTrace;

        public Builder setTransactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder setLoggingContext(Map<String, String> loggingContext) {
            this.loggingContext = loggingContext;
            return this;
        }

        public Builder setWeakReadSemantics(WeakReadSemantics weakReadSemantics) {
            this.weakReadSemantics = weakReadSemantics;
            return this;
        }

        public Builder setTransactionPriority(Priority priority) {
            this.priority = priority;
            return this;
        }

        public Builder setTransactionTimeoutMillis(long transactionTimeoutMillis) {
            this.transactionTimeoutMillis = transactionTimeoutMillis;
            return this;
        }

        public Builder setEnableAssertions(boolean enableAssertions) {
            this.enableAssertions = enableAssertions;
            return this;
        }

        public Builder setLogTransaction(boolean logTransaction) {
            this.logTransaction = logTransaction;
            return this;
        }

        public Builder setTrackOpen(boolean trackOpen) {
            this.trackOpen = trackOpen;
            return this;
        }

        public Builder setSaveOpenStackTrace(boolean saveOpenStackTrace) {
            this.saveOpenStackTrace = saveOpenStackTrace;
            return this;
        }

        public TransactionConfig build() {
            return new TransactionConfig(transactionId, loggingContext, weakReadSemantics, priority,
                    transactionTimeoutMillis, enableAssertions, logTransaction, trackOpen, saveOpenStackTrace);
        }
    }

    public static class WeakReadSemantics {
        private final long minVersion;
        private final long stalenessBoundMillis;
        private final boolean isCausalReadRisky;

        /**
         * To specify a stale read semantics for the transaction.
         * Stale read never cause inconsistency within the database. If something had changed beforehand but was not seen,
         * the commit will conflict, while it would have succeeded if the read has been current.
         * @param minVersion Minimum version at which the read should be performed (usually the last version seen by the client)
         * @param stalenessBoundMillis How stale a cached read version can be in milliseconds
         * @param isCausalReadRisky Whether the transaction should be set with a causal read risky flag
         */
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

    public enum Priority {
        /**
         * The priority level that should be used for batch operations. This should be used for discretionary work
         * such as background jobs that do not have strict time pressure to complete in the near future.
         * This signals to the database that this work should be rate limited at a higher rate if it would impact work done at a higher priority.
         */
        BATCH,
        /**
         * The default priority level for most transactions. This should be used for most work unless {@link #BATCH}
         * priority is more appropriate. The system will apply normal rate limiting and throttling mechanisms to the transaction.
         */
        DEFAULT,
        /**
         * The priority level for system operations. This is used  for operations that must complete immediately for all operations,
         * and it bypasses any rate limiting. For this reason, it should
         * generally <em>not</em> be used by users of the system, and it is included mainly for completeness.
         */
        SYSTEM_IMMEDIATE
    }
}
