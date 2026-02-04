/*
 * RecordRepair.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.CursorFactory;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ThrottledRetryingIterator;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.CloseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A class that iterates through all records in a given store and validates (and optionally repairs) them.
 * <p>
 * When records in a store are suspected to be corrupt, this class can be used to bring the store back up to consistent state.
 * The runner will relax many of the record-loading constraints to allow records to be scanned and validated. The current
 * validation capabilities include detecting missing splits (payload and version) and corrupt data that results in records
 * that cannot be deserialized.
 * <p>
 * This runner is expected to run for extended period of time, and therefore makes use of the {@link ThrottledRetryingIterator}
 * to provide transaction resource management. The runner will create transactions and commit them as needed (and so does
 * not have to be run from within an existing transaction).
 * <p>
 * The runner provides two main entry points:
 * <ul>
 *     <li>{@link RecordRepairStatsRunner#run} that iterates through the store and returns an aggregated
 *     count of all found issues</li>
 *     <li>{@link RecordRepairValidateRunner#run} that iterates through the store
 *     and returns a list of all found issues</li>
 * </ul>
 * There is no significant performance difference between the two. The intent is to use the former to get a view of the store
 * status and to verify that a store is fully repaired, and to use the latter to iterate through the store record, one chunk
 * at a time and to perform repairs as needed.
 * <p>
 * There are currently two kinds of validations that can be performed:
 * <ul>
 *     <li>{@link ValidationKind#RECORD_VALUE} will verify that the record payload is in good shape: The data exists and
 *     the record can be deserialized</li>
 *     <li>{@link ValidationKind#RECORD_VALUE_AND_VERSION} will add to the previous validation the check that the record
 *     has a version present</li>
 * </ul>
 * The idea is that stores that are configured to not store version data can avoid the overhead of checking for version
 * existence.
 * <p>
 * A note on repair: Repairing a corrupt data would normally mean deleting the data (without trying to update indexes).
 * Repairing missing version would normally mean creating a new version for the record.
 * <p>
 * A note on versions: Some stores may have had their metadata changed to include versions after some records have already
 * been saved. In this case, the version repair operation may attempt to create many versions - for all the older records.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class RecordRepair implements AutoCloseable {
    /**
     * The type of validation to perform.
     */
    public enum ValidationKind { RECORD_VALUE, RECORD_VALUE_AND_VERSION }

    private static final Logger logger = LoggerFactory.getLogger(RecordRepair.class);

    @Nonnull
    private final FDBDatabase database;
    @Nonnull
    private final FDBRecordStore.Builder storeBuilder;
    @Nonnull
    private final ValidationKind validationKind;
    @Nonnull
    private final ThrottledRetryingIterator<Tuple> throttledIterator;
    private final boolean allowRepair;

    protected RecordRepair(@Nonnull final Builder config, boolean allowRepair) {
        this.database = config.database;
        this.storeBuilder = config.getStoreBuilder();
        this.validationKind = config.getValidationKind();
        ThrottledRetryingIterator.Builder<Tuple> iteratorBuilder =
                ThrottledRetryingIterator.builder(database, storeBuilder.getContext().getConfig().toBuilder(), cursorFactory(), this::handleOneItem);
        this.allowRepair = allowRepair;
        // This will also ensure the transaction only commits when needed
        throttledIterator = configureThrottlingIterator(iteratorBuilder, config, allowRepair).build();
    }

    /**
     * Create a builder for the runner.
     * @param database the FDB database to use to create new transactions
     * @param storeBuilder the store builder to use
     * @return the builder instance
     */
    public static Builder builder(@Nonnull FDBDatabase database, final FDBRecordStore.Builder storeBuilder) {
        return new Builder(database, storeBuilder);
    }

    @Override
    public void close() throws CloseException {
        throttledIterator.close();
    }

    @Nonnull
    protected abstract CompletableFuture<Void> handleOneItem(@Nonnull FDBRecordStore store,
                                                             @Nonnull RecordCursorResult<Tuple> lastResult,
                                                             @Nonnull ThrottledRetryingIterator.QuotaManager quotaManager);

    /**
     * Internal utility ot start the iteration with the underlying iterator.
     * @return a Future that completes when the iteration is done
     */
    protected CompletableFuture<Void> iterateAll() {
        return throttledIterator.iterateAll(storeBuilder);
    }

    private CursorFactory<Tuple> cursorFactory() {
        return (@Nonnull FDBRecordStore store, @Nullable RecordCursorResult<Tuple> lastResult, int rowLimit) -> {
            byte[] continuation = lastResult == null ? null : lastResult.getContinuation().toBytes();
            ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(executeProperties -> executeProperties.setReturnedRowLimit(rowLimit));
            return store.scanRecordKeys(continuation, scanProperties);
        };
    }

    protected CompletableFuture<RecordRepairResult> validateInternal(@Nonnull final RecordCursorResult<Tuple> primaryKey,
                                                                     @Nonnull final FDBRecordStore store,
                                                                     boolean allowRepair) {
        RecordValueValidator valueValidator = new RecordValueValidator(store);
        // The following is dependent on the semantics of value and version repairs. A more elaborate scheme
        // to introduce flow control and abort/continue mechanisms would make this more generic but is yet unnecessary.
        return valueValidator.validateRecordAsync(primaryKey.get()).thenCompose(valueValidationResult -> {
            if (!valueValidationResult.isValid()) {
                if (allowRepair) {
                    return valueValidator.repairRecordAsync(valueValidationResult);
                } else {
                    return CompletableFuture.completedFuture(valueValidationResult);
                }
            } else if (validationKind == ValidationKind.RECORD_VALUE_AND_VERSION) {
                RecordVersionValidator versionValidator = new RecordVersionValidator(store);
                return versionValidator.validateRecordAsync(primaryKey.get()).thenCompose(versionValidationResult -> {
                    if (!versionValidationResult.isValid() && allowRepair) {
                        return versionValidator.repairRecordAsync(versionValidationResult);
                    } else {
                        return CompletableFuture.completedFuture(versionValidationResult);
                    }
                });
            } else {
                return CompletableFuture.completedFuture(valueValidationResult);
            }
        });
    }

    private ThrottledRetryingIterator.Builder<Tuple> configureThrottlingIterator(ThrottledRetryingIterator.Builder<Tuple> builder, Builder config, boolean allowRepair) {
        return builder
                .withTransactionInitNotification(this::logStartTransaction)
                .withTransactionSuccessNotification(this::logCommitTransaction)
                .withTransactionTimeQuotaMillis(config.getTransactionTimeQuotaMillis())
                .withMaxRecordsDeletesPerTransaction(config.getMaxRecordDeletesPerTransaction())
                .withMaxRecordsScannedPerSec(config.getMaxRecordScannedPerSec())
                .withMaxRecordsDeletesPerSec(config.getMaxRecordDeletesPerSec())
                .withNumOfRetries(config.getNumOfRetries())
                .withCommitWhenDone(allowRepair);
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private void logStartTransaction(ThrottledRetryingIterator.QuotaManager quotaManager) {
        if (logger.isDebugEnabled()) {
            logger.debug(KeyValueLogMessage.of("RecordRepairRunner: transaction started"));
        }
    }

    private void logCommitTransaction(ThrottledRetryingIterator.QuotaManager quotaManager) {
        if (logger.isDebugEnabled()) {
            String message = allowRepair ? "RecordRepairRunner: transaction committed" : "RecordRepairRunner: transaction ended";
            logger.debug(KeyValueLogMessage.of(message,
                    LogMessageKeys.RECORDS_SCANNED, quotaManager.getScannedCount(),
                    LogMessageKeys.RECORDS_DELETED, quotaManager.getDeletesCount()));
        }
    }

    /**
     * A builder to configure and create a {@link RecordRepair}.
     */
    public static class Builder {
        @Nonnull
        private final FDBDatabase database;
        @Nonnull
        private final FDBRecordStore.Builder storeBuilder;

        private int maxResultsReturned = 10_000;
        @Nonnull
        private ValidationKind validationKind = ValidationKind.RECORD_VALUE_AND_VERSION;

        private int transactionTimeQuotaMillis = (int)TimeUnit.SECONDS.toMillis(4);
        private int maxRecordDeletesPerTransaction = 0;
        private int maxRecordScannedPerSec = 0;
        private int maxRecordDeletesPerSec = 1000;
        private int numOfRetries = 4;
        private int userVersion;
        private @Nullable FormatVersion minimumPossibleFormatVersion;

        /**
         * Constructor.
         * @param database the FDB database to use
         * @param storeBuilder the store builder to use
         */
        public Builder(@Nonnull final FDBDatabase database, @Nonnull final FDBRecordStore.Builder storeBuilder) {
            this.database = database;
            this.storeBuilder = storeBuilder;
        }

        /**
         * Finalize the build and create a stats runner.
         * @return the newly created stats runner
         */
        public RecordRepairStatsRunner buildStatsRunner() {
            return new RecordRepairStatsRunner(this);
        }

        /**
         * Finalize the build and create a repair  runner.
         * @param allowRepair whether to repair the found issues (TRUE) or run in read-only mode (FALSE)
         * @return the newly created repair runner
         */
        public RecordRepairValidateRunner buildRepairRunner(boolean allowRepair) {
            return new RecordRepairValidateRunner(this, allowRepair);
        }

        /**
         * Limit the number of issues found.
         * This parameter is intended to stop the iteration once a number of issues has been found, as a means of controlling
         * the size of the list returned. Note that this is only relevant for a repair runner.
         * @param maxResultsReturned the maximum number of issues to be returned from the {@link RecordRepairValidateRunner#run} method.
         * Default: 10,000. Use 0 for Unlimited.
         * @return this builder
         */
        public Builder withMaxResultsReturned(int maxResultsReturned) {
            this.maxResultsReturned = maxResultsReturned;
            return this;
        }

        /**
         * The {@link ValidationKind} to use for the validation or repair.
         * Default: {@link ValidationKind#RECORD_VALUE_AND_VERSION}.
         * @param validationKind the validation kind to use
         * @return this builder
         */
        public Builder withValidationKind(ValidationKind validationKind) {
            this.validationKind = validationKind;
            return this;
        }

        /**
         * Limit the number of records deleted in a transaction.
         * Records can be deleted as part of the repair process. Once this number is reached, the transaction gets committed
         * and a new one is started.
         * @param maxRecordDeletesPerTransaction the max number of records allowed to be deleted in a transaction.
         * Default: 0 (unlimited)
         * @return this builder
         */
        public Builder withMaxRecordDeletesPerTransaction(final int maxRecordDeletesPerTransaction) {
            this.maxRecordDeletesPerTransaction = maxRecordDeletesPerTransaction;
            return this;
        }

        /**
         * Limit the amount of time a transaction can take.
         * This will instruct the runner to stop a transaction once this duration has been reached. Note that each transaction
         * is limited (to 5 seconds normally) by FDB as well. If set to 0 the runner will not limit transaction time,
         * which may result in FDB failing to commit (transaction too old).
         * @param transactionTimeQuotaMillis the max number of milliseconds to spend in a transaction.
         * Default: 4000. Use 0 for unlimited.
         * @return this builder
         */
        public Builder withTransactionTimeQuotaMillis(final int transactionTimeQuotaMillis) {
            this.transactionTimeQuotaMillis = transactionTimeQuotaMillis;
            return this;
        }

        /**
         * Limit the number of records that can be scanned every second.
         * This would delay the next transaction to ensure the limit is maintained (while there are no delays added during a transaction).
         * @param maxRecordScannedPerSec the average number of records to scan in per second.
         * Default: 0 (unlimited)
         * @return this builder
         */
        public Builder withMaxRecordScannedPerSec(final int maxRecordScannedPerSec) {
            this.maxRecordScannedPerSec = maxRecordScannedPerSec;
            return this;
        }

        /**
         * Limit the number of records that can be deleted every second.
         * This would delay the next transaction to ensure the limit is maintained (while there are no delays added during a transaction).
         * @param maxRecordDeletesPerSec the average number of records to delete in per second.
         * Default: 1000. Use 0 for unlimited.
         * @return this builder
         */
        public Builder withMaxRecordDeletesPerSec(final int maxRecordDeletesPerSec) {
            this.maxRecordDeletesPerSec = maxRecordDeletesPerSec;
            return this;
        }

        /**
         * Control the number of retries before failure.
         * The runner will retry a transaction if failed. Once the max number of retries has been reached, the operation would fail.
         * @param numOfRetries the maximum number of times to retry a transaction upon failure.
         * Default: 4
         * @return this builder
         */
        public Builder withNumOfRetries(final int numOfRetries) {
            this.numOfRetries = numOfRetries;
            return this;
        }

        /**
         * Set the store header repair parameters.
         * If set, the runner will try to repair the store header (See {@link FDBRecordStore.Builder#repairMissingHeader(int, FormatVersion)})
         * as part of the repair operation in case the store fails to open.
         * If the runner is running in dry run mode (repair not allowed) then the operation will be rolled back once the run
         * is complete, making no change to the header.
         * @param userVersion the user version for the header repair
         * @param minimumPossibleFormatVersion the minimum store format version to use for the repair
         * Default: null minimumPossibleFormatVersion will not attempt to repair the header
         * @return this builder
         */
        public Builder withHeaderRepairParameters(int userVersion, @Nullable FormatVersion minimumPossibleFormatVersion) {
            this.userVersion = userVersion;
            this.minimumPossibleFormatVersion = minimumPossibleFormatVersion;
            return this;
        }

        @Nonnull
        public FDBDatabase getDatabase() {
            return database;
        }

        @Nonnull
        public FDBRecordStore.Builder getStoreBuilder() {
            if (minimumPossibleFormatVersion != null) {
                // override the store builder to repair the header if necessary
                return new StoreBuilderWithRepair(storeBuilder, userVersion, minimumPossibleFormatVersion);
            } else {
                return storeBuilder;
            }
        }

        @Nonnull
        public ValidationKind getValidationKind() {
            return validationKind;
        }

        public int getMaxResultsReturned() {
            return maxResultsReturned;
        }

        public int getTransactionTimeQuotaMillis() {
            return transactionTimeQuotaMillis;
        }

        public int getMaxRecordDeletesPerTransaction() {
            return maxRecordDeletesPerTransaction;
        }

        public int getMaxRecordScannedPerSec() {
            return maxRecordScannedPerSec;
        }

        public int getMaxRecordDeletesPerSec() {
            return maxRecordDeletesPerSec;
        }

        public int getNumOfRetries() {
            return numOfRetries;
        }
    }
}
