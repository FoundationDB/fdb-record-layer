/*
 * IndexingThrottle.java
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.runners.ExponentialDelay;
import com.apple.foundationdb.util.LoggableException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class provides build/commit/retry with throttling to the OnlineIndexer. In the future,
 * this class can be generalized to serve other FDB modules.
 */
@API(API.Status.INTERNAL)
public class IndexingThrottle {

    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(IndexingThrottle.class);
    @Nonnull private final IndexingCommon common;
    @Nonnull private final Booker booker;
    private final IndexState expectedIndexState;

    // These error codes represent a list of errors that can occur if there is too much work to be done
    // in a single transaction.
    private static final Set<Integer> lessenWorkCodes = new HashSet<>(Arrays.asList(
            FDBError.TIMED_OUT.code(),
            FDBError.TRANSACTION_TOO_OLD.code(),
            FDBError.NOT_COMMITTED.code(),
            FDBError.TRANSACTION_TIMED_OUT.code(),
            FDBError.COMMIT_READ_INCOMPLETE.code(),
            FDBError.TRANSACTION_TOO_LARGE.code()));

    static class Booker {
        /**
         * Keep track of success/failures and adjust transactions' scanned records limit when needed.
         * Note that when adjustLimits=true, a single thread processing is assumed.
         */
        @Nonnull private final IndexingCommon common;
        private long recordsLimit;
        private long lastFailureRecordsScanned;
        private long totalRecordsScannedSuccess = 0;
        private long totalRecordsScannedFailure = 0;
        private long countSuccessfulTransactions = 0;
        private long countFailedTransactions = 0;
        private long countRunnerFailedTransactions = 0;
        private int consecutiveSuccessCount = 0;
        private long forcedDelayTimestampMilliSeconds = 0;
        private long recordsScannedSinceForcedDelayMilliSeconds = 0;

        Booker(@Nonnull IndexingCommon common) {
            this.common = common;
            this.recordsLimit = common.config.getInitialLimit();
        }

        long getRecordsLimit() {
            return recordsLimit;
        }

        long waitTimeMilliseconds() {
            // let delta = transaction(s) actual time in millis
            // let count = transaction(s) actual count
            // keeping the ratio:
            //   count / ((delta + waitMillis) / 1000) = recordsPerSecond
            //   --> waitMillis = 1000 * count / recordsPerSecond - delta
            // Notes:
            // - For simplicity and locality, assume that the next chunk starts at nowMillis+waitMillis
            // - Avoiding negative delta and restricting toWait's range implies self initialization
            // - Ignore failed transactions (they should be rare, and limited in number)
            int recordsPerSecond = common.config.getRecordsPerSecond();
            if (recordsPerSecond == IndexingCommon.UNLIMITED) {
                // in case config loader changes this value from UNLIMITED to limit
                recordsScannedSinceForcedDelayMilliSeconds = 0;
                forcedDelayTimestampMilliSeconds = 0;
                return 0;
            }
            final long now = System.currentTimeMillis();
            final long delta = Math.max(0, now - forcedDelayTimestampMilliSeconds);
            final long toWait = Math.min(999, Math.max(0, (1000 * recordsScannedSinceForcedDelayMilliSeconds) / recordsPerSecond - delta)); // to avoid floor we could have added (recordsPerSecond / 2) to the numerator, but it's neglectable here
            forcedDelayTimestampMilliSeconds = now + toWait;
            recordsScannedSinceForcedDelayMilliSeconds = 0;
            return toWait;
        }

        public List<Object> logMessageKeyValues() {
            return Arrays.asList(LogMessageKeys.LIMIT, recordsLimit,
                    LogMessageKeys.RECORDS_PER_SECOND, common.config.getRecordsPerSecond(),
                    LogMessageKeys.SUCCESSFUL_TRANSACTIONS_COUNT, countSuccessfulTransactions,
                    LogMessageKeys.FAILED_TRANSACTIONS_COUNT, countFailedTransactions,
                    LogMessageKeys.FAILED_TRANSACTIONS_COUNT_IN_RUNNER, countRunnerFailedTransactions,
                    LogMessageKeys.TOTAL_RECORDS_SCANNED, totalRecordsScannedSuccess,
                    LogMessageKeys.TOTAL_RECORDS_SCANNED_DURING_FAILURES, totalRecordsScannedFailure
                    );
        }

        void decreaseLimit(@Nonnull FDBException fdbException,
                           @Nullable List<Object> additionalLogMessageKeyValues,
                           final boolean adjustLimits) {
            // TODO: decrease the limit only for certain errors
            if (!adjustLimits) {
                return; // no accounting for endpoints operations
            }
            countFailedTransactions++;
            long oldLimit = recordsLimit;
            recordsLimit = Math.max(1, Math.min(lastFailureRecordsScanned - 1, ((lastFailureRecordsScanned * 9) / 10)));
            if (LOGGER.isInfoEnabled()) {
                final KeyValueLogMessage message = KeyValueLogMessage.build("Lessening limit of online index build",
                                LogMessageKeys.ERROR, fdbException.getMessage(),
                                LogMessageKeys.ERROR_CODE, fdbException.getCode(),
                                LogMessageKeys.OLD_LIMIT, oldLimit)
                        .addKeysAndValues(logMessageKeyValues())
                        .addKeysAndValues(common.indexLogMessageKeyValues());
                if (additionalLogMessageKeyValues != null) {
                    message.addKeysAndValues(additionalLogMessageKeyValues);
                }
                LOGGER.info(message.toString(), fdbException);
            }
        }

        void handleLimitsPostRunnerTransaction(@Nullable Throwable exception,
                                               @Nonnull final AtomicLong recordsScanned,
                                               final boolean adjustLimits,
                                               final @Nullable List<Object> additionalLogMessageKeyValues) {
            final long recordsScannedThisTransaction = recordsScanned.get();
            if (!adjustLimits) {
                if (exception == null) {
                    synchronized (this) { // In this mode, multi threads are allowed
                        totalRecordsScannedSuccess += recordsScannedThisTransaction;
                    }
                }
                return; // no adjustments here
            }
            // Here: assuming a single thread
            if (exception == null) {
                countSuccessfulTransactions++;
                totalRecordsScannedSuccess += recordsScannedThisTransaction;
                recordsScannedSinceForcedDelayMilliSeconds += recordsScannedThisTransaction;
                if (consecutiveSuccessCount >= common.config.getIncreaseLimitAfter()) {
                    increaseLimit(additionalLogMessageKeyValues != null ? additionalLogMessageKeyValues : new ArrayList<>());
                    consecutiveSuccessCount = 0; // do not increase again immediately after the next success
                } else {
                    consecutiveSuccessCount++;
                }
            } else {
                // Here: memorize the actual records count for the decrease limit function (if applicable) and reset the counter
                countRunnerFailedTransactions++;
                lastFailureRecordsScanned = recordsScannedThisTransaction;
                totalRecordsScannedFailure += recordsScannedThisTransaction;
                recordsScanned.set(0);
            }
        }

        private void increaseLimit(final @Nonnull List<Object> additionalLogMessageKeyValues) {
            final long maxLimit = common.config.getMaxLimit();
            if (recordsLimit >= maxLimit) {
                return; // quietly
            }
            final long oldLimit = recordsLimit;
            recordsLimit = Math.min(maxLimit, Math.max(recordsLimit + 1, getIncreasedLimit(oldLimit)));

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.build("Re-increasing limit of online index build",
                                LogMessageKeys.OLD_LIMIT, oldLimit)
                        .addKeysAndValues(additionalLogMessageKeyValues)
                        .addKeysAndValues(logMessageKeyValues())
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .toString());
            }
        }

        private long getIncreasedLimit(long oldLimit) {
            if (oldLimit < 5) {
                return oldLimit + 5;
            }
            if (oldLimit < 100) {
                return oldLimit * 2;
            }
            return (4 * oldLimit) / 3;
        }

        void refreshConfigLimits() {
            // this is a rare event, called synchronized
            long maxLimit = common.config.getMaxLimit();
            if (recordsLimit > maxLimit) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(
                            KeyValueLogMessage.build("Decreasing the limit to the new max limit.",
                                    LogMessageKeys.OLD_LIMIT, recordsLimit,
                                    LogMessageKeys.LIMIT, maxLimit,
                                    LogMessageKeys.MAX_LIMIT, maxLimit)
                                    .addKeysAndValues(logMessageKeyValues())
                                    .addKeysAndValues(common.indexLogMessageKeyValues())
                                    .toString());
                }
                recordsLimit = maxLimit;
            }
        }
    }

    IndexingThrottle(@Nonnull IndexingCommon common, IndexState expectedIndexState) {
        this.common = common;
        this.expectedIndexState = expectedIndexState;
        this.booker = new Booker(common);
    }

    public long waitTimeMilliseconds() {
        return booker.waitTimeMilliseconds();
    }

    public List<Object> logMessageKeyValues() {
        return booker.logMessageKeyValues();
    }

    private synchronized void loadConfig() {
        if (common.loadConfig()) {
            booker.refreshConfigLimits();
        }
    }

    // Finds the FDBException that ultimately caused some throwable or
    // null if there is none. This can be then used to determine, for
    // example, the error code associated with this FDBException.
    @Nullable
    private FDBException getFDBException(@Nullable Throwable e) {
        return IndexingBase.findException(e, FDBException.class);
    }

    @SuppressWarnings("squid:S3776") // cognitive complexity is high, candidate for refactoring
    @Nonnull
    public <R> CompletableFuture<R> buildCommitRetryAsync(@Nonnull final BiFunction<FDBRecordStore, AtomicLong, CompletableFuture<R>> buildFunction,
                                                          @Nullable final Function<FDBException, Optional<R>> shouldReturnQuietly,
                                                          @Nullable final List<Object> additionalLogMessageKeyValues,
                                                          final boolean adjustLimits) {
        List<Object> onlineIndexerLogMessageKeyValues = new ArrayList<>(common.indexLogMessageKeyValues());
        if (additionalLogMessageKeyValues != null) {
            onlineIndexerLogMessageKeyValues.addAll(additionalLogMessageKeyValues);
        }

        AtomicInteger tries = new AtomicInteger(0);
        AtomicLong recordsScanned = new AtomicLong(0);
        CompletableFuture<R> ret = new CompletableFuture<>();
        final ExponentialDelay delay = new ExponentialDelay(common.getRunner().getDatabase().getFactory().getInitialDelayMillis(),
                common.getRunner().getDatabase().getFactory().getMaxDelayMillis());
        AsyncUtil.whileTrue(() -> {
            loadConfig();
            return common.getRunner().runAsync(context -> common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync().thenCompose(store -> {
                expectedIndexStatesOrThrow(store, context);
                return buildFunction.apply(store, recordsScanned);
            }), (result, exception) -> {
                booker.handleLimitsPostRunnerTransaction(exception, recordsScanned, adjustLimits, additionalLogMessageKeyValues);
                return Pair.of(result, exception);
            }, onlineIndexerLogMessageKeyValues).handle((value, e) -> {
                if (e == null) {
                    // Here: success path - also the common path (or so we hope)
                    ret.complete(value);
                    return AsyncUtil.READY_FALSE;
                }
                FDBException fdbE = getFDBException(e);
                if (shouldReturnQuietly != null) {
                    Optional<R> retVal = shouldReturnQuietly.apply(fdbE);
                    if (retVal.isPresent()) {
                        // Here: a non-empty answer signals to return this <R> value rather than handling the exception.
                        // This is useful when the caller wishes to handle this exception itself.
                        ret.complete(retVal.get());
                        return AsyncUtil.READY_FALSE;
                    }
                }
                int currTries = tries.getAndIncrement();
                if (currTries >= common.config.getMaxRetries() || fdbE == null || !lessenWorkCodes.contains(fdbE.getCode())) {
                    // Here: should not retry. Or no more retries.
                    return completeExceptionally(ret, e, onlineIndexerLogMessageKeyValues);
                }
                // Here: decrease limit, log, delay continue
                booker.decreaseLimit(fdbE, onlineIndexerLogMessageKeyValues, adjustLimits);
                if (LOGGER.isWarnEnabled()) {
                    final KeyValueLogMessage message = KeyValueLogMessage.build("Retrying Runner Exception",
                                    LogMessageKeys.INDEXER_CURR_RETRY, currTries,
                                    LogMessageKeys.INDEXER_MAX_RETRIES, common.config.getMaxRetries(),
                                    LogMessageKeys.DELAY, delay.getNextDelayMillis())
                            .addKeysAndValues(onlineIndexerLogMessageKeyValues) // already contains common.indexLogMessageKeyValues()
                            .addKeysAndValues(logMessageKeyValues());
                    LOGGER.warn(message.toString(), e);
                }
                CompletableFuture<Boolean> delayedContinue = delay.delay().thenApply(ignore -> true);
                if (common.getRunner().getTimer() != null) {
                    delayedContinue = common.getRunner().getTimer().instrument(FDBStoreTimer.Events.RETRY_DELAY,
                            delayedContinue, common.getRunner().getExecutor());
                }
                return delayedContinue;
            }).thenCompose(Function.identity());
        }, common.getRunner().getExecutor()).whenComplete((ignore, e) -> {
            if (e != null) {
                // Just update ret and ignore the returned future.
                completeExceptionally(ret, e, onlineIndexerLogMessageKeyValues);
            }
        });
        return ret;
    }

    private void expectedIndexStatesOrThrow(FDBRecordStore store, FDBRecordContext context) {
        List<IndexState> indexStates = common.getTargetIndexes().stream().map(store::getIndexState).collect(Collectors.toList());
        if (indexStates.stream().allMatch(state -> state == expectedIndexState)) {
            return;
        }
        // possible exceptions:
        // 1. All the indexes are now readable.
        // 2. Some indexes are built, but all the others are in the expected state.
        // 3. Some indexes are not in the expected state (disabled?).
        // During mutual indexing, the first two may be part of the valid path
        if (indexStates.stream().allMatch(state -> (state == IndexState.READABLE || state == IndexState.READABLE_UNIQUE_PENDING))) {
            throw new IndexingBase.UnexpectedReadableException(true, "All indexes are built");
        }
        if (indexStates.stream().allMatch(state -> state == expectedIndexState || state == IndexState.READABLE || state == IndexState.READABLE_UNIQUE_PENDING)) {
            throw new IndexingBase.UnexpectedReadableException(false, "Some indexes are built");
        }
        final SubspaceProvider subspaceProvider = common.getRecordStoreBuilder().getSubspaceProvider();
        throw new RecordCoreStorageException("Unexpected index state(s)",
                subspaceProvider == null ? "nullSubspaceProvider" : subspaceProvider.logKey(), subspaceProvider == null ? "" : subspaceProvider.toString(context),
                LogMessageKeys.INDEX_NAME, common.getTargetIndexesNames(),
                LogMessageKeys.INDEX_STATE, indexStates,
                LogMessageKeys.INDEX_STATE_PRECONDITION, expectedIndexState);
    }

    private <R> CompletableFuture<Boolean> completeExceptionally(CompletableFuture<R> ret, Throwable e, List<Object> additionalLogMessageKeyValues) {
        if (e instanceof LoggableException) {
            ((LoggableException)e).addLogInfo(additionalLogMessageKeyValues.toArray());
        }
        ret.completeExceptionally(common.getRunner().getDatabase().mapAsyncToSyncException(e));
        return AsyncUtil.READY_FALSE;
    }

    public int getLimit() {
        return (int) booker.getRecordsLimit();
    }

    public long getTotalRecordsScannedScuccessfully() {
        return booker.totalRecordsScannedSuccess;
    }
}

