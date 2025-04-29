/*
 * ThrottledRetryingIterator.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * An iterator that can handle resource constraints and failures.
 * This class iterates over an inner cursor, applying resource controls (# of ops per transaction and per time), and
 * retrying failed operations. The iterator will build its own transactions and stores so that it can handle long-running
 * operations.
 *
 * The iterator is currently optimized for RO operations and Deletes. If any other use case is required, it can
 * easily be extended for writes (should add write limit per transaction/second).
 *
 * @param <T> The iterated item type
 */
public class ThrottledRetryingIterator<T> {
    private static final Logger logger = LoggerFactory.getLogger(ThrottledRetryingIterator.class);

    private final int transactionTimeQuotaMillis;
    // If a write quota per transaction is ever needed, it can be added. So far it seems that the main usages is for
    // RO iteration and cleanups (i.e. lazy deletes)
    private final int maxRecordScannedPerTransaction;
    private final int maxRecordDeletesPerTransaction;
    private final int maxRecordScannedPerSec;
    private final int maxRecordDeletesPerSec;
    private final FDBDatabaseRunner runner;
    // TODO: use an interface instead of lambdas
    private final TriFunction<FDBRecordStore, RecordCursorResult<T>, Integer, RecordCursor<T>> cursorCreator;
    private final TriFunction<FDBRecordStore, RecordCursorResult<T>, QuotaManager, CompletableFuture<Void>> singleItemHandler;
    private final Consumer<QuotaManager> rangeSuccessNotification;
    private final Consumer<QuotaManager> rangeInitNotification;

    // Starting time of the current/most-recent transaction
    private long rangeIterationStartTimeMilliseconds = 0;

    // Cursor limit in a single transaction (throttled)
    private int cursorRowsLimit;

    private int failureRetriesCounter = 0; // reset at each success
    private int successCounter = 0; // reset on each failure

    public ThrottledRetryingIterator(Builder<T> builder) {
        this.runner = builder.runner;
        this.cursorCreator = builder.cursorCreator;
        this.singleItemHandler = builder.singleItemHandler;
        this.transactionTimeQuotaMillis = builder.transactionTimeQuotaMillis;
        this.maxRecordScannedPerTransaction = builder.maxRecordScannedPerTransaction;
        this.maxRecordDeletesPerTransaction = builder.maxRecordDeletesPerTransaction;
        this.maxRecordScannedPerSec = builder.maxRecordScannedPerSec;
        this.maxRecordDeletesPerSec = builder.maxRecordDeletesPerSec;
        this.rangeSuccessNotification = builder.rangeSuccessNotification;
        this.rangeInitNotification = builder.rangeInitNotification;
        this.cursorRowsLimit = Math.min(builder.initialRecordsScannedPerTransaction, maxRecordScannedPerTransaction);
    }

    public CompletableFuture<Void> iterateAll(FDBRecordStore userStore) {
        final AtomicReference<RecordCursorResult<T>> lastSuccessCont = new AtomicReference<>(null);
        final QuotaManager singleIterationQuotaManager = new QuotaManager();
        FDBRecordStore.Builder userStoreBuilder = userStore.asBuilder();
        return AsyncUtil.whileTrue(() ->
                // iterate ranges
                iterateOneRange(userStoreBuilder, lastSuccessCont.get(), singleIterationQuotaManager)
                        .handle((continuation, ex) -> {
                            if (ex == null) {
                                lastSuccessCont.set(continuation);
                                return handleSuccess(singleIterationQuotaManager);
                            }
                            return handleFailure(ex, singleIterationQuotaManager);
                        })
                        .thenCompose(ret -> ret)
        );
    }

    private CompletableFuture<RecordCursorResult<T>> iterateOneRange(FDBRecordStore.Builder userStoreBuilder,
                                                                     RecordCursorResult<T> cursorStartPoint,
                                                                     QuotaManager singleIterationQuotaManager) {
        AtomicReference<RecordCursorResult<T>> cont = new AtomicReference<>();
        return runner.runAsync(transaction -> {
            // this layer returns last cursor result
            singleIterationQuotaManager.init();

            runUnlessNull(rangeInitNotification, singleIterationQuotaManager); // let the user know about this range iteration attempt
            final FDBRecordStore store = userStoreBuilder.setContext(transaction).build();
            RecordCursor<T> cursor = cursorCreator.apply(store, cursorStartPoint, cursorRowsLimit);

            rangeIterationStartTimeMilliseconds = nowMillis();

            return AsyncUtil.whileTrue(() -> cursor.onNext()
                            .thenCompose(result -> {
                                cont.set(result);
                                if (!result.hasNext()) {
                                    if (result.getNoNextReason().isSourceExhausted()) {
                                        singleIterationQuotaManager.hasMore = false;
                                    }
                                    return AsyncUtil.READY_FALSE; // end of this one range
                                }
                                singleIterationQuotaManager.scannedCount++;
                                CompletableFuture<Void> future = singleItemHandler.apply(store, result, singleIterationQuotaManager);
                                return future.thenCompose(ignore -> AsyncUtil.READY_TRUE);
                            })
                            .thenApply(rangeHasMore -> {
                                if (rangeHasMore && ((0 < transactionTimeQuotaMillis && elapsedTimeMillis() > transactionTimeQuotaMillis) ||
                                                             (0 < maxRecordDeletesPerTransaction && singleIterationQuotaManager.deletesCount > maxRecordDeletesPerTransaction))) {
                                    // Reached time/delete quota in this transaction. Continue in a new one (possibly after throttling)
                                    return false;
                                }
                                return rangeHasMore;
                            }),
                    runner.getExecutor());
        }).thenApply(ignore -> cont.get());
    }

    CompletableFuture<Boolean> handleSuccess(QuotaManager quotaManager) {
        runUnlessNull(rangeSuccessNotification, quotaManager); // let the user know about this successful range iteration

        if (!quotaManager.hasMore) {
            // Here: all done, no need for throttling
            return AsyncUtil.READY_FALSE;
        }

        // Maybe increase cursor's row limit
        if (((++successCounter) % 40) == 0 && cursorRowsLimit < (quotaManager.scannedCount + 3)) {
            final int oldLimit = cursorRowsLimit;
            cursorRowsLimit = Math.min(maxRecordScannedPerTransaction, (cursorRowsLimit * 5) / 4);
            if (logger.isInfoEnabled()) {
                logger.info(KeyValueLogMessage.of("ThrottledIterator: iterate one range success: increase limit",
                        LogMessageKeys.LIMIT, cursorRowsLimit,
                        LogMessageKeys.OLD_LIMIT, oldLimit,
                        LogMessageKeys.SUCCESSFUL_TRANSACTIONS_COUNT, successCounter));
            }
        }
        failureRetriesCounter = 0;

        // Here: calculate delay
        long rangeProcessingTimeMillis = Math.max(0, elapsedTimeMillis());
        long toWaitMillis = Collections.max(List.of(
                // delay required for max deletes per second throttling
                throttlePerSecGetDelayMillis(rangeProcessingTimeMillis, maxRecordDeletesPerSec, quotaManager.deletesCount),
                // delay required for max records scanned per second throttling
                throttlePerSecGetDelayMillis(rangeProcessingTimeMillis, maxRecordScannedPerSec, quotaManager.scannedCount)
        ));

        return toWaitMillis > 0 ?
               MoreAsyncUtil.delayedFuture(toWaitMillis, TimeUnit.MILLISECONDS, runner.getScheduledExecutor()).thenApply(ignore -> true) :
               AsyncUtil.READY_TRUE;
    }

    @VisibleForTesting
    public static long throttlePerSecGetDelayMillis(long rangeProcessingTimeMillis, int maxPerSec, int eventsCount) {
        if (maxPerSec <= 0) {
            return 0; // do not throttle
        }
        // get the number of events, get the min time they should have taken,
        // and return a padding time (if positive)
        // MS(count / perSec) - ptimeMillis ==>  MS(count) / perSec - ptimeMillis (avoid floating point, the floor effect is a neglectable 0.005%)
        long waitMillis = (TimeUnit.SECONDS.toMillis(eventsCount) / maxPerSec) - rangeProcessingTimeMillis;
        return waitMillis > 0 ? waitMillis : 0;
    }

    CompletableFuture<Boolean> handleFailure(Throwable ex, QuotaManager quotaManager) {
        if (++failureRetriesCounter > 100) {
            if (logger.isWarnEnabled()) {
                logger.warn(KeyValueLogMessage.of("ThrottledIterator: iterate one range failure: will abort",
                        LogMessageKeys.LIMIT, cursorRowsLimit,
                        LogMessageKeys.RETRY_COUNT, failureRetriesCounter),
                        ex);
            }

            // Complete exceptionally
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.completeExceptionally(ex);
            return future;
        }
        // Here: after a failure, try setting a scan quota that is smaller than the number of scanned items during the failure
        // Note: the runner does not retry
        successCounter = 0;
        final int oldLimit = cursorRowsLimit;
        cursorRowsLimit = Math.max(1, (quotaManager.scannedCount * 9) / 10);
        if (logger.isInfoEnabled()) {
            logger.info(KeyValueLogMessage.of("ThrottledIterator: iterate one range failure: will retry",
                            LogMessageKeys.LIMIT, cursorRowsLimit,
                            LogMessageKeys.OLD_LIMIT, oldLimit,
                            LogMessageKeys.RETRY_COUNT, failureRetriesCounter),
                    ex);
        }

        return AsyncUtil.READY_TRUE; // retry
    }

    private long nowMillis() {
        return System.currentTimeMillis();
    }

    private long elapsedTimeMillis() {
        return rangeIterationStartTimeMilliseconds <= 0 ? 0 :
               nowMillis() - rangeIterationStartTimeMilliseconds;
    }

    private static void runUnlessNull(@Nullable Consumer<QuotaManager> func, QuotaManager quotaManager) {
        if (func != null) {
            func.accept(quotaManager);
        }
    }

    /**
     * A class that manages the resource constraints of the ioterator.
     * This class is used by the iterator and is also given to the callbacks. It reflects the current state of the controlled
     * constraints and helps determine whether a transaction should be committed and another started.
     */
    // TODO: Should this be made thread safe?
    public static class QuotaManager {
        int deletesCount;
        int scannedCount;
        boolean hasMore;
        boolean stopIteration;

        public int getDeletesCount() {
            return deletesCount;
        }

        public int getScannedCount() {
            return scannedCount;
        }

        public void deleteCountAdd(int count) {
            deletesCount += count;
        }

        public void deleteCountInc() {
            deletesCount++;
        }

        public void markExhausted() {
            hasMore = false;
        }

        void init() {
            deletesCount = 0;
            scannedCount = 0;
            hasMore = true;
            stopIteration = false;
        }
    }

    @FunctionalInterface
    public interface TriFunction<A, B, C, R> {
        R apply(A a, B b, C c);
    }

    public static <T> Builder<T> builder(FDBDatabaseRunner runner,
                                         TriFunction<FDBRecordStore, RecordCursorResult<T>, Integer, RecordCursor<T>> cursorCreator,
                                         TriFunction<FDBRecordStore, RecordCursorResult<T>, QuotaManager, CompletableFuture<Void>> singleItemHandler) {
        return new Builder<>(runner, cursorCreator, singleItemHandler);
    }

    /**
     * A builder class for the iterator.
     *
     * @param <T> the item type being iterated on.
     */
    public static class Builder<T> {
        private final FDBDatabaseRunner runner;
        private final TriFunction<FDBRecordStore, RecordCursorResult<T>, Integer, RecordCursor<T>> cursorCreator;
        private final TriFunction<FDBRecordStore, RecordCursorResult<T>, QuotaManager, CompletableFuture<Void>> singleItemHandler;
        private Consumer<QuotaManager> rangeSuccessNotification;
        private Consumer<QuotaManager> rangeInitNotification;
        private int transactionTimeQuotaMillis;
        private int maxRecordScannedPerTransaction;
        private int initialRecordsScannedPerTransaction;
        private int maxRecordDeletesPerTransaction;
        private int maxRecordScannedPerSec = 0;
        private int maxRecordDeletesPerSec = 0;

        /**
         * Constructor.
         * @param runner the FDB runner to use when creating transactions
         * @param cursorCreator the method to use when creating the inner cursor
         * @param singleItemHandler the callback to use for handling a single item while iterating
         */
        Builder(FDBDatabaseRunner runner,
                TriFunction<FDBRecordStore, RecordCursorResult<T>, Integer, RecordCursor<T>> cursorCreator,
                TriFunction<FDBRecordStore, RecordCursorResult<T>, QuotaManager, CompletableFuture<Void>> singleItemHandler) {
            // Mandatory fields are set in the constructor. Everything else is optional.
            this.runner = runner;
            this.cursorCreator = cursorCreator;
            this.singleItemHandler = singleItemHandler;
            // set defaults
            this.maxRecordScannedPerTransaction = 0;
            this.transactionTimeQuotaMillis = (int)TimeUnit.SECONDS.toMillis(4);
            this.initialRecordsScannedPerTransaction = 0;
            this.maxRecordDeletesPerTransaction = 0;
        }

        /**
         * Set the amount of time for each transaction before committing and starting another.
         * Defaults to 0 (no limit).
         * @param transactionTimeQuotaMillis the maximum duration of a transaction.
         * @return this builder
         */
        public Builder<T> withTransactionTimeQuotaMillis(int transactionTimeQuotaMillis) {
            this.transactionTimeQuotaMillis = Math.max(0, transactionTimeQuotaMillis);
            return this;
        }

        /**
         * Set the maximum number of items scanned within a transaction.
         * The actual row limit for the inner cursor is dynamic and changes based on the success and failure rate. The
         * maximum value, though, will never exceed this parameter.
         * Defaults to 0 (no limit).
         * @param maxRecordsScannedPerTransaction the maximum number of items scanned in a transaction
         * @return this builder
         */
        public Builder<T> withMaxRecordsScannedPerTransaction(int maxRecordsScannedPerTransaction) {
            this.maxRecordScannedPerTransaction = Math.max(0, maxRecordsScannedPerTransaction);
            if (initialRecordsScannedPerTransaction == 0) {
                // set a reasonable default if not otherwise set
                initialRecordsScannedPerTransaction = maxRecordScannedPerTransaction / 4;
            }
            return this;
        }

        /**
         * Set the initial number of records scanned per transaction.
         * The actual row limit for the inner cursor is dynamic and changes based on the success and failure rate. The
         * value is set to the parameter at the beginning of each transaction.
         * Defaults to maxRecordsScannedPerTransaction / 4. 0 means no limit.
         * @param initialRecordsScannedPerTransaction the initial row limit for the inner iterator
         * @return this builder
         */
        public Builder<T> withInitialRecordsScannedPerTransaction(int initialRecordsScannedPerTransaction) {
            this.initialRecordsScannedPerTransaction = Math.max(0, initialRecordsScannedPerTransaction);
            return this;
        }

        /**
         * Set the max number of records that can be scanned in a given second.
         * This parameter will control the delay between transactions (not within a single transaction). Once a transaction
         * has been committed, this will govern whether the iterator will delay starting the next one.
         * Defaults to 0 (no limit).
         * @param maxRecordsScannedPerSec the number of items scanned (on average) per second by the iterator
         * @return this builder
         */
        public Builder<T> withMaxRecordsScannedPerSec(int maxRecordsScannedPerSec) {
            this.maxRecordScannedPerSec = Math.max(0, maxRecordsScannedPerSec);
            return this;
        }

        /**
         * Set the max number of records that can be deleted in a given second.
         * This parameter will control the delay between transactions (not within a single transaction). Once a transaction
         * has been committed, this will govern whether the iterator will delay starting the next one.
         * Defaults to 0 (no limit).
         * @param maxRecordsDeletesPerSec the number of items deleted (on average) per second by the iterator
         * @return this builder
         */
        public Builder<T> withMaxRecordsDeletesPerSec(int maxRecordsDeletesPerSec) {
            this.maxRecordDeletesPerSec = Math.max(0, maxRecordsDeletesPerSec);
            return this;
        }

        /**
         * Set the callback to invoke on transaction commit.
         * @param rangeSuccessNotification the callback invoked every time a transaction is successfully committed
         * Defaults to null (no callback).
         * @return this builder
         */
        public Builder<T> withRangeSuccessNotification(Consumer<QuotaManager> rangeSuccessNotification) {
            this.rangeSuccessNotification = rangeSuccessNotification;
            return this;
        }

        /**
         * Set the callback to invoke on transaction start.
         * @param rangeInitNotification the callback invoked every time a transaction is created
         * Defaults to null (no callback).
         * @return this builder
         */
        public Builder<T> withRangeInitNotification(Consumer<QuotaManager> rangeInitNotification) {
            this.rangeInitNotification = rangeInitNotification;
            return this;
        }

        /**
         * Set the maximum number of items deleted within a transaction.
         * Once this number has been reached the transaction will be committed and another will start. The actual number
         * of deletes is determined by the {@link QuotaManager#deletesCount}, affected by the {@link #singleItemHandler}
         * implementation.
         * Defaults to 0 (no limit).
         * @param maxRecordsDeletesPerTransaction the maximum number of items scanned in a transaction
         * @return this builder
         */
        public Builder<T> withMaxRecordsDeletesPerTransaction(int maxRecordsDeletesPerTransaction) {
            this.maxRecordDeletesPerTransaction = Math.max(0, maxRecordsDeletesPerTransaction);
            return this;
        }

        /**
         * Create the iterator.
         * @return the newly minted iterator
         */
        public ThrottledRetryingIterator<T> build() {
            return new ThrottledRetryingIterator<>(this);
        }
    }
}
