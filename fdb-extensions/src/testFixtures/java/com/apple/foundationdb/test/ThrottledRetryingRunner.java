/*
 * ThrottledRetryingRunner.java
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

package com.apple.foundationdb.test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Runs a task repeatedly across multiple transactions, with adaptive limit management, optional
 * rate limiting, and retry logic.
 * <p>
 * The caller supplies a {@link BiFunction} that receives a fresh {@link Transaction} and a
 * {@link QuotaManager}, and returns a {@code CompletableFuture<Void>}. The task reads
 * {@link QuotaManager#getLimit()} to know how much work to attempt in this transaction, reports
 * how many items it processed via {@link QuotaManager#processedCountInc()} (and optionally
 * {@link QuotaManager#deletedCountInc()}), and signals completion by calling
 * {@link QuotaManager#markExhausted()}.
 * </p>
 * <p>
 * The runner adjusts the limit across transactions:
 * </p>
 * <ul>
 *   <li>After {@code increaseLimitAfter} consecutive successes the limit is increased.</li>
 *   <li>After any failure the limit is decreased based on how many items were processed before
 *       the failure, so the next attempt is less likely to exceed the transaction budget.</li>
 * </ul>
 * <p>
 * A limit of {@code 0} is treated as "no limit" — the task may do as much work as it likes in
 * each transaction and the limit is never adjusted.
 * </p>
 * <p>
 * Between successful transactions the runner can optionally delay to enforce a maximum item
 * processing or deletion rate ({@code maxItemsScannedPerSec} / {@code maxItemsDeletedPerSec}).
 * On failure the transaction is retried up to {@code numOfRetries} times before failing.
 * </p>
 */
public class ThrottledRetryingRunner implements AutoCloseable {

    public static final int DEFAULT_NUM_RETRIES = 100;
    public static final int DEFAULT_INCREASE_LIMIT_AFTER = 40;

    @Nonnull
    private final TransactionalRunner transactionalRunner;
    @Nonnull
    private final ScheduledExecutorService scheduledExecutor;
    private final int maxItemsScannedPerSec;
    private final int maxItemsDeletedPerSec;
    private final int numOfRetries;
    private final boolean commitWhenDone;
    private final int increaseLimitAfter;
    private final int maxLimit;

    private boolean closed = false;
    private long transactionStartTimeMillis = 0;
    private int failureRetriesCounter = 0;
    private int consecutiveSuccessCount = 0;

    private ThrottledRetryingRunner(Builder builder) {
        this.transactionalRunner = new TransactionalRunner(builder.database);
        this.scheduledExecutor = builder.scheduledExecutor;
        this.maxItemsScannedPerSec = builder.maxItemsScannedPerSec;
        this.maxItemsDeletedPerSec = builder.maxItemsDeletedPerSec;
        this.numOfRetries = builder.numOfRetries;
        this.commitWhenDone = builder.commitWhenDone;
        this.increaseLimitAfter = builder.increaseLimitAfter;
        this.maxLimit = builder.maxLimit;
    }

    /**
     * Run the given task repeatedly across multiple transactions until it calls
     * {@link QuotaManager#markExhausted()}.
     * <p>
     * A single {@link QuotaManager} is created for the lifetime of this call. Its per-transaction
     * counts ({@link QuotaManager#getProcessedCount()}, {@link QuotaManager#getDeletedCount()})
     * are reset at the start of each transaction. Its limit ({@link QuotaManager#getLimit()}) is
     * adjusted by the runner after each transaction and persists across them.
     * </p>
     *
     * @param task the task to run; reads the limit, reports counts, calls
     *             {@link QuotaManager#markExhausted()} to stop
     * @return a future that completes normally when the task exhausts the source, or exceptionally
     *         if the retry limit is exceeded or the runner is closed
     */
    @Nonnull
    public CompletableFuture<Void> iterateAll(
            @Nonnull BiFunction<? super Transaction, QuotaManager, CompletableFuture<Void>> task) {
        if (closed) {
            return CompletableFuture.failedFuture(new TransactionalRunner.RunnerClosed());
        }

        final QuotaManager quotaManager = new QuotaManager(maxLimit);
        return AsyncUtil.whileTrue(() ->
                runOneTransaction(task, quotaManager)
                        .handle((ignored, ex) -> {
                            if (ex == null) {
                                return handleSuccess(quotaManager);
                            }
                            return handleFailure(ex, quotaManager);
                        })
                        .thenCompose(ret -> ret));
    }

    private CompletableFuture<Void> runOneTransaction(
            @Nonnull BiFunction<? super Transaction, QuotaManager, CompletableFuture<Void>> task,
            @Nonnull QuotaManager quotaManager) {
        transactionStartTimeMillis = System.currentTimeMillis();
        return transactionalRunner.runAsync(commitWhenDone, transaction -> {
            quotaManager.initTransaction();
            return task.apply(transaction, quotaManager);
        });
    }

    private CompletableFuture<Boolean> handleSuccess(QuotaManager quotaManager) {
        failureRetriesCounter = 0;
        ++consecutiveSuccessCount;

        // Increase limit after enough consecutive successes, but only when a limit is active
        if (quotaManager.limit > 0 && consecutiveSuccessCount >= increaseLimitAfter) {
            quotaManager.increaseLimit();
            consecutiveSuccessCount = 0;
        }

        if (!quotaManager.hasMore) {
            return AsyncUtil.READY_FALSE;
        }

        long elapsedMillis = Math.max(0, System.currentTimeMillis() - transactionStartTimeMillis);
        long delayMillis = Collections.max(List.of(
                throttlePerSecDelayMillis(elapsedMillis, maxItemsScannedPerSec, quotaManager.processedCount),
                throttlePerSecDelayMillis(elapsedMillis, maxItemsDeletedPerSec, quotaManager.deletedCount)
        ));

        if (delayMillis > 0) {
            return MoreAsyncUtil.delayedFuture(delayMillis, TimeUnit.MILLISECONDS, scheduledExecutor)
                    .thenApply(ignore -> true);
        }
        return AsyncUtil.READY_TRUE;
    }

    private CompletableFuture<Boolean> handleFailure(Throwable ex, QuotaManager quotaManager) {
        ++failureRetriesCounter;
        consecutiveSuccessCount = 0;

        if (ex instanceof CompletionException) {
            ex = ex.getCause();
        }
        if (ex instanceof TransactionalRunner.RunnerClosed) {
            return CompletableFuture.failedFuture(ex);
        }
        if (failureRetriesCounter > numOfRetries) {
            return CompletableFuture.failedFuture(ex);
        }

        // Decrease the limit based on how much work was in-flight when the failure occurred,
        // so the next attempt is less likely to exceed the transaction budget.
        quotaManager.decreaseLimit();

        return AsyncUtil.READY_TRUE; // retry
    }

    /**
     * Compute how long to wait to keep the event rate at or below {@code maxPerSec}.
     * Matches the formula used by {@code ThrottledRetryingIterator}.
     */
    static long throttlePerSecDelayMillis(long elapsedMillis, int maxPerSec, int eventCount) {
        if (maxPerSec <= 0) {
            return 0;
        }
        long waitMillis = (TimeUnit.SECONDS.toMillis(eventCount) / maxPerSec) - elapsedMillis;
        return waitMillis > 0 ? waitMillis : 0;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        transactionalRunner.close();
    }

    /**
     * Tracks per-transaction resource usage, exposes the adaptive limit, and controls whether
     * the loop continues.
     * <p>
     * One instance is created per {@link #iterateAll} call. Per-transaction counts are reset at
     * the start of each transaction; the limit persists and is adjusted by the runner between
     * transactions.
     * </p>
     * <p>
     * The task should call {@link #getLimit()} at the start of each transaction to know how much
     * work to attempt, increment the counts as it goes, and call {@link #markExhausted()} when
     * the source is fully consumed.
     * </p>
     */
    public static class QuotaManager {
        private int processedCount;
        private int deletedCount;
        private boolean hasMore;
        private int limit;
        private int lastProcessedCount;
        private final int maxLimit;

        QuotaManager(int maxLimit) {
            this.maxLimit = maxLimit;
            this.limit = 0;
            this.lastProcessedCount = 0;
        }

        /**
         * Return the current limit.
         * <p>
         * The task should attempt at most this many units of work in the current transaction.
         * A value of {@code 0} means no limit is active.
         * </p>
         */
        public int getLimit() {
            return limit;
        }

        /**
         * Return the number of items processed in the current transaction.
         */
        public int getProcessedCount() {
            return processedCount;
        }

        /**
         * Return the number of items deleted in the current transaction.
         */
        public int getDeletedCount() {
            return deletedCount;
        }

        /**
         * Increment the processed-item count by {@code count}.
         *
         * @param count number of items to add
         */
        public void processedCountAdd(int count) {
            processedCount += count;
        }

        /**
         * Increment the processed-item count by 1.
         */
        public void processedCountInc() {
            processedCount++;
        }

        /**
         * Increment the deleted-item count by {@code count}.
         *
         * @param count number of items to add
         */
        public void deletedCountAdd(int count) {
            deletedCount += count;
        }

        /**
         * Increment the deleted-item count by 1.
         */
        public void deletedCountInc() {
            deletedCount++;
        }

        /**
         * Signal that the source is exhausted. The loop will stop after the current transaction
         * commits, without starting a new one.
         */
        public void markExhausted() {
            hasMore = false;
        }

        void initTransaction() {
            lastProcessedCount = processedCount;
            processedCount = 0;
            deletedCount = 0;
            hasMore = true;
        }

        void increaseLimit() {
            if (limit == 0) {
                return; // no-limit mode: never adjust
            }
            final int increased = Math.max((limit * 5) / 4, limit + 4);
            limit = maxLimit > 0 ? Math.min(maxLimit, increased) : increased;
        }

        void decreaseLimit() {
            if (limit == 0) {
                return; // no-limit mode: never adjust
            }
            limit = Math.max(1, (lastProcessedCount * 9) / 10);
        }
    }

    /**
     * Create a new builder for a {@link ThrottledRetryingRunner}.
     *
     * @param database the database to open transactions against
     * @param scheduledExecutor executor used for throttle delays between transactions
     * @return a new builder
     */
    public static Builder builder(@Nonnull Database database,
                                  @Nonnull ScheduledExecutorService scheduledExecutor) {
        return new Builder(database, scheduledExecutor);
    }

    /**
     * Builder for {@link ThrottledRetryingRunner}.
     */
    public static class Builder {
        @Nonnull
        private final Database database;
        @Nonnull
        private final ScheduledExecutorService scheduledExecutor;
        private int maxItemsScannedPerSec = 0;
        private int maxItemsDeletedPerSec = 0;
        private int numOfRetries = DEFAULT_NUM_RETRIES;
        private boolean commitWhenDone = true;
        private int increaseLimitAfter = DEFAULT_INCREASE_LIMIT_AFTER;
        private int maxLimit = 0;

        private Builder(@Nonnull Database database, @Nonnull ScheduledExecutorService scheduledExecutor) {
            this.database = database;
            this.scheduledExecutor = scheduledExecutor;
        }

        /**
         * Set the maximum number of items that may be scanned (processed) per second.
         * The runner will delay between transactions to stay at or below this rate.
         * A value of {@code 0} (the default) disables scanned-item throttling.
         *
         * @param maxItemsScannedPerSec the rate limit; {@code 0} disables it
         * @return this builder
         */
        public Builder withMaxItemsScannedPerSec(int maxItemsScannedPerSec) {
            this.maxItemsScannedPerSec = Math.max(0, maxItemsScannedPerSec);
            return this;
        }

        /**
         * Set the maximum number of items that may be deleted per second.
         * The runner will delay between transactions to stay at or below this rate.
         * A value of {@code 0} (the default) disables deleted-item throttling.
         *
         * @param maxItemsDeletedPerSec the rate limit; {@code 0} disables it
         * @return this builder
         */
        public Builder withMaxItemsDeletedPerSec(int maxItemsDeletedPerSec) {
            this.maxItemsDeletedPerSec = Math.max(0, maxItemsDeletedPerSec);
            return this;
        }

        /**
         * Set the number of consecutive successful transactions required before the limit is
         * increased. Defaults to {@value DEFAULT_INCREASE_LIMIT_AFTER}.
         *
         * @param increaseLimitAfter consecutive successes before a limit increase
         * @return this builder
         */
        public Builder withIncreaseLimitAfter(int increaseLimitAfter) {
            this.increaseLimitAfter = Math.max(1, increaseLimitAfter);
            return this;
        }

        /**
         * Set the initial and maximum per-transaction limit passed to the task via
         * {@link QuotaManager#getLimit()}.
         * <p>
         * The limit starts at this value. It will not be increased beyond {@code maxLimit} on
         * success. If {@code maxLimit} is {@code 0} (the default) the limit feature is disabled:
         * {@link QuotaManager#getLimit()} always returns {@code 0} and is never adjusted.
         * </p>
         *
         * @param maxLimit the initial and maximum limit; {@code 0} disables limit management
         * @return this builder
         */
        public Builder withMaxLimit(int maxLimit) {
            this.maxLimit = Math.max(0, maxLimit);
            return this;
        }

        /**
         * Set the maximum number of times to retry a failed transaction before giving up.
         * The counter resets after each successful commit. Defaults to {@value DEFAULT_NUM_RETRIES}.
         *
         * @param numOfRetries maximum retries
         * @return this builder
         */
        public Builder withNumOfRetries(int numOfRetries) {
            this.numOfRetries = Math.max(0, numOfRetries);
            return this;
        }

        /**
         * Set whether to commit each transaction when the task returns.
         * If {@code false}, transactions are rolled back instead of committed (useful for read-only tasks).
         * Defaults to {@code true}.
         *
         * @param commitWhenDone {@code true} to commit, {@code false} to roll back
         * @return this builder
         */
        public Builder withCommitWhenDone(boolean commitWhenDone) {
            this.commitWhenDone = commitWhenDone;
            return this;
        }

        /**
         * Build the {@link ThrottledRetryingRunner}.
         *
         * @return a new runner
         */
        public ThrottledRetryingRunner build() {
            return new ThrottledRetryingRunner(this);
        }
    }
}
