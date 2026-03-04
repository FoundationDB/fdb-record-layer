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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs a task repeatedly across multiple transactions, with adaptive limit management, optional
 * rate limiting, and retry logic.
 * <p>
 * The caller supplies a {@link Task} that receives a fresh {@link Transaction}, a
 * {@link QuotaManager}, and the {@link Continuation} from the last <em>successful</em>
 * transaction (or {@link StartContinuation} on the first call). The task returns a
 * {@code CompletableFuture<Continuation>}; returning a continuation with
 * {@link Continuation#hasMore()} {@code == false} stops the loop.
 * </p>
 * <p>
 * On failure the continuation is <em>not</em> advanced — the last successful continuation is
 * re-passed to the retry, so the task can resume from the same position. This distinguishes a
 * retry (same continuation) from a fresh transaction after success (new continuation).
 * </p>
 * <p>
 * The {@link QuotaManager} is reset at the start of each transaction. The task uses it to report
 * how many items were processed ({@link QuotaManager#processedCountInc()}) and deleted
 * ({@link QuotaManager#deletedCountInc()}). These counts drive the between-transaction throttle
 * delay when {@code maxItemsScannedPerSec} or {@code maxItemsDeletedPerSec} are configured, and
 * inform the adaptive limit adjustment on failure.
 * </p>
 * <p>
 * A limit of {@code 0} is treated as "no limit" — the task may do as much work as it likes in
 * each transaction and the limit is never adjusted.
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
     * Run the given task repeatedly across multiple transactions until it returns a
     * {@link Continuation} with {@link Continuation#hasMore()} {@code == false}.
     * <p>
     * On the very first call the task receives {@link StartContinuation#INSTANCE}. On each
     * subsequent call after a successful commit it receives the continuation returned by the
     * previous call. On a retry after failure it receives the same continuation that was passed
     * to the failed attempt — i.e. the last <em>successful</em> continuation is preserved.
     * </p>
     * <p>
     * A single {@link QuotaManager} is created for the lifetime of this call. Its per-transaction
     * counts are reset at the start of each transaction; its limit persists and is adjusted
     * between transactions.
     * </p>
     *
     * @param task the task to run
     * @return a future that completes normally when the task returns a continuation with
     *         {@link Continuation#hasMore()} {@code == false}, or exceptionally if the retry
     *         limit is exceeded or the runner is closed
     */
    @Nonnull
    public CompletableFuture<Void> iterateAll(@Nonnull Task task) {
        if (closed) {
            return CompletableFuture.failedFuture(new TransactionalRunner.RunnerClosed());
        }

        final QuotaManager quotaManager = new QuotaManager(maxLimit);
        final AtomicReference<Continuation> lastSuccessfulCont =
                new AtomicReference<>(StartContinuation.INSTANCE);
        return AsyncUtil.whileTrue(() ->
                runOneTransaction(task, quotaManager, lastSuccessfulCont.get())
                        .handle((continuation, ex) -> {
                            if (ex == null) {
                                lastSuccessfulCont.set(continuation);
                                return handleSuccess(quotaManager, continuation);
                            }
                            return handleFailure(ex, quotaManager);
                        })
                        .thenCompose(ret -> ret));
    }

    private CompletableFuture<Continuation> runOneTransaction(
            @Nonnull Task task,
            @Nonnull QuotaManager quotaManager,
            @Nonnull Continuation continuation) {
        transactionStartTimeMillis = System.currentTimeMillis();
        return transactionalRunner.runAsync(commitWhenDone, transaction -> {
            quotaManager.initTransaction();
            return task.run(transaction, quotaManager, continuation);
        });
    }

    private CompletableFuture<Boolean> handleSuccess(
            @Nonnull QuotaManager quotaManager,
            @Nonnull Continuation continuation) {
        failureRetriesCounter = 0;
        ++consecutiveSuccessCount;

        // Increase limit after enough consecutive successes, but only when a limit is active
        if (quotaManager.limit > 0 && consecutiveSuccessCount >= increaseLimitAfter) {
            quotaManager.increaseLimit();
            consecutiveSuccessCount = 0;
        }

        if (!continuation.hasMore()) {
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

    private CompletableFuture<Boolean> handleFailure(
            @Nonnull Throwable ex,
            @Nonnull QuotaManager quotaManager) {
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

    // -------------------------------------------------------------------------
    // Continuation
    // -------------------------------------------------------------------------

    /**
     * Represents the result of one transaction's work and signals whether the runner should
     * start another transaction.
     * <p>
     * Implementations are free to carry any state needed to resume work at the right position
     * in the next transaction. The runner itself only inspects {@link #hasMore()}.
     * </p>
     */
    @FunctionalInterface
    public interface Continuation {
        /**
         * Returns {@code true} if there is more work to do; {@code false} if the source has
         * been exhausted and the loop should stop after committing the current transaction.
         */
        boolean hasMore();
    }

    /**
     * The continuation passed to the task on the very first transaction.
     * <p>
     * Tasks can detect the first call with {@code continuation instanceof StartContinuation}.
     * </p>
     */
    public static final class StartContinuation implements Continuation {
        /** The singleton instance to pass on the first call. */
        public static final StartContinuation INSTANCE = new StartContinuation();

        private StartContinuation() {
        }

        @Override
        public boolean hasMore() {
            return true;
        }
    }

    // -------------------------------------------------------------------------
    // Task
    // -------------------------------------------------------------------------

    /**
     * A task that performs work inside a single transaction and returns a continuation.
     */
    @FunctionalInterface
    public interface Task {
        /**
         * Run one transaction's worth of work.
         *
         * @param transaction the transaction for this attempt
         * @param quotaManager tracks work done this transaction and exposes the current limit
         * @param continuation the continuation from the last <em>successful</em> transaction,
         *                     or {@link StartContinuation#INSTANCE} on the very first call;
         *                     on a retry this is the same continuation that was passed to the
         *                     failed attempt
         * @return a future containing the continuation describing where the next transaction
         *         should resume; return a continuation with {@link Continuation#hasMore()}
         *         {@code == false} to stop the loop
         */
        @Nonnull
        CompletableFuture<Continuation> run(@Nonnull Transaction transaction,
                                             @Nonnull QuotaManager quotaManager,
                                             @Nonnull Continuation continuation);
    }

    // -------------------------------------------------------------------------
    // QuotaManager
    // -------------------------------------------------------------------------

    /**
     * Tracks per-transaction resource usage and exposes the adaptive limit.
     * <p>
     * One instance is created per {@link #iterateAll} call. Per-transaction counts are reset at
     * the start of each transaction; the limit persists and is adjusted by the runner between
     * transactions.
     * </p>
     * <p>
     * The task should call {@link #getLimit()} at the start of each transaction to know how much
     * work to attempt, and increment the counts as it goes.
     * </p>
     */
    public static class QuotaManager {
        private int processedCount;
        private int deletedCount;
        private int limit;
        private final int maxLimit;

        QuotaManager(int maxLimit) {
            this.maxLimit = maxLimit;
            this.limit = maxLimit;
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

        void initTransaction() {
            processedCount = 0;
            deletedCount = 0;
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
            // Use the count from the transaction that just failed (before the next initTransaction resets it)
            limit = Math.max(1, (processedCount * 9) / 10);
        }
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

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
         * The limit starts at this value and will not be increased beyond it. If {@code 0}
         * (the default) the limit feature is disabled: {@link QuotaManager#getLimit()} always
         * returns {@code 0} and is never adjusted.
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