/*
 * LimitedRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.runners.ExponentialDelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A class for doing an operation repeatedly, in a chunked fashion, where the limit per chunk may adjust depending on
 * failure/success rates.
 * <p>
 *     As an example, one might use this to alter every record in a database, over many transactions. Rather than having
 *     to hardcode how many records to modify per transaction, this will provide a limit to the code, which will then
 *     update N records. If the transaction fails for a generic retryable error, the operation will be retried. If it
 *     fails due to an error that indicates that it tried to do too much, it will retry with a lower limit. If previous
 *     success rates indicate that perhaps a higher limit could be used, the limit may be increased for future attempts.
 * </p>
 * <p>
 *     Right now this is not designed for multiple workers concurrently using the same {@link LimitedRunner}, it is
 *     intended to be used more as a one-off, or sequentially. If used sequentially, later calls to {@link #runAsync}
 *     will start with the limit that the previous call ended with. Most likely, you only want to call {@link #runAsync}
 *     once per instance of {@link LimitedRunner}. Similarly, the setters are only valid to be called before calling
 *     {@link #runAsync}, or within the {@code runner}.
 * </p>
 *
 * @see TransactionalLimitedRunner for code that connects this to opening/committing transactions
 * @see FDBDatabaseRunner if limit management does not make sense
 */
public class LimitedRunner implements AutoCloseable {

    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(LimitedRunner.class);

    // These error codes represent a list of errors that can occur if there is too much work to be done
    // in a single transaction.
    private static final Set<Integer> lessenWorkCodes = Set.of(
            FDBError.TIMED_OUT.code(),
            FDBError.TRANSACTION_TOO_OLD.code(),
            FDBError.NOT_COMMITTED.code(),
            FDBError.TRANSACTION_TIMED_OUT.code(),
            FDBError.COMMIT_READ_INCOMPLETE.code(),
            FDBError.TRANSACTION_TOO_LARGE.code());

    public static final int DO_NOT_INCREASE_LIMIT = -1;

    private final Executor executor;
    private final ExponentialDelay exponentialDelay;
    @Nonnull
    private final List<CompletableFuture<?>> futuresToCompleteExceptionally;
    private int currentLimit = 100;
    private int maxLimit = 100;
    private int increaseLimitAfter = DO_NOT_INCREASE_LIMIT;
    private int decreaseLimitAfter = 10;
    private int maxDecreaseRetries = 100;
    private int successSinceLastIncrease = 0;
    private int failuresSinceLastDecrease = 0;
    private int failuresSinceLastSuccess = 0;
    private boolean closed = false;

    public LimitedRunner(final Executor executor, final int maxLimit,
                         final ExponentialDelay exponentialDelay) {
        this.executor = executor;
        this.currentLimit = maxLimit;
        this.maxLimit = maxLimit;
        this.exponentialDelay = exponentialDelay;
        futuresToCompleteExceptionally = new ArrayList<>();
    }

    /**
     * Run the {@code runner} code, retrying, and adjusting the limits depending on failures/successes.
     * @param runner some code to run with a given limit
     * @param additionalLogMessageKeyValues additional log keys and values to be included in logs about retries and
     * increasing/decreasing the limit. See also: {@link RunState#addLogMessageKeyValue(Object, Object)}.
     * @return a future that will be completed when the future returned by {@code runner.runAsync} returns {@code false},
     * or the retries are exhausted
     */
    public CompletableFuture<Void> runAsync(Runner runner, final List<Object> additionalLogMessageKeyValues) {
        final CompletableFuture<Void> overallResult = new CompletableFuture<>();
        addFutureToCompleteExceptionally(overallResult);
        successSinceLastIncrease = 0;
        failuresSinceLastSuccess = 0;
        failuresSinceLastDecrease = 0;
        AsyncUtil.whileTrue(() -> {
            if (closed) {
                overallResult.completeExceptionally(new FDBDatabaseRunner.RunnerClosed());
                return AsyncUtil.READY_FALSE;
            }
            final RunState runState = new RunState(currentLimit);
            try {
                return runner.runAsync(runState)
                        .handle((shouldContinue, error) -> handle(overallResult, shouldContinue, error,
                                additionalLogMessageKeyValues, runState))
                        .thenCompose(Function.identity());
            } catch (RuntimeException e) {
                return handle(overallResult, true, e, additionalLogMessageKeyValues, runState);
            }
        }, executor);
        return overallResult;
    }

    private CompletableFuture<Boolean> handle(final CompletableFuture<Void> overallResult,
                                              final Boolean shouldContinue,
                                              final Throwable error,
                                              final List<Object> additionalLogMessageKeyValues,
                                              final RunState runState) {
        if (error == null) {
            failuresSinceLastSuccess = 0;
            successSinceLastIncrease++;
            maybeIncreaseLimit(additionalLogMessageKeyValues);
            exponentialDelay.reset();
            if (!shouldContinue) {
                overallResult.complete(null);
            }
            return CompletableFuture.completedFuture(shouldContinue);
        } else {
            successSinceLastIncrease = 0;
            failuresSinceLastSuccess++;
            if (!maybeDecreaseLimit(error, additionalLogMessageKeyValues, runState)) {
                overallResult.completeExceptionally(error);
                return AsyncUtil.READY_FALSE;
            } else {
                final CompletableFuture<Void> delayFuture = exponentialDelay.delay();
                addFutureToCompleteExceptionally(delayFuture);
                return delayFuture.thenApply(vignore -> true);
            }
        }
    }

    private void maybeIncreaseLimit(final List<Object> additionalLogMessageKeyValues) {
        if (increaseLimitAfter > 0 && successSinceLastIncrease >= increaseLimitAfter && currentLimit < maxLimit) {
            currentLimit = Math.min(maxLimit, Math.max(currentLimit + 1, (4 * currentLimit) / 3));
            logIncreaseLimit(additionalLogMessageKeyValues);
            successSinceLastIncrease = 0;
        }
    }

    private boolean maybeDecreaseLimit(final Throwable error,
                                       final List<Object> additionalLogMessageKeyValues,
                                       final RunState runState) {
        FDBException fdbException = getFDBException(error);
        if (fdbException != null && isRetryable(fdbException)) {
            failuresSinceLastDecrease++;
            if (failuresSinceLastDecrease < decreaseLimitAfter) {
                logRetryException(false, additionalLogMessageKeyValues, error, fdbException, runState);
                return true;
            }
        }
        if (fdbException != null && lessenWorkCodes.contains(fdbException.getCode())) {
            if (failuresSinceLastSuccess > maxDecreaseRetries) {
                return false;
            } else {
                failuresSinceLastDecrease = 0;
                logRetryException(true, additionalLogMessageKeyValues, error, fdbException, runState);
                // Note: the way this works it means that if maxDecreaseRetries is substantially higher than
                // the limit, it will retry at 1 many times. This might not make much sense, and instead it should
                // only retry at one in the `isRetryable` path, and not here.
                currentLimit = Math.max(1, (3 * currentLimit) / 4);
                return true;
            }
        } else {
            // Not something to lessen work, so don't retry
            return false;
        }
    }

    private boolean isRetryable(@Nonnull final FDBException fdbException) {
        // FDBException.isRetryable requires the api version to be set, otherwise it throws IllegalStateException
        // we catch it here, because otherwise, we would just not complete the future ever.
        // We won't complete with this exception, we'll just fail with the original exception
        try {
            return fdbException.isRetryable();
        } catch (IllegalStateException e) {
            LOGGER.warn("Failed to check if exception is retryable", e);
            return false;
        }
    }

    // Finds the FDBException that ultimately caused some throwable or
    // null if there is none. This can be then used to determine, for
    // example, the error code associated with this FDBException.
    @Nullable
    private FDBException getFDBException(@Nonnull Throwable e) {
        Throwable curr = e;
        while (curr != null) {
            if (curr instanceof FDBException) {
                return (FDBException)curr;
            } else {
                curr = curr.getCause();
            }
        }
        return null;
    }

    private void logRetryException(final boolean isDecreasingLimit,
                                   final List<Object> additionalLogMessageKeyValues,
                                   final Throwable error,
                                   final FDBException fdbException,
                                   final RunState runState) {
        if (LOGGER.isWarnEnabled()) {
            final KeyValueLogMessage message = KeyValueLogMessage.build(
                    isDecreasingLimit ? "Decreasing limit" : "Retrying with same limit",
                    LogMessageKeys.MESSAGE, fdbException.getMessage(),
                    LogMessageKeys.CODE, fdbException.getCode(),
                    LogMessageKeys.CURR_ATTEMPT, isDecreasingLimit ? failuresSinceLastSuccess : failuresSinceLastDecrease,
                    LogMessageKeys.MAX_ATTEMPTS, isDecreasingLimit ? maxDecreaseRetries : decreaseLimitAfter,
                    LogMessageKeys.LIMIT, runState.getLimit(),
                    LogMessageKeys.DELAY, exponentialDelay.getNextDelayMillis());
            if (additionalLogMessageKeyValues != null) {
                message.addKeysAndValues(additionalLogMessageKeyValues);
            }
            runState.addLogMessageKeyValues(message);
            LOGGER.warn(message.toString(), error);
        }
    }

    private void logIncreaseLimit(final List<Object> additionalLogMessageKeyValues) {
        if (LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage message = KeyValueLogMessage.build("Increasing limit",
                    LogMessageKeys.CURR_ATTEMPT, successSinceLastIncrease,
                    LogMessageKeys.MAX_LIMIT, maxLimit,
                    LogMessageKeys.LIMIT, currentLimit);
            if (additionalLogMessageKeyValues != null) {
                message.addKeysAndValues(additionalLogMessageKeyValues);
            }
            LOGGER.info(message.toString());
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        final Exception exception = new FDBDatabaseRunner.RunnerClosed();
        for (CompletableFuture<?> future : futuresToCompleteExceptionally) {
            if (!future.isDone()) {
                future.completeExceptionally(exception);
            }
        }
    }

    private synchronized void addFutureToCompleteExceptionally(@Nonnull CompletableFuture<?> future) {
        if (closed) {
            final FDBDatabaseRunner.RunnerClosed exception = new FDBDatabaseRunner.RunnerClosed();
            future.completeExceptionally(exception);
            throw exception;
        }
        futuresToCompleteExceptionally.removeIf(CompletableFuture::isDone);
        futuresToCompleteExceptionally.add(future);
    }

    public int getMaxLimit() {
        return this.maxLimit;
    }

    /**
     * Set the maximum limit for individual attempts.
     * <p>
     *     Note: this will decrease the current limit if it is lower, but it will not increase the current limit, that
     *     will increase based on {@link #setIncreaseLimitAfter}. This includes if this is called after construction,
     *     but before calling {@link #runAsync}.
     * </p>
     * @param maxLimit a new maximum limit
     * @return this runner
     */
    public LimitedRunner setMaxLimit(final int maxLimit) {
        this.maxLimit = maxLimit;
        if (currentLimit > maxLimit) {
            currentLimit = maxLimit;
        }
        return this;
    }

    /**
     * After the given number of consecutive successes, the runner will increase the limit up to the max limit,
     * if this is greater than 0.
     * @param increaseLimitAfter after this many successes the runner will increase the limit up to the max limit.
     * If this is less than 0, the limit will never increase.
     * @return this runner
     */
    public LimitedRunner setIncreaseLimitAfter(final int increaseLimitAfter) {
        this.increaseLimitAfter = increaseLimitAfter;
        return this;
    }

    /**
     * For exceptions that are retriable, this is the number of times the given limit will be retried before decreasing.
     * @param decreaseLimitAfter the number of retries to do without decreasing the limit
     * @return this runner
     */
    public LimitedRunner setDecreaseLimitAfter(final int decreaseLimitAfter) {
        this.decreaseLimitAfter = decreaseLimitAfter;
        return this;
    }

    /**
     * Every time the runner goes to decrease the limit (based on {@link #setDecreaseLimitAfter}, if the runner has
     * failed more than this many times since its last success, the overall run will fail.
     * <p>
     *     Note: this is not checked while retrying with a given limit, so if {@link #setDecreaseLimitAfter} is 10, and
     *     this is 5, and the runner fails with an exception that is retriable and can lessen work codes, it will fail
     *     the overall run after retrying with the same limit 10 times.
     * </p>
     * @param maxDecreases the new value for max number of retries
     * @return this runner
     */
    public LimitedRunner setMaxDecreaseRetries(final int maxDecreases) {
        this.maxDecreaseRetries = maxDecreases;
        return this;
    }

    /**
     * A single operation to be run by the {@link LimitedRunner}.
     */
    @FunctionalInterface
    public interface Runner {
        /**
         * Run some code with a limit.
         * @param runState the state/configuration of this attempt/range of work
         * @return a future that will have a value of {@code true} if there are more operations to do, or {@code false},
         * if the work has been completed.
         */
        CompletableFuture<Boolean> runAsync(RunState runState);
    }

    /**
     * Parameter object for {@link Runner#runAsync(RunState)}.
     */
    public static class RunState {
        private final int limit;
        private final List<Object> additionalLogMessageKeyValues;

        private RunState(final int limit) {
            this.limit = limit;
            this.additionalLogMessageKeyValues = new ArrayList<>();
        }

        /**
         * The limit of work to be processed.
         * @return the maximum number of items this run should process
         */
        public int getLimit() {
            return limit;
        }

        /**
         * Add additional log message key/values that are determined while running.
         * <p>
         *     For example, if one is iterating over all records, and maintaining which records have been processed in
         *     the database (like {@link OnlineIndexer} does), one may want to add the start of the range being
         *     processed, so that it appears in logs, so that you know whether this is making progress or not.
         * </p>
         * @param key the log message key
         * @param value the log message value
         */
        public synchronized void addLogMessageKeyValue(Object key, Object value) {
            additionalLogMessageKeyValues.add(key);
            additionalLogMessageKeyValues.add(value);
        }

        private synchronized void addLogMessageKeyValues(KeyValueLogMessage message) {
            // synchronized with addLogMessageKeyValue to avoid ConcurrentModificationException
            message.addKeysAndValues(additionalLogMessageKeyValues);
        }
    }
}
