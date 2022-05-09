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
            try {
                return runner.runAsync(currentLimit)
                        .handle((shouldContinue, error) -> handle(overallResult, shouldContinue, error,
                                additionalLogMessageKeyValues))
                        .thenCompose(Function.identity());
            } catch (RuntimeException e) {
                return handle(overallResult, true, e, additionalLogMessageKeyValues);
            }
        }, executor);
        return overallResult;
    }

    private CompletableFuture<Boolean> handle(final CompletableFuture<Void> overallResult,
                           final Boolean shouldContinue,
                           final Throwable error,
                           final List<Object> additionalLogMessageKeyValues) {
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
            if (!maybeDecreaseLimit(error, additionalLogMessageKeyValues)) {
                overallResult.completeExceptionally(error);
                return AsyncUtil.READY_FALSE;
            } else {
                // TODO test and ensure that the delay gets reset after success
                final CompletableFuture<Void> delayFuture = exponentialDelay.delay();
                addFutureToCompleteExceptionally(delayFuture);
                return delayFuture.thenApply(vignore -> true);
            }
        }
    }

    private void maybeIncreaseLimit(final List<Object> additionalLogMessageKeyValues) {
        if (successSinceLastIncrease >= increaseLimitAfter && currentLimit < maxLimit) {
            currentLimit = Math.min(maxLimit, Math.max(currentLimit + 1, (4 * currentLimit) / 3));
            logIncreaseLimit("Increasing limit", additionalLogMessageKeyValues);
            successSinceLastIncrease = 0;
        }
    }

    private boolean maybeDecreaseLimit(final Throwable error, final List<Object> additionalLogMessageKeyValues) {
        FDBException fdbException = getFDBException(error);
        if (fdbException != null && isRetryable(fdbException)) {
            failuresSinceLastDecrease++;
            if (failuresSinceLastDecrease < decreaseLimitAfter) {
                logRetryException(false, additionalLogMessageKeyValues, error, fdbException);
                return true;
            }
        }
        if (fdbException != null && lessenWorkCodes.contains(fdbException.getCode())) {
            if (failuresSinceLastSuccess > maxDecreaseRetries) {
                return false;
            } else {
                failuresSinceLastDecrease = 0;
                logRetryException(true, additionalLogMessageKeyValues, error, fdbException);
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
                                   final FDBException fdbException) {
        if (LOGGER.isWarnEnabled()) {
            final KeyValueLogMessage message = KeyValueLogMessage.build(
                    isDecreasingLimit ? "Decreasing limit" : "Retrying with same limit",
                    LogMessageKeys.MESSAGE, fdbException.getMessage(),
                    LogMessageKeys.CODE, fdbException.getCode(),
                    LogMessageKeys.CURR_ATTEMPT, isDecreasingLimit ? failuresSinceLastSuccess : failuresSinceLastDecrease,
                    LogMessageKeys.MAX_ATTEMPTS, isDecreasingLimit ? maxDecreaseRetries : decreaseLimitAfter,
                    LogMessageKeys.LIMIT, currentLimit,
                    LogMessageKeys.DELAY, exponentialDelay.getNextDelayMillis());
            // TODO does this need to add information about the specific attempt, since that wolud change
            //      after each success, perhaps expect additionalLogMessageKeyValues to change?
            if (additionalLogMessageKeyValues != null) {
                message.addKeysAndValues(additionalLogMessageKeyValues);
            }
            LOGGER.warn(message.toString(), error);
        }
    }

    private void logIncreaseLimit(final String staticMessage,
                                   final List<Object> additionalLogMessageKeyValues) {
        if (LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage message = KeyValueLogMessage.build(staticMessage,
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

    public LimitedRunner setMaxLimit(final int maxLimit) {
        // TODO does this need to protect for multiple threads
        this.maxLimit = maxLimit;
        if (currentLimit > maxLimit) {
            currentLimit = maxLimit;
        }
        return this;
    }

    public LimitedRunner setIncreaseLimitAfter(final int increaseLimitAfter) {
        // TODO does this need to protect for multiple threads
        this.increaseLimitAfter = increaseLimitAfter;
        return this;
    }

    public LimitedRunner setDecreaseLimitAfter(final int decreaseLimitAfter) {
        this.decreaseLimitAfter = decreaseLimitAfter;
        return this;
    }

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
         * @param limit the code should process this many operations (however it concludes it can split up work).
         * @return a future that will have a value of {@code true} if there are more operations to do, or {@code false},
         * if the work has been completed.
         */
        CompletableFuture<Boolean> runAsync(int limit);
    }
}
