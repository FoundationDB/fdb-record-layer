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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
    private int currentLimit = 100;
    private int maxLimit = 100;
    private int increaseLimitAfter = DO_NOT_INCREASE_LIMIT;
    private int decreaseLimitAfter = 10;
    private int maxDecreaseRetries = 100;
    private int successCount = 0;
    private int failuresSinceLastDecrease = 0;
    private int failuresSinceLastSuccess = 0;
    private boolean closed = false;

    public LimitedRunner(final Executor executor, final int maxLimit) {
        this.executor = executor;
        this.currentLimit = maxLimit;
        this.maxLimit = maxLimit;
    }

    public CompletableFuture<Void> runAsync(Runner runner) {
        final CompletableFuture<Void> overallResult = new CompletableFuture<>();
        successCount = 0;
        failuresSinceLastSuccess = 0;
        failuresSinceLastDecrease = 0;
        AsyncUtil.whileTrue(() -> {
            if (closed) {
                overallResult.completeExceptionally(new FDBDatabaseRunner.RunnerClosed());
                return AsyncUtil.READY_FALSE;
            }
            try {
                return runner.runAsync(currentLimit)
                        .handle((shouldContinue, error) -> handle(overallResult, shouldContinue, error));
            } catch (RuntimeException e) {
                return CompletableFuture.completedFuture(handle(overallResult, true, e));
            }
        }, executor);
        return overallResult;
    }

    private Boolean handle(final CompletableFuture<Void> overallResult, final Boolean shouldContinue, final Throwable error) {
        if (error == null) {
            failuresSinceLastSuccess = 0;
            successCount++;
            maybeIncreaseLimit();
            if (!shouldContinue) {
                overallResult.complete(null);
            }
            return shouldContinue;
        } else {
            successCount = 0;
            failuresSinceLastSuccess++;
            if (!maybeDecreaseLimit(error)) {
                overallResult.completeExceptionally(error);
                return false;
            } else {
                return true;
            }
        }
    }

    private boolean maybeDecreaseLimit(final Throwable error) {
        FDBException fdbException = getFDBException(error);
        if (fdbException != null && isRetryable(fdbException)) {
            failuresSinceLastDecrease++;
            if (failuresSinceLastDecrease < decreaseLimitAfter) {
                return true;
            }
        }
        if (fdbException != null && lessenWorkCodes.contains(fdbException.getCode())) {
            if (failuresSinceLastSuccess > maxDecreaseRetries) {
                return false;
            } else {
                failuresSinceLastDecrease = 0;
                // TODO delay here
                // TODO log
                //      does the log need to include logging details from the runner
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

    private void maybeIncreaseLimit() {
        if (successCount >= increaseLimitAfter && currentLimit < maxLimit) {
            currentLimit = Math.min(maxLimit, Math.max(currentLimit + 1, (4 * currentLimit) / 3));
            // TODO log
            //      does the log need to include logging details from the runner
        }
    }

    @Override
    public void close() {
        this.closed = true;
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

    public interface Runner {
        CompletableFuture<Boolean> runAsync(int limit);
    }
}
