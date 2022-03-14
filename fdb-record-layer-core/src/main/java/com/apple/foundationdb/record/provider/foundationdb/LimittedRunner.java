/*
 * LimittedRunner.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class LimittedRunner implements AutoCloseable {

    // These error codes represent a list of errors that can occur if there is too much work to be done
    // in a single transaction.
    private static final Set<Integer> lessenWorkCodes = Set.of(
            FDBError.TIMED_OUT.code(),
            FDBError.TRANSACTION_TOO_OLD.code(),
            FDBError.NOT_COMMITTED.code(),
            FDBError.TRANSACTION_TIMED_OUT.code(),
            FDBError.COMMIT_READ_INCOMPLETE.code(),
            FDBError.TRANSACTION_TOO_LARGE.code());

    private int currentLimit;
    private int maxLimit;
    private int retriesAtMinimum = 0;
    private int successCount = 0;
    private int increaseLimitAfter;
    private int maxRetriesAtMinimum = 10; // maybe this should be configurable
    private boolean closed = false;

    public LimittedRunner(final int maxLimit, final int increaseLimitAfter) {
        this.currentLimit = maxLimit;
        this.maxLimit = maxLimit;
        this.increaseLimitAfter = increaseLimitAfter;
    }

    public CompletableFuture<Void> runAsync(Runner runner) {
        final CompletableFuture<Void> overallResult = new CompletableFuture<>();
        AsyncUtil.whileTrue(() -> {
            if (closed) {
                overallResult.completeExceptionally(new FDBDatabaseRunner.RunnerClosed());
                return AsyncUtil.READY_FALSE;
            }
            return runner.runAsync(currentLimit)
                    .handle((shouldContinue, error) -> handle(overallResult, shouldContinue, error));
        });
        return overallResult;
    }

    private Boolean handle(final CompletableFuture<Void> overallResult, final Boolean shouldContinue, final Throwable error) {
        if (error == null) {
            // TODO indexer tracks records scanned only for successful
            maybeIncreaseLimit();
            if (!shouldContinue) {
                overallResult.complete(null);
            }
            return shouldContinue;
        } else {
            successCount = 0;
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
        if (fdbException != null && lessenWorkCodes.contains(fdbException.getCode())) {
            if (currentLimit == 1) {
                retriesAtMinimum++;
                return retriesAtMinimum < maxRetriesAtMinimum;
            } else {
                // TODO should we delay ?
                // TODO log
                //      does the log need to include logging details from the runner
                currentLimit = Math.max(1, (3 * currentLimit) / 4);
                return true;
            }
        } else {
            // Not something to lessen work, so don't retry
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
        successCount++;
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

    public void setMaxLimit(final int maxLimit) {
        // TODO does this need to protect for multiple threads
        this.maxLimit = maxLimit;
        if (currentLimit > maxLimit) {
            currentLimit = maxLimit;
        }
    }

    public void setIncreaseLimitAfter(final int increaseLimitAfter) {
        // TODO does this need to protect for multiple threads
        this.increaseLimitAfter = increaseLimitAfter;
    }

    public interface Runner {
        CompletableFuture<Boolean> runAsync(int limit);
    }
}
