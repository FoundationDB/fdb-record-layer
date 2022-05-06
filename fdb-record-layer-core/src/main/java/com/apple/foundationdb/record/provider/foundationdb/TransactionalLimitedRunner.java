/*
 * TransactionalLimitedRunner.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.runners.ExponentialDelay;
import com.apple.foundationdb.record.provider.foundationdb.runners.TransactionalRunner;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class similar to {@link LimitedRunner}, but that combines with {@link TransactionalRunner} to run each operation
 * in a transaction and commit it.
 */
@API(API.Status.EXPERIMENTAL)
public class TransactionalLimitedRunner implements AutoCloseable {

    private final TransactionalRunner transactionalRunner;
    private final LimitedRunner limitedRunner;
    private boolean closed;

    public TransactionalLimitedRunner(@Nonnull FDBDatabase database,
                                      FDBRecordContextConfig.Builder contextConfigBuilder,
                                      int maxLimit) {
        this.limitedRunner = new LimitedRunner(database.newContextExecutor(contextConfigBuilder.getMdcContext()),
                maxLimit, new ExponentialDelay(3, 10)); // TODO make these literals come from somewhere
        this.transactionalRunner = new TransactionalRunner(database, contextConfigBuilder);
    }

    public CompletableFuture<Void> runAsync(Runner runnable, final List<Object> additionalLogMessageKeyValues) {
        AtomicBoolean isFirst = new AtomicBoolean(true);
        return limitedRunner.runAsync(limit -> transactionalRunner.runAsync(isFirst.getAndSet(false),
                context -> runnable.runAsync(context, limit)), additionalLogMessageKeyValues);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        transactionalRunner.close();
        limitedRunner.close();
        this.closed = true;
    }

    public TransactionalLimitedRunner setIncreaseLimitAfter(final int increaseLimitAfter) {
        limitedRunner.setIncreaseLimitAfter(increaseLimitAfter);
        return this;
    }

    public TransactionalLimitedRunner setDecreaseLimitAfter(final int maxAttempts) {
        limitedRunner.setDecreaseLimitAfter(maxAttempts);
        return this;
    }

    public TransactionalLimitedRunner setMaxDecreaseRetries(final int maxDecreases) {
        limitedRunner.setMaxDecreaseRetries(maxDecreases);
        return this;
    }

    public TransactionalLimitedRunner setMaxLimit(final int maxLimit) {
        limitedRunner.setMaxLimit(maxLimit);
        return this;
    }

    /**
     * A single operation to be run by the {@link TransactionalLimitedRunner}.
     * @see LimitedRunner.Runner
     */
    @FunctionalInterface
    public interface Runner {
        /**
         * Run some code in a transaction with some limit.
         * @param context the transaction for this run of the operation. At the time this has been passed in, no
         * operations will have been done, so one does not have to worry about the transaction time limit.
         * @param limit the number of operations to do with this context.
         * @return a future that will have a value of {@code true} if there are more operations to do, or {@code false},
         * if the work has been completed.
         */
        CompletableFuture<Boolean> runAsync(FDBRecordContext context, int limit);
    }
}
