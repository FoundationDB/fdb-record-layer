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

    /**
     * Create a new {@link TransactionalLimitedRunner} that can be used to run limited code within a transaction.
     * @param database the database to run against
     * @param contextConfig the config to use when opening the transaction
     * @param maxLimit the maximum limit to apply when running code
     * @param exponentialDelay delay to be used between failures
     */
    public TransactionalLimitedRunner(@Nonnull final FDBDatabase database,
                                      @Nonnull final FDBRecordContextConfig contextConfig,
                                      final int maxLimit,
                                      @Nonnull final ExponentialDelay exponentialDelay) {
        this.limitedRunner = new LimitedRunner(database.newContextExecutor(contextConfig.getMdcContext()),
                maxLimit, exponentialDelay);
        this.transactionalRunner = new TransactionalRunner(database, contextConfig);
    }


    /**
     * Create a new {@link TransactionalLimitedRunner} with a <em>mutable</em> {@link FDBRecordContextConfig.Builder}.
     * <p>
     *     You probably don't want to call this, and should probably call
     *     {@link #TransactionalLimitedRunner(FDBDatabase, FDBRecordContextConfig, int, ExponentialDelay)} instead.
     * </p>
     * @param database the database to run against
     * @param contextConfigBuilder the config to use when opening the transaction
     * Note: The same as FDBDatabaseRunnerImpl, this maintains mutability, but that mutability is not thread safe, so
     * there is risk if it is changed while, while simultaneously calling {@link #runAsync}.
     * @param maxLimit the maximum limit to apply when running code
     * @param exponentialDelay delay to be used between failures
     */
    public TransactionalLimitedRunner(@Nonnull final FDBDatabase database,
                                      @Nonnull final FDBRecordContextConfig.Builder contextConfigBuilder,
                                      final int maxLimit,
                                      @Nonnull final ExponentialDelay exponentialDelay) {
        this.limitedRunner = new LimitedRunner(database.newContextExecutor(contextConfigBuilder.getMdcContext()),
                maxLimit, exponentialDelay);
        this.transactionalRunner = new TransactionalRunner(database, contextConfigBuilder);
    }

    public CompletableFuture<Void> runAsync(Runner runnable, final List<Object> additionalLogMessageKeyValues) {
        AtomicBoolean clearWeakReadSemantics = new AtomicBoolean(false);
        return limitedRunner.runAsync(runState -> transactionalRunner.runAsync(
                clearWeakReadSemantics.getAndSet(true),
                context -> runnable.runAsync(new RunState(runState, context))), additionalLogMessageKeyValues);
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

    public int getMaxLimit() {
        return limitedRunner.getMaxLimit();
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
         * @param runState the state/configuration of this attempt/range of work
         * @return a future that will have a value of {@code true} if there are more operations to do, or {@code false},
         * if the work has been completed.
         */
        CompletableFuture<Boolean> runAsync(RunState runState);
    }

    /**
     * Parameter object for {@link Runner#runAsync(RunState)}.
     * @see LimitedRunner.RunState
     */
    public static class RunState {
        @Nonnull
        private final LimitedRunner.RunState parent;
        @Nonnull
        private final FDBRecordContext context;

        public RunState(@Nonnull final LimitedRunner.RunState parent, @Nonnull final FDBRecordContext context) {
            this.parent = parent;
            this.context = context;
        }

        @Nonnull
        public FDBRecordContext getContext() {
            return context;
        }

        /**
         *
         * The limit of work to be processed.
         * @return the maximum number of items this run should process
         * @see LimitedRunner.RunState#getLimit()
         */
        public int getLimit() {
            return parent.getLimit();
        }

        /**
         * Add additional log message key/values that are determined while running.
         * @param key the log message key
         * @param value the log message value
         * @see LimitedRunner.RunState#addLogMessageKeyValue(Object, Object)
         */
        public void addLogMessageKeyValue(Object key, Object value) {
            parent.addLogMessageKeyValue(key, value);
        }
    }
}
