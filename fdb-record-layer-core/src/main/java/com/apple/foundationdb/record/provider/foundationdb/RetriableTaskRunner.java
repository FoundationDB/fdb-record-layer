/*
 * RetryTask.java
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

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.util.LoggableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class RetriableTaskRunner<T> implements AutoCloseable {
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(RetriableTaskRunner.class);

    @Nonnull private final Supplier<CompletableFuture<? extends T>> retriableTask;
    private final int maxAttempts;

    // This can also handle other things need to be done post the retriable task.
    @Nonnull private final Function<TaskState<T>, CompletableFuture<Boolean>> possiblyRetry;
    @Nonnull private final Consumer<TaskState<T>> handleIfDoRetry;
    private final long initDelayMillis;
    private final long minDelayMillis;
    private final long maxDelayMillis;
    @Nonnull private final Logger logger;
    @Nonnull private final List<Object> additionalLogMessageKeyValues;
    @Nonnull private final Executor executor;
    @Nullable private final Function<Throwable,RuntimeException> exceptionMapper;

    private boolean closed = false;
    @Nonnull
    private final List<CompletableFuture<?>> futuresToCompleteExceptionally = new ArrayList<>();

    public static <T> Builder<T> newBuilder(Supplier<CompletableFuture<? extends T>> retriableTask, int maxAttempts) {
        return new Builder<>(retriableTask, maxAttempts);
    }

    public CompletableFuture<T> runAsync() {
        final TaskState<T> taskState = new TaskState<>(initDelayMillis, initGlobalLogs());
        return AsyncUtil.whileTrue(() -> MoreAsyncUtil.composeWhenCompleteAndHandle(retriableTask.get(), (result, ex) -> {
            if (closed) {
                taskState.setPossibleException(new RetriableTaskRunnerClosed());
                return AsyncUtil.READY_FALSE;
            }
            taskState.setPossibleResult(result);
            taskState.setPossibleException(ex);
            return AsyncUtil.applySafely(possiblyRetry, taskState).thenCompose(possible -> {
                if (possible && (taskState.getCurrAttempt() + 1 < maxAttempts)) {
                    handleIfDoRetry.accept(taskState); // Exceptions in handleIfDoRetry should be caught by handle later.
                    long delay = (long)(Math.random() * taskState.getCurrDelayRangeMillis());
                    taskState.setCurrAttempt(taskState.getCurrAttempt() + 1);
                    taskState.setCurrDelayRangeMillis(Math.max(Math.min(delay * 2, maxDelayMillis), minDelayMillis));
                    logCurrentAttempt(taskState, delay);
                    CompletableFuture<Void> delayedFuture = MoreAsyncUtil.delayedFuture(delay, TimeUnit.MILLISECONDS);
                    addFutureToCompleteExceptionally(delayedFuture);
                    return delayedFuture.thenApply(vignore -> true);
                } else {
                    return AsyncUtil.READY_FALSE;
                }
            });
        }, exceptionMapper), executor).handle((vignore, otherException) -> {
            if (taskState.getPossibleException() == null) {
                // This handles the possible exceptions in handleIfDoRetry.
                taskState.setPossibleException(otherException);
            }
            return null;
        }).thenCompose(vignore -> {
            CompletableFuture<T> ret = new CompletableFuture<>();
            addFutureToCompleteExceptionally(ret);
            Throwable e = taskState.getPossibleException();
            if (e != null) {
                ret.completeExceptionally(addLogsToException(taskState, e));
            } else {
                ret.complete(taskState.getPossibleResult());
            }
            return ret;
        });
    }

    private List<Object> initGlobalLogs() {
        List<Object> globalLogs = new ArrayList<>(additionalLogMessageKeyValues);
        globalLogs.addAll(Arrays.asList(
                LogMessageKeys.MAX_ATTEMPTS, maxAttempts
        ));
        return globalLogs;
    }

    private void logCurrentAttempt(TaskState<T> taskState, long delay) {
        if (logger.isWarnEnabled()) {
            final KeyValueLogMessage message = KeyValueLogMessage.build("Retrying Exception",
                    LogMessageKeys.CURR_ATTEMPT, taskState.getCurrAttempt(),
                    LogMessageKeys.DELAY, delay);
            message.addKeysAndValues(taskState.getLocalLogs());
            message.addKeysAndValues(additionalLogMessageKeyValues);
            logger.warn(message.toString(), taskState.getPossibleException());
        }
        taskState.getLocalLogs().clear();
    }

    private Throwable addLogsToException(TaskState<T> taskState, Throwable e) {
        LoggableException loggableException = (e instanceof LoggableException) ?
                                              (LoggableException)e : new LoggableException(e);
        return loggableException
                .addLogInfo(taskState.getGlobalLogs().toArray())
                // Include the local logs of the last retry as well.
                .addLogInfo(taskState.getLocalLogs().toArray());
    }

    private synchronized void addFutureToCompleteExceptionally(@Nonnull CompletableFuture<?> future) {
        if (closed) {
            final RetriableTaskRunnerClosed exception = new RetriableTaskRunnerClosed();
            future.completeExceptionally(exception);
            throw exception;
        }
        futuresToCompleteExceptionally.removeIf(CompletableFuture::isDone);
        futuresToCompleteExceptionally.add(future);
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (!futuresToCompleteExceptionally.stream().allMatch(CompletableFuture::isDone)) {
            final Exception exception = new FDBDatabaseRunner.RunnerClosed();
            for (CompletableFuture<?> future : futuresToCompleteExceptionally) {
                future.completeExceptionally(exception);
            }
        }
    }

    public static class TaskState<T> {
        @Nullable
        private T possibleResult;
        @Nullable
        private Throwable possibleException;
        private int currAttempt = 0;
        private long currDelayRangeMillis;
        // Keep the logs for the current attempt.
        @Nonnull
        private final List<Object> localLogs = new ArrayList<>();
        // Keep the logs for the entire run, including the additional ones provided by caller.
        @Nonnull
        private final List<Object> globalLogs;

        TaskState(long initDelayMillis, List<Object> globalLogs) {
            this.currDelayRangeMillis = initDelayMillis;
            this.globalLogs = new ArrayList<>(globalLogs);
        }

        @Nullable
        public T getPossibleResult() {
            return possibleResult;
        }

        public void setPossibleResult(@Nullable T possibleResult) {
            this.possibleResult = possibleResult;
        }

        @Nullable
        public Throwable getPossibleException() {
            return possibleException;
        }

        public void setPossibleException(@Nullable Throwable possibleException) {
            this.possibleException = possibleException;
        }

        public int getCurrAttempt() {
            return currAttempt;
        }

        public void setCurrAttempt(int currAttempt) {
            this.currAttempt = currAttempt;
        }

        public long getCurrDelayRangeMillis() {
            return currDelayRangeMillis;
        }

        public void setCurrDelayRangeMillis(long currDelayRangeMillis) {
            this.currDelayRangeMillis = currDelayRangeMillis;
        }

        @Nonnull
        public List<Object> getLocalLogs() {
            return localLogs;
        }

        @Nonnull
        public List<Object> getGlobalLogs() {
            return globalLogs;
        }
    }

    public static class Builder<T> {
        @Nonnull private final Supplier<CompletableFuture<? extends T>> retriableTask;
        private final int maxAttempts;

        @Nonnull private Function<TaskState<T>, CompletableFuture<Boolean>> possiblyRetry =
                taskState -> (taskState.getPossibleResult() == null) ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
        @Nonnull private Consumer<TaskState<T>> handleIfDoRetry = taskState -> { };
        private long initDelayMillis = 0;
        private long minDelayMillis = 0;
        private long maxDelayMillis = 0;
        @Nonnull private Logger logger = DEFAULT_LOGGER;
        @Nonnull private List<Object> additionalLogMessageKeyValues = Collections.emptyList();
        @Nonnull private Executor executor = FDB.DEFAULT_EXECUTOR;
        @Nullable private Function<Throwable,RuntimeException> exceptionMapper = FDBExceptions::wrapException;

        private Builder(Supplier<CompletableFuture<? extends T>> retriableTask, int maxAttempts) {
            this.retriableTask = retriableTask;
            this.maxAttempts = maxAttempts;
        }

        public Builder<T> setPossiblyRetry(Function<TaskState<T>, CompletableFuture<Boolean>> possiblyRetry) {
            this.possiblyRetry = possiblyRetry;
            return this;
        }

        public Builder<T> setHandleIfDoRetry(Consumer<TaskState<T>> handleIfDoRetry) {
            this.handleIfDoRetry = handleIfDoRetry;
            return this;
        }

        public Builder<T> setInitDelayMillis(long initDelayMillis) {
            this.initDelayMillis = initDelayMillis;
            return this;
        }

        public Builder<T> setMinDelayMillis(long minDelayMillis) {
            this.minDelayMillis = minDelayMillis;
            return this;
        }

        public Builder<T> setMaxDelayMillis(long maxDelayMillis) {
            this.maxDelayMillis = maxDelayMillis;
            return this;
        }

        public Builder<T> setLogger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder<T> setAdditionalLogMessageKeyValues(@Nullable List<Object> additionalLogMessageKeyValues) {
            if (additionalLogMessageKeyValues != null) {
                this.additionalLogMessageKeyValues = additionalLogMessageKeyValues;
            }
            return this;
        }

        public Builder<T> setExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Builder<T> setExceptionMapper(@Nullable Function<Throwable, RuntimeException> exceptionMapper) {
            this.exceptionMapper = exceptionMapper;
            return this;
        }

        public RetriableTaskRunner<T> build() {
            return new RetriableTaskRunner<>(this);
        }
    }

    private RetriableTaskRunner(Builder<T> builder) {
        retriableTask = builder.retriableTask;
        maxAttempts = builder.maxAttempts;
        possiblyRetry = builder.possiblyRetry;
        handleIfDoRetry = builder.handleIfDoRetry;
        initDelayMillis = builder.initDelayMillis;
        minDelayMillis = builder.minDelayMillis;
        maxDelayMillis = builder.maxDelayMillis;
        logger = builder.logger;
        additionalLogMessageKeyValues = builder.additionalLogMessageKeyValues;
        executor = builder.executor;
        exceptionMapper = builder.exceptionMapper;
    }
}
