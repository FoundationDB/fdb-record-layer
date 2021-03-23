/*
 * RetriableTaskRunner.java
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A runner that can run and retry a task synchronously ({@link #run(Function, FDBDatabaseRunner)}) or asynchronously
 * ({@link #runAsync(Function)}). Users can configure the maximum number of retries, decide whether or not retry given
 * the exception. It supports back off (controled by <code>initDelayMillis</code>, <code>minDelayMillis</code>,
 * <code>maxDelayMillis</code>) and jitter when retrying. It also provides convenient API for logging information scoped
 * to the current attempt ({@link TaskState#getLocalLogs()}) or then entire run ({@link TaskState#getGlobalLogs()}).
 * All exceptions are wrapped with {@link LoggableException} (if they are not originally) and combined with internal or
 * user-provided logs through {@link TaskState} API.
 * @param <T> the type of the task's result
 */
@SuppressWarnings({"PMD.MoreThanOneLogger", "PMD.LoggerIsNotStaticFinal"})
public class RetriableTaskRunner<T> implements AutoCloseable {
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(RetriableTaskRunner.class);
    private final int maxAttempts;

    // This can also handle other things need to be done post the retriable task.
    @Nonnull private final Function<TaskState<T>, Boolean> handleAndCheckRetriable;
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

    public static <T> Builder<T> newBuilder(int maxAttempts) {
        return new Builder<>(maxAttempts);
    }

    public T run(@Nonnull final Function<TaskState<T>, ? extends T> retriableTask, FDBDatabaseRunner runner) {
        final TaskState<T> taskState = initializeTaskState();
        boolean again = true;
        while (again) {
            T result = null;
            Exception ex = null;
            try {
                result = retriableTask.apply(taskState);

            } catch (Exception taskException) {
                ex = taskException;
            }
            // There shouldn't be any exception coming out of handleResultOrException (it is saved in TaskState instead),
            // so no need wrap with try.
            again = runner.asyncToSync(FDBStoreTimer.Waits.WAIT_RETRY_DELAY, handleResultOrException(taskState, result, ex));
        }
        return getResultOrExceptionForRun(taskState);
    }

    private T getResultOrExceptionForRun(final TaskState<T> taskState) {
        RuntimeException e = taskState.getPossibleException();
        if (e != null) {
            throw addLogsToException(taskState, e);
        } else {
            return taskState.getPossibleResult();
        }
    }

    public CompletableFuture<T> runAsync(@Nonnull final Function<TaskState<T>, CompletableFuture<? extends T>> retriableTask) {
        final TaskState<T> taskState = initializeTaskState();
        // There shouldn't be any exception coming out of handleResultOrException (it is saved in TaskState instead),
        // so just use simple AsyncUtil.composeHandle.
        CompletableFuture<T> ret = new CompletableFuture<>();
        addFutureToCompleteExceptionally(ret);

        AsyncUtil.whileTrue(() -> AsyncUtil.composeHandle(
                // Make sure sync exceptions in retriableTask are also handled by the handler
                // TODO: AsyncUtil.whileTrue hangs if we simply use `retriableTask.apply(taskState)`, this seems unexpected even if we might be doing it wrong. We need to reproduce it and fdb-java and may need a fix.
                AsyncUtil.DONE.thenCompose(ignore -> retriableTask.apply(taskState)),
                (result, ex) -> handleResultOrException(taskState, result, ex)
        ), executor)
                .thenCompose(vignore -> getResultOrExceptionForRunAsync(ret, taskState));
        return ret;
    }

    private CompletableFuture<Void> getResultOrExceptionForRunAsync(final CompletableFuture<T> ret, final TaskState<T> taskState) {
        RuntimeException e = taskState.getPossibleException();
        if (e != null) {
            LoggableException ex = addLogsToException(taskState, e);
            ret.completeExceptionally(ex);
        } else {
            ret.complete(taskState.getPossibleResult());
        }
        return AsyncUtil.DONE;
    }

    @Nonnull
    private TaskState<T> initializeTaskState() {
        return new TaskState<>(initDelayMillis, initGlobalLogs());
    }

    // It doesn't return any result or throw any exception. Result and exceptions are saved in taskState.
    private CompletableFuture<Boolean> handleResultOrException(final TaskState<T> taskState,
                                                               final T result,
                                                               final Throwable ex) {
        try {
            if (closed) {
                taskState.setPossibleException(new RetriableTaskRunnerClosed());
                return AsyncUtil.READY_FALSE;
            }
            taskState.setPossibleResult(result);
            taskState.setPossibleException(mapException(ex));

            final boolean possible = handleAndCheckRetriable.apply(taskState);
            if (possible && (taskState.getCurrAttempt() + 1 < maxAttempts)) {
                handleIfDoRetry.accept(taskState); // Exceptions in handleIfDoRetry should be caught by handle later.
                taskState.setCurrAttempt(taskState.getCurrAttempt() + 1);

                long delay = clampByDelayLimits((long)(Math.random() * taskState.getCurrMaxDelayMillis()));
                // Double the current max delay for the next iteration. Note that it should be doubled from the
                // iteration's max delay rather than actual delay, because multiplying by Math.random() * 2
                // repeatedly will converge it to 0.
                taskState.setCurrMaxDelayMillis(clampByDelayLimits(taskState.getCurrMaxDelayMillis() * 2));

                logCurrentAttempt(taskState, delay);
                CompletableFuture<Void> delayedFuture = MoreAsyncUtil.delayedFuture(delay, TimeUnit.MILLISECONDS);
                addFutureToCompleteExceptionally(delayedFuture);
                return delayedFuture.thenApply(vignore -> true);
            } else {
                return AsyncUtil.READY_FALSE;
            }
        } catch (Exception handlersException) {
            if (taskState.getPossibleException() == null) {
                // This handles the possible exceptions in handleResultOrException.
                taskState.setPossibleException(mapException(handlersException));
            }
            return AsyncUtil.READY_FALSE;
        }
    }

    private RuntimeException mapException(Throwable e) {
        if (e == null) {
            return null;
        } else if (exceptionMapper == null) {
            return new RuntimeException(e);
        } else {
            return exceptionMapper.apply(e);
        }
    }

    private long clampByDelayLimits(long delay) {
        return Math.max(Math.min(delay, maxDelayMillis), minDelayMillis);
    }

    private List<Object> initGlobalLogs() {
        List<Object> globalLogs = new ArrayList<>(additionalLogMessageKeyValues);
        globalLogs.addAll(Arrays.asList(
                LogMessageKeys.MAX_ATTEMPTS, maxAttempts,
                LogMessageKeys.INIT_DELAY_MILLIS, initDelayMillis,
                LogMessageKeys.MIN_DELAY_MILLIS, minDelayMillis,
                LogMessageKeys.MAX_DELAY_MILLIS, maxDelayMillis
        ));
        return globalLogs;
    }

    private void logCurrentAttempt(TaskState<T> taskState, long delay) {
        if (logger.isWarnEnabled()) {
            final KeyValueLogMessage message = KeyValueLogMessage.build("Retrying Exception",
                    LogMessageKeys.CURR_ATTEMPT, taskState.getCurrAttempt(),
                    LogMessageKeys.DELAY, delay);
            addOtherLogs(taskState, message);
            logger.warn(message.toString(), taskState.getPossibleException());
        }
        taskState.getLocalLogs().clear();
    }

    private void addOtherLogs(final TaskState<T> taskState, final KeyValueLogMessage message) {
        message.addKeysAndValues(taskState.getLocalLogs());
        message.addKeysAndValues(taskState.getGlobalLogs());
        message.addKeysAndValues(additionalLogMessageKeyValues);
    }

    private LoggableException addLogsToException(TaskState<T> taskState, RuntimeException e) {
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

    /**
     * <code>TaskState</code> includes possible result and exception, current attemp and delay, and other important
     * information that may be useful in logging.
     * @param <T> the type of the task's result
     */
    public static class TaskState<T> {
        @Nullable
        private T possibleResult;
        @Nullable
        private RuntimeException possibleException;
        private int currAttempt = 0;
        private long currMaxDelayMillis;
        // Keep the logs for the current attempt.
        @Nonnull
        private final List<Object> localLogs = new ArrayList<>();
        // Keep the logs for the entire run, including the additional ones provided by caller.
        @Nonnull
        private final List<Object> globalLogs;
        @Nonnull
        private UUID uuid;

        TaskState(long initDelayMillis, List<Object> globalLogs) {
            this.currMaxDelayMillis = initDelayMillis;
            this.globalLogs = new ArrayList<>(globalLogs);

            uuid = UUID.randomUUID();
            this.globalLogs.addAll(Arrays.asList(
                    LogMessageKeys.UUID, uuid
            ));
        }

        @Nullable
        public T getPossibleResult() {
            return possibleResult;
        }

        public void setPossibleResult(@Nullable T possibleResult) {
            this.possibleResult = possibleResult;
        }

        @Nullable
        public RuntimeException getPossibleException() {
            return possibleException;
        }

        public void setPossibleException(@Nullable RuntimeException possibleException) {
            this.possibleException = possibleException;
        }

        public int getCurrAttempt() {
            return currAttempt;
        }

        public void setCurrAttempt(int currAttempt) {
            this.currAttempt = currAttempt;
        }

        public long getCurrMaxDelayMillis() {
            return currMaxDelayMillis;
        }

        public void setCurrMaxDelayMillis(long currMaxDelayMillis) {
            this.currMaxDelayMillis = currMaxDelayMillis;
        }

        @Nonnull
        public List<Object> getLocalLogs() {
            return localLogs;
        }

        @Nonnull
        public List<Object> getGlobalLogs() {
            return globalLogs;
        }

        @Nonnull
        public UUID getUuid() {
            return uuid;
        }
    }

    /**
     * Builder of {@link RetriableTaskRunner}.
     * @param <T> the type of the task's result
     */
    public static class Builder<T> {
        private final int maxAttempts;

        @Nonnull private Function<TaskState<T>, Boolean> handleAndCheckRetriable =
                taskState -> taskState.getPossibleResult() == null;
        @Nonnull private Consumer<TaskState<T>> handleIfDoRetry = taskState -> { };
        private long initDelayMillis = 0;
        private long minDelayMillis = 0;
        private long maxDelayMillis = 0;
        @Nonnull private Logger logger = DEFAULT_LOGGER;
        @Nonnull private List<Object> additionalLogMessageKeyValues = Collections.emptyList();
        @Nonnull private Executor executor = FDB.DEFAULT_EXECUTOR;
        @Nullable private Function<Throwable,RuntimeException> exceptionMapper = FDBExceptions::wrapException;

        private Builder(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        // Note this should not be blocking.
        public Builder<T> setHandleAndCheckRetriable(Function<TaskState<T>, Boolean> handleAndCheckRetriable) {
            this.handleAndCheckRetriable = handleAndCheckRetriable;
            return this;
        }

        // Note this should not be blocking.
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
        maxAttempts = builder.maxAttempts;
        handleAndCheckRetriable = builder.handleAndCheckRetriable;
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
