/*
 * FDBDatabaseRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A context for running against an {@link FDBDatabase} with retrying of transient exceptions.
 *
 * Implements {@link #run} and {@link #runAsync} methods for executing functions in a new {@link FDBRecordContext} and returning their result.
 *
 * Implements {@link #close} in such a way that {@code runAsync} will not accidentally do more work afterwards. In particular, in the case
 * of timeouts and transaction retries, it is otherwise possible for a whole new transaction to start at some time in the future and change
 * the database even after the caller has given up, due to a timeout, for example.
 *
 *
 * <pre><code>
 * try (FDBDatabaseRunner runner = fdb.newRunner()) {
 *     CompleteableFuture&lt;Void&lt; future = runner.runAsync(context -&gt; ...);
 *     fdb.asyncToSync(timer, WAIT_XXX, future);
 * }
 * </code></pre>
 *
 * @see FDBDatabase
 */
@API(API.Status.MAINTAINED)
public class FDBDatabaseRunner implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseRunner.class);

    @Nonnull
    private final FDBDatabase database;
    @Nonnull
    private Executor executor;

    @Nullable
    private FDBStoreTimer timer;
    @Nullable
    private Map<String, String> mdcContext;
    @Nullable
    private FDBDatabase.WeakReadSemantics weakReadSemantics;

    private int maxAttempts;
    private long maxDelayMillis;
    private long initialDelayMillis;

    private boolean closed;
    @Nonnull
    private final List<FDBRecordContext> contextsToClose;
    @Nonnull
    private final List<CompletableFuture<?>> futuresToCompleteExceptionally;

    public FDBDatabaseRunner(@Nonnull FDBDatabase database,
                             @Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                             @Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        this.database = database;

        this.timer = timer;
        this.mdcContext = mdcContext;
        this.weakReadSemantics = weakReadSemantics;

        final FDBDatabaseFactory factory = database.getFactory();
        this.maxAttempts = factory.getMaxAttempts();
        this.maxDelayMillis = factory.getMaxDelayMillis();
        this.initialDelayMillis = factory.getInitialDelayMillis();

        this.executor = FDBRecordContext.initExecutor(database, mdcContext);

        contextsToClose = new ArrayList<>();
        futuresToCompleteExceptionally = new ArrayList<>();
    }

    public FDBDatabaseRunner(@Nonnull FDBDatabase database,
                             @Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext) {
        this(database, timer, mdcContext, null);
    }

    public FDBDatabaseRunner(@Nonnull FDBDatabase database) {
        this(database, null, null, null);
    }

    /**
     * Get the database against which functions are run.
     * @return the database used to run
     */
    @Nonnull
    public FDBDatabase getDatabase() {
        return database;
    }

    /**
     * Get the executor that will be used for {@link #runAsync}.
     * @return the executor to use
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get the timer used in record contexts opened by this runner.
     * @return timer to use
     */
    @Nullable
    public FDBStoreTimer getTimer() {
        return timer;
    }

    /**
     * Set the timer used in record contexts opened by this runner.
     * @param timer timer to use
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    public void setTimer(@Nullable FDBStoreTimer timer) {
        this.timer = timer;
    }

    /**
     * Get the logging context used in record contexts opened by this runner.
     * @return the logging context to use
     */
    @Nullable
    public Map<String, String> getMdcContext() {
        return mdcContext;
    }

    /**
     * Set the logging context used in record contexts opened by this runner.
     * This will change the executor to be one that restores this new {@code mdcContext}.
     * @param mdcContext the logging context to use
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    public void setMdcContext(@Nullable Map<String, String> mdcContext) {
        this.mdcContext = mdcContext;
        executor = FDBRecordContext.initExecutor(database, mdcContext);
    }

    /**
     * Get the read semantics used in record contexts opened by this runner.
     * @return allowable staleness parameters if caching read versions
     */
    @Nullable
    public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        return weakReadSemantics;
    }

    /**
     * Set the read semantics used in record contexts opened by this runner.
     * @param weakReadSemantics allowable staleness parameters if caching read versions
     * @see FDBDatabase#openContext(Map,FDBStoreTimer,FDBDatabase.WeakReadSemantics)
     */
    public void setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        this.weakReadSemantics = weakReadSemantics;
    }

    /**
     * Gets the maximum number of attempts for a database to make when running a
     * retriable transactional operation. This is used by {@link #run} and {@link #runAsync} to limit the number of
     * attempts that an operation is retried.
     * @return the maximum number of times to run a transactional database operation
     */
    public int getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * Sets the maximum number of attempts for a database to make when running a
     * retriable transactional operation. This is used by {@link #run} and {@link #runAsync} to limit the number of
     * attempts that an operation is retried.
     * @param maxAttempts the maximum number of times to run a transactional database operation
     * @throws IllegalArgumentException if a non-positive number is given
     */
    public void setMaxAttempts(int maxAttempts) {
        if (maxAttempts <= 0) {
            throw new RecordCoreException("Cannot set maximum number of attempts to less than or equal to zero");
        }
        this.maxAttempts = maxAttempts;
    }

    /**
     * Gets the minimum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to limit the time spent
     * between successive attempts at completing a database operation.
     * Currently this value is fixed at 2 milliseconds and is not settable.
     * @return the minimum delay between attempts when retrying operations
     */
    public long getMinDelayMillis() {
        return 2;
    }

    /**
     * Gets the maximum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to limit the time spent
     * between successive attempts at completing a database operation. The default value is 1000 so that
     * there will not be more than 1 second between attempts.
     * @return the maximum delay between attempts when retrying operations
     */
    public long getMaxDelayMillis() {
        return maxDelayMillis;
    }

    /**
     * Sets the maximum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to limit the time spent
     * between successive attempts at completing a database operation. The default value is 1000 so that
     * there will not be more than 1 second between attempts.
     * @param maxDelayMillis the maximum delay between attempts when retrying operations
     * @throws IllegalArgumentException if the value is negative or less than the minimum delay
     */
    public void setMaxDelayMillis(long maxDelayMillis) {
        if (maxDelayMillis < 0) {
            throw new RecordCoreException("Cannot set maximum delay milliseconds to less than or equal to zero");
        } else if (maxDelayMillis < initialDelayMillis) {
            throw new RecordCoreException("Cannot set maximum delay to less than minimum delay");
        }
        this.maxDelayMillis = maxDelayMillis;
    }

    /**
     * Gets the delay ceiling (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initial delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     * @return the delay ceiling between the first and second attempts at running a database operation
     */
    public long getInitialDelayMillis() {
        return initialDelayMillis;
    }

    /**
     * Sets the delay ceiling (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initial delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     * @param initialDelayMillis the delay ceiling between the first and second attempts at running a database operation
     * @throws IllegalArgumentException if the value is negative or greater than the maximum delay
     */
    public void setInitialDelayMillis(long initialDelayMillis) {
        if (initialDelayMillis < 0) {
            throw new RecordCoreException("Cannot set initial delay milleseconds to less than zero");
        } else if (initialDelayMillis > maxDelayMillis) {
            throw new RecordCoreException("Cannot set initial delay to greater than maximum delay");
        }
        this.initialDelayMillis = initialDelayMillis;
    }

    /**
     * Open a new record context.
     * @return a new open record context
     * @see FDBDatabase#openContext(Map,FDBStoreTimer,FDBDatabase.WeakReadSemantics)
     */
    @Nonnull
    public FDBRecordContext openContext() {
        if (closed) {
            throw new RunnerClosed();
        }
        FDBRecordContext context = database.openContext(mdcContext, timer, weakReadSemantics);
        addContextToClose(context);
        return context;
    }

    private class RunRetriable<T> {
        private int tries = 0;
        private long currDelay = getInitialDelayMillis();
        @Nullable private FDBRecordContext context;
        @Nullable T retVal = null;
        @Nullable RuntimeException exception = null;
        @Nullable private final List<Object> additionalLogMessageKeyValues;

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        private RunRetriable(@Nullable List<Object> additionalLogMessageKeyValues) {
            this.additionalLogMessageKeyValues = additionalLogMessageKeyValues;
        }

        @Nonnull
        private CompletableFuture<Boolean> handle(@Nullable T val, @Nullable Throwable e) {
            if (context != null) {
                context.close();
                context = null;
            }

            if (closed) {
                // Outermost future should be cancelled, but be sure that this doesn't appear to be successful.
                this.exception = new RunnerClosed();
                return AsyncUtil.READY_FALSE;
            } else if (e == null) {
                // Successful completion. We are done.
                retVal = val;
                return AsyncUtil.READY_FALSE;
            } else {
                // Unsuccessful. Possibly retry.
                Throwable t = e;
                String fdbMessage = null;
                int code = -1;
                boolean retry = false;
                while (t != null) {
                    if (t instanceof FDBException) {
                        FDBException fdbE = (FDBException)t;
                        retry = retry || fdbE.isRetryable();
                        fdbMessage = fdbE.getMessage();
                        code = fdbE.getCode();
                    } else if (t instanceof RecordCoreRetriableTransactionException) {
                        retry = true;
                    }
                    t = t.getCause();
                }

                if (tries + 1 < getMaxAttempts() && retry) {
                    if (LOGGER.isWarnEnabled()) {
                        final KeyValueLogMessage message = KeyValueLogMessage.build("Retrying FDB Exception",
                                                                LogMessageKeys.MESSAGE, fdbMessage,
                                                                LogMessageKeys.CODE, code);
                        if (additionalLogMessageKeyValues != null) {
                            message.addKeysAndValues(additionalLogMessageKeyValues);
                        }
                        LOGGER.warn(message.toString(), e);
                    }
                    long delay = (long)(Math.random() * currDelay);
                    CompletableFuture<Void> future = MoreAsyncUtil.delayedFuture(delay, TimeUnit.MILLISECONDS);
                    addFutureToCompleteExceptionally(future);
                    return future.thenApply(vignore -> {
                        tries += 1;
                        currDelay = Math.max(Math.min(delay * 2, getMaxDelayMillis()), getMinDelayMillis());
                        return true;
                    });
                } else {
                    this.exception = database.mapAsyncToSyncException(e);
                    return AsyncUtil.READY_FALSE;
                }
            }
        }

        @SuppressWarnings("squid:S1181")
        public CompletableFuture<T> runAsync(@Nonnull final Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nonnull final BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction) {
            CompletableFuture<T> future = new CompletableFuture<>();
            addFutureToCompleteExceptionally(future);
            AsyncUtil.whileTrue(() -> {
                try {
                    context = openContext();
                    return retriable.apply(context).thenCompose(val ->
                        context.commitAsync().thenApply( vignore -> val)
                    ).handle((result, ex) -> {
                        Pair<? extends T, ? extends Throwable> newResult = handlePostTransaction.apply(result, ex);
                        return handle(newResult.getLeft(), newResult.getRight());
                    }).thenCompose(Function.identity());
                } catch (Exception e) {
                    return handle(null, e);
                }
            }, getExecutor()).handle((vignore, e) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else if (e != null) {
                    // This handles an uncaught exception
                    // showing up somewhere in the handle function.
                    future.completeExceptionally(e);
                } else {
                    future.complete(retVal);
                }
                return null;
            });
            return future;
        }

        @SuppressWarnings("squid:S1181")
        public T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
            boolean again = true;
            while (again) {
                try {
                    context = openContext();
                    T ret = retriable.apply(context);
                    context.commit();
                    again = database.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_RETRY_DELAY, handle(ret, null));
                } catch (Exception e) {
                    again = database.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_RETRY_DELAY, handle(null, e));
                } finally {
                    if (context != null) {
                        context.close();
                        context = null;
                    }
                }
            }
            if (exception == null) {
                return retVal;
            } else {
                throw exception;
            }
        }
    }

    /**
     * Runs a transactional function against with retry logic.
     * This is a blocking call. See the appropriate overload of {@link #runAsync}
     * for the non-blocking version of this method.
     *
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return result of function after successful run and commit
     * @see #runAsync(Function)
     */
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        return run(retriable, null);
    }

    /**
     * Runs a transactional function against with retry logic.
     * This is a blocking call. See the appropriate overload of {@link #runAsync}
     * for the non-blocking version of this method.
     *
     * @param <T> return type of function to run
     * @param retriable the database operation to run transactionally
     * @param additionalLogMessageKeyValues additional key/value pairs to be included in logs
     * @return result of function after successful run and commit
     * @see #runAsync(Function)
     */
    @API(API.Status.EXPERIMENTAL)
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable,
                     @Nullable List<Object> additionalLogMessageKeyValues) {
        return new RunRetriable<T>(additionalLogMessageKeyValues).run(retriable);
    }

    /**
     * Runs a transactional function asynchronously with retry logic.
     * This is also a non-blocking call. See the appropriate overload of {@link #run}
     * for the blocking version of this method.
     *
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     * @return future that will contain the result of {@code retriable} after successful run and commit
     * @see #run(Function)
     */
    @Nonnull
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        return runAsync(retriable, Pair::of);
    }

    /**
     * Runs a transactional function asynchronously with retry logic.
     * This is also a non-blocking call.
     *
     * @param retriable the database operation to run transactionally
     * @param handlePostTransaction after the transaction is committed, or fails to commit, this function is called with the
     * result or exception respectively. This handler should return a new pair with either the result to return from
     * {@code runAsync} or an exception to be checked whether {@code retriable} should be retried.
     * @param <T> return type of function to run
     * @return future that will contain the result of {@code retriable} after successful run and commit
     * @see #run(Function)
     */
    @Nonnull
    public <T> CompletableFuture<T> runAsync(@Nonnull final Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nonnull final BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction) {
        return runAsync(retriable, handlePostTransaction, null);
    }

    /**
     * Runs a transactional function asynchronously with retry logic.
     * This is also a non-blocking call.
     *
     * @param <T> return type of function to run
     * @param retriable the database operation to run transactionally
     * @param handlePostTransaction after the transaction is committed, or fails to commit, this function is called with the
     * result or exception respectively. This handler should return a new pair with either the result to return from
     * {@code runAsync} or an exception to be checked whether {@code retriable} should be retried.
     * @param additionalLogMessageKeyValues additional key/value pairs to be included in logs
     * @return future that will contain the result of {@code retriable} after successful run and commit
     * @see #run(Function)
     */
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    public <T> CompletableFuture<T> runAsync(@Nonnull final Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nonnull final BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction,
                                             @Nullable List<Object> additionalLogMessageKeyValues) {
        return new RunRetriable<T>(additionalLogMessageKeyValues).runAsync(retriable, handlePostTransaction);
    }

    @Nullable
    public <T> T asyncToSync(FDBStoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        return database.asyncToSync(timer, event, async);
    }

    /**
     * Close this runner.
     *
     * <ul>
     * <li>Disallows starting more transactions</li>
     * <li>Closes already open transactions</li>
     * <li>Cancels futures returned by {@code runAsync}</li>
     * </ul>
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (!futuresToCompleteExceptionally.stream().allMatch(CompletableFuture::isDone)) {
            final Exception exception = new RunnerClosed();
            for (CompletableFuture<?> future : futuresToCompleteExceptionally) {
                future.completeExceptionally(exception);
            }
        }
        contextsToClose.forEach(FDBRecordContext::close);
    }

    private synchronized void addContextToClose(@Nonnull FDBRecordContext context) {
        if (closed) {
            context.close();
            throw new RunnerClosed();
        }
        contextsToClose.removeIf(FDBRecordContext::isClosed);
        contextsToClose.add(context);
    }

    private synchronized void addFutureToCompleteExceptionally(@Nonnull CompletableFuture<?> future) {
        if (closed) {
            final RunnerClosed exception = new RunnerClosed();
            future.completeExceptionally(exception);
            throw exception;
        }
        futuresToCompleteExceptionally.removeIf(CompletableFuture::isDone);
        futuresToCompleteExceptionally.add(future);
    }

    /**
     * Exception thrown when {@link FDBDatabaseRunner} has been closed but tries to do work.
     */
    @SuppressWarnings("serial")
    public static class RunnerClosed extends RecordCoreException {
        public RunnerClosed() {
            super("runner has been closed");
        }
    }
}
