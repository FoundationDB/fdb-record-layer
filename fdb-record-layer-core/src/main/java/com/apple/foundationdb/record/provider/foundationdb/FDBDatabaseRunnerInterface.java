/*
 * FDBDatabaseRunnerInterface.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
public interface FDBDatabaseRunnerInterface extends AutoCloseable {
    /**
     * Get the database against which functions are run.
     * @return the database used to run
     */
    @Nonnull
    FDBDatabase getDatabase();

    /**
     * Get the executor that will be used for {@link #runAsync}.
     * @return the executor to use
     */
    Executor getExecutor();

    /**
     * Get the timer used in record contexts opened by this runner.
     * @return timer to use
     */
    @Nullable
    FDBStoreTimer getTimer();

    /**
     * Set the timer used in record contexts opened by this runner.
     * @param timer timer to use
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    void setTimer(@Nullable FDBStoreTimer timer);

    /**
     * Get the logging context used in record contexts opened by this runner.
     * @return the logging context to use
     */
    @Nullable
    Map<String, String> getMdcContext();

    /**
     * Set the logging context used in record contexts opened by this runner.
     * This will change the executor to be one that restores this new {@code mdcContext}.
     * @param mdcContext the logging context to use
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    void setMdcContext(@Nullable Map<String, String> mdcContext);

    /**
     * Get the read semantics used in record contexts opened by this runner.
     * @return allowable staleness parameters if caching read versions
     */
    @Nullable
    FDBDatabase.WeakReadSemantics getWeakReadSemantics();

    /**
     * Set the read semantics used in record contexts opened by this runner.
     * @param weakReadSemantics allowable staleness parameters if caching read versions
     * @see FDBDatabase#openContext(Map,FDBStoreTimer,FDBDatabase.WeakReadSemantics)
     */
    void setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics);

    /**
     * Gets the maximum number of attempts for a database to make when running a
     * retriable transactional operation. This is used by {@link #run} and {@link #runAsync} to limit the number of
     * attempts that an operation is retried.
     * @return the maximum number of times to run a transactional database operation
     */
    int getMaxAttempts();

    /**
     * Sets the maximum number of attempts for a database to make when running a
     * retriable transactional operation. This is used by {@link #run} and {@link #runAsync} to limit the number of
     * attempts that an operation is retried.
     * @param maxAttempts the maximum number of times to run a transactional database operation
     * @throws IllegalArgumentException if a non-positive number is given
     */
    void setMaxAttempts(int maxAttempts);

    /**
     * Gets the minimum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to limit the time spent
     * between successive attempts at completing a database operation.
     * Currently this value is fixed at 2 milliseconds and is not settable.
     * @return the minimum delay between attempts when retrying operations
     */
    long getMinDelayMillis();

    /**
     * Gets the maximum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to limit the time spent
     * between successive attempts at completing a database operation. The default value is 1000 so that
     * there will not be more than 1 second between attempts.
     * @return the maximum delay between attempts when retrying operations
     */
    long getMaxDelayMillis();

    /**
     * Sets the maximum delay (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to limit the time spent
     * between successive attempts at completing a database operation. The default value is 1000 so that
     * there will not be more than 1 second between attempts.
     * @param maxDelayMillis the maximum delay between attempts when retrying operations
     * @throws IllegalArgumentException if the value is negative or less than the minimum delay
     */
    void setMaxDelayMillis(long maxDelayMillis);

    /**
     * Gets the delay ceiling (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initial delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     * @return the delay ceiling between the first and second attempts at running a database operation
     */
    long getInitialDelayMillis();

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
    void setInitialDelayMillis(long initialDelayMillis);

    /**
     * Open a new record context.
     * @return a new open record context
     * @see FDBDatabase#openContext(Map,FDBStoreTimer,FDBDatabase.WeakReadSemantics)
     */
    @Nonnull
    FDBRecordContext openContext();

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
    <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable);

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
    <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable,
              @Nullable List<Object> additionalLogMessageKeyValues);

    /**
     * Runs a transactional function asynchronously with retry logic.
     * This is also a non-blocking call. See the appropriate overload of {@link #run}
     * for the blocking version of this method.
     *
     * @param <T> return type of function to run
     * @param retriable the database operation to run transactionally
     * @return future that will contain the result of {@code retriable} after successful run and commit
     * @see #run(Function)
     */
    @Nonnull
    <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable);

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
    <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                      @Nonnull BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction);

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
    <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                      @Nonnull BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction,
                                      @Nullable List<Object> additionalLogMessageKeyValues);

    @Nullable
    <T> T asyncToSync(FDBStoreTimer.Wait event, @Nonnull CompletableFuture<T> async);

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
    void close();

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
