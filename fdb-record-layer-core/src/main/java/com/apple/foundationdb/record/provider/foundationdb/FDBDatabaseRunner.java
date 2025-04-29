/*
 * FDBDatabaseRunner.java
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
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.provider.foundationdb.runners.ExponentialDelay;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.record.util.Result;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.synchronizedsession.SynchronizedSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A context for running against an {@link FDBDatabase} with retrying of transient exceptions.
 *
 * <p>
 * Implements {@link #run} and {@link #runAsync} methods for executing functions in a new {@link FDBRecordContext} and returning their result.
 * </p>
 *
 * <p>
 * Implements {@link #close} in such a way that {@code runAsync} will not accidentally do more work afterwards. In particular, in the case
 * of timeouts and transaction retries, it is otherwise possible for a whole new transaction to start at some time in the future and change
 * the database even after the caller has given up, due to a timeout, for example.
 * </p>
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
@API(API.Status.UNSTABLE)
public interface FDBDatabaseRunner extends AutoCloseable {
    /**
     * Get the database against which functions are run.
     * @return the database used to run
     */
    @Nonnull
    FDBDatabase getDatabase();

    /**
     * Get the configuration used in record contexts opened by this runner.
     * Any changes made to the returned builder will be reflected in contexts opened by this runner.
     * @return configuration to use
     */
    @Nonnull
    FDBRecordContextConfig.Builder getContextConfigBuilder();

    /**
     * Set the configuration used in record contexts opened by this runner.
     * @param contextConfigBuilder configuration to use
     */
    void setContextConfigBuilder(FDBRecordContextConfig.Builder contextConfigBuilder);

    /**
     * Get the executor that will be used for {@link #runAsync}.
     * @return the executor to use
     */
    Executor getExecutor();

    /**
     * Get the scheduled executor to use to schedule return delays.
     *
     * @return the scheduled executor for this runner to use
     */
    default ScheduledExecutorService getScheduledExecutor() {
        return getDatabase().getScheduledExecutor();
    }

    /**
     * Get the timer used in record contexts opened by this runner.
     * @return timer to use
     */
    @Nullable
    default FDBStoreTimer getTimer() {
        return getContextConfigBuilder().getTimer();
    }

    /**
     * Set the timer used in record contexts opened by this runner.
     * @param timer timer to use
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    default void setTimer(@Nullable FDBStoreTimer timer) {
        getContextConfigBuilder().setTimer(timer);
    }

    /**
     * Get the logging context used in record contexts opened by this runner.
     * @return the logging context to use
     */
    @Nullable
    default Map<String, String> getMdcContext() {
        return getContextConfigBuilder().getMdcContext();
    }

    /**
     * Set the logging context used in record contexts opened by this runner.
     * This will change the executor to be one that restores this new {@code mdcContext}.
     * @param mdcContext the logging context to use
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    default void setMdcContext(@Nullable Map<String, String> mdcContext) {
        getContextConfigBuilder().setMdcContext(mdcContext);
    }

    /**
     * Get the read semantics used in record contexts opened by this runner.
     * @return allowable staleness parameters if caching read versions
     */
    @Nullable
    default FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        return getContextConfigBuilder().getWeakReadSemantics();
    }

    /**
     * Set the read semantics used in record contexts opened by this runner. Within the retry loops
     * {@link #run(Function)}, {@link #runAsync(Function)}, and their variants, only the first
     * context (from the first iteration round the loop) actually sets the created record context's
     * {@link FDBDatabase.WeakReadSemantics} to the provided value. This is because there are several
     * reasons why a transaction may fail due to a stale read version (for example, a
     * {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBExceptions.FDBStoreTransactionConflictException conflict}
     * or {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBExceptions.FDBStoreTransactionIsTooOldException expired transaction}),
     * and subsequent attempts will fail if their read version is not updated to a newer value.
     *
     * @param weakReadSemantics allowable staleness parameters if caching read versions
     * @see FDBDatabase#openContext(Map,FDBStoreTimer,FDBDatabase.WeakReadSemantics)
     */
    default void setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        getContextConfigBuilder().setWeakReadSemantics(weakReadSemantics);
    }

    /**
     * Get the priority of transactions opened by this runner.
     * @return the priority of transactions opened by this runner
     * @see FDBRecordContext#getPriority()
     */
    @Nonnull
    default FDBTransactionPriority getPriority() {
        return getContextConfigBuilder().getPriority();
    }

    /**
     * Set the priority of transactions opened by this runner.
     * @param priority the priority of transactions by this runner
     * @see FDBRecordContext#getPriority()
     */
    default void setPriority(@Nonnull FDBTransactionPriority priority) {
        getContextConfigBuilder().setPriority(priority);
    }

    /**
     * Get the transaction timeout for all transactions started by this runner. This will return the value configured
     * for this runner through {@link #setTransactionTimeoutMillis(long)}. Note, however, that if the transaction timeout
     * is set to {@link FDBDatabaseFactory#DEFAULT_TR_TIMEOUT_MILLIS}, then the actual timeout set for this transaction
     * will be set to the value in the originating factory.
     *
     * @return the configured transacation timeout time in milliseconds
     * @see #setTransactionTimeoutMillis(long)
     */
    default long getTransactionTimeoutMillis() {
        return getContextConfigBuilder().getTransactionTimeoutMillis();
    }

    /**
     * Set the transaction timeout for all transactions started by this runner. If set to {@link FDBDatabaseFactory#DEFAULT_TR_TIMEOUT_MILLIS},
     * then this will use the value as set in the originating database's factory. If set to {@link FDBDatabaseFactory#UNLIMITED_TR_TIMEOUT_MILLIS},
     * then no timeout will be imposed on transactions used by this runner.
     *
     * <p>
     * Note that the error that the transaction hits, {@link FDBExceptions.FDBStoreTransactionTimeoutException},
     * is not retriable, so if the runner encounters such an error, it will terminate.
     * </p>
     *
     * @param transactionTimeoutMillis the transaction timeout time in milliseconds
     * @see FDBDatabaseFactory#setTransactionTimeoutMillis(long)
     */
    default void setTransactionTimeoutMillis(long transactionTimeoutMillis) {
        getContextConfigBuilder().setTransactionTimeoutMillis(transactionTimeoutMillis);
    }

    /**
     * Set the properties configured by adopter of Record-Layer for transactions opened by this runner.
     * @param propertyStorage the storage of properties
     * @see FDBRecordContext#getPropertyStorage()
     */
    default void setRecordLayerPropertyStorage(@Nonnull RecordLayerPropertyStorage propertyStorage) {
        getContextConfigBuilder().setRecordContextProperties(propertyStorage);
    }

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
     * and the initials delay, and will use that value each successive iteration to determine
     * how long that wait should be. The default value is 10 milliseconds.
     * @return the delay ceiling between the first and second attempts at running a database operation
     */
    long getInitialDelayMillis();

    /**
     * Sets the delay ceiling (in milliseconds) that will be applied between attempts to
     * run a transactional database operation. This is used within {@link #run} and {@link #runAsync} to determine how
     * long to wait between the first and second attempts at running a database operation.
     * The exponential backoff algorithm will choose an amount of time to wait between zero
     * and the initials delay, and will use that value each successive iteration to determine
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
     * Construct an {@link ExponentialDelay} based on this runner's configuration.
     *
     * @return an {@link ExponentialDelay} that can be used to inject retry delay
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    default ExponentialDelay createExponentialDelay() {
        return new ExponentialDelay(getInitialDelayMillis(), getMaxDelayMillis(), getDatabase().getScheduledExecutor());
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
    default <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
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
    default <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        return runAsync(retriable, Result::of);
    }

    /**
     * Runs a transactional function asynchronously with retry logic.
     * This is also a non-blocking call. See the appropriate overload of {@link #run}
     * for the blocking version of this method.
     *
     * @param <T> return type of function to run
     * @param retriable the database operation to run transactionally
     * @param additionalLogMessageKeyValues additional key/value pairs to be included in logs
     * @return future that will contain the result of {@code retriable} after successful run and commit
     * @see #run(Function)
     */
    @Nonnull
    default <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                              @Nullable List<Object> additionalLogMessageKeyValues) {
        return runAsync(retriable, Result::of, additionalLogMessageKeyValues);
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
    default <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                              @Nonnull BiFunction<? super T, Throwable, Result<? extends T, ? extends Throwable>> handlePostTransaction) {
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
    <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                      @Nonnull BiFunction<? super T, Throwable, Result<? extends T, ? extends Throwable>> handlePostTransaction,
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
     * Produces a new runner, wrapping this runner, which performs all work in the context of a new
     * {@link SynchronizedSession}.
     * <p>
     * The returned runner will have acquired and started the lease, so care must be taken to ensure that
     * work begins before the lease expiration period.
     * </p>
     * @param lockSubspace the lock for which the session contends
     * @param leaseLengthMillis length between last access and lease's end time in milliseconds
     * @return a future that will return a runner maintaining a new synchronized session
     * @see SynchronizedSession
     */
    @API(API.Status.EXPERIMENTAL)
    CompletableFuture<SynchronizedSessionRunner> startSynchronizedSessionAsync(@Nonnull Subspace lockSubspace, long leaseLengthMillis);

    /**
     * Synchronous/blocking version of {@link #startSynchronizedSessionAsync(Subspace, long)}.
     * @param lockSubspace the lock for which the session contends
     * @param leaseLengthMillis length between last access and lease's end time in milliseconds
     * @return a runner maintaining a new synchronized session
     */
    @API(API.Status.EXPERIMENTAL)
    SynchronizedSessionRunner startSynchronizedSession(@Nonnull Subspace lockSubspace, long leaseLengthMillis);

    /**
     * Produces a new runner, wrapping this runner, which performs all work in the context of an existing
     * {@link SynchronizedSession}.
     * @param lockSubspace the lock for which the session contends
     * @param sessionId session ID
     * @param leaseLengthMillis length between last access and lease's end time in milliseconds
     * @return a runner maintaining a existing synchronized session
     * @see SynchronizedSession
     */
    @API(API.Status.EXPERIMENTAL)
    SynchronizedSessionRunner joinSynchronizedSession(@Nonnull Subspace lockSubspace, @Nonnull UUID sessionId, long leaseLengthMillis);

    /**
     * Exception thrown when {@link FDBDatabaseRunner} has been closed but tries to do work.
     */
    @SuppressWarnings("serial")
    class RunnerClosed extends RecordCoreException {
        public RunnerClosed() {
            super("runner has been closed");
        }
    }
}
