/*
 * SynchronizedSessionRunner.java
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

package com.apple.foundationdb.record.provider.foundationdb.synchronizedsession;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunnerImpl;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.util.Result;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.synchronizedsession.SynchronizedSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An {@link FDBDatabaseRunner} implementation that performs all work in the context of a
 * {@link SynchronizedSession}.
 * <p>
 * For all variations of {@code run} and {@code runAsync} methods, the work in the {@code retriable} lambda
 * is wrapped by calls that check locks and update leases to ensure that two synchronized sessions are not
 * concurrently running at the same time.
 * </p>
 * @see SynchronizedSession
 */
@API(API.Status.EXPERIMENTAL)
public class SynchronizedSessionRunner implements FDBDatabaseRunner {

    private FDBDatabaseRunnerImpl underlying;
    private SynchronizedSession session;


    /**
     * Produces a new runner, wrapping a given runner, which performs all work in the context of a new
     * {@link SynchronizedSession}.
     * <p>
     * The returned runner will have acquired and started the lease, so care must be taken to ensure that
     * work begins before the lease expiration period.
     * </p>
     * @param lockSubspace the lock for which the session contends
     * @param leaseLengthMill length between last access and lease's end time in milliseconds
     * @param runner the underlying runner
     * @return a future that will return a runner maintaining a new synchronized session
     */
    public static CompletableFuture<SynchronizedSessionRunner> startSessionAsync(@Nonnull Subspace lockSubspace,
                                                                                 long leaseLengthMill,
                                                                                 @Nonnull FDBDatabaseRunnerImpl runner) {
        final UUID newSessionId = UUID.randomUUID();
        SynchronizedSession session = new SynchronizedSession(lockSubspace, newSessionId, leaseLengthMill);
        return runner.runAsync(context -> session.initializeSessionAsync(context.ensureActive()),
                Arrays.asList(
                        LogMessageKeys.TRANSACTION_NAME, "SynchronizedSessionRunner::startSession",
                        LogMessageKeys.SESSION_ID, session.getSessionId(),
                        LogMessageKeys.SUBSPACE, lockSubspace))
                .thenApply(vignore -> new SynchronizedSessionRunner(runner, session));
    }

    /**
     * Synchronous/blocking version of {@link #startSessionAsync(Subspace, long, FDBDatabaseRunnerImpl)}.
     * @param lockSubspace the lock for which the session contends
     * @param leaseLengthMill length between last access and lease's end time in milliseconds
     * @param runner the underlying runner
     * @return a runner maintaining a new synchronized session
     */
    public static SynchronizedSessionRunner startSession(@Nonnull Subspace lockSubspace,
                                                         long leaseLengthMill,
                                                         @Nonnull FDBDatabaseRunnerImpl runner) {
        return runner.asyncToSync(FDBStoreTimer.Waits.WAIT_INIT_SYNC_SESSION,
                startSessionAsync(lockSubspace, leaseLengthMill, runner));
    }


    /**
     * Produces a new runner, wrapping a given runner, which performs all work in the context of an existing
     * {@link SynchronizedSession}.
     * @param lockSubspace the lock for which the session contends
     * @param sessionId session ID
     * @param leaseLengthMill length between last access and lease's end time in milliseconds
     * @param runner the underlying runner
     * @return a runner maintaining a existing synchronized session
     */
    public static SynchronizedSessionRunner joinSession(@Nonnull Subspace lockSubspace,
                                                        @Nonnull UUID sessionId,
                                                        long leaseLengthMill,
                                                        @Nonnull FDBDatabaseRunnerImpl runner) {
        SynchronizedSession session = new SynchronizedSession(lockSubspace, sessionId, leaseLengthMill);
        return new SynchronizedSessionRunner(runner, session);
    }

    private SynchronizedSessionRunner(@Nonnull FDBDatabaseRunnerImpl underlyingRunner,
                                      @Nonnull SynchronizedSession session) {
        this.underlying = underlyingRunner;
        this.session = session;
    }

    // Check and renew the lock when the session in being used.
    // TODO: Maybe the time should be updated even if the work is failed. For example, online indexer may fail
    // in the first a few transactions to find the optimal number of records to scan in one transaction.
    private <T> Function<FDBRecordContext, T> runInSession(
            @Nonnull Function<? super FDBRecordContext, ? extends T> work) {
        return context -> {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_SYNC_SESSION, session.checkLockAsync((context.ensureActive())));
            T result = work.apply(context);
            session.updateLockSessionLeaseEndTime(context.ensureActive());
            return result;
        };
    }

    private <T> Function<? super FDBRecordContext, CompletableFuture<? extends T>> runInSessionAsync(
            @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> work) {
        return context -> session.checkLockAsync(context.ensureActive())
                .thenCompose(vignore -> work.apply(context))
                .thenApply(result -> {
                    session.updateLockSessionLeaseEndTime(context.ensureActive());
                    return result;
                });
    }

    public UUID getSessionId() {
        return session.getSessionId();
    }

    /**
     * Releases the lock to end the synchronized session. It does nothing if the current session does not hold the lock.
     * This should not necessarily be called when closing the runner because there can be multiple runners for the same
     * session.
     * @return a future that will return {@code null} when the session is ended
     */
    public CompletableFuture<Void> endSessionAsync() {
        // Using the underlying runner rather than itself because it should not throw any exception if the session
        // does not hold the lock.
        return underlying.runAsync(context -> session.releaseLock(context.ensureActive()),
                Arrays.asList(LogMessageKeys.TRANSACTION_NAME, "SynchronizedSessionRunner::endSession",
                        LogMessageKeys.SESSION_ID, session.getSessionId()));
    }

    /**
     * Synchronous/blocking version of {@link #endSessionAsync()}.
     */
    public void endSession() {
        underlying.asyncToSync(FDBStoreTimer.Waits.WAIT_END_SYNC_SESSION, endSessionAsync());
    }

    /**
     * Releases the lock to end any synchronized session on the same lock subspace, no matter whether the current
     * session holds the lock or not.
     * @return a future that will return {@code null} when the session is ended
     * @see SynchronizedSession#endAnySession(Transaction, Subspace)
     */
    public CompletableFuture<Void> endAnySessionAsync() {
        return underlying.runAsync(context -> {
            session.endAnySession(context.ensureActive());
            return AsyncUtil.DONE;
        }, Arrays.asList(
                LogMessageKeys.TRANSACTION_NAME, "SynchronizedSessionRunner::endAnySession",
                LogMessageKeys.SESSION_ID, session.getSessionId()));
    }

    /**
     * Synchronous/blocking version of {@link #endAnySessionAsync()}.
     */
    public void endAnySession() {
        underlying.asyncToSync(FDBStoreTimer.Waits.WAIT_END_SYNC_SESSION, endAnySessionAsync());
    }

    @Override
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable,
                     @Nullable List<Object> additionalLogMessageKeyValues) {
        return underlying.run(runInSession(retriable), additionalLogMessageKeyValues);
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nonnull BiFunction<? super T, Throwable, Result<? extends T, ? extends Throwable>> handlePostTransaction,
                                             @Nullable List<Object> additionalLogMessageKeyValues) {
        final List<Object> logDetails;
        if (additionalLogMessageKeyValues == null || additionalLogMessageKeyValues.isEmpty()) {
            logDetails = Arrays.asList(LogMessageKeys.SESSION_ID, session.getSessionId());
        } else {
            logDetails = new ArrayList<>(additionalLogMessageKeyValues);
            logDetails.add(LogMessageKeys.SESSION_ID);
            logDetails.add(session.getSessionId());
        }

        return underlying.runAsync(runInSessionAsync(retriable), handlePostTransaction, logDetails);
    }

    // The other methods below simply delegate to the implementations of the underlying runner, including the three
    // methods that create SynchronizedSessionRunner.

    @Override
    @Nonnull
    public FDBDatabase getDatabase() {
        return underlying.getDatabase();
    }

    @Override
    public FDBRecordContextConfig.Builder getContextConfigBuilder() {
        return underlying.getContextConfigBuilder();
    }

    @Override
    public void setContextConfigBuilder(final FDBRecordContextConfig.Builder contextConfigBuilder) {
        underlying.setContextConfigBuilder(contextConfigBuilder);
    }

    @Override
    public Executor getExecutor() {
        return underlying.getExecutor();
    }

    @Override
    public int getMaxAttempts() {
        return underlying.getMaxAttempts();
    }

    @Override
    public void setMaxAttempts(int maxAttempts) {
        underlying.setMaxAttempts(maxAttempts);
    }

    @Override
    public long getMinDelayMillis() {
        return underlying.getMinDelayMillis();
    }

    @Override
    public long getMaxDelayMillis() {
        return underlying.getMaxDelayMillis();
    }

    @Override
    public void setMaxDelayMillis(long maxDelayMillis) {
        underlying.setMaxDelayMillis(maxDelayMillis);
    }

    @Override
    public long getInitialDelayMillis() {
        return underlying.getInitialDelayMillis();
    }

    @Override
    public void setInitialDelayMillis(long initialDelayMillis) {
        underlying.setInitialDelayMillis(initialDelayMillis);
    }

    @Override
    @Nonnull
    public FDBRecordContext openContext() {
        return underlying.openContext();
    }

    @Override
    @Nullable
    public <T> T asyncToSync(StoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        return underlying.asyncToSync(event, async);
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Override
    public CompletableFuture<SynchronizedSessionRunner> startSynchronizedSessionAsync(@Nonnull Subspace lockSubspace, long leaseLengthMillis) {
        return underlying.startSynchronizedSessionAsync(lockSubspace, leaseLengthMillis);
    }

    @Override
    public SynchronizedSessionRunner startSynchronizedSession(@Nonnull Subspace lockSubspace, long leaseLengthMillis) {
        return underlying.startSynchronizedSession(lockSubspace, leaseLengthMillis);
    }

    @Override
    public SynchronizedSessionRunner joinSynchronizedSession(@Nonnull Subspace lockSubspace, @Nonnull UUID sessionId, long leaseLengthMillis) {
        return underlying.joinSynchronizedSession(lockSubspace, sessionId, leaseLengthMillis);
    }


}
