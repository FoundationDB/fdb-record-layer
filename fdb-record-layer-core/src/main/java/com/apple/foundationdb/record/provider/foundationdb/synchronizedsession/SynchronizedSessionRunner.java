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
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunnerInterface;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * It delegates all methods to {@link FDBDatabaseRunner}. For {@code run} and {@code runAsync} methods that take the
 * work {@code retriable}, it injects necessary operations to keep the session synchronized by wrapping the work} with
 * {@link SynchronizedSession#workInSession(Function)} or {@link SynchronizedSession#workInSessionAsync(Function)}.
 */
@API(API.Status.EXPERIMENTAL)
public class SynchronizedSessionRunner implements FDBDatabaseRunnerInterface {

    private FDBDatabaseRunner underlying;
    private SynchronizedSession session;

    /**
     * SynchronizedSessionRunner should only be constructed by {@link SynchronizedSession#initializeSessionAndCreateRunner()}
     * or {@link SynchronizedSession#reuseSessionAndCreateRunner(UUID)} so we can be sure that the {@code session} has
     * a session ID so {@link SynchronizedSession#workInSessionAsync(Function)} can be used.
     * @param underlyingRunner the runner that runs underlying the synchronized session runner
     * @param session a synchronized session with session ID created or assigned
     */
    SynchronizedSessionRunner(FDBDatabaseRunner underlyingRunner, SynchronizedSession session) {
        this.underlying = underlyingRunner;
        this.session = session;
    }

    @Override
    @Nonnull
    public FDBDatabase getDatabase() {
        return underlying.getDatabase();
    }

    @Override
    public Executor getExecutor() {
        return underlying.getExecutor();
    }

    @Override
    @Nullable
    public FDBStoreTimer getTimer() {
        return underlying.getTimer();
    }

    @Override
    public void setTimer(@Nullable FDBStoreTimer timer) {
        underlying.setTimer(timer);
    }

    @Override
    @Nullable
    public Map<String, String> getMdcContext() {
        return underlying.getMdcContext();
    }

    @Override
    public void setMdcContext(@Nullable Map<String, String> mdcContext) {
        underlying.setMdcContext(mdcContext);
    }

    @Override
    @Nullable
    public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        return underlying.getWeakReadSemantics();
    }

    @Override
    public void setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        underlying.setWeakReadSemantics(weakReadSemantics);
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
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        return underlying.run(session.workInSession(retriable));
    }

    @Override
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable, @Nullable List<Object> additionalLogMessageKeyValues) {
        return underlying.run(session.workInSession(retriable), additionalLogMessageKeyValues);
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        return underlying.runAsync(session.workInSessionAsync(retriable));
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable, @Nonnull BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction) {
        return underlying.runAsync(session.workInSessionAsync(retriable), handlePostTransaction);
    }

    @Override
    @Nonnull
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable, @Nonnull BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction, @Nullable List<Object> additionalLogMessageKeyValues) {
        return underlying.runAsync(session.workInSessionAsync(retriable), handlePostTransaction, additionalLogMessageKeyValues);
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
}
