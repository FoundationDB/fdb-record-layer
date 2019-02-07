/*
 * FDBStoreBase.java
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

import com.apple.foundationdb.API;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base class for record stores and meta-data stores, which have in common that they are opened by an {@link FDBRecordContext} and occupy
 * some {@link Subspace} in the database.
 */
@API(API.Status.STABLE)
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class FDBStoreBase {
    @Nonnull
    protected final FDBRecordContext context;
    @Nonnull
    protected final SubspaceProvider subspaceProvider;

    // It is recommended to use {@link #FDBStoreBase(FDBRecordContext, SubspaceProvider)} instead.
    @API(API.Status.UNSTABLE)
    protected FDBStoreBase(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace) {
        this.context = context;
        this.subspaceProvider = new SubspaceProviderBySubspace(subspace);
    }

    protected FDBStoreBase(@Nonnull FDBRecordContext context, @Nonnull SubspaceProvider subspaceProvider) {
        this.context = context;
        this.subspaceProvider = subspaceProvider;
    }

    @Nonnull
    public Executor getExecutor() {
        return context.getExecutor();
    }

    /**
     * Get the {@link FDBRecordContext} within which this store operates.
     * @return the associated context
     */
    @Nonnull
    public FDBRecordContext getRecordContext() {
        return context;
    }

    @Nonnull
    public Transaction ensureContextActive() {
        return context.ensureActive();
    }

    @Nonnull
    public SubspaceProvider getSubspaceProvider() {
        return subspaceProvider;
    }

    @Nonnull
    public Subspace getSubspace() {
        return subspaceProvider.getSubspace(context);
    }

    @Nonnull
    public CompletableFuture<Subspace> getSubspaceAsync() {
        return subspaceProvider.getSubspaceAsync(context);
    }

    public void addConflictForSubspace(boolean write) {
        final Range range = getSubspace().range();
        final Transaction tr = context.ensureActive();
        if (write) {
            tr.addWriteConflictRange(range.begin, range.end);
        } else {
            tr.addReadConflictRange(range.begin, range.end);
        }
    }

    @Nullable
    public FDBStoreTimer getTimer() {
        return context.getTimer();
    }

    public <T> CompletableFuture<T> instrument(StoreTimer.Event event, CompletableFuture<T> future) {
        return context.instrument(event, future);
    }

    public <T> CompletableFuture<T> instrument(Set<StoreTimer.Event> events, CompletableFuture<T> future) {
        return context.instrument(events, future);
    }

    public <T> CompletableFuture<T> instrument(StoreTimer.Event event, CompletableFuture<T> future, long startTime) {
        return context.instrument(event, future, startTime);
    }

    /**
     * Deprecated. Users should use {@link #increment(StoreTimer.Count)} instead.
     *
     * @param count the event being recorded
     * @deprecated use {@link #increment(StoreTimer.Count)} instead
     */
    @Deprecated
    public void record(@Nonnull StoreTimer.Count count) {
        context.record(count);
    }

    /**
     * Record the amount of time an event took to run.
     *
     * @param event the event being recorded
     * @param timeDelta the time the event took to complete
     * @see StoreTimer#record(StoreTimer.Event, long) StoreTimer.record()
     */
    public void record(@Nonnull StoreTimer.Event event, long timeDelta) {
        context.record(event, timeDelta);
    }

    /**
     * Record that an event occurred one time.
     *
     * @param count the event being recorded
     * @see StoreTimer#increment(StoreTimer.Count) StoreTimer.increment()
     */
    public void increment(@Nonnull StoreTimer.Count count) {
        context.increment(count);
    }

    /**
     * Record that an event occurred one or more times.
     *
     * @param count the event being recorded
     * @param amount the number of times the event occurred
     * @see StoreTimer#increment(StoreTimer.Count, int) StoreTimer.increment()
     */
    public void increment(@Nonnull StoreTimer.Count count, int amount) {
        context.increment(count, amount);
    }
}
