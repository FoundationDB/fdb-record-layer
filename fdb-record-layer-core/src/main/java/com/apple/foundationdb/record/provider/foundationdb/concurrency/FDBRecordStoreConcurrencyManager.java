/*
 * FDBRecordStoreConcurrencyManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.concurrency;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.locking.LockIdentifier;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Default implementation of the {@link StoreConcurrencyManager}. It manages the locks used for
 * read and write operations on behalf of the store.
 */
@API(API.Status.INTERNAL)
public final class FDBRecordStoreConcurrencyManager implements StoreConcurrencyManager {
    @Nonnull
    private static final Object MUTATION_LOCK_KEY = 0L;

    @Nonnull
    private final SubspaceProvider subspaceProvider;
    @Nonnull
    private final FDBRecordContext context;
    @Nullable
    private volatile Subspace cachedStoreSubspace;
    @Nullable
    private volatile LockIdentifier cachedMutationLockId;
    @Nullable
    private volatile Subspace cachedRecordsSubspace;

    public FDBRecordStoreConcurrencyManager(@Nonnull SubspaceProvider subspaceProvider, @Nonnull FDBRecordContext context) {
        this.subspaceProvider = subspaceProvider;
        this.context = context;
    }

    @Nonnull
    private CompletableFuture<Subspace> getStoreSubspaceAsync() {
        Subspace cached = cachedStoreSubspace;
        if (cached == null) {
            // If we don't have a cached subspace, re-create it. We prefer this over using Suppliers::memoize
            // so that subspace resolution throws a transient error, we don't memoize that future.
            // It is possible that there are multiple subspace resolutions happening at the same time, but that's
            // fine as they will all compute equivalent values, so it doesn't matter which one(s) win the race
            // to set the cached subspace
            return subspaceProvider.getSubspaceAsync(context).thenApply(storeSubspace -> {
                cachedStoreSubspace = storeSubspace;
                return cachedStoreSubspace;
            });
        } else {
            return CompletableFuture.completedFuture(cached);
        }
    }

    @Nonnull
    private CompletableFuture<LockIdentifier> getRecordMutationLockIdAsync() {
        LockIdentifier cached = cachedMutationLockId;
        if (cached == null) {
            return getStoreSubspaceAsync().thenApply(storeSubspace -> {
                // This lock ID is not over any explicit key (range) in the store. It doesn't matter that we
                // never actually write to the key; we only need to ensure that all record mutations to
                // the same store pick the same one, and that mutations to different stores pick different ones.
                // The latter is ensured by prefixing the LockId by the store's subspace.
                LockIdentifier mutationLockId = new LockIdentifier(storeSubspace.subspace(Tuple.from(FDBRecordStoreKeyspace.STORE_INFO.key(), MUTATION_LOCK_KEY)));
                cachedMutationLockId = mutationLockId;
                return mutationLockId;
            });
        }
        return CompletableFuture.completedFuture(cached);
    }

    @Nonnull
    private CompletableFuture<Subspace> getRecordsSubspaceAsync() {
        Subspace cached = cachedRecordsSubspace;
        if (cached == null) {
            return getStoreSubspaceAsync().thenApply(storeSubspace -> {
                Subspace recordsSubspace = storeSubspace.subspace(Tuple.from(FDBRecordStoreKeyspace.RECORD.key()));
                cachedRecordsSubspace = recordsSubspace;
                return recordsSubspace;
            });
        }
        return CompletableFuture.completedFuture(cached);
    }

    @Nonnull
    private CompletableFuture<LockIdentifier> lockIdentifierForRecord(@Nonnull Tuple primaryKey) {
        return getRecordsSubspaceAsync()
                .thenApply(recordsSubspace -> recordsSubspace.subspace(primaryKey))
                .thenApply(LockIdentifier::new);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * This implementation will acquire a shared lock covering the record's primary storage. The
     * corresponding {@link #doWithRecordWriteLock(Tuple, Supplier)} operation acquires an exclusive
     * lock over the same subspace, and so this ensures that the read will be queued behind any
     * ongoing writes to the record, and that any writes will be queued behind all outstanding reads.
     * </p>
     */
    @Override
    public <T> CompletableFuture<T> doWithRecordReadLock(@Nonnull final Tuple primaryKey, @Nonnull final Supplier<CompletableFuture<T>> operation) {
        return lockIdentifierForRecord(primaryKey).thenCompose(id ->
                context.doWithReadLock(id, operation));
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * This implementation grabs two exclusive locks:
     * </p>
     *
     * <ol>
     *     <li>
     *         <strong>A mutation lock.</strong> This is a lock that covers all mutations to the store, ensuring that no
     *         two records are mutated concurrently. This is currently necessary because not all index maintainers are
     *         safe to be mutated concurrently (for example, the
     *         {@link com.apple.foundationdb.record.provider.foundationdb.indexes.RankIndexMaintainer RankIndexMaintainer}).
     *         It also prevents a deadlock in the
     *         {@link com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainer SlidingWindowIndexMaintainer}
     *         which is detailed more below. Once we have updated the remaining index maintainers and found a more elegant
     *         solution to the deadlock in the {@code SlidingWindowIndexMaintainer}, we could remove this lock.
     *     </li>
     *     <li>
     *         <strong>A record-scoped lock.</strong> This is a lock that covers a single record. This is the same lock
     *         ID covered by this implementation's {@link #doWithRecordReadLock(Tuple, Supplier)} method, so single-record reads
     *         will be queued behind any previously queued writes, and write operations will not proceed until all
     *         reads have completed.
     *     </li>
     * </ol>
     *
     * <p>
     * Note that the {@link #doWithRecordReadLock(Tuple, Supplier)} method does <em>not</em> grab a shared lock on
     * the mutation lock. The reason for this is that the
     * {@link com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainer SlidingWindowIndexMaintainer}
     * requires being able to read records from the store during a mutation, and as these locks are not re-entrant, blocking
     * reads on all mutations completing would actually create a deadlock. This is still safe for single record operations,
     * as reads themselves will queue up appropriately.
     * </p>
     */
    @Override
    public <T> CompletableFuture<T> doWithRecordWriteLock(@Nonnull final Tuple primaryKey, @Nonnull final Supplier<CompletableFuture<T>> operation) {
        return getRecordMutationLockIdAsync().thenCompose(mutationId ->
                context.doWithWriteLock(mutationId, () ->
                        lockIdentifierForRecord(primaryKey).thenCompose(recordId ->
                                context.doWithWriteLock(recordId, operation))));
    }
}
