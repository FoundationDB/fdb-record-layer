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
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Default implementation of the {@link StoreConcurrencyManager}. It manages the locks used for
 * read and write operations on behalf of the store.
 */
@API(API.Status.INTERNAL)
public final class FDBRecordStoreConcurrencyManager implements StoreConcurrencyManager {
    @Nonnull
    private final SubspaceProvider subspaceProvider;
    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final Supplier<CompletableFuture<Subspace>> recordSubspaceFutureSupplier = Suppliers.memoize(this::computeRecordsSubspaceAsync);

    public FDBRecordStoreConcurrencyManager(@Nonnull SubspaceProvider subspaceProvider, @Nonnull FDBRecordContext context) {
        this.subspaceProvider = subspaceProvider;
        this.context = context;
    }

    @Nonnull
    private CompletableFuture<Subspace> computeRecordsSubspaceAsync() {
        return subspaceProvider.getSubspaceAsync(context)
                .thenApply(baseSubspace -> baseSubspace.subspace(Tuple.from(FDBRecordStoreKeyspace.RECORD.key())));
    }

    @Nonnull
    private CompletableFuture<LockIdentifier> lockIdentifierForRecord(@Nonnull Tuple primaryKey) {
        return recordSubspaceFutureSupplier.get()
                .thenApply(recordsSubspace -> recordsSubspace.subspace(primaryKey))
                .thenApply(LockIdentifier::new);
    }

    @Override
    public <T> CompletableFuture<T> doWithRecordReadLock(@Nonnull final Tuple primaryKey, @Nonnull final Supplier<CompletableFuture<T>> operation) {
        return lockIdentifierForRecord(primaryKey).thenCompose(id -> context.doWithReadLock(id, operation));
    }

    @Override
    public <T> CompletableFuture<T> doWithRecordWriteLock(@Nonnull final Tuple primaryKey, @Nonnull final Supplier<CompletableFuture<T>> operation) {
        return lockIdentifierForRecord(primaryKey).thenCompose(id -> context.doWithWriteLock(id, operation));
    }
}
