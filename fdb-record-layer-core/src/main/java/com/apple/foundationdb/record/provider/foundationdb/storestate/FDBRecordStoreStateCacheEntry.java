/*
 * FDBRecordStoreStateCacheEntry.java
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

package com.apple.foundationdb.record.provider.foundationdb.storestate;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Information needed to initialize an {@link FDBRecordStore} that is not stored within the record store's
 * {@link com.apple.foundationdb.record.RecordMetaData RecordMetaData}. This is the information that can make it
 * distinct from otherwise homogeneous record stores such as the readability state of its indexes or what format
 * or meta-data version the store was created using.
 *
 * <p>
 * This class is used internally to cache the retrieved results from initializing a record store so that
 * future record stores initialized from the same subspace can be initialized without needing to perform
 * initialization checks multiple times.
 * </p>
 */
@API(API.Status.INTERNAL)
public class FDBRecordStoreStateCacheEntry {
    @Nonnull
    private final SubspaceProvider subspaceProvider;
    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final RecordStoreState recordStoreState;
    @Nullable
    private final byte[] metaDataVersionStamp;

    private FDBRecordStoreStateCacheEntry(@Nonnull SubspaceProvider subspaceProvider,
                                          @Nonnull Subspace subspace,
                                          @Nonnull RecordStoreState recordStoreState,
                                          @Nullable byte[] metaDataVersionStamp) {
        this.subspaceProvider = subspaceProvider;
        this.subspace = subspace;
        this.recordStoreState = recordStoreState;
        this.metaDataVersionStamp = metaDataVersionStamp;
    }

    /**
     * Get the {@link RecordStoreState} contained within this entry.
     *
     * @return the {@link RecordStoreState} contained within this entry
     */
    @Nonnull
    public RecordStoreState getRecordStoreState() {
        return recordStoreState;
    }

    @Nullable
    byte[] getMetaDataVersionStamp() {
        return metaDataVersionStamp;
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Void> handleCachedState(@Nonnull FDBRecordContext context, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        final Transaction tr = context.ensureActive();
        tr.addReadConflictKey(subspace.pack(FDBRecordStoreKeyspace.STORE_INFO.key()));
        return FDBRecordStore.checkStoreHeader(recordStoreState.getStoreHeader(), context, subspaceProvider, subspace, existenceCheck);
    }

    @Nonnull
    static CompletableFuture<FDBRecordStoreStateCacheEntry> load(@Nonnull FDBRecordStore recordStore,
                                                                 @Nonnull FDBRecordStore.StoreExistenceCheck existenceCheck) {
        // This is primarily needed because of https://github.com/apple/foundationdb/issues/11500 where the call to
        // getMetaDataVersionStampAsync might never complete. In the tests we don't set a timeout on the futures, and
        // thus the overall test times out, but in production situations, this should mostly make a difference, because
        // "Batch GRV rate limit exceeded" is clearer than an asyncToSync timeout, on whatever eventual future depends
        // on this.
        final CompletableFuture<byte[]> metaDataVersionStampFuture = recordStore.getContext().getMetaDataVersionStampAsync(IsolationLevel.SNAPSHOT);
        return MoreAsyncUtil.combineAndFailFast(recordStore.loadRecordStoreStateAsync(existenceCheck),
                metaDataVersionStampFuture, (recordStoreState, metaDataVersionStamp) ->
                        new FDBRecordStoreStateCacheEntry(recordStore.getSubspaceProvider(), recordStore.getSubspace(), recordStoreState.toImmutable(), metaDataVersionStamp));
    }
}
