/*
 * ReadVersionRecordStoreStateCache.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.AsyncLoadingCache;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;
import com.apple.foundationdb.record.util.pair.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of the {@link FDBRecordStoreStateCache} that uses the read-version of the associated transaction
 * as the cache invalidation key. In general, it is not particularly likely that multiple transactions started
 * against the same database will share the same read version, so without any other work, this cache should be
 * expected to have a rather low hit-rate. However, when combined with version-caching and setting the
 * {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics weak read semantics}
 * of a context to use cached read versions, one can greatly increase the cache hit rate. This means that in cases where stale
 * reads are acceptable and many transactions share the same read version, opening the same record store multiple times
 * with the same read version (even if the transactions are different) can be made very cheap.
 *
 * @see com.apple.foundationdb.record.provider.foundationdb.FDBDatabase#setTrackLastSeenVersion(boolean)
 * @see com.apple.foundationdb.record.provider.foundationdb.FDBDatabase.WeakReadSemantics
 */
@API(API.Status.EXPERIMENTAL)
public class ReadVersionRecordStoreStateCache implements FDBRecordStoreStateCache {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadVersionRecordStoreStateCache.class);
    @Nonnull
    private final AsyncLoadingCache<Pair<SubspaceProvider, Long>, FDBRecordStoreStateCacheEntry> cache;
    @Nonnull
    private final FDBDatabase database;

    ReadVersionRecordStoreStateCache(@Nonnull FDBDatabase database, long refreshTimeMillis, long deadlineTimeMillis, long maxSize) {
        this.database = database;
        this.cache = new AsyncLoadingCache<>(refreshTimeMillis, deadlineTimeMillis, maxSize, database.getScheduledExecutor());
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<FDBRecordStoreStateCacheEntry> get(@Nonnull FDBRecordStore recordStore,
                                                                @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        final FDBRecordContext context = recordStore.getContext();
        validateContext(context);
        if (context.hasDirtyStoreState()) {
            // If a store info header has been modified during the course of this transaction's run,
            // don't go to the cache as its results may be stale.
            context.increment(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS);
            return FDBRecordStoreStateCacheEntry.load(recordStore, existenceCheck);
        } else {
            return context.getReadVersionAsync().thenCompose(readVersion -> {
                final SubspaceProvider subspaceProvider = recordStore.getSubspaceProvider();
                CompletableFuture<FDBRecordStoreStateCacheEntry> storeStateFuture = cache.orElseGet(Pair.of(subspaceProvider, readVersion), () -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(KeyValueLogMessage.of("store state cache miss",
                                subspaceProvider.logKey(), subspaceProvider.toString(context),
                                LogMessageKeys.READ_VERSION, readVersion));
                    }
                    context.increment(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS);
                    return FDBRecordStoreStateCacheEntry.load(recordStore, existenceCheck);
                }).thenCompose(storeState -> storeState.handleCachedState(context, existenceCheck).thenApply(vignore -> storeState));

                if (context.getTimer() != null && MoreAsyncUtil.isCompletedNormally(storeStateFuture)) {
                    context.increment(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT);
                }
                return storeStateFuture;
            });
        }
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public void validateDatabase(@Nonnull FDBDatabase database) {
        if (database != this.database) {
            throw new RecordCoreArgumentException("record store state cache used with different database than the one it was initialized with");
        }
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
