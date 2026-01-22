/*
 * MetaDataVersionStampStoreStateCache.java
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

import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.cache.Cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of the {@link FDBRecordStoreStateCache} that uses the database's
 * {@linkplain FDBRecordContext#getMetaDataVersionStampAsync(IsolationLevel) meta-data version-stamp} key as the
 * cache invalidation key. This will only cache the meta-data if it is "safe", that is, if the record store
 * has marked its state as cacheable. This means that one should first set the state cacheability to {@code true}
 * on any record store whose state one would want cached by calling {@link FDBRecordStore#setStateCacheability(boolean)}.
 * Then only those stores will be cached by this store state cache.
 *
 * @see FDBRecordStore#setStateCacheabilityAsync(boolean)
 * @see FDBRecordContext#getMetaDataVersionStamp(IsolationLevel)
 */
public class MetaDataVersionStampStoreStateCache implements FDBRecordStoreStateCache {
    @Nonnull
    private final FDBDatabase database;
    @Nonnull
    private final Cache<SubspaceProvider, FDBRecordStoreStateCacheEntry> cache;

    MetaDataVersionStampStoreStateCache(@Nonnull FDBDatabase database, @Nonnull Cache<SubspaceProvider, FDBRecordStoreStateCacheEntry> cache) {
        this.database = database;
        this.cache = cache;
    }

    @Nonnull
    private FDBRecordStoreStateCacheEntry getNewerEntry(@Nonnull FDBRecordStoreStateCacheEntry entry1, @Nonnull FDBRecordStoreStateCacheEntry entry2) {
        if (entry1.getMetaDataVersionStamp() == null) {
            return entry2;
        } else if (entry2.getMetaDataVersionStamp() == null) {
            return entry1;
        } else {
            return ByteArrayUtil.compareUnsigned(entry1.getMetaDataVersionStamp(), entry2.getMetaDataVersionStamp()) >= 0 ? entry1 : entry2;
        }
    }

    private void addToCache(@Nonnull SubspaceProvider subspaceProvider, @Nonnull FDBRecordStoreStateCacheEntry cacheEntry) {
        cache.asMap().merge(subspaceProvider, cacheEntry, (entry1, entry2) -> {
            final FDBRecordStoreStateCacheEntry newerEntry = getNewerEntry(entry1, entry2);
            if (newerEntry.getRecordStoreState().getStoreHeader().getCacheable()) {
                return newerEntry;
            } else {
                return null;
            }
        });
    }

    private void invalidateOlderEntry(@Nonnull SubspaceProvider subspaceProvider, @Nonnull byte[] metaDataVersionStamp) {
        cache.asMap().computeIfPresent(subspaceProvider, (ignore, existingEntry) -> {
            // Invalidate the key unless the cached meta-data is newer than or matches this meta-data version stamp
            if (existingEntry.getMetaDataVersionStamp() == null || ByteArrayUtil.compareUnsigned(metaDataVersionStamp, existingEntry.getMetaDataVersionStamp()) > 0) {
                return null;
            } else {
                return existingEntry;
            }
        });
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<FDBRecordStoreStateCacheEntry> get(@Nonnull FDBRecordStore recordStore, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck, @Nullable String bypassFullStoreLockReason) {
        final FDBRecordContext context = recordStore.getContext();
        validateContext(context);
        if (context.hasDirtyStoreState()) {
            recordStore.increment(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS);
            return FDBRecordStoreStateCacheEntry.load(recordStore, existenceCheck, bypassFullStoreLockReason);
        }
        final SubspaceProvider subspaceProvider = recordStore.getSubspaceProvider();
        final FDBRecordStoreStateCacheEntry existingEntry = cache.getIfPresent(subspaceProvider);
        if (existingEntry == null) {
            recordStore.increment(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS);
            return FDBRecordStoreStateCacheEntry.load(recordStore, existenceCheck, bypassFullStoreLockReason).whenComplete((cacheEntry, err) -> {
                if (err == null && cacheEntry.getRecordStoreState().getStoreHeader().getCacheable()) {
                    addToCache(subspaceProvider, cacheEntry);
                }
            });
        } else {
            return recordStore.getContext().getMetaDataVersionStampAsync(IsolationLevel.SNAPSHOT).thenCompose(metaDataVersionStamp -> {
                if (metaDataVersionStamp == null || existingEntry.getMetaDataVersionStamp() == null ||
                        ByteArrayUtil.compareUnsigned(metaDataVersionStamp, existingEntry.getMetaDataVersionStamp()) != 0) {
                    recordStore.increment(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS);
                    return FDBRecordStoreStateCacheEntry.load(recordStore, existenceCheck, bypassFullStoreLockReason).whenComplete((cacheEntry, err) -> {
                        if (err == null && metaDataVersionStamp != null) {
                            if (cacheEntry.getRecordStoreState().getStoreHeader().getCacheable()) {
                                addToCache(subspaceProvider, cacheEntry);
                            } else {
                                invalidateOlderEntry(subspaceProvider, metaDataVersionStamp);
                            }
                        }
                    });
                } else {
                    recordStore.increment(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT);
                    return existingEntry.handleCachedState(context, existenceCheck, bypassFullStoreLockReason).thenApply(ignore -> existingEntry);
                }
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
        cache.invalidateAll();
    }
}
