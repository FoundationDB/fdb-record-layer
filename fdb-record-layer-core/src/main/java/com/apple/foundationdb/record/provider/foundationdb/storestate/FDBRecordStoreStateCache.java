/*
 * FDBRecordStoreStateCache.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A cache for {@link FDBRecordStoreStateCacheEntry} objects. These cache entries contain information necessary
 * to initialize a record store, so caching this information can make accessing a record store multiple times
 * in quick succession cheaper both in that latency is saved at record store creation time. Additionally, in scenarios
 * when a client is connecting to a very small number of record stores, loading this information from disk can
 * load potential hot keys which can generally degrade cluster performance. Caching this information avoids performing
 * those reads.
 *
 * <p>
 * Note that if the client is connecting to multiple clusters, there should be one cache per cluster. In general, as
 * there is one {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase FDBDatabase} object per cluster,
 * it is sufficient to provide a {@link FDBRecordStoreStateCacheFactory} to the
 * {@link FDBDatabaseFactory FDBDatabaseFactory}
 * singleton. One can also provide a cache instance to an {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore FDBRecordStore}
 * at build time.
 * </p>
 *
 * <p>
 * Clients should generally not call methods defined on this interface, as they are intended mainly for internal use
 * within the Record Layer.
 * </p>
 *
 * @see FDBDatabaseFactory#setStoreStateCacheFactory(FDBRecordStoreStateCacheFactory)
 * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore.BaseBuilder#setStoreStateCache(FDBRecordStoreStateCache)
 */
@API(API.Status.EXPERIMENTAL)
public interface FDBRecordStoreStateCache {

    /**
     * Load the record store state for the record store stored at the given location. If this information is available
     * in the cache, it will be served from memory without communicating with the database. Otherwise, the information
     * may require performing network and disk I/O. Note that the cache entry returned should be consistent with the
     * view of the database as viewed by the transaction provided as input. The cache implementations are generally
     * differentiated by the manner in which they invalidate data to ensure that the value returned is consistent with
     * this contract.
     *
     * <p>
     * This method should generally not be called by clients importing the Record Layer.
     * </p>
     *
     * @param recordStore the record store to load the store state of
     * @param existenceCheck whether to error if the store does or does not exist
     *
     * @return a future that will complete with the cached store state
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    CompletableFuture<FDBRecordStoreStateCacheEntry> get(@Nonnull FDBRecordStore recordStore,
                                                         @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck);

    @API(API.Status.INTERNAL)
    void validateDatabase(@Nonnull FDBDatabase database);

    @API(API.Status.INTERNAL)
    default void validateContext(@Nonnull FDBRecordContext context) {
        validateDatabase(context.getDatabase());
    }

    /**
     * Remove all entries from the cache.
     */
    void clear();
}
