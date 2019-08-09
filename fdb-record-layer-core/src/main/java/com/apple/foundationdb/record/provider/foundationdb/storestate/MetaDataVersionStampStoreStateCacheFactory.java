/*
 * MetaDataVersionStampStoreStateCacheFactory.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * A factory for creating {@link MetaDataVersionStampStoreStateCache}s.
 */
public class MetaDataVersionStampStoreStateCacheFactory implements FDBRecordStoreStateCacheFactory {
    /**
     * A constant indicating that the cache should be of unlimited size or keep items for an unlimited time.
     */
    public static final long UNLIMITED = Long.MAX_VALUE;
    /**
     * The default maximum number of items to include in the cache.
     */
    public static final long DEFAULT_MAX_SIZE = 500;
    /**
     * The default amount of time in milliseconds after last access that cache entries should start to be expired.
     */
    public static final long DEFAULT_EXPIRE_AFTER_ACCESS_MILLIS = TimeUnit.MINUTES.toMillis(1L);

    private long maxSize = DEFAULT_MAX_SIZE;
    private long expireAfterAccessMillis = DEFAULT_EXPIRE_AFTER_ACCESS_MILLIS;

    @Nonnull
    @Override
    public FDBRecordStoreStateCache getCache(@Nonnull FDBDatabase database) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (maxSize != UNLIMITED) {
            cacheBuilder.maximumSize(maxSize);
        }
        if (expireAfterAccessMillis != UNLIMITED) {
            cacheBuilder.expireAfterAccess(expireAfterAccessMillis, TimeUnit.MILLISECONDS);
        }
        Cache<SubspaceProvider, FDBRecordStoreStateCacheEntry> cache = cacheBuilder.build();
        return new MetaDataVersionStampStoreStateCache(database, cache);
    }

    /**
     * Set the number of milliseconds to keep an item in produced caches after it has been accessed.
     * This value can be set to {@link #UNLIMITED} to indicate that the items in caches produced
     * by this factory should not be limited by time.
     *
     * @param expireAfterAccessMillis the amount of time to keep the item in each cache after last access
     * @return this factory
     */
    @Nonnull
    public MetaDataVersionStampStoreStateCacheFactory setExpireAfterAccessMillis(long expireAfterAccessMillis) {
        this.expireAfterAccessMillis = expireAfterAccessMillis;
        return this;
    }

    /**
     * Get the amount of time in milliseconds that each entry is kept in each cache after its last access.
     *
     * @return the amount of time to keep the item in each cache after last access
     */
    public long getDefaultExpireAfterAccessMillis() {
        return expireAfterAccessMillis;
    }

    /**
     * Set the maximum number of elements to keep in produced caches. This value can be set to {@link #UNLIMITED} to
     * indicate that no maximum size should be imposed on the number of items in each cache.
     *
     * @param maxSize the maximum number of elements to keep in each cache
     * @return this factory
     */
    @Nonnull
    public MetaDataVersionStampStoreStateCacheFactory setMaxSize(long maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    /**
     * Get the maximum number of elements to keep in produced caches.
     *
     * @return the maximum number of elements to keep in each cache
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * Create a new factory.
     *
     * @return a new factory of {@link MetaDataVersionStampStoreStateCache}s
     */
    @Nonnull
    public static MetaDataVersionStampStoreStateCacheFactory newInstance() {
        return new MetaDataVersionStampStoreStateCacheFactory();
    }
}
