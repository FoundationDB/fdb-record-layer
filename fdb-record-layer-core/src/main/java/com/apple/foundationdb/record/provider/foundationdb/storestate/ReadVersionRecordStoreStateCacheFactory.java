/*
 * ReadVersionRecordStoreStateCacheFactory.java
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
import com.apple.foundationdb.record.AsyncLoadingCache;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;

import javax.annotation.Nonnull;

/**
 * A factory for creating {@link ReadVersionRecordStoreStateCache}s.
 */
@API(API.Status.EXPERIMENTAL)
public class ReadVersionRecordStoreStateCacheFactory implements FDBRecordStoreStateCacheFactory {
    /**
     * Default refresh time for cache entries (in milliseconds). By default, this is set to 5 seconds, which
     * is equal to the FoundationDB
     * <a href="https://apple.github.io/foundationdb/known-limitations.html#long-running-transactions">global transaction time limit</a>.
     */
    public static final long DEFAULT_REFRESH_TIME_MILLIS = 5000L;
    /**
     * Default deadline time to load entries into this cache. This is set to the same default
     * as the {@link AsyncLoadingCache}'s default deadline time.
     *
     * @see AsyncLoadingCache#DEFAULT_DEADLINE_TIME_MILLIS
     */
    public static final long DEFAULT_DEADLINE_TIME_MILLIS = AsyncLoadingCache.DEFAULT_DEADLINE_TIME_MILLIS;
    /**
     * Default maximum number of elements to cache.
     */
    public static final long DEFAULT_MAX_SIZE = 1000L;

    private long refreshTimeMillis = DEFAULT_REFRESH_TIME_MILLIS;
    private long deadlineTimeMillis = DEFAULT_DEADLINE_TIME_MILLIS;
    private long maxSize = DEFAULT_MAX_SIZE;

    private ReadVersionRecordStoreStateCacheFactory() {
    }

    /**
     * Set the maximum amount of time to keep an entry in the cache. Note that as read versions are only
     * valid for a limited time in FoundationDB, it makes little sense to set this value to something
     * above the <a href="https://apple.github.io/foundationdb/known-limitations.html#long-running-transactions">global transaction time limit</a>.
     *
     * @param refreshTimeMillis the maximum amount of time to keep an entry in milliseconds
     * @return this factory
     */
    @Nonnull
    public ReadVersionRecordStoreStateCacheFactory setRefreshTimeMillis(long refreshTimeMillis) {
        this.refreshTimeMillis = refreshTimeMillis;
        return this;
    }

    /**
     * Set the maximum amount of time to wait for an entry to be loaded.
     *
     * @param deadlineTimeMillis the maximum amount of to wait for an entry to be loaded in milliseconds
     * @return this factory
     */
    @Nonnull
    public ReadVersionRecordStoreStateCacheFactory setDeadlineTimeMillis(long deadlineTimeMillis) {
        this.deadlineTimeMillis = deadlineTimeMillis;
        return this;
    }

    /**
     * Set the maximum number of entries to cache.
     *
     * @param maxSize the maximum number of elements to store in the cache
     * @return this factory
     */
    @Nonnull
    public ReadVersionRecordStoreStateCacheFactory setMaxSize(long maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    /**
     * Create a new {@link ReadVersionRecordStoreStateCache}. It will inherit the refresh time, the
     * deadline time, and the maximum size set by this factory.
     *
     * @return a new {@link ReadVersionRecordStoreStateCache}
     */
    @Nonnull
    @Override
    public ReadVersionRecordStoreStateCache getCache(@Nonnull FDBDatabase database) {
        return new ReadVersionRecordStoreStateCache(database, refreshTimeMillis, deadlineTimeMillis, maxSize);
    }

    /**
     * Create a new factory.
     *
     * @return a new factory of {@link ReadVersionRecordStoreStateCache}s
     */
    @Nonnull
    public static ReadVersionRecordStoreStateCacheFactory newInstance() {
        return new ReadVersionRecordStoreStateCacheFactory();
    }
}
