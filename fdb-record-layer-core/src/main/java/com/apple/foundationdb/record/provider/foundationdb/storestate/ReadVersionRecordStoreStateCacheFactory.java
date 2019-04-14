/*
 * ReadVersionRecordStoreInfoCacheFactory.java
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

import javax.annotation.Nonnull;

/**
 * A factory for creating {@link ReadVersionRecordStoreStateCache}s.
 */
@API(API.Status.EXPERIMENTAL)
public class ReadVersionRecordStoreStateCacheFactory implements FDBRecordStoreStateCacheFactory {
    private long refreshTimeMillis;
    private long deadlineTimeMillis;
    private long maxSize;

    private ReadVersionRecordStoreStateCacheFactory() {
        this.refreshTimeMillis = 5000;
        this.deadlineTimeMillis = 5000;
        this.maxSize = 1000;
    }

    /**
     * Set the maximum amount of time to keep an entry in the cache.
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
