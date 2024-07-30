/*
 * PassThroughRecordStoreInfoCache.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of the {@link FDBRecordStoreStateCache} interface that does not actually cache anything. This should
 * be used if caching the record store state is not desired. For example, this might be the case if the cache miss rate
 * is high and therefore keeping the machinery around to maintain the cache is not worth the cost. This is also the
 * default implementation.
 *
 * <p>
 * Note that this class is a singleton as it requires no state.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class PassThroughRecordStoreStateCache implements FDBRecordStoreStateCache {
    @Nonnull
    private static final PassThroughRecordStoreStateCache INSTANCE = new PassThroughRecordStoreStateCache();

    private PassThroughRecordStoreStateCache() {
    }

    @Override
    @Nonnull
    public CompletableFuture<FDBRecordStoreStateCacheEntry> get(@Nonnull FDBRecordStore recordStore, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        // No cache to check. Always go to the database.
        return FDBRecordStoreStateCacheEntry.load(recordStore, existenceCheck);
    }

    @Override
    public void validateDatabase(@Nonnull FDBDatabase database) {
        // All databases are valid with this cache.
    }

    @Override
    public void clear() {
        // Do nothing.
    }

    @SpotBugsSuppressWarnings(value = "MS_EXPOSE_REP", justification = "Object is not actually mutable")
    @Nonnull
    public static PassThroughRecordStoreStateCache instance() {
        return INSTANCE;
    }
}
