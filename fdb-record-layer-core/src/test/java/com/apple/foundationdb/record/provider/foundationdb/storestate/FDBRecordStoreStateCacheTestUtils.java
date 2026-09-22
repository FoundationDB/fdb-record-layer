/*
 * RecordStoreStateCacheTestUtils.java
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

package com.apple.foundationdb.record.provider.foundationdb.storestate;

import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Utilities for testing interactions with {@link FDBRecordStoreStateCache}.
 */
public class FDBRecordStoreStateCacheTestUtils {
    @Nonnull
    static final ReadVersionRecordStoreStateCacheFactory readVersionCacheFactory = ReadVersionRecordStoreStateCacheFactory.newInstance();
    @Nonnull
    static final MetaDataVersionStampStoreStateCacheFactory metaDataVersionStampCacheFactory = MetaDataVersionStampStoreStateCacheFactory.newInstance();

    @Nonnull
    public static Stream<StateCacheTestContext> testContextSource() {
        return Stream.of(new ReadVersionStateCacheTestContext(), new MetaDataVersionStampStateCacheTestContext());
    }

    static void assertCacheMissAndHitAndReset(final FDBStoreTimer timer, final int expectedMissCount,
                                              final int expectedHitCount) {
        assertCacheMiss(timer, expectedMissCount);
        assertCacheHit(timer, expectedHitCount);
        timer.reset();
    }

    static void assertCacheHit(final FDBStoreTimer timer, int expected) {
        assertEquals(expected, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
    }

    static void assertCacheMiss(final FDBStoreTimer timer, int expected) {
        assertEquals(expected, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
    }

    /**
     * A wrapper interface for dealing with the differences between the different {@link FDBRecordStoreStateCache}
     * implementations.
     */
    public interface StateCacheTestContext {
        @Nonnull
        FDBRecordStoreStateCache getCache(@Nonnull FDBDatabase database);

        @Nonnull
        default FDBRecordContext getCachedContext(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordStore.Builder storeBuilder) {
            return getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY);
        }

        @Nonnull
        FDBRecordContext getCachedContext(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordStore.Builder storeBuilder,
                                          @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck);

        void invalidateCache(@Nonnull FDBDatabase fdb);
    }

    /**
     * An implementation of the {@link StateCacheTestContext} that handles caching by read version.
     */
    public static class ReadVersionStateCacheTestContext implements StateCacheTestContext {
        @Nonnull
        @Override
        public FDBRecordStoreStateCache getCache(@Nonnull FDBDatabase database) {
            return readVersionCacheFactory.getCache(database);
        }

        @Nonnull
        @Override
        public FDBRecordContext getCachedContext(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordStore.Builder storeBuilder,
                                                 @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
            long readVersion;
            try (FDBRecordContext context = fdb.openContext()) {
                storeBuilder.copyBuilder().setContext(context).createOrOpen(existenceCheck);
                readVersion = context.getReadVersion();
            }
            FDBRecordContext context = fdb.openContext(null, new FDBStoreTimer());
            context.setReadVersion(readVersion);
            return context;
        }

        @Override
        public void invalidateCache(@Nonnull FDBDatabase fdb) {
            // Ensure that the next read version includes at least one new commit.
            try (FDBRecordContext context = fdb.openContext()) {
                context.ensureActive().addWriteConflictKey(Tuple.from(UUID.randomUUID()).pack());
                context.commit();
            }
        }

        @Override
        public String toString() {
            return "ReadVersionStateCacheTestContext";
        }
    }

    /**
     * An implementation of the {@link StateCacheTestContext} that handles caching by the meta-data version-stamp.
     */
    public static class MetaDataVersionStampStateCacheTestContext implements StateCacheTestContext {

        @Nonnull
        @Override
        public FDBRecordStoreStateCache getCache(@Nonnull FDBDatabase database) {
            return metaDataVersionStampCacheFactory.getCache(database);
        }

        @Nonnull
        @Override
        public FDBRecordContext getCachedContext(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordStore.Builder storeBuilder,
                                                 @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
            boolean cacheable = true;
            try (FDBRecordContext context = fdb.openContext()) {
                FDBRecordStore store = storeBuilder.copyBuilder().setContext(context).createOrOpen(existenceCheck);
                if (!store.getRecordStoreState().getStoreHeader().getCacheable()) {
                    cacheable = false;
                    assertTrue(store.setStateCacheability(true));
                    context.commit();
                }
            }
            if (!cacheable) {
                try (FDBRecordContext context = fdb.openContext()) {
                    storeBuilder.copyBuilder().setContext(context).createOrOpen(existenceCheck);
                    context.commit();
                }
            }
            FDBRecordContext context = fdb.openContext(null, new FDBStoreTimer());
            context.getMetaDataVersionStampAsync(IsolationLevel.SNAPSHOT).join();
            return context;
        }

        @Override
        public void invalidateCache(@Nonnull FDBDatabase fdb) {
            // Ensure that the next read version includes at least one new commit.
            try (FDBRecordContext context = fdb.openContext()) {
                context.setMetaDataVersionStamp();
                context.commit();
            }
        }

        @Override
        public String toString() {
            return "MetaDataVersionStampStateCacheTestContext";
        }
    }
}
