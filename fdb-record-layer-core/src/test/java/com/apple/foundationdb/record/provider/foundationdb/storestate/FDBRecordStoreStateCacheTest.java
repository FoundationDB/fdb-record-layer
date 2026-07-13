/*
 * FDBRecordStoreStateCacheTest.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersionTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreAlreadyExistsException;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreNoInfoAndNotEmptyException;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreStaleMetaDataVersionException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FakeClusterFileUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests to make sure that caching {@link FDBRecordStoreStateCacheEntry} objects work.
 */
@Tag(Tags.RequiresFDB)
@Isolated // Needs to be run in isolation because key space path deletion updates the special meta-data versionstamp key, which is read within these tests
public class FDBRecordStoreStateCacheTest extends FDBRecordStoreTestBase {
    @Nonnull
    private static final ReadVersionRecordStoreStateCacheFactory readVersionCacheFactory = ReadVersionRecordStoreStateCacheFactory.newInstance();
    @Nonnull
    private static final MetaDataVersionStampStoreStateCacheFactory metaDataVersionStampCacheFactory = MetaDataVersionStampStoreStateCacheFactory.newInstance();

    @Nonnull
    public static Stream<FDBRecordStoreStateCacheFactory> factorySource() {
        return Stream.of(readVersionCacheFactory, metaDataVersionStampCacheFactory);
    }

    @Nonnull
    public static Stream<StateCacheTestContext> testContextSource() {
        return Stream.of(new ReadVersionStateCacheTestContext(), new MetaDataVersionStampStateCacheTestContext());
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

    /**
     * Validate that caching by read version works.
     */
    @Test
    public void cacheByReadVersion() throws Exception {
        fdb.setStoreStateCache(readVersionCacheFactory.getCache(fdb));
        long readVersion;
        int metaDataVersion;

        // Open a record store but do not commit to make sure that the updated value is not cached
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(context.hasDirtyStoreState());
            readVersion = context.getReadVersion();
            metaDataVersion = recordStore.getRecordMetaData().getVersion();
            // do not commit
        }

        // Open a record store and validate that the cached state is updated
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            // For this specific case, we hit the cache, but then we need to validate that the store is empty
            // in order to match its null store header.
            assertEquals(0, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(context.hasDirtyStoreState());
            assertEquals(metaDataVersion, recordStore.getRecordMetaData().getVersion());
            commit(context); // commit so a stable value is put into the database
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertFalse(context.hasDirtyStoreState());
            assertEquals(metaDataVersion, recordStore.getRecordMetaData().getVersion());
            readVersion = context.getReadVersion();
            // does not matter whether we commit or not
        }

        try (FDBRecordContext context = openContext()) {
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            assertFalse(context.hasDirtyStoreState());
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertEquals(metaDataVersion, recordStore.getRecordMetaData().getVersion());
        }

        // Make a change to the stored info
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            assertFalse(context.hasDirtyStoreState());
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            recordStore.markIndexWriteOnly("MySimpleRecord$str_value_indexed").get();
            assertTrue(context.hasDirtyStoreState());
            assertFalse(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));
            FDBRecordStore initialRecordStore = recordStore;

            // Reopen the store with the same context and ensure the index is still not readable
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertNotSame(initialRecordStore, recordStore);
            assertNotSame(initialRecordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertFalse(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));

            commit(context);
        }

        // Validate that that change is not present in the cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));

            // Add a random write-conflict range to ensure conflicts are actually checked
            context.ensureActive().addWriteConflictKey(recordStore.recordsSubspace().pack(UUID.randomUUID()));

            // Should not be able to commit due to conflict on str_value_indexed key in record store store
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context::commit);
        }

        // Get a fresh read version
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            long newReadVersion = context.getReadVersion();
            assertThat(newReadVersion, greaterThan(readVersion));
            readVersion = newReadVersion;
            assertFalse(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertFalse(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));
        }
    }

    /**
     * Validate that caching by the meta-data version works.
     */
    @Test
    public void cacheByMetaDataVersion() throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));
        byte[] metaDataVersionStamp;

        // Open the store; save the meta-data
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            context.setMetaDataVersionStamp();
            commit(context);
        }

        // Load the meta-data. It should not be cached as the store meta-data are not cacheable.
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            metaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(metaDataVersionStamp);
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertArrayEquals(metaDataVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            recordStore.markIndexWriteOnly("MySimpleRecord$str_value_indexed").get();
            assertTrue(context.hasDirtyStoreState());
            assertNotNull(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            commit(context);
        }

        // Note that the meta-data version has not been updated
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertArrayEquals(metaDataVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            assertTrue(recordStore.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
            commit(context);
        }

        // Mark the meta-data as cacheable
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(recordStore.setStateCacheability(true));
            assertTrue(context.hasDirtyStoreState());
            assertArrayEquals(metaDataVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            commit(context);
        }

        // Load the store state into cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertArrayEquals(metaDataVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(recordStore.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
            // don't need to commit
        }

        // The first meta-data cache hit!
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
            recordStore.markIndexReadable("MySimpleRecord$str_value_indexed").get();
            assertTrue(context.hasDirtyStoreState());
            assertNull(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            commit(context);
        }

        // Load the updated the meta-data into cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));
            byte[] trMetaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(trMetaDataVersionStamp);
            assertThat(ByteArrayUtil.compareUnsigned(metaDataVersionStamp, trMetaDataVersionStamp), lessThan(0));
            metaDataVersionStamp = trMetaDataVersionStamp;
        }

        // The updated meta-data should now be in cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));
            assertArrayEquals(metaDataVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
        }

        // Changing the meta-data to a non-cacheable state should increment the meta-data version stamp
        long readVersion;
        byte[] commitVersionStamp;
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            readVersion = context.getReadVersion();
            assertTrue(recordStore.setStateCacheability(false));
            assertTrue(context.hasDirtyStoreState());
            assertNull(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            commit(context);
            commitVersionStamp = context.getVersionStamp();
            assertNotNull(commitVersionStamp);
        }

        // This should hit the cache because it uses an older read version where the meta-data versionstamp is good.
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().getCacheable());
        }

        // These should both miss the cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            byte[] trMetaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(trMetaDataVersionStamp);
            assertThat(ByteArrayUtil.compareUnsigned(metaDataVersionStamp, trMetaDataVersionStamp), lessThan(0));
            assertArrayEquals(commitVersionStamp, trMetaDataVersionStamp);
            metaDataVersionStamp = trMetaDataVersionStamp;
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            byte[] trMetaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(trMetaDataVersionStamp);
            assertArrayEquals(metaDataVersionStamp, trMetaDataVersionStamp);
        }
    }

    @Test
    public void cacheByMetaDataVersionFirstTimeEver() throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));

        // Clear out the meta-data version-stamp key
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            byte[] metaDataVersionStampKey = ByteArrayUtil2.unprint("\\xff/metadataVersion");
            context.ensureActive().options().setAccessSystemKeys();
            context.ensureActive().clear(metaDataVersionStampKey);
            commit(context);
        }

        byte[] commitVersionStamp;
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertNull(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));

            recordStore.setStateCacheability(true);
            commit(context);
            commitVersionStamp = context.getVersionStamp();
            assertNotNull(commitVersionStamp);
        }

        // Usually, marking the store state as cacheable from a non-cacheable state won't update the
        // meta-data version. However, in the case where the key is initially unset, to make sure that
        // caching actually happens, the meta-data version *is* updated.
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertArrayEquals(commitVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertArrayEquals(commitVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
        }
    }

    /**
     * Make sure that if one transaction changes the store header then an open store in another transaction that
     * loaded the store state from cache will fail at commit time with conflict.
     */
    @ParameterizedTest(name = "conflictWhenCachedChanged (test context = {0})")
    @MethodSource("testContextSource")
    public void conflictWhenCachedChanged(@Nonnull StateCacheTestContext testContext) {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");
        RecordMetaData metaData2 = metaDataBuilder.getRecordMetaData();
        assertThat(metaData1.getVersion(), lessThan(metaData2.getVersion()));

        FDBRecordStore.Builder storeBuilder;

        // Initialize the record store with a meta-data store
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();

            FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metaData1)
                    .setKeySpacePath(path)
                    .create();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context);

            storeBuilder = recordStore.asBuilder();
        }

        // Load the record store state into the cache.
        try (FDBRecordContext context1 = testContext.getCachedContext(fdb, storeBuilder); FDBRecordContext context2 = testContext.getCachedContext(fdb, storeBuilder)) {
            FDBRecordStore recordStore1 = storeBuilder.copyBuilder()
                    .setContext(context1)
                    .setMetaDataProvider(metaData1)
                    .open();
            assertEquals(1, context1.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertEquals(metaData1.getVersion(), recordStore1.getRecordMetaData().getVersion());
            assertEquals(metaData1.getVersion(), recordStore1.getRecordStoreState().getStoreHeader().getMetaDataversion());

            // Update the meta-data in the second transaction
            FDBRecordStore recordStore2 = storeBuilder.copyBuilder()
                    .setContext(context2)
                    .setMetaDataProvider(metaData2)
                    .open();
            assertEquals(1, context2.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertEquals(Collections.singletonList(recordStore2.getRecordMetaData().getRecordType("MySimpleRecord")),
                    recordStore2.getRecordMetaData().recordTypesForIndex(recordStore2.getRecordMetaData().getIndex("MySimpleRecord$num_value_2")));
            assertEquals(metaData2.getVersion(), recordStore2.getRecordMetaData().getVersion());
            assertEquals(metaData2.getVersion(), recordStore2.getRecordStoreState().getStoreHeader().getMetaDataversion());
            context2.commit();

            // Add a write to context1 so that the conflict ranges actually get checked.
            recordStore1.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066)
                    .setNumValue2(1415)
                    .build());

            // Should conflict on store header even though not actually read in this transaction
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context1::commit);
        }

        // New transaction should now see the new meta-data version
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();

            // Trying to load with the old meta-data should fail
            assertThrows(RecordStoreStaleMetaDataVersionException.class, () -> storeBuilder.copyBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metaData1)
                    .open());
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));

            // Trying to load with the new meta-data should succeed
            FDBRecordStore recordStore = storeBuilder.copyBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metaData2)
                    .open();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
        }
    }

    /**
     * Validate that the store existence check is still performed on the cached store info.
     */
    @ParameterizedTest(name = "existenceCheckOnCachedStoreStates (test context = {0})")
    @MethodSource("testContextSource")
    public void existenceCheckOnCachedStoreStates(@Nonnull StateCacheTestContext testContext) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        // Create a record store
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(context.hasDirtyStoreState());
            // Save a record so that when the store header is deleted, it won't be an empty record store
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build());
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }

        byte[] storeInfoKey;
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder)) {
            storeBuilder.setContext(context);
            assertThrows(RecordStoreAlreadyExistsException.class, storeBuilder::create);
            context.getTimer().reset();
            FDBRecordStore store = storeBuilder.open();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));

            // Delete the store header
            storeInfoKey = store.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key());
            context.ensureActive().clear(storeInfoKey);
            commit(context);
        }

        // The caches have dirty information from the out-of-band "clear".
        testContext.invalidateCache(fdb);
        assertThrows(RecordStoreNoInfoAndNotEmptyException.class, () -> testContext.getCachedContext(fdb, storeBuilder));

        // Ensure the store info key is still empty
        try (FDBRecordContext context = fdb.openContext()) {
            assertNull(context.readTransaction(true).get(storeInfoKey).get());
        }
    }

    /**
     * Validate that deleting a record store causes the record store to go back to the database as it's possible the
     * cached stuff is what was deleted.
     */
    @ParameterizedTest(name = "storeDeletionInSameContext (test context = {0})")
    @MethodSource("testContextSource")
    public void storeDeletionInSameContext(@Nonnull StateCacheTestContext testContext) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder)) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));

            context.getTimer().reset();
            FDBRecordStore.deleteStore(context, recordStore.getSubspace());
            recordStore.asBuilder().create();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));

            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder)) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            path.deleteAllData(context);

            context.getTimer().reset();
            recordStore.asBuilder().create();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        }

        // Deleting all records should not disable the index, so the result should still be cacheable.
        // See: https://github.com/FoundationDB/fdb-record-layer/issues/399
        final String disabledIndex = "MySimpleRecord$str_value_indexed";
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            recordStore.markIndexDisabled(disabledIndex).get();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            recordStore.deleteAllRecords();

            context.getTimer().reset();
            recordStore = recordStore.asBuilder().open();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            commit(context);
        }
    }

    /**
     * After a store is deleted, validate that future transactions need to reload it from cache.
     */
    @ParameterizedTest(name = "storeDeletionAcrossContexts (test context = {0})")
    @MethodSource("testContextSource")
    public void storeDeletionAcrossContexts(@Nonnull StateCacheTestContext testContext) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.setStateCacheability(true));
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }

        // Delete by calling deleteStore.
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            FDBRecordStore.deleteStore(context, recordStore.getSubspace());
            commit(context);
        }

        // After deleting it, when opening the same store again, it shouldn't be cached.
        try (FDBRecordContext context = fdb.openContext(null, new FDBStoreTimer())) {
            FDBRecordStore store = storeBuilder.setContext(context).create();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(store.setStateCacheability(true));
            commit(context);
        }

        // Delete by calling path.deleteAllData
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            path.deleteAllData(context);
            commit(context);
        }

        try (FDBRecordContext context = fdb.openContext(null, new FDBStoreTimer())) {
            FDBRecordStore store = storeBuilder.setContext(context).create();
            store.setStateCacheabilityAsync(true).get();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            commit(context);
        }

        // Deleting all records should not disable the index state.
        final String disabledIndex = "MySimpleRecord$str_value_indexed";
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            recordStore.markIndexDisabled(disabledIndex).get();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            recordStore.deleteAllRecords();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            commit(context);
        }
    }

    /**
     * Deleting a non-cacheable store must NOT bump the meta-data version stamp: no other
     * client can possibly hold a cached copy of a non-cacheable header, so the version stamp
     * (a JVM-wide bottleneck on the {@code \xff/metadataVersion} key) shouldn't be touched.
     * This is the low-level property that lets parallel tests share the SYS catalog without
     * conflicting on catalog teardown.
     */
    @Test
    void deleteNonCacheableStoreDoesNotBumpMetaDataVersionStamp() throws Exception {
        // Bootstrap: ensure the meta-data version stamp key has a value before the test runs,
        // so we can distinguish "unchanged" from "was never set". The store below is opened
        // with cacheability disabled (which is the default from setStateCacheability(false)).
        try (FDBRecordContext context = fdb.openContext()) {
            if (context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT) == null) {
                context.setMetaDataVersionStamp();
            }
            commit(context);
        }

        Subspace subspace;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertFalse(recordStore.getRecordStoreState().getStoreHeader().getCacheable(),
                    "test presumes the store is non-cacheable by default");
            subspace = recordStore.getSubspace();
            commit(context);
        }

        // Snapshot the version stamp before deletion — reading via SNAPSHOT so this txn doesn't
        // conflict with the delete-txn below and skew the result.
        final byte[] beforeStamp;
        try (FDBRecordContext context = fdb.openContext()) {
            beforeStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
        }
        assertNotNull(beforeStamp, "bootstrap should have populated the meta-data version stamp");

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, subspace);
            commit(context);
        }

        try (FDBRecordContext context = fdb.openContext()) {
            final byte[] afterStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertArrayEquals(beforeStamp, afterStamp,
                    "deleting a non-cacheable store should not have bumped the meta-data version stamp");
        }
    }

    /**
     * Complement of {@link #deleteNonCacheableStoreDoesNotBumpMetaDataVersionStamp()}: deleting
     * a cacheable store MUST bump the stamp — otherwise sibling clients could keep serving
     * reads out of a stale cached header long after the store is gone.
     */
    @Test
    void deleteCacheableStoreBumpsMetaDataVersionStamp() throws Exception {
        try (FDBRecordContext context = fdb.openContext()) {
            if (context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT) == null) {
                context.setMetaDataVersionStamp();
            }
            commit(context);
        }

        Subspace subspace;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.setStateCacheability(true), "flipping to cacheable should have changed something");
            subspace = recordStore.getSubspace();
            commit(context);
        }
        // Commit above already bumped the stamp (transition to cacheable). Snapshot after that.
        final byte[] beforeStamp;
        try (FDBRecordContext context = fdb.openContext()) {
            beforeStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
        }
        assertNotNull(beforeStamp);

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, subspace);
            commit(context);
        }

        try (FDBRecordContext context = fdb.openContext()) {
            final byte[] afterStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(afterStamp);
            assertFalse(java.util.Arrays.equals(beforeStamp, afterStamp),
                    "deleting a cacheable store should have bumped the meta-data version stamp");
        }
    }

    /**
     * Deleting an empty subspace (no store header present) must not bump the stamp either —
     * there's no cached header to invalidate.
     */
    @Test
    void deleteMissingStoreDoesNotBumpMetaDataVersionStamp() throws Exception {
        try (FDBRecordContext context = fdb.openContext()) {
            if (context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT) == null) {
                context.setMetaDataVersionStamp();
            }
            commit(context);
        }

        // Use the test's per-instance path, but never open a store there.
        final Subspace subspace;
        try (FDBRecordContext context = openContext()) {
            subspace = path.toSubspace(context);
        }
        final byte[] beforeStamp;
        try (FDBRecordContext context = fdb.openContext()) {
            beforeStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
        }
        assertNotNull(beforeStamp);

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, subspace);
            commit(context);
        }

        try (FDBRecordContext context = fdb.openContext()) {
            final byte[] afterStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertArrayEquals(beforeStamp, afterStamp,
                    "deleting an empty subspace (no header) should not have bumped the meta-data version stamp");
        }
    }

    /**
     * Regression test that pins the current behaviour: on a state-cache <em>miss</em>, opening a
     * cacheable store issues a {@code SERIALIZABLE getRange(subspace.range(), 1)} inside
     * {@code FDBRecordStore.loadStoreHeaderAsync}, which registers a read-conflict range that
     * covers (at minimum) the store header key. Any concurrent transaction that writes to the
     * store header — including the very common case of a second opener that itself hits a cache
     * miss and updates the header via {@code checkVersion} — will therefore serialise-fail the
     * first transaction if the writer commits first.
     *
     * <p>This is the shape of the failure we hit in the yaml-tests parallel harness: a "reader"
     * transaction opens the {@code /__SYS/CATALOG} store, misses the cache, adds a wide read
     * conflict, and then loses the commit race to a concurrent writer inside the same subspace.
     * The paired positive control below ({@link #cacheHitOpenDoesNotConflictWithConcurrentRecordInsert()})
     * shows that when the cache actually serves the open, the read conflict shrinks to a single
     * key on {@code STORE_INFO_KEY} and no conflict occurs.</p>
     */
    @Test
    void cacheMissOpenConflictsWithConcurrentHeaderWrite() throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));

        // Bootstrap: create the store as cacheable, then invalidate the cache so the next opens
        // are guaranteed to miss.
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.setStateCacheability(true));
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        // Bump the meta-data version stamp to invalidate any cache entries. From here forward,
        // opens will miss until at least one commits and repopulates the cache.
        try (FDBRecordContext context = fdb.openContext()) {
            context.setMetaDataVersionStamp();
            commit(context);
        }

        // Open two contexts *concurrently* — both will fall through to loadStoreHeaderAsync, both
        // will add a range read conflict that covers (at minimum) the header key.
        try (FDBRecordContext readerContext = fdb.openContext(null, new FDBStoreTimer());
             FDBRecordContext writerContext = fdb.openContext(null, new FDBStoreTimer())) {
            // Pin both read versions *before* the writer commits, so the conflict has to be
            // resolved at commit time rather than being masked by an updated read version.
            readerContext.getReadVersion();
            writerContext.getReadVersion();

            // Reader: opens the store — this is where the SERIALIZABLE range read that will
            // eventually serialise-fail us gets added to the read-conflict set.
            FDBRecordStore reader = storeBuilder.copyBuilder().setContext(readerContext).open();
            // Give the reader an explicit write so its commit actually runs conflict resolution
            // (empty transactions skip it). Any unrelated key works.
            readerContext.ensureActive().addWriteConflictKey(reader.recordsSubspace().pack(UUID.randomUUID()));

            // Writer: open the same store (also a cache miss), then write the header
            // (setHeaderUserField sets STORE_INFO_KEY) and commit first.
            FDBRecordStore writer = storeBuilder.copyBuilder().setContext(writerContext).open();
            writer.setHeaderUserField("miss-conflict-probe",
                    com.google.protobuf.ByteString.copyFromUtf8("probe-" + UUID.randomUUID()));
            commit(writerContext);

            // Now the reader must fail: its cache-miss read range on the header overlaps the
            // writer's header write.
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, readerContext::commit,
                    "cache-miss reader should conflict with concurrent header writer");
        }
    }

    /**
     * Positive control for {@link #cacheMissOpenConflictsWithConcurrentHeaderWrite()}: when the
     * cache serves the open, the state-cache path adds only a point read conflict on
     * {@code STORE_INFO_KEY} (see {@code FDBRecordStoreStateCacheEntry.handleCachedState}). A
     * concurrent writer that inserts a record without touching the header will not conflict.
     *
     * <p>Together with the negative test above, this proves that the cache-miss ↔ range-read is
     * the mechanism that causes the yaml-tests {@code /__SYS/CATALOG} conflicts: eliminating
     * misses (e.g. by pinning the cache or by narrowing the miss path) would remove the
     * conflict class entirely.</p>
     */
    @Test
    void cacheHitOpenDoesNotConflictWithConcurrentRecordInsert() throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.setStateCacheability(true));
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        // Prime the cache with an open+commit so the entry is loaded and valid.
        try (FDBRecordContext context = fdb.openContext(null, new FDBStoreTimer())) {
            storeBuilder.copyBuilder().setContext(context).open();
            commit(context);
        }

        try (FDBRecordContext readerContext = fdb.openContext(null, new FDBStoreTimer());
             FDBRecordContext writerContext = fdb.openContext(null, new FDBStoreTimer())) {
            readerContext.getReadVersion();
            writerContext.getReadVersion();

            // Reader: cache HIT — only STORE_INFO_KEY gets a point read conflict.
            FDBRecordStore reader = storeBuilder.copyBuilder().setContext(readerContext).open();
            // Force conflict resolution to actually run by giving the reader a write of its own,
            // targeting a fresh unrelated key so it doesn't collide with the record we insert below.
            readerContext.ensureActive().addWriteConflictKey(
                    reader.recordsSubspace().pack(Tuple.from("cache-hit-probe-" + UUID.randomUUID())));

            // Writer: insert a new record. This writes only to <subspace>/RECORDS/<pk> and index
            // keys — it does not touch STORE_INFO_KEY. Cache HIT reader's point read conflict on
            // STORE_INFO_KEY does not overlap.
            FDBRecordStore writer = storeBuilder.copyBuilder().setContext(writerContext).open();
            writer.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(4242L)
                    .setStrValueIndexed("cache-hit-probe")
                    .build());
            commit(writerContext);

            // Reader commits cleanly — its point read conflict on STORE_INFO_KEY doesn't overlap
            // the writer's per-record writes.
            commit(readerContext);
        }
    }

    /**
     * Companion to the two tests above that isolates the record-insert axis. It confirms — perhaps
     * surprisingly — that two concurrent cache-miss opens that both only insert records (no header
     * mutation) do <em>not</em> conflict with each other. The SERIALIZABLE
     * {@code getRange(subspace.range(), 1)} that the miss path runs in
     * {@code loadStoreHeaderAsync} appears to add a read conflict only up to (roughly) the header
     * key, not the full subspace, so writes at {@code <subspace>/RECORDS/<pk>} (which sit past the
     * header keyspace) don't overlap.
     *
     * <p>This constrains the shape of the yaml-tests {@code /__SYS/CATALOG} failure: since the
     * observed conflict range there covers the entire CATALOG subspace, the failure cannot be
     * explained by "two cache-miss opens both inserting records" alone. There must be an
     * additional mechanism — most likely one of the concurrent transactions implicitly writing
     * {@code STORE_INFO_KEY} via {@code updateStoreHeaderAsync} on the {@code checkVersion} dirty
     * path (format-version bump, metadata-version bump, or cacheability flip), or a wider write
     * from a DDL constant action.</p>
     */
    @Test
    void concurrentCacheMissOpensBothInsertingRecordsDoNotConflict() throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.setStateCacheability(true));
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        // Invalidate any cached entries so both concurrent opens below are guaranteed to miss.
        try (FDBRecordContext context = fdb.openContext()) {
            context.setMetaDataVersionStamp();
            commit(context);
        }

        try (FDBRecordContext firstContext = fdb.openContext(null, new FDBStoreTimer());
             FDBRecordContext secondContext = fdb.openContext(null, new FDBStoreTimer())) {
            // Pin both read versions before either commits, so conflict resolution runs on both.
            firstContext.getReadVersion();
            secondContext.getReadVersion();

            FDBRecordStore first = storeBuilder.copyBuilder().setContext(firstContext).open();
            FDBRecordStore second = storeBuilder.copyBuilder().setContext(secondContext).open();

            // Both insert distinct records — no header touched, distinct primary keys, distinct
            // index entries (different str_value_indexed).
            first.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1001L)
                    .setStrValueIndexed("first-writer")
                    .build());
            second.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1002L)
                    .setStrValueIndexed("second-writer")
                    .build());

            // Both commit cleanly — the cache-miss read conflict does not extend far enough to
            // overlap a plain record-write, so this is not by itself the yaml-tests failure mode.
            commit(secondContext);
            commit(firstContext);
        }
    }

    /**
     * Verify that updating a header user field will be updated if the store state is cached.
     */
    @ParameterizedTest(name = "cacheUserFields (test context = {0})")
    @MethodSource("testContextSource")
    public void cacheUserFields(@Nonnull StateCacheTestContext testContext) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.setStateCacheability(true));
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertNull(recordStore.getHeaderUserField("expr"));
            recordStore.setHeaderUserField("expr", Key.Expressions.field("parent").nest("child").toKeyExpression().toByteString());
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertNotNull(recordStore.getHeaderUserField("expr"));
            KeyExpression expr = KeyExpression.fromProto(RecordKeyExpressionProto.KeyExpression.parseFrom(recordStore.getHeaderUserField("expr")));
            assertEquals(Key.Expressions.field("parent").nest("child"), expr);
            recordStore.clearHeaderUserField("expr");
            commit(context);
        }

        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            openSimpleRecordStore(context);
            assertNull(recordStore.getHeaderUserField("expr"));
            commit(context);
        }
    }

    /**
     * Make sure that caching two different subspaces are both cached but with separate entries.
     */
    @ParameterizedTest(name = "cacheTwoSubspaces (test context = {0})")
    @MethodSource("testContextSource")
    public void cacheTwoSubspaces(@Nonnull StateCacheTestContext testContext) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));
        final KeySpacePath path1 = pathManager.createPath();
        final KeySpacePath path2 = pathManager.createPath();

        final FDBRecordStore.Builder storeBuilder1;
        final FDBRecordStore.Builder storeBuilder2;

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            path1.deleteAllData(context);
            path2.deleteAllData(context);

            openSimpleRecordStore(context);
            FDBRecordStore store1 = recordStore.asBuilder().setKeySpacePath(path1).create();
            store1.setStateCacheabilityAsync(true).get();
            storeBuilder1 = store1.asBuilder();
            store1.markIndexWriteOnly("MySimpleRecord$str_value_indexed").get();

            FDBRecordStore store2 = recordStore.asBuilder().setKeySpacePath(path2).create();
            store2.setStateCacheabilityAsync(true).get();
            storeBuilder2 = store2.asBuilder();
            store2.markIndexDisabled("MySimpleRecord$num_value_3_indexed").get();

            commit(context);
        }

        // Open both paths. Only the one in path1 should be cached.
        long readVersion;
        try (FDBRecordContext context = testContext.getCachedContext(fdb, storeBuilder1, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)) {
            FDBRecordStore store1 = storeBuilder1.setContext(context).open();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(store1.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
            assertTrue(store1.isIndexReadable("MySimpleRecord$num_value_3_indexed"));
            FDBRecordStore store2 = storeBuilder2.setContext(context).open();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(store2.isIndexReadable("MySimpleRecord$str_value_indexed"));
            assertTrue(store2.isIndexDisabled("MySimpleRecord$num_value_3_indexed"));

            readVersion = context.getReadVersion();
        }

        // Open both paths. Now they are both cached.
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            FDBRecordStore store1 = storeBuilder1.setContext(context).open();
            assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(store1.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
            assertTrue(store1.isIndexReadable("MySimpleRecord$num_value_3_indexed"));
            FDBRecordStore store2 = storeBuilder2.setContext(context).open();
            assertEquals(2, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(store2.isIndexReadable("MySimpleRecord$str_value_indexed"));
            assertTrue(store2.isIndexDisabled("MySimpleRecord$num_value_3_indexed"));
        }
    }

    /**
     * Validate that caching just naturally works.
     */
    @ParameterizedTest(name = "cacheWithVersionTracking (test context = {0})")
    @MethodSource("testContextSource")
    public void cacheWithVersionTracking(@Nonnull StateCacheTestContext testContext) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));
        fdb.setTrackLastSeenVersion(true);
        FDBStoreTimer timer = new FDBStoreTimer();
        final FDBDatabase.WeakReadSemantics readSemantics = new FDBDatabase.WeakReadSemantics(0L, 5000, false);

        // Load up a read version
        try (FDBRecordContext context = fdb.openContext(null, timer, null)) {
            context.getReadVersion();
            commit(context);
        }

        // Commit a new meta-data
        long commitVersion;
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            openSimpleRecordStore(context);
            recordStore.setStateCacheabilityAsync(true).get();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            recordStore.markIndexDisabled("MySimpleRecord$str_value_indexed").get();
            commit(context);
            commitVersion = context.getCommittedVersion();
        }

        // Version caching will elect to use the commit version, which is not cached
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            assertEquals(commitVersion, context.getReadVersion());
            openSimpleRecordStore(context);
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));
            commit(context); // should be read only-so won't change commit version
        }

        // Version caching will still use the commit version from the first (non read-only commit), but now it is in cache
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            assertEquals(commitVersion, context.getReadVersion());
            openSimpleRecordStore(context);
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));

            // Add a dummy write to increase the DB version
            context.ensureActive().addWriteConflictKey(recordStore.recordsSubspace().pack(UUID.randomUUID()));
            commit(context);
            assertThat(context.getCommittedVersion(), greaterThan(commitVersion));
            commitVersion = context.getCommittedVersion();
        }

        // The commit version will be from the commit above. This should invalidate the
        // read-version cache, but not the meta-data version cache.
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            assertEquals(commitVersion, context.getReadVersion());
            openSimpleRecordStore(context);
            if (testContext instanceof ReadVersionStateCacheTestContext) {
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            } else {
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            }
            assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));

            // Add a dummy write to increase the DB version
            context.ensureActive().addWriteConflictKey(recordStore.recordsSubspace().pack(UUID.randomUUID()));
            commit(context);
            assertThat(context.getCommittedVersion(), greaterThan(commitVersion));
            commitVersion = context.getCommittedVersion();
        }

        // Load a new read version.
        timer.reset();
        final long readVersion;
        try (FDBRecordContext context = fdb.openContext(null, timer, null)) {
            readVersion = context.getReadVersion();
            assertThat(readVersion, greaterThanOrEqualTo(commitVersion));
            openSimpleRecordStore(context);
            if (testContext instanceof ReadVersionStateCacheTestContext) {
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            } else {
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            }
            assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));
        }

        // Load the meta-data using the cached read version.
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            assertEquals(readVersion, context.getReadVersion());
            openSimpleRecordStore(context);
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));
        }
    }

    private void openSimpleStoreWithCacheabilityOnOpen(FDBRecordContext context, FDBRecordStore.StateCacheabilityOnOpen cacheabilityOnOpen) {
        RecordMetaData metaData = simpleMetaData(NO_HOOK);
        recordStore = getStoreBuilder(context, metaData)
                .setStateCacheabilityOnOpen(cacheabilityOnOpen)
                .createOrOpen();
    }

    /**
     * Validate that the store state cacheability flag can be set during check version.
     */
    @Test
    void setCacheabilityDuringStoreOpening() {
        FDBRecordStoreStateCache storeStateCache = MetaDataVersionStampStoreStateCacheFactory.newInstance()
                .getCache(fdb);
        fdb.setStoreStateCache(storeStateCache);

        try (FDBRecordContext context = fdb.openContext()) {
            if (context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT) == null) {
                context.setMetaDataVersionStamp();
            }
            commit(context);
        }

        // Create the store, initially not cacheable
        FDBStoreTimer timer = new FDBStoreTimer();
        byte[] metaDataVersionStamp1;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertNotCacheable();
            metaDataVersionStamp1 = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            commit(context);
        }
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

        // Open the store, this time changing to make cacheable
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE_IF_NEW);
            assertNotCacheable();
            commit(context);
        }
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

        // Open the store, this time changing to make cacheable
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE);
            assertCacheable();
            commit(context);
        }
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

        // Open the store, again with DEFAULT behavior. It should now be marked cacheable
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertCacheable();
            commit(context);
        }
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

        // Turn off caching during check version. The actual opening should be a hit, but the next one
        // should miss as it turns off state cacheability
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.NOT_CACHEABLE);
            assertNotCacheable();
            commit(context);
        }
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

        // Opening the store again should be a cache miss
        byte[] metaDataVersionStamp2;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            metaDataVersionStamp2 = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertFalse(Arrays.equals(metaDataVersionStamp2, metaDataVersionStamp1),
                    "Turning off store state cacheability should update the meta-data version stamp");
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE_IF_NEW);
            assertNotCacheable();
            commit(context);
        }
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

        // Open again. This time, using NOT_CACHEABLE should not induce any changes (including to the meta-data versionstamp)
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp2);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.NOT_CACHEABLE);
            assertNotCacheable();
            commit(context);
        }
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp2,
                    "Meta-data version stamp should not be changed if the store state was originally not cacheable");
        }
    }

    @Test
    void setCacheabilityOnStoreCreation() {
        FDBRecordStoreStateCache storeStateCache = MetaDataVersionStampStoreStateCacheFactory.newInstance()
                .getCache(fdb);
        fdb.setStoreStateCache(storeStateCache);

        try (FDBRecordContext context = openContext()) {
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertNotCacheable();
            // do not commit
        }

        // Creating a new store with CACHEABLE_IF_NEW should set the store state cacheability
        try (FDBRecordContext context = openContext()) {
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE_IF_NEW);
            assertCacheable();
            commit(context);
        }

        final FDBStoreTimer timer = new FDBStoreTimer();

        // Open the store again loading the cache
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertCacheable();
        }
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

        // Open the store a third time, this time using the cache
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertCacheable();
        }
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
        timer.reset();

    }

    @Test
    void doNotSetCacheabilityDuringCheckVersionOnOldFormatVersion() throws Exception {
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            // do not commit
        }

        storeBuilder.setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.CACHEABLE_STATE));

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder
                    .setContext(context)
                    .setStateCacheabilityOnOpen(FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE_IF_NEW)
                    .create();
            // At this older format version, the store should not be marked as cacheable
            assertNotCacheable();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder
                    .setContext(context)
                    .setStateCacheabilityOnOpen(FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE)
                    .open();
            assertNotCacheable();
            commit(context);
        }

        // Update the format version and commit
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder
                    .setContext(context)
                    .setStateCacheabilityOnOpen(FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                    .open();
            assertCacheable();
            recordStore.setStateCacheability(false);
            commit(context);
        }

        // Set the format version on the builder so that store state caching isn't supported. However,
        // it should read the format version from the store and determine that it actually *is* supported,
        // and therefore should update the cacheability
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder
                    .setContext(context)
                    .setStateCacheabilityOnOpen(FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE)
                    .setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.CACHEABLE_STATE))
                    .open();
            assertCacheable();
            commit(context);
        }
    }

    @ParameterizedTest(name = "useWithDifferentDatabase (factory = {0})")
    @MethodSource("factorySource")
    public void useWithDifferentDatabase(FDBRecordStoreStateCacheFactory storeStateCacheFactory) throws Exception {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        String clusterFile = FakeClusterFileUtil.createFakeClusterFile("record_store_cache_");
        FDBDatabaseFactory.instance().setStoreStateCacheFactory(readVersionCacheFactory);
        FDBDatabase secondDatabase = FDBDatabaseFactory.instance().getDatabase(clusterFile);

        // Using the cache with a context from the wrong database shouldn't work
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCoreArgumentException ex = assertThrows(RecordCoreArgumentException.class,
                    () -> secondDatabase.getStoreStateCache().get(recordStore, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY));
            assertThat(ex.getMessage(), containsString("record store state cache used with different database"));
        }

        // Setting the database's cache to a record store of the wrong database shouldn't work
        FDBRecordStoreStateCache originalCache = fdb.getStoreStateCache();
        RecordCoreArgumentException ex = assertThrows(RecordCoreArgumentException.class,
                () -> fdb.setStoreStateCache(secondDatabase.getStoreStateCache()));
        assertThat(ex.getMessage(), containsString("record store state cache used with different database"));
        assertSame(originalCache, fdb.getStoreStateCache());
    }

    @Test
    public void setCacheableAtWrongFormatVersion() throws Exception {
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));

        // Initialize the store at the format version prior to the cacheable state version
        FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(RecordMetaData.build(TestRecords1Proto.getDescriptor()))
                .setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.CACHEABLE_STATE));
        try (FDBRecordContext context = openContext()) {
            storeBuilder.copyBuilder().setContext(context).create();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = storeBuilder.copyBuilder()
                    .setContext(context)
                    .open();
            assertEquals(FormatVersion.SAVE_VERSION_WITH_RECORD, recordStore.getFormatVersionEnum());
            RecordCoreException e = assertThrows(RecordCoreException.class, () -> recordStore.setStateCacheability(true));
            assertThat(e.getMessage(), containsString("cannot mark record store state cacheable at format version"));
            commit(context);
        }

        // Update the format version
        try (FDBRecordContext context = openContext()) {
            storeBuilder.copyBuilder()
                    .setContext(context)
                    .setFormatVersion(FormatVersion.CACHEABLE_STATE)
                    .open();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            // Assert that format version happens because of the upgrade behind the scenes
            assertEquals(FormatVersionTestUtils.previous(FormatVersion.CACHEABLE_STATE), storeBuilder.getFormatVersionEnum());
            FDBRecordStore recordStore = storeBuilder.copyBuilder()
                    .setContext(context)
                    .open();
            assertEquals(FormatVersion.CACHEABLE_STATE, recordStore.getFormatVersionEnum());
            assertTrue(recordStore.setStateCacheability(true));
            commit(context);
        }
    }

    private void assertCacheable() {
        assertTrue(isStoreCachable(), "Store state should be cacheable");
    }

    private void assertNotCacheable() {
        assertFalse(isStoreCachable(), "Store state should not be cacheable");
    }

    private boolean isStoreCachable() {
        return recordStore.getRecordStoreState().getStoreHeader().getCacheable();
    }
}
