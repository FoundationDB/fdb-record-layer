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
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
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
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
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
    public static Stream<FDBRecordStoreStateCacheFactory> factorySource() {
        return Stream.of(FDBRecordStoreStateCacheTestUtils.readVersionCacheFactory, FDBRecordStoreStateCacheTestUtils.metaDataVersionStampCacheFactory);
    }

    public static Stream<FDBRecordStoreStateCacheTestUtils.StateCacheTestContext> testContextSource() {
        return FDBRecordStoreStateCacheTestUtils.testContextSource();
    }

    /**
     * Validate that caching by read version works.
     */
    @Test
    public void cacheByReadVersion() throws Exception {
        fdb.setStoreStateCache(FDBRecordStoreStateCacheTestUtils.readVersionCacheFactory.getCache(fdb));
        long readVersion;
        int metaDataVersion;

        // Open a record store but do not commit to make sure that the updated value is not cached
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 0);
            assertTrue(context.hasDirtyStoreState());
            assertEquals(metaDataVersion, recordStore.getRecordMetaData().getVersion());
            commit(context); // commit so a stable value is put into the database
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            assertFalse(context.hasDirtyStoreState());
            assertEquals(metaDataVersion, recordStore.getRecordMetaData().getVersion());
            readVersion = context.getReadVersion();
            // does not matter whether we commit or not
        }

        try (FDBRecordContext context = openContext()) {
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            assertFalse(context.hasDirtyStoreState());
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertEquals(metaDataVersion, recordStore.getRecordMetaData().getVersion());
        }

        // Make a change to the stored info
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            assertFalse(context.hasDirtyStoreState());
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            recordStore.markIndexWriteOnly("MySimpleRecord$str_value_indexed").get();
            assertTrue(context.hasDirtyStoreState());
            assertFalse(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isReadable());
            FDBRecordStore initialRecordStore = recordStore;

            // Reopen the store with the same context and ensure the index is still not readable
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            assertNotSame(initialRecordStore, recordStore);
            assertNotSame(initialRecordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertFalse(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isReadable());

            commit(context);
        }

        // Validate that that change is not present in the cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isReadable());

            // Add a random write-conflict range to ensure conflicts are actually checked
            context.ensureActive().addWriteConflictKey(recordStore.recordsSubspace().pack(UUID.randomUUID()));

            // Should not be able to commit due to conflict on str_value_indexed key in record store store
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context::commit);
        }

        // Get a fresh read version
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            long newReadVersion = context.getReadVersion();
            assertThat(newReadVersion, greaterThan(readVersion));
            readVersion = newReadVersion;
            assertFalse(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isReadable());
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertFalse(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isReadable());
        }
    }

    /**
     * Validate that caching by the meta-data version works.
     */
    @Test
    public void cacheByMetaDataVersion() throws Exception {
        fdb.setStoreStateCache(FDBRecordStoreStateCacheTestUtils.metaDataVersionStampCacheFactory.getCache(fdb));
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            metaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(metaDataVersionStamp);
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            assertArrayEquals(metaDataVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isWriteOnly());
            commit(context);
        }

        // Mark the meta-data as cacheable
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isWriteOnly());
            // don't need to commit
        }

        // The first meta-data cache hit!
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isWriteOnly());
            recordStore.markIndexReadable("MySimpleRecord$str_value_indexed").get();
            assertTrue(context.hasDirtyStoreState());
            assertNull(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
            commit(context);
        }

        // Load the updated the meta-data into cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isReadable());
            byte[] trMetaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(trMetaDataVersionStamp);
            assertThat(ByteArrayUtil.compareUnsigned(metaDataVersionStamp, trMetaDataVersionStamp), lessThan(0));
            metaDataVersionStamp = trMetaDataVersionStamp;
        }

        // The updated meta-data should now be in cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isReadable());
            assertArrayEquals(metaDataVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
        }

        // Changing the meta-data to a non-cacheable state should increment the meta-data version stamp
        long readVersion;
        byte[] commitVersionStamp;
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().getCacheable());
        }

        // These should both miss the cache
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            byte[] trMetaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(trMetaDataVersionStamp);
            assertThat(ByteArrayUtil.compareUnsigned(metaDataVersionStamp, trMetaDataVersionStamp), lessThan(0));
            assertArrayEquals(commitVersionStamp, trMetaDataVersionStamp);
            metaDataVersionStamp = trMetaDataVersionStamp;
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            byte[] trMetaDataVersionStamp = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            assertNotNull(trMetaDataVersionStamp);
            assertArrayEquals(metaDataVersionStamp, trMetaDataVersionStamp);
        }
    }

    @Test
    public void cacheByMetaDataVersionFirstTimeEver() throws Exception {
        fdb.setStoreStateCache(FDBRecordStoreStateCacheTestUtils.metaDataVersionStampCacheFactory.getCache(fdb));

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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            assertArrayEquals(commitVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
        }

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertArrayEquals(commitVersionStamp, context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT));
        }
    }

    /**
     * Make sure that if one transaction changes the store header then an open store in another transaction that
     * loaded the store state from cache will fail at commit time with conflict.
     */
    @ParameterizedTest(name = "conflictWhenCachedChanged (test context = {0})")
    @MethodSource("testContextSource")
    public void conflictWhenCachedChanged(@Nonnull FDBRecordStoreStateCacheTestUtils.StateCacheTestContext testContext) {
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context1.getTimer(), 1);
            assertEquals(metaData1.getVersion(), recordStore1.getRecordMetaData().getVersion());
            assertEquals(metaData1.getVersion(), recordStore1.getRecordStoreState().getStoreHeader().getMetaDataversion());

            // Update the meta-data in the second transaction
            FDBRecordStore recordStore2 = storeBuilder.copyBuilder()
                    .setContext(context2)
                    .setMetaDataProvider(metaData2)
                    .open();
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context2.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);

            // Trying to load with the new meta-data should succeed
            FDBRecordStore recordStore = storeBuilder.copyBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metaData2)
                    .open();
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
        }
    }

    /**
     * Validate that the store existence check is still performed on the cached store info.
     */
    @ParameterizedTest(name = "existenceCheckOnCachedStoreStates (test context = {0})")
    @MethodSource("testContextSource")
    public void existenceCheckOnCachedStoreStates(@Nonnull FDBRecordStoreStateCacheTestUtils.StateCacheTestContext testContext) throws Exception {
        fdb.setStoreStateCache(testContext.getCache(fdb));

        // Create a record store
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
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
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);

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
     * Verify that updating a header user field will be updated if the store state is cached.
     */
    @ParameterizedTest(name = "cacheUserFields (test context = {0})")
    @MethodSource("testContextSource")
    public void cacheUserFields(@Nonnull FDBRecordStoreStateCacheTestUtils.StateCacheTestContext testContext) throws Exception {
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
    public void cacheTwoSubspaces(@Nonnull FDBRecordStoreStateCacheTestUtils.StateCacheTestContext testContext) throws Exception {
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
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertTrue(store1.getIndexState("MySimpleRecord$str_value_indexed").isWriteOnly());
            assertTrue(store1.getIndexState("MySimpleRecord$num_value_3_indexed").isReadable());
            FDBRecordStore store2 = storeBuilder2.setContext(context).open();
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(context.getTimer(), 1);
            assertTrue(store2.getIndexState("MySimpleRecord$str_value_indexed").isReadable());
            assertTrue(store2.getIndexState("MySimpleRecord$num_value_3_indexed").isDisabled());

            readVersion = context.getReadVersion();
        }

        // Open both paths. Now they are both cached.
        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            context.setReadVersion(readVersion);
            FDBRecordStore store1 = storeBuilder1.setContext(context).open();
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 1);
            assertTrue(store1.getIndexState("MySimpleRecord$str_value_indexed").isWriteOnly());
            assertTrue(store1.getIndexState("MySimpleRecord$num_value_3_indexed").isReadable());
            FDBRecordStore store2 = storeBuilder2.setContext(context).open();
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(context.getTimer(), 2);
            assertTrue(store2.getIndexState("MySimpleRecord$str_value_indexed").isReadable());
            assertTrue(store2.getIndexState("MySimpleRecord$num_value_3_indexed").isDisabled());
        }
    }

    /**
     * Validate that caching just naturally works.
     */
    @ParameterizedTest(name = "cacheWithVersionTracking (test context = {0})")
    @MethodSource("testContextSource")
    public void cacheWithVersionTracking(@Nonnull FDBRecordStoreStateCacheTestUtils.StateCacheTestContext testContext) throws Exception {
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
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(timer, 1);
            recordStore.markIndexDisabled("MySimpleRecord$str_value_indexed").get();
            commit(context);
            commitVersion = context.getCommittedVersion();
        }

        // Version caching will elect to use the commit version, which is not cached
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            assertEquals(commitVersion, context.getReadVersion());
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheMiss(timer, 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isDisabled());
            commit(context); // should be read only-so won't change commit version
        }

        // Version caching will still use the commit version from the first (non read-only commit), but now it is in cache
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            assertEquals(commitVersion, context.getReadVersion());
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(timer, 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isDisabled());

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
            if (testContext instanceof FDBRecordStoreStateCacheTestUtils.ReadVersionStateCacheTestContext) {
                FDBRecordStoreStateCacheTestUtils.assertCacheMiss(timer, 1);
            } else {
                FDBRecordStoreStateCacheTestUtils.assertCacheHit(timer, 1);
            }
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isDisabled());

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
            if (testContext instanceof FDBRecordStoreStateCacheTestUtils.ReadVersionStateCacheTestContext) {
                FDBRecordStoreStateCacheTestUtils.assertCacheMiss(timer, 1);
            } else {
                FDBRecordStoreStateCacheTestUtils.assertCacheHit(timer, 1);
            }
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isDisabled());
        }

        // Load the meta-data using the cached read version.
        timer.reset();
        try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
            assertEquals(readVersion, context.getReadVersion());
            openSimpleRecordStore(context);
            FDBRecordStoreStateCacheTestUtils.assertCacheHit(timer, 1);
            assertTrue(recordStore.getIndexState("MySimpleRecord$str_value_indexed").isDisabled());
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

        ensureMetaDataVersionStampInitialized();

        // Create the store, initially not cacheable
        FDBStoreTimer timer = new FDBStoreTimer();
        byte[] metaDataVersionStamp1;
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertNotCacheable();
            metaDataVersionStamp1 = context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT);
            commit(context);
        }
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 1, 0);

        // Open the store, this time changing to make cacheable
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE_IF_NEW);
            assertNotCacheable();
            commit(context);
        }
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 1, 0);

        // Open the store, this time changing to make cacheable
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.CACHEABLE);
            assertCacheable();
            commit(context);
        }
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 1, 0);

        // Open the store, again with DEFAULT behavior. It should now be marked cacheable
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertCacheable();
            commit(context);
        }
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 1, 0);

        // Turn off caching during check version. The actual opening should be a hit, but the next one
        // should miss as it turns off state cacheability
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp1);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.NOT_CACHEABLE);
            assertNotCacheable();
            commit(context);
        }
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 0, 1);

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
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 1, 0);

        // Open again. This time, using NOT_CACHEABLE should not induce any changes (including to the meta-data versionstamp)
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertArrayEquals(context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT), metaDataVersionStamp2);
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.NOT_CACHEABLE);
            assertNotCacheable();
            commit(context);
        }
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 1, 0);
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
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 1, 0);

        // Open the store a third time, this time using the cache
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            openSimpleStoreWithCacheabilityOnOpen(context, FDBRecordStore.StateCacheabilityOnOpen.DEFAULT);
            assertCacheable();
        }
        FDBRecordStoreStateCacheTestUtils.assertCacheMissAndHitAndReset(timer, 0, 1);

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
        FDBDatabaseFactory.instance().setStoreStateCacheFactory(FDBRecordStoreStateCacheTestUtils.readVersionCacheFactory);
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
        fdb.setStoreStateCache(FDBRecordStoreStateCacheTestUtils.metaDataVersionStampCacheFactory.getCache(fdb));

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

    /**
     * Bootstrap that guarantees the cluster-wide meta-data version stamp key exists, so callers
     * can compare before/after snapshots without special-casing null.
     */
    private void ensureMetaDataVersionStampInitialized() {
        try (FDBRecordContext context = fdb.openContext()) {
            if (context.getMetaDataVersionStamp(IsolationLevel.SNAPSHOT) == null) {
                context.setMetaDataVersionStamp();
            }
            commit(context);
        }
    }

}
