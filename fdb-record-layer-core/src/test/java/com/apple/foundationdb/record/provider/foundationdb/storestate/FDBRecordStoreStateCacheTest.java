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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreAlreadyExistsException;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreDoesNotExistException;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreNoInfoAndNotEmptyException;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreStaleMetaDataVersionException;
import com.apple.foundationdb.record.provider.foundationdb.TestKeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests to make sure that caching {@link FDBRecordStoreStateCacheEntry} objects work.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreStateCacheTest extends FDBRecordStoreTestBase {
    @Nonnull
    private static final ReadVersionRecordStoreStateCacheFactory readVersionCacheFactory = ReadVersionRecordStoreStateCacheFactory.newInstance();
    @Nonnull
    private KeySpacePath multiStorePath = TestKeySpace.getKeyspacePath(new Object[]{"record-test", "unit", "multiRecordStore"});

    /**
     * Validate that caching by read version works.
     */
    @Test
    public void cacheByReadVersion() throws Exception {
        FDBRecordStoreStateCache origStoreStateCache = fdb.getStoreStateCache();
        try {
            fdb.setStoreStateCache(readVersionCacheFactory.getCache(fdb));
            long readVersion;
            int metaDataVersion;

            // Open a record store but do not commit to make sure that the updated value is not cached
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                assertTrue(context.hasDirtyStoreState());
                readVersion = context.ensureActive().getReadVersion().get();
                metaDataVersion = recordStore.getRecordMetaData().getVersion();
                // do not commit
            }

            // Open a record store and validate that the cached state is updated
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
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
                readVersion = context.ensureActive().getReadVersion().get();
                // does not matter whether we commit or not
            }

            try (FDBRecordContext context = openContext()) {
                context.ensureActive().setReadVersion(readVersion);
                openSimpleRecordStore(context);
                assertFalse(context.hasDirtyStoreState());
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertEquals(metaDataVersion, recordStore.getRecordMetaData().getVersion());
            }

            // Make a change to the stored info
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
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
                context.ensureActive().setReadVersion(readVersion);
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
                long newReadVersion = context.ensureActive().getReadVersion().get();
                assertThat(newReadVersion, greaterThan(readVersion));
                readVersion = newReadVersion;
                assertFalse(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));
            }

            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
                openSimpleRecordStore(context);
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertFalse(recordStore.isIndexReadable("MySimpleRecord$str_value_indexed"));
            }
        } finally {
            fdb.setStoreStateCache(origStoreStateCache);
        }
    }

    /**
     * Make sure that if one transaction changes the store header then an open store in another transaction that
     * loaded the store state from cache will fail at commit time with conflict.
     */
    @Test
    public void readVersionConflictWhenCachedChanged() throws Exception {
        FDBRecordStoreStateCache origStoreStateCache = fdb.getStoreStateCache();
        try {
            fdb.setStoreStateCache(readVersionCacheFactory.getCache(fdb));

            RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");
            RecordMetaData metaData2 = metaDataBuilder.getRecordMetaData();
            assertThat(metaData1.getVersion(), lessThan(metaData2.getVersion()));

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
            }

            // Load the record store state into the cache.
            long readVersion;
            try (FDBRecordContext context = openContext()) {
                readVersion = context.ensureActive().getReadVersion().get();
                FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                        .setContext(context)
                        .setMetaDataProvider(metaData1)
                        .setKeySpacePath(path)
                        .open();
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                assertEquals(metaData1.getVersion(), recordStore.getRecordMetaData().getVersion());
                assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            }

            // Use the cache to load the record store states
            try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
                context1.setTimer(new FDBStoreTimer());
                context1.ensureActive().setReadVersion(readVersion);
                context2.setTimer(new FDBStoreTimer());
                context2.ensureActive().setReadVersion(readVersion);

                FDBRecordStore recordStore1 = FDBRecordStore.newBuilder()
                        .setContext(context1)
                        .setMetaDataProvider(metaData1)
                        .setKeySpacePath(path)
                        .open();
                assertEquals(1, context1.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertEquals(metaData1.getVersion(), recordStore1.getRecordMetaData().getVersion());
                assertEquals(metaData1.getVersion(), recordStore1.getRecordStoreState().getStoreHeader().getMetaDataversion());

                // Update the meta-data in the second transaction
                FDBRecordStore recordStore2 = FDBRecordStore.newBuilder()
                        .setContext(context2)
                        .setMetaDataProvider(metaData2)
                        .setKeySpacePath(path)
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
                assertThrows(RecordStoreStaleMetaDataVersionException.class, () -> FDBRecordStore.newBuilder()
                        .setContext(context)
                        .setMetaDataProvider(metaData1)
                        .setKeySpacePath(path)
                        .open());
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));

                // Trying to load with the new meta-data should succeed
                FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                        .setContext(context)
                        .setMetaDataProvider(metaData2)
                        .setKeySpacePath(path)
                        .open();
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            }

        } finally {
            fdb.setStoreStateCache(origStoreStateCache);
        }
    }

    /**
     * Validate that the store existence check is still performed on the cached store info.
     */
    @Test
    public void existenceCheckOnCachedStoreStates() throws Exception {
        FDBRecordStoreStateCache origStoreStateCache = fdb.getStoreStateCache();
        try {
            fdb.setStoreStateCache(readVersionCacheFactory.getCache(fdb));

            // Create a record store
            long readVersion;
            FDBRecordStore.Builder storeBuilder;
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                readVersion = context.ensureActive().getReadVersion().get();
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                assertTrue(context.hasDirtyStoreState());
                storeBuilder = recordStore.asBuilder();
                commit(context);
            }

            try (FDBRecordContext context = openContext()) {
                context.ensureActive().setReadVersion(readVersion);
                storeBuilder.setContext(context);
                assertThrows(RecordStoreDoesNotExistException.class, storeBuilder::open);
            }

            // Load the record store state into the cache
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                openSimpleRecordStore(context);
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                readVersion = context.ensureActive().getReadVersion().get();
                storeBuilder = recordStore.asBuilder();
            }

            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
                storeBuilder.setContext(context);
                assertThrows(RecordStoreAlreadyExistsException.class, storeBuilder::create);
                FDBRecordStore store = storeBuilder.open();
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));

                // Delete the store header
                context.ensureActive().clear(store.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key()));
                commit(context);
            }

            // Load the record store state into cache
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                storeBuilder.setContext(context);
                assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
                storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                readVersion = context.ensureActive().getReadVersion().get();
            }

            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
                storeBuilder.setContext(context);
                assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
                storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            }
        } finally {
            fdb.setStoreStateCache(origStoreStateCache);
        }
    }

    /**
     * Validate that deleting a record store causes the record store to go back to the database as it's possible the
     * cached stuff is what was deleted.
     */
    @Test
    public void storeDeletionInSameContext() throws Exception {
        FDBRecordStoreStateCache storeStateCache = fdb.getStoreStateCache();
        try {
            fdb.setStoreStateCache(readVersionCacheFactory.getCache(fdb));

            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                commit(context);
            }

            // Load the record store state into the cache
            long readVersion;
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                openSimpleRecordStore(context);
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                readVersion = context.ensureActive().getReadVersion().get();
            }

            try (FDBRecordContext context = openContext()) {
                context.ensureActive().setReadVersion(readVersion);
                openSimpleRecordStore(context);
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));

                context.getTimer().reset();
                FDBRecordStore.deleteStore(context, recordStore.getSubspace());
                recordStore.asBuilder().create();
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));

                commit(context);
            }

            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                openSimpleRecordStore(context);
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                readVersion = context.ensureActive().getReadVersion().get();
            }

            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
                openSimpleRecordStore(context);
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                path.deleteAllData(context);

                context.getTimer().reset();
                recordStore.asBuilder().create();
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            }
        } finally {
            fdb.setStoreStateCache(storeStateCache);
        }
    }

    /**
     * Make sure that caching two different subspaces are both cached but with separate entries.
     */
    @Test
    public void cacheTwoSubspacesByReadVersion() throws Exception {
        FDBRecordStoreStateCache origStoreStateCache = fdb.getStoreStateCache();
        try {
            fdb.setStoreStateCache(readVersionCacheFactory.getCache(fdb));
            final KeySpacePath path1 = multiStorePath.add("storePath", "path1");
            final KeySpacePath path2 = multiStorePath.add("storePath", "path2");

            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                path1.deleteAllData(context);
                path2.deleteAllData(context);

                openSimpleRecordStore(context);
                FDBRecordStore store1 = recordStore.asBuilder().setKeySpacePath(path1).create();
                store1.markIndexWriteOnly("MySimpleRecord$str_value_indexed").get();
                FDBRecordStore store2 = recordStore.asBuilder().setKeySpacePath(path2).create();
                store2.markIndexDisabled("MySimpleRecord$num_value_3_indexed").get();

                commit(context);
            }

            // Get a read version and load one of the record store's info into cache
            long readVersion;
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                openSimpleRecordStore(context);
                readVersion = context.ensureActive().getReadVersion().get();
                FDBRecordStore store1 = recordStore.asBuilder().setKeySpacePath(path1).open();
                assertTrue(store1.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
                assertTrue(store1.isIndexReadable("MySimpleRecord$num_value_3_indexed"));
                assertEquals(2, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            }

            // Open both paths. Only the one in path1 should be cached.
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
                openSimpleRecordStore(context);
                FDBRecordStore store1 = recordStore.asBuilder().setKeySpacePath(path1).open();
                assertEquals(2, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertTrue(store1.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
                assertTrue(store1.isIndexReadable("MySimpleRecord$num_value_3_indexed"));
                FDBRecordStore store2 = recordStore.asBuilder().setKeySpacePath(path2).open();
                assertEquals(1, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                assertTrue(store2.isIndexReadable("MySimpleRecord$str_value_indexed"));
                assertTrue(store2.isIndexDisabled("MySimpleRecord$num_value_3_indexed"));
            }

            // Open both paths. Now they are both cached.
            try (FDBRecordContext context = openContext()) {
                context.getTimer().reset();
                context.ensureActive().setReadVersion(readVersion);
                openSimpleRecordStore(context);
                FDBRecordStore store1 = recordStore.asBuilder().setKeySpacePath(path1).open();
                assertEquals(2, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertTrue(store1.isIndexWriteOnly("MySimpleRecord$str_value_indexed"));
                assertTrue(store1.isIndexReadable("MySimpleRecord$num_value_3_indexed"));
                FDBRecordStore store2 = recordStore.asBuilder().setKeySpacePath(path2).open();
                assertEquals(3, context.getTimer().getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertTrue(store2.isIndexReadable("MySimpleRecord$str_value_indexed"));
                assertTrue(store2.isIndexDisabled("MySimpleRecord$num_value_3_indexed"));
            }
        } finally {
            fdb.setStoreStateCache(origStoreStateCache);
        }
    }

    /**
     * Validate that caching just naturally works.
     */
    @Test
    public void cacheByReadWithVersionTracking() throws Exception {
        boolean trackCommitVersions = fdb.isTrackLastSeenVersionOnCommit();
        boolean trackReadVersion = fdb.isTrackLastSeenVersionOnCommit();
        FDBRecordStoreStateCache currentCache = fdb.getStoreStateCache();
        try {
            fdb.setStoreStateCache(readVersionCacheFactory.getCache(fdb));
            fdb.setTrackLastSeenVersion(true);
            FDBStoreTimer timer = new FDBStoreTimer();
            final FDBDatabase.WeakReadSemantics readSemantics = new FDBDatabase.WeakReadSemantics(0L, 5000, false);

            // Load up a read version
            try (FDBRecordContext context = fdb.openContext(null, timer, null)) {
                fdb.getReadVersion(context).join();
                commit(context);
            }

            // Commit a new meta-data
            final long commitVersion;
            timer.reset();
            try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
                openSimpleRecordStore(context);
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                recordStore.markIndexDisabled("MySimpleRecord$str_value_indexed").get();
                commit(context);
                commitVersion = context.getCommittedVersion();
            }

            // Version caching will elect to use the commit version, which is not cached
            timer.reset();
            try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
                assertEquals(commitVersion, (long)context.ensureActive().getReadVersion().get());
                openSimpleRecordStore(context);
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));
                commit(context); // should be read only-so won't change commit version
            }

            // Version caching will still use the commit version, but now it is in cache
            timer.reset();
            try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
                assertEquals(commitVersion, (long)context.ensureActive().getReadVersion().get());
                openSimpleRecordStore(context);
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));
                commit(context);
            }

            // Make a (no-op) commit to ensure that the db version is increased.
            try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
                openSimpleRecordStore(context);
                context.ensureActive().addWriteConflictKey(recordStore.recordsSubspace().pack(UUID.randomUUID()));
                commit(context);
            }

            // Load a new read version
            timer.reset();
            final long readVersion;
            try (FDBRecordContext context = fdb.openContext(null, timer, null)) {
                readVersion = fdb.getReadVersion(context).get();
                openSimpleRecordStore(context);
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
                assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));
            }

            // Load the meta-data using the cached read version
            timer.reset();
            try (FDBRecordContext context = fdb.openContext(null, timer, readSemantics)) {
                assertEquals(readVersion, (long)context.ensureActive().getReadVersion().get());
                openSimpleRecordStore(context);
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
                assertTrue(recordStore.isIndexDisabled("MySimpleRecord$str_value_indexed"));
            }
        } finally {
            fdb.setTrackLastSeenVersionOnCommit(trackCommitVersions);
            fdb.setTrackLastSeenVersionOnRead(trackReadVersion);
            fdb.setStoreStateCache(currentCache);
        }
    }

    @Test
    public void useWithDifferentDatabase() throws Exception {
        FDBRecordStoreStateCacheFactory currentCacheFactory = FDBDatabaseFactory.instance().getStoreStateCacheFactory();
        try {
            File clusterFile = File.createTempFile("record_store_cache_", ".cluster");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(clusterFile))) {
                writer.write("fake:fdbcluster@127.0.0.1:65537\n");
            }
            FDBDatabaseFactory.instance().setStoreStateCacheFactory(readVersionCacheFactory);
            FDBDatabase secondDatabase = FDBDatabaseFactory.instance().getDatabase(clusterFile.getAbsolutePath());

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
        } finally {
            FDBDatabaseFactory.instance().setStoreStateCacheFactory(currentCacheFactory);
        }
    }
}
