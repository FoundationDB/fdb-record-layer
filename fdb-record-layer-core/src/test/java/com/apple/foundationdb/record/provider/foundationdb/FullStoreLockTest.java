/*
 * FullStoreLockTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.StoreIsFullyLockedException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.UnknownStoreLockStateException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.provider.foundationdb.storestate.MetaDataVersionStampStoreStateCacheFactory;
import com.apple.test.Tags;
import com.google.common.collect.Comparators;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(Tags.RequiresFDB)
class FullStoreLockTest extends FDBRecordStoreTestBase {

    FormatVersion formatVersion = FormatVersion.getMaximumSupportedVersion();

    @BeforeEach
    void beforeEach() {
        this.formatVersion = Comparators.max(FormatVersion.FULL_STORE_LOCK, this.formatVersion);
    }

    @Test
    void testFullStoreLockPreventsOpen() {
        // Create a store and set FULL_STORE lock, then verify it cannot be opened
        withStore(getStoreBuilder(), store -> {
            saveSomeRecords(store, 10);
            setFullStoreLock(store, "Testing full store lock");
        });

        // Attempt to open the store - should fail
        assertCannotOpen();
    }

    @Test
    void testUnspecifiedLockStateFailsToOpen() {
        // With FULL_STORE_LOCK format version, UNSPECIFIED state should prevent opening
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder().setContext(context).create();

            // Explicitly set UNSPECIFIED
            recordStore.setStoreLockStateAsync(
                    RecordMetaDataProto.DataStoreInfo.StoreLockState.State.UNSPECIFIED,
                    "Explicitly setting unspecified"
            ).join();

            commit(context);
        }

        // Attempt to open - should fail with UnknownStoreLockStateException
        try (FDBRecordContext context = openContext()) {
            assertThrows(UnknownStoreLockStateException.class,
                    () -> getStoreBuilder().setContext(context).open());
        }
    }

    @Test
    void testUnknownLockStateFailsToOpen() throws InvalidProtocolBufferException {
        try (FDBRecordContext context = openContext()) {
            recordStore = openSimpleRecordStore(context, null, formatVersion);
            final byte[] storeInfoKey = recordStore.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.id());
            final int lockStateFieldNumber = RecordMetaDataProto.DataStoreInfo.StoreLockState.getDescriptor()
                    .findFieldByName("lock_state").getNumber();
            final UnknownFieldSet.Field unexpectedEnumValue = UnknownFieldSet.Field.newBuilder().addVarint(10345).build();
            final byte[] newStoreInfo = RecordMetaDataProto.DataStoreInfo.parseFrom(
                            context.ensureActive().get(storeInfoKey).join())
                    .toBuilder()
                    .setStoreLockState(
                            RecordMetaDataProto.DataStoreInfo.StoreLockState.newBuilder()
                                    .setReason("Future locking")
                                    .setTimestamp(System.currentTimeMillis())
                                    .setUnknownFields(UnknownFieldSet.newBuilder()
                                            .addField(lockStateFieldNumber, unexpectedEnumValue).build()))
                    .build().toByteArray();
            context.ensureActive().set(storeInfoKey, newStoreInfo);
            commit(context);
        }

        // Attempt to open - should fail with UnknownStoreLockStateException
        try (FDBRecordContext context = openContext()) {
            assertThrows(UnknownStoreLockStateException.class,
                    () -> openSimpleRecordStore(context, null, formatVersion));
        }
    }

    @Test
    void testClearFullStoreLock() {
        // we also test getBypassFullStoreLockReason on the builder in this test
        final String lockReason = "Testing lock clearing";

        // Test setting FULL_STORE lock
        withStore(getStoreBuilder(), store -> setFullStoreLock(store, lockReason));

        // Verify cannot open normally
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder().setContext(context);
            assertNull(builder.getBypassFullStoreLockReason());
            assertThrows(StoreIsFullyLockedException.class, builder::open);
        }

        // Open with bypass to clear the lock
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder().setContext(context)
                    .setBypassFullStoreLockReason(lockReason);
            assertEquals(lockReason, builder.getBypassFullStoreLockReason());
            recordStore = builder.open();

            // Clear the lock
            recordStore.clearStoreLockStateAsync().join();

            commit(context);
        }

        // Verify can now open normally
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder().setContext(context).open();
            commit(context);
        }
    }

    @Test
    void testBypassLockBypassesCache() throws Exception {
        final String lockReason = "Testing cache bypass";

        // Create a meta-data version stamp cache for this test
        FDBRecordStoreStateCache cache = MetaDataVersionStampStoreStateCacheFactory.newInstance()
                .getCache(fdb);

        enableStoreStateCache(cache);

        // Now set the lock
        final FDBRecordStore.Builder builder = getStoreBuilder().setStoreStateCache(cache);
        try (FDBRecordContext context = openContext()) {
            recordStore = builder.copyBuilder().setContext(context).open();

            setFullStoreLock(recordStore, lockReason);

            commit(context);
        }

        // Verify normal open fails
        assertCannotOpen(builder.copyBuilder());

        // Open with bypass - should bypass the cache and succeed
        timer.reset();
        try (FDBRecordContext context = openContext()) {
            recordStore = builder.copyBuilder()
                    .setContext(context)
                    .setBypassFullStoreLockReason(lockReason)
                    .open();

            // Verify we did NOT hit the cache (bypassed it)
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));

            // Clear the lock and re-enable cacheability
            recordStore.clearStoreLockStateAsync().join();
            commit(context);
        }

        // Verify can now open normally (expect cache miss after header change)
        timer.reset();
        try (FDBRecordContext context = openContext()) {
            recordStore = builder.copyBuilder()
                    .setContext(context).open();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
        }

        // Now we should get a cache hit on the next open
        timer.reset();
        try (FDBRecordContext context = openContext()) {
            recordStore = builder.copyBuilder()
                    .setContext(context).open();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            commit(context);
        }
    }

    private void enableStoreStateCache(final FDBRecordStoreStateCache cache) throws InterruptedException, ExecutionException {
        // Create store with cacheability enabled (no lock yet)
        try (FDBRecordContext context = openContext()) {
            context.setMetaDataVersionStamp();
            recordStore = getStoreBuilder()
                    .setStoreStateCache(cache)
                    .setContext(context).createOrOpen();
            recordStore.setStateCacheabilityAsync(true).get();
            commit(context);
        }

        // Prime the cache by opening once
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder()
                    .setStoreStateCache(cache)
                    .setContext(context).open();
        }
    }

    @Test
    void testFullStoreLockWithIndexRebuild() {
        final String lockReason = "Performing index maintenance";
        final Index newIndex = new Index("newIndex", field("num_value_2"));

        // Create a store with a new index to rebuild
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", newIndex);

        final RecordMetaData metaData = simpleMetaData(hook);

        // Populate some data
        withStore(metaData, store -> saveSomeRecords(store, 20));

        // Verify index is readable before we start
        assertIndexState(getStoreBuilder(metaData), IndexState.READABLE, newIndex);

        // Lock the store for maintenance
        withStore(metaData, store -> setFullStoreLock(store, lockReason));

        // Verify normal open fails
        assertCannotOpen(getStoreBuilder(metaData));

        final FDBRecordStore.Builder storeBuilderWithLockBypass = getStoreBuilder(metaData)
                .setBypassFullStoreLockReason(lockReason);
        // Use bypass to mark the index as disabled
        withStore(storeBuilderWithLockBypass, store -> store.markIndexDisabled(newIndex).join());

        // Verify index is now disabled (using bypass)
        assertIndexState(storeBuilderWithLockBypass, IndexState.DISABLED, newIndex);

        // Rebuild the index using OnlineIndexer with bypass
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setRecordStoreBuilder(storeBuilderWithLockBypass)
                .setIndex(newIndex)
                .build()) {
            indexBuilder.buildIndex();
        }

        // Verify index is now readable (using bypass)
        assertIndexState(storeBuilderWithLockBypass, IndexState.READABLE, newIndex);

        // Unlock the store
        withStore(storeBuilderWithLockBypass, store -> store.clearStoreLockStateAsync().join());

        // Verify can now open normally and index is still readable
        withStore(metaData, store -> {
            assertEquals(IndexState.READABLE, store.getRecordStoreState().getState(newIndex.getName()));

            // Verify we can query the index, we don't really care about the results
            try (RecordCursor<IndexEntry> cursor = store.scanIndex(newIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                cursor.asList().join();
            }
        });
    }

    private void assertIndexState(final FDBRecordStore.Builder storeBuilderWithLockBypass, final IndexState disabled, final Index newIndex) {
        withStore(storeBuilderWithLockBypass,
                store -> assertEquals(disabled, store.getRecordStoreState().getState(newIndex.getName())));
    }

    private void withStore(@Nonnull final RecordMetaData metaData, @Nonnull final Consumer<FDBRecordStore> action) {
        withStore(getStoreBuilder(metaData), action);
    }

    private void withStore(@Nonnull final FDBRecordStore.Builder storeBuilder, @Nonnull final Consumer<FDBRecordStore> action) {
        try (FDBRecordContext context = openContext()) {
            action.accept(storeBuilder.setContext(context).createOrOpen());
            commit(context);
        }
    }

    private void assertCannotOpen() {
        assertCannotOpen(getStoreBuilder());
    }

    private void assertCannotOpen(@Nonnull final FDBRecordStore.Builder storeBuilder) {
        try (FDBRecordContext context = openContext()) {
            StoreIsFullyLockedException exception = assertThrows(StoreIsFullyLockedException.class,
                    () -> storeBuilder.setContext(context).open());
            assertNotNull(exception.getMessage());
        }
    }

    private static void saveSomeRecords(@Nonnull final FDBRecordStore recordStore, final int count) {
        for (int i = 0; i < count; i++) {
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(i)
                    .setNumValue2(i * 100)
                    .build());
        }
    }

    private static void setFullStoreLock(@Nonnull final FDBRecordStore store, @Nonnull final String lockReason) {
        store.setStoreLockStateAsync(
                RecordMetaDataProto.DataStoreInfo.StoreLockState.State.FULL_STORE,
                lockReason
        ).join();
    }

    @Nonnull
    private FDBRecordStore.Builder getStoreBuilder() {
        return getStoreBuilder(simpleMetaData(null));
    }

    @Nonnull
    private FDBRecordStore.Builder getStoreBuilder(@Nonnull final RecordMetaData metadata) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(formatVersion)
                .setKeySpacePath(path)
                .setMetaDataProvider(metadata);
    }
}
