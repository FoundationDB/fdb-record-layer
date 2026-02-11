/*
 * FDBRecordStoreRepairHeaderTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.StoreIsFullyLockedException;
import com.apple.foundationdb.record.StoreIsLockedForRecordUpdates;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.provider.foundationdb.storestate.MetaDataVersionStampStoreStateCacheFactory;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(Tags.RequiresFDB)
// These update the MetaDataVersion in the database, which is global state, and will play poorly with other
// tests that depend on that, specifically tests of the caching. If we ever start running tests in parallel against
// the same FDB we may want to consider creating a specific ResourceLock for the MetaDataVersion.
@Isolated
public class FDBRecordStoreRepairHeaderTest extends FDBRecordStoreConcurrentTestBase {

    static final String REPAIR_REASON = "Repair Reason";

    @Nonnull
    protected final KeySpacePath path;

    public FDBRecordStoreRepairHeaderTest() {
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    static Stream<FormatVersion> formatVersions() {
        return Arrays.stream(FormatVersion.values());
    }

    @Test
    void checkMaxFormatVersion() {
        assertThat(FormatVersion.getMaximumSupportedVersion())
                .as("The behavior of repairMissingHeader needs to be validated with the new format version")
                // This should only fail when a new format version is added. If this test fails, the developer should
                // make any modifications to repairMissingHeader to accommodate the new version.
                // If this new format version is adding additional items to the store header, make sure to update the
                // comments as to how it is being reset in the repair, even if it is not. Then update this
                // to the new value
                .isEqualTo(FormatVersion.FULL_STORE_LOCK);
    }

    static Stream<NonnullPair<FormatVersion, FormatVersion>> oldAndNewVersion() {
        return formatVersions()
                .flatMap(oldVersion -> formatVersions()
                        .filter(newVersion -> newVersion.isAtLeast(oldVersion))
                        .map(newVersion -> NonnullPair.of(oldVersion, newVersion)));
    }


    static Stream<DisableIndexBehavior> disableIndexBehaviors(FormatVersion newVersion) {
        if (newVersion.isAtLeast(FormatVersion.FULL_STORE_LOCK)) {
            return Arrays.stream(DisableIndexBehavior.values());
        } else {
            return Stream.of(DisableIndexBehavior.ExplicitlyDisableThem, DisableIndexBehavior.ImplicitlyDisableThem);
        }
    }

    static Stream<Arguments> repairUpgradeFormatVersion() {
        return oldAndNewVersion()
                .flatMap(oldAndNewVersion -> ParameterizedTestUtils.booleans("supportSplitRecords")
                        .flatMap(supportSplitRecords ->
                                disableIndexBehaviors(oldAndNewVersion.getRight())
                                        .map(disableIndexBehavior ->
                                                Arguments.of(oldAndNewVersion.getLeft(), oldAndNewVersion.getRight(),
                                                        supportSplitRecords, disableIndexBehavior))));
    }

    @ParameterizedTest
    @MethodSource
    void repairUpgradeFormatVersion(FormatVersion oldFormatVersion, FormatVersion newFormatVersion,
                                    boolean supportSplitRecords, DisableIndexBehavior disableIndexBehavior) {
        final RecordMetaData recordMetaData = getRecordMetaData(supportSplitRecords);
        final List<Tuple> primaryKeys = createInitialStore(oldFormatVersion, recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = loadOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(newFormatVersion);
            FDBRecordStore recordStore;
            if (oldFormatVersion.isAtLeast(FormatVersion.SAVE_VERSION_WITH_RECORD)) {
                recordStore = disableIndexBehavior.repair(context, builder, 1, FormatVersion.SAVE_VERSION_WITH_RECORD);
            } else if (!newFormatVersion.isAtLeast(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX)) {
                recordStore = disableIndexBehavior.repair(context, builder, 1, FormatVersion.INFO_ADDED);
            } else {
                assertThatThrownBy(() -> disableIndexBehavior.repair(context, builder, 1, oldFormatVersion))
                        .isInstanceOf(RecordCoreException.class);
                return; // nothing left to test
            }

            commitRepair(originalRecords, recordStore);
        }

        validateRepaired(newFormatVersion, recordMetaData, originalRecords, disableIndexBehavior);
    }

    static Stream<Arguments> repairUserVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(1, 3),
                Stream.of(DisableIndexBehavior.values()));
    }

    @ParameterizedTest
    @MethodSource
    void repairUserVersion(int userVersion, DisableIndexBehavior disableIndexBehavior) {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = loadOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final FDBRecordStoreBase.UserVersionChecker userVersionChecker = new AssertMatchingUserVersion(userVersion, userVersion);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = disableIndexBehavior.repair(context,
                    getStoreBuilder(context, recordMetaData)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                            .setUserVersionChecker(userVersionChecker),
                    userVersion,
                    FormatVersion.SAVE_VERSION_WITH_RECORD);

            commitRepair(originalRecords, recordStore);
        }

        validateRepaired(userVersion, recordMetaData, userVersionChecker, originalRecords, disableIndexBehavior);
    }

    @Test
    void repairMetaDataVersion() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "ToRemove1", "num_value_2");
        metaDataBuilder.addIndex("MySimpleRecord", "ToRemove2", "num_value_unique");
        RecordMetaData metadata1 = metaDataBuilder.build();
        metaDataBuilder.removeIndex("ToRemove1");
        metaDataBuilder.removeIndex("ToRemove2");
        final RecordMetaData metadata2 = metaDataBuilder.build();


        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), metadata1);
        final List<FDBStoredRecord<Message>> originalRecords = loadOriginalRecords(metadata1, primaryKeys);

        clearStoreHeader(metadata1);
        validateCannotOpen(metadata1);

        final DisableIndexBehavior disableIndexBehavior = DisableIndexBehavior.ExplicitlyDisableThem;
        try (FDBRecordContext context = openContext()) {
            disableIndexBehavior.repair(context,
                    getStoreBuilder(context, metadata2)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                            .setUserVersionChecker(new AssertMatchingMetaDataVersion(metadata2)),
                    1,
                    FormatVersion.SAVE_VERSION_WITH_RECORD);
            commit(context);
        }
        validateRepaired(1, metadata2, new AssertMatchingMetaDataVersion(metadata2), originalRecords, disableIndexBehavior);

    }

    @ParameterizedTest
    @EnumSource(DisableIndexBehavior.class)
    void needRebuildIndex(DisableIndexBehavior disableIndexBehavior) {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = loadOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final int userVersion = 2;
        final FDBRecordStoreBase.UserVersionChecker userVersionChecker = new AssertMatchingUserVersion(userVersion, userVersion) {
            @Nonnull
            @Override
            public CompletableFuture<IndexState> needRebuildIndex(final Index index, final Supplier<CompletableFuture<Long>> lazyRecordCount, final Supplier<CompletableFuture<Long>> lazyEstimatedSize, final boolean indexOnNewRecordTypes) {
                return Assertions.fail("needRebuildIndex should not be called");
            }

            @Override
            public IndexState needRebuildIndex(final Index index, final long recordCount, final boolean indexOnNewRecordTypes) {
                return Assertions.fail("needRebuildIndex should not be called");
            }
        };

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = disableIndexBehavior.repair(context,
                    getStoreBuilder(context, recordMetaData)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                            .setUserVersionChecker(userVersionChecker),
                    userVersion,
                    FormatVersion.SAVE_VERSION_WITH_RECORD);

            commitRepair(originalRecords, recordStore);
        }

        validateRepaired(userVersion, recordMetaData, userVersionChecker, originalRecords, disableIndexBehavior);
    }

    @Test
    void repairUserField() {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        loadOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final int userVersion = 2;
        final String key = "someState";
        final ByteString value = ByteString.copyFromUtf8("My value");
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = DisableIndexBehavior.ExplicitlyDisableThem.repair(context,
                    getStoreBuilder(context, recordMetaData)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion()),
                    userVersion,
                    FormatVersion.SAVE_VERSION_WITH_RECORD);
            recordStore.setHeaderUserField(key, value);
            commit(context);
        }

        withStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData,
                recordStore -> assertEquals(value, recordStore.getHeaderUserField(key)));
    }

    @Test
    void repairIncarnation() {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path).open();
            assertEquals(0, recordStore.getIncarnation());
            recordStore.updateIncarnation(current -> current + 100);
            commit(context);
        }
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final int userVersion = 2;
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = DisableIndexBehavior.ExplicitlyDisableThem.repair(context,
                    getStoreBuilder(context, recordMetaData)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion()),
                    userVersion,
                    FormatVersion.SAVE_VERSION_WITH_RECORD);
            assertEquals(0, recordStore.getIncarnation());
            recordStore.updateIncarnation(current -> current + 100);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                    .open();
            assertEquals(100, recordStore.getIncarnation());
            commit(context);
        }
    }

    @ParameterizedTest
    @BooleanSource("doLock")
    void repairWithRecordsAndSetRecordUpdateLock(boolean doLock) {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        loadOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final int userVersion = 2;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = DisableIndexBehavior.ExplicitlyDisableThem.repair(context,
                    getStoreBuilder(context, recordMetaData)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion()),
                    userVersion,
                    FormatVersion.SAVE_VERSION_WITH_RECORD);
            if (doLock) {
                recordStore.setStoreLockStateAsync(RecordMetaDataProto.DataStoreInfo.StoreLockState.State.FORBID_RECORD_UPDATE, "testing")
                        .join();
            }
            commit(context);
        }
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(200).setNumValue2(2100).build();
        withStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData,
                recordStore -> {
                    if (doLock) {
                        // assert update record failure, then release
                        assertThrows(StoreIsLockedForRecordUpdates.class, () -> recordStore.saveRecord(record));
                        recordStore.clearStoreLockStateAsync().join();
                    }
                    // Successfully update a records after either not setting or releasing the records updates lock
                    recordStore.saveRecord(record);
                });
    }

    @Test
    void repairCacheable() {
        // Generally speaking, if a store state is cacheable, you won't notice that the header is missing until the
        // cache is invalidated, however, it is possible that one instance is bounced, and see's that the header is
        // missing, while another store is not bounced, and thus is still interacting with the cache. Thus, we need
        // to make sure that the cache is invalidated globally when we update the store. Particularly this means that
        // we need to bump the MetaDataVersionStamp in the database to invalidate globally.
        // This does mean that if you have an environment that has a lot of stores that are definitely never cached,
        // and a couple that are, the repair would be causing invalidation of the store that is cacheable, but this
        // performance hit is worth correctness.
        final var metaDataVersionStampCacheFactory = MetaDataVersionStampStoreStateCacheFactory.newInstance();
        final FDBRecordStoreStateCache populatedCache = metaDataVersionStampCacheFactory.getCache(fdb);
        fdb.setStoreStateCache(populatedCache);
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        loadOriginalRecords(recordMetaData, primaryKeys);

        FDBRecordStoreBase.UserVersionChecker userVersionChecker = setupCachedStoreState(recordMetaData);

        clearStoreHeader(recordMetaData);

        // this will use the cache and not fail
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setUserVersionChecker(userVersionChecker).open();
            assertEquals(1, recordStore.getUserVersion());
        }

        // Now pretend we are on an instance that was not populated
        fdb.setStoreStateCache(metaDataVersionStampCacheFactory.getCache(fdb));
        validateCannotOpen(recordMetaData);


        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData)
                    // it should not call checkVersion, so just give random values
                    .setUserVersionChecker(new AssertMatchingUserVersion(3249, 234908));
            DisableIndexBehavior.ExplicitlyDisableThem.repair(context, builder, 3, FormatVersion.SAVE_VERSION_WITH_RECORD);
            commit(context);
        }

        // now go back to the original store which has a cached version of the store state
        // it should have to read from the database, and pick up the new store header
        fdb.setStoreStateCache(populatedCache);

        userVersionChecker = new AssertMatchingUserVersion(3, 3);
        timer.reset();
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setUserVersionChecker(userVersionChecker)
                    .open();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertEquals(3, recordStore.getUserVersion());
        }
    }

    @ParameterizedTest
    @BooleanSource("cached")
    void noRepairIfHeaderExists(boolean cached) {
        // We don't allow repair if it is in the database, or if it is cached.
        // In theory, we could use the cached value to inform what we fabricate, but the chance that you will discover
        // that it is missing from the DB, but still have it in cache is pretty slim.
        if (cached) {
            final var metaDataVersionStampCacheFactory = MetaDataVersionStampStoreStateCacheFactory.newInstance();
            final FDBRecordStoreStateCache populatedCache = metaDataVersionStampCacheFactory.getCache(fdb);
            fdb.setStoreStateCache(populatedCache);
        }
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        loadOriginalRecords(recordMetaData, primaryKeys);

        if (cached) {
            setupCachedStoreState(recordMetaData);
            clearStoreHeader(recordMetaData);
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion());
            final NonnullPair<Boolean, FDBRecordStore> result = context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION,
                    builder.repairMissingHeader(1, FormatVersion.SAVE_VERSION_WITH_RECORD, null));
            assertFalse(result.getLeft(), "Repairing header should have just opened it");
            commit(context);
        }
    }

    static Stream<Arguments> disableRecordCountKeyOnRepair() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("hasRecordCountKey"),
                Stream.of(FormatVersion.getMaximumSupportedVersion(),
                        FormatVersionTestUtils.previous(FormatVersion.RECORD_COUNT_STATE)),
                Stream.of(DisableIndexBehavior.values()));
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("deprecation")
    void disableRecordCountKeyOnRepair(boolean hasRecordCountKey, FormatVersion formatVersion,
                                       DisableIndexBehavior disableIndexBehavior) {
        if (!formatVersion.isAtLeast(FormatVersion.FULL_STORE_LOCK)) {
            Assumptions.assumeThat(disableIndexBehavior.leaveIndexes())
                    .isFalse();
        }
        // We cannot tell whether the recordCountKey had changed since the last time we did checkVersion, so
        // we can't guarantee that it is correct. If there is a RecordCountKey and we're on a new enough format version
        // we'll disable the record count key, otherwise, we'll throw an exception
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.setSplitLongRecords(true);
        if (hasRecordCountKey) {
            metaDataBuilder.setRecordCountKey(Key.Expressions.empty());
        }
        final RecordMetaData recordMetaData = metaDataBuilder.build();
        final List<Tuple> primaryKeys = createInitialStore(formatVersion, recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = loadOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final int userVersion = 2;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(formatVersion);
            if (hasRecordCountKey) {
                if (formatVersion.isAtLeast(FormatVersion.RECORD_COUNT_STATE)) {
                    FDBRecordStore recordStore = disableIndexBehavior.repair(context, storeBuilder, userVersion, formatVersion);
                    assertThat(recordStore.getRecordStoreState().getStoreHeader().getRecordCountState())
                            .isEqualTo(RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED);

                    commitRepair(originalRecords, recordStore);
                } else {
                    assertThatThrownBy(() -> disableIndexBehavior.repair(context, storeBuilder, userVersion, formatVersion))
                            .isInstanceOf(RecordCoreException.class);
                }
            } else {
                FDBRecordStore recordStore = disableIndexBehavior.repair(context, storeBuilder, userVersion, formatVersion);

                commitRepair(originalRecords, recordStore);
            }
        }

        if (hasRecordCountKey && !formatVersion.isAtLeast(FormatVersion.RECORD_COUNT_STATE)) {
            validateCannotOpen(recordMetaData);
        } else {
            validateRepaired(formatVersion, recordMetaData, originalRecords, disableIndexBehavior);
            if (hasRecordCountKey) {
                validateRecordCountKeyIsDisabled(formatVersion, recordMetaData);
            } else {
                withStore(formatVersion, recordMetaData,
                        store -> assertThat(store.getRecordStoreState().getStoreHeader().hasRecordCountKey()).isFalse());
            }
        }
    }

    private void validateRecordCountKeyIsDisabled(final FormatVersion formatVersion, final RecordMetaData recordMetaData) {
        withStore(formatVersion, recordMetaData, recordStore -> {
            assertThat(recordStore.getRecordStoreState().getStoreHeader().getRecordCountState())
                    .isEqualTo(RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED);
            assertThat(recordStore.getRecordStoreState().getStoreHeader().getRecordCountKey())
                    .isEqualTo(recordMetaData.getRecordCountKey().toKeyExpression());
        });
    }

    static Stream<Arguments> clearAllFormerIndexes() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("removeBefore"),
                Stream.of(DisableIndexBehavior.values()));
    }

    @ParameterizedTest
    @MethodSource
    void clearAllFormerIndexes(final boolean removeBefore, DisableIndexBehavior disableIndexBehavior) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "ToRemove1", "num_value_2");
        metaDataBuilder.addIndex("MySimpleRecord", "ToRemove2", "num_value_unique");
        RecordMetaData recordMetaData = metaDataBuilder.build();
        final List<Object> subspaceKeys = List.of(
                recordMetaData.getIndex("ToRemove1").getSubspaceTupleKey(),
                recordMetaData.getIndex("ToRemove2").getSubspaceTupleKey());
        if (removeBefore) {
            metaDataBuilder.removeIndex("ToRemove1");
            metaDataBuilder.removeIndex("ToRemove2");
            recordMetaData = metaDataBuilder.build();
        }

        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = loadOriginalRecords(recordMetaData, primaryKeys);
        rawAssertHasIndexData(recordMetaData, !removeBefore, subspaceKeys);

        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);


        if (!removeBefore) {
            metaDataBuilder.removeIndex("ToRemove1");
            metaDataBuilder.removeIndex("ToRemove2");
            recordMetaData = metaDataBuilder.build();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData);
            final FDBRecordStore recordStore = disableIndexBehavior.repair(context, builder, 1, FormatVersion.SAVE_VERSION_WITH_RECORD);

            commitRepair(originalRecords, recordStore);
        }

        if (!disableIndexBehavior.leaveIndexes()) {
            try (FDBRecordContext context = openContext()) {
                FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                        .open();
                final List<String> indexes = recordStore.getAllIndexStates().keySet().stream()
                        .map(Index::getName).collect(Collectors.toList());
                assertThat(indexes).doesNotContain("ToRemove1", "ToRemove2");
                commit(context);
            }

            rawAssertHasIndexData(recordMetaData, false, subspaceKeys);

            validateRepaired(FormatVersion.getMaximumSupportedVersion(), recordMetaData, originalRecords, disableIndexBehavior);
        }
    }

    /**
     * We don't repair everything.
     */
    @Test
    void corruptHeader() {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        loadOriginalRecords(recordMetaData, primaryKeys);
        try (FDBRecordContext context1 = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context1, recordMetaData, path).createOrOpen();
            context1.ensureActive().set(recordStore.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key()),
                    "Definitely not a valid store header".getBytes(StandardCharsets.UTF_8));
            commit(context1);
        }
        validateCannotOpen(recordMetaData, RecordCoreStorageException.class);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion());
            assertThatThrownBy(() -> DisableIndexBehavior.ExplicitlyDisableThem.repair(context, builder, 1, FormatVersion.CACHEABLE_STATE))
                    .isInstanceOf(RecordCoreStorageException.class);
        }

        validateCannotOpen(recordMetaData, RecordCoreStorageException.class);
    }

    /**
     * If the store didn't exist at all, {@link FDBRecordStore.Builder#repairMissingHeader(int, FormatVersion)}
     * should behave the same as {@link FDBRecordStore.Builder#createAsync()}.
     */
    @Test
    void noStore() {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final RecordMetaDataProto.DataStoreInfo asIfCreated;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, recordMetaData, path)
                    .setUserVersionChecker(new AssertMatchingUserVersion(0, 1));
            asIfCreated = storeBuilder.create().getRecordStoreState()
                    .getStoreHeader();
            // do not commit it
        }
        // ensure nothing else created the store header
        try (FDBRecordContext context1 = openContext()) {
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context1, recordMetaData, path);
            assertThatThrownBy(storeBuilder::open) // explicitly call open, not create
                    .isInstanceOf(RecordStoreDoesNotExistException.class);
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion());
            DisableIndexBehavior.ExplicitlyDisableThem.repair(context, builder, 1, FormatVersion.CACHEABLE_STATE);
            commit(context);
        }

        try (FDBRecordContext context1 = openContext()) {
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context1, recordMetaData, path);
            final FDBRecordStore recordStore = storeBuilder.open(); // should succeed, because repair created it
            assertThat(recordStore.getRecordStoreState().getStoreHeader().toBuilder().clearLastUpdateTime().build())
                    .isEqualTo(asIfCreated.toBuilder().clearLastUpdateTime().build());
        }
    }

    @ParameterizedTest
    @EnumSource(DisableIndexBehavior.class)
    void repairWithoutDisablingIndexes(final DisableIndexBehavior disableIndexBehavior) {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData);
            final FDBRecordStore recordStore = disableIndexBehavior.repair(context, builder, 1, FormatVersion.CACHEABLE_STATE);

            if (disableIndexBehavior.leaveIndexes()) {
                assertFullyLocked(recordStore);
            } else {
                assertUnlocked(recordStore);
            }
            commit(context);
        }

        final FDBRecordStore.Builder builder;
        final List<Index> toRebuild;
        try (FDBRecordContext context = openContext()) {
            builder = getStoreBuilder(context, recordMetaData);
            if (disableIndexBehavior.leaveIndexes()) {
                builder.setBypassFullStoreLockReason(REPAIR_REASON);
            }
            final FDBRecordStore recordStore = builder.open();
            toRebuild = List.copyOf(recordStore.getAllIndexStates().keySet());
            if (disableIndexBehavior.leaveIndexes()) {
                assertThat(recordStore.getAllIndexStates().values())
                        .contains(IndexState.READABLE);
                for (final Index index : toRebuild) {
                    recordStore.markIndexDisabled(index);
                }
            } else {
                assertAllIndexStates(recordStore, IndexState.DISABLED);
            }
            commit(context);
        }

        buildIndexes(builder, toRebuild);

        if (disableIndexBehavior.leaveIndexes()) {
            clearStoreLock(builder);
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData).open();
            assertAllIndexStates(recordStore, IndexState.READABLE);
        }

    }

    private void clearStoreLock(final FDBRecordStore.Builder builder) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = builder.setContext(context).open();
            recordStore.clearStoreLockStateAsync();
            commit(context);
        }
    }

    private void buildIndexes(final FDBRecordStore.Builder builder, final List<Index> toRebuild) {
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setRecordStoreBuilder(builder)
                .setTargetIndexes(toRebuild)
                .build()) {
            indexBuilder.buildIndex();
        }
    }

    private static void assertAllIndexStates(final FDBRecordStore recordStore, IndexState expected) {
        assertThat(recordStore.getAllIndexStates())
                .allSatisfy((index, indexState) ->
                        assertThat(indexState).isEqualTo(expected))
                .isNotEmpty();
    }

    private static void assertUnlocked(final FDBRecordStore recordStore) {
        assertThat(recordStore.getRecordStoreState().getStoreHeader().getStoreLockState())
                .isEqualTo(RecordMetaDataProto.DataStoreInfo.StoreLockState.getDefaultInstance());
    }

    private static void assertFullyLocked(final FDBRecordStore recordStore) {
        assertThat(recordStore.getRecordStoreState().getStoreHeader().getStoreLockState().getLockState())
                .isEqualTo(RecordMetaDataProto.DataStoreInfo.StoreLockState.State.FULL_STORE);
    }

    private List<FDBStoredRecord<Message>> loadOriginalRecords(final RecordMetaData recordMetaData, final List<Tuple> primaryKeys) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path).open();
            return primaryKeys.stream()
                    .map(recordStore::loadRecord)
                    .collect(Collectors.toList());
        }
    }

    private void commitRepair(final List<FDBStoredRecord<Message>> originalRecords,
                              final FDBRecordStoreBase<Message> recordStore) {
        validateRecords(originalRecords, recordStore);
        commit(recordStore.getContext());
    }

    private void validateRepaired(final int userVersion, final RecordMetaData recordMetaData,
                                  final FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                  final List<FDBStoredRecord<Message>> originalRecords,
                                  final DisableIndexBehavior disableIndexBehavior) {
        if (disableIndexBehavior.leaveIndexes()) {
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData, path)
                        .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                        .setUserVersionChecker(userVersionChecker);
                assertThrows(StoreIsFullyLockedException.class, builder::open);
            }

            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                        .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                        .setUserVersionChecker(userVersionChecker)
                        .setBypassFullStoreLockReason(REPAIR_REASON)
                        .open();
                disableAllIndexesAndClearLock(recordStore);
                commit(context);
            }
        }
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                    .setUserVersionChecker(userVersionChecker)
                    .open();
            assertEquals(userVersion, recordStore.getUserVersion());
            validateRecords(originalRecords, recordStore);
            validateIndexesAreDisabled(recordStore, recordMetaData);
            commit(context);
        }
    }

    private void validateRepaired(final FormatVersion newFormatVersion, final RecordMetaData recordMetaData,
                                  final List<FDBStoredRecord<Message>> originalRecords,
                                  final DisableIndexBehavior disableIndexBehavior) {
        if (disableIndexBehavior.leaveIndexes()) {

            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData, path)
                        .setFormatVersion(newFormatVersion);
                assertThrows(StoreIsFullyLockedException.class, builder::open);
            }

            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                        .setFormatVersion(newFormatVersion)
                        .setBypassFullStoreLockReason(REPAIR_REASON)
                        .open();
                disableAllIndexesAndClearLock(recordStore);
                commit(context);
            }
        }
        withStore(newFormatVersion, recordMetaData,
                recordStore -> {
                    validateRecords(originalRecords, recordStore);
                    validateIndexesAreDisabled(recordStore, recordMetaData);
                });
    }

    private static void disableAllIndexesAndClearLock(final FDBRecordStore recordStore) {
        recordStore.getAllIndexStates().forEach((index, indexState) ->
                recordStore.markIndexDisabled(index).join());
        recordStore.clearStoreLockStateAsync();
    }

    private void validateRecords(final List<FDBStoredRecord<Message>> records, final FDBRecordStoreBase<Message> recordStore) {
        for (final FDBStoredRecord<Message> record : records) {
            final FDBStoredRecord<Message> reloaded = recordStore.loadRecord(record.getPrimaryKey());
            assertEquals(record.getRecord(), reloaded.getRecord());
            assertEquals(record.getVersion(), reloaded.getVersion());
        }
    }

    /**
     * It is possible that the old (lost) metadata version was after the record type was created, and before the
     * index was added, and thus the index needs to be rebuilt. We could optimize this if the record type was added
     * in the same version as the index, but for now, we're just going to disable all indexes unless
     * leavePotentiallyCorruptIndexesReadable is true.
     * If leavePotentiallyCorruptIndexesReadable is false, all indexes should be disabled.
     * If leavePotentiallyCorruptIndexesReadable is true, indexes should remain in their previous state.
     */
    private void validateIndexesAreDisabled(final FDBRecordStore recordStore, final RecordMetaData recordMetaData) {
        for (final Index index : recordMetaData.getAllIndexes()) {
            assertEquals(IndexState.DISABLED, recordStore.getIndexState(index));
        }
    }

    private void rawAssertHasIndexData(final RecordMetaData metaData, final boolean hasIndexData, final List<Object> subspaceKeys) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = createOrOpenRecordStore(context, metaData, path).getKey();
            final List<FDBRecordStoreKeyspace> subspaces = List.of(FDBRecordStoreKeyspace.INDEX,
                    FDBRecordStoreKeyspace.INDEX_SECONDARY_SPACE,
                    FDBRecordStoreKeyspace.INDEX_RANGE_SPACE,
                    FDBRecordStoreKeyspace.INDEX_STATE_SPACE,
                    FDBRecordStoreKeyspace.INDEX_UNIQUENESS_VIOLATIONS_SPACE);

            final List<KeyValue> allData = context.ensureActive().getRange(recordStore.getSubspace().range()).asList().join();

            final List<KeyValue> actual = subspaces.stream().flatMap(subspace ->
                    subspaceKeys.stream().flatMap(indexSubspace -> {
                        if (subspace == FDBRecordStoreKeyspace.INDEX_STATE_SPACE) {
                            final byte[] key = recordStore.getSubspace().pack(Tuple.from(subspace.key(), indexSubspace));
                            final byte[] value = context.ensureActive().get(key).join();
                            if (value == null) {
                                return Stream.of();
                            } else {
                                return Stream.of(new KeyValue(key, value));
                            }
                        } else {
                            return context.ensureActive().getRange(recordStore.getSubspace().range(Tuple.from(subspace.key(), indexSubspace))).asList().join().stream();
                        }
                    })
            ).collect(Collectors.toList());

            if (hasIndexData) {
                assertNotEquals(List.of(), actual);
            } else {
                assertEquals(List.of(), actual);
            }
        }
    }

    private RecordMetaData getRecordMetaData(final boolean supportSplitRecords) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.setSplitLongRecords(supportSplitRecords);
        return metaDataBuilder.getRecordMetaData();
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        return getStoreBuilder(context, metaData, path);
    }

    private void withStore(final FormatVersion formatVersion,
                           final RecordMetaData recordMetaData,
                           final Consumer<FDBRecordStore> useStore) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setFormatVersion(formatVersion)
                    .open();
            useStore.accept(recordStore);
            commit(context);
        }
    }

    private void validateCannotOpen(final RecordMetaDataProvider metaData) {
        validateCannotOpen(metaData, RecordStoreNoInfoAndNotEmptyException.class);
    }

    private void validateCannotOpen(final RecordMetaDataProvider metaData, final Class<? extends RecordCoreException> exceptionType) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, metaData, path);
            assertThatThrownBy(storeBuilder::createOrOpen)
                    .isInstanceOf(exceptionType);
            commit(context);
        }
    }

    private void clearStoreHeader(final RecordMetaDataProvider metaData) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, metaData, path).createOrOpen();
            context.ensureActive().clear(recordStore.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key()));
            commit(context);
        }
    }

    @Nonnull
    private static FDBRecordStore joinRepair(final FDBRecordContext context, final CompletableFuture<NonnullPair<Boolean, FDBRecordStore>> repairFuture) {
        final NonnullPair<Boolean, FDBRecordStore> result = context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION,
                repairFuture);
        assertTrue(result.getLeft(), "Repairing header should have done something");
        return result.getRight();
    }

    @Nonnull
    private FDBRecordStoreBase.UserVersionChecker setupCachedStoreState(final RecordMetaData recordMetaData) {
        FDBRecordStoreBase.UserVersionChecker userVersionChecker = new AssertMatchingUserVersion(0, 1);

        try (FDBRecordContext context = openContext()) {
            getStoreBuilder(context, recordMetaData)
                    .setUserVersionChecker(userVersionChecker)
                    .open()
                    .setStateCacheability(true);
            commit(context);
        }

        userVersionChecker = new AssertMatchingUserVersion(1, 1);

        timer.reset();
        try (FDBRecordContext context = openContext()) {
            getStoreBuilder(context, recordMetaData)
                    .setUserVersionChecker(userVersionChecker)
                    .open();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            commit(context);
        }

        timer.reset();
        try (FDBRecordContext context = openContext()) {
            getStoreBuilder(context, recordMetaData)
                    .setUserVersionChecker(userVersionChecker)
                    .open();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_HIT));
            commit(context);
        }

        timer.reset();
        return userVersionChecker;
    }

    private List<Tuple> createInitialStore(final FormatVersion initialFormatVersion, final RecordMetaDataProvider metaData) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = getStoreBuilder(context, metaData, path)
                    .setFormatVersion(initialFormatVersion)
                    .createOrOpen();
            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .setStrValueIndexed("a_1")
                    .setNumValueUnique(1)
                    .setNumValue2(7)
                    .build();
            final FDBStoredRecord<Message> storedRecord = recordStore.saveRecord(rec);
            commit(context);
            return List.of(storedRecord.getPrimaryKey());
        }
    }

    private static class AssertMatchingUserVersion implements FDBRecordStoreBase.UserVersionChecker {
        private final int oldUserVersion;
        private final int newUserVersion;

        public AssertMatchingUserVersion(final int oldUserVersion, final int newUserVersion) {
            this.oldUserVersion = oldUserVersion;
            this.newUserVersion = newUserVersion;
        }

        @Override
        public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
            assertEquals(oldUserVersion, storeHeader.getUserVersion());
            return CompletableFuture.completedFuture(newUserVersion);
        }

        @Override
        @SuppressWarnings("deprecation") // overriding deprecated method
        public CompletableFuture<Integer> checkUserVersion(final int oldUserVersion, final int oldMetaDataVersion,
                                                           final RecordMetaDataProvider metaData) {
            return fail("Should not call deprecated checkUserVersion");
        }
    }

    private static class AssertMatchingMetaDataVersion implements FDBRecordStoreBase.UserVersionChecker {
        private final RecordMetaData expected;

        public AssertMatchingMetaDataVersion(final RecordMetaData recordMetaData) {
            expected = recordMetaData;
        }

        @Override
        public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
            assertThat(storeHeader.getMetaDataversion()).isEqualTo(expected.getVersion());
            assertThat(metaData).isEqualTo(expected);
            return CompletableFuture.completedFuture(storeHeader.getUserVersion());
        }

        @Override
        @SuppressWarnings("deprecation") // overriding deprecated method
        public CompletableFuture<Integer> checkUserVersion(final int oldUserVersion, final int oldMetaDataVersion,
                                                           final RecordMetaDataProvider metaData) {
            return fail("Should not call deprecated checkUserVersion");
        }
    }

    enum DisableIndexBehavior {
        LeaveCorrupted {
            FDBRecordStore repair(FDBRecordContext context, FDBRecordStore.Builder builder, int userVersion, FormatVersion minimalPossibleFormatVersion) {
                return joinRepair(context,
                        builder.repairMissingHeader(userVersion, minimalPossibleFormatVersion, REPAIR_REASON));
            }
        },
        ExplicitlyDisableThem {
            FDBRecordStore repair(FDBRecordContext context, FDBRecordStore.Builder builder, int userVersion, FormatVersion minimalPossibleFormatVersion) {
                return joinRepair(context,
                        builder.repairMissingHeader(userVersion, minimalPossibleFormatVersion, null));
            }
        },
        /** Call the old overload that does not specify the argument. **/
        ImplicitlyDisableThem {
            FDBRecordStore repair(FDBRecordContext context, FDBRecordStore.Builder builder, int userVersion, FormatVersion minimalPossibleFormatVersion) {
                return joinRepair(context, builder.repairMissingHeader(userVersion, minimalPossibleFormatVersion));
            }
        };

        abstract FDBRecordStore repair(FDBRecordContext context, FDBRecordStore.Builder builder, int userVersion, FormatVersion newFormatVersion);

        boolean leaveIndexes() {
            return this == LeaveCorrupted;
        }
    }
}
