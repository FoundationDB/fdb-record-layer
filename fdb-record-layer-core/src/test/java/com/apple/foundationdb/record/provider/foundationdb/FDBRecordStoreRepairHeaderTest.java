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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.provider.foundationdb.storestate.MetaDataVersionStampStoreStateCacheFactory;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Tag(Tags.RequiresFDB)
public class FDBRecordStoreRepairHeaderTest extends FDBRecordStoreTestBase {

    static Stream<FormatVersion> formatVersions() {
        return Arrays.stream(FormatVersion.values());
    }

    static Stream<Arguments> repairUpgradeFormatVersion() {
        return formatVersions()
                .flatMap(oldVersion -> formatVersions().filter(newVersion -> newVersion.isAtLeast(oldVersion))
                .flatMap(newVersion -> Stream.of(true, false)
                        .map(supportSplitRecords -> Arguments.of(oldVersion, newVersion, supportSplitRecords))));
    }

    @ParameterizedTest
    @MethodSource
    void repairUpgradeFormatVersion(FormatVersion oldFormatVersion, FormatVersion newFormatVersion, boolean supportSplitRecords) {
        final RecordMetaData recordMetaData = getRecordMetaData(supportSplitRecords);
        final List<Tuple> primaryKeys = createInitialStore(oldFormatVersion, recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = createOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(newFormatVersion);
            if (oldFormatVersion.isAtLeast(FormatVersion.SAVE_VERSION_WITH_RECORD)) {
                repairHeader(context, 1, builder, FormatVersion.SAVE_VERSION_WITH_RECORD);
            } else if (!newFormatVersion.isAtLeast(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX)) {
                repairHeader(context, 1, builder, FormatVersion.INFO_ADDED);
            } else {
                assertThatThrownBy(() -> repairHeader(context, 1, builder, oldFormatVersion))
                        .isInstanceOf(RecordCoreException.class);
                return; // nothing left to test
            }
            commit(context);
        }

        validateRepaired(newFormatVersion, recordMetaData, originalRecords);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void repairUserVersion(int userVersion) {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = createOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final FDBRecordStoreBase.UserVersionChecker userVersionChecker = new AssertMatchingUserVersion(userVersion, userVersion);

        try (FDBRecordContext context = openContext()) {
            repairHeader(context, userVersion, getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                    .setUserVersionChecker(userVersionChecker));
            commit(context);
        }

        validateRepaired(userVersion, recordMetaData, userVersionChecker, originalRecords);
    }

    @Test
    void needRebuildIndex() {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = createOriginalRecords(recordMetaData, primaryKeys);
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
            repairHeader(context, userVersion,
                    getStoreBuilder(context, recordMetaData)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                            .setUserVersionChecker(userVersionChecker));
            commit(context);
        }

        validateRepaired(userVersion, recordMetaData, userVersionChecker, originalRecords);
    }

    @Test
    void repairUserField() {
        final RecordMetaData recordMetaData = getRecordMetaData(true);
        final List<Tuple> primaryKeys = createInitialStore(FormatVersion.getMaximumSupportedVersion(), recordMetaData);
        createOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final int userVersion = 2;
        final String key = "someState";
        final ByteString value = ByteString.copyFromUtf8("My value");
        try (FDBRecordContext context = openContext()) {
            repairHeader(context, userVersion,
                    getStoreBuilder(context, recordMetaData)
                            .setFormatVersion(FormatVersion.getMaximumSupportedVersion()));
            recordStore.setHeaderUserField(key, value);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                    .open();
            assertEquals(value, recordStore.getHeaderUserField(key));
            commit(context);
        }
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
        createOriginalRecords(recordMetaData, primaryKeys);

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
            repairHeader(context, 3, builder);
            commit(context);
        }

        // now go back to the original store which has a cached version of the store state
        // it should have to read from the database, and pick up the new store header
        fdb.setStoreStateCache(populatedCache);

        userVersionChecker = new AssertMatchingUserVersion(3, 3);
        timer.reset();
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setUserVersionChecker(userVersionChecker)
                    .open();
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.STORE_STATE_CACHE_MISS));
            assertEquals(3, recordStore.getUserVersion());
        }
    }

    @ParameterizedTest
    @BooleanSource
    void failIfHeaderExists(boolean cached) {
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
        createOriginalRecords(recordMetaData, primaryKeys);

        if (cached) {
            setupCachedStoreState(recordMetaData);
            clearStoreHeader(recordMetaData);
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder builder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion());
            assertThatThrownBy(() -> repairHeader(context, 1, builder))
                    .isInstanceOf(RecordCoreException.class);
            commit(context);
        }
    }

    static Stream<Arguments> disableRecordCountKeyOnRepair() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("hasRecordCountKey"),
                Stream.of(FormatVersion.getMaximumSupportedVersion(),
                        FormatVersionTestUtils.previous(FormatVersion.RECORD_COUNT_STATE)));
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("deprecation")
    void disableRecordCountKeyOnRepair(boolean hasRecordCountKey, FormatVersion formatVersion) {
        // We cannot tell whether the recordCountKey had changed since the last time we did checkVersion, so
        // we can't guarantee that it is correct. If there is a RecordCountKey and we're on a new enough format version
        // we'll disable the record count key, otherwise, we'll throw an exception
        final RecordMetaData recordMetaData = simpleMetaData(metadata -> {
            metadata.setSplitLongRecords(true);
            if (hasRecordCountKey) {
                metadata.setRecordCountKey(Key.Expressions.empty());
            }
        });
        final List<Tuple> primaryKeys = createInitialStore(formatVersion, recordMetaData);
        final List<FDBStoredRecord<Message>> originalRecords = createOriginalRecords(recordMetaData, primaryKeys);
        clearStoreHeader(recordMetaData);
        validateCannotOpen(recordMetaData);

        final int userVersion = 2;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, recordMetaData)
                    .setFormatVersion(formatVersion);
            if (hasRecordCountKey) {
                if (formatVersion.isAtLeast(FormatVersion.RECORD_COUNT_STATE)) {
                    repairHeader(context, userVersion, storeBuilder);
                    assertThat(recordStore.getRecordStoreState().getStoreHeader().getRecordCountState())
                            .isEqualTo(RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED);
                    commit(context);
                } else {
                    assertThatThrownBy(() -> repairHeader(context, userVersion, storeBuilder))
                            .isInstanceOf(RecordCoreException.class);
                }
            } else {
                repairHeader(context, userVersion, storeBuilder);
                commit(context);
            }
        }

        if (hasRecordCountKey) {
            if (formatVersion.isAtLeast(FormatVersion.RECORD_COUNT_STATE)) {
                validateRepaired(formatVersion, recordMetaData, originalRecords);
                try (FDBRecordContext context = openContext()) {
                    recordStore = getStoreBuilder(context, recordMetaData, path)
                            .setFormatVersion(formatVersion)
                            .open();

                    assertThat(recordStore.getRecordStoreState().getStoreHeader().getRecordCountState())
                            .isEqualTo(RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED);
                    assertThat(recordStore.getRecordStoreState().getStoreHeader().getRecordCountKey())
                            .isEqualTo(recordMetaData.getRecordCountKey().toKeyExpression());
                }
            } else {
                validateCannotOpen(recordMetaData);
            }
        } else {
            validateRepaired(formatVersion, recordMetaData, originalRecords);
            try (FDBRecordContext context = openContext()) {
                recordStore = getStoreBuilder(context, recordMetaData, path)
                        .setFormatVersion(formatVersion)
                        .open();

                assertThat(recordStore.getRecordStoreState().getStoreHeader().hasRecordCountKey())
                        .isFalse();
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void clearAllFormerIndexes(final boolean removeBefore) {
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
        final List<FDBStoredRecord<Message>> originalRecords = createOriginalRecords(recordMetaData, primaryKeys);
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
            repairHeader(context, 1, builder);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, recordMetaData, path)
                    .open();
            final List<String> indexes = recordStore.getAllIndexStates().keySet().stream()
                    .map(Index::getName).collect(Collectors.toList());
            assertThat(indexes).doesNotContain("ToRemove1", "ToRemove2");
            commit(context);
        }

        rawAssertHasIndexData(recordMetaData, false, subspaceKeys);

        validateRepaired(FormatVersion.getMaximumSupportedVersion(), recordMetaData, originalRecords);
    }

    private List<FDBStoredRecord<Message>> createOriginalRecords(final RecordMetaData recordMetaData, final List<Tuple> primaryKeys) {
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, recordMetaData, path).open();
            return primaryKeys.stream()
                    .map(primaryKey -> recordStore.loadRecord(primaryKey))
                    .collect(Collectors.toList());
        }
    }

    private void validateRepaired(final int userVersion, final RecordMetaData recordMetaData,
                                  final FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                  final List<FDBStoredRecord<Message>> originalRecords) {
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                    .setUserVersionChecker(userVersionChecker)
                    .open();
            assertEquals(userVersion, recordStore.getUserVersion());
            validateRecords(originalRecords);
            validateIndexesDisabled(recordMetaData);
            commit(context);
        }
    }

    private void validateRepaired(final FormatVersion newFormatVersion, final RecordMetaData recordMetaData,
                                  final List<FDBStoredRecord<Message>> originalRecords) {
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, recordMetaData, path)
                    .setFormatVersion(newFormatVersion)
                    .open();
            validateRecords(originalRecords);
            validateIndexesDisabled(recordMetaData);
            commit(context);
        }
    }

    private void validateRecords(final List<FDBStoredRecord<Message>> records) {
        for (final FDBStoredRecord<Message> record : records) {
            final FDBStoredRecord<Message> reloaded = recordStore.loadRecord(record.getPrimaryKey());
            assertEquals(record.getRecord(), reloaded.getRecord());
            assertEquals(record.getVersion(), reloaded.getVersion());
        }
    }

    /**
     * It is possible that the old (lost) metadata version was after the record type was created, and before the
     * index was added, and thus the index needs to be rebuilt. We could optimize this if the record type was added
     * in the same version as the index, but for now, we're just going to disable all indexes.
     */
    private void validateIndexesDisabled(final RecordMetaData recordMetaData) {
        for (final Index index : recordMetaData.getAllIndexes()) {
            assertEquals(IndexState.DISABLED, recordStore.getIndexState(index));
        }
    }

    private void rawAssertHasIndexData(final RecordMetaData metaData, final boolean hasIndexData, final List<Object> subspaceKeys) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
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
        return simpleMetaData(metadata -> metadata.setSplitLongRecords(supportSplitRecords));
    }

    private void validateCannotOpen(final RecordMetaDataProvider metaData) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, metaData, path);
            assertThatThrownBy(storeBuilder::createOrOpen)
                    .isInstanceOf(RecordStoreNoInfoAndNotEmptyException.class);
            commit(context);
        }
    }

    private void clearStoreHeader(final RecordMetaDataProvider metaData) {
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, metaData, path).createOrOpen();
            context.ensureActive().clear(recordStore.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key()));
            commit(context);
        }
    }

    private void repairHeader(final FDBRecordContext context, final int userVersion,
                              final FDBRecordStore.Builder builder) {
        repairHeader(context, userVersion, builder, FormatVersion.SAVE_VERSION_WITH_RECORD);
    }

    private void repairHeader(final FDBRecordContext context, final int userVersion,
                              final FDBRecordStore.Builder builder, final FormatVersion minimumPossibleFormatVersion) {
        recordStore = context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_VERSION,
                builder.repairMissingHeader(userVersion, minimumPossibleFormatVersion));
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
            recordStore = getStoreBuilder(context, metaData, path)
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
        public CompletableFuture<Integer> checkUserVersion(final int oldUserVersion, final int oldMetaDataVersion, final RecordMetaDataProvider metaData) {
            assertEquals(this.oldUserVersion, oldUserVersion);
            return CompletableFuture.completedFuture(newUserVersion);
        }
    }
}
