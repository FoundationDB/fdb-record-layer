/*
 * FDBRecordStoreSplitRecordsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FDBRecordStoreProperties;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests directly related to whether we split records across multiple key/value pairs or not.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreSplitRecordsTest extends FDBRecordStoreTestBase {

    static class SplitRecordsTestConfig {
        private final boolean unrollRecordDeletes;
        private final boolean loadViaGets;

        public SplitRecordsTestConfig(boolean unrollRecordDeletes, boolean loadViaGets) {
            this.unrollRecordDeletes = unrollRecordDeletes;
            this.loadViaGets = loadViaGets;
        }

        public RecordLayerPropertyStorage.Builder setProps(RecordLayerPropertyStorage.Builder props) {
            return props
                    .addProp(FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES, unrollRecordDeletes)
                    .addProp(FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS, loadViaGets);
        }

        @Override
        public String toString() {
            return "SplitRecordsTestConfig{" +
                   "unrollRecordDeletes=" + unrollRecordDeletes +
                   ", loadViaGets=" + loadViaGets +
                   '}';
        }

        static Stream<SplitRecordsTestConfig> allConfigs() {
            return Stream.of(false, true).flatMap(unrollRecordDeletes ->
                    Stream.of(false, true).map(loadViaGets ->
                            new SplitRecordsTestConfig(unrollRecordDeletes, loadViaGets)));
        }

        static SplitRecordsTestConfig getDefault() {
            return new SplitRecordsTestConfig(
                    FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES.getDefaultValue(),
                    FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS.getDefaultValue()
            );
        }
    }

    static Stream<Arguments> testConfigs() {
        return SplitRecordsTestConfig.allConfigs().map(Arguments::of);
    }

    private SplitRecordsTestConfig testConfig = SplitRecordsTestConfig.getDefault();

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return testConfig.setProps(super.addDefaultProps(props));
    }

    @ParameterizedTest(name = "unsplitCompatibility[{0}]")
    @MethodSource("testConfigs")
    public void unsplitCompatibility(SplitRecordsTestConfig testConfig) throws Exception {
        this.testConfig = testConfig;
        TestRecords1Proto.MySimpleRecord rec1 = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1415L).build();
        try (FDBRecordContext context = openContext()) {
            // Write a record using the old format
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK))
                    .setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX))
                    .create();
            assertEquals(FormatVersionTestUtils.previous(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX), recordStore.getFormatVersionEnum());
            recordStore.saveRecord(rec1);
            final byte[] rec1Key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L));
            FDBStoredRecord<Message> readRec1 = recordStore.loadRecord(Tuple.from(1415L));
            assertNotNull(readRec1);
            assertFalse(readRec1.isSplit());
            assertEquals(1, readRec1.getKeyCount());
            assertEquals(rec1Key.length, readRec1.getKeySize());
            assertEquals(Tuple.from(1415L), readRec1.getPrimaryKey());
            assertEquals(rec1, readRec1.getRecord());

            // Upgrade the format version to use new format
            recordStore = recordStore.asBuilder()
                    .setFormatVersion(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX)
                    .open();
            assertEquals(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX, recordStore.getFormatVersionEnum());
            Message rec2 = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build();
            recordStore.saveRecord(rec2);

            // Read old-record that was written using old format.
            readRec1 = recordStore.loadRecord(Tuple.from(1415L));
            assertNotNull(readRec1);
            assertFalse(readRec1.isSplit());
            assertEquals(1, readRec1.getKeyCount());
            assertEquals(rec1Key.length, readRec1.getKeySize());
            assertEquals(Tuple.from(1415L), readRec1.getPrimaryKey());
            assertEquals(rec1, readRec1.getRecord());

            // Ensure written using old format.
            final byte[] rec2Key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L));
            final byte[] rawRecord = context.ensureActive().get(recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L))).get();
            assertNotNull(rawRecord);
            assertEquals(rec2, TestRecords1Proto.RecordTypeUnion.parseFrom(rawRecord).getMySimpleRecord());

            // Ensure can still read using point-lookup.
            FDBStoredRecord<Message> readRec2 = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(readRec2);
            assertFalse(readRec2.isSplit());
            assertEquals(1, readRec2.getKeyCount());
            assertEquals(Tuple.from(1066L), readRec2.getPrimaryKey());
            assertEquals(rec2Key.length, readRec2.getKeySize());
            assertEquals(rec2, readRec2.getRecord());

            // Ensure can still read using range scan.
            List<FDBStoredRecord<Message>> recs = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(2, recs.size());

            assertFalse(recs.get(0).isSplit());
            assertEquals(1, recs.get(0).getKeyCount());
            assertEquals(rec2Key.length, recs.get(0).getKeySize());
            assertEquals(Tuple.from(1066L), recs.get(0).getPrimaryKey());
            assertEquals(rec2, recs.get(0).getRecord());

            assertFalse(recs.get(1).isSplit());
            assertEquals(rec1, recs.get(1).getRecord());
            assertEquals(1, recs.get(1).getKeyCount());
            assertEquals(rec1Key.length, recs.get(0).getKeySize());
            assertEquals(Tuple.from(1415L), recs.get(1).getPrimaryKey());
            assertEquals(rec1, recs.get(1).getRecord());

            // Ensure can still delete.
            recordStore.deleteRecord(Tuple.from(1066L));
            assertEquals(1, recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().get().intValue());
            recordStore.deleteRecord(Tuple.from(1415L));
            assertEquals(0, recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().get().intValue());

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            // Add an index so that we have to check the record count. (This automatic upgrade happens only
            // if we are already reading the record count anyway, which is why it happens here.)
            uncheckedOpenSimpleRecordStore(context, metaDataBuilder ->
                    metaDataBuilder.addUniversalIndex(new Index("global$newCount", FDBRecordStoreTestBase.globalCountIndex().getRootExpression(), IndexTypes.COUNT))
            );
            recordStore = recordStore.asBuilder().setFormatVersion(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX).open();
            assertEquals(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX, recordStore.getFormatVersionEnum());

            final byte[] rec1Key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L, SplitHelper.UNSPLIT_RECORD));
            FDBStoredRecord<Message> writtenRec1 = recordStore.saveRecord(rec1);
            assertNotNull(writtenRec1);
            assertFalse(writtenRec1.isSplit());
            assertEquals(1, writtenRec1.getKeyCount());
            assertEquals(rec1Key.length, writtenRec1.getKeySize());
            assertEquals(Tuple.from(1415L), writtenRec1.getPrimaryKey());
            assertEquals(rec1, writtenRec1.getRecord());

            final byte[] rawRec1 = context.ensureActive().get(rec1Key).get();
            assertNotNull(rawRec1);
            assertEquals(writtenRec1.getValueSize(), rawRec1.length);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            // Read from the now upgraded store with a record store that sets its format version to be older to make sure
            // it correctly switches to the new one.
            // Use same meta-data as last test
            uncheckedOpenSimpleRecordStore(context, metaDataBuilder ->
                    metaDataBuilder.addUniversalIndex(new Index("global$newCount", globalCountIndex().getRootExpression(), IndexTypes.COUNT))
            );
            recordStore = recordStore.asBuilder().setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX)).open();
            assertEquals(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX, recordStore.getFormatVersionEnum());

            final byte[] rawKey1 = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L, SplitHelper.UNSPLIT_RECORD));
            FDBStoredRecord<Message> writtenRec1 = recordStore.loadRecord(Tuple.from(1415L));
            assertNotNull(writtenRec1);
            assertEquals(rec1, writtenRec1.getRecord());
            assertEquals(Tuple.from(1415L), writtenRec1.getPrimaryKey());
            assertEquals(rawKey1.length, writtenRec1.getKeySize());

            final TestRecords1Proto.MySimpleRecord rec2 = rec1.toBuilder().setRecNo(1623L).build();
            final byte[] rawKey2 = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1623L, SplitHelper.UNSPLIT_RECORD));
            FDBStoredRecord<Message> writtenRec2 = recordStore.saveRecord(rec1.toBuilder().setRecNo(1623L).build());
            assertEquals(rec2, writtenRec2.getRecord());
            assertEquals(1, writtenRec2.getKeyCount());
            assertEquals(rawKey2.length, writtenRec2.getKeySize());
            assertEquals(Tuple.from(1623L), writtenRec2.getPrimaryKey());

            final byte[] rawRec2 = context.ensureActive().get(rawKey2).get();
            assertNotNull(rawRec2);
            assertEquals(writtenRec2.getValueSize(), rawRec2.length);

            commit(context);
        }
    }

    /**
     * Test mode for {@link #clearOmitUnsplitRecordSuffix(ClearOmitUnsplitRecordSuffixMode)}.
     */
    public enum ClearOmitUnsplitRecordSuffixMode {
        EMPTY, SAVE, DELETE
    }

    @EnumSource(ClearOmitUnsplitRecordSuffixMode.class)
    @ParameterizedTest(name = "clearOmitUnsplitRecordSuffix [mode = {0}]")
    public void clearOmitUnsplitRecordSuffix(ClearOmitUnsplitRecordSuffixMode mode) {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData)
                .setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX));

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).create();
            if (mode != ClearOmitUnsplitRecordSuffixMode.EMPTY) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1L)
                        .setStrValueIndexed("abc")
                        .build();
                recordStore.saveRecord(record);
            }
            commit(context);
        }

        if (mode == ClearOmitUnsplitRecordSuffixMode.DELETE) {
            try (FDBRecordContext context = openContext()) {
                recordStore = builder.setContext(context).open();
                recordStore.deleteRecord(Tuple.from(1L));
                commit(context);
            }
        }

        metaData.addIndex("MySimpleRecord", "num_value_2");
        builder.setFormatVersion(FormatVersion.getMaximumSupportedVersion());

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).open();
            assertEquals(mode == ClearOmitUnsplitRecordSuffixMode.SAVE,
                    recordStore.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());
        }
    }

    @Test
    public void clearOmitUnsplitRecordSuffixTyped() {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        final KeyExpression pkey = concat(Key.Expressions.recordType(), field("rec_no"));
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(pkey);
        metaData.getRecordType("MyOtherRecord").setPrimaryKey(pkey);

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData)
                .setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX));

        final FDBStoredRecord<Message> saved;
        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).create();
            TestRecords1Proto.MyOtherRecord record = TestRecords1Proto.MyOtherRecord.newBuilder()
                    .setRecNo(1L)
                    .build();
            saved = recordStore.saveRecord(record);
            commit(context);
        }

        metaData.addIndex("MySimpleRecord", "num_value_2");
        builder.setFormatVersion(FormatVersion.getMaximumSupportedVersion());

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).open();
            FDBStoredRecord<Message> loaded = recordStore.loadRecord(saved.getPrimaryKey());
            assertNotNull(loaded);
            assertEquals(saved.getRecord(), loaded.getRecord());
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());
        }
    }

    @Test
    public void clearOmitUnsplitRecordSuffixOverlapping() {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData)
                .setFormatVersion(FormatVersionTestUtils.previous(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX));

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).create();
            commit(context);
        }

        FDBRecordContext context1 = openContext();
        FDBRecordStore recordStore = builder.setContext(context1).open();
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1L)
                .setStrValueIndexed("abc")
                .build();
        FDBStoredRecord<Message> saved = recordStore.saveRecord(record);

        metaData.addIndex("MySimpleRecord", "num_value_2");
        builder.setFormatVersion(FormatVersion.getMaximumSupportedVersion());

        // If we did build, we'd create a read confict on the new index.
        // We want to test that a conflict comes from the clearing itself.
        final FDBRecordStoreBase.UserVersionChecker dontBuild = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(0);
            }

            @Deprecated
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                throw new RecordCoreException("deprecated checkUserVersion called");
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.DISABLED;
            }
        };
        builder.setUserVersionChecker(dontBuild);

        FDBRecordContext context2 = openContext();
        FDBRecordStore recordStore2 = builder.setContext(context2).open();
        assertFalse(recordStore2.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());

        commit(context1);
        assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(context2));

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).open();
            FDBStoredRecord<Message> loaded = recordStore.loadRecord(saved.getPrimaryKey());
            assertNotNull(loaded);
            assertEquals(saved.getRecord(), loaded.getRecord());
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());
        }
    }

    @ParameterizedTest(name = "unsplitToSplitUpgrade[{0}]")
    @MethodSource("testConfigs")
    public void unsplitToSplitUpgrade(SplitRecordsTestConfig testConfig) throws Exception {
        this.testConfig = testConfig;
        TestRecords1Proto.MySimpleRecord rec1 = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build();
        TestRecords1Proto.MySimpleRecord rec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1415L)
                .setStrValueIndexed(Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2))
                .build();
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
                metaDataBuilder.setStoreRecordVersions(false);
            });
            assertTrue(recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).get());
            assertFalse(recordStore.getRecordMetaData().isSplitLongRecords());

            FDBStoredRecord<Message> storedRecord = recordStore.saveRecord(rec1);
            assertNotNull(storedRecord);
            assertEquals(1, storedRecord.getKeyCount());
            assertEquals(recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L, SplitHelper.UNSPLIT_RECORD)).length,
                    storedRecord.getKeySize());
            assertEquals(Tuple.from(1066L), storedRecord.getPrimaryKey());
            assertFalse(storedRecord.isSplit());
            assertEquals(rec1, storedRecord.getRecord());

            try {
                recordStore.saveRecord(rec2);
            } catch (RecordCoreException e) {
                assertThat(e.getMessage(), startsWith("Record is too long"));
            }
            assertNull(recordStore.loadRecord(Tuple.from(1415L)));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, TEST_SPLIT_HOOK);
            assertTrue(recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).get());
            assertTrue(recordStore.getRecordMetaData().isSplitLongRecords());

            // Single key lookup should still work.
            final FDBStoredRecord<Message> storedRec1 = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(storedRec1);
            assertEquals(1, storedRec1.getKeyCount());
            assertEquals(recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L, SplitHelper.UNSPLIT_RECORD)).length,
                    storedRec1.getKeySize());
            assertEquals(Tuple.from(1066L), storedRec1.getPrimaryKey());
            assertFalse(storedRec1.isSplit());
            assertEquals(rec1, storedRec1.getRecord());

            // Scan should return only that record
            RecordCursorIterator<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> scannedRec1 = cursor.next();
            assertEquals(storedRec1, scannedRec1);
            assertFalse(cursor.hasNext());
            cursor = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> scannedReverseRec1 = cursor.next();
            assertEquals(storedRec1, scannedReverseRec1);
            assertFalse(cursor.hasNext());

            // Save a split record
            final FDBStoredRecord<Message> storedRec2 = recordStore.saveRecord(rec2);
            assertNotNull(storedRec2);
            assertEquals(2, storedRec2.getKeyCount());
            assertEquals(Tuple.from(1415L), storedRec2.getPrimaryKey());
            assertEquals(
                    2 * recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L)).length
                    + Tuple.from(SplitHelper.START_SPLIT_RECORD).pack().length + Tuple.from(SplitHelper.START_SPLIT_RECORD + 1).pack().length,
                    storedRec2.getKeySize()
            );
            assertTrue(storedRec2.isSplit());
            assertEquals(rec2, storedRec2.getRecord());

            // Scan should now contain both records.
            cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec1 = cursor.next();
            assertEquals(storedRec1, scannedRec1);
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertFalse(cursor.hasNext());
            cursor = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertTrue(cursor.hasNext());
            scannedRec1 = cursor.next();
            assertEquals(storedRec1, scannedRec1);
            assertFalse(cursor.hasNext());

            // Delete the unsplit record.
            assertTrue(recordStore.deleteRecord(Tuple.from(1066L)));

            // Scan should now have just the second record
            cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertFalse(cursor.hasNext());
            cursor = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertFalse(cursor.hasNext());

            commit(context);
        }
    }

    @ParameterizedTest(name = "longRecords[{0}]")
    @MethodSource("testConfigs")
    public void longRecords(SplitRecordsTestConfig testConfig) {
        this.testConfig = testConfig;

        Random rand = new Random();
        byte[] bytes;
        bytes = new byte[10000];
        rand.nextBytes(bytes);
        ByteString bytes1 = ByteString.copyFrom(bytes);
        bytes = new byte[250000];
        rand.nextBytes(bytes);
        ByteString bytes2 = ByteString.copyFrom(bytes);
        bytes = new byte[1000];
        rand.nextBytes(bytes);
        ByteString bytes3 = ByteString.copyFrom(bytes);

        try (FDBRecordContext context = openContext()) {
            openLongRecordStore(context);

            TestRecords2Proto.MyLongRecord.Builder recBuilder = TestRecords2Proto.MyLongRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setBytesValue(bytes1);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = TestRecords2Proto.MyLongRecord.newBuilder();
            recBuilder.setRecNo(2);
            recBuilder.setBytesValue(bytes2);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = TestRecords2Proto.MyLongRecord.newBuilder();
            recBuilder.setRecNo(3);
            recBuilder.setBytesValue(bytes3);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openLongRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecords2Proto.MyLongRecord.Builder myrec1 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(bytes1, myrec1.getBytesValue());
            FDBStoredRecord<Message> rec2 = recordStore.loadRecord(Tuple.from(2L));
            assertNotNull(rec2);
            TestRecords2Proto.MyLongRecord.Builder myrec2 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec2.mergeFrom(rec2.getRecord());
            assertEquals(bytes2, myrec2.getBytesValue());
            FDBStoredRecord<Message> rec3 = recordStore.loadRecord(Tuple.from(3L));
            assertNotNull(rec3);
            TestRecords2Proto.MyLongRecord.Builder myrec3 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec3.mergeFrom(rec3.getRecord());
            assertEquals(bytes3, myrec3.getBytesValue());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openLongRecordStore(context);
            RecordCursorIterator<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> rec1 = cursor.next();
            TestRecords2Proto.MyLongRecord.Builder myrec1 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(bytes1, myrec1.getBytesValue());
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> rec2 = cursor.next();
            TestRecords2Proto.MyLongRecord.Builder myrec2 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec2.mergeFrom(rec2.getRecord());
            assertEquals(bytes2, myrec2.getBytesValue());
            FDBStoredRecord<Message> rec3 = cursor.next();
            TestRecords2Proto.MyLongRecord.Builder myrec3 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec3.mergeFrom(rec3.getRecord());
            assertEquals(bytes3, myrec3.getBytesValue());
            assertFalse(cursor.hasNext());
            commit(context);
        }
    }

    @Test
    public void testSplitContinuation() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            commit(context);
        }

        final String bigValue = Strings.repeat("X", SplitHelper.SPLIT_RECORD_SIZE + 10);
        final String smallValue = Strings.repeat("Y", 5);

        final List<FDBStoredRecord<Message>> createdRecords = new ArrayList<>();
        createdRecords.add(saveAndSplitSimpleRecord(1L, smallValue, 1));
        createdRecords.add(saveAndSplitSimpleRecord(2L, smallValue, 2));
        createdRecords.add(saveAndSplitSimpleRecord(3L, bigValue, 3));
        createdRecords.add(saveAndSplitSimpleRecord(4L, smallValue, 4));
        createdRecords.add(saveAndSplitSimpleRecord(5L, bigValue, 5));
        createdRecords.add(saveAndSplitSimpleRecord(6L, bigValue, 6));
        createdRecords.add(saveAndSplitSimpleRecord(7L, smallValue, 7));
        createdRecords.add(saveAndSplitSimpleRecord(8L, smallValue, 8));
        createdRecords.add(saveAndSplitSimpleRecord(9L, smallValue, 9));

        // Scan one record at a time using continuations
        final List<FDBStoredRecord<Message>> scannedRecords = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            RecordCursorIterator<FDBStoredRecord<Message>> messageCursor = recordStore.scanRecords(null,
                    new ScanProperties(ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build()))
                    .asIterator();
            while (messageCursor.hasNext()) {
                scannedRecords.add(messageCursor.next());
                messageCursor = recordStore.scanRecords(messageCursor.getContinuation(), new ScanProperties(
                        ExecuteProperties.newBuilder()
                                .setReturnedRowLimit(1)
                                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                .build())).asIterator();
            }
            commit(context);
        }
        assertEquals(createdRecords, scannedRecords);
    }

    @Test
    public void testSplitSkip() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            commit(context);
        }

        final String bigValue = Strings.repeat("X", SplitHelper.SPLIT_RECORD_SIZE + 10);
        final String smallValue = Strings.repeat("Y", 5);

        final List<FDBStoredRecord<Message>> createdRecords = new ArrayList<>();
        createdRecords.add(saveAndSplitSimpleRecord(1L, smallValue, 1));
        createdRecords.add(saveAndSplitSimpleRecord(2L, smallValue, 2));
        createdRecords.add(saveAndSplitSimpleRecord(3L, bigValue, 3));
        createdRecords.add(saveAndSplitSimpleRecord(4L, smallValue, 4));
        createdRecords.add(saveAndSplitSimpleRecord(5L, bigValue, 5));
        createdRecords.add(saveAndSplitSimpleRecord(6L, bigValue, 6));
        createdRecords.add(saveAndSplitSimpleRecord(7L, smallValue, 7));
        createdRecords.add(saveAndSplitSimpleRecord(8L, smallValue, 8));
        createdRecords.add(saveAndSplitSimpleRecord(9L, smallValue, 9));

        // Scan one record at a time using continuations
        final List<FDBStoredRecord<Message>> scannedRecords = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            RecordCursorIterator<FDBStoredRecord<Message>> messageCursor = recordStore.scanRecords(null,
                            new ScanProperties(ExecuteProperties.newBuilder()
                                    .setReturnedRowLimit(1)
                                    .setSkip(scannedRecords.size())
                                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                    .build()))
                    .asIterator();
            while (messageCursor.hasNext()) {
                scannedRecords.add(messageCursor.next());
                messageCursor = recordStore.scanRecords(null, new ScanProperties(
                        ExecuteProperties.newBuilder()
                                .setReturnedRowLimit(1)
                                .setSkip(scannedRecords.size())
                                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                .build())).asIterator();
            }
            commit(context);
        }
        assertEquals(createdRecords, scannedRecords);
    }

    @ParameterizedTest(name = "testSaveRecordWithDifferentSplits[{0}]")
    @MethodSource("testConfigs")
    public void testSaveRecordWithDifferentSplits(SplitRecordsTestConfig testConfig) {
        this.testConfig = testConfig;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            commit(context);
        }

        final long recno = 1;
        final String shortString = Strings.repeat("x", 10);
        final String mediumString = Strings.repeat("y", SplitHelper.SPLIT_RECORD_SIZE + 10);
        final String longString = Strings.repeat("y", SplitHelper.SPLIT_RECORD_SIZE * 2 + 10);

        // unsplit
        saveAndCheckSplitSimpleRecord(recno, shortString, 123);
        // ... -> split
        saveAndCheckSplitSimpleRecord(recno, mediumString, 456);
        // ... -> longer split
        saveAndCheckSplitSimpleRecord(recno, longString, 789);
        // ... -> shorter split
        saveAndCheckSplitSimpleRecord(recno, mediumString, 456);
        // ... -> unsplit
        saveAndCheckSplitSimpleRecord(recno, shortString, 123);
        // ... -> deleted
        deleteAndCheckSplitSimpleRecord(recno);

        // long split
        saveAndCheckSplitSimpleRecord(recno, longString, 789);
        // ... -> unsplit
        saveAndCheckSplitSimpleRecord(recno, shortString, 123);
        // ... -> long split
        saveAndCheckSplitSimpleRecord(recno, longString, 789);
        // ... -> deleted
        deleteAndCheckSplitSimpleRecord(recno);

        // Check corruption with no split
        saveAndCheckCorruptSplitSimpleRecord(recno, shortString, 1066);
        //    "       "     with a short split
        saveAndCheckCorruptSplitSimpleRecord(recno, mediumString, 1415);
        //    "       "     with a long split
        saveAndCheckCorruptSplitSimpleRecord(recno, longString, 1066);
        // and delete
        deleteAndCheckSplitSimpleRecord(recno);
    }

    private void saveAndCheckSplitSimpleRecord(long recno, String strValue, int numValue) {
        FDBStoredRecord<Message> savedRecord = saveAndSplitSimpleRecord(recno, strValue, numValue);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            FDBStoredRecord<Message> loadedRecord = recordStore.loadRecord(Tuple.from(recno));
            assertEquals(savedRecord, loadedRecord);

            List<FDBStoredRecord<Message>> scannedRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(Collections.singletonList(savedRecord), scannedRecords);

            List<FDBStoredRecord<Message>> scanOneRecord = recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())).asList().join();
            assertEquals(Collections.singletonList(savedRecord), scanOneRecord);

            List<FDBStoredRecord<Message>> reversedScannedRecords = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().join();
            assertEquals(Collections.singletonList(savedRecord), reversedScannedRecords);

            List<FDBStoredRecord<Message>> reversedScannedOneRecord = recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build(), true)).asList().join();
            assertEquals(Collections.singletonList(savedRecord), reversedScannedOneRecord);

            commit(context);
        }
    }

    private void runAndCheckSplitException(TestHelpers.DangerousRunnable runnable, String msgPrefix, String failureMessage) {
        try {
            runnable.run();
            fail(failureMessage);
        } catch (Exception e) {
            RuntimeException runE = FDBExceptions.wrapException(e);
            if (runE instanceof RecordCoreException) {
                assertThat(runE.getMessage(), startsWith(msgPrefix));
            } else {
                throw runE;
            }
        }
    }

    private void saveAndCheckCorruptSplitSimpleRecord(long recno, String strValue, int numValue) {
        FDBStoredRecord<Message> savedRecord = saveAndSplitSimpleRecord(recno, strValue, numValue);
        if (!savedRecord.isSplit()) {
            return;
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            // Corrupt the data by removing the first key.
            byte[] key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, recno, SplitHelper.START_SPLIT_RECORD));
            context.ensureActive().clear(key);

            if (context.getPropertyStorage().getPropertyValue(FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS)) {
                // If we load via single key gets, we cannot distinguish between a record where the first split
                // point is gone from an empty record because we do not a follow up scan. The whole point of this
                // execution is to avoid those scans, so there's not much that can be done
                assertNull(recordStore.loadRecord(Tuple.from(recno)));
            } else {
                runAndCheckSplitException(() -> recordStore.loadRecord(Tuple.from(recno)),
                        "Found split record without start", "Loaded split record missing start key");
            }
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get(),
                    "Found split record without start", "Scanned split records missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                            ExecuteProperties.newBuilder()
                                    .setReturnedRowLimit(1)
                                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                    .build())).asList().get(),
                    "Found split record without start", "Scanned one split record missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().get(),
                    "Found split record without start", "Scanned split records in reverse missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                            ExecuteProperties.newBuilder()
                                    .setReturnedRowLimit(1)
                                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                    .build(), true)).asList().get(),
                    "Found split record without start", "Scanned one split record in reverse missing start key");

            // Redo the scans with non-corrupt elements on either side of the corrupted split record.
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recno - 1).build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recno + 1).build());


            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get(),
                    "Found split record without start", "Scanned split records missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().get(),
                    "Found split record without start", "Scanned split records in reverse missing start key");

            // DO NOT COMMIT
        }
        if (savedRecord.getKeyCount() <= 2) {
            return;
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            // Corrupt the data by removing a middle key.
            byte[] key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, recno, SplitHelper.START_SPLIT_RECORD + 1));
            context.ensureActive().clear(key);

            runAndCheckSplitException(() -> recordStore.loadRecord(Tuple.from(recno)),
                    "Split record segments out of order", "Loaded split record missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get(),
                    "Split record segments out of order", "Scanned split records missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                            ExecuteProperties.newBuilder()
                                    .setReturnedRowLimit(1)
                                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                    .build())).asList().get(),
                    "Split record segments out of order", "Scanned one split record missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().get(),
                    "Split record segments out of order", "Scanned split records in reverse missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                            ExecuteProperties.newBuilder()
                                    .setReturnedRowLimit(1)
                                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                    .build(), true)).asList().get(),
                    "Split record segments out of order", "Scanned one split record in reverse missing middle key");

            // DO NOT COMMIT
        }
    }

    private void deleteAndCheckSplitSimpleRecord(long recno) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            recordStore.deleteRecord(Tuple.from(recno));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            FDBStoredRecord<Message> loadedRecord = recordStore.loadRecord(Tuple.from(recno));
            assertNull(loadedRecord);

            List<FDBStoredRecord<Message>> scannedRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(Collections.emptyList(), scannedRecords);

            List<FDBStoredRecord<Message>> reverseScannedRecords = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().join();
            assertEquals(Collections.emptyList(), reverseScannedRecords);

            commit(context);
        }
    }

    private void openLongRecordStore(FDBRecordContext context) {
        createOrOpenRecordStore(context, RecordMetaData.build(TestRecords2Proto.getDescriptor()));
    }

    private FDBRecordStore openStoreForConflicts(final FDBRecordContext context, FormatVersion formatVersion, boolean splitLongRecords) {
        final RecordMetaDataHook hook = metaData -> metaData.setSplitLongRecords(splitLongRecords);
        return getStoreBuilder(context, simpleMetaData(hook))
                .setFormatVersion(formatVersion)
                .createOrOpen();
    }

    private void checkForConflicts(FormatVersion formatVersion, boolean splitLongRecords, @Nonnull Consumer<FDBRecordStore> operation1, @Nonnull Consumer<FDBRecordStore> operation2) {
        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            // Ensure the store is created here to avoid conflicts on the store header
            // and ensure that the format version to the parameter given
            FDBRecordStore store = openStoreForConflicts(context, formatVersion, splitLongRecords);
            commit(context);
            storeBuilder = store.asBuilder();
        }
        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            FDBRecordStore store1 = storeBuilder.copyBuilder().setContext(context1).open();
            FDBRecordStore store2 = storeBuilder.copyBuilder().setContext(context2).open();

            operation1.accept(store1);
            operation2.accept(store2);

            commit(context1);
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context2::commit,
                    "second transaction did not fail with a conflict when the format version was " + formatVersion + " and long records were" + (splitLongRecords ? "" : " not") + " split");
        }
    }

    @ParameterizedTest(name = "recordReadConflict [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionAndSplitArgs")
    public void recordReadConflict(FormatVersion formatVersion, boolean splitLongRecords) {
        SplitRecordsTestConfig.allConfigs().forEach(config -> {
            this.testConfig = config;
            checkForConflicts(formatVersion, splitLongRecords,
                    store1 -> store1.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build()),
                    store2 -> {
                        store2.addRecordReadConflict(Tuple.from(1066L));
                        store2.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1415L).build());
                    });
        });
    }

    @ParameterizedTest(name = "recordWriteConflict [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionAndSplitArgs")
    public void recordWriteConflict(FormatVersion formatVersion, boolean splitLongRecords) {
        SplitRecordsTestConfig.allConfigs().forEach(config -> {
            this.testConfig = config;
            checkForConflicts(formatVersion, splitLongRecords,
                    store1 -> store1.addRecordWriteConflict(Tuple.from(1066L)),
                    store2 -> {
                        store2.loadRecord(Tuple.from(1066L));
                        store2.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1415L).build());
                    });
        });
    }

    @ParameterizedTest(name = "recordDeleteConflict [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionAndSplitArgs")
    public void recordDeleteConflict(FormatVersion formatVersion, boolean splitLongRecords) {
        SplitRecordsTestConfig.allConfigs().forEach(config -> {
            this.testConfig = config;
            try (FDBRecordContext context = openContext()) {
                FDBRecordStore recordStore = openStoreForConflicts(context, formatVersion, splitLongRecords);
                // Insert a record here so that the delete logic (1) sees the record is there and then
                // (2) issues a real delete
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build());
                commit(context);
            }
            checkForConflicts(formatVersion, splitLongRecords,
                    store1 -> store1.deleteRecord(Tuple.from(1066L)),
                    store2 -> {
                        store2.loadRecord(Tuple.from(1066L));
                        store2.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1415L).build());
                    });
        });
    }

    @ParameterizedTest(name = "saveDeleteConflict [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionAndSplitArgs")
    public void saveDeleteConflict(FormatVersion formatVersion, boolean splitLongRecords) {
        SplitRecordsTestConfig.allConfigs().forEach(config -> {
            this.testConfig = config;
            try (FDBRecordContext context = openContext()) {
                FDBRecordStore recordStore = openStoreForConflicts(context, formatVersion, splitLongRecords);
                // Insert a record here so that the delete logic (1) sees the record is there and then
                // (2) issues a real delete
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build());
                commit(context);
            }
            // Ensure that deletes and saves to the same record conflict. This should be the case
            // regardless of whether the save or the delete is committed first
            checkForConflicts(formatVersion, splitLongRecords,
                    store1 -> store1.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed("foo").build()),
                    store2 -> store2.deleteRecord(Tuple.from(1066L)));
            checkForConflicts(formatVersion, splitLongRecords,
                    store1 -> store1.deleteRecord(Tuple.from(1066L)),
                    store2 -> store2.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed("bar").build()));
        });
    }
}
