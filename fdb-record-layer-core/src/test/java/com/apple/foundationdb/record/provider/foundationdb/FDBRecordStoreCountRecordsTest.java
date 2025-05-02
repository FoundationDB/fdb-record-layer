/*
 * FDBRecordStoreCountRecordsTest.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanArguments;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests related to built in functionality for getting the count of records in a store.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreCountRecordsTest extends FDBRecordStoreTestBase {

    @Test
    @SuppressWarnings("deprecation")
    public void testUpdateRecordCounts() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("path", "num", "rec_no")));
            builder.setRecordCountKey(field("header").nest(concat(field("path"), field("num"))));
            createOrOpenRecordStore(context, builder.getRecordMetaData());

            saveHeaderRecord(1, "/FirstPath", 0, "johnny");
            saveHeaderRecord(2, "/FirstPath", 0, "apple");
            saveHeaderRecord(3, "/LastPath", 2016, "seed");
            saveHeaderRecord(3, "/LastPath", 2017, "seed");

            saveHeaderRecord(4, "/SecondPath", 0, "cloud");
            saveHeaderRecord(5, "/SecondPath", 0, "apple");
            saveHeaderRecord(6, "/SecondPath", 0, "seed");
            saveHeaderRecord(7, "/SecondPath", 0, "johnny");

            assertEquals(8L, recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get().size());

            assertEquals(8L, recordStore.getSnapshotRecordCount().get().intValue());

            // Delete 2 records
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("/FirstPath")));

            assertEquals(6L, recordStore.getSnapshotRecordCount().get().intValue());

            // Delete 4 records
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("/SecondPath")));

            assertEquals(2L, recordStore.getSnapshotRecordCount().get().intValue());

            // Delete a single record
            recordStore.deleteRecordsWhere(Query.field("header").matches(
                    Query.and(
                            Query.field("path").equalsValue("/LastPath"),
                            Query.field("num").equalsValue(2016))));


            assertEquals(1L, recordStore.getSnapshotRecordCount().get().intValue());

            // Delete a single record
            recordStore.deleteRecordsWhere(Query.field("header").matches(
                    Query.and(
                            Query.field("path").equalsValue("/LastPath"),
                            Query.field("num").equalsValue(2017))));

            assertEquals(0L, recordStore.getSnapshotRecordCount().get().intValue());

            context.commit();
        }
    }

    @Test
    public void countRecordsIndex() {
        countRecords(true);
    }

    @Test
    public void countRecords() {
        countRecords(false);
    }

    private void countRecords(boolean useIndex) {
        final RecordMetaDataHook hook = countKeyHook(EmptyKeyExpression.EMPTY, useIndex, 0);

        saveRecords(simpleMetaData(hook), 0, 100,
                () -> assertUngroupedCount(0),
                () -> assertUngroupedCount(100));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertUngroupedCount(100);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 5; i++) {
                int recNo = i * 10;
                recordStore.deleteRecord(Tuple.from(recNo));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertUngroupedCount(95);
            commit(context);
        }
    }

    @Test
    public void countRecordsKeyedIndex() {
        countRecordsKeyed(true);
    }

    @Test
    public void countRecordsKeyed() {
        countRecordsKeyed(false);
    }

    private void countRecordsKeyed(boolean useIndex) {
        final KeyExpression key = field("num_value_3_indexed");
        final RecordMetaDataHook hook = countKeyHook(key, useIndex, 0);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                recordStore.saveRecord(makeRecord(i, 0, i % 5));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertUngroupedCount(100);
            assertEquals(20, recordStore.getSnapshotRecordCount(key, Key.Evaluated.scalar(1)).join().longValue());
            commit(context);
        }
    }

    @Test
    public void recountAndClearRecordsIndex() throws Exception {
        recountAndClearRecords(true);
    }

    @Test
    public void recountAndClearRecords() throws Exception {
        recountAndClearRecords(false);
    }

    private void recountAndClearRecords(boolean useIndex) throws Exception {
        final CountMetaDataHook countMetaDataHook = new CountMetaDataHook();
        countMetaDataHook.baseHook = metaData -> metaData.removeIndex(globalCountIndex().getName());

        final int startingPoint = 7890;
        final int value1 = 12345;
        final int value2 = 54321;
        final int value3 = 24567;
        try (FDBRecordContext context = openContext()) {
            // Simulate the state the store would be in if this were done before counting was added.
            recordStore = getStoreBuilder(context, simpleMetaData(countMetaDataHook))
                    .setFormatVersion(FormatVersion.INFO_ADDED)
                    .uncheckedOpen();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();

            for (int i = 0; i < 90; i++) {
                recordStore.saveRecord(makeRecord(i + startingPoint, value1, i % 5));
            }
            commit(context);
        }

        KeyExpression key3 = field("num_value_3_indexed");
        countMetaDataHook.metaDataVersion++;
        countMetaDataHook.baseHook = countKeyHook(key3, useIndex, countMetaDataHook.metaDataVersion);

        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(countMetaDataHook))
                    .setFormatVersion(FormatVersion.RECORD_COUNT_ADDED)
                    .uncheckedOpen();

            for (int i = 90; i < 100; i++) {
                recordStore.saveRecord(makeRecord(i + startingPoint, value2, i % 5));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(countMetaDataHook))
                    .setFormatVersion(FormatVersion.RECORD_COUNT_ADDED)
                    .uncheckedOpen();

            assertEquals(10, recordStore.getSnapshotRecordCount().join().longValue(), "should only see new records");
            commit(context);
        }

        // Need to allow immediate rebuild of new count index.
        final FDBRecordStoreBase.UserVersionChecker alwaysEnabled = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(1);
            }

            @Deprecated
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                throw new RecordCoreException("deprecated checkUserVersion called");
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.READABLE;
            }
        };

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore.checkVersion(alwaysEnabled, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join(); // Index is rebuilt here automatically in useIndex case

            assertEquals(100, recordStore.getSnapshotRecordCount().join().longValue(), "should see all records");
            assertEquals(20, recordStore.getSnapshotRecordCount(key3, Key.Evaluated.scalar(2)).join().longValue());
            commit(context);
        }

        KeyExpression key2 = field("num_value_2");
        countMetaDataHook.metaDataVersion++;
        countMetaDataHook.baseHook = countKeyHook(key2, useIndex, countMetaDataHook.metaDataVersion);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            if (useIndex) {
                // Need to manually rebuild index in this case.
                Index index = recordStore.getRecordMetaData().getIndex("record_count");
                recordStore.rebuildIndex(index).get();
                assertThat(recordStore.isIndexReadable(index), is(true));
            }

            assertEquals(100, recordStore.getSnapshotRecordCount().join().longValue(), "should see all records");

            for (int i = 0; i < 32; i++) {
                recordStore.saveRecord(makeRecord(i + startingPoint + 1000, value3, 0));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            assertEquals(90, recordStore.getSnapshotRecordCount(key2, Key.Evaluated.scalar(value1)).join().longValue());
            assertEquals(10, recordStore.getSnapshotRecordCount(key2, Key.Evaluated.scalar(value2)).join().longValue());
            assertEquals(32, recordStore.getSnapshotRecordCount(key2, Key.Evaluated.scalar(value3)).join().longValue());
        }

        KeyExpression pkey = field("rec_no");
        countMetaDataHook.metaDataVersion++;
        countMetaDataHook.baseHook = countKeyHook(pkey, useIndex, countMetaDataHook.metaDataVersion);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.NONE).join();

            if (useIndex) {
                // Need to manually rebuild index in this case.
                Index index = recordStore.getRecordMetaData().getIndex("record_count");
                recordStore.rebuildIndex(index).get();
                assertThat(recordStore.isIndexReadable(index), is(true));
            }

            assertUngroupedCount(132);
            for (int i = 0; i < 100; i++) {
                assertEquals(1, recordStore.getSnapshotRecordCount(pkey, Key.Evaluated.scalar(i + startingPoint)).join().longValue(), "Incorrect when i is " + i);
            }
        }
    }

    @Test
    public void addCountIndex() throws Exception {
        RecordMetaDataHook removeCountHook = metaData -> metaData.removeIndex(COUNT_INDEX_NAME);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, removeCountHook);

            for (int i = 0; i < 10; i++) {
                recordStore.saveRecord(makeRecord(i, 1066, i % 5));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, removeCountHook);
            recordStore.getSnapshotRecordCount().get();
            fail("evaluated count without index or key");
        } catch (RecordCoreException e) {
            assertThat(e.getMessage(), containsString("requires appropriate index"));
        }

        RecordMetaDataHook hook = countKeyHook(field("num_value_3_indexed"), true, 10);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            Index countIndex = recordStore.getRecordMetaData().getIndex("record_count");
            assertThat(recordStore.getRecordStoreState().isReadable(countIndex), is(false));
            assertThat(recordStore.getRecordStoreState().isDisabled(countIndex), is(true));
            RecordCoreException e = assertThrows(RecordCoreException.class, () -> recordStore.getSnapshotRecordCount().get());
            assertThat(e.getMessage(), containsString("requires appropriate index"));
        }

        // Build the index
        try (OnlineIndexer onlineIndexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "record_count")) {
            onlineIndexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            Index countIndex = recordStore.getRecordMetaData().getIndex("record_count");
            assertThat(recordStore.getRecordStoreState().isWriteOnly(countIndex), is(false));
            assertEquals(10L, recordStore.getSnapshotRecordCount().get().longValue());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void addCountKey() throws Exception {
        RecordMetaDataHook removeCountHook = metaData -> metaData.removeIndex(COUNT_INDEX_NAME);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, removeCountHook);

            for (int i = 0; i < 10; i++) {
                recordStore.saveRecord(makeRecord(i, 1066, i % 5));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, removeCountHook);
            recordStore.getSnapshotRecordCount().get();
            fail("evaluated count without index or key");
        } catch (RecordCoreException e) {
            assertThat(e.getMessage(), containsString("requires appropriate index"));
        }

        AtomicInteger versionCounter = new AtomicInteger(recordStore.getRecordMetaData().getVersion());
        RecordMetaDataHook hook = md -> {
            md.setRecordCountKey(field("num_value_3_indexed"));
            md.setVersion(md.getVersion() + versionCounter.incrementAndGet());
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertThat(timer.getCount(FDBStoreTimer.Events.RECOUNT_RECORDS), equalTo(1));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertThat(timer.getCount(FDBStoreTimer.Events.RECOUNT_RECORDS), equalTo(0));
            assertEquals(10L, recordStore.getSnapshotRecordCount().get().longValue());
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            // Before it was deprecated, this is how a key would have been written.
            RecordMetaDataProto.DataStoreInfo.Builder infoBuilder = recordStore.getRecordStoreState().getStoreHeader().toBuilder();
            infoBuilder.getRecordCountKeyBuilder().getFieldBuilder().clearNullInterpretation();
            recordStore.saveStoreHeader(infoBuilder.build());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertThat(timer.getCount(FDBStoreTimer.Events.RECOUNT_RECORDS), equalTo(0));
            assertEquals(10L, recordStore.getSnapshotRecordCount().get().longValue());
        }
    }

    @Test
    public void testCountRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            saveSimpleRecord2("a", 1);

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            assertEquals(1, (int)recordStore.countRecords(
                    null, null, EndpointType.TREE_START, EndpointType.TREE_END).join());
            assertEquals(1, (int)recordStore.countRecords(
                    Tuple.from("a"), Tuple.from("a"),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE).join());

            saveSimpleRecord2("b", 1);
            saveSimpleRecord2("c", 1);

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            assertEquals(3, (int)recordStore.countRecords(
                    null, null, EndpointType.TREE_START, EndpointType.TREE_END).join());
            assertEquals(1, (int)recordStore.countRecords(
                    Tuple.from("a"), Tuple.from("a"),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE).join());
            assertEquals(2, (int)recordStore.countRecords(
                    Tuple.from("a"), Tuple.from("c"),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE).join());
        }

        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            assertEquals(3, (int)recordStore.countRecords(
                    null, null, EndpointType.TREE_START, EndpointType.TREE_END, null,
                    new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(IsolationLevel.SNAPSHOT).build())).join());

            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder()
                    .setStrValueIndexed("xz")
                    .setEtag(1)
                    .build());
            try (FDBRecordContext context2 = openContext()) {
                FDBRecordStore recordStore2 = openNewUnionRecordStore(context2);

                recordStore2.loadRecord(Tuple.from("xz"));
                recordStore2.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder()
                        .setStrValueIndexed("ab")
                        .setEtag(1)
                        .build());
                context2.commit();
            }
            context.commit();
        }

    }

    private void checkRecordUpdateCounts(HashMap<Integer, Integer> expectedCounts,
                                         RecordMetaDataHook hook,
                                         KeyExpression key) {
        int sum = expectedCounts.values().stream().mapToInt(Number::intValue).sum();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertEquals(sum, recordStore.getSnapshotRecordUpdateCount().join().longValue());

            expectedCounts.forEach((bucketNum, expected) ->
                    assertEquals(expectedCounts.get(bucketNum).longValue(),
                            recordStore.getSnapshotRecordUpdateCount(key, Key.Evaluated.scalar(bucketNum)).join().longValue()));
        }
    }

    @Test
    public void countRecordUpdates() {
        final KeyExpression key = field("num_value_3_indexed");
        final RecordMetaDataHook hook = countUpdatesKeyHook(key, 0);
        HashMap<Integer, Integer> expectedCountBuckets = new HashMap<>();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(0, recordStore.getSnapshotRecordUpdateCount().join().longValue());

            // Create 100 records
            for (int i = 0; i < 100; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
                expectedCountBuckets.put(numBucket, expectedCountBuckets.getOrDefault(numBucket, 0) + 1);
            }
            commit(context);
        }

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);

        // Delete 5 records, this shouldn't change the counts
        deleteRecords(simpleMetaData(hook), 95, 100);

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Update 10 records
            for (int i = 0; i < 10; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
                expectedCountBuckets.put(numBucket, expectedCountBuckets.getOrDefault(numBucket, 0) + 1);
            }
            commit(context);
        }

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Update and create (or re-create) some records
            for (int i = 90; i < 110; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
                expectedCountBuckets.put(numBucket, expectedCountBuckets.getOrDefault(numBucket, 0) + 1);
            }
            // Delete 5 records
            for (int i = 20; i < 25; i++) {
                recordStore.deleteRecord(Tuple.from(i));
            }
            commit(context);
        }

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);
    }

    @Test
    void countKeyOnNewStore() throws ExecutionException, InterruptedException {
        final KeyExpression countKey = field("num_value_2");
        RecordMetaDataHook origHook = countKeyHook(countKey, false, 0);

        Map<Integer, Long> countByNumValue2 = new HashMap<>();
        long totalCount;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, origHook);
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.RECOUNT_RECORDS));

            assertEquals(0, recordStore.getSnapshotRecordCount().get());

            for (int i = 0; i < 10; i++) {
                assertEquals(0L, recordStore.getSnapshotRecordCount(countKey, Key.Evaluated.scalar(i)).get());
            }

            Random r = new Random();
            for (int i = 0; i < 50; i++) {
                int numValue2 = r.nextInt(10);
                Message rec;
                if (r.nextBoolean()) {
                    rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setNumValue2(numValue2)
                            .setRecNo(r.nextLong())
                            .build();
                } else {
                    rec = TestRecords1Proto.MyOtherRecord.newBuilder()
                            .setNumValue2(numValue2)
                            .setRecNo(r.nextLong())
                            .build();
                }
                recordStore.saveRecord(rec);
                countByNumValue2.compute(numValue2, (k, count) -> count == null ? 1L : count + 1L);
            }

            totalCount = countByNumValue2.values().stream()
                    .mapToLong(Long::longValue)
                    .sum();

            assertEquals(totalCount, recordStore.getSnapshotRecordCount().get());
            for (int i = 0; i < 10; i++) {
                assertEquals(countByNumValue2.getOrDefault(i, 0L),
                        recordStore.getSnapshotRecordCount(countKey, Key.Evaluated.scalar(i)).get());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, origHook);
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.RECOUNT_RECORDS));

            assertEquals(totalCount, recordStore.getSnapshotRecordCount().get());
            for (int i = 0; i < 10; i++) {
                assertEquals(countByNumValue2.getOrDefault(i, 0L),
                        recordStore.getSnapshotRecordCount(countKey, Key.Evaluated.scalar(i)).get());
            }
        }
    }

    static Stream<Arguments> disableRecordCountKey() {
        return BooleanArguments.of("fromWriteOnly", "fromReadable")
                .flatMap(fromWriteOnly -> BooleanArguments.of("with fallback")
                        .map(hasFallBack -> Arguments.of(fromWriteOnly, hasFallBack)));
    }

    // TODO test with old format version
    // TODO test more with grouped
    @ParameterizedTest
    @MethodSource
    void disableRecordCountKey(boolean fromWriteOnly, boolean hasFallback) {
        final RecordMetaDataBuilder metaDataBuilder = addUngroupedRecordCountKey(simpleMetaDataBuilder());
        if (hasFallback) {
            addUngroupedCountIndex(metaDataBuilder);
        }
        final RecordMetaData metaData = metaDataBuilder.build();
        saveRecords(metaData, 0, 100,
                () -> assertUngroupedCount(0),
                () -> assertUngroupedCount(100));

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertUngroupedCount(100);
        }

        rawAssertHasRecordCount(metaData, true);
        if (fromWriteOnly) {
            updateRecordCountState(metaData, RecordMetaDataProto.DataStoreInfo.RecordCountState.WRITE_ONLY);
        }

        updateRecordCountState(metaData, RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED);

        rawAssertHasRecordCount(metaData, false);

        // We should be able to save new records without issue
        saveRecords(metaData, 100, 110, () -> { }, () -> { });

        // We should be able to delete some records without issue
        deleteRecords(metaData, 95, 100);

        // We cannot get the record count because there is no index, or recordCountKey
        if (hasFallback) {
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, metaData);
                assertUngroupedCount(105);
            }
        } else {
            assertCannotGetUngroupedRecordCount(metaData);
        }

        // ensure that the updates did not write anything to the RecordCountKey space
        rawAssertHasRecordCount(metaData, false);
    }

    static Stream<Arguments> shouldNotRebuildIndexesWhenNotReadable() {
        return BooleanArguments.of("startsWithCountKey")
                .flatMap(startsWithCountKey -> BooleanArguments.of("disabled", "WriteOnly")
                        .map(disabled -> Arguments.of(startsWithCountKey, disabled)));
    }

    @ParameterizedTest
    @MethodSource
    void shouldNotRebuildIndexesWhenNotReadable(boolean startsWithCountKey, boolean disabled) {
        // If disabled or WriteOnly, rebuilding indexes should behave the same as if the RecordCountKey didn't exist
        final RecordMetaDataBuilder builder;
        if (startsWithCountKey) {
            builder = addUngroupedRecordCountKey(simpleMetaDataBuilder());
        } else {
            builder = simpleMetaDataBuilder();
        }
        final RecordMetaData initialMetaData = builder.build();
        saveRecords(initialMetaData, 0, 100,
                () -> {
                    if (startsWithCountKey) {
                        assertUngroupedCount(0);
                    }
                },
                () -> {
                    if (startsWithCountKey) {
                        assertUngroupedCount(100);
                    }
                });

        if (startsWithCountKey) {
            updateRecordCountState(initialMetaData, disabled ? RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED :
                    RecordMetaDataProto.DataStoreInfo.RecordCountState.WRITE_ONLY);
        }

        final Index index = new Index("OnNum", "num_value_2");
        builder.addIndex("MySimpleRecord", index);
        final RecordMetaData newMetaData = builder.build();

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, newMetaData);
            assertEquals(IndexState.DISABLED, recordStore.getAllIndexStates().get(index));
            commit(context);
        }
    }

    @Test
    void recordCountKeyWriteOnly() {
        final RecordMetaData metaData = addUngroupedRecordCountKey(simpleMetaDataBuilder()).build();
        saveRecords(metaData, 0, 100,
                () -> assertUngroupedCount(0),
                () -> assertUngroupedCount(100));

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertUngroupedCount(100);
        }

        updateRecordCountState(metaData, RecordMetaDataProto.DataStoreInfo.RecordCountState.WRITE_ONLY);

        // We should be able to save new records without issue
        saveRecords(metaData, 100, 110, () -> { }, () -> { });

        // We should be able to delete some records without issue
        deleteRecords(metaData, 95, 100);

        // We cannot get the record count because there is no index, or recordCountKey
        assertCannotGetUngroupedRecordCount(metaData);

        updateRecordCountState(metaData, RecordMetaDataProto.DataStoreInfo.RecordCountState.READABLE);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertUngroupedCount(105); // we added 10 and deleted 5
        }
    }

    @Test
    void fromDisabledShouldFail() {
        final RecordMetaData metaData = addUngroupedRecordCountKey(simpleMetaDataBuilder()).build();
        saveRecords(metaData, 0, 100,
                () -> assertUngroupedCount(0),
                () -> assertUngroupedCount(100));

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            assertUngroupedCount(100);
        }

        updateRecordCountState(metaData, RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED);

        // Once disabled we cannot change it to not disabled
        // We could implement something to rebuild the data, but it's deprecated, and that's a fair amount of work
        try (FDBRecordContext context1 = openContext()) {
            createOrOpenRecordStore(context1, metaData);
            assertThrows(RecordCoreException.class, this::markRecordCountWriteOnly);
            context1.commit();
        }

        try (FDBRecordContext context1 = openContext()) {
            createOrOpenRecordStore(context1, metaData);
            assertThrows(RecordCoreException.class, this::markRecordCountReadable);
            context1.commit();
        }
    }

    @ParameterizedTest
    @EnumSource(RecordMetaDataProto.DataStoreInfo.RecordCountState.class)
    @SuppressWarnings("deprecation")
    void deleteWhereWhenNotReadable(RecordMetaDataProto.DataStoreInfo.RecordCountState newState) {
        // If disabled or WriteOnly, deleteWhere should work
        final RecordMetaDataBuilder recordMetaDataBuilder = simpleMetaDataBuilder();
        final String groupingField = "num_value_3_indexed";
        final FieldKeyExpression keyExpression = field(groupingField);
        recordMetaDataBuilder.setRecordCountKey(keyExpression);
        recordMetaDataBuilder.getRecordType("MySimpleRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields(groupingField, "rec_no"));
        recordMetaDataBuilder.getRecordType("MyOtherRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields(groupingField, "rec_no"));
        recordMetaDataBuilder.removeIndex("MySimpleRecord$num_value_unique");
        recordMetaDataBuilder.removeIndex("MySimpleRecord$num_value_3_indexed");
        recordMetaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
        final RecordMetaData metaData = recordMetaDataBuilder.build();
        saveRecords(metaData, 0, 100,
                () -> assertGroupedCount(0, keyExpression, Key.Evaluated.concatenate(1)),
                () -> {
                    assertGroupedCount(20, keyExpression, Key.Evaluated.concatenate(1));
                    assertGroupedCount(20, keyExpression, Key.Evaluated.concatenate(2));
                    assertGroupedCount(20, keyExpression, Key.Evaluated.concatenate(3));
                });

        updateRecordCountState(metaData, newState);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            recordStore.deleteRecordsWhere(Query.field(groupingField).equalsValue(2));
            commit(context);
        }

        if (newState != RecordMetaDataProto.DataStoreInfo.RecordCountState.READABLE) {
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, metaData);
                final Key.Evaluated value = Key.Evaluated.concatenate(1);
                final RecordCoreException recordCoreException = assertThrows(RecordCoreException.class,
                        () -> recordStore.getSnapshotRecordCount(keyExpression, value).join());
                assertThat(recordCoreException.getMessage(), containsString("requires appropriate index"));
            }
        }
        if (newState == RecordMetaDataProto.DataStoreInfo.RecordCountState.WRITE_ONLY) {
            updateRecordCountState(metaData, RecordMetaDataProto.DataStoreInfo.RecordCountState.READABLE);
        }

        if (newState != RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED) {
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, metaData);
                assertGroupedCount(20, keyExpression, Key.Evaluated.concatenate(1));
                assertGroupedCount(0, keyExpression, Key.Evaluated.concatenate(2));
                assertGroupedCount(20, keyExpression, Key.Evaluated.concatenate(3));
                commit(context);
            }
        }
    }

    private void updateRecordCountState(final RecordMetaData metaData,
                                        final RecordMetaDataProto.DataStoreInfo.RecordCountState newState) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            recordStore.updateRecordCountStateAsync(newState).join();
            context.commit();
        }
    }


    private Void markRecordCountWriteOnly() {
        return recordStore.updateRecordCountStateAsync(RecordMetaDataProto.DataStoreInfo.RecordCountState.WRITE_ONLY).join();
    }

    private Void markRecordCountReadable() {
        return recordStore.updateRecordCountStateAsync(RecordMetaDataProto.DataStoreInfo.RecordCountState.READABLE).join();
    }

    private void rawAssertHasRecordCount(final RecordMetaData metaData, final boolean hasRecordCounts) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            final Tuple tuple = Tuple.from(FDBRecordStoreKeyspace.RECORD_COUNT.key());
            final byte[] subspace = tuple.pack(recordStore.getSubspace().pack());
            final List<KeyValue> counts = context.ensureActive()
                    .getRange(subspace,
                            ByteArrayUtil.strinc(subspace))
                    .asList().join();

            assertEquals(counts.isEmpty(), !hasRecordCounts, counts.toString());
        }
    }

    private void assertCannotGetUngroupedRecordCount(final RecordMetaData metaData) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            final RecordCoreException recordCoreException = assertThrows(RecordCoreException.class,
                    () -> recordStore.getSnapshotRecordCount().get());
            assertThat(recordCoreException.getMessage(), containsString("requires appropriate index"));
        }
    }

    private void assertUngroupedCount(final int expected) {
        assertEquals(expected, recordStore.getSnapshotRecordCount().join().longValue());
    }

    private void assertGroupedCount(final int expected, final KeyExpression key, final Key.Evaluated value) {
        assertEquals(expected, recordStore.getSnapshotRecordCount(key, value).join().longValue());
    }

    private void saveRecords(final RecordMetaData metaData, final int start, final int end,
                             final Runnable beforeHook, final Runnable afterHook) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            beforeHook.run();

            for (int i = start; i < end; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
            }

            afterHook.run();
            commit(context);
        }
    }

    private void deleteRecords(final RecordMetaData metaData, final int start, final int end) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (int i = start; i < end; i++) {
                recordStore.deleteRecord(Tuple.from(i));
            }
            commit(context);
        }
    }

    private TestRecords1Proto.MySimpleRecord makeRecord(long recordNo, int numValue2, int numValue3Indexed) {
        TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
        recBuilder.setRecNo(recordNo);
        recBuilder.setNumValue2(numValue2);
        recBuilder.setNumValue3Indexed(numValue3Indexed);
        return recBuilder.build();
    }

    @SuppressWarnings("deprecation")
    private static RecordMetaDataBuilder addUngroupedCountIndex(@Nonnull final RecordMetaDataBuilder recordMetaDataBuilder) {
        addCountIndex(EmptyKeyExpression.EMPTY, -1, recordMetaDataBuilder);
        recordMetaDataBuilder.setRecordCountKey(EmptyKeyExpression.EMPTY);
        return recordMetaDataBuilder;
    }

    @SuppressWarnings("deprecation")
    private static RecordMetaDataBuilder addUngroupedRecordCountKey(@Nonnull final RecordMetaDataBuilder recordMetaDataBuilder) {
        recordMetaDataBuilder.setRecordCountKey(EmptyKeyExpression.EMPTY);
        return recordMetaDataBuilder;
    }

    @Nonnull
    private static RecordMetaDataBuilder simpleMetaDataBuilder() {
        return RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
    }

    // Get a new metadata version every time we change the count key definition.
    static class CountMetaDataHook implements RecordMetaDataHook {
        int metaDataVersion = 100;
        RecordMetaDataHook baseHook = null;

        @Override
        public void apply(RecordMetaDataBuilder metaData) {
            if (baseHook != null) {
                baseHook.apply(metaData);
            }
            metaData.setVersion(metaDataVersion);
        }
    }

    private static RecordMetaDataHook countUpdatesKeyHook(KeyExpression key, int indexVersion) {
        return md -> {
            md.removeIndex(COUNT_UPDATES_INDEX_NAME);
            addIndex("record_update_count", key, IndexTypes.COUNT_UPDATES, indexVersion, md);
        };
    }

    @SuppressWarnings("deprecation")
    private static RecordMetaDataHook countKeyHook(KeyExpression key, boolean useIndex, int indexVersion) {
        if (useIndex) {
            return md -> {
                md.removeIndex(COUNT_INDEX_NAME);
                addCountIndex(key, indexVersion, md);
            };
        } else {
            return md -> md.setRecordCountKey(key);
        }
    }

    private static void addCountIndex(final KeyExpression key, final int indexVersion,
                                      final RecordMetaDataBuilder metaData) {
        addIndex("record_count", key, IndexTypes.COUNT, indexVersion, metaData);
    }

    private static void addIndex(final String record_update_count, final KeyExpression key, final String countUpdates,
                                 final int indexVersion, final RecordMetaDataBuilder metaData) {
        Index index = new Index(record_update_count, new GroupingKeyExpression(key, 0), countUpdates);
        index.setLastModifiedVersion(indexVersion);
        metaData.addUniversalIndex(index);
    }

}
