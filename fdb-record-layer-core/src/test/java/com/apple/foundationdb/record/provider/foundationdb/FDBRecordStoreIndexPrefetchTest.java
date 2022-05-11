/*
 * FDBRecordStoreIndexPrefetchTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.MappedKeyValue;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

/**
 * A test for the Index Prefetch FDB API.
 */
@Tag(Tags.RequiresFDB)
class FDBRecordStoreIndexPrefetchTest extends FDBRecordStoreQueryTestBase {
    private static RecordQuery NUM_VALUES_LARGER_THAN_990 = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(990))
            .build();

    private static RecordQuery NUM_VALUES_LARGER_THAN_1000_REVERSE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(1000))
            .setSort(Key.Expressions.field("num_value_unique"), true)
            .build();

    private static RecordQuery NUM_VALUES_LARGER_THAN_990_REVERSE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(990))
            .setSort(Key.Expressions.field("num_value_unique"), true)
            .build();

    private static RecordQuery STR_VALUE_EVEN = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("str_value_indexed").equalsValue("even"))
            .build();

    private boolean useSplitRecords = true;

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetup(simpleMetadataHook);
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSimpleIndexTest(RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSimpleIndexReverseTest(RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990_REVERSE, useIndexPrefetch);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
    }

    @ParameterizedTest(name = "indexPrefetchComplexIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchComplexIndexTest(RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(STR_VALUE_EVEN, useIndexPrefetch);
        executeAndVerifyData(plan, 50, (rec, i) -> {
            int primaryKey = i * 2;
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, "even", numValue, "MySimpleRecord$str_value_indexed", "even", primaryKey); // we are filtering out all odd entries, so count*2 are the keys of the even ones
        });
    }

    @ParameterizedTest(name = "indexPrefetchWithContinuationTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchWithContinuationTest(RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(5)
                .build();

        // First iteration - first 5 records
        byte[] continuation = executeAndVerifyData(plan, null, executeProperties, 5, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
        // Second iteration - last 5 records
        continuation = executeAndVerifyData(plan, continuation, executeProperties, 5, (rec, i) -> {
            int primaryKey = 4 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
        // Third iteration - no more values to read
        continuation = executeAndVerifyData(plan, continuation, executeProperties, 0, (rec, i) -> {
        });
        assertNull(continuation);
    }

    /*
     * Test continuation where the continued plan uses a different prefetch mode than the original plan.
     */
    @Test
    void indexPrefetchWithMixedContinuationTest() throws Exception {
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH);
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.NONE);
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(4)
                .build();

        // First iteration - first 4 records
        byte[] continuation = executeAndVerifyData(planWithPrefetch, null, executeProperties, 4, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
        // Second iteration - second 4 records
        continuation = executeAndVerifyData(planWithScan, continuation, executeProperties, 4, (rec, i) -> {
            int primaryKey = 5 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
        // Third iteration - last 2 records
        continuation = executeAndVerifyData(planWithPrefetch, continuation, executeProperties, 2, (rec, i) -> {
            int primaryKey = 1 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
        assertNull(continuation);
    }

    @ParameterizedTest(name = "indexPrefetchByteLimitContinuation(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    @Disabled // This test is inconsistently failing (for the NONE case)
    void indexPrefetchByteLimitContinuation(RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);
        // TODO: Why should the index prefetch take so many more bytes to scan the same number of records? Maybe the index scan counts the records and the fetch does not?
        int scanBytesLimit = (useIndexPrefetch == RecordQueryPlannerConfiguration.IndexPrefetchUse.NONE) ? 350 : 1300;
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setScannedBytesLimit(scanBytesLimit)
                .build();

        byte[] continuation = executeAndVerifyData(plan, null, executeProperties, 8, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
        executeProperties = ExecuteProperties.newBuilder()
                .setScannedBytesLimit(500)
                .build();

        continuation = executeAndVerifyData(plan, continuation, executeProperties, 2, (rec, i) -> {
            int primaryKey = 1 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        });
        assertNull(continuation);
    }

    @Test
    void testIndexPrefetchWithComparatorPlan() throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.NONE);
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH);
        ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
        RecordQueryPlan plan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithPrefetch), primaryKey(), 0, true);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                // Will throw exception if plans do not match
                // This only compares the primary key and not all values
                assertNotNull(cursor.asList().get());
            }
        }
    }

    @Test
    void testIndexPrefetchWithComparatorPlanFails() throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.NONE);
        RecordQueryPlan planWithPrefetch = plan(STR_VALUE_EVEN, RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH);
        ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
        RecordQueryPlan plan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithPrefetch), primaryKey(), 0, true);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                assertThrows(ExecutionException.class, () -> cursor.asList().get());
            }
        }
    }

    @Test
    void testIndexPrefetchReadYourWriteFails() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            // Save record (don't commit)
            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1000);
            recBuilder.setStrValueIndexed("even");
            recBuilder.setNumValueUnique(1000);
            recBuilder.setNumValue2(3);
            recBuilder.setNumValue3Indexed(5);
            recordStore.saveRecord(recBuilder.build());

            // Execute the query (will fail because a record in memory cannot be processed by index prefetch)
            RecordQueryPlan plan = plan(STR_VALUE_EVEN, RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH);
            ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
            RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties);

            // This should fail when index prefetch is fully supported. Since for now the API_VERSION is 630, prefetch mode
            // will fall back to index scan, which would not fail in this case.
            assertNotNull(cursor.getNext());
            // assertThrows(RecordCoreException.class, () -> cursor.getNext());
        }
    }

    @Test
    void testIndexPrefetchReadYourWriteFallbackSucceeds() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            // Save record (don't commit)
            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1000);
            recBuilder.setStrValueIndexed("even");
            recBuilder.setNumValueUnique(1000);
            recBuilder.setNumValue2(3);
            recBuilder.setNumValue3Indexed(5);
            recordStore.saveRecord(recBuilder.build());

            // Execute the query (will fail because a record in memory cannot be processed by index prefetch)
            RecordQueryPlan plan = plan(STR_VALUE_EVEN, RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH_WITH_FALLBACK);
            ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
            RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties);
            assertNotNull(cursor.getNext());
        }
    }

    @Test
    void indexPrefetchSimpleIndexFallbackTest() throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.NONE);
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH);
        RecordQueryPlan planWithFallback = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH_WITH_FALLBACK);
        RecordQueryPlan comparatorPlan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithFallback), primaryKey(), 0, true);

        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();

        // This will throw an exception since SNAPSHOT is not supported
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            // This should fail when index prefetch is fully supported. Since for now the API_VERSION is 630, prefetch mode
            // will fall back to index scan, which would not fail in this case.
            recordStore.executeQuery(planWithPrefetch, null, executeProperties);
            // assertThrows(UnsupportedOperationException.class, () -> recordStore.executeQuery(planWithPrefetch, null, executeProperties));
        }
        // This will compare the plans successfully since fallback resorts to the scan plan
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(comparatorPlan, null, executeProperties)) {
                // Will throw exception if plans do not match
                assertNotNull(cursor.asList().get());
            }
        }
    }

    @Test
    void testOrphanPolicyError() throws Exception {
        List<FDBIndexedRawRecord> mappedKeyValues = buildIndexedRawRecordWithOrphanIndexEntry();
        RecordCursor<FDBIndexedRawRecord> recordCursor = new ListCursor<>(mappedKeyValues, null);
        Subspace recordSubspace = new Subspace(new byte[] {21, 8, 20, 21, 17, 21, 1});
        RecordCursor<FDBIndexedRecord<Message>> result = recordStore.indexEntriesToIndexRecords(ScanProperties.FORWARD_SCAN, IndexOrphanBehavior.ERROR, recordSubspace, recordCursor, recordStore.getSerializer());
        assertThrows(ExecutionException.class, () -> result.getCount().get());
    }

    @Test
    void testOrphanPolicySkip() throws Exception {
        List<FDBIndexedRawRecord> mappedKeyValues = buildIndexedRawRecordWithOrphanIndexEntry();
        RecordCursor<FDBIndexedRawRecord> recordCursor = new ListCursor<>(mappedKeyValues, null);
        Subspace recordSubspace = new Subspace(new byte[] {21, 8, 20, 21, 17, 21, 1});
        RecordCursor<FDBIndexedRecord<Message>> resultFuture = recordStore.indexEntriesToIndexRecords(ScanProperties.FORWARD_SCAN, IndexOrphanBehavior.SKIP, recordSubspace, recordCursor, recordStore.getSerializer());
        List<FDBIndexedRecord<Message>> result = resultFuture.asList().get();
        assertEquals(2, result.size());
        assertRecord(FDBQueriedRecord.indexed(result.get(0)), 9, "odd", 991, "MyIndex", 991L, 9);
        assertRecord(FDBQueriedRecord.indexed(result.get(1)), 7, "odd", 993, "MyIndex", 991L, 7);
    }

    @Test
    void testOrphanPolicyReturn() throws Exception {
        List<FDBIndexedRawRecord> mappedKeyValues = buildIndexedRawRecordWithOrphanIndexEntry();
        RecordCursor<FDBIndexedRawRecord> recordCursor = new ListCursor<>(mappedKeyValues, null);
        Subspace recordSubspace = new Subspace(new byte[] {21, 8, 20, 21, 17, 21, 1});
        RecordCursor<FDBIndexedRecord<Message>> resultFuture = recordStore.indexEntriesToIndexRecords(ScanProperties.FORWARD_SCAN, IndexOrphanBehavior.RETURN, recordSubspace, recordCursor, recordStore.getSerializer());
        List<FDBIndexedRecord<Message>> result = resultFuture.asList().get();
        assertEquals(3, result.size());
        assertRecord(FDBQueriedRecord.indexed(result.get(0)), 9, "odd", 991, "MyIndex", (long)991, 9);
        assertFalse(result.get(1).hasStoredRecord());
        assertRecord(FDBQueriedRecord.indexed(result.get(2)), 7, "odd", 993, "MyIndex", (long)991, 7);
    }

    public boolean isUseSplitRecords() {
        return useSplitRecords;
    }

    public void setUseSplitRecords(final boolean useSplitRecords) {
        this.useSplitRecords = useSplitRecords;
    }

    private KeyExpression primaryKey() {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();
    }

    private void assertRecord(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue, final int numValue, final String indexName, Object indexedValue, final int localVersion) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo(indexName));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(2));
        assertThat(indexElements.get(0), equalTo(indexedValue));
        assertThat(indexElements.get(1), equalTo(primaryKey));
        List<Object> indexPrimaryKey = indexEntry.getPrimaryKey().getItems();
        assertThat(indexPrimaryKey.size(), equalTo(1));
        assertThat(indexPrimaryKey.get(0), equalTo(primaryKey));

        FDBStoredRecord<Message> storedRecord = rec.getStoredRecord();
        assertThat(storedRecord.getPrimaryKey().get(0), equalTo(primaryKey));
        assertThat(storedRecord.getRecordType().getName(), equalTo("MySimpleRecord"));

        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
        assertThat(myrec.getRecNo(), equalTo(primaryKey));
        assertThat(myrec.getStrValueIndexed(), equalTo(strValue));
        assertThat(myrec.getNumValueUnique(), equalTo(numValue));

        FDBRecordVersion version = storedRecord.getVersion();
        assertThat(version.getLocalVersion(), equalTo(localVersion));
    }

    @Nonnull
    private final RecordMetaDataHook simpleMetadataHook = metaDataBuilder -> {
        // UseSplitRecords can be set to different values to impact the way the store is opened
        metaDataBuilder.setSplitLongRecords(isUseSplitRecords());
    };

    @Nonnull
    private RecordQueryPlan plan(final RecordQuery query, final RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) {
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(useIndexPrefetch)
                .build());
        return planner.plan(query);
    }

    private byte[] executeAndVerifyData(RecordQueryPlan plan, int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier) throws Exception {
        return executeAndVerifyData(plan, null, ExecuteProperties.SERIAL_EXECUTE, expectedRecords, recordVerifier);
    }

    private byte[] executeAndVerifyData(RecordQueryPlan plan, byte[] continuation, ExecuteProperties executeProperties, int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier) throws Exception {
        int count = 0;
        byte[] lastContinuation;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> record = cursor.next();
                    recordVerifier.accept(record, count);
                    count++;
                }
                lastContinuation = cursor.getContinuation();
            }
        }
        assertThat(count, equalTo(expectedRecords));
        return lastContinuation;
    }

    private List<FDBIndexedRawRecord> buildIndexedRawRecordWithOrphanIndexEntry() throws Exception {
        long i = 9;
        Index index = new Index("MyIndex", "a");
        List<MappedKeyValue> mappedKeyValues = buildMappedResultWithOrphanIndexEntry();
        List<FDBIndexedRawRecord> result = new ArrayList<>();
        for (MappedKeyValue mappedKeyValue : mappedKeyValues) {
            Tuple key = Tuple.from(991L, i--);
            Tuple value = Tuple.from();
            result.add(new FDBIndexedRawRecord(new IndexEntry(index, key, value), mappedKeyValue));
        }
        return result;
    }

    private List<MappedKeyValue> buildMappedResultWithOrphanIndexEntry() throws Exception {
        List<MappedKeyValue> result = new ArrayList<>();
        // Fully populated record
        result.add(buildMappedKeyValue(new byte[] {21, 8, 20, 21, 17, 21, 2, 9}, new byte[0],
                List.of(new KeyValue(new byte[] {21, 8, 20, 21, 17, 21, 1, 21, 9, 19, -2}, new byte[] {51, 0, 0, 1, -46, -35, 49, 113, 85, 0, 0, 0, 9}),
                        new KeyValue(new byte[] {21, 8, 20, 21, 17, 21, 1, 21, 9, 20}, new byte[] {10, 32, 8, 9, 18, 3, 111, 100, 100, 24, -33, 7, 32, 0, 40, 4, 48, 0, 48, 1, 48, 2, 48, 3, 48, 4, 48, 5, 48, 6, 48, 7, 48, 8}))));
        // Orphan index entry
        result.add(buildMappedKeyValue(new byte[] {21, 8, 20, 21, 17, 21, 2, 8}, new byte[0],
                Collections.emptyList()));
        // Fully populated record
        result.add(buildMappedKeyValue(new byte[] {21, 8, 20, 21, 17, 21, 2, 7}, new byte[0],
                List.of(new KeyValue(new byte[] {21, 8, 20, 21, 17, 21, 1, 21, 7, 19, -2}, new byte[] {51, 0, 0, 1, -46, -35, 49, 113, 85, 0, 0, 0, 7}),
                        new KeyValue(new byte[] {21, 8, 20, 21, 17, 21, 1, 21, 7, 20}, new byte[] {10, 28, 8, 7, 18, 3, 111, 100, 100, 24, -31, 7, 32, 1, 40, 2, 48, 0, 48, 1, 48, 2, 48, 3, 48, 4, 48, 5, 48, 6}))));
        return result;
    }

    private MappedKeyValue buildMappedKeyValue(byte[] indexKey, byte[] indexValue, List<KeyValue> recordSplits) throws Exception {
        Constructor<MappedKeyValue> constructor = MappedKeyValue.class.getDeclaredConstructor(byte[].class, byte[].class, byte[].class, byte[].class, List.class);
        constructor.setAccessible(true);
        return constructor.newInstance(indexKey, indexValue, null, null, recordSplits);
    }
}
