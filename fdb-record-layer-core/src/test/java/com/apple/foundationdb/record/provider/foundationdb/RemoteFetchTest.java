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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

/**
 * A test for the remote fetch feature.
 */
@Tag(Tags.RequiresFDB)
class RemoteFetchTest extends RemoteFetchTestBase {

    protected static final RecordQuery IN_VALUE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").in(List.of(1000, 990, 980, 970, 960)))
            .build();

    protected static final RecordQuery OR_AND_VALUE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.or(
                    Query.field("num_value_unique").equalsValue(1000),
                    Query.and(
                            Query.field("num_value_unique").greaterThanOrEquals(900),
                            Query.field("num_value_unique").lessThan(910))))
            .build();

    private boolean useSplitRecords = true;

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetup(splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSimpleIndexTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSimpleIndexReverseTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990_REVERSE, useIndexPrefetch);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
    }

    /**
     * Unlike the other tests in this class that are using the "MySimpleRecord$num_value_unique" index (and its 2 index
     * entries), this test uses a different index ("PrimaryKeyIndex"). The test runs through a scenario where the primary
     * key component is also present in the index entry and is therefore removed from the actual index key primary key
     * location (since it is duplicate) - see {@link Index#getEntryPrimaryKeyPositions(int)}
     * @param useIndexPrefetch the fetch method mode to use
     */
    @ParameterizedTest(name = "indexPrefetchPrimaryKeyIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchPrimaryKeyIndexTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(PRIMARY_KEY_EQUAL, useIndexPrefetch);
        executeAndVerifyData(plan, 1, (rec, i) -> {
            int primaryKey = 1;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecordWithPrimaryKeyIndex(rec, primaryKey, strValue, numValue, "PrimaryKeyIndex", (long)numValue);
        }, splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchComplexIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchComplexIndexTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(STR_VALUE_EVEN, useIndexPrefetch);
        executeAndVerifyData(plan, 50, (rec, i) -> {
            int primaryKey = i * 2;
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, "even", numValue, "MySimpleRecord$str_value_indexed", "even", primaryKey); // we are filtering out all odd entries, so count*2 are the keys of the even ones
        }, splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchInQueryTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchInQueryTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(IN_VALUE, useIndexPrefetch);
        executeAndVerifyData(plan, 5, (rec, i) -> {
            int primaryKey = i*10;
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, "even", numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchAndOrQueryTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchAndOrQueryTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(OR_AND_VALUE, useIndexPrefetch);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = (i == 9) ? 0 : (99 - i);
            int numValue = 1000 - primaryKey;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchWithContinuationTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchWithContinuationTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
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
        }, splitRecordsHook);
        // Second iteration - last 5 records
        continuation = executeAndVerifyData(plan, continuation, executeProperties, 5, (rec, i) -> {
            int primaryKey = 4 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
        // Third iteration - no more values to read
        continuation = executeAndVerifyData(plan, continuation, executeProperties, 0, (rec, i) -> {
        }, splitRecordsHook);
        assertNull(continuation);
    }

    /*
     * Test continuation where the continued plan uses a different prefetch mode than the original plan.
     */
    @Test
    void indexPrefetchWithMixedContinuationTest() throws Exception {
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.USE_REMOTE_FETCH);
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH);
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(4)
                .build();

        // First iteration - first 4 records
        byte[] continuation = executeAndVerifyData(planWithPrefetch, null, executeProperties, 4, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
        // Second iteration - second 4 records
        continuation = executeAndVerifyData(planWithScan, continuation, executeProperties, 4, (rec, i) -> {
            int primaryKey = 5 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
        // Third iteration - last 2 records
        continuation = executeAndVerifyData(planWithPrefetch, continuation, executeProperties, 2, (rec, i) -> {
            int primaryKey = 1 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
        assertNull(continuation);
    }

    @ParameterizedTest(name = "indexPrefetchByteLimitContinuation(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    @Disabled("This test is inconsistently failing when running as part of the larger suite")
    void indexPrefetchByteLimitContinuation(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);
        // TODO: Why should the index prefetch take so many more bytes to scan the same number of records? Maybe the index scan counts the records and the fetch does not?
        int scanBytesLimit = (useIndexPrefetch == RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH) ? 350 : 1300;
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setScannedBytesLimit(scanBytesLimit)
                .build();

        byte[] continuation = executeAndVerifyData(plan, null, executeProperties, 8, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
        executeProperties = ExecuteProperties.newBuilder()
                .setScannedBytesLimit(500)
                .build();

        continuation = executeAndVerifyData(plan, continuation, executeProperties, 2, (rec, i) -> {
            int primaryKey = 1 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
        }, splitRecordsHook);
        assertNull(continuation);
    }

    @Test
    void testIndexPrefetchWithComparatorPlan() throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH);
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.USE_REMOTE_FETCH);
        ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
        RecordQueryPlan plan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithPrefetch), primaryKey(), 0, true);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                // Will throw exception if plans do not match
                // This only compares the primary key and not all values
                assertNotNull(cursor.asList().get());
            }
        }
    }

    @Test
    void testIndexPrefetchWithComparatorPlanFails() throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH);
        RecordQueryPlan planWithPrefetch = plan(STR_VALUE_EVEN, RecordQueryPlannerConfiguration.IndexFetchMethod.USE_REMOTE_FETCH);
        ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
        RecordQueryPlan plan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithPrefetch), primaryKey(), 0, true);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                assertThrows(ExecutionException.class, () -> cursor.asList().get());
            }
        }
    }

    @ParameterizedTest(name = "testReadYourWriteInRange(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void testReadYourWriteInRange(RecordQueryPlannerConfiguration.IndexFetchMethod fetchMethod) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            // Save record in range (don't commit)
            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setNumValueUnique(991);
            recBuilder.setStrValueIndexed("blah");
            recordStore.saveRecord(recBuilder.build());

            RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod);
            ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
            RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties);

            // When the API_VERSION is too low (remote fetch not supported) the plan will fallback to regular scan and
            // no exception is thrown. When in fallback mode, same thing, no exception.
            if (context.isAPIVersionAtLeast(APIVersion.API_VERSION_7_1) && (fetchMethod == RecordQueryPlannerConfiguration.IndexFetchMethod.USE_REMOTE_FETCH)) {
                assertThrows(RecordCoreException.class, () -> cursor.getNext());
            } else {
                assertNotNull(cursor.getNext());
            }
        }
    }

    @ParameterizedTest(name = "testReadYourWriteOutOfRangeSucceeds(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void testReadYourWriteOutOfRangeSucceeds(RecordQueryPlannerConfiguration.IndexFetchMethod fetchMethod) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            // Save record out of range (don't commit)
            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(20);
            recBuilder.setNumValueUnique(980);
            recBuilder.setStrValueIndexed("blah");
            recordStore.saveRecord(recBuilder.build());

            // Execute the query (will fail because a record in memory cannot be processed by fdb)
            RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod);
            ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
            RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties);

            assertNotNull(cursor.getNext());
        }
    }

    @Test
    void indexPrefetchSimpleIndexFallbackTest() throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH);
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.USE_REMOTE_FETCH);
        RecordQueryPlan planWithFallback = plan(NUM_VALUES_LARGER_THAN_990, RecordQueryPlannerConfiguration.IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK);
        RecordQueryPlan comparatorPlan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithFallback), primaryKey(), 0, true);

        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();

        // This will throw an exception since SNAPSHOT is not supported
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            // When the API_VERSION is too low (remote fetch not supported) the plan will fallback to regular scan and
            // no exception is thrown.
            if (context.isAPIVersionAtLeast(APIVersion.API_VERSION_7_1)) {
                assertThrows(UnsupportedOperationException.class, () -> recordStore.executeQuery(planWithPrefetch, null, executeProperties));
            } else {
                recordStore.executeQuery(planWithPrefetch, null, executeProperties);
            }
        }
        // This will compare the plans successfully since fallback resorts to the scan plan
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(comparatorPlan, null, executeProperties)) {
                // Will throw exception if plans do not match
                assertNotNull(cursor.asList().get());
            }
        }
    }

    @Test
    void testOrphanPolicyError() throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            Exception ex = assertThrows(ExecutionException.class, () -> scanIndex(IndexOrphanBehavior.ERROR));
            assertTrue(ex.getCause() instanceof RecordCoreStorageException);
        }
    }

    @Test
    void testOrphanPolicySkip() throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        List<FDBIndexedRecord<Message>> records;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            records = scanIndex(IndexOrphanBehavior.SKIP);
        }
        assertEquals(99, records.size());
        long c = 99;
        for (FDBIndexedRecord<Message> rec : records) {
            if (c != 2) {
                assertEquals(c, rec.getStoredRecord().getPrimaryKey().get(0));
            } else {
                // skip the missing record
                c--;
            }
            c--;
        }
    }

    @Test
    void testOrphanPolicyReturn() throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        List<FDBIndexedRecord<Message>> records;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            records = scanIndex(IndexOrphanBehavior.RETURN);
        }
        assertEquals(100, records.size());
        long c = 99;
        for (FDBIndexedRecord<Message> rec : records) {
            if (c != 2) {
                assertEquals(c, rec.getStoredRecord().getPrimaryKey().get(0));
            } else {
                assertFalse(rec.hasStoredRecord());
            }
            c--;
        }
    }

    private List<FDBIndexedRecord<Message>> scanIndex(final IndexOrphanBehavior orphanBehavior) throws InterruptedException, ExecutionException {
        return recordStore.scanIndexRemoteFetch("MySimpleRecord$num_value_unique", new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL),
                primaryKey(), null, ScanProperties.FORWARD_SCAN, orphanBehavior).asList().get();
    }

    private void createOrphanEntry() throws Exception {
        // Unchecked open the store, remove the index and then delete a record. This will create an orphan entry in the index.
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, builder -> {
                builder.removeIndex("MySimpleRecord$num_value_unique");
            });
            recordStore.deleteRecord(Tuple.from(2L));
            commit(context);
        }
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

    @Nonnull
    private final RecordMetaDataHook splitRecordsHook = metaDataBuilder -> {
        // UseSplitRecords can be set to different values to impact the way the store is opened
        metaDataBuilder.setSplitLongRecords(isUseSplitRecords());
        metaDataBuilder.addIndex("MySimpleRecord", "PrimaryKeyIndex", "rec_no");
    };

    private void assertRecordWithPrimaryKeyIndex(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue, final int numValue, final String indexName, final Object indexedValue) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo(indexName));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(1));
        assertThat(indexElements.get(0), equalTo(primaryKey));
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
    }
}
