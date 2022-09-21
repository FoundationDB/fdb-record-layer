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
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
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

    // The setup parameter to control the set up of the store (to use split records or not)
    private boolean useSplitRecords = true;

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetup(splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchSimpleIndexTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod, indexEntryReturnPolicy);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey,
                    fetchMethod, indexEntryReturnPolicy);
        }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 11);
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchSimpleIndexReverseTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990_REVERSE, fetchMethod, indexEntryReturnPolicy);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey,
                    fetchMethod, indexEntryReturnPolicy);
        }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 11);
    }

    /**
     * Unlike the other tests in this class that are using the "MySimpleRecord$num_value_unique" index (and its 2 index
     * entries), this test uses a different index ("PrimaryKeyIndex"). The test runs through a scenario where the primary
     * key component is also present in the index entry and is therefore removed from the actual index key primary key
     * location (since it is duplicate) - see {@link Index#getEntryPrimaryKeyPositions(int)}
     * @param fetchMethod the fetch method mode to use
     */
    @ParameterizedTest(name = "indexPrefetchPrimaryKeyIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchPrimaryKeyIndexTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan plan = plan(PRIMARY_KEY_EQUAL, fetchMethod, indexEntryReturnPolicy);
        executeAndVerifyData(plan, 1, (rec, i) -> {
            int primaryKey = 1;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            // There is always only one returned record, so it has the index entry regardless of mode
            assertRecordWithPrimaryKeyIndex(rec, primaryKey, strValue, numValue, "PrimaryKeyIndex");
        }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 2);
    }

    @ParameterizedTest(name = "indexPrefetchComplexIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchComplexIndexTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan plan = plan(STR_VALUE_EVEN, fetchMethod, indexEntryReturnPolicy);
        executeAndVerifyData(plan, 50, (rec, i) -> {
            int primaryKey = i * 2; // we are filtering out all odd entries, so count*2 are the keys of the even ones
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, "even", numValue, "MySimpleRecord$str_value_indexed", "even", primaryKey,
                    fetchMethod, indexEntryReturnPolicy);
        }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 51);
    }

    @ParameterizedTest(name = "indexPrefetchInQueryTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchInQueryTest(IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(IN_VALUE, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        executeAndVerifyData(plan, 5, (rec, i) -> {
            int primaryKey = i * 10;
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, "even", numValue, "MySimpleRecord$num_value_unique", (long)numValue, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        }, splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchAndOrQueryTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchAndOrQueryTest(IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(OR_AND_VALUE, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        executeAndVerifyData(plan, 10, (rec, i) -> {
            int primaryKey = (i == 9) ? 0 : (99 - i);
            int numValue = 1000 - primaryKey;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        }, splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchWithContinuationTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchWithContinuationTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod, indexEntryReturnPolicy);
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(5)
                .build();

        // First iteration - first 5 records
        byte[] continuation = executeAndVerifyData(plan, null, executeProperties, 5, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey, fetchMethod, indexEntryReturnPolicy);
        }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 6);
        // Second iteration - last 5 records
        continuation = executeAndVerifyData(plan, continuation, executeProperties, 5, (rec, i) -> {
            int primaryKey = 4 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey, fetchMethod, indexEntryReturnPolicy);
        }, splitRecordsHook);
        assertCounters(fetchMethod, 2, 12);
        // Third iteration - no more values to read
        continuation = executeAndVerifyData(plan, continuation, executeProperties, 0, (rec, i) -> {
        }, splitRecordsHook);
        assertNull(continuation);
        assertCounters(fetchMethod, 3, 13);
    }

    /*
     * Test continuation where the continued plan uses a different prefetch mode than the original plan.
     */
    @ParameterizedTest(name = "indexPrefetchWithMixedContinuationTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedReturnPolicies")
    void indexPrefetchWithMixedContinuationTest(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.SCAN_AND_FETCH, indexEntryReturnPolicy);
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(4)
                .build();

        // First iteration - first 4 records
        byte[] continuation = executeAndVerifyData(planWithPrefetch, null, executeProperties, 4, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
        }, splitRecordsHook);
        // Second iteration - second 4 records
        continuation = executeAndVerifyData(planWithScan, continuation, executeProperties, 4, (rec, i) -> {
            int primaryKey = 5 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey, IndexFetchMethod.SCAN_AND_FETCH, indexEntryReturnPolicy);
        }, splitRecordsHook);
        // Third iteration - last 2 records
        continuation = executeAndVerifyData(planWithPrefetch, continuation, executeProperties, 2, (rec, i) -> {
            int primaryKey = 1 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
        }, splitRecordsHook);
        assertNull(continuation);
        assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 2, 8);
    }

    @ParameterizedTest(name = "indexPrefetchByteLimitContinuation(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    @Disabled("This test is inconsistently failing when running as part of the larger suite")
    void indexPrefetchByteLimitContinuation(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod, indexEntryReturnPolicy);
        // TODO: Why should the index prefetch take so many more bytes to scan the same number of records? Maybe the index scan counts the records and the fetch does not?
        int scanBytesLimit = (fetchMethod == IndexFetchMethod.SCAN_AND_FETCH) ? 350 : 1300;
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setScannedBytesLimit(scanBytesLimit)
                .build();

        byte[] continuation = executeAndVerifyData(plan, null, executeProperties, 8, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey, fetchMethod, indexEntryReturnPolicy);
        }, splitRecordsHook);
        executeProperties = ExecuteProperties.newBuilder()
                .setScannedBytesLimit(500)
                .build();

        continuation = executeAndVerifyData(plan, continuation, executeProperties, 2, (rec, i) -> {
            int primaryKey = 1 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey, fetchMethod, indexEntryReturnPolicy);
        }, splitRecordsHook);
        assertNull(continuation);
    }

    @ParameterizedTest(name = "testScanLimit(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void testScanLimit(IndexFetchMethod useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setScannedRecordsLimit(3)
                .build();

        byte[] continuation = executeAndVerifyData(plan, null, executeProperties, 3, (rec, i) -> {
            int primaryKey = 9 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        }, splitRecordsHook);

        executeProperties = ExecuteProperties.newBuilder()
                .setScannedRecordsLimit(1)
                .build();

        continuation = executeAndVerifyData(plan, continuation, executeProperties, 1, (rec, i) -> {
            int primaryKey = 6 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        }, splitRecordsHook);

        executeProperties = ExecuteProperties.newBuilder()
                .setScannedRecordsLimit(100)
                .build();

        continuation = executeAndVerifyData(plan, continuation, executeProperties, 6, (rec, i) -> {
            int primaryKey = 5 - i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, useIndexPrefetch, IndexEntryReturnPolicy.ALL);
        }, splitRecordsHook);

        assertNull(continuation);
    }

    @ParameterizedTest(name = "testIndexPrefetchWithComparatorPlan(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedReturnPolicies")
    void testIndexPrefetchWithComparatorPlan(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.SCAN_AND_FETCH, indexEntryReturnPolicy);
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
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
        assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 1, 11);
    }

    @ParameterizedTest(name = "testIndexPrefetchWithComparatorPlanFails(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedReturnPolicies")
    void testIndexPrefetchWithComparatorPlanFails(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.SCAN_AND_FETCH, indexEntryReturnPolicy);
        RecordQueryPlan planWithPrefetch = plan(STR_VALUE_EVEN, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
        ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
        RecordQueryPlan plan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithPrefetch), primaryKey(), 0, true);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                assertThrows(ExecutionException.class, () -> cursor.asList().get());
            }
        }
        assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 1, 1);
    }

    /**
     * This test writes a value to the store within the range of the scan.
     */
    @ParameterizedTest(name = "testReadYourWriteInRange(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void testReadYourWriteInRange(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            // Save record in range (don't commit)
            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setNumValueUnique(999);
            recBuilder.setStrValueIndexed("blah");
            recordStore.saveRecord(recBuilder.build());

            RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod, indexEntryReturnPolicy);

            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(ExecutionException.class, () -> executeToList(context, plan, null, ExecuteProperties.SERIAL_EXECUTE));
            } else {
                executeAndVerifyData(context, plan, null, ExecuteProperties.SERIAL_EXECUTE, 10, (rec, i) -> {
                    int primaryKey = 9 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    if (primaryKey == 1) {
                        strValue = "blah";
                    }
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, IndexEntryReturnPolicy.ALL);
                });
            }
        }
        assertCounters(fetchMethod, 1, 1);
    }

    /**
     * This test writes a value to the store outside the range of the scan.
     */
    @ParameterizedTest(name = "testReadYourWriteOutOfRangeSucceeds(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void testReadYourWriteOutOfRangeSucceeds(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            // Save record out of range (don't commit)
            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(20);
            recBuilder.setNumValueUnique(980);
            recBuilder.setStrValueIndexed("blah");
            recordStore.saveRecord(recBuilder.build());

            // Execute the query (will fail because a record in memory cannot be processed by fdb)
            RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod, indexEntryReturnPolicy);

            executeAndVerifyData(context, plan, null, ExecuteProperties.SERIAL_EXECUTE, 10, (rec, i) -> {
                int primaryKey = 9 - i;
                String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                int numValue = 1000 - primaryKey;
                assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, IndexEntryReturnPolicy.ALL);
            });
            assertCounters(fetchMethod, 1, 11);
        }
    }

    /**
     * This test captures the case that FDB fails the scan after it has already returned several records. A record gets
     * modified in the transaction at a point that would allow FDB to fetch a few pages of payload before encountering
     * the modified range conflict. This should be recovered by the fallback mode.
     */
    @ParameterizedTest(name = "failAfterRecordsReturnedTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void failAfterRecordsReturnedTest(IndexFetchMethod fetchMethod) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        List<TestRecords1Proto.MySimpleRecord> created = saveManyRecords();

        // Modify record and then scan unmodified index
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);

            // Update a record. This record will eventually be returned in a query, but we do *not* modify
            // a field in the index being scanned, so that FDB does not detect the range conflict until later
            TestRecords1Proto.MySimpleRecord lastRecord = created.get(created.size() - 1);
            recordStore.saveRecord(lastRecord.toBuilder()
                    .setStrValueIndexed("foo")
                    .build());

            // Use remote fetch to scan the num_value_unique index. The first few results should return
            // data (essentially the first few pages of data from scanning the index), but once it
            // gets to the final page, it should fail because it sees a modified range when trying to look
            // up the record
            RecordQueryPlan plan = plan(NUM_VALUES_LARGER_EQUAL_0, fetchMethod, IndexEntryReturnPolicy.ALL);
            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(ExecutionException.class, () -> executeToList(context, plan, null, ExecuteProperties.SERIAL_EXECUTE));
            } else {
                executeAndVerifyData(context, plan, null, ExecuteProperties.SERIAL_EXECUTE, 500, (rec, i) -> {
                    int primaryKey = i;
                    int numValue = i;
                    String strValue = (i == created.size() - 1) ? "foo" : "";
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, IndexEntryReturnPolicy.ALL);
                });
            }
        }
    }

    @ParameterizedTest(name = "failAfterRecordsReturnedReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void failAfterRecordsReturnedReverseTest(IndexFetchMethod fetchMethod) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        List<TestRecords1Proto.MySimpleRecord> created = saveManyRecords();

        // Modify record and then scan unmodified index
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);

            // Update a record. This record will eventually be returned in a query, but we do *not* modify
            // a field in the index being scanned, so that FDB does not detect the range conflict until later
            TestRecords1Proto.MySimpleRecord lastRecord = created.get(0);
            recordStore.saveRecord(lastRecord.toBuilder()
                    .setStrValueIndexed("foo")
                    .build());

            // Use remote fetch to scan the num_value_unique index. The first few results should return
            // data (essentially the first few pages of data from scanning the index), but once it
            // gets to the final page, it should fail because it sees a modified range when trying to look
            // up the record
            RecordQueryPlan plan = plan(NUM_VALUES_LARGER_EQUAL_0_REVERSE, fetchMethod, IndexEntryReturnPolicy.ALL);
            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(ExecutionException.class, () -> executeToList(context, plan, null, ExecuteProperties.SERIAL_EXECUTE));
            } else {
                executeAndVerifyData(context, plan, null, ExecuteProperties.SERIAL_EXECUTE, 500, (rec, i) -> {
                    int primaryKey = 499 - i;
                    int numValue = primaryKey;
                    String strValue = (i == (created.size() - 1)) ? "foo" : "";
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, IndexEntryReturnPolicy.ALL);
                });
            }
        }
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexFallbackTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedReturnPolicies")
    void indexPrefetchSimpleIndexFallbackTest(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        RecordQueryPlan planWithScan = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.SCAN_AND_FETCH, indexEntryReturnPolicy);
        RecordQueryPlan planWithPrefetch = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
        RecordQueryPlan planWithFallback = plan(NUM_VALUES_LARGER_THAN_990, IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK, indexEntryReturnPolicy);
        RecordQueryPlan comparatorPlan = RecordQueryComparatorPlan.from(List.of(planWithScan, planWithFallback), primaryKey(), 0, true);

        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();

        // This will throw an exception since SNAPSHOT is not supported
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            assertThrows(UnsupportedOperationException.class, () -> executeToList(context, planWithPrefetch, null, executeProperties));
        }
        // This will compare the plans successfully since fallback resorts to the scan plan
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            // Will throw exception if plans do not match
            executeToList(context, comparatorPlan, null, executeProperties);
        }
    }

    @ParameterizedTest(name = "testOrphanPolicyError(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedReturnPolicies")
    void testOrphanPolicyError(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            Exception ex = assertThrows(ExecutionException.class, () -> scanIndex(IndexOrphanBehavior.ERROR, indexEntryReturnPolicy));
            assertTrue(ex.getCause() instanceof RecordCoreStorageException);
        }
    }

    @ParameterizedTest(name = "testOrphanPolicySkip(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedReturnPolicies")
    void testOrphanPolicySkip(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        List<FDBIndexedRecord<Message>> records;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            records = scanIndex(IndexOrphanBehavior.SKIP, indexEntryReturnPolicy);
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
        assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 1, 100);
    }

    @ParameterizedTest(name = "testOrphanPolicySkip(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedReturnPolicies")
    void testOrphanPolicyReturn(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        List<FDBIndexedRecord<Message>> records;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            records = scanIndex(IndexOrphanBehavior.RETURN, indexEntryReturnPolicy);
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
        assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 1, 101);
    }

    private List<FDBIndexedRecord<Message>> scanIndex(final IndexOrphanBehavior orphanBehavior, final IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        return recordStore.scanIndexRemoteFetch("MySimpleRecord$num_value_unique", new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL),
                null, ScanProperties.FORWARD_SCAN, orphanBehavior, indexEntryReturnPolicy).asList().get();
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

    private void assertRecordWithPrimaryKeyIndex(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue, final int numValue, final String indexName) {
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

    private List<TestRecords1Proto.MySimpleRecord> saveManyRecords() {
        List<TestRecords1Proto.MySimpleRecord> created = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            created.add(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(i)
                    .setNumValue3Indexed(i % 3)
                    .setNumValueUnique(i)
                    .build());
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            recordStore.deleteAllRecords();
            commit(context);
        }
        Iterator<TestRecords1Proto.MySimpleRecord> createdIterator = created.iterator();
        while (createdIterator.hasNext()) {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, splitRecordsHook);
                int i = 0;
                while (i < 50 && createdIterator.hasNext()) {
                    recordStore.saveRecord(createdIterator.next());
                    i++;
                }
                commit(context);
            }
        }

        return created;
    }
}
