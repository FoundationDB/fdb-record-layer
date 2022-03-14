/*
 * FDBRecordStoreQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    //TODO: Add tests with split records, and with unsplit records...

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetup(simpleMetadataHook);
    }

    @ParameterizedTest
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

    @ParameterizedTest
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

    @ParameterizedTest
    @EnumSource()
    void indexPrefetchComplexIndexTest(RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(STR_VALUE_EVEN, useIndexPrefetch);
        executeAndVerifyData(plan, 50, (rec, i) -> {
            int primaryKey = i * 2;
            int numValue = 1000 - primaryKey;
            assertRecord(rec, primaryKey, "even", numValue, "MySimpleRecord$str_value_indexed", "even", primaryKey); // we are filtering out all odd entries, so count*2 are the keys of the even ones
        });
    }

    @ParameterizedTest
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

    @ParameterizedTest
    @EnumSource()
    void indexPrefetchByteLimitContinuation(RecordQueryPlannerConfiguration.IndexPrefetchUse useIndexPrefetch) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);
        // TODO: Why should hte index prefetch take so many more bytes to scan the same number of records? Maybe the index scan counts the records and the fetch does not?
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
                List<FDBQueriedRecord<Message>> result = cursor.asList().get();
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
            assertThrows(UnsupportedOperationException.class, () -> recordStore.executeQuery(planWithPrefetch, null, executeProperties));
        }
        // This will compare the plans successfully since fallback resorts to the scan plan
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(comparatorPlan, null, executeProperties)) {
                // Will throw exception if plans do not match
                List<FDBQueriedRecord<Message>> result = cursor.asList().get();
            }
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
}
