/*
 * QueryPlanCursorTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for cursor behavior of query plans.
 */
@Tag(Tags.RequiresFDB)
public class QueryPlanCursorTest extends FDBRecordStoreTestBase {
    private static final Logger logger = LoggerFactory.getLogger(QueryPlanCursorTest.class);

    @BeforeEach
    public void setupStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (int i = 0; i < 200; i++) {
                TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder();
                builder.setRecNo(i);
                builder.setNumValue2(i ^ 80);
                builder.setNumValue3Indexed(i % 5);
                builder.setStrValueIndexed((i % 2 == 0) ? "even" : "odd");
                recordStore.saveRecord(builder.build());
            }
            commit(context);
        }
    }

    private void compareSkipsAndCursors(RecordQueryPlan plan, int amount) throws Exception {
        final Function<FDBQueriedRecord<Message>, Long> getRecNo = r -> {
            TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder();
            record.mergeFrom(r.getRecord());
            return record.getRecNo();
        };

        final ExecuteProperties justLimit = ExecuteProperties.newBuilder().setReturnedRowLimit(amount).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final List<Long> whole;
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                whole = cursor.map(getRecNo).asList().get();
            }
            assertTrue(whole.size() > amount, "should have more than one batch");

            final List<Long> byCursors = new ArrayList<>();
            byte[] continuation = null;
            do {
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, justLimit)) {
                    RecordCursorResult<FDBQueriedRecord<Message>> result = null;
                    do {
                        result = cursor.getNext();
                        if (result.hasNext()) {
                            byCursors.add(getRecNo.apply(result.get()));
                        }
                    } while (result.hasNext());
                    continuation = result.getContinuation().toBytes();
                }
            } while (continuation != null);
            assertEquals(whole, byCursors);

            final List<Long> byOffsets = new ArrayList<>();
            int offset = 0;
            while (true) {
                final ExecuteProperties skipAndLimit = ExecuteProperties.newBuilder()
                        .setSkip(offset)
                        .setReturnedRowLimit(amount)
                        .setIsolationLevel(justLimit.getIsolationLevel())
                        .build();
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, skipAndLimit)) {
                    final List<Long> next = cursor.map(getRecNo).asList().get();
                    byOffsets.addAll(next);
                    if (next.size() < amount) {
                        break;
                    } else {
                        offset += next.size();
                    }
                }
            }
            assertEquals(whole, byOffsets);
        }
    }

    final int[] amounts = { 1, 2, 3, 10 };

    private void compareSkipsAndCursors(RecordQueryPlan plan) throws Exception {
        for (int amount : amounts) {
            compareSkipsAndCursors(plan, amount);
        }
    }

    private RecordQueryPlan scanPlan() {
        return new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
    }

    private RecordQueryPlan indexPlanEquals(String indexName, Object value) {
        IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Arrays.asList(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value)),
                        Collections.emptySet()));
        return new RecordQueryIndexPlan(indexName, scan, false);
    }

    private KeyExpression primaryKey() {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();
    }

    @Test
    public void fullScan() throws Exception {
        final RecordQueryPlan plan = scanPlan();
        compareSkipsAndCursors(plan);
    }

    @Test
    public void recordTypeFilter() throws Exception {
        final RecordQueryPlan plan = new RecordQueryTypeFilterPlan(scanPlan(), Arrays.asList("MySimpleRecord"));
        compareSkipsAndCursors(plan);
    }

    @Test
    public void indexlessFilter() throws Exception {
        final RecordQueryPlan plan = new RecordQueryFilterPlan(scanPlan(), Query.field("num_value_2").lessThan(50));
        compareSkipsAndCursors(plan);
    }

    @Test
    public void indexed() throws Exception {
        final RecordQueryPlan plan = indexPlanEquals("MySimpleRecord$num_value_3_indexed", 2);
        compareSkipsAndCursors(plan);
    }

    @Test
    public void indexRange() throws Exception {
        final IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Collections.emptyList(), ImmutableSet.of(
                        new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 2),
                        new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 4))));
        final RecordQueryPlan plan = new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", scan, false);
        compareSkipsAndCursors(plan);
    }

    @Test
    public void reverse() throws Exception {
        final IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Collections.emptyList(), ImmutableSet.of(
                        new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 2),
                        new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 4))));
        final RecordQueryPlan plan = new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", scan, true);
        compareSkipsAndCursors(plan);
    }

    @Test
    public void in() throws Exception {
        final IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Arrays.asList(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "in_num")), Collections.emptySet()));
        final RecordQueryPlan plan = new RecordQueryInValuesJoinPlan(
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", scan, false),
                "in_num",
                Bindings.Internal.IN,
                Arrays.asList(2, 4), false, false);
        compareSkipsAndCursors(plan);
    }

    @Test
    public void union() throws Exception {
        final RecordQueryPlan plan = RecordQueryUnionPlan.from(
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", 2),
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", 4),
                primaryKey(), false);
        compareSkipsAndCursors(plan);
    }

    @Test
    public void unionOneSideAtATime() throws Exception {
        final RecordQueryPlan plan = RecordQueryUnionPlan.from(
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", 2),
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", 4),
                Key.Expressions.concat(Key.Expressions.field("num_value_3_indexed"),
                        primaryKey()),
                true);
        compareSkipsAndCursors(plan);
    }

    @Test
    public void intersection() throws Exception {
        final RecordQueryPlan plan = RecordQueryIntersectionPlan.from(
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", 2),
                indexPlanEquals("MySimpleRecord$str_value_indexed", "even"),
                primaryKey());
        compareSkipsAndCursors(plan);
    }

    @Test
    public void filter() throws Exception {
        final RecordQueryPlan plan = new RecordQueryFilterPlan(
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", 2),
                Query.field("str_value_indexed").equalsValue("even")
        );
        compareSkipsAndCursors(plan);
    }

    private void filterKeyCount(int amount) throws Exception {
        final QueryComponent filter = Query.field("str_value_indexed").equalsValue("even");
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.getTimer().reset();

            final RecordQueryPlan plan = indexPlanEquals("MySimpleRecord$num_value_3_indexed", 2);
            byte[] continuation = null;
            int unfilteredCount = 0;

            // Read with no filter.
            do {
                RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.SERIAL_EXECUTE)
                        .limitRowsTo(amount);
                int count = cursor.getCount().get();
                assertThat(count, lessThanOrEqualTo(amount));
                unfilteredCount += count;
                continuation = cursor.getNext().getContinuation().toBytes();
            } while (continuation != null);

            recordStore.getTimer().reset();

            // Read with a filter.
            continuation = null;
            int filteredCount = 0;
            do {
                RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.SERIAL_EXECUTE)
                        .limitRowsTo(amount)
                        .filterInstrumented(rec -> filter.eval(recordStore, EvaluationContext.EMPTY, rec),
                                recordStore.getTimer(), Collections.singleton(FDBStoreTimer.Counts.QUERY_FILTER_PLAN_GIVEN), Collections.emptySet(),
                                Collections.singleton(FDBStoreTimer.Counts.QUERY_FILTER_PLAN_PASSED), Collections.emptySet());
                int count = cursor.getCount().get();
                assertThat(count, lessThanOrEqualTo(amount));
                filteredCount += count;
                continuation = cursor.getNext().getContinuation().toBytes();
            } while (continuation != null);

            int filteredGiven = recordStore.getTimer().getCount(FDBStoreTimer.Counts.QUERY_FILTER_PLAN_GIVEN);
            int filteredPassed = recordStore.getTimer().getCount(FDBStoreTimer.Counts.QUERY_FILTER_PLAN_PASSED);

            // We should have passed as many keys through our filter as there are records returned.
            assertEquals(filteredCount, filteredPassed);

            // We should read the same amount of data whether we are filtering or not.
            assertEquals(unfilteredCount, filteredGiven);
        }
    }

    @Test
    public void filterKeyCount() throws Exception {
        for (int amount : amounts) {
            filterKeyCount(amount);
        }
    }
}
