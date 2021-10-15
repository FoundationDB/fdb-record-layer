/*
 * FDBStreamAggregateTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregatePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.AggregateValues;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests related to planning and executing queries with string collation.
 */
@Tag(Tags.RequiresFDB)
public class FDBStreamAggregateContinuationTest extends FDBRecordStoreQueryTestBase {

    /*
     * The database contains:
     *
     * MySimpleRecord:
     * -----------------------------------------------------------------------------------------------------------
     * | recno | NumValue2 | NumValue3Indexed | StrValueIndexed |
     * ----------------------------------------------------------
     * |     0 |         0 |                0 |             "0" |
     * ----------------------------------------------------------
     * |     1 |         1 |                0 |             "0" |
     * ----------------------------------------------------------
     * |     2 |         2 |                1 |             "0" |
     * ----------------------------------------------------------
     * |     3 |         3 |                1 |             "1" |
     * ----------------------------------------------------------
     * |     4 |         4 |                2 |             "1" |
     * ----------------------------------------------------------
     * |     5 |         5 |                2 |             "1" |
     * ----------------------------------------------------------
     * |     6 |      null |                2 |             "1" |
     * ----------------------------------------------------------
     * |     7 |      null |                2 |             "2" |
     * ----------------------------------------------------------
     *
     * MyOtherRecord: Empty.
     *
     */
    @BeforeEach
    public void setup() throws Exception {
        populateDB(5);
    }

    @Test
    public void noAggNoGroupingLimit2() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .build();

            // First query - reading 0, 0, no group break
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 2, null);
            // No group, one result with continuation
            Assertions.assertEquals(1, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Second query - reading 1, 1, no group break
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Third query - reading 2, 2, no group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Fourth query - reading 2, 2, no group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // One result: no groups, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Fifth query - stream exhausted, empty group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a END continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf());
        }
    }

    @Test
    public void noAggNoGroupingLimit3() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .build();

            // First query - reading 0, 0, 1, no group break
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 3, null);
            // No group, one result with continuation
            Assertions.assertEquals(1, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Second query - reading 1, 2, 2, no group break
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Third query - reading 2, 2, stream exhausted, empty group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf());
        }
    }

    @Test
    public void yesAggNoGroupingLimit2() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withAggregateValue("num_value_2", "SumInteger")
                    .build();

            // First query - reading 0, 0, no group break
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 2, null);
            // No group, one result with continuation
            Assertions.assertEquals(1, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Second query - reading 1, 1, no group break
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Third query - reading 2, 2, no group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Fourth query - reading 2, 2, no group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // One result: no groups, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Fifth query - stream exhausted, empty group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a END continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(15));
        }
    }

    @Test
    public void yesAggNoGroupingLimit3() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withAggregateValue("num_value_2", "SumInteger")
                    .build();

            // First query - reading 0, 0, 1, no group break
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 3, null);
            // No group, one result with continuation
            Assertions.assertEquals(1, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Second query - reading 1, 2, 2, no group break
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Third query - reading 2, 2, stream exhausted, empty group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(15));
        }
    }

    @Test
    public void noAggYesGroupingLimit2() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withGroupCriterion("num_value_3_indexed")
                    .build();

            // First query - reading 0, 0, no group break
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 2, null);
            // No group, one result with continuation
            Assertions.assertEquals(1, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Second query - reading 1, 1, group break at 0
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(0));

            // Third query - reading 2, 2, group break at 1
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(1));

            // Fourth query - reading 2, 2, no group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // One result: no groups, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Fifth query - stream exhausted, group break at 2
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a END continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(2));
        }
    }

    @Test
    public void noAggYesGroupingLimit3() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withGroupCriterion("num_value_3_indexed")
                    .build();

            // First query - reading 0, 0, 1, group break at 0
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 3, null);
            // No group, one result with continuation
            Assertions.assertEquals(2, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(0));

            // Second query - reading 1, 2, 2, group break at 1
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(1));

            // Third query - reading 2, 2, stream exhausted group break at 2
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(2));
        }
    }

    @Test
    public void yesAggYesGroupingLimit2() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withGroupCriterion("num_value_3_indexed")
                    .withAggregateValue("num_value_2", "SumInteger")
                    .build();

            // First query - reading 0, 0, no group break
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 2, null);
            // No group, one result with continuation
            Assertions.assertEquals(1, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Second query - reading 1, 1, group break at 0
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(0, 1));

            // Third query - reading 2, 2, group break at 1
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(1, 5));

            // Fourth query - reading 2, 2, no group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // One result: no groups, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Fifth query - stream exhausted, group break at 2
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a END continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(2, 9));
        }
    }

    @Test
    public void yesAggYesGroupingLimit3() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withGroupCriterion("num_value_3_indexed")
                    .withAggregateValue("num_value_2", "SumInteger")
                    .build();

            // First query - reading 0, 0, 1, group break at 0
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 3, null);
            // No group, one result with continuation
            Assertions.assertEquals(2, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(0, 1));

            // Second query - reading 1, 2, 2, group break at 1
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(1, 5));

            // Third query - reading 2, 2, stream exhausted group break at 2
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(2, 9));
        }
    }

    @Test
    public void testAggregationTypesLimit2() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withGroupCriterion("num_value_3_indexed")
                    .withAggregateValue("num_value_2", "SumInteger")
                    .withAggregateValue("num_value_2", "MinInteger")
                    .withAggregateValue("num_value_2", "MaxInteger")
                    .withAggregateValue("num_value_2", "AvgInteger")
                    .withAggregateValue("num_value_2", "Count")
                    .withAggregateValue("num_value_2", "CountNonNull")
                    .build();

            // First query - reading 0, 0, no group break
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 2, null);
            // No group, one result with continuation
            Assertions.assertEquals(1, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Second query - reading 1, 1, group break at 0
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(0, 1, 0, 1, 0.5D, 2L, 2L));

            // Third query - reading 2, 2, group break at 1
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(1, 5, 2, 3, 2.5D, 2L, 2L));

            // Fourth query - reading 2, 2, no group break
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // One result: no groups, one empty with a continuation
            Assertions.assertEquals(1, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            Assertions.assertTrue(queryResults.isEmpty());

            // Fifth query - stream exhausted, group break at 2
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 2, continuation.toBytes());
            // Two results: One group, one empty with a END continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(2, 9, 4, 5, 4.5D, 4L, 2L));
        }
    }

    @Test
    public void testAggregationTypesLimit3() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            AggregatePlanBuilder builder = new AggregatePlanBuilder("MySimpleRecord");
            RecordQueryPlan plan = builder
                    .withGroupCriterion("num_value_3_indexed")
                    .withAggregateValue("num_value_2", "SumInteger")
                    .withAggregateValue("num_value_2", "MinInteger")
                    .withAggregateValue("num_value_2", "MaxInteger")
                    .withAggregateValue("num_value_2", "AvgInteger")
                    .withAggregateValue("num_value_2", "Count")
                    .withAggregateValue("num_value_2", "CountNonNull")
                    .build();

            // First query - reading 0, 0, 1, group break at 0
            List<RecordCursorResult<QueryResult>> cursorResults = executePlanToCursorResults(plan, 3, null);
            // No group, one result with continuation
            Assertions.assertEquals(2, cursorResults.size());
            List<QueryResult> queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(0, 1, 0, 1, 0.5D, 2L, 2L));

            // Second query - reading 1, 2, 2, group break at 1
            RecordCursorContinuation continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(1, 5, 2, 3, 2.5D, 2L, 2L));

            // Third query - reading 2, 2, stream exhausted group break at 2
            continuation = cursorResults.get(cursorResults.size() - 1).getContinuation();
            cursorResults = executePlanToCursorResults(plan, 3, continuation.toBytes());
            // Two results: One group, one empty with a continuation
            Assertions.assertEquals(2, cursorResults.size());
            queryResults = toQueryResults(cursorResults);
            assertResults(queryResults, resultOf(2, 9, 4, 5, 4.5D, 4L, 2L));
        }
    }

    private void populateDB(final int numRecords) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            for (int i = 0; i <= numRecords; i++) {
                recBuilder.setRecNo(i);
                recBuilder.setNumValue2(i);
                recBuilder.setNumValue3Indexed(i / 2); // some field that changes every 2nd record
                recBuilder.setStrValueIndexed(Integer.toString(i / 3)); // some field that changes every 3rd record
                recordStore.saveRecord(recBuilder.build());
            }

            recBuilder.setRecNo(numRecords + 1);
            recBuilder.clearNumValue2();
            recBuilder.setNumValue3Indexed(2);
            recBuilder.setStrValueIndexed("1");
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setRecNo(numRecords + 2);
            recBuilder.clearNumValue2();
            recBuilder.setNumValue3Indexed(2); // some field that changes every 2nd record
            recBuilder.setStrValueIndexed("2"); // some field that changes every 3rd record
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }
    }

    /*
     * Return all the cursor results (even the non-value ones)
     */
    @Nonnull
    private List<RecordCursorResult<QueryResult>> executePlanToCursorResults(final RecordQueryPlan plan, int rowLimit, byte[] continuation) throws InterruptedException, java.util.concurrent.ExecutionException {
        Bindings bindings = Bindings.newBuilder().build();
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setReturnedRowLimit(rowLimit).build();
        RecordCursor<QueryResult> cursor = plan.executePlan(recordStore, EvaluationContext.forBindings(bindings), continuation, executeProperties);
        List<RecordCursorResult<QueryResult>> results = new ArrayList<>();
        RecordCursorResult<QueryResult> lastResult = cursor.forEachResult(results::add).get();
        results.add(lastResult);
        return results;
    }

    /*
     * Return the query results from all the cursor results. Filter out the ones without a value
     */
    private List<QueryResult> toQueryResults(final List<RecordCursorResult<QueryResult>> cursorResults) {
        return cursorResults.stream().filter(RecordCursorResult::hasNext).map(RecordCursorResult::get).collect(Collectors.toList());
    }

    private void assertResults(List<QueryResult> actual, final List<?>... expected) {
        Assertions.assertEquals(expected.length, actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertResult(actual.get(i), expected[i]);
        }
    }

    private void assertResult(final QueryResult actual, final List<?> expected) {
        Assertions.assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i), actual.get(i));
        }
    }

    private List<?> resultOf(Object... objs) {
        return Arrays.asList(objs);
    }

    private static class AggregatePlanBuilder {
        private final Quantifier.Physical quantifier;
        private final List<AggregateValue<?, ?>> aggregateValues;
        private final List<Value> groupValues;
        private String recordName;

        public AggregatePlanBuilder(final String recordName) {
            this.recordName = recordName;
            this.quantifier = createBaseQuantifier();
            aggregateValues = new ArrayList<>();
            groupValues = new ArrayList<>();
        }

        public AggregatePlanBuilder withAggregateValue(final String fieldName, final String aggregateType) {
            this.aggregateValues.add(createAggregateValue(createValue(fieldName), aggregateType));
            return this;
        }

        public AggregatePlanBuilder withGroupCriterion(final String fieldName) {
            this.groupValues.add(createValue(fieldName));
            return this;
        }

        public RecordQueryPlan build() {
            return new RecordQueryStreamingAggregatePlan(quantifier, groupValues, aggregateValues);
        }

        private Value createValue(final String fieldName) {
            return new FieldValue(QuantifiedColumnValue.of(quantifier.getAlias(), 1), Collections.singletonList(fieldName));
        }

        private Quantifier.Physical createBaseQuantifier() {
            RecordQueryScanPlan scanPlan = new RecordQueryScanPlan(ImmutableSet.of(recordName), ScanComparisons.EMPTY, false);
            RecordQueryTypeFilterPlan filterPlan = new RecordQueryTypeFilterPlan(scanPlan, Collections.singleton(recordName));
            return Quantifier.physical(GroupExpressionRef.of(filterPlan));
        }

        private AggregateValue<?, ?> createAggregateValue(Value value, String aggregateType) {
            switch (aggregateType) {
                case ("SumInteger"):
                    return AggregateValues.sumInt(value);
                case ("MinInteger"):
                    return AggregateValues.minInt(value);
                case ("MaxInteger"):
                    return AggregateValues.maxInt(value);
                case ("AvgInteger"):
                    return AggregateValues.averageInt(value);
                case ("Count"):
                    return AggregateValues.count(value);
                case ("CountNonNull"):
                    return AggregateValues.countNonNull(value);
                default:
                    throw new IllegalArgumentException("Cannot parse function name " + aggregateType);
            }
        }
    }
}
