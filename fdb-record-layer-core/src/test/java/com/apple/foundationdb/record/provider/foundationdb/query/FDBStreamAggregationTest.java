/*
 * FDBStreamAggregationTest.java
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

import com.apple.foundationdb.record.ByteScanLimiterFactory;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordScanLimiterFactory;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Tests related to planning and executing queries with string collation.
 */
@Tag(Tags.RequiresFDB)
class FDBStreamAggregationTest extends FDBRecordStoreQueryTestBase {

    /*
     * The database contains:
     *
     * MySimpleRecord:
     * -----------------------------------------------------------------------------------------------------------
     * | recno | NumValue2 | NumValue3Indexed | StrValueIndexed | NumValueUnique
     * -------------------------------------------------------------------------
     * |     0 |         0 |                0 |             "0" |             0|
     * -------------------------------------------------------------------------
     * |     1 |         1 |                0 |             "0" |             1|
     * -------------------------------------------------------------------------
     * |     2 |         2 |                1 |             "0" |             2|
     * -------------------------------------------------------------------------
     * |     3 |         3 |                1 |             "1" |             3|
     * -------------------------------------------------------------------------
     * |     4 |         4 |                2 |             "1" |             4|
     * -------------------------------------------------------------------------
     * |     5 |         5 |                2 |             "1" |             5|
     * -------------------------------------------------------------------------
     *
     * MyOtherRecord: Empty.
     *
     */
    @BeforeEach
    public void setup() throws Exception {
        populateDB(5);
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void noAggregateGroupByNone(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByOne(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, 1), resultOf(1, 5), resultOf(2, 9));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByNone(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(15));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void noAggregateGroupByOne(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0), resultOf(1), resultOf(2));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByTwo(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1), resultOf(1, "0", 2), resultOf(1, "1", 3), resultOf(2, "1", 9));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateTwoGroupByTwo(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1, 0), resultOf(1, "0", 2, 2), resultOf(1, "1", 3, 3), resultOf(2, "1", 9, 4));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateThreeGroupByTwo(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1, 0, 0.5), resultOf(1, "0", 2, 2, 2.0), resultOf(1, "1", 3, 3, 3.0), resultOf(2, "1", 9, 4, 4.5));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByThree(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        // each group only has one row
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .withGroupCriterion("num_value_unique")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 0, 0), resultOf(0, "0", 1, 1), resultOf(1, "0", 2, 2), resultOf(1, "1", 3, 3), resultOf(2, "1", 4, 4), resultOf(2, "1", 5, 5));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecords(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecordsNoGroup(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan = new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                    .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecordsNoAggregate(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecordsNoGroupNoAggregate(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .build(useNestedResult, serializationMode);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void partialAggregateAggregateThreeGroupByTwo(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);
            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(false, serializationMode);
            // row0: group0 groupKey = (0, "0")
            // row1: group0
            // row2: group1 groupKey = (1, "0")
            // row3: group2 groupKey = (1, "1")
            // row4: group3 groupKey = (2, "1")
            // row5: group3 groupKey = (2, "1")
            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // scans row0,1,2, return group0 aggregation result.
                // row2 is actually a group, but we don't know if the group is finished, so didn't return this group
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null, resultOf(0, "0", 1, 0, 0.5));
                // scans row3, returns group1 aggregation result, again, row3 is actually a group, but nothing is returned because we don't know if the group is finished
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf(1, "0", 2, 2, 2.0));
                // scans row4, returns group2 aggregation result
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes(), resultOf(1, "1", 3, 3, 3.0));
                // scans row5, hit SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes());
                // hit SOURCE_EXHAUSTED, returns group3 aggregation result
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes(), resultOf(2, "1", 9, 4, 4.5));
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            } else {
                // scan row0,1,2, return group0 and group1 aggregation results
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null, resultOf(0, "0", 1, 0, 0.5), resultOf(1, "0", 2, 2, 2.0));
                // scan row3, return group2 aggregation results
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf(1, "1", 3, 3, 3.0));
                // scan row4, return partial aggregation result of group3
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes(), resultOf(2, "1", 4, 4, 4.0));
                // start the next scan from 6th row, and scans the 6th row (recordScanLimit = 2), hit SCAN_LIMIT_REACHED, so return nothing
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes(), resultOf(2, "1", 5, 5, 5.0));
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes());
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void partialAggregateSum(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("str_value_indexed")
                            .build(false, serializationMode);
            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // In the testing data, there are 2 groups, each group has 3 rows.
                // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
                // although the first group contains exactly 3 rows, we don't know we've finished the first group before we get to the 4th row, so nothing is returned
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null);
                // start the next scan from 4th row, and scans the 4th row (recordScanLimit = 1), return the aggregated result of the first group
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf("0", 3));
                // start the next scan from 5th row, and scans the 5th row (recordScanLimit = 1), return nothing
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes());
                // start the next scan from 6th row, and scans the 6th row (recordScanLimit = 2), hit SCAN_LIMIT_REACHED, so return nothing
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes());
                // return the aggregated result of the second group
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes(), resultOf("1", 12));

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            } else {
                // scans first 3 rows, returns aggregation result
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null, resultOf("0", 3));
                // scans 4th row, returns "partial" aggregation result
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf("1", 3));
                // scans 5th row, returns "partial" aggregation result
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes(), resultOf("1", 4));
                // scans 6th row, returns "partial" aggregation result
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes(), resultOf("1", 5));
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes());

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void testFilterOutSecondGroup(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            // in the table, group1(str_value_indexed = "0") -> num_value_2 = 0, 1, 2, group2(str_value_indexed = "1") -> num_value_2 = 3, 4, 5.
            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("str_value_indexed")
                            .withQueryPredicate("num_value_2", Comparisons.Type.LESS_THAN, 3)
                            .build(false, serializationMode);

            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // scans num_value_2 = 0, 1, 2 then hit SCAN_LIMIT_REACHED, not knowing if this is end of the group, so return nothing
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 4, null);
                // scans num_value_2 = 3, 4, all filtered out by FilterCursor, so AggregateCursor doesn't receive any "innerResult", not knowing if this is end of the group, so return nothing
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 2, continuation1.toBytes());
                // scans num_value_2 = 5, hits SOURCE_EXHAUSTED, returns the result
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 2, continuation2.toBytes(), resultOf("0", 3));
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation3);
            } else {
                // scans num_value_2 = 0, 1, 2, returns result
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 4, null, resultOf("0", 3));
                // scans num_value_2 = 3, 4, all filtered out by FilterCursor
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 2, continuation1.toBytes());
                // scans num_value_2 = 5, hits SOURCE_EXHAUSTED
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 2, continuation2.toBytes());
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation3);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void testFilterOutFirstGroup(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            // in the table, group1(str_value_indexed = "0") -> num_value_2 = 0, 1, 2, group2(str_value_indexed = "1") -> num_value_2 = 3, 4, 5.
            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("str_value_indexed")
                            .withQueryPredicate("num_value_2", Comparisons.Type.GREATER_THAN, 2)
                            .build(false, serializationMode);

            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // scans num_value_2 = 0, 1, 2 then hit SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 4, null);
                // scans num_value_2 = 3, 4, then hit SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 2, continuation1.toBytes());
                // scans num_value_2 = 5, then hit SOURCE_EXHAUSTED, returns result
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 2, continuation2.toBytes(), resultOf("1", 12));
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation3);
            } else {
                // scans num_value_2 = 0, 1, 2 then hit SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 4, null);
                // scans num_value_2 = 3, 4, then hit SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 2, continuation1.toBytes(), resultOf("1", 7));
                // scans num_value_2 = 5, then hit SOURCE_EXHAUSTED, returns result
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 2, continuation2.toBytes(), resultOf("1", 5));
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation3);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void testFilterOutEverything(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            // in the table, group1(str_value_indexed = "0") -> num_value_2 = 0, 1, 2, group2(str_value_indexed = "1") -> num_value_2 = 3, 4, 5.
            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("str_value_indexed")
                            .withQueryPredicate("num_value_2", Comparisons.Type.LESS_THAN, 0)
                            .build(false, serializationMode);

            // scans num_value_2 = 0, 1, 2, then hit scan_limit_reached
            RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 4, null);
            // scans num_value_2 = 3, 4, then hit scan_limit_reached
            RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 2, continuation1.toBytes());
            // scans num_value_2 = 5, then hit source_exhausted
            RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 2, continuation2.toBytes());
            Assertions.assertEquals(RecordCursorEndContinuation.END, continuation3);
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void partialAggregateCountToNew(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new CountValue(CountValue.PhysicalOperator.COUNT, value))
                            .withGroupCriterion("str_value_indexed")
                            .build(false, serializationMode);

            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // In the testing data, there are 2 groups, each group has 3 rows.
                // scans 4 rows at a time
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 6, null, resultOf("0", 3L));
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 6, continuation1.toBytes(), resultOf("1", 3L));
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation2);
            } else {
                // In the testing data, there are 2 groups, each group has 3 rows.
                // scans 4 rows at a time
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 6, null, resultOf("0", 3L), resultOf("1", 1L));
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 6, continuation1.toBytes(), resultOf("1", 2L));
                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation2);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void partialAggregateSumWithoutGroupingKey(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .build(false, serializationMode);

            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // In the testing data, there are 6 rows.
                // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
                // because source is not exhausted, nothing is returned
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null);
                // start the next scan from 4th row, and scans the 4th row (recordScanLimit = 1), nothing is returned
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes());
                // start the next scan from 5th row, and scans the 5th row (recordScanLimit = 1), nothing is returned
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes());
                // start the next scan from 6th row, and scans the 6th row (recordScanLimit = 2), hit SCAN_LIMIT_REACHED, so return nothing
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes());
                // return the aggregated result of the second group
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes(), resultOf(15));

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            } else {
                // In the testing data, there are 6 rows.
                // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null, resultOf(3));
                // start the next scan from 4th row, and scans the 4th row (recordScanLimit = 1)
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf(3));
                // start the next scan from 5th row, and scans the 5th row (recordScanLimit = 1)
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes(), resultOf(4));
                // start the next scan from 6th row, and scans the 6th row (recordScanLimit = 2), hit SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes(), resultOf(5));
                // hits SOURCE_EXHAUSTED
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes());

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void partialAggregateAvg(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("str_value_indexed")
                            .build(false, serializationMode);

            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // In the testing data, there are 2 groups, each group has 3 rows.
                // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
                // although the first group contains exactly 3 rows, we don't know we've finished the first group before we get to the 4th row, so nothing is returned, continuation is back to START
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null);
                // start the next scan from 4th row, and scans the 4th row (recordScanLimit = 1), return the aggregated result of the first group
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf("0", 1.0));
                // start the next scan from 5th row, and scans the 5th row (recordScanLimit = 1), return nothing
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes());
                // start the next scan from 6th row, and scans the 6th row (recordScanLimit = 2), hit SCAN_LIMIT_REACHED, so return nothing
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes());
                // return the aggregated result of the second group
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes(), resultOf("1", 4.0));

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            } else {
                // In the testing data, there are 2 groups, each group has 3 rows.
                // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null, resultOf("0", 1.0));
                // start the next scan from 4th row
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf("1", 3.0));
                // start the next scan from 5th row
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes(), resultOf("1", 4.0));
                // start the next scan from 6th row
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes(), resultOf("1", 5.0));
                // hits SOURCE_EXHAUSTED
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes());

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordQueryStreamingAggregationPlan.SerializationMode.class, names = {"TO_OLD", "TO_NEW"})
    void partialAggregateBitmap(RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.BitmapConstructAgg(NumericAggregationValue.PhysicalOperator.BITMAP_CONSTRUCT_AGG_I, value))
                            .withGroupCriterion("str_value_indexed")
                            .build(false, serializationMode);

            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW) {
                // In the testing data, there are 2 groups, each group has 3 rows.
                // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
                // although the first group contains exactly 3 rows, we don't know we've finished the first group before we get to the 4th row, so nothing is returned, continuation is back to START
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null);
                // start the next scan from 4th row, and scans the 4th row (recordScanLimit = 1), return the aggregated result of the first group
                byte[] first = new byte[1250];
                // first[0] = b'00000111
                first[0] = 7;
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf("0", ByteString.copyFrom(first)));
                // start the next scan from 5th row, and scans the 5th row (recordScanLimit = 1), return nothing
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes());
                // start the next scan from 6th row, and scans the 6th row (recordScanLimit = 2), hit SCAN_LIMIT_REACHED, so return nothing
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes());
                // return the aggregated result of the second group
                byte[] second = new byte[1250];
                // second[0] = b'00111000
                second[0] = 56;
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes(), resultOf("1", ByteString.copyFrom(second)));

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            } else {
                // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
                byte[] first = new byte[1250];
                // first[0] = b'00000111
                first[0] = 7;
                RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null, resultOf("0", ByteString.copyFrom(first)));
                // start the next scan from 4th row
                byte[] fourth = new byte[1250];
                // fourth[0] = b'00001000
                fourth[0] = 8;
                RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf("1", ByteString.copyFrom(fourth)));
                // start the next scan from 5th row
                byte[] fifth = new byte[1250];
                // fifth[0] = b'00010000
                fifth[0] = 16;
                RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes(), resultOf("1", ByteString.copyFrom(fifth)));
                // start the next scan from 6th row
                byte[] sixth = new byte[1250];
                // sixth[0] = b'00100000
                sixth[0] = 32;
                RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes(), resultOf("1", ByteString.copyFrom(sixth)));
                RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes());

                Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
            }
        }
    }

    private static Stream<Arguments> provideArguments() {
        // (boolean, rowLimit)
        // setting rowLimit = 0 is equivalent to no limit
        List<Arguments> arguments = new LinkedList<>();
        for (int i = 0; i <= 4; i++) {
            arguments.add(Arguments.of(false, RecordQueryStreamingAggregationPlan.SerializationMode.TO_OLD, i));
            arguments.add(Arguments.of(false, RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW, i));
            arguments.add(Arguments.of(true, RecordQueryStreamingAggregationPlan.SerializationMode.TO_OLD, i));
            arguments.add(Arguments.of(true, RecordQueryStreamingAggregationPlan.SerializationMode.TO_NEW, i));
        }
        return arguments.stream();
    }

    private void populateDB(final int numRecords) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            final var recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            for (var i = 0; i <= numRecords; i++) {
                recBuilder.setRecNo(i);
                recBuilder.setNumValue2(i);
                recBuilder.setNumValue3Indexed(i / 2); // some field that changes every 2nd record
                recBuilder.setStrValueIndexed(Integer.toString(i / 3)); // some field that changes every 3rd record
                recBuilder.setNumValueUnique(i);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    @Nonnull
    private RecordCursor<QueryResult> executePlan(final RecordQueryPlan originalPlan, final int rowLimit, final int recordScanLimit, final byte[] continuation) {
        final RecordQueryPlan plan = verifySerialization(originalPlan);
        final var types = plan.getDynamicTypes();
        final var typeRepository = TypeRepository.newBuilder().addAllTypes(types).build();
        ExecuteState executeState;
        if (recordScanLimit > 0) {
            executeState = new ExecuteState(RecordScanLimiterFactory.enforce(recordScanLimit), ByteScanLimiterFactory.tracking());
        } else {
            executeState = ExecuteState.NO_LIMITS;
        }
        ExecuteProperties executeProperties = ExecuteProperties.SERIAL_EXECUTE;
        executeProperties = executeProperties.setReturnedRowLimit(rowLimit).setState(executeState);
        return plan.executePlan(recordStore, EvaluationContext.forTypeRepository(typeRepository), continuation, executeProperties);
    }

    private RecordCursorContinuation executePlanWithRecordScanLimit(final RecordQueryPlan plan, final int recordScanLimit, byte[] continuation, @Nullable List<?>... expectedResult) {
        List<QueryResult> queryResults = new LinkedList<>();
        try (RecordCursor<QueryResult> currentCursor = executePlan(plan, 0, recordScanLimit, continuation)) {
            RecordCursorResult<QueryResult> currentCursorResult;
            RecordCursorContinuation cursorContinuation;
            while (true) {
                currentCursorResult = currentCursor.getNext();
                cursorContinuation = currentCursorResult.getContinuation();
                if (!currentCursorResult.hasNext()) {
                    break;
                } else {
                    final var cur = currentCursorResult.get();
                    if (cur.getDatum() != null) {
                        queryResults.add(cur);
                    }
                }
            }
            RecordCursorResult<QueryResult> last = currentCursor.getNext();
            Assertions.assertFalse(last.hasNext());

            if (expectedResult == null) {
                Assertions.assertTrue(queryResults.isEmpty());
            } else {
                assertResults(this::assertResultFlattened, queryResults, expectedResult);
            }
            return cursorContinuation;
        }
    }

    private List<QueryResult> executePlanWithRowLimit(final RecordQueryPlan plan, final int rowLimit) {
        byte[] continuation = null;
        List<QueryResult> queryResults = new LinkedList<>();
        while (true) {
            try (RecordCursor<QueryResult> currentCursor = executePlan(plan, rowLimit, 0, continuation)) {
                RecordCursorResult<QueryResult> currentCursorResult;
                while (true) {
                    currentCursorResult = currentCursor.getNext();
                    continuation = currentCursorResult.getContinuation().toBytes();
                    if (!currentCursorResult.hasNext()) {
                        break;
                    }
                    queryResults.add(currentCursorResult.get());
                }
                if (currentCursorResult.getNoNextReason() == RecordCursor.NoNextReason.SOURCE_EXHAUSTED) {
                    break;
                }
            }
        }
        return queryResults;
    }

    private void assertResults(@Nonnull final BiConsumer<QueryResult, List<?>> checkConsumer, @Nonnull final List<QueryResult> actual, @Nonnull final List<?>... expected) {
        Assertions.assertEquals(expected.length, actual.size());
        for (var i = 0; i < actual.size(); i++) {
            checkConsumer.accept(actual.get(i), expected[i]);
        }
    }

    private void assertResultFlattened(final QueryResult actual, final List<?> expected) {
        final var message = actual.getMessage();
        Assertions.assertNotNull(message);
        final var resultFieldsBuilder = ImmutableList.builder();
        final var descriptor = message.getDescriptorForType();
        for (final var field : descriptor.getFields()) {
            resultFieldsBuilder.add(message.getField(field));
        }
        final var resultFields = resultFieldsBuilder.build();

        Assertions.assertEquals(resultFields.size(), expected.size());
        for (var i = 0; i < resultFields.size(); i++) {
            Assertions.assertEquals(expected.get(i), resultFields.get(i));
        }
    }

    private void assertResultNested(final QueryResult actual, final List<?> expected) {
        final var message = actual.getMessage();
        Assertions.assertNotNull(message);
        final var resultFieldsBuilder = ImmutableList.builder();

        final var descriptor = message.getDescriptorForType();
        final var topLevelFields = descriptor.getFields();
        Assertions.assertEquals(2, topLevelFields.size());

        final var groupingKeyFieldDescriptor = topLevelFields.get(0);
        final var groupingKeyDescriptor = groupingKeyFieldDescriptor.getMessageType();
        final var groupingKeyMessage = (Message)message.getField(groupingKeyFieldDescriptor);
        for (final var field : groupingKeyDescriptor.getFields()) {
            resultFieldsBuilder.add(groupingKeyMessage.getField(field));
        }

        final var aggregateFieldDescriptor = topLevelFields.get(1);
        final var aggregateDescriptor = aggregateFieldDescriptor.getMessageType();
        final var aggregateMessage = (Message)message.getField(aggregateFieldDescriptor);
        for (final var field : aggregateDescriptor.getFields()) {
            resultFieldsBuilder.add(aggregateMessage.getField(field));
        }

        final var resultFields = resultFieldsBuilder.build();

        Assertions.assertEquals(resultFields.size(), expected.size());
        for (var i = 0; i < resultFields.size(); i++) {
            Assertions.assertEquals(expected.get(i), resultFields.get(i));
        }
    }

    private List<?> resultOf(final Object... objects) {
        return Arrays.asList(objects);
    }

    private static class AggregationPlanBuilder {
        private final Quantifier.Physical quantifier;
        private final List<String> groupFieldNames;
        private final List<String> aggregateFieldNames;
        private final List<Function<Value, AggregateValue>> aggregateFunctions;
        private final List<QueryPredicate> queryPredicates;
        private final RecordMetaData recordMetaData;
        private final String recordTypeName;

        public AggregationPlanBuilder(final RecordMetaData recordMetaData, final String recordTypeName) {
            this.recordMetaData = recordMetaData;
            this.recordTypeName = recordTypeName;
            this.quantifier = createBaseQuantifier();
            this.groupFieldNames = new ArrayList<>();
            this.aggregateFieldNames = new ArrayList<>();
            this.aggregateFunctions = new ArrayList<>();
            this.queryPredicates = new ArrayList<>();
        }

        public AggregationPlanBuilder withAggregateValue(final String fieldName, final Function<Value, AggregateValue> aggregateValueFunction) {
            this.aggregateFieldNames.add(fieldName);
            this.aggregateFunctions.add(aggregateValueFunction);
            return this;
        }

        public AggregationPlanBuilder withGroupCriterion(final String fieldName) {
            this.groupFieldNames.add(fieldName);
            return this;
        }

        public AggregationPlanBuilder withQueryPredicate(final String fieldName, final Comparisons.Type comparisonType, final Object comparand) {
            this.queryPredicates.add(new ValuePredicate(createFieldValue(quantifier, fieldName), new Comparisons.SimpleComparison(comparisonType, comparand)));
            return this;
        }

        public RecordQueryPlan build(final boolean useNestedResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
            final var currentQuantifier = queryPredicates.isEmpty() ? quantifier : Quantifier.physical(Reference.plannedOf(new RecordQueryPredicatesFilterPlan(quantifier, queryPredicates)));

            List<Value> groupValues = new ArrayList<>();
            for (String f: groupFieldNames) {
                groupValues.add(createFieldValue(currentQuantifier, f));
            }
            List<Value> aggregateValues = new ArrayList<>();
            for (int i = 0; i < aggregateFieldNames.size(); i++) {
                aggregateValues.add(aggregateFunctions.get(i).apply(createFieldValue(currentQuantifier, aggregateFieldNames.get(i))));
            }
            final var groupingKeyValue = RecordConstructorValue.ofUnnamed(groupValues);
            final var aggregateValue = RecordConstructorValue.ofUnnamed(aggregateValues);
            if (useNestedResult) {
                return RecordQueryStreamingAggregationPlan.ofNested(currentQuantifier, groupingKeyValue, aggregateValue, serializationMode);
            } else {
                return RecordQueryStreamingAggregationPlan.ofFlattened(currentQuantifier, groupingKeyValue, aggregateValue, serializationMode);
            }
        }

        private Value createFieldValue(final Quantifier.Physical q, final String fieldName) {
            return FieldValue.ofFieldName(q.getFlowedObjectValue(), fieldName);
        }

        private Quantifier.Physical createBaseQuantifier() {
            final var resultType = recordMetaData.getPlannerType(recordTypeName);
            final var scanPlan = new RecordQueryScanPlan(ImmutableSet.of(recordTypeName), new Type.AnyRecord(false), null, ScanComparisons.EMPTY, false, false);
            final var filterPlan =
                    new RecordQueryTypeFilterPlan(Quantifier.physical(Reference.plannedOf(scanPlan)),
                            Collections.singleton(recordTypeName),
                            resultType);
            return Quantifier.physical(Reference.plannedOf(filterPlan));
        }
    }
}
