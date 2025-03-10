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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.ByteScanLimiterFactory;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordScanLimiterFactory;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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
    void noAggregateGroupByNone(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByOne(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, 1), resultOf(1, 5), resultOf(2, 9));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByNone(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(15));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void noAggregateGroupByOne(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0), resultOf(1), resultOf(2));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByTwo(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1), resultOf(1, "0", 2), resultOf(1, "1", 3), resultOf(2, "1", 9));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateTwoGroupByTwo(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1, 0), resultOf(1, "0", 2, 2), resultOf(1, "1", 3, 3), resultOf(2, "1", 9, 4));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateThreeGroupByTwo(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1, 0, 0.5), resultOf(1, "0", 2, 2, 2.0), resultOf(1, "1", 3, 3, 3.0), resultOf(2, "1", 9, 4, 4.5));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateOneGroupByThree(final boolean useNestedResult, final int rowLimit) {
        // each group only has one row
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .withGroupCriterion("num_value_unique")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 0, 0), resultOf(0, "0", 1, 1), resultOf(1, "0", 2, 2), resultOf(1, "1", 3, 3), resultOf(2, "1", 4, 4), resultOf(2, "1", 5, 5));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecords(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecordsNoGroup(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan = new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Min(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Avg(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                    .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecordsNoAggregate(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @MethodSource("provideArguments")
    void aggregateNoRecordsNoGroupNoAggregate(final boolean useNestedResult, final int rowLimit) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .build(useNestedResult);

            final var result = executePlanWithRowLimit(plan, rowLimit);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @Test
    void aggregateHitScanLimitReached() {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("str_value_indexed")
                            .build(false);

            // In the testing data, there are 2 groups, each group has 3 rows.
            // recordScanLimit = 5: scans 3 rows, and the 4th scan hits SCAN_LIMIT_REACHED
            // although the first group contains exactly 3 rows, we don't know we've finished the first group before we get to the 4th row, so nothing is returned, continuation is back to START
            RecordCursorContinuation continuation1 = executePlanWithRecordScanLimit(plan, 5, null, null);
            // start the next scan from 4th row, and scans the 4th row (recordScanLimit = 1), return the aggregated result of the first group
            RecordCursorContinuation continuation2 = executePlanWithRecordScanLimit(plan, 1, continuation1.toBytes(), resultOf("0", 3));
            // start the next scan from 5th row, and scans the 5th row (recordScanLimit = 1), return nothing
            RecordCursorContinuation continuation3 = executePlanWithRecordScanLimit(plan, 1, continuation2.toBytes(), null);
            // start the next scan from 6th row, and scans the 6th row (recordScanLimit = 2), return 2nd group aggregated result
            RecordCursorContinuation continuation4 = executePlanWithRecordScanLimit(plan, 1, continuation3.toBytes(), null);
            // (TODO): return exhausted, but not result, probably when finish scan x row, needs to return (x-1) to avoid this from happening
            RecordCursorContinuation continuation5 = executePlanWithRecordScanLimit(plan, 1, continuation4.toBytes(), resultOf("1", 12));

            Assertions.assertEquals(RecordCursorEndContinuation.END, continuation5);
        }
    }

    private static Stream<Arguments> provideArguments() {
        // (boolean, rowLimit)
        // setting rowLimit = 0 is equivalent to no limit
        List<Arguments> arguments = new LinkedList<>();
        for (int i = 0; i <= 4; i++) {
            arguments.add(Arguments.of(false, i));
            arguments.add(Arguments.of(true, i));
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
    private RecordCursor<QueryResult> executePlan(final RecordQueryPlan plan, final int rowLimit, final int recordScanLimit, final byte[] continuation) {
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
        try {
            if (continuation == null) {
                return plan.executePlan(recordStore, EvaluationContext.forTypeRepository(typeRepository), null, executeProperties);
            } else {
                RecordCursorProto.AggregateCursorContinuation continuationProto = RecordCursorProto.AggregateCursorContinuation.parseFrom(continuation);
                if (continuationProto.hasPartialAggregationResults()) {
                    return plan.executePlan(recordStore, EvaluationContext.forBindingsAndTypeRepositoryAndPartialAggregationResult(Bindings.EMPTY_BINDINGS, typeRepository, continuationProto.getPartialAggregationResults()), continuationProto.getContinuation().toByteArray(), executeProperties);
                } else {
                    return plan.executePlan(recordStore, EvaluationContext.forTypeRepository(typeRepository), continuationProto.getContinuation().toByteArray(), executeProperties);
                }
            }
        } catch (final Throwable t) {
            throw Assertions.<RuntimeException>fail(t);
        }
    }

    private RecordCursorContinuation executePlanWithRecordScanLimit(final RecordQueryPlan plan, final int recordScanLimit, byte[] continuation, @Nullable List<?> expectedResult) {
        List<QueryResult> queryResults = new LinkedList<>();
        RecordCursor<QueryResult> currentCursor = executePlan(plan, 0, recordScanLimit, continuation);
        RecordCursorResult<QueryResult> currentCursorResult;
        RecordCursorContinuation cursorContinuation;
        while (true) {
            currentCursorResult = currentCursor.getNext();
            cursorContinuation = currentCursorResult.getContinuation();
            if (!currentCursorResult.hasNext()) {
                break;
            }
            queryResults.add(currentCursorResult.get());
        }
        if (expectedResult == null) {
            Assertions.assertTrue(queryResults.isEmpty());
        } else {
            assertResults(this::assertResultFlattened, queryResults, expectedResult);
        }
        return cursorContinuation;
    }

    private List<QueryResult> executePlanWithRowLimit(final RecordQueryPlan plan, final int rowLimit) {
        byte[] continuation = null;
        List<QueryResult> queryResults = new LinkedList<>();
        while (true) {
            RecordCursor<QueryResult> currentCursor = executePlan(plan, rowLimit, 0, continuation);
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
        private final List<AggregateValue> aggregateValues;
        private final List<Value> groupValues;
        private final RecordMetaData recordMetaData;
        private final String recordTypeName;

        public AggregationPlanBuilder(final RecordMetaData recordMetaData, final String recordTypeName) {
            this.recordMetaData = recordMetaData;
            this.recordTypeName = recordTypeName;
            this.quantifier = createBaseQuantifier();
            this.aggregateValues = new ArrayList<>();
            this.groupValues = new ArrayList<>();
        }

        public AggregationPlanBuilder withAggregateValue(final String fieldName, final Function<Value, AggregateValue> aggregateValueFunction) {
            this.aggregateValues.add(aggregateValueFunction.apply(createFieldValue(fieldName)));
            return this;
        }

        public AggregationPlanBuilder withGroupCriterion(final String fieldName) {
            this.groupValues.add(createFieldValue(fieldName));
            return this;
        }

        public RecordQueryPlan build(final boolean useNestedResult) {
            final var groupingKeyValue = RecordConstructorValue.ofUnnamed(groupValues);
            final var aggregateValue = RecordConstructorValue.ofUnnamed(aggregateValues);
            if (useNestedResult) {
                return RecordQueryStreamingAggregationPlan.ofNested(quantifier, groupingKeyValue, aggregateValue);
            } else {
                return RecordQueryStreamingAggregationPlan.ofFlattened(quantifier, groupingKeyValue, aggregateValue);
            }
        }

        private Value createFieldValue(final String fieldName) {
            return FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), fieldName);
        }

        private Quantifier.Physical createBaseQuantifier() {
            final var resultType = Type.Record.fromFieldDescriptorsMap(recordMetaData.getFieldDescriptorMapFromNames(ImmutableSet.of(recordTypeName)));
            final var scanPlan = new RecordQueryScanPlan(ImmutableSet.of(recordTypeName), resultType, null, ScanComparisons.EMPTY, false, false);
            final var filterPlan =
                    new RecordQueryTypeFilterPlan(Quantifier.physical(Reference.of(scanPlan)),
                            Collections.singleton(recordTypeName),
                            resultType);
            return Quantifier.physical(Reference.of(filterPlan));
        }
    }
}
