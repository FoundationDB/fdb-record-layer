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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.dynamic.TypeRepository;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.NumericAggregationValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

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
     *
     * MyOtherRecord: Empty.
     *
     */
    @BeforeEach
    public void setup() throws Exception {
        populateDB(5);
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void noAggregateGroupByNone(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateOneGroupByOne(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                    .withGroupCriterion("num_value_3_indexed")
                    .build(useNestedResult);

            final var result = executePlan(plan);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, 1), resultOf(1, 5), resultOf(2, 9));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateOneGroupByNone(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .build(useNestedResult);

            final var result = executePlan(plan);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(15));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void noAggregateGroupByOne(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0), resultOf(1), resultOf(2));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateOneGroupByTwo(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1), resultOf(1, "0", 2), resultOf(1, "1", 3), resultOf(2, "1", 9));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateTwoGroupByTwo(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1, 0), resultOf(1, "0", 2, 2), resultOf(1, "1", 3, 3), resultOf(2, "1", 9, 4));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateThreeGroupByTwo(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MySimpleRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .withGroupCriterion("str_value_indexed")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            assertResults(useNestedResult ? this::assertResultNested : this::assertResultFlattened, result, resultOf(0, "0", 1, 0, 0.5), resultOf(1, "0", 2, 2, 2.0), resultOf(1, "1", 3, 3, 3.0), resultOf(2, "1", 9, 4, 4.5));
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateNoRecords(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                            .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateNoRecordsNoGroup(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan = new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, value))
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.MIN_I, value))
                    .withAggregateValue("num_value_2", value -> new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.AVG_I, value))
                    .build(useNestedResult);

            final var result = executePlan(plan);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateNoRecordsNoAggregate(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .withGroupCriterion("num_value_3_indexed")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @ParameterizedTest(name = "[{displayName}-{index}] {0}")
    @BooleanSource
    void aggregateNoRecordsNoGroupNoAggregate(final boolean useNestedResult) {
        try (final var context = openContext()) {
            openSimpleRecordStore(context, NO_HOOK);

            final var plan =
                    new AggregationPlanBuilder(recordStore.getRecordMetaData(), "MyOtherRecord")
                            .build(useNestedResult);

            final var result = executePlan(plan);
            Assertions.assertTrue(result.isEmpty());
        }
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
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    @Nonnull
    private List<QueryResult> executePlan(final RecordQueryPlan plan) {
        final var types = plan.getDynamicTypes();
        final var typeRepository = TypeRepository.newBuilder().addAllTypes(types).build();
        try {
            return plan.executePlan(recordStore, EvaluationContext.forTypeRepository(typeRepository), null, ExecuteProperties.SERIAL_EXECUTE).asList().get();
        } catch (final Throwable t) {
            throw Assertions.<RuntimeException>fail(t);
        }
    }

    private void assertResults(@Nonnull final BiConsumer<QueryResult, List<?>> checkConsumer, @Nonnull final List<QueryResult> actual, @Nonnull final List<?>... expected) {
        Assertions.assertEquals(expected.length, actual.size());
        for (var i = 0 ; i < actual.size() ; i++) {
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
        for (var i = 0 ; i < resultFields.size() ; i++) {
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
        for (var i = 0 ; i < resultFields.size() ; i++) {
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
            return RecordQueryStreamingAggregationPlan.of(quantifier,
                    RecordConstructorValue.ofUnnamed(groupValues),
                    RecordConstructorValue.ofUnnamed(aggregateValues),
                    useNestedResult ? RecordQueryStreamingAggregationPlan::nestedResults : RecordQueryStreamingAggregationPlan::flattenedResults);
        }

        private Value createFieldValue(final String fieldName) {
            return new FieldValue(quantifier.getFlowedObjectValue(), Collections.singletonList(fieldName));
        }

        private Quantifier.Physical createBaseQuantifier() {
            final var scanPlan = new RecordQueryScanPlan(ImmutableSet.of(recordTypeName), ScanComparisons.EMPTY, false);
            final var filterPlan =
                    new RecordQueryTypeFilterPlan(Quantifier.physical(GroupExpressionRef.of(scanPlan)),
                            Collections.singleton(recordTypeName),
                            Type.Record.fromFieldDescriptorsMap(recordMetaData.getFieldDescriptorMapFromNames(ImmutableSet.of(recordTypeName))));
            return Quantifier.physical(GroupExpressionRef.of(filterPlan));
        }
    }
}
