/*
 * GroupByTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.aggregateIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.aggregations;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.groupings;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.streamingAggregationPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.bitmapAggregationValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.sumAggregationValue;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * test suite for {@code GROUP BY} expression planning and execution.
 */
@Tag(Tags.RequiresFDB)
public class GroupByTest extends FDBRecordStoreQueryTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimpleGroupBy() {
        setupHookAndAddData(true, false);
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructGroupByPlan(false, false),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        assertMatchesExactly(plan,
                mapPlan(
                        streamingAggregationPlan(
                                mapPlan(
                                        indexPlan()
                                                .where(scanComparisons(range("<,>")))
                                )).where(aggregations(recordConstructorValue(exactly(sumAggregationValue())))
                                .and(groupings(ValueMatchers.fieldValueWithFieldNames("select_grouping_cols"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void attemptToPlanGroupByWithoutCompatiblySortedIndexFails() {
        setupHookAndAddData(false, false);
        final var cascadesPlanner = (CascadesPlanner)planner;
        Assertions.assertThrows(RecordCoreException.class, () -> cascadesPlanner.planGraph(
                () -> constructGroupByPlan(false, false),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()), "Cascades planner could not plan query");
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testAggregateIndexPlanning() {
        setupHookAndAddData(false, true);
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructGroupByPlan(false, false),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        assertMatchesExactly(plan, mapPlan(aggregateIndexPlan()));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testIndexPlanningWithPredicateInSelectWhere() {
        setupHookAndAddData(true, false);
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructGroupByPlan(true, false),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        assertMatchesExactly(plan,
                mapPlan(
                        streamingAggregationPlan(
                                mapPlan(
                                        indexPlan()
                                                .where(scanComparisons(range("[[42],>")))
                                )).where(aggregations(recordConstructorValue(exactly(sumAggregationValue())))
                                .and(groupings(ValueMatchers.fieldValueWithFieldNames("select_grouping_cols"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testIndexPlanningWithPredicateInSelectWhereMatchesAggregateIndex() {
        setupHookAndAddData(true, true);
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructGroupByPlan(true, false),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        assertMatchesExactly(plan, mapPlan(aggregateIndexPlan().where(scanComparisons(range("[[42],>")))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testIndexPlanningWithPredicateInSelectWhereAndSelectHavingMatchesAggregateIndex() {
        setupHookAndAddData(true, true);
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructGroupByPlan(true, true),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();
        assertMatchesExactly(plan, mapPlan(aggregateIndexPlan().where(scanComparisons(range("[[42],[44]]")))));
    }

    @Nonnull
    private Reference constructGroupByPlan(final boolean withPredicateInSelectWhere,
                                           final boolean withPredicateInSelectHaving) {
        final var allRecordTypes = ImmutableSet.of("MySimpleRecord", "MyOtherRecord");
        var qun =
                Quantifier.forEach(Reference.of(
                        new FullUnorderedScanExpression(allRecordTypes,
                                new Type.AnyRecord(false),
                                new AccessHints())));

        qun = Quantifier.forEach(Reference.of(
                new LogicalTypeFilterExpression(ImmutableSet.of("MySimpleRecord"),
                        qun,
                        Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()))));

        final var num2Value = FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_2");

        final var scanAlias = qun.getAlias();
        final var groupByColAlias = CorrelationIdentifier.of("select_grouping_cols");
        final var groupingCol = Column.of(Optional.of("num_value_2"), num2Value);
        final var groupingColGroup = RecordConstructorValue.ofColumns(ImmutableList.of(groupingCol));

        // 1. build the underlying select, result expr = ( (num_value_2 as GB) <group1>, ($qun as qun) <group2>)
        {
            final var selectBuilder = GraphExpansion.builder();

            // <group1>
            final var col1 = Column.of(Optional.of(groupByColAlias.getId()), groupingColGroup);

            // <group2>
            final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
            final var col2 = Column.of(Optional.of(qun.getAlias().getId()), quantifiedValue);

            selectBuilder.addQuantifier(qun).addAllResultColumns(List.of(col1, col2));

            if (withPredicateInSelectWhere) {
                selectBuilder.addPredicate(new ValuePredicate(num2Value, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 42)));
            }

            qun = Quantifier.forEach(Reference.of(selectBuilder.build().buildSelect()));
        }

        // 2. build the group by expression, for that we need the aggregation expression and the grouping expression.
        {
            // 2.1. construct aggregate expression.
            final var aggCol = Column.of(Type.Record.Field.unnamedOf(Type.primitiveType(Type.TypeCode.LONG)),
                    new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.of(scanAlias.getId(), "num_value_3_indexed"))));
            final var aggregationExpr = RecordConstructorValue.ofColumns(ImmutableList.of(aggCol));

            // 2.2. construct grouping columns expression.
            final var groupingExpr = FieldValue.ofFieldName(qun.getFlowedObjectValue(), groupByColAlias.getId());

            // 2.3. construct the group by expression
            final var groupByExpression = new GroupByExpression(groupingExpr, aggregationExpr, GroupByExpression::nestedResults, qun);
            qun = Quantifier.forEach(Reference.of(groupByExpression));
        }

        // 3. construct the select expression on top containing the final result set
        {
            // construct a result set that makes sense.
            final var numValue2FieldValue = FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 0), "num_value_2");
            final var numValue2Reference = Column.of(Optional.of("num_value_2"), numValue2FieldValue);
            final var aggregateReference = Column.unnamedOf(FieldValue.ofOrdinalNumber(FieldValue.ofOrdinalNumber(ObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 0), 0));

            final var graphBuilder = GraphExpansion.builder().addQuantifier(qun).addAllResultColumns(ImmutableList.of(numValue2Reference,  aggregateReference));

            if (withPredicateInSelectHaving) {
                graphBuilder.addPredicate(new ValuePredicate(numValue2FieldValue, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 44)));
            }

            final var result = graphBuilder.build().buildSelect();
            qun = Quantifier.forEach(Reference.of(result));
            return Reference.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testBitMap() {
        RecordMetaDataHook hook = setupHookAndAddData(true, true);

        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructBitMapGroupByPlan(4),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        assertMatchesExactly(plan,
                mapPlan(
                        streamingAggregationPlan(
                                mapPlan(
                                        indexPlan()

                                )).where(aggregations(recordConstructorValue(exactly(bitmapAggregationValue(anyValue()))))
                                .and(groupings(ValueMatchers.fieldValueWithFieldNames("select_grouping_cols"))))));

        final Map<String, Integer> expectedResult = new HashMap<>();
        expectedResult.put("1", 10);
        expectedResult.put("2", 2);
        expectedResult.put("3", 2);
        expectedResult.put("4", 6);
        expectedResult.put("5", 4);
        expectedResult.put("6", 4);
        expectedResult.put("7", 12);
        expectedResult.put("8", 8);
        expectedResult.put("9", 8);
        expectedResult.put("10", 1);
        expectedResult.put("11", 1);

        final Map<String, Integer> bitMapGroupByStrValue = new HashMap<>();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            try (RecordCursor<QueryResult> cursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan, Bindings.EMPTY_BINDINGS)) {
                cursor.forEach(queryResult -> {
                    final Message queriedMessage = queryResult.getMessage();
                    final Descriptors.Descriptor recDescriptor = queriedMessage.getDescriptorForType();
                    String strValue = (String) queriedMessage.getField(recDescriptor.findFieldByName("str_value_indexed"));
                    int value = (int) queriedMessage.getField(recDescriptor.findFieldByName("bitmap_field"));
                    bitMapGroupByStrValue.put(strValue, value);
                }).join();
            }
        }
        assertEquals(expectedResult, bitMapGroupByStrValue);
    }

    @Nonnull
    private Reference constructBitMapGroupByPlan(int bucketSize) {
        // select str_value_indexed, bitmap(num_value_2 % bucketSize) from MySimpleRecord group by str_value_indexed
        final var allRecordTypes = ImmutableSet.of("MySimpleRecord", "MyOtherRecord");
        var qun =
                Quantifier.forEach(Reference.of(
                        new FullUnorderedScanExpression(allRecordTypes,
                                new Type.AnyRecord(false),
                                new AccessHints())));

        qun = Quantifier.forEach(Reference.of(
                new LogicalTypeFilterExpression(ImmutableSet.of("MySimpleRecord"),
                        qun,
                        Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()))));

        final LiteralValue<Integer> bucketSizeValue = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), bucketSize);

        final var strValueIndexed = FieldValue.ofFieldName(qun.getFlowedObjectValue(), "str_value_indexed");

        final var scanAlias = qun.getAlias();
        final var groupByColAlias = CorrelationIdentifier.of("select_grouping_cols");
        final var groupingColStrValueIndexed = Column.of(Optional.of("str_value_indexed"), strValueIndexed);
        final var groupingColGroup = RecordConstructorValue.ofColumns(ImmutableList.of(groupingColStrValueIndexed));

        // 1. build the underlying select, result expr = ( (num_value_2 as GB) <group1>, ($qun as qun) <group2>)
        {
            final var selectBuilder = GraphExpansion.builder();

            // <group1>
            final var col1 = Column.of(Optional.of(groupByColAlias.getId()), groupingColGroup);

            // <group2>
            final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
            final var col2 = Column.of(Optional.of(qun.getAlias().getId()), quantifiedValue);

            selectBuilder.addQuantifier(qun).addAllResultColumns(List.of(col1, col2));

            qun = Quantifier.forEach(Reference.of(selectBuilder.build().buildSelect()));
        }

        // 2. build the group by expression, for that we need the aggregation expression and the grouping expression.
        {
            // 2.1. construct aggregate expression.
            FieldValue num2 = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.of(scanAlias.getId(), "num_value_2"));
            List<Typed> aggregatedFieldRef = List.of(new ArithmeticValue.ModFn().encapsulate(List.of(num2, bucketSizeValue)));
            final Value bitMapValue = (Value) new NumericAggregationValue.BitMapFn().encapsulate(aggregatedFieldRef);
            final var aggCol = Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("bitmap_field")), bitMapValue);
            final var aggregationExpr = RecordConstructorValue.ofColumns(ImmutableList.of(aggCol));

            // 2.2. construct grouping columns expression.
            final var groupingExpr = FieldValue.ofFieldName(qun.getFlowedObjectValue(), groupByColAlias.getId());

            // 2.3. construct the group by expression
            final var groupByExpression = new GroupByExpression(groupingExpr, aggregationExpr, GroupByExpression::nestedResults, qun);
            qun = Quantifier.forEach(Reference.of(groupByExpression));
        }

        // 3. construct the select expression on top containing the final result set
        {
            // construct a result set that makes sense.
            final var strValueIndexedFieldValue = FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 0), "str_value_indexed");
            final var strValueIndexedReference = Column.of(Optional.of("str_value_indexed"), strValueIndexedFieldValue);
            final var bitMapNumValue2FieldValue = FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 1), "bitmap_field");
            final var bitMapNumValue2Reference = Column.of(Optional.of("bitmap_field"), bitMapNumValue2FieldValue);

            final var graphBuilder = GraphExpansion.builder().addQuantifier(qun).addAllResultColumns(ImmutableList.of(strValueIndexedReference, bitMapNumValue2Reference));

            final var result = graphBuilder.build().buildSelect();
            qun = Quantifier.forEach(Reference.of(result));
            return Reference.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
        }
    }

    protected RecordMetaDataHook setupHookAndAddData(final boolean addIndex, final boolean addAggregateIndex) {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataHook hook = (metaDataBuilder) -> {
                complexQuerySetupHook().apply(metaDataBuilder);
                if (addIndex) {
                    metaDataBuilder.addIndex("MySimpleRecord", "MySimpleRecord$num_value_2", field("num_value_2"));
                }
                if (addAggregateIndex) {
                    metaDataBuilder.addIndex("MySimpleRecord", new Index("AggIndex", field("num_value_3_indexed").groupBy(field("num_value_2")), IndexTypes.SUM));
                }
            };
            openSimpleRecordStore(context, hook);
            var rec = TestRecords1Proto.MySimpleRecord.newBuilder();
            rec.setRecNo(1).setStrValueIndexed("1").setNumValueUnique(1).setNumValue2(1).setNumValue3Indexed(10);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(12).setStrValueIndexed("1").setNumValueUnique(12).setNumValue2(7).setNumValue3Indexed(10);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(2).setStrValueIndexed("2").setNumValueUnique(2).setNumValue2(1).setNumValue3Indexed(20);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(3).setStrValueIndexed("3").setNumValueUnique(3).setNumValue2(1).setNumValue3Indexed(30);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(4).setStrValueIndexed("4").setNumValueUnique(4).setNumValue2(2).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(13).setStrValueIndexed("4").setNumValueUnique(13).setNumValue2(5).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(5).setStrValueIndexed("5").setNumValueUnique(5).setNumValue2(2).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(6).setStrValueIndexed("6").setNumValueUnique(6).setNumValue2(2).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(7).setStrValueIndexed("7").setNumValueUnique(7).setNumValue2(3).setNumValue3Indexed(-10);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(14).setStrValueIndexed("7").setNumValueUnique(14).setNumValue2(6).setNumValue3Indexed(-10);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(8).setStrValueIndexed("8").setNumValueUnique(8).setNumValue2(3).setNumValue3Indexed(-20);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(9).setStrValueIndexed("9").setNumValueUnique(9).setNumValue2(3).setNumValue3Indexed(-30);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(10).setStrValueIndexed("10").setNumValueUnique(10).setNumValue2(4).setNumValue3Indexed(100);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(11).setStrValueIndexed("11").setNumValueUnique(11).setNumValue2(4).setNumValue3Indexed(2000);
            recordStore.saveRecord(rec.build());
            commit(context);
            return hook;
        }
    }
}
