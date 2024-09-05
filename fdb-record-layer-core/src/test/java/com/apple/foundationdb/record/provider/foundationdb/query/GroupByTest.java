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
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.BitmapValueIndexMaintainer;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
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
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.Collections;
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
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.bitmapConstructAggValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.sumAggregationValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                                .and(groupings(ValueMatchers.anyValue())))));
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
                                .and(groupings(anyValue())))));
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

        // 1. build the underlying select, result expr = ( ($qun as qun) <qun>)
        {
            final var selectBuilder = GraphExpansion.builder();

            // <qun>
            final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
            final var col2 = Column.of(Optional.of(qun.getAlias().getId()), quantifiedValue);

            selectBuilder.addQuantifier(qun).addAllResultColumns(List.of(col2));

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
            final var groupingExpr = RecordConstructorValue.ofUnnamed(ImmutableList.of(FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 0), "num_value_2")));

            // 2.3. construct the group by expression
            final var groupByExpression = new GroupByExpression(groupingExpr, aggregationExpr, GroupByExpression::nestedResults, qun);
            qun = Quantifier.forEach(Reference.of(groupByExpression));
        }

        // 3. construct the select expression on top containing the final result set
        {
            // construct a result set that makes sense.
            final var numValue2FieldValue = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 0), 0);
            final var numValue2Reference = Column.of(Optional.of("num_value_2"), numValue2FieldValue);
            final var aggregateReference = Column.unnamedOf(FieldValue.ofOrdinalNumber(FieldValue.ofOrdinalNumber(ObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 1), 0));

            final var graphBuilder = GraphExpansion.builder().addQuantifier(qun).addAllResultColumns(ImmutableList.of(numValue2Reference,  aggregateReference));

            if (withPredicateInSelectHaving) {
                graphBuilder.addPredicate(new ValuePredicate(numValue2FieldValue, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 44)));
            }

            final var result = graphBuilder.build().buildSelect();
            qun = Quantifier.forEach(Reference.of(result));
            return Reference.of(LogicalSortExpression.unsorted(qun));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testBitMapWithStreamAggregation() {
        int bitBucketSize = BitmapValueIndexMaintainer.DEFAULT_ENTRY_SIZE;
        RecordMetaDataHook hook = setupHookAndAddData(true, false, false, true, bitBucketSize);

        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructBitMapGroupByPlan(bitBucketSize),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();

        assertBitMapResult(hook, plan, 1);
        assertMatchesExactly(plan,
                mapPlan(
                        streamingAggregationPlan(
                                mapPlan(
                                        indexPlan()

                                )).where(aggregations(recordConstructorValue(exactly(bitmapConstructAggValue(anyValue()))))
                                .and(groupings(ValueMatchers.anyValue())))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testBitMapWithBitMapIndex() {
        int bitBucketSize = 4;
        RecordMetaDataHook hook = setupHookAndAddData(true, true, true, false, bitBucketSize);
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                () -> constructBitMapGroupByPlan(bitBucketSize),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();
        assertBitMapResult(hook, plan, 1);
        assertMatchesExactly(plan, mapPlan(aggregateIndexPlan()));
        assertEquals(1, plan.getUsedIndexes().size());
        assertTrue(plan.getUsedIndexes().contains("BitMapIndex"));
    }

    private void assertBitMapResult(RecordMetaDataHook hook, RecordQueryPlan plan, int expectedByteArrayLength) {
        final Map<Pair<String, Integer>, List<Long>> expectedResult = new HashMap<>();
        expectedResult.put(Pair.of("1", 0), List.of(1L)); // str_value_indexed = "1", num_value_2 = 1 and 7
        expectedResult.put(Pair.of("1", 4), List.of(3L)); // 7 mod 4 = 3
        expectedResult.put(Pair.of("2", 0), List.of(1L)); // str_value_indexed = "2", num_value_2 = 1
        expectedResult.put(Pair.of("3", 0), List.of(1L)); // str_value_indexed = "3", num_value_2 = 1
        expectedResult.put(Pair.of("4", 0), List.of(2L)); // str_value_indexed = "4", num_value_2 = 2 and 5
        expectedResult.put(Pair.of("4", 4), List.of(1L)); // 5 mod 4 = 1
        expectedResult.put(Pair.of("5", 0), List.of(2L)); // str_value_indexed = "5", num_value_2 = 2
        expectedResult.put(Pair.of("6", 0), List.of(2L)); // str_value_indexed = "6", num_value_2 = 2
        expectedResult.put(Pair.of("7", 0), List.of(3L)); // str_value_indexed = "7", num_value_2 = 3 and 6
        expectedResult.put(Pair.of("7", 4), List.of(2L)); // 6 mod 4 = 2
        expectedResult.put(Pair.of("8", 0), List.of(3L)); // str_value_indexed = "8", num_value_2 = 3
        expectedResult.put(Pair.of("9", 0), List.of(3L)); // str_value_indexed = "9", num_value_2 = 3
        expectedResult.put(Pair.of("10", 4), List.of(0L)); // str_value_indexed = "10", num_value_2 = 4
        expectedResult.put(Pair.of("11", 4), List.of(0L)); // str_value_indexed = "11", num_value_2 = 4

        final Map<Pair<String, Integer>, List<Long>> bitMapGroupByStrValue = new HashMap<>();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            try (RecordCursor<QueryResult> cursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan, Bindings.EMPTY_BINDINGS)) {
                cursor.forEach(queryResult -> {
                    final Message queriedMessage = queryResult.getMessage();
                    final Descriptors.Descriptor recDescriptor = queriedMessage.getDescriptorForType();
                    String strValue = (String) queriedMessage.getField(recDescriptor.findFieldByName("str_value_indexed"));
                    Integer bitBucketValue = (int) queriedMessage.getField(recDescriptor.findFieldByName("bit_bucket_num_value_2"));
                    byte[] value = ((ByteString) queriedMessage.getField(recDescriptor.findFieldByName("bitmap_field"))).toByteArray();
                    assertEquals(expectedByteArrayLength, value.length);
                    bitMapGroupByStrValue.put(Pair.of(strValue, bitBucketValue), AggregateValueTest.collectOnBits(value, expectedByteArrayLength));
                }).join();
            }
        }
        assertEquals(expectedResult, bitMapGroupByStrValue);
    }

    @Nonnull
    private Reference constructBitMapGroupByPlan(int bucketSize) {
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

        final var scanAlias = qun.getAlias();
        final LiteralValue<Integer> bucketSizeValue = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), bucketSize);

        // 1. build the underlying select, result expr = ($qun as qun)
        {
            final var selectBuilder = GraphExpansion.builder();
            final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
            selectBuilder.addQuantifier(qun).addAllResultColumns(List.of(Column.of(Optional.of(qun.getAlias().getId()), quantifiedValue)));
            qun = Quantifier.forEach(Reference.of(selectBuilder.build().buildSelect()));
        }

        // 2. build the group by expression, for that we need the aggregation expression and the grouping expression.
        {
            // 2.1. construct aggregate expression.
            final var num2 = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.of(scanAlias.getId(), "num_value_2"));
            final var bitBucketNumValue2FieldValue = (Value)new ArithmeticValue.BitMapBucketOffsetFn().encapsulate(List.of(num2, bucketSizeValue));

            final Value bitMapValue = (Value) new NumericAggregationValue.BitMapConstructAggFn().encapsulate(List.of(new ArithmeticValue.ModFn().encapsulate(List.of(num2, bucketSizeValue))));
            final var aggCol = Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES), Optional.of("bitmap_field")), bitMapValue);
            final var aggregationExpr = RecordConstructorValue.ofColumns(ImmutableList.of(aggCol));

            // 2.2. construct grouping columns expression.
            final var strValueIndexed = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.of(scanAlias.getId(), "str_value_indexed"));
            final var groupingColStrValueIndexed = Column.of(Optional.of("str_value_indexed"), strValueIndexed);

            final var groupingColNumValue2 = Column.of(Optional.of("bit_bucket_num_value_2"), bitBucketNumValue2FieldValue);

            RecordConstructorValue groupingExpr = RecordConstructorValue.ofColumns(ImmutableList.of(groupingColStrValueIndexed, groupingColNumValue2));

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

            final var bitBucketNumValue2FieldValue = FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 0), "bit_bucket_num_value_2");
            final var bitBucketNumValue2Reference = Column.of(Optional.of("bit_bucket_num_value_2"), bitBucketNumValue2FieldValue);
            GraphExpansion.Builder graphBuilder = GraphExpansion.builder().addQuantifier(qun).addAllResultColumns(ImmutableList.of(strValueIndexedReference, bitBucketNumValue2Reference, bitMapNumValue2Reference));

            final var result = graphBuilder.build().buildSelect();
            qun = Quantifier.forEach(Reference.of(result));
            return Reference.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
        }
    }

    protected RecordMetaDataHook setupHookAndAddData(final boolean addIndex, final boolean addAggregateIndex) {
        return setupHookAndAddData(addIndex, addAggregateIndex, false, false, 0);
    }

    protected RecordMetaDataHook setupHookAndAddData(final boolean addIndex, final boolean addAggregateIndex, final boolean addBitMapIndex, final boolean addBitBucketFunctionIndex, int bucketSize) {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataHook hook = (metaDataBuilder) -> {
                complexQuerySetupHook().apply(metaDataBuilder);
                if (addIndex) {
                    metaDataBuilder.addIndex("MySimpleRecord", "MySimpleRecord$num_value_2", field("num_value_2"));
                }
                if (addAggregateIndex) {
                    metaDataBuilder.addIndex("MySimpleRecord", new Index("AggIndex", field("num_value_3_indexed").groupBy(field("num_value_2")), IndexTypes.SUM));
                }
                if (addBitMapIndex) {
                    metaDataBuilder.addIndex("MySimpleRecord", new Index("BitMapIndex", field("num_value_2").groupBy(field("str_value_indexed")), IndexTypes.BITMAP_VALUE, ImmutableMap.of(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, String.valueOf(bucketSize))));
                }
                if (addBitBucketFunctionIndex) {
                    metaDataBuilder.addIndex("MySimpleRecord", "MySimpleRecord$bit_bucket", concat(field("str_value_indexed"), bitBucketExpression(field("num_value_2"), bucketSize)));
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

    @Nonnull
    private static FunctionKeyExpression bitBucketExpression(@Nonnull KeyExpression fieldExpression, int bucketSize) {
        return Key.Expressions.function("bitmap_bucket_offset", concat(fieldExpression, Key.Expressions.value(bucketSize)));
    }

    @Nonnull
    private static FunctionKeyExpression modExpression(@Nonnull KeyExpression fieldExpression, int bucketSize) {
        return Key.Expressions.function("mod", concat(fieldExpression, Key.Expressions.value(bucketSize)));
    }
}
