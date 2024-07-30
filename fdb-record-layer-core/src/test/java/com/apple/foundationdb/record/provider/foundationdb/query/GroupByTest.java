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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

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
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.sumAggregationValue;

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

    protected void setupHookAndAddData(final boolean addIndex, final boolean addAggregateIndex) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStoreTestBase.RecordMetaDataHook hook = (metaDataBuilder) -> {
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
            rec.setRecNo(2).setStrValueIndexed("2").setNumValueUnique(2).setNumValue2(1).setNumValue3Indexed(20);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(3).setStrValueIndexed("3").setNumValueUnique(3).setNumValue2(1).setNumValue3Indexed(30);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(4).setStrValueIndexed("4").setNumValueUnique(4).setNumValue2(2).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(5).setStrValueIndexed("5").setNumValueUnique(5).setNumValue2(2).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(6).setStrValueIndexed("6").setNumValueUnique(6).setNumValue2(2).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(7).setStrValueIndexed("7").setNumValueUnique(7).setNumValue2(3).setNumValue3Indexed(-10);
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
        }
    }
}
