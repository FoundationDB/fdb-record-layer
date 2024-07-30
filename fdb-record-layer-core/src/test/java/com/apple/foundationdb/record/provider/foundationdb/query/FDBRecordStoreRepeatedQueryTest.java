/*
 * FDBRecordStoreRepeatedQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.aggregateIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests of aggregate queries where one of the grouping columns is a repeated field.
 */
@Tag(Tags.RequiresFDB)
class FDBRecordStoreRepeatedQueryTest extends FDBRecordStoreQueryTestBase {
    private static final String REPEATED_INDEX_NAME = "repeaterIndex";
    private static final String SUM_BY_REPEATED_INDEX_NAME = "repeatedGroupSum";

    static RecordMetaDataHook addRepeaterIndex() {
        return metaDataBuilder -> {
            final Index index = new Index(
                    REPEATED_INDEX_NAME,
                    field("repeater", FanType.FanOut)
            );
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
    }

    @Nonnull
    static RecordMetaDataHook addSumByRepeatedIndex() {
        return metaDataBuilder -> {
            final Index index = new Index(
                    SUM_BY_REPEATED_INDEX_NAME,
                    field("num_value_2").groupBy(concat(field("repeater", FanType.FanOut), field("num_value_3_indexed"))),
                    IndexTypes.SUM
            );
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
    }

    @Nonnull
    private Quantifier repeatExpansion(@Nonnull Quantifier baseQun, @Nonnull Function<Value, List<QueryPredicate>> repeatValuePredicates) {
        ExplodeExpression explodeExpression = ExplodeExpression.explodeField((Quantifier.ForEach)baseQun, ImmutableList.of("repeater"));
        Quantifier repeatQun = Quantifier.forEach(Reference.of(explodeExpression));
        final var repeatSelectBuilder = GraphExpansion.builder();
        repeatSelectBuilder.addQuantifier(repeatQun);
        List<QueryPredicate> predicates = repeatValuePredicates.apply(repeatQun.getFlowedObjectValue());
        repeatSelectBuilder.addAllPredicates(predicates);
        repeatSelectBuilder.addResultColumn(Column.unnamedOf(repeatQun.getFlowedObjectValue()));
        return Quantifier.forEach(Reference.of(repeatSelectBuilder.build().buildSelect()));
    }

    @Nonnull
    private Quantifier selectWhereSumByRepeated(@Nonnull Quantifier baseQun, @Nonnull Function<Value, List<QueryPredicate>> repeatValuePredicates) {
        final var selectWhereBuilder = GraphExpansion.builder();
        selectWhereBuilder.addQuantifier(baseQun);
        Quantifier repeatQun = repeatExpansion(baseQun, repeatValuePredicates);
        selectWhereBuilder.addQuantifier(repeatQun);
        selectWhereBuilder
                .addResultValue(baseQun.getFlowedObjectValue())
                .addResultValue(repeatQun.getFlowedObjectValue());
        return Quantifier.forEach(Reference.of(selectWhereBuilder.build().buildSelect()));
    }

    @Nonnull
    private Quantifier selectWhereSumByRepeated(@Nonnull Quantifier baseQun) {
        return selectWhereSumByRepeated(baseQun, ignore -> ImmutableList.of());
    }

    @Nonnull
    private Quantifier sumByGroup(@Nonnull Quantifier selectWhere) {
        var baseReference = FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 0);
        final FieldValue groupedValue = FieldValue.ofFieldName(baseReference, "num_value_2");
        var aggregatedFieldRef = FieldValue.ofFields(selectWhere.getFlowedObjectValue(), baseReference.getFieldPath().withSuffix(groupedValue.getFieldPath()));
        final Value sumValue = (Value) new NumericAggregationValue.SumFn().encapsulate(ImmutableList.of(aggregatedFieldRef));
        final FieldValue groupingCol1 = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumberAndFuseIfPossible(selectWhere.getFlowedObjectValue(), 1), 0);
        final FieldValue groupingCol2 = FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumberAndFuseIfPossible(selectWhere.getFlowedObjectValue(), 0), "num_value_3_indexed");
        final RecordConstructorValue groupingValue = RecordConstructorValue.ofUnnamed(ImmutableList.of(groupingCol1, groupingCol2));
        final GroupByExpression groupByExpression = new GroupByExpression(groupingValue, RecordConstructorValue.ofUnnamed(ImmutableList.of(sumValue)),
                GroupByExpression::nestedResults, selectWhere);
        return Quantifier.forEach(Reference.of(groupByExpression));
    }

    @Nonnull
    private GraphExpansion.Builder selectHavingByGroup(@Nonnull Quantifier groupedSum) {
        final var selectHavingBuilder = GraphExpansion.builder().addQuantifier(groupedSum);
        FieldValue groupVal = FieldValue.ofOrdinalNumber(groupedSum.getFlowedObjectValue(), 0);
        return selectHavingBuilder
                .addResultColumn(Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("num_value_3_indexed")), FieldValue.ofOrdinalNumberAndFuseIfPossible(groupVal, 1)))
                .addResultColumn(Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("aggregate")), FieldValue.ofOrdinalNumber(groupedSum.getFlowedObjectValue(), 1)));
    }

    @DualPlannerTest
    void queryByRepeated() {
        final RecordMetaDataHook hook = addRepeaterIndex();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("repeater").oneOfThem().equalsValue(4))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            if (isUseCascadesPlanner()) {
                assertMatchesExactly(plan, fetchFromPartialRecordPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                coveringIndexPlan()
                                .where(indexPlanOf(indexPlan()
                                        .where(indexName(REPEATED_INDEX_NAME))
                                        .and(scanComparisons(range("[[4],[4]]")))
                                )))
                        )
                );
            } else {
                assertMatchesExactly(plan, unorderedPrimaryKeyDistinctPlan(
                        indexPlan()
                                .where(indexName(REPEATED_INDEX_NAME))
                                .and(scanComparisons(range("[[4],[4]]")))
                        )
                );
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.OLD)
    void querySumOldPlanner() {
        final RecordMetaDataHook hook = addSumByRepeatedIndex();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("repeater").oneOfThem().equalsValue(42))
                    .setRequiredResults(ImmutableList.of(field("num_value_3_indexed")))
                    .build();
            final RecordQueryPlanner queryPlanner = (RecordQueryPlanner)planner;
            final RecordQueryPlan plan = queryPlanner.planCoveringAggregateIndex(query, SUM_BY_REPEATED_INDEX_NAME);
            assertNotNull(plan, "plan should not be null");
            assertMatchesExactly(plan, coveringIndexPlan()
                    .where(indexPlanOf(indexPlan()
                            .where(indexName(SUM_BY_REPEATED_INDEX_NAME))
                            .and(scanComparisons(range("[[42],[42]]")))
                    ))
            );
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void querySumByRepeatedPredicateOnGroup() {
        final RecordMetaDataHook hook = addSumByRepeatedIndex();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final RecordQueryPlan plan = planGraph(() -> {
                final Quantifier base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                final Quantifier selectWhere = selectWhereSumByRepeated(base, repeat -> ImmutableList.of(new ValuePredicate(repeat, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42))));
                final Quantifier groupedSum = sumByGroup(selectWhere);

                final var selectHavingBuilder = selectHavingByGroup(groupedSum);
                final Quantifier selectHaving = Quantifier.forEach(Reference.of(selectHavingBuilder.build().buildSelect()));
                return Reference.of(LogicalSortExpression.unsorted(selectHaving));
            });
            assertMatchesExactly(plan, mapPlan(aggregateIndexPlan()
                    .where(scanComparisons(range("[[42],[42]]")))));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void querySumByRepeatedPredicateOnHaving() {
        final RecordMetaDataHook hook = addSumByRepeatedIndex();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final RecordQueryPlan plan = planGraph(() -> {
                final Quantifier base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                final Quantifier selectWhere = selectWhereSumByRepeated(base);
                final Quantifier groupedSum = sumByGroup(selectWhere);

                final var selectHavingBuilder = selectHavingByGroup(groupedSum);
                selectHavingBuilder.addPredicate(
                        new ValuePredicate(
                                FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedSum.getFlowedObjectValue(), 0), 0),
                                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42L))
                );
                final Quantifier selectHaving = Quantifier.forEach(Reference.of(selectHavingBuilder.build().buildSelect()));
                return Reference.of(LogicalSortExpression.unsorted(selectHaving));
            });
            assertMatchesExactly(plan, mapPlan(aggregateIndexPlan()
                    .where(scanComparisons(range("[[42],[42]]")))));
        }
    }
}
