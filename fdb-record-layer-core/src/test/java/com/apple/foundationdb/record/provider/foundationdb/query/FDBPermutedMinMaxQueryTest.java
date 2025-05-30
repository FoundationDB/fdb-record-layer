/*
 * FDBPermutedMinMaxQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.executeCascades;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullTypeScan;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.projectColumn;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.sortExpression;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for queries that use the {@link IndexTypes#PERMUTED_MIN} and {@link IndexTypes#PERMUTED_MAX} index types.
 * These tests focus more on executing queries with a min- and max-function within it.
 */
@Tag(Tags.RequiresFDB)
class FDBPermutedMinMaxQueryTest extends FDBRecordStoreQueryTestBase {
    @Nonnull
    private static final String MAX_UNIQUE_BY_2_3 = "maxNumValueUniqueBy2And3";
    @Nonnull
    private static final String MAX_UNIQUE_BY_STR_VALUE_2_3 = "maxNumValueUniqueByStrValue2And3";
    @Nonnull
    private static final String MAX_2_BY_STR_VALUE_3 = "maxNumValue2ByStrValueThree";
    @Nonnull
    private static final String MAX_3_BY_STR_VALUE_REPEATER_AND_2 = "maxNumValue3ByStrValueRepeaterAnd2";
    @Nonnull
    private static final GroupingKeyExpression UNIQUE_BY_2_3 = field("num_value_unique")
            .groupBy(concatenateFields("num_value_2", "num_value_3_indexed"));
    @Nonnull
    private static final GroupingKeyExpression UNIQUE_BY_STR_VALUE_2_3 = field("num_value_unique")
            .groupBy(concatenateFields("str_value_indexed", "num_value_2", "num_value_3_indexed"));

    @Nonnull
    private static final GroupingKeyExpression NUM_VALUE_2_BY_STR_VALUE_3 = field("num_value_2")
            .groupBy(concatenateFields("str_value_indexed", "num_value_3_indexed"));

    @Nonnull
    private static final GroupingKeyExpression NUM_VALUE_3_BY_STR_VALUE_REPEATER_AND_2 = field("num_value_3_indexed")
            .groupBy(concat(field("str_value_indexed"), field("repeater", KeyExpression.FanType.FanOut), field("num_value_2")));

    @Nonnull
    private static Index maxUniqueBy2And3() {
        return new Index(MAX_UNIQUE_BY_2_3, UNIQUE_BY_2_3, IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1"));
    }

    @Nonnull
    private static Index maxUniqueByStrValueOrderBy2And3() {
        return new Index(MAX_UNIQUE_BY_STR_VALUE_2_3, UNIQUE_BY_STR_VALUE_2_3, IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "2"));
    }

    @Nonnull
    private static Index max2ByStrValueAnd3() {
        return new Index(MAX_2_BY_STR_VALUE_3, NUM_VALUE_2_BY_STR_VALUE_3, IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1"));
    }

    @Nonnull
    private static Index max3ByStrValueRepeaterAnd2() {
        return new Index(MAX_3_BY_STR_VALUE_REPEATER_AND_2, NUM_VALUE_3_BY_STR_VALUE_REPEATER_AND_2, IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1"));
    }

    @Nonnull
    private static Quantifier explodeRepeated(@Nonnull Quantifier baseQun, @Nonnull String repeatedField) {
        final ExplodeExpression explode = new ExplodeExpression(FieldValue.ofFieldNameAndFuseIfPossible(baseQun.getFlowedObjectValue(), repeatedField));
        final Quantifier explodeQun = Quantifier.forEach(Reference.initialOf(explode));

        return Quantifier.forEach(Reference.initialOf(GraphExpansion.builder()
                .addQuantifier(explodeQun)
                .addResultValue(explodeQun.getFlowedObjectValue())
                .build()
                .buildSelect()
        ));
    }

    @Nonnull
    private static Quantifier selectWhereQun(@Nonnull Quantifier baseQun, @Nullable QueryPredicate predicate) {
        return selectWhereWithMultipleQuns(List.of(baseQun), predicate == null ? Collections.emptyList() : List.of(predicate));
    }

    @Nonnull
    private static Quantifier selectWhereWithMultipleQuns(@Nonnull List<Quantifier> baseQuns, @Nonnull List<QueryPredicate> predicates) {
        final var selectWhereBuilder = GraphExpansion.builder()
                .addAllQuantifiers(baseQuns)
                .addAllPredicates(predicates);
        baseQuns.stream().map(Quantifier::getFlowedObjectValue).forEach(selectWhereBuilder::addResultValue);
        return Quantifier.forEach(Reference.initialOf(selectWhereBuilder.build().buildSelect()));
    }

    @Nonnull
    private static Quantifier maxUniqueByGroupQun(@Nonnull Quantifier selectWhere) {
        return maxByGroup(selectWhere, "num_value_unique", List.of("num_value_2", "num_value_3_indexed"));
    }

    @Nonnull
    private static Quantifier maxByGroup(@Nonnull Quantifier selectWhere, @Nonnull String maxField, @Nonnull List<String> groupingFields) {
        var baseReference = FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 0);
        final FieldValue groupedValue = FieldValue.ofFieldName(baseReference, maxField);
        final FieldValue aggregatedFieldRef = FieldValue.ofFields(selectWhere.getFlowedObjectValue(), baseReference.getFieldPath().withSuffix(groupedValue.getFieldPath()));
        final List<Column<? extends Value>> groupingColumns = groupingFields.stream()
                .map(groupingField -> projectColumn(baseReference, groupingField))
                .collect(Collectors.toList());
        return maxByGroup(selectWhere, aggregatedFieldRef, groupingColumns);
    }

    @Nonnull
    private static Quantifier maxByGroup(@Nonnull Quantifier selectWhere, @Nonnull FieldValue aggregatedFieldValue, @Nonnull List<Column<? extends Value>> groupingColumns) {
        final Value maxUniqueValue = (Value) new NumericAggregationValue.MaxFn().encapsulate(List.of(aggregatedFieldValue));
        final RecordConstructorValue groupingValue = RecordConstructorValue.ofColumns(groupingColumns);
        final GroupByExpression groupByExpression = new GroupByExpression(groupingValue, RecordConstructorValue.ofUnnamed(List.of(maxUniqueValue)),
                GroupByExpression::nestedResults, selectWhere);
        return Quantifier.forEach(Reference.initialOf(groupByExpression));
    }

    @Nonnull
    private static Quantifier selectHaving(@Nonnull Quantifier groupedByQun, @Nullable QueryPredicate predicate, @Nonnull List<String> resultColumns) {
        final var selectHavingBuilder = GraphExpansion.builder().addQuantifier(groupedByQun);
        final var groupingValueReference = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
        final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 1), 0);
        if (predicate != null) {
            selectHavingBuilder.addPredicate(predicate);
        }
        for (String resultColumn : resultColumns) {
            if (resultColumn.equals("m")) {
                selectHavingBuilder.addResultColumn(Column.of(Optional.of(resultColumn), aggregateValueReference));
            } else {
                selectHavingBuilder.addResultColumn(projectColumn(groupingValueReference, resultColumn));
            }
        }
        return Quantifier.forEach(Reference.initialOf(selectHavingBuilder.build().buildSelect()));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @BooleanSource
    void selectMaxOrderByFirstGroup(boolean reverse) throws Exception {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueBy2And3());
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Issue a query equivalent to:
            //   SELECT num_value_2, num_value_3_indexed, max(num_value_unique) as m FROM MySimpleRecord GROUP BY num_value_2, num_value_3_indexed ORDER BY num_value_2
            RecordQueryPlan plan = planGraph(() -> {
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var selectWhere = selectWhereQun(base, null);
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var qun = selectHaving(groupedByQun, null, List.of("num_value_2", "num_value_3_indexed", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 0).rebase(aliasMap)), reverse, qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                            .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.unbounded()))
                    )
            );

            final List<Tuple> tupleResults = executeAndGetTuples(plan, Bindings.EMPTY_BINDINGS, List.of("num_value_2", "num_value_3_indexed", "m"));
            final Map<Integer, List<Tuple>> byNumValue2 = new HashMap<>();
            int lastNumValue2 = reverse ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            for (Tuple tupleResult : tupleResults) {
                int numValue2 = (int) tupleResult.getLong(0);
                assertTrue(reverse ? numValue2 <= lastNumValue2 : numValue2 >= lastNumValue2, "tuple " + tupleResult + " should have num_value_2 that is " + (reverse ? "less" : "greater") + " than or equal to " + numValue2);
                lastNumValue2 = numValue2;

                List<Tuple> grouped = byNumValue2.computeIfAbsent(numValue2, ignore -> new ArrayList<>());
                grouped.add(TupleHelpers.subTuple(tupleResult, 1, 3));
            }
            assertEquals(Set.of(0, 1, 2), byNumValue2.keySet());

            for (Map.Entry<Integer, List<Tuple>> groupedResult : byNumValue2.entrySet()) {
                int numValue2 = groupedResult.getKey();
                final List<Tuple> groupedTuples = groupedResult.getValue();
                final Map<Integer, Integer> expectedMaxes = expectedMaxUniquesByNumValue3(val -> val == numValue2);
                assertThat(groupedTuples, hasSize(expectedMaxes.size()));
                final List<Matcher<? super Tuple>> expectedTuples = expectedTuples(expectedMaxes, reverse);
                assertThat(groupedTuples, contains(expectedTuples));
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void selectMaxByGroupWithFilter() throws Exception {
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueBy2And3());
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Issue a query equivalent to:
            //   SELECT num_value_3_indexed, max(num_value_unique) as m FROM MySimpleRecord WHERE num_value_2 = ?numValue2 GROUP BY num_value_3_indexed
            final String numValue2Param = "numValue2";
            RecordQueryPlan plan = planGraph(() -> {
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var qun = selectHaving(groupedByQun, null, List.of("num_value_3_indexed", "m"));
                return Reference.initialOf(LogicalSortExpression.unsorted(qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                        .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + numValue2Param + "]")))
                    )
            );

            for (int numValue2 = -1; numValue2 <= 4; numValue2++) {
                final List<Tuple> tupleResults = executeAndGetTuples(plan, Bindings.newBuilder().set(numValue2Param, numValue2).build(), List.of("num_value_3_indexed", "m"));

                final int numValue2Value = numValue2;
                final Map<Integer, Integer> expectedMaxes = expectedMaxUniquesByNumValue3(val -> val == numValue2Value);
                assertThat(tupleResults, hasSize(expectedMaxes.size()));
                if (!expectedMaxes.isEmpty()) {
                    final List<Matcher<? super Tuple>> expectedTuples = expectedTuples(expectedMaxes, false);
                    assertThat(tupleResults, contains(expectedTuples));
                }
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @BooleanSource
    void selectMaxByGroupWithOrder(boolean reverse) throws Exception {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueBy2And3());
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Issue a query equivalent to:
            //   SELECT num_value_3_indexed, max(num_value_unique) as m FROM MySimpleRecord WHERE num_value_2 = ?numValue2 GROUP BY num_value_3_indexed ORDER BY max(num_value_unique)
            final String numValue2Param = "numValue2";
            RecordQueryPlan plan = planGraph(() -> {
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var qun = selectHaving(groupedByQun, null, List.of("m", "num_value_3_indexed"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 0).rebase(aliasMap)), reverse, qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                            .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + numValue2Param + "]")))
                    )
            );
            assertEquals(reverse, plan.isReverse());

            for (int numValue2 = -1; numValue2 <= 4; numValue2++) {
                final List<Tuple> tupleResults = executeAndGetTuples(plan, Bindings.newBuilder().set(numValue2Param, numValue2).build(), List.of("num_value_3_indexed", "m"));

                final int numValue2Value = numValue2;
                final Map<Integer, Integer> expectedMaxes = expectedMaxUniquesByNumValue3(val -> val == numValue2Value);
                assertThat(tupleResults, hasSize(expectedMaxes.size()));
                if (!expectedMaxes.isEmpty()) {
                    final List<Matcher<? super Tuple>> expectedTuples = expectedTuples(expectedMaxes, reverse);
                    assertThat(tupleResults, contains(expectedTuples));
                }
            }

            commit(context);
        }
    }

    static class InComparisonCase {
        @Nonnull
        private final String name;
        @Nonnull
        private final Comparisons.Comparison comparison;
        @Nonnull
        private final Function<List<?>, Bindings> bindingsFunction;
        private final int legacyPlanHash;
        private final int continuationPlanHash;

        protected InComparisonCase(@Nonnull String name, @Nonnull Comparisons.Comparison comparison, @Nonnull Function<List<?>, Bindings> bindingsFunction, int legacyPlanHash, int continuationPlanHash) {
            this.name = name;
            this.comparison = comparison;
            this.bindingsFunction = bindingsFunction;
            this.legacyPlanHash = legacyPlanHash;
            this.continuationPlanHash = continuationPlanHash;
        }

        @Nonnull
        Comparisons.Comparison getComparison() {
            return comparison;
        }

        @Nonnull
        Bindings getBindings(@Nonnull List<?> nv2List) {
            return bindingsFunction.apply(nv2List);
        }

        int getLegacyPlanHash() {
            return legacyPlanHash;
        }

        int getContinuationPlanHash() {
            return continuationPlanHash;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Nonnull
    static Stream<InComparisonCase> selectMaxWithInOrderByMax() {
        ConstantObjectValue constant = ConstantObjectValue.of(Quantifier.uniqueID(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false)));
        return Stream.of(
                new InComparisonCase("byParameter", new Comparisons.ParameterComparison(Comparisons.Type.IN, "numValue2List"), nv2List -> Bindings.newBuilder().set("numValue2List", nv2List).build(), 2026350341, -272644765),
                new InComparisonCase("byLiteral", new Comparisons.ListComparison(Comparisons.Type.IN, List.of(-1, -1)), nv2List -> {
                    Assumptions.assumeTrue(nv2List.equals(List.of(-1, -1)));
                    return Bindings.EMPTY_BINDINGS;
                }, -1983342670, 12629520),
                new InComparisonCase("byConstantObjectValue", new Comparisons.ValueComparison(Comparisons.Type.IN, constant), nv2List -> constantBindings(constant, nv2List), -591261801, 1404710389)
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @MethodSource
    void selectMaxWithInOrderByMax(InComparisonCase inComparisonCase) throws Exception {
        Assumptions.assumeTrue(useCascadesPlanner);
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueBy2And3());
        complexQuerySetup(hook);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            planner.setConfiguration(planner.getConfiguration().asBuilder()
                    .setAttemptFailedInJoinAsUnionMaxSize(20)
                    .build());

            RecordQueryPlan plan = planGraph(() -> {
                // Equivalent to something like:
                //   SELECT num_value_2, num_value_3_indexed, max(num_value_unique) as m FROM MySimpleRecord GROUP BY num_value_2, num_value_3_indexed HAVING num_value_2 IN ? ORDER BY m DESC
                // Different IN cases use different values for the IN list comparand ?, including a literal value, a parameter, and a constant object value,
                // but they should all have the same semantics
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                final var selectWhere = selectWhereQun(base, null);
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var groupingValue = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
                final var qun = selectHaving(groupedByQun,
                        FieldValue.ofOrdinalNumberAndFuseIfPossible(groupingValue, 0).withComparison(inComparisonCase.getComparison()),
                        List.of("num_value_2", "num_value_3_indexed", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "m").rebase(aliasMap)), true, qun));
            });

            assertMatchesExactly(plan, RecordQueryPlanMatchers.inUnionOnValuesPlan(
                    RecordQueryPlanMatchers.mapPlan(
                            RecordQueryPlanMatchers.aggregateIndexPlan()
                                    .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.equalities(ListMatcher.exactly(ScanComparisons.anyValueComparison()))))
                                    .and(RecordQueryPlanMatchers.isReverse())
                    )
            ).where(RecordQueryPlanMatchers.comparisonKeyValues(ListMatcher.exactly(
                    ValueMatchers.fieldValueWithFieldNames("m"), ValueMatchers.fieldValueWithFieldNames("num_value_2"), ValueMatchers.fieldValueWithFieldNames("num_value_3_indexed")
            ))));

            assertEquals(inComparisonCase.getContinuationPlanHash(), plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            for (int i = -1; i < 4; i++) {
                final int nv2I = i;
                final Map<Integer, Integer> maxesByI = expectedMaxUniquesByNumValue3(nv2 -> nv2 == nv2I);
                final List<Tuple> expectedTuplesByI = maxesByI.entrySet().stream()
                        .map(entry -> Tuple.from(nv2I, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());
                for (int j = -1; j < 4; j++) {
                    final int nv2J = j;
                    final Map<Integer, Integer> maxesByJ = expectedMaxUniquesByNumValue3(nv2 -> nv2 == nv2J);
                    final List<Tuple> expectedTuplesByJ = maxesByJ.entrySet().stream()
                            .map(entry -> Tuple.from(nv2J, entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList());

                    List<Integer> nv2List = ImmutableList.of(i, j);
                    List<Tuple> queried = executeAndGetTuples(plan, inComparisonCase.getBindings(nv2List), ImmutableList.of("num_value_2", "num_value_3_indexed", "m"));
                    List<Tuple> expected = Stream.concat(expectedTuplesByI.stream(), expectedTuplesByJ.stream())
                            .sorted(Comparator.comparingLong(t -> -1L * t.getLong(2)))
                            .distinct()
                            .collect(Collectors.toList());
                    assertEquals(expected, queried, () -> "entries should match when num_value_2 IN " + nv2List);
                }
            }

            commit(context);
        }
    }

    @Nonnull
    static Stream<InComparisonCase> testMaxWithInAndDupes() {
        ConstantObjectValue constant = ConstantObjectValue.of(Quantifier.uniqueID(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.STRING, false)));
        return Stream.of(
                new InComparisonCase("byParameter", new Comparisons.ParameterComparison(Comparisons.Type.IN, "strValueList"), strValueList -> Bindings.newBuilder().set("strValueList", strValueList).build(), 2106093264, 1809779597),
                new InComparisonCase("byLiteral", new Comparisons.ListComparison(Comparisons.Type.IN, List.of("even", "odd")), strValueList -> {
                    Assumptions.assumeTrue(strValueList.equals(List.of("even", "odd")));
                    return Bindings.EMPTY_BINDINGS;
                }, -1932450623, 2066203006),
                new InComparisonCase("byConstantObjectValue", new Comparisons.ValueComparison(Comparisons.Type.IN, constant), strValueList -> constantBindings(constant, strValueList), 747556219, 451242552)
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @MethodSource
    void testMaxWithInAndDupes(InComparisonCase inComparisonCase) throws Exception {
        Assumptions.assumeTrue(useCascadesPlanner);
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), max2ByStrValueAnd3());
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            planner.setConfiguration(planner.getConfiguration().asBuilder()
                    .setAttemptFailedInJoinAsUnionMaxSize(20)
                    .build());

            RecordQueryPlan plan = planGraph(() -> {
                // Equivalent to something like:
                //   SELECT str_value_indexed, num_value_3_indexed, max(num_value_2) as m FROM MySimpleRecord GROUP BY str_value_indexed, num_value_3_indexed HAVING str_value_indexed IN ? ORDER BY m DESC
                // Different IN cases use different values for the IN list comparand ?, including a literal value, a parameter, and a constant object value,
                // but they should all have the same semantics
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                final var selectWhere = selectWhereQun(base, null);
                final var groupedByQun = maxByGroup(selectWhere, "num_value_2", List.of("str_value_indexed", "num_value_3_indexed"));

                final var groupingValue = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
                final var qun = selectHaving(groupedByQun,
                        FieldValue.ofFieldNameAndFuseIfPossible(groupingValue, "str_value_indexed").withComparison(inComparisonCase.getComparison()),
                        List.of("str_value_indexed", "num_value_3_indexed", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "m").rebase(aliasMap)), true, qun));
            });

            assertMatchesExactly(plan, RecordQueryPlanMatchers.inUnionOnValuesPlan(
                    RecordQueryPlanMatchers.mapPlan(
                            RecordQueryPlanMatchers.aggregateIndexPlan()
                                    .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.equalities(ListMatcher.exactly(ScanComparisons.anyValueComparison()))))
                                    .and(RecordQueryPlanMatchers.isReverse())
                    )
            ).where(RecordQueryPlanMatchers.comparisonKeyValues(ListMatcher.exactly(
                    ValueMatchers.fieldValueWithFieldNames("m"), ValueMatchers.fieldValueWithFieldNames("str_value_indexed"), ValueMatchers.fieldValueWithFieldNames("num_value_3_indexed")
            ))));

            assertEquals(inComparisonCase.getContinuationPlanHash(), plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            final Map<Integer, Integer> evenMaxes = expectedMaxNumValue2ByNumValue3WithStrValue("even");
            final List<Tuple> evenTuples = evenMaxes.entrySet().stream().map(entry -> Tuple.from("even", entry.getKey(), entry.getValue())).collect(Collectors.toList());
            final Map<Integer, Integer> oddMaxes = expectedMaxNumValue2ByNumValue3WithStrValue("odd");
            final List<Tuple> oddTuples = oddMaxes.entrySet().stream().map(entry -> Tuple.from("odd", entry.getKey(), entry.getValue())).collect(Collectors.toList());
            final List<Tuple> queriedAll = executeAndGetTuples(plan, inComparisonCase.getBindings(List.of("even", "odd")), List.of("str_value_indexed", "num_value_3_indexed", "m"));
            final List<Tuple> expectedAll = ImmutableList.<Tuple>builder().addAll(evenTuples).addAll(oddTuples).build();
            assertThat(queriedAll, containsInAnyOrder(expectedAll.toArray()));

            final List<Tuple> queriedEven = executeAndGetTuples(plan, inComparisonCase.getBindings(List.of("even")), List.of("str_value_indexed", "num_value_3_indexed", "m"));
            assertThat(queriedEven, containsInAnyOrder(evenTuples.toArray()));
            final List<Tuple> queriedOdd = executeAndGetTuples(plan, inComparisonCase.getBindings(List.of("odd")), List.of("str_value_indexed", "num_value_3_indexed", "m"));
            assertThat(queriedOdd, containsInAnyOrder(oddTuples.toArray()));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testSortedMaxWithEqualityOnRepeater() {
        Assumptions.assumeTrue(useCascadesPlanner);
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), max3ByStrValueRepeaterAnd2());
        setUpWithRepeaters(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            planner.setConfiguration(planner.getConfiguration().asBuilder()
                    .setAttemptFailedInJoinAsUnionMaxSize(20)
                    .build());

            final String strValueParam = "strValue";
            final String xValueParam = "xValue";
            RecordQueryPlan plan = planGraph(() -> {
                // Equivalent to something like:
                //   SELECT num_value_2, max(num_value_3_indexed) as m
                //      FROM MySimpleRecord, (SELECT x FROM MySimpleRecord.repeater) r
                //      WHERE strValueIndexed = ?strValue
                //      GROUP BY str_value_indexed, r.x, num_value_2
                //      HAVING r.x = ?xValue
                //      ORDER BY m DESC
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                final var repeater = explodeRepeated(base, "repeater");
                final var selectWhere = selectWhereWithMultipleQuns(List.of(base, repeater),
                        List.of(FieldValue.ofFieldName(base.getFlowedObjectValue(), "str_value_indexed").withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, strValueParam))));

                final FieldValue baseInWhere = FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 0);
                final var groupedByQun = maxByGroup(selectWhere, FieldValue.ofFieldNameAndFuseIfPossible(baseInWhere, "num_value_3_indexed"),
                        List.of(
                                Column.of(Optional.of("str_value_indexed"), FieldValue.ofFieldNameAndFuseIfPossible(baseInWhere, "str_value_indexed")),
                                Column.of(Optional.of("x"), FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 1), 0)),
                                Column.of(Optional.of("num_value_2"), FieldValue.ofFieldNameAndFuseIfPossible(baseInWhere, "num_value_2"))
                        )
                );

                final var groupingValue = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
                final var qun = selectHaving(groupedByQun,
                        FieldValue.ofFieldNameAndFuseIfPossible(groupingValue, "x").withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, xValueParam)),
                        List.of("num_value_2", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "m").rebase(aliasMap)), true, qun));
            });

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                            .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + strValueParam + ", EQUALS $" + xValueParam + "]")))
                            .and(RecordQueryPlanMatchers.isReverse())
                    )
            );

            assertEquals(616507734, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));


            for (String strValue : List.of("even", "odd")) {
                IntStream.range(0, 9).forEach(xValue -> {
                    final List<Tuple> queried = executeAndGetTuples(plan,
                            Bindings.newBuilder().set(strValueParam, strValue).set(xValueParam, xValue).build(),
                            List.of("num_value_2", "m"));

                    final Map<Integer, Integer> expectedMap = expectedMax3ByRepeater(strValue, xValue);
                    final List<Tuple> expectedTuples = expectedMap.entrySet().stream()
                            .sorted(Comparator.comparingInt(e -> -1 * e.getValue()))
                            .map(entry -> Tuple.from(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList());
                    assertEquals(expectedTuples, queried);
                });
            }
        }
    }

    @Nonnull
    static Stream<InComparisonCase> testSortedMaxWithInOnRepeater() {
        ConstantObjectValue constant = ConstantObjectValue.of(Quantifier.uniqueID(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false)));
        return Stream.of(
                new InComparisonCase("byParameter", new Comparisons.ParameterComparison(Comparisons.Type.IN, "xValueList"), xValueList -> Bindings.newBuilder().set("xValueList", xValueList).build(), -1502950720, -1056018522),
                new InComparisonCase("byLiteral", new Comparisons.ListComparison(Comparisons.Type.IN, List.of(0, 0)), strValueList -> {
                    Assumptions.assumeTrue(strValueList.equals(List.of(0, 0)));
                    return Bindings.EMPTY_BINDINGS;
                }, 645291999, 1092224197),
                new InComparisonCase("byConstantObjectValue", new Comparisons.ValueComparison(Comparisons.Type.IN, constant), strValueList -> constantBindings(constant, strValueList), 2037371876, -1810663222)
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @MethodSource
    void testSortedMaxWithInOnRepeater(InComparisonCase inComparisonCase) {
        Assumptions.assumeTrue(useCascadesPlanner);
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), max3ByStrValueRepeaterAnd2());
        setUpWithRepeaters(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            planner.setConfiguration(planner.getConfiguration().asBuilder()
                    .setAttemptFailedInJoinAsUnionMaxSize(20)
                    .build());

            final String strValueParam = "strValue";
            RecordQueryPlan plan = planGraph(() -> {
                // Equivalent to something like:
                //   SELECT r.x, num_value_2, max(num_value_3_indexed) as m
                //      FROM MySimpleRecord, (SELECT x FROM MySimpleRecord.repeater) r
                //      WHERE strValueIndexed = ?strValue
                //      GROUP BY str_value_indexed, r.x, num_value_2
                //      HAVING r.x IN ?xValueList
                //      ORDER BY m DESC
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                final var repeater = explodeRepeated(base, "repeater");
                final var selectWhere = selectWhereWithMultipleQuns(List.of(base, repeater),
                        List.of(FieldValue.ofFieldName(base.getFlowedObjectValue(), "str_value_indexed").withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, strValueParam))));

                final FieldValue baseInWhere = FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 0);
                final var groupedByQun = maxByGroup(selectWhere, FieldValue.ofFieldNameAndFuseIfPossible(baseInWhere, "num_value_3_indexed"),
                        List.of(
                                Column.of(Optional.of("str_value_indexed"), FieldValue.ofFieldNameAndFuseIfPossible(baseInWhere, "str_value_indexed")),
                                Column.of(Optional.of("x"), FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 1), 0)),
                                Column.of(Optional.of("num_value_2"), FieldValue.ofFieldNameAndFuseIfPossible(baseInWhere, "num_value_2"))
                        )
                );

                final var groupingValue = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
                final var qun = selectHaving(groupedByQun,
                        FieldValue.ofFieldNameAndFuseIfPossible(groupingValue, "x").withComparison(inComparisonCase.getComparison()),
                        List.of("x", "num_value_2", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "m").rebase(aliasMap)), true, qun));
            });

            assertMatchesExactly(plan, RecordQueryPlanMatchers.inUnionOnValuesPlan(
                    RecordQueryPlanMatchers.mapPlan(
                            RecordQueryPlanMatchers.aggregateIndexPlan()
                                    .where(RecordQueryPlanMatchers.isReverse())
                    )
            ).where(RecordQueryPlanMatchers.comparisonKeyValues(ListMatcher.exactly(
                    ValueMatchers.fieldValueWithFieldNames("m"), ValueMatchers.fieldValueWithFieldNames("x"), ValueMatchers.fieldValueWithFieldNames("num_value_2"))
            )));

            assertEquals(inComparisonCase.getContinuationPlanHash(), plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            for (String strValue : List.of("even", "odd")) {
                IntStream.range(0, 9).forEach(xValue1 -> {
                    final Map<Integer, Integer> expected1 = expectedMax3ByRepeater(strValue, xValue1);
                    final List<Tuple> tuples1 = expected1.entrySet().stream()
                                    .map(entry -> Tuple.from(xValue1, entry.getKey(), entry.getValue()))
                                    .collect(Collectors.toList());

                    IntStream.range(0, 9).forEach(xValue2 -> {
                        final List<Tuple> queried = executeAndGetTuples(plan,
                                inComparisonCase.getBindings(List.of(xValue1, xValue2)).childBuilder().set(strValueParam, strValue).build(),
                                List.of("x", "num_value_2", "m"));

                        Stream<Tuple> baseStream;
                        if (xValue1 == xValue2) {
                            baseStream = tuples1.stream();
                        } else {
                            final Map<Integer, Integer> expected2 = expectedMax3ByRepeater(strValue, xValue2);
                            baseStream = Stream.concat(tuples1.stream(), expected2.entrySet().stream()
                                    .map(entry -> Tuple.from(xValue2, entry.getKey(), entry.getValue()))
                            );
                        }
                        final List<Tuple> evenTuples = baseStream
                                .sorted((t1, t2) -> {
                                    long m1 = t1.getLong(2);
                                    long m2 = t2.getLong(2);
                                    if (m1 != m2) {
                                        return -1 * Long.compare(m1, m2);
                                    }
                                    long x1 = t1.getLong(0);
                                    long x2 = t2.getLong(0);
                                    return -1 * Long.compare(x1, x2);
                                })
                                .collect(Collectors.toList());
                        assertEquals(evenTuples, queried, () -> "value mismatch for strValue = \"" + strValue + "\" and xValue1 = " + xValue1 + " and xValue2 = " + xValue2);
                    });
                });
            }
        }
    }

    @Nonnull
    static Stream<InComparisonCase> testMaxUniqueByStr2And3WithDifferentOrderingKeys() {
        ConstantObjectValue constant = ConstantObjectValue.of(Quantifier.uniqueID(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.STRING, false)));
        List<String> literalStrList = ImmutableList.of("even", "odd", "empty", "other1", "other2");
        return Stream.of(
                new InComparisonCase("byParameter", new Comparisons.ParameterComparison(Comparisons.Type.IN, "strValueList"), strValueList -> Bindings.newBuilder().set("strValueList", strValueList).build(), 755361732, -85959138),
                new InComparisonCase("byLiteral", new Comparisons.ListComparison(Comparisons.Type.IN, literalStrList), strValueList -> {
                    Assumptions.assumeTrue(strValueList.equals(literalStrList));
                    return Bindings.EMPTY_BINDINGS;
                }, 381518259, -459802611),
                new InComparisonCase("byConstantObjectValue", new Comparisons.ValueComparison(Comparisons.Type.IN, constant), strValueList -> constantBindings(constant, strValueList), -603175313, -1444496183)
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "testMaxUniqueByStr2And3WithDifferentOrderingKeys[inComparisonCase={0}]")
    @MethodSource
    void testMaxUniqueByStr2And3WithDifferentOrderingKeys(InComparisonCase inComparisonCase) throws Exception {
        Assumptions.assumeTrue(useCascadesPlanner);
        final RecordMetaDataHook hook = metaData -> {
            metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueByStrValueOrderBy2And3());
            metaData.removeIndex("MySimpleRecord$num_value_unique"); // get rid of unique index as we don't want uniqueness constraint
        };
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Add in a few more records. These will serve to create duplicates with the records included in the
            // complex query setup hook
            final Map<Tuple, Integer> expected = expectedMaxUniquesByStrValueNumValue2NumValue3();
            final AtomicLong recNoCounter = new AtomicLong(100_000L);
            saveOtherMax("other1", 980, 0, 0, recNoCounter, expected);
            saveOtherMax("other2", 980, 1, 0, recNoCounter, expected);
            saveOtherMax("other1", 992, 2, 0, recNoCounter, expected);
            saveOtherMax("other2", 992, 2, 1, recNoCounter, expected);
            saveOtherMax("other1", 972, 1, 3, recNoCounter, expected);
            saveOtherMax("other2", 972, 2, 3, recNoCounter, expected);

            final Object[] expectedArr = expected.entrySet().stream()
                    .map(entry -> {
                        Tuple key = entry.getKey();
                        return ImmutableMap.<String, Object>of(
                                "str_value_indexed", key.getString(0),
                                "num_value_2", (int) key.getLong(1),
                                "num_value_3_indexed", (int) key.getLong(2),
                                "m", entry.getValue());
                    })
                    .toArray();

            planner.setConfiguration(planner.getConfiguration().asBuilder().setAttemptFailedInJoinAsUnionMaxSize(10).build());

            // Issue a query like:
            //  SELECT str_value_indexed, max(num_value_unique) as m, num_value_2, num_value_3_indexed
            //    FROM MySimpleRecord
            //    GROUP BY str_value_indexed, num_value_2_indexed, num_value_3
            //    HAVING str_value_indexed IN $strValueList
            //    ORDER BY max(num_value_unique), num_value_2_indexed, num_value_3
            // We will issue this with different ordering keys

            final List<String> totalOrderingKey = ImmutableList.of("m", "num_value_2", "num_value_3_indexed");
            boolean checkedPlanHash = false;
            for (int i = 0; i <= totalOrderingKey.size(); i++) {
                final List<String> orderingKey = totalOrderingKey.subList(0, i);

                final RecordQueryPlan plan = planGraph(() -> {
                    final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                    final var selectWhere = selectWhereQun(base, null);
                    final var groupedByQun = maxByGroup(selectWhere, "num_value_unique", ImmutableList.of("str_value_indexed", "num_value_2", "num_value_3_indexed"));

                    final var strValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0), 0);
                    final var qun = selectHaving(groupedByQun,
                            strValueReference.withComparison(inComparisonCase.getComparison()),
                            ImmutableList.of("str_value_indexed", "m", "num_value_2", "num_value_3_indexed"));
                    final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                    final List<Value> sortValues = orderingKey.stream()
                            .map(fieldName -> FieldValue.ofFieldName(qun.getFlowedObjectValue(), fieldName).rebase(aliasMap))
                            .collect(Collectors.toList());
                    return Reference.initialOf(sortExpression(sortValues, true, qun));
                });

                var aggregatePlanMatcher = RecordQueryPlanMatchers.aggregateIndexPlan()
                        .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.equalities(ListMatcher.exactly(ScanComparisons.anyValueComparison()))));
                if (orderingKey.isEmpty()) {
                    // If we don't have an ordering constraint, use an IN-join.
                    assertMatchesExactly(plan,
                            RecordQueryPlanMatchers.inJoinPlan(
                                    RecordQueryPlanMatchers.mapPlan(aggregatePlanMatcher)));
                } else {
                    // If we do have an ordering constraint, we need to use an IN-union.
                    // The ordering key should be:
                    //  1. Fields in the requested order
                    //  2. The str_value_indexed field (meaning that we'll consume all values from a leg of the union for a fixed value of the ordering key)
                    //  3. Any remaining elements in the underlying order
                    final List<BindingMatcher<FieldValue>> comparisonKeyFieldMatchers = Stream.concat(
                            orderingKey.stream(),
                            Stream.concat(Stream.of("str_value_indexed"), totalOrderingKey.subList(i, totalOrderingKey.size()).stream())
                    ).map(ValueMatchers::fieldValueWithFieldNames).collect(Collectors.toList());
                    assertMatchesExactly(plan,
                            RecordQueryPlanMatchers.inUnionOnValuesPlan(
                                    RecordQueryPlanMatchers.mapPlan(
                                            aggregatePlanMatcher.and(RecordQueryPlanMatchers.isReverse())))
                                    .where(RecordQueryPlanMatchers.comparisonKeyValues(ListMatcher.exactly(comparisonKeyFieldMatchers)))
                    );
                }

                // When we have a total ordering, compare the plan hashes. We only check this one plan because we only encode
                // one plan hash overall, and we've hard-coded the one with the largest comparison key
                if (orderingKey.size() == totalOrderingKey.size()) {
                    assertEquals(inComparisonCase.getContinuationPlanHash(), plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                    assertEquals(inComparisonCase.getLegacyPlanHash(), plan.planHash(PlanHashable.CURRENT_LEGACY));
                    checkedPlanHash = true;
                }

                // Validate contents
                final List<Map<String, Object>> queriedList = queryAsMaps(plan, inComparisonCase.getBindings(ImmutableList.of("even", "odd", "empty", "other1", "other2")));
                assertThat(queriedList, containsInAnyOrder(expectedArr));

                // Validate ordering
                Tuple lastSeen = null;
                for (Map<String, Object> queried : queriedList) {
                    Tuple comparisonKey = Tuple.fromItems(orderingKey.stream().map(queried::get).collect(Collectors.toList()));
                    if (lastSeen != null) {
                        assertThat(comparisonKey, lessThanOrEqualTo(lastSeen));
                    }
                    lastSeen = comparisonKey;
                }
            }

            // Validate that we actually checked the plan hash. This also double checks that we try the full comparison
            // key case, which is also the case that exposes the bug alluded to by:
            //  https://github.com/FoundationDB/fdb-record-layer/issues/3331
            assertTrue(checkedPlanHash, "should have checked plan hashes during test");
        }
    }

    private void saveOtherMax(@Nonnull String strValue, int numValueUnique, int numValue2, int numValue3, @Nonnull AtomicLong recNoCounter, @Nonnull Map<Tuple, Integer> collector) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNoCounter.getAndIncrement())
                .setStrValueIndexed(strValue)
                .setNumValueUnique(numValueUnique)
                .setNumValue2(numValue2)
                .setNumValue3Indexed(numValue3)
                .build());
        collector.compute(Tuple.from(strValue, numValue2, numValue3),
                (key, oldMax) -> oldMax == null || oldMax < numValueUnique ? numValueUnique : oldMax);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void selectMaxGroupByWithPredicateOnMax() throws Exception {
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueBy2And3());
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Issue a query equivalent to:
            //   SELECT num_value_3_indexed, max(num_value_unique) as m FROM MySimpleRecord WHERE num_value_2 = ?numValue2 GROUP BY num_value_3_indexed HAVING max(num_value_unique) < ?maxValue
            final String numValue2Param = "numValue2";
            final String maxValueParam = "maxValue";
            RecordQueryPlan plan = planGraph(() -> {
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 1), 0);
                final var qun = selectHaving(groupedByQun, aggregateValueReference.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.LESS_THAN, maxValueParam)), List.of("num_value_3_indexed", "m"));
                return Reference.initialOf(LogicalSortExpression.unsorted(qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                            .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + numValue2Param + ", [LESS_THAN $" + maxValueParam + "]]")))
                    )
            );

            for (int numValue2 = -1; numValue2 <= 4; numValue2++) {
                final int numValue2Value = numValue2;
                final Map<Integer, Integer> baseMaxes = expectedMaxUniquesByNumValue3(val -> val == numValue2Value);
                int maxValue = (int) baseMaxes.values().stream().mapToInt(i -> i).average().orElse(0.0);

                final List<Tuple> tupleResults = executeAndGetTuples(plan, Bindings.newBuilder().set(numValue2Param, numValue2).set(maxValueParam, maxValue).build(), List.of("num_value_3_indexed", "m"));
                final Map<Integer, Integer> expectedMaxes = baseMaxes.entrySet().stream()
                        .filter(entry -> entry.getValue() < maxValue)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                assertThat(tupleResults, hasSize(expectedMaxes.size()));
                if (!baseMaxes.isEmpty()) {
                    final List<Matcher<? super Tuple>> expectedTuples = expectedTuples(expectedMaxes, false);
                    assertThat(tupleResults, contains(expectedTuples));
                }
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @BooleanSource
    void selectMaxGroupByWithPredicateAndOrderByOnMax(boolean reverse) throws Exception {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueBy2And3());
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Issue a query equivalent to:
            //   SELECT num_value_3_indexed, max(num_value_unique) as m FROM MySimpleRecord WHERE num_value_2 = ?numValue2 GROUP BY num_value_3_indexed HAVING max(num_value_unique) < ?maxValue ORDER BY max(num_value_unique)
            final String numValue2Param = "numValue2";
            final String maxValueParam = "maxValue";
            RecordQueryPlan plan = planGraph(() -> {
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 1), 0);
                final var qun = selectHaving(groupedByQun, aggregateValueReference.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.GREATER_THAN, maxValueParam)), List.of("num_value_3_indexed", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 1).rebase(aliasMap)), reverse, qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                            .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + numValue2Param + ", [GREATER_THAN $" + maxValueParam + "]]")))
                    )
            );

            for (int numValue2 = -1; numValue2 <= 4; numValue2++) {
                final int numValue2Value = numValue2;
                final Map<Integer, Integer> baseMaxes = expectedMaxUniquesByNumValue3(val -> val == numValue2Value);
                int maxValue = (int) baseMaxes.values().stream().mapToInt(i -> i).average().orElse(0.0);

                final List<Tuple> tupleResults = executeAndGetTuples(plan, Bindings.newBuilder().set(numValue2Param, numValue2).set(maxValueParam, maxValue).build(), List.of("num_value_3_indexed", "m"));
                final Map<Integer, Integer> expectedMaxes = baseMaxes.entrySet().stream()
                        .filter(entry -> entry.getValue() > maxValue)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                assertThat(tupleResults, hasSize(expectedMaxes.size()));
                if (!baseMaxes.isEmpty()) {
                    final List<Matcher<? super Tuple>> expectedTuples = expectedTuples(expectedMaxes, reverse);
                    assertThat(tupleResults, contains(expectedTuples));
                }
            }

            commit(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @BooleanSource
    void maxUniqueFilterOnEntries(boolean reverse) throws Exception {
        Assumptions.assumeTrue(useCascadesPlanner);
        final RecordMetaDataHook hook = metaData -> metaData.addIndex(metaData.getRecordType("MySimpleRecord"), maxUniqueByStrValueOrderBy2And3());
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Issue a query equivalent to:
            //   SELECT num_value_3_indexed, max(num_value_unique) as m FROM MySimpleRecord WHERE str_value_indexed = ?strValue AND num_value_2 = ?numValue2 GROUP BY num_value_3_indexed ORDER BY max(num_value_unique)
            final String strValueParam = "strValue";
            final String numValue2Param = "numValue2";
            RecordQueryPlan plan = planGraph(() -> {
                final var base = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var selectWhereQun = Quantifier.forEach(Reference.initialOf(GraphExpansion.builder()
                                .addQuantifier(base)
                                .addResultColumn(Column.unnamedOf(base.getFlowedObjectValue()))
                        .build()
                        .buildSelect()
                ));
                var baseReference = FieldValue.ofOrdinalNumber(selectWhereQun.getFlowedObjectValue(), 0);
                final FieldValue groupedValue = FieldValue.ofFieldName(baseReference, "num_value_unique");
                var aggregatedFieldRef = FieldValue.ofFields(selectWhereQun.getFlowedObjectValue(), baseReference.getFieldPath().withSuffix(groupedValue.getFieldPath()));
                final Value maxUniqueValue = (Value) new NumericAggregationValue.MaxFn().encapsulate(List.of(aggregatedFieldRef));
                final var strValue = FieldValue.ofFieldNameAndFuseIfPossible(baseReference, "str_value_indexed");
                final var num2Value = FieldValue.ofFieldNameAndFuseIfPossible(baseReference, "num_value_2");
                final var num3ValueIndexed = FieldValue.ofFieldNameAndFuseIfPossible(baseReference, "num_value_3_indexed");
                final var groupingValue = RecordConstructorValue.ofColumns(List.of(
                        Column.of(Optional.of("str_value_indexed"), strValue),
                        Column.of(Optional.of("num_value_2"), num2Value),
                        Column.of(Optional.of("num_value_3_indexed"), num3ValueIndexed)));
                final GroupByExpression groupByExpression = new GroupByExpression(groupingValue, RecordConstructorValue.ofUnnamed(List.of(maxUniqueValue)),
                        GroupByExpression::nestedResults, selectWhereQun);
                final var groupedByQun  = Quantifier.forEach(Reference.initialOf(groupByExpression));

                final var groupingValueRef = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
                final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 1), 0);
                final var selectHavingQun = Quantifier.forEach(Reference.initialOf(GraphExpansion.builder()
                        .addQuantifier(groupedByQun)
                        .addPredicate(FieldValue.ofFieldNameAndFuseIfPossible(groupingValueRef, "str_value_indexed").withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, strValueParam)))
                        .addPredicate(FieldValue.ofFieldNameAndFuseIfPossible(groupingValueRef, "num_value_2").withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)))
                        .addResultColumn(Column.of(Optional.of("num_value_3_indexed"), FieldValue.ofFieldNameAndFuseIfPossible(groupingValueRef, "num_value_3_indexed")))
                        .addResultColumn(Column.of(Optional.of("m"), aggregateValueReference))
                        .build()
                        .buildSelect()));
                final AliasMap aliasMap = AliasMap.ofAliases(selectHavingQun.getAlias(), Quantifier.current());
                return Reference.initialOf(sortExpression(List.of(FieldValue.ofOrdinalNumber(selectHavingQun.getFlowedObjectValue(), 1).rebase(aliasMap)), reverse, selectHavingQun));
            });

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.predicatesFilterPlan(
                            RecordQueryPlanMatchers.aggregateIndexPlan()
                                    .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + strValueParam + "]")))
                                    .and(reverse ? RecordQueryPlanMatchers.isReverse() : RecordQueryPlanMatchers.isNotReverse()))
                    )
            );

            assertEquals(reverse ? 165935152 : 171476278, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            for (int i = -1; i < 4; i++) {
                final int nv2 = i;
                for (String s : List.of("even", "odd", "neither")) {
                    List<Tuple> queried = executeAndGetTuples(plan, Bindings.newBuilder().set(numValue2Param, nv2).set(strValueParam, s).build(), List.of("num_value_3_indexed", "m"));
                    Map<Integer, Integer> expectedMaxes = expectedMaxUniquesByNumValue3(x -> x == nv2, s::equals);
                    List<Tuple> expectedTuples = expectedMaxes.entrySet().stream()
                            .map(entry -> Tuple.from(entry.getKey(), entry.getValue()))
                            .sorted(Comparator.comparingLong(t -> (reverse ? -1 * t.getLong(1) : t.getLong(1))))
                            .collect(Collectors.toList());
                    assertEquals(expectedTuples, queried);
                }
            }
        }
    }

    @Nonnull
    private List<Tuple> executeAndGetTuples(@Nonnull RecordQueryPlan plan, @Nonnull Bindings bindings, @Nonnull List<String> fieldNames)  {
        try (RecordCursor<QueryResult> cursor = executeCascades(recordStore, plan, bindings)) {
            return cursor
                    .map(rec -> {
                        final Message msg = rec.getMessage();
                        final Descriptors.Descriptor desc = msg.getDescriptorForType();
                        List<Object> values = new ArrayList<>(fieldNames.size());
                        for (String fieldName : fieldNames) {
                            final Descriptors.FieldDescriptor fieldDescriptor = desc.findFieldByName(fieldName);
                            values.add(msg.getField(fieldDescriptor));
                        }
                        return Tuple.fromItems(values);
                    })
                    .asList()
                    .join();
        }
    }

    private void setUpWithRepeaters(@Nullable RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                var simpleBuilder = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i + 100L)
                        .setNumValue2(i % 3)
                        .setStrValueIndexed((i % 2) == 0 ? "even" : "odd")
                        .setNumValueUnique(1000 - i)
                        .setNumValue3Indexed(i / 2);

                // Set a value for each bit set in the overall counter
                BitSet setBits = BitSet.valueOf(new long[]{(long)i});
                setBits.stream().forEach(simpleBuilder::addRepeater);

                recordStore.saveRecord(simpleBuilder.build());
            }

            commit(context);
        }
    }

    @Nonnull
    private static Map<Tuple, Integer> expectedMaxUniquesByStrValueNumValue2NumValue3() {
        Map<Tuple, Integer> expected = new HashMap<>();
        for (String strValue : List.of("even", "odd")) {
            for (int numValue2 = 0; numValue2 < 3; numValue2++) {
                final int nv2 = numValue2;
                Map<Integer, Integer> maxBy3 = expectedMaxUniquesByNumValue3(x -> x == nv2, strValue::equals);
                maxBy3.forEach((nv3, maxUnique) -> expected.put(Tuple.from(strValue, nv2, nv3), maxUnique));
            }
        }
        return expected;
    }

    @Nonnull
    private static Map<Integer, Integer> expectedMaxUniquesByNumValue3(Predicate<Integer> numValue2Filter) {
        return expectedMaxUniquesByNumValue3(numValue2Filter, Predicates.alwaysTrue());
    }

    @Nonnull
    private static Map<Integer, Integer> expectedMaxUniquesByNumValue3(Predicate<Integer> numValue2Filter, Predicate<String> strValueParam) {
        final Map<Integer, Integer> expectedMaxes = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            int numValue2 = i % 3;
            if (!numValue2Filter.test(numValue2)) {
                continue;
            }
            final String strValue = ((i & 1) == 0) ? "even" : "odd";
            if (!strValueParam.test(strValue)) {
                continue;
            }
            int numValue3 = i % 5;
            int numValueUnique = 1000 - i;
            expectedMaxes.compute(numValue3, (k, existing) -> existing == null ? numValueUnique : Math.max(numValueUnique, existing));
        }
        return expectedMaxes;
    }

    @Nonnull
    private static Map<Integer, Integer> expectedMaxNumValue2ByNumValue3WithStrValue(String strValueParam) {
        final Map<Integer, Integer> expectedMaxes = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            final String strValue = ((i & 1) == 0) ? "even" : "odd";
            if (!strValueParam.equals(strValue)) {
                continue;
            }
            int numValue2 = i % 3;
            int numValue3 = i % 5;
            expectedMaxes.compute(numValue3, (k, existing) -> existing == null ? numValue2 : Math.max(numValue2, existing));
        }
        return expectedMaxes;
    }

    private static Map<Integer, Integer> expectedMax3ByRepeater(@Nonnull String strValue, int repeater) {
        int repeaterMask = 1 << repeater;

        final Map<Integer, Integer> expectedMaxes = new HashMap<>();
        boolean even = "even".equals(strValue);
        for (int i = 0; i < 100; i++) {
            if ((i % 2 == 0) != even) {
                continue;
            }
            if ((i & repeaterMask) == 0) {
                continue;
            }
            int numValue2 = i % 3;
            int numValue3 = i / 2;
            expectedMaxes.compute(numValue2, (k, existing) -> existing == null ? numValue3 : Math.max(numValue3, existing));
        }
        return expectedMaxes;
    }

    @Nonnull
    private static List<Matcher<? super Tuple>> expectedTuples(@Nonnull Map<Integer, Integer> expectedMaxes, boolean reverse) {
        return expectedMaxes.entrySet().stream()
                .map(entry -> Tuple.from(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparingLong(t -> (reverse ? -1L : 1L) * t.getLong(1)))
                .map(Matchers::equalTo)
                .collect(Collectors.toList());
    }
}
