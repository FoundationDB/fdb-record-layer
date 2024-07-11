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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
    private static final GroupingKeyExpression UNIQUE_BY_2_3 = field("num_value_unique")
            .groupBy(concatenateFields("num_value_2", "num_value_3_indexed"));
    @Nonnull
    private static final GroupingKeyExpression UNIQUE_BY_STR_VALUE_2_3 = field("num_value_unique")
            .groupBy(concatenateFields("str_value_indexed", "num_value_2", "num_value_3_indexed"));

    private static Index maxUniqueBy2And3() {
        return new Index(MAX_UNIQUE_BY_2_3, UNIQUE_BY_2_3, IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1"));
    }

    @Nonnull
    private static Index maxUniqueByStrValueOrderBy2And3() {
        return new Index(MAX_UNIQUE_BY_STR_VALUE_2_3, UNIQUE_BY_STR_VALUE_2_3, IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "2"));
    }

    @Nonnull
    private static Quantifier selectWhereQun(@Nonnull Quantifier baseQun, @Nullable QueryPredicate predicate) {
        final var selectWhereBuilder = GraphExpansion.builder();
        selectWhereBuilder.addQuantifier(baseQun);
        if (predicate != null) {
            selectWhereBuilder.addPredicate(predicate);
        }
        selectWhereBuilder.addResultValue(baseQun.getFlowedObjectValue());
        return Quantifier.forEach(Reference.of(selectWhereBuilder.build().buildSelect()));
    }

    @Nonnull
    private static Quantifier maxUniqueByGroupQun(@Nonnull Quantifier selectWhere) {
        var baseReference = FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 0);
        final FieldValue groupedValue = FieldValue.ofFieldName(baseReference, "num_value_unique");
        var aggregatedFieldRef = FieldValue.ofFields(selectWhere.getFlowedObjectValue(), baseReference.getFieldPath().withSuffix(groupedValue.getFieldPath()));
        final Value maxUniqueValue = (Value) new NumericAggregationValue.MaxFn().encapsulate(List.of(aggregatedFieldRef));
        final var num2Value = FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 0), "num_value_2");
        final var num3Value = FieldValue.ofFieldNameAndFuseIfPossible(FieldValue.ofOrdinalNumber(selectWhere.getFlowedObjectValue(), 0), "num_value_3_indexed");
        final List<Column<? extends Value>> groupingColumns = List.of(
                Column.of(Optional.of("num_value_2"), num2Value),
                Column.of(Optional.of("num_value_3_indexed"), num3Value)
        );
        final RecordConstructorValue groupingValue = RecordConstructorValue.ofColumns(groupingColumns);
        final GroupByExpression groupByExpression = new GroupByExpression(groupingValue, RecordConstructorValue.ofUnnamed(List.of(maxUniqueValue)),
                GroupByExpression::nestedResults, selectWhere);
        return Quantifier.forEach(Reference.of(groupByExpression));
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
            Value value;
            switch (resultColumn) {
                case "m":
                    value = aggregateValueReference;
                    break;
                case "num_value_2":
                    value = FieldValue.ofOrdinalNumber(groupingValueReference, 0);
                    break;
                case "num_value_3_indexed":
                    value = FieldValue.ofOrdinalNumber(groupingValueReference, 1);
                    break;
                default:
                    value = fail("Unknown result column name " + resultColumn);
            }
            selectHavingBuilder.addResultColumn(FDBSimpleQueryGraphTest.resultColumn(value, resultColumn));
        }
        return Quantifier.forEach(Reference.of(selectHavingBuilder.build().buildSelect()));
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
                final var base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var selectWhere = selectWhereQun(base, null);
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var qun = selectHaving(groupedByQun, null, List.of("num_value_2", "num_value_3_indexed", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.of(new LogicalSortExpression(List.of(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 0).rebase(aliasMap)), reverse, qun));
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
                final Map<Integer, Integer> expectedMaxes = expectedMaxesByNumValue3(val -> val == numValue2);
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
                final var base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var qun = selectHaving(groupedByQun, null, List.of("num_value_3_indexed", "m"));
                return Reference.of(LogicalSortExpression.unsorted(qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                        .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + numValue2Param + "]")))
                    )
            );

            for (int numValue2 = -1; numValue2 <= 4; numValue2++) {
                final List<Tuple> tupleResults = executeAndGetTuples(plan, Bindings.newBuilder().set(numValue2Param, numValue2).build(), List.of("num_value_3_indexed", "m"));

                final int numValue2Value = numValue2;
                final Map<Integer, Integer> expectedMaxes = expectedMaxesByNumValue3(val -> val == numValue2Value);
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
                final var base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var qun = selectHaving(groupedByQun, null, List.of("m", "num_value_3_indexed"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.of(new LogicalSortExpression(List.of(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 0).rebase(aliasMap)), reverse, qun));
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
                final Map<Integer, Integer> expectedMaxes = expectedMaxesByNumValue3(val -> val == numValue2Value);
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
        private final Function<List<Integer>, Bindings> bindingsFunction;
        private final int legacyPlanHash;
        private final int continuationPlanHash;

        protected InComparisonCase(@Nonnull String name, @Nonnull Comparisons.Comparison comparison, @Nonnull Function<List<Integer>, Bindings> bindingsFunction, @Nonnull int legacyPlanHash, int continuationPlanHash) {
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
        Bindings getBindings(@Nonnull List<Integer> nv2List) {
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
                new InComparisonCase("byParameter", new Comparisons.ParameterComparison(Comparisons.Type.IN, "numValue2List"), nv2List -> Bindings.newBuilder().set("numValue2List", nv2List).build(), 1892469314, 236900508),
                new InComparisonCase("byLiteral", new Comparisons.ListComparison(Comparisons.Type.IN, List.of(-1, -1)), nv2List -> {
                    Assumptions.assumeTrue(nv2List.equals(List.of(-1, -1)));
                    return Bindings.EMPTY_BINDINGS;
                }, -2117223697, 522174793),
                new InComparisonCase("byConstantObjectValue", new Comparisons.ValueComparison(Comparisons.Type.IN, constant), nv2List -> constantBindings(constant, nv2List), -725142828, 1914255662)
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
                final var base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                final var selectWhere = selectWhereQun(base, null);
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var groupingValue = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
                final var qun = selectHaving(groupedByQun,
                        FieldValue.ofOrdinalNumberAndFuseIfPossible(groupingValue, 0).withComparison(inComparisonCase.getComparison()),
                        List.of("num_value_2", "num_value_3_indexed", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.of(new LogicalSortExpression(List.of(FieldValue.ofFieldName(qun.getFlowedObjectValue(), "m").rebase(aliasMap)), true, qun));
            });

            assertMatchesExactly(plan, RecordQueryPlanMatchers.inUnionOnValuesPlan(
                    RecordQueryPlanMatchers.mapPlan(
                            RecordQueryPlanMatchers.aggregateIndexPlan()
                                    .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.equalities(ListMatcher.exactly(ScanComparisons.anyValueComparison()))))
                                    .and(RecordQueryPlanMatchers.isReverse())
                    )
            ));
            assertEquals(inComparisonCase.getLegacyPlanHash(), plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(inComparisonCase.getContinuationPlanHash(), plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            for (int i = -1; i < 4; i++) {
                final int nv2I = i;
                final Map<Integer, Integer> maxesByI = expectedMaxesByNumValue3(nv2 -> nv2 == nv2I);
                final List<Tuple> expectedTuplesByI = maxesByI.entrySet().stream()
                        .map(entry -> Tuple.from(nv2I, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());
                for (int j = -1; j < 4; j++) {
                    final int nv2J = j;
                    final Map<Integer, Integer> maxesByJ = expectedMaxesByNumValue3(nv2 -> nv2 == nv2J);
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
                final var base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 1), 0);
                final var qun = selectHaving(groupedByQun, aggregateValueReference.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.LESS_THAN, maxValueParam)), List.of("num_value_3_indexed", "m"));
                return Reference.of(LogicalSortExpression.unsorted(qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                            .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + numValue2Param + ", [LESS_THAN $" + maxValueParam + "]]")))
                    )
            );

            for (int numValue2 = -1; numValue2 <= 4; numValue2++) {
                final int numValue2Value = numValue2;
                final Map<Integer, Integer> baseMaxes = expectedMaxesByNumValue3(val -> val == numValue2Value);
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
                final var base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var num2Value = FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2");
                final var selectWhere = selectWhereQun(base, num2Value.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)));
                final var groupedByQun = maxUniqueByGroupQun(selectWhere);

                final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 1), 0);
                final var qun = selectHaving(groupedByQun, aggregateValueReference.withComparison(new Comparisons.ParameterComparison(Comparisons.Type.GREATER_THAN, maxValueParam)), List.of("num_value_3_indexed", "m"));
                final AliasMap aliasMap = AliasMap.ofAliases(qun.getAlias(), Quantifier.current());
                return Reference.of(new LogicalSortExpression(List.of(FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), 1).rebase(aliasMap)), reverse, qun));
            }, MAX_UNIQUE_BY_2_3);

            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.aggregateIndexPlan()
                            .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + numValue2Param + ", [GREATER_THAN $" + maxValueParam + "]]")))
                    )
            );

            for (int numValue2 = -1; numValue2 <= 4; numValue2++) {
                final int numValue2Value = numValue2;
                final Map<Integer, Integer> baseMaxes = expectedMaxesByNumValue3(val -> val == numValue2Value);
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
                final var base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var selectWhereQun = Quantifier.forEach(Reference.of(GraphExpansion.builder()
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
                final var groupedByQun  = Quantifier.forEach(Reference.of(groupByExpression));

                final var groupingValueRef = FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 0);
                final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupedByQun.getFlowedObjectValue(), 1), 0);
                final var selectHavingQun = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                        .addQuantifier(groupedByQun)
                        .addPredicate(FieldValue.ofFieldNameAndFuseIfPossible(groupingValueRef, "str_value_indexed").withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, strValueParam)))
                        .addPredicate(FieldValue.ofFieldNameAndFuseIfPossible(groupingValueRef, "num_value_2").withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, numValue2Param)))
                        .addResultColumn(Column.of(Optional.of("num_value_3_indexed"), FieldValue.ofFieldNameAndFuseIfPossible(groupingValueRef, "num_value_3_indexed")))
                        .addResultColumn(Column.of(Optional.of("m"), aggregateValueReference))
                        .build()
                        .buildSelect()));
                final AliasMap aliasMap = AliasMap.ofAliases(selectHavingQun.getAlias(), Quantifier.current());
                return Reference.of(new LogicalSortExpression(List.of(FieldValue.ofOrdinalNumber(selectHavingQun.getFlowedObjectValue(), 1).rebase(aliasMap)), reverse, selectHavingQun));
            });


            assertMatchesExactly(plan, RecordQueryPlanMatchers.mapPlan(
                    RecordQueryPlanMatchers.predicatesFilterPlan(
                            RecordQueryPlanMatchers.aggregateIndexPlan()
                                    .where(RecordQueryPlanMatchers.scanComparisons(ScanComparisons.range("[EQUALS $" + strValueParam + "]")))
                                    .and(reverse ? RecordQueryPlanMatchers.isReverse() : RecordQueryPlanMatchers.isNotReverse()))
                    )
            );
            assertEquals(reverse ? -1876387967 : -1876388928, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 1127834451 : 1133375577, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));


            for (int i = -1; i < 4; i++) {
                final int nv2 = i;
                for (String s : List.of("even", "odd", "neither")) {
                    List<Tuple> queried = executeAndGetTuples(plan, Bindings.newBuilder().set(numValue2Param, nv2).set(strValueParam, s).build(), List.of("num_value_3_indexed", "m"));
                    Map<Integer, Integer> expectedMaxes = expectedMaxesByNumValue3(x -> x == nv2, s::equals);
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
        try (RecordCursor<QueryResult> cursor = FDBSimpleQueryGraphTest.executeCascades(recordStore, plan, bindings)) {
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

    @Nonnull
    private static Map<Integer, Integer> expectedMaxesByNumValue3(Predicate<Integer> numValue2Filter) {
        return expectedMaxesByNumValue3(numValue2Filter, Predicates.alwaysTrue());
    }

    @Nonnull
    private static Map<Integer, Integer> expectedMaxesByNumValue3(Predicate<Integer> numValue2Filter, Predicate<String> strValueParam) {
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
    private static List<Matcher<? super Tuple>> expectedTuples(@Nonnull Map<Integer, Integer> expectedMaxes, boolean reverse) {
        return expectedMaxes.entrySet().stream()
                .map(entry -> Tuple.from(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparingLong(t -> (reverse ? -1L : 1L) * t.getLong(1)))
                .map(Matchers::equalTo)
                .collect(Collectors.toList());
    }
}
