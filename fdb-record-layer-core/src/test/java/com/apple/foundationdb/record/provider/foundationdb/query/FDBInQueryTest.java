/*
 * FDBInQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.InOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.PlanHashable.CURRENT_FOR_CONTINUATION;
import static com.apple.foundationdb.record.PlanHashable.CURRENT_LEGACY;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.anyParameterComparison;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.anyValueComparison;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.equalities;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.containsAll;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.notPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.comparisonKey;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.comparisonKeyValues;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.equalsInList;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inComparandJoinPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inJoinPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inParameterJoinPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionBindingName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionComparisonKey;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionComparisonValues;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionInParameter;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionInValues;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionOnExpressionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionOnValuesPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inUnionValuesSources;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inValuesJoinPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.inValuesList;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.isNotReverse;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.isReverse;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.selfOrDescendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unionOnExpressionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedUnionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValueWithFieldNames;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests related to planning queries with an IN clause.
 */
@Tag(Tags.RequiresFDB)
class FDBInQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that an IN without an index is implemented as a filter on a scan, as opposed to a loop of a filter on a scan.
     */
    @DualPlannerTest
    void testInQueryNoIndex() throws Exception {
        complexQuerySetup(NO_HOOK);
        final QueryComponent filter = Query.field("num_value_2").in(asList(0, 2));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .build();

        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN [0, 2]
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(queryComponents(exactly(equalsObject(filter)))));

            assertEquals(-1139367278, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1691932867, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(predicates(valuePredicate(fieldValueWithFieldNames("num_value_2"), new Comparisons.ListComparison(Comparisons.Type.IN, ImmutableList.of(0, 2))))));

            assertEquals(738091077, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-2062922437, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(67, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValue2(), anyOf(is(0), is(2))),
                context -> assertDiscardedAtMost(33, context)));
    }

    /**
     * Verify that an IN (with parameter) without an index is implemented as a filter on a scan.
     */
    @DualPlannerTest
    void testInQueryNoIndexWithParameter() throws Exception {
        complexQuerySetup(NO_HOOK);
        final QueryComponent filter = Query.field("num_value_2").in("valuesThree");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)    // num_value_2 is i%3
                .build();

        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN $valuesThree
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(queryComponents(exactly(equalsObject(filter)))));

            assertEquals(-1677754212, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-888563796, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(predicates(valuePredicate(fieldValueWithFieldNames("num_value_2"), new Comparisons.ParameterComparison(Comparisons.Type.IN, "valuesThree")))));

            assertEquals(199704143, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-348451804, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(33, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valuesThree", asList(1, 3)),
                rec -> assertThat(rec.getNumValue2(), anyOf(is(1), is(3))),
                context -> assertDiscardedAtMost(67, context)));
    }

    /**
     * Verify that an IN with an index is implemented as an index scan, with an IN join.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testInQueryIndex[reverse={0}]")
    @BooleanSource
    void testInQueryIndex(boolean reverse) throws Exception {
        complexQuerySetup(NO_HOOK);
        List<Integer> ls = asList(1, 2, 4);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in(ls))
                .setSort(field("num_value_3_indexed"), reverse)
                .build();
        final List<Integer> comparisonLs = reverse ? Lists.reverse(ls) : ls;

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .build());

        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN [1, 2, 4]
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                                    .and(reverse ? isReverse() : isNotReverse())
                    ).where(inValuesList(equalsObject(comparisonLs))));
            assertEquals(reverse ? -2004057427 : -2004060309, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 399454035 : 571226247, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                    inValuesJoinPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan()
                                            .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                            .and(scanComparisons(equalities(only(anyValueComparison())))))
                                            .and(isNotReverse())
                                    ))
                            .where(inValuesList(equalsObject(comparisonLs)))));
            assertEquals(reverse ? 721946094 : 702573636, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 521033883 : 518266389, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(60, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                rec -> Tuple.from(rec.getNumValue3Indexed()),
                reverse,
                TestHelpers::assertDiscardedNone));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testInQueryWithConstantValueUnsorted() throws Exception {
        complexQuerySetup(NO_HOOK);
        final ConstantObjectValue constant = ConstantObjectValue.of(CorrelationIdentifier.uniqueID(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false)));
        final RecordQueryPlan plan = planGraph(() -> {
            final Quantifier base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
            var select = GraphExpansion.builder()
                    .addQuantifier(base)
                    .addPredicate(FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_3_indexed")
                            .withComparison(new Comparisons.ValueComparison(Comparisons.Type.IN, constant)))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base.getFlowedObjectValue(), "num_value_3_indexed"))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base.getFlowedObjectValue(), "rec_no"))
                    .build()
                    .buildSelect();
            Quantifier selectQun = Quantifier.forEach(Reference.of(select));
            return Reference.of(LogicalSortExpression.unsorted(selectQun));
        });
        assertMatchesExactly(plan, inJoinPlan(
                mapPlan(
                        coveringIndexPlan()
                                .where(indexPlanOf(
                                        indexPlan()
                                                .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                                .and(scanComparisons(equalities(exactly(anyValueComparison()))))
                                ))
                )
        ));
        assertEquals(1428357657, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-1222046388, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final List<Integer> ints = List.of(1, 2, 4);
            final List<Map<String, Object>> results = queryAsMaps(plan, constantBindings(constant, ints));
            assertThat(results, hasSize(60));
            results.forEach(res -> assertThat(res.get("num_value_3_indexed"), in(ints.toArray())));

            final List<Integer> ints2 = List.of(3, 5);
            final List<Map<String, Object>> results2 = queryAsMaps(plan, constantBindings(constant, ints2));
            assertThat(results2, hasSize(20));
            results2.forEach(res -> assertThat(res.get("num_value_3_indexed"), in(ints2.toArray())));

            final List<Integer> ints3 = List.of(1, 3, 3);
            final List<Map<String, Object>> results3 = queryAsMaps(plan, constantBindings(constant, ints3));
            assertThat(results3, hasSize(60)); // todo: de-dupe
            results3.forEach(res -> assertThat(res.get("num_value_3_indexed"), in(ints3.toArray())));

            TestHelpers.assertDiscardedNone(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testTupleInList() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            final Index compoundIndex = new Index("compoundIndex", Key.Expressions.concat(Key.Expressions.field("str_value_indexed"), Key.Expressions.field("num_value_3_indexed")));
            metaData.addIndex("MySimpleRecord", compoundIndex);
        };

        complexQuerySetup(hook);
        final var plan = planGraph(
                () -> {
                    var qun = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var strValueIndexed =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "str_value_indexed");
                    final var numValue3Indexed =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_3_indexed");

                    final var inArrayValue = RecordConstructorValue.ofUnnamed(List.of(strValueIndexed, numValue3Indexed));
                    Column<LiteralValue<?>> str1 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "foo"), "str_value_indexed");
                    Column<LiteralValue<?>> str2 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "bar"), "str_value_indexed");
                    Column<LiteralValue<?>> n1 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1), "num_value_3_indexed");
                    Column<LiteralValue<?>> n2 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2), "num_value_3_indexed");

                    final var comparandValue = AbstractArrayConstructorValue.LightArrayConstructorValue.of(RecordConstructorValue.ofColumns(List.of(str1, n1)), RecordConstructorValue.ofColumns(List.of(str2, n2)));

                    final var encapsulatedIn = (BooleanValue)new InOpValue.InFn().encapsulate(ImmutableList.of(inArrayValue, comparandValue));
                    graphExpansionBuilder.addPredicate(encapsulatedIn.toQueryPredicate(null, Quantifier.current()).orElseThrow());

                    graphExpansionBuilder.addResultColumn(Column.unnamedOf(strValueIndexed));
                    graphExpansionBuilder.addResultColumn(Column.unnamedOf(numValue3Indexed));
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                });

        assertMatchesExactly(plan,
                inComparandJoinPlan(
                        mapPlan(
                                coveringIndexPlan()
                                        .where(indexPlanOf(
                                                indexPlan()
                                                        .where(indexName("compoundIndex"))
                                                        .and(isNotReverse())
                                        ))
                        )));
        assertEquals(-379608724, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testTupleInListNoIndex() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
        };

        complexQuerySetup(hook);
        final var plan = planGraph(
                () -> {
                    var qun = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var strValueIndexed =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "str_value_indexed");
                    final var numValue3Indexed =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_3_indexed");

                    final var inArrayValue = RecordConstructorValue.ofUnnamed(List.of(strValueIndexed, numValue3Indexed));
                    Column<LiteralValue<String>> str1 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "odd"), "str_value_indexed");
                    Column<LiteralValue<String>> str2 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "even"), "str_value_indexed");
                    Column<LiteralValue<Integer>> n1 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1), "num_value_3_indexed");
                    Column<LiteralValue<Integer>> n2 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2), "num_value_3_indexed");

                    final var comparandValue = AbstractArrayConstructorValue.LightArrayConstructorValue.of(RecordConstructorValue.ofColumns(List.of(str1, n1)), RecordConstructorValue.ofColumns(List.of(str2, n2)));

                    final var encapsulatedIn = (BooleanValue)new InOpValue.InFn().encapsulate(ImmutableList.of(inArrayValue, comparandValue));
                    graphExpansionBuilder.addPredicate(encapsulatedIn.toQueryPredicate(null, Quantifier.current()).orElseThrow());

                    graphExpansionBuilder.addResultColumn(Column.unnamedOf(strValueIndexed));
                    graphExpansionBuilder.addResultColumn(Column.unnamedOf(numValue3Indexed));
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                });

        assertMatchesExactly(plan,
                        mapPlan(predicatesFilterPlan(typeFilterPlan(scanPlan()))));
        assertEquals(-1783159911, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        final var usedTypes = UsedTypesProperty.evaluate(plan);
        final var evaluationContext =
                EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(usedTypes).build());
        assertEquals(20, querySimpleRecordStore(hook, plan, () -> evaluationContext,
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2))),
                context -> TestHelpers.assertDiscardedExactly(80, context)));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void testTupleInListCannotPromote() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            final Index compoundIndex = new Index("compoundIndex", Key.Expressions.concat(Key.Expressions.field("str_value_indexed"), Key.Expressions.field("num_value_3_indexed")));
            metaData.addIndex("MySimpleRecord", compoundIndex);
        };

        complexQuerySetup(hook);
        Assertions.assertThrows(SemanticException.class, () -> planGraph(
                () -> {
                    var qun = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                    final var graphExpansionBuilder = GraphExpansion.builder();

                    graphExpansionBuilder.addQuantifier(qun);
                    final var strValueIndexed =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "str_value_indexed");
                    final var numValue3Indexed =
                            FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_3_indexed");

                    final var inArrayValue = RecordConstructorValue.ofUnnamed(List.of(strValueIndexed, numValue3Indexed));
                    Column<LiteralValue<?>> str1 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "foo"), "str_value_indexed");
                    Column<LiteralValue<?>> str2 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "bar"), "str_value_indexed");
                    Column<LiteralValue<?>> n1 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L), "num_value_3_indexed");
                    Column<LiteralValue<?>> n2 = FDBSimpleQueryGraphTest.resultColumn(new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L), "num_value_3_indexed");

                    final var comparandValue = AbstractArrayConstructorValue.LightArrayConstructorValue.of(RecordConstructorValue.ofColumns(List.of(str1, n1)), RecordConstructorValue.ofColumns(List.of(str2, n2)));

                    final var encapsulatedIn = (BooleanValue)new InOpValue.InFn().encapsulate(ImmutableList.of(inArrayValue, comparandValue));
                    graphExpansionBuilder.addPredicate(encapsulatedIn.toQueryPredicate(null, Quantifier.current()).orElseThrow());

                    graphExpansionBuilder.addResultColumn(Column.unnamedOf(strValueIndexed));
                    graphExpansionBuilder.addResultColumn(Column.unnamedOf(numValue3Indexed));
                    qun = Quantifier.forEach(Reference.of(graphExpansionBuilder.build().buildSelect()));
                    return Reference.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
                }));
    }

    /**
     * Verify that an IN with an index is implemented as an index scan, with an IN join.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testInQueryCoveringIndex[reverse={0}]")
    @BooleanSource
    void testInQueryCoveringIndex(boolean reverse) throws Exception {
        complexQuerySetup(NO_HOOK);
        List<Integer> ls = asList(1, 2, 4);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in(ls))
                .setSort(field("num_value_3_indexed"), reverse)
                .setRequiredResults(ImmutableList.of(field("rec_no")))
                .build();
        final List<Integer> comparisonLs = reverse ? Lists.reverse(ls) : ls;

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .setDeferFetchAfterUnionAndIntersection(true)
                .setDeferFetchAfterInJoinAndInUnion(true)
                .build());

        final RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(
                                            indexPlan()
                                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                                                    .and(reverse ? isReverse() : isNotReverse())
                                    ))
                    ).where(inValuesList(equalsObject(comparisonLs))));
            assertEquals(reverse ? -2004057427 : -2004060309, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 467383055 : 639155267, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                            inValuesJoinPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan()
                                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                                    .and(scanComparisons(equalities(only(anyValueComparison()))))
                                                    .and(isNotReverse())
                                            )))
                                    .where(inValuesList(equalsObject(comparisonLs))));
            assertEquals(reverse ? 716701911 : 713934417, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? -1862646972 : -1865414466, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(60, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                rec -> Tuple.from(rec.getNumValue3Indexed()),
                reverse,
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that an IN (with parameter) with an index is implemented as an index scan, with an IN join.
     */
    @DualPlannerTest
    void testInQueryParameter() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in("valueThrees"))
                .build();

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .build());

        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN $valueThrees
            assertMatchesExactly(plan,
                    inParameterJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                    ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees"))));
            assertEquals(883815022, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(514739864, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(inParameterJoinPlan(
                            coveringIndexPlan().where(indexPlanOf(indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(equalities(only(anyValueComparison()))))))
                            ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees")))));
            assertEquals(-371539778, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-248745141, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        int count = querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", asList(1, 3, 4)),
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(3), is(4))),
                TestHelpers::assertDiscardedNone);
        assertEquals(60, count);
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testInQuerySortedByParameter[reverse={0}]")
    @BooleanSource
    void testInQuerySortedByParameter(boolean reverse) throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in("valueThrees"))
                .setSort(field("num_value_3_indexed"), reverse)
                .build();

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .build());

        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN $valueThrees SORTED (DESC)
            assertMatchesExactly(plan,
                    inParameterJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                    ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees"))));
            assertEquals(reverse ? 883815025 : 883815023, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 342959006 : 514734098, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(inParameterJoinPlan(
                            coveringIndexPlan().where(indexPlanOf(indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(equalities(only(anyValueComparison()))))))
                    ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees")))));
            assertEquals(reverse ? 1904349145 : 1904350447, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 76381848 : 76382034, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        int count = querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", asList(1, 3, 4)),
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(3), is(4))),
                rec -> Tuple.from(rec.getNumValue3Indexed()),
                reverse,
                TestHelpers::assertDiscardedNone);
        assertEquals(60, count);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @BooleanSource
    void testInQuerySortedByConstantValue(boolean reverse) throws Exception {
        Assumptions.assumeTrue(useCascadesPlanner);
        complexQuerySetup(NO_HOOK);

        final ConstantObjectValue constant = ConstantObjectValue.of(Quantifier.uniqueID(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false)));
        planner.setConfiguration(planner.getConfiguration().asBuilder()
                .setAttemptFailedInJoinAsUnionMaxSize(100)
                .build());
        final RecordQueryPlan plan = planGraph(() -> {
            final Quantifier base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
            var selectQun = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                    .addQuantifier(base)
                    .addPredicate(FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_3_indexed")
                            .withComparison(new Comparisons.ValueComparison(Comparisons.Type.IN, constant)))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base.getFlowedObjectValue(), "num_value_3_indexed"))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base.getFlowedObjectValue(), "rec_no"))
                    .build()
                    .buildSelect()));

            final AliasMap aliasMap = AliasMap.ofAliases(selectQun.getAlias(), Quantifier.current());
            return Reference.of(new LogicalSortExpression(ImmutableList.of(FieldValue.ofFieldName(selectQun.getFlowedObjectValue(), "num_value_3_indexed").rebase(aliasMap)), reverse, selectQun));
        });

        // map(Covering(Index(MySimpleRecord$num_value_3_indexed [EQUALS $q50]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]])[($q91.num_value_3_indexed as num_value_3_indexed, $q91.rec_no as rec_no)]) WHERE __corr_q50 IN @0
        // map(Covering(Index(MySimpleRecord$num_value_3_indexed [EQUALS $q50] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]])[($q91.num_value_3_indexed as num_value_3_indexed, $q91.rec_no as rec_no)]) WHERE __corr_q50 IN @0 SORTED DESC
        assertMatchesExactly(plan, inComparandJoinPlan(mapPlan(
                coveringIndexPlan().where(indexPlanOf(
                        indexPlan()
                                .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                .and(scanComparisons(equalities(exactly(anyValueComparison()))))
                                .and(isNotReverse())
                ))
        )));

        assertEquals(reverse ? 1779528640 : 1779528826, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(reverse ? -870875405 : -870875219, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            final List<Map<String, Object>> results = queryAsMaps(plan, constantBindings(constant, List.of(1, 3, 4)));
            assertThat(results, hasSize(60));
            int lastNumValue3 = reverse ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            for (Map<String, Object> result : results) {
                int numValue3 = (Integer) result.get("num_value_3_indexed");
                assertThat(numValue3, in(List.of(1, 3, 4)));
                assertThat(numValue3, reverse ? lessThanOrEqualTo(lastNumValue3) : greaterThanOrEqualTo(lastNumValue3));
            }

            TestHelpers.assertDiscardedAtMost(reverse ? 40 : 0, context);
            commit(context);
        }
    }

    /**
     * Verify that an IN (with parameter) with an index is implemented as an index scan, with an IN union in the presence
     * of other equality-bound index parts.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testInQueryParameter2[reverse={0}]")
    @BooleanSource
    void testInQueryParameter2(boolean reverse) throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_3_indexed", "num_value_2", "num_value_unique"));
        };

        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").equalsParameter("p"),
                        Query.field("num_value_2").in("valueThrees")))
                .setSort(Key.Expressions.field("num_value_unique"), reverse)
                .build();

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .build());

        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inUnionOnExpressionPlan(
                            indexPlan()
                                    .where(indexName("multi_index"))
                                    .and(scanComparisons(range("[EQUALS $p, EQUALS $__in_num_value_2__0]")))
                                    .and(reverse ? isReverse() : isNotReverse())
                    ).where(inUnionValuesSources(exactly(RecordQueryPlanMatchers.inUnionInParameter(equalsObject("valueThrees"))))));
            assertEquals(reverse ? -1109407604 : -1109408565, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 600067025 : 600245771, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inUnionOnValuesPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("multi_index"))
                                            .and(scanComparisons(equalities(exactly(anyParameterComparison(), anyValueComparison()))))
                                            .and(reverse ? isReverse() : isNotReverse())
                                    ))
                            ).where(RecordQueryPlanMatchers.inUnionValuesSources(exactly(inUnionInParameter(equalsObject("valueThrees")))))));
            assertEquals(reverse ? -1896400167 : -1896406894, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? -1763552763 : -1763374017, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        for (int i = 0; i < 5; i++) {
            final int p = i;
            int count = querySimpleRecordStore(hook, plan,
                    () -> EvaluationContext.forBindings(Bindings.newBuilder().set("p", p).set("valueThrees", List.of(0, 2)).build()),
                    rec -> {
                        assertEquals(p, rec.getNumValue3Indexed());
                        assertThat(rec.getNumValue2(), anyOf(is(0), is(2)));
                    },
                    rec -> Tuple.from(rec.getNumValueUnique()),
                    reverse,
                    TestHelpers::assertDiscardedNone);
            assertThat(count, anyOf(is(13), is(14)));
        }
    }

    /**
     * Verify that an IN (with parameter) with an index is implemented as an index scan, with an IN union in the presence
     * of other equality-bound index parts.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testInQueryParameter2UsingCoveringIndexScans[reverse={0}]")
    @BooleanSource
    void testInQueryParameter2UsingCoveringIndexScans(boolean reverse) throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_3_indexed", "num_value_2", "num_value_unique"));
        };

        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").equalsParameter("p"),
                        Query.field("num_value_2").in("valueThrees")))
                .setSort(Key.Expressions.field("num_value_unique"), reverse)
                .setRequiredResults(ImmutableList.of(field("num_value_unique")))
                .build();

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .setDeferFetchAfterUnionAndIntersection(true)
                .setDeferFetchAfterInJoinAndInUnion(true)
                .build());

        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inUnionOnExpressionPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan()
                                            .where(indexName("multi_index"))
                                            .and(scanComparisons(range("[EQUALS $p, EQUALS $__in_num_value_2__0]")))
                                            .and(reverse ? isReverse() : isNotReverse())
                                    ))
                    ).where(inUnionValuesSources(exactly(RecordQueryPlanMatchers.inUnionInParameter(equalsObject("valueThrees"))))));
            assertEquals(reverse ? -1109407604 : -1109408565, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? -1017737587 : -1017558841, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                            inUnionOnValuesPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("multi_index"))
                                            .and(scanComparisons(equalities(exactly(anyParameterComparison(), anyValueComparison()))))
                                            .and(reverse ? isReverse() : isNotReverse())
                                    ))
                            ).where(RecordQueryPlanMatchers.inUnionValuesSources(exactly(inUnionInParameter(equalsObject("valueThrees"))))));
            assertEquals(reverse ? -884481068 : -884482029, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 147733678 : 147912424, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        for (int i = 0; i < 5; i++) {
            final int p = i;
            int count = querySimpleRecordStore(hook, plan,
                    () -> EvaluationContext.forBindings(Bindings.newBuilder().set("p", p).set("valueThrees", List.of(0, 2)).build()),
                    rec -> {
                        assertEquals(p, rec.getNumValue3Indexed());
                        assertThat(rec.getNumValue2(), anyOf(is(0), is(2)));
                    },
                    rec -> Tuple.from(rec.getNumValueUnique()),
                    reverse,
                    TestHelpers::assertDiscardedNone);
            assertThat(count, anyOf(is(13), is(14)));
        }
    }

    /**
     * Verify that an in with a bad parameter plans correctly but fails upon execution.
     */
    @DualPlannerTest
    void testInQueryParameterBad() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in("valueThrees"))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN $valueThrees
        RecordQueryPlan plan = planQuery(query);
        if (useCascadesPlanner) {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inParameterJoinPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(
                                                    indexPlan()
                                                            .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison()))))
                                            ))
                            ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees"))))
            );
            assertEquals(-371539778, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-248745141, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    inParameterJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                    ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees"))));
            assertEquals(883815022, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(514739864, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", Collections.emptyList()),
                rec -> fail("There should be no results")));
        assertThrows(RecordCoreException.class, TestHelpers.toCallable(() ->
                assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                        EvaluationContext::empty, /* no binding for valueThrees */
                        rec -> fail("There should be no results")))));
        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", null), /* no binding for valueThrees */
                rec -> fail("There should be no results")));
    }


    /**
     * Verify that NOT IN is planned correctly, and fails if no binding is provided.
     */
    @DualPlannerTest
    void testNotInQueryParameterBad() throws Exception {
        complexQuerySetup(NO_HOOK);
        final QueryComponent filter = Query.not(Query.field("num_value_3_indexed").in("valueThrees"));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .build();

        // Scan(<,>) | [MySimpleRecord] | Not(num_value_3_indexed IN $valueThrees)
        RecordQueryPlan plan = planQuery(query);
        if (useCascadesPlanner) {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(predicates(notPredicate(exactly(anyPredicate())))));
            assertEquals(-669043655, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1485376113, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    filterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(queryComponents(exactly(equalsObject(filter)))));
            assertEquals(1667070490, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1108868609, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(100, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", Collections.emptyList()),
                rec -> {
                },
                TestHelpers::assertDiscardedNone));
        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", null), /* no binding for valueThrees */
                rec -> fail("There should be no results")));
    }

    /**
     * Verify that an IN against an unsorted list with an index is implemented as an index scan, with an IN join on
     * a sorted copy of the list.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testInQueryIndexSorted[reverse={0}]")
    @BooleanSource
    void testInQueryIndexSorted(boolean reverse) throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in(asList(1, 4, 2)))
                .setSort(field("num_value_3_indexed"), reverse)
                .build();
        final List<Integer> comparisonList = reverse ? List.of(4, 2, 1) : List.of(1, 2, 4);

        // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN [1, 2, 4] SORTED
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                    ).where(inValuesList(equalsObject(comparisonList))));
            assertEquals(reverse ? -2004057427 : -2004060309, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 399454035 : 571226247, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                            .and(scanComparisons(equalities(only(anyValueComparison()))))))
                                    ).where(inValuesList(equalsObject(comparisonList)))));
            assertEquals(reverse ? 721946094 : 702573636, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 521033883 : 518266389, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(60, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that an IN against an unsorted list with an index is not implemented as an IN JOIN when the query sort is
     * not by the field with an IN filter.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testInQueryIndexSortedDifferently[reverse={0}]")
    @BooleanSource
    void testInQueryIndexSortedDifferently(boolean reverse) throws Exception {
        complexQuerySetup(NO_HOOK);
        final QueryComponent filter = Query.field("num_value_3_indexed").in(asList(1, 4, 2));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("str_value_indexed"), reverse)
                .build();

        // Index(MySimpleRecord$str_value_indexed <,>) | num_value_3_indexed IN [1, 4, 2]
        RecordQueryPlan plan = planQuery(query);
        // IN join is cancelled on account of incompatible sorting.
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(selfOrDescendantPlans(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(unbounded())).and(reverse ? isReverse() : isNotReverse())))
                            .where(queryComponents(exactly(equalsObject(filter)))));
            assertEquals(reverse ? 1775865787 : 1775865786, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? 966501 : 972267, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(selfOrDescendantPlans(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(unbounded()).and(reverse ? isReverse() : isNotReverse()))))
                            .where(predicates(valuePredicate(fieldValueWithFieldNames("num_value_3_indexed"), new Comparisons.ListComparison(Comparisons.Type.IN, ImmutableList.of(1, 4, 2))))));
            assertEquals(reverse ? -560248358 : -560248359, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(reverse ? -1230651057 : -1230645291, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(60, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                rec -> Tuple.from(rec.getStrValueIndexed()),
                reverse,
                context -> TestHelpers.assertDiscardedAtMost(40, context)));
    }

    @SuppressWarnings("unused") // parameter source for parameterized test
    @Nonnull
    static Stream<Arguments> testParameterizedInQuerySortedWithExtraUnorderedColumns() {
        return Stream.of(false, true).flatMap(omitPrimaryKeyInOrdering ->
                Stream.of(false, true).map(reverse ->
                        Arguments.of(omitPrimaryKeyInOrdering, reverse)));
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testParameterizedInQuerySortedWithExtraUnorderedColumns[omitPrimaryKeyInOrdering={0}, reverse={1}]")
    @MethodSource
    void testParameterizedInQuerySortedWithExtraUnorderedColumns(boolean omitPrimaryKeyInOrdering, boolean reverse) throws Exception {
        final Index index = new Index("indexForInUnion", concatenateFields("num_value_2", "num_value_3_indexed", "num_value_unique", "str_value_indexed"));
        final RecordMetaDataHook hook = complexQuerySetupHook()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));
        complexQuerySetup(hook);

        final String value2Param = "value2";
        final String value3ListParam = "value3List";
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsParameter(value2Param),
                        Query.field("num_value_3_indexed").in(value3ListParam)
                ))
                .setSort(field("num_value_unique"), reverse)
                .setRequiredResults(List.of(field("num_value_unique"), field("str_value_indexed"), field("rec_no")))
                .build();

        planner.setConfiguration(planner.getConfiguration().asBuilder()
                .setAttemptFailedInJoinAsUnionMaxSize(1000)
                .setDeferFetchAfterInJoinAndInUnion(true)
                .setOmitPrimaryKeyInOrderingKeyForInUnion(omitPrimaryKeyInOrdering)
                .build());
        final RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            if (omitPrimaryKeyInOrdering) {
                // Execute this plan as an in-union:
                //     ∪(__in_num_value_3_indexed__0 IN $value3List) Covering(Index(indexForInUnion [EQUALS $value2, EQUALS $__in_num_value_3_indexed__0]) -> [num_value_2: KEY[0], num_value_3_indexed: KEY[1], num_value_unique: KEY[2], rec_no: KEY[4], str_value_indexed: KEY[3]])
                assertMatchesExactly(plan,
                        inUnionOnExpressionPlan(
                                coveringIndexPlan().where(indexPlanOf(indexPlan()
                                        .where(indexName(index.getName()))
                                        .and(reverse ? isReverse() : isNotReverse())
                                        .and(scanComparisons(range("[EQUALS $" + value2Param + ", EQUALS $__in_num_value_3_indexed__0]")))
                                ))
                        ).where(inUnionValuesSources(exactly(inUnionInParameter(equalsObject(value3ListParam))))).and(inUnionComparisonKey(concatenateFields("num_value_unique", "str_value_indexed", "rec_no"))));

                assertEquals(reverse ? -1664186714 : -1664187675, plan.planHash(CURRENT_LEGACY));
                assertEquals(reverse ? -1525853359 : -1525674613, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                // In this config, the extra str_value_indexed column trips up the ordering key calculation, so we have to scan the num_value_unique index and filter:
                //     Index(MySimpleRecord$num_value_unique <,>) | And([num_value_2 EQUALS $value2, num_value_3_indexed IN $value3List])
                List<QueryComponent> filters = Objects.requireNonNull((AndComponent)query.getFilter()).getChildren();
                assertMatchesExactly(plan,
                        filterPlan(indexPlan()
                                .where(indexName("MySimpleRecord$num_value_unique"))
                                .and(scanComparisons(unbounded()))
                                .and(reverse ? isReverse() : isNotReverse())
                        ).where(queryComponents(exactly(filters.stream().map(PrimitiveMatchers::equalsObject).collect(Collectors.toList())))));

                assertEquals(reverse ? 2025860977 : 2025860976, plan.planHash(CURRENT_LEGACY));
                assertEquals(reverse ? 1776783587 : 1776789353, plan.planHash(CURRENT_FOR_CONTINUATION));
            }
        } else {
            // Similar to the old planner with the more permissive ordering key option, but using Cascades planner objects:
            //     ∪(__corr_q70 IN $value3List) Covering(Index(indexForInUnion [EQUALS $value2, EQUALS $q70]) -> [num_value_2: KEY[0], num_value_3_indexed: KEY[1], num_value_unique: KEY[2], rec_no: KEY[4], str_value_indexed: KEY[3]])
            assertMatchesExactly(plan,
                    inUnionOnValuesPlan(
                            coveringIndexPlan().where(indexPlanOf(indexPlan()
                                    .where(indexName(index.getName()))
                                    .and(reverse ? isReverse() : isNotReverse())
                                    .and(scanComparisons(equalities(exactly(anyParameterComparison(), anyValueComparison()))))
                            ))
                    ).where(inUnionComparisonValues(exactly(fieldValueWithFieldNames("num_value_unique"), fieldValueWithFieldNames("str_value_indexed"), fieldValueWithFieldNames("rec_no"), fieldValueWithFieldNames("num_value_3_indexed")))));

            assertEquals(reverse ? 2090127536 : 2090126575, plan.planHash(CURRENT_LEGACY));
            assertEquals(reverse ? 1582256076 : 1582434822, plan.planHash(CURRENT_FOR_CONTINUATION));
        }

        assertEquals(21, querySimpleRecordStore(hook, plan,
                () -> EvaluationContext.forBindings(Bindings.newBuilder().set(value2Param, 1).set(value3ListParam, List.of(1, 2, 4)).build()),
                rec -> {
                    assertEquals(1, rec.getNumValue2());
                    assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4)));
                },
                rec -> Tuple.from(rec.getNumValueUnique()),
                reverse,
                (isUseCascadesPlanner() || omitPrimaryKeyInOrdering) ? TestHelpers::assertDiscardedNone : (cx -> TestHelpers.assertDiscardedAtMost(79, cx))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest
    @BooleanSource
    void testConstantObjectInQuerySortedWithExtraUnorderedColumns(boolean reverse) throws Exception {
        Assumptions.assumeTrue(useCascadesPlanner);
        final Index index = new Index("indexForInUnion", concatenateFields("num_value_2", "num_value_3_indexed", "num_value_unique", "str_value_indexed"));
        final RecordMetaDataHook hook = complexQuerySetupHook()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));
        complexQuerySetup(hook);

        final CorrelationIdentifier constantAlias = CorrelationIdentifier.uniqueID();
        final ConstantObjectValue nv2Constant = ConstantObjectValue.of(constantAlias, "0", Type.primitiveType(Type.TypeCode.INT, false));
        final ConstantObjectValue listConstant = ConstantObjectValue.of(constantAlias, "1", new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false)));
        planner.setConfiguration(planner.getConfiguration().asBuilder().setAttemptFailedInJoinAsUnionMaxSize(10).build());
        final RecordQueryPlan plan = planGraph(() -> {
            final Quantifier base = FDBSimpleQueryGraphTest.fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
            final Quantifier select = Quantifier.forEach(Reference.of(GraphExpansion.builder()
                    .addQuantifier(base)
                    .addPredicate(FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_2")
                            .withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, nv2Constant)))
                    .addPredicate(FieldValue.ofFieldName(base.getFlowedObjectValue(), "num_value_3_indexed")
                            .withComparison(new Comparisons.ValueComparison(Comparisons.Type.IN, listConstant)))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base, "num_value_unique"))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base, "str_value_indexed"))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base, "num_value_3_indexed"))
                    .addResultColumn(FDBSimpleQueryGraphTest.projectColumn(base, "rec_no"))
                    .build()
                    .buildSelect()));

            final AliasMap aliasMap = AliasMap.ofAliases(select.getAlias(), Quantifier.current());
            return Reference.of(new LogicalSortExpression(ImmutableList.of(FieldValue.ofFieldName(select.getFlowedObjectValue(), "num_value_unique").rebase(aliasMap)), reverse, select));
        });

        assertMatchesExactly(plan,
                inUnionOnValuesPlan(
                        mapPlan(
                            coveringIndexPlan().where(indexPlanOf(indexPlan()
                                    .where(indexName(index.getName()))
                                    .and(reverse ? isReverse() : isNotReverse())
                                    .and(scanComparisons(equalities(exactly(anyValueComparison(), anyValueComparison()))))
                            ))
                        )
                ).where(inUnionComparisonValues(exactly(fieldValueWithFieldNames("num_value_unique"), fieldValueWithFieldNames("str_value_indexed"), fieldValueWithFieldNames("num_value_3_indexed"), fieldValueWithFieldNames("rec_no"))))
        );

        assertEquals(reverse ? -621059059 : -621088850,  plan.planHash(CURRENT_LEGACY));
        assertEquals(reverse ? -296784631 : -291243505, plan.planHash(CURRENT_FOR_CONTINUATION));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            List<Map<String, Object>> results = queryAsMaps(plan, constantBindings(Map.of(nv2Constant, 1, listConstant, List.of(1, 2, 4))));
            assertThat(results, hasSize(21));
            int lastNumUnique = reverse ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            for (Map<String, Object> res : results) {
                assertThat(res.get("num_value_3_indexed"), in(new Integer[]{1, 2, 4}));
                int numUnique = (Integer) res.get("num_value_unique");
                assertThat(numUnique, reverse ? lessThanOrEqualTo(lastNumUnique) : greaterThanOrEqualTo(lastNumUnique));
                lastNumUnique = numUnique;
            }

            TestHelpers.assertDiscardedNone(context);
            commit(context);
        }
    }

    enum InAsOrUnionMode {
        NONE,
        AS_OR,
        AS_UNION;

        RecordQueryPlannerConfiguration.Builder configure(RecordQueryPlannerConfiguration.Builder config) {
            switch (this) {
                case AS_OR:
                    config.setAttemptFailedInJoinAsOr(true);
                    config.setAttemptFailedInJoinAsUnionMaxSize(0);
                    break;
                case AS_UNION:
                    config.setAttemptFailedInJoinAsOr(true);
                    config.setAttemptFailedInJoinAsUnionMaxSize(1000);
                    break;
                case NONE:
                default:
                    config.setAttemptFailedInJoinAsOr(false);
                    config.setAttemptFailedInJoinAsUnionMaxSize(0);
                    break;
            }
            return config;
        }
    }

    /**
     * Verify that an IN query with a sort can be implemented as an ordered union of compound indexes that can satisfy
     * the sort once the equality predicates from the IN have been pushed onto the indexes.
     * @see com.apple.foundationdb.record.query.plan.planning.InExtractor#asOr()
     */
    @DualPlannerTest
    @ParameterizedTest
    @EnumSource(InAsOrUnionMode.class)
    void inQueryWithSortBySecondFieldOfCompoundIndex(InAsOrUnionMode inAsOrMode) throws Exception {
        RecordMetaDataHook hook = metaData ->
                metaData.addIndex("MySimpleRecord", "compoundIndex",
                        concat(field("num_value_3_indexed"), field("str_value_indexed")));
        complexQuerySetup(hook);
        final List<Integer> inList = asList(1, 4, 2);
        final QueryComponent filter = Query.field("num_value_3_indexed").in(inList);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("str_value_indexed"))
                .build();

        planner.setConfiguration(inAsOrMode.configure(planner.getConfiguration().asBuilder())
                .build());

        // Index(MySimpleRecord$str_value_indexed <,>) | num_value_3_indexed IN [1, 4, 2]
        // Index(compoundIndex [[1],[1]]) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex [[4],[4]]) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex [[2],[2]])
        // ∪(__in_num_value_3_indexed__0 IN [1, 4, 2]) Index(compoundIndex [EQUALS $__in_num_value_3_indexed__0])
        RecordQueryPlan plan = planQuery(query);
        if (useCascadesPlanner) {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inUnionOnValuesPlan(
                                    coveringIndexPlan().where(indexPlanOf(
                                            indexPlan()
                                                    .where(indexName("compoundIndex"))
                                                    .and(isNotReverse())
                                                    .and(scanComparisons(equalities(exactly(anyValueComparison()))))
                                    ))
                            )
                    ));
            assertEquals(637018497, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1575544496, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            // Cascades always plans AS_UNION, so if we execute the plan with other modes, we get errors corresponding to
            // the number of legs exceeding the limit (i.e., 0).
            Assumptions.assumeTrue(inAsOrMode == InAsOrUnionMode.AS_UNION);
        } else if (inAsOrMode == InAsOrUnionMode.AS_OR) {
            assertMatchesExactly(plan,
                    unionOnExpressionPlan(inList.stream().map(number ->
                            indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range("[[" + number + "],[" + number + "]]"))))
                            .collect(ImmutableList.toImmutableList()))
                            .where(comparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord")))));
            assertEquals(-1813975352, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1188407258, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (inAsOrMode == InAsOrUnionMode.AS_UNION) {
            assertMatchesExactly(plan,
                    inUnionOnExpressionPlan(indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]"))))
                            .where(inUnionComparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))
                            .and(inUnionValuesSources(exactly(inUnionInValues(equalsObject(inList))
                                    .and(inUnionBindingName("__in_num_value_3_indexed__0"))))));
            assertEquals(-1777083372, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1930894388, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    filterPlan(indexPlan()
                            .where(indexName("MySimpleRecord$str_value_indexed"))
                            .and(scanComparisons(unbounded()))
                    ).where(queryComponents(exactly(equalsObject(filter)))));

            assertEquals(1775865786, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(972267, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        assertEquals(60, querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                rec -> Tuple.from(rec.getStrValueIndexed()),
                false,
                context -> TestHelpers.assertDiscardedAtMost(40, context)));
    }

    /**
     * Verify that an IN query with a sort and range predicate can be implemented as an ordered union of compound indexes
     * that can satisfy the sort once the equality predicates from the IN have been pushed onto the indexes.
     * @see com.apple.foundationdb.record.query.plan.planning.InExtractor#asOr()
     */
    @DualPlannerTest
    @ParameterizedTest
    @EnumSource(InAsOrUnionMode.class)
    void inQueryWithSortAndRangePredicateOnSecondFieldOfCompoundIndex(InAsOrUnionMode inAsOrMode) throws Exception {
        RecordMetaDataHook hook = metaData ->
                metaData.addIndex("MySimpleRecord", "compoundIndex",
                        concat(field("num_value_3_indexed"), field("str_value_indexed")));
        complexQuerySetup(hook);
        final List<Integer> inList = asList(1, 4, 2);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").in(inList),
                        Query.field("str_value_indexed").greaterThan("bar"),
                        Query.field("str_value_indexed").lessThan("foo")))
                .setSort(field("str_value_indexed"))
                .build();

        planner.setConfiguration(inAsOrMode.configure(planner.getConfiguration().asBuilder())
                .build());

        // Index(MySimpleRecord$str_value_indexed ([bar],[foo])) | num_value_3_indexed IN [1, 4, 2]
        // Index(compoundIndex ([1, bar],[1, foo])) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex ([4, bar],[4, foo])) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex ([2, bar],[2, foo]))
        // ∪(__in_num_value_3_indexed__0 IN [1, 4, 2]) Index(compoundIndex [EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]])
        RecordQueryPlan plan = planQuery(query);
        if (useCascadesPlanner) {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inUnionOnValuesPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("compoundIndex"))
                                            .and(isNotReverse())
                                            .and(scanComparisons(equalities(exactly(anyValueComparison()))))
                                    ))
                            )
                    )
            );
            assertEquals(-2146200107, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-769647490, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            // Cascades always plans AS_UNION, so if we execute the plan with other modes, we get errors corresponding to
            // the number of legs exceeding the limit (i.e., 0).
            Assumptions.assumeTrue(inAsOrMode == InAsOrUnionMode.AS_UNION);
        } else if (inAsOrMode == InAsOrUnionMode.AS_OR) {
            // IN join is impossible because of incompatible sorting, but we can still plan as an OR on the compound index.
            assertMatchesExactly(plan,
                    unionOnExpressionPlan(
                            inList.stream()
                                    .map(number -> indexPlan().where(indexName("compoundIndex")).and(scanComparisons(range("([" + number + ", bar],[" + number + ", foo])"))))
                                    .collect(ImmutableList.toImmutableList()))
                            .where(RecordQueryPlanMatchers.comparisonKey(equalsObject(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))));
            assertEquals(651476052, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-2091774924, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (inAsOrMode == InAsOrUnionMode.AS_UNION) {
            assertMatchesExactly(plan,
                    inUnionOnExpressionPlan(indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]]"))))
                            .where(inUnionComparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))
                            .and(inUnionValuesSources(exactly(inUnionInValues(equalsObject(inList))
                                    .and(inUnionBindingName("__in_num_value_3_indexed__0"))))));
            assertEquals(-333985760, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(18880922, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    filterPlan(indexPlan()
                            .where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([bar],[foo])")))
                    ).where(queryComponents(only(equalsObject(Query.field("num_value_3_indexed").in(inList))))));
            assertEquals(-1681846586, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(340962909, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        assertEquals(30, querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                rec -> Tuple.from(rec.getStrValueIndexed()),
                false,
                context -> { }));
    }

    @SuppressWarnings("unused") // parameter source for parameterized test
    @Nonnull
    static Stream<Arguments> testListInQuerySortedWithExtraUnorderedColumns() {
        return Arrays.stream(InAsOrUnionMode.values()).flatMap(inAsOrUnionMode ->
                Stream.of(false, true).flatMap(omitPrimaryKeyInOrdering ->
                        Stream.of(false, true).map(reverse ->
                                Arguments.of(inAsOrUnionMode, omitPrimaryKeyInOrdering, reverse))));
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testListInQuerySortedWithExtraUnorderedColumns[inAsOrUnionMode={0}, omitPrimaryKeyInOrdering={1}, reverse={2}]")
    @MethodSource
    void testListInQuerySortedWithExtraUnorderedColumns(InAsOrUnionMode inAsOrUnionMode, boolean omitPrimaryKeyInOrdering, boolean reverse) throws Exception {
        final Index index = new Index("indexForInUnion", concatenateFields("num_value_2", "num_value_3_indexed", "num_value_unique", "str_value_indexed"));
        final RecordMetaDataHook hook = complexQuerySetupHook()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));
        complexQuerySetup(hook);

        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").in(List.of(1, 2, 4))
                ))
                .setSort(field("num_value_unique"), reverse)
                .setRequiredResults(List.of(field("num_value_unique"), field("str_value_indexed"), field("rec_no")))
                .build();

        planner.setConfiguration(inAsOrUnionMode.configure(planner.getConfiguration().asBuilder())
                .setDeferFetchAfterInJoinAndInUnion(true)
                .setOmitPrimaryKeyInUnionOrderingKey(omitPrimaryKeyInOrdering) // for AS_UNION mode
                .setOmitPrimaryKeyInOrderingKeyForInUnion(omitPrimaryKeyInOrdering) // for AS_OR mode
                .build());
        final RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            if (inAsOrUnionMode == InAsOrUnionMode.NONE || !omitPrimaryKeyInOrdering) {
                // In this config, the extra str_value_indexed column trips up the ordering key calculation, so we have to scan the num_value_unique index and filter:
                //     Index(MySimpleRecord$num_value_unique <,>) | And([num_value_2 EQUALS 1, num_value_3_indexed IN (1, 2, 4)])
                List<QueryComponent> filters = Objects.requireNonNull((AndComponent)query.getFilter()).getChildren();
                assertMatchesExactly(plan,
                        filterPlan(indexPlan()
                                .where(indexName("MySimpleRecord$num_value_unique"))
                                .and(scanComparisons(unbounded()))
                                .and(reverse ? isReverse() : isNotReverse())
                        ).where(queryComponents(exactly(filters.stream().map(PrimitiveMatchers::equalsObject).collect(Collectors.toList())))));

                assertEquals(reverse ? -207141918 : -207141919, plan.planHash(CURRENT_LEGACY));
                assertEquals(reverse ? 898139552 : 898145318, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else if (inAsOrUnionMode == InAsOrUnionMode.AS_UNION) {
                // Execute this plan as an in-union:
                //     ∪(__in_num_value_3_indexed__0 IN $value3List) Covering(Index(indexForInUnion [EQUALS $value2, EQUALS $__in_num_value_3_indexed__0]) -> [num_value_2: KEY[0], num_value_3_indexed: KEY[1], num_value_unique: KEY[2], rec_no: KEY[4], str_value_indexed: KEY[3]])
                assertMatchesExactly(plan,
                        inUnionOnExpressionPlan(
                                coveringIndexPlan().where(indexPlanOf(indexPlan()
                                        .where(indexName(index.getName()))
                                        .and(reverse ? isReverse() : isNotReverse())
                                        .and(scanComparisons(range("[EQUALS 1, EQUALS $__in_num_value_3_indexed__0]")))
                                ))
                        ).where(inUnionValuesSources(exactly(inUnionInValues(containsAll(Set.of(1, 2, 4)))))).and(inUnionComparisonKey(concatenateFields("num_value_unique", "str_value_indexed", "rec_no"))));

                assertEquals(reverse ? -1827031155 : -1827032116, plan.planHash(CURRENT_LEGACY));
                assertEquals(reverse ? 1591762319 : 1591941065, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                // Execute this plan as an normal union:
                //     ∪(__in_num_value_3_indexed__0 IN $value3List) Covering(Index(indexForInUnion [EQUALS $value2, EQUALS $__in_num_value_3_indexed__0]) -> [num_value_2: KEY[0], num_value_3_indexed: KEY[1], num_value_unique: KEY[2], rec_no: KEY[4], str_value_indexed: KEY[3]])
                assertMatchesExactly(plan,
                        unionOnExpressionPlan(
                                coveringIndexPlan().where(indexPlanOf(indexPlan()
                                        .where(indexName(index.getName()))
                                        .and(scanComparisons(range("[[1, 1],[1, 1]]")))
                                        .and(reverse ? isReverse() : isNotReverse())
                                )),
                                coveringIndexPlan().where(indexPlanOf(indexPlan()
                                        .where(indexName(index.getName()))
                                        .and(scanComparisons(range("[[1, 2],[1, 2]]")))
                                        .and(reverse ? isReverse() : isNotReverse())
                                )),
                                coveringIndexPlan().where(indexPlanOf(indexPlan()
                                        .where(indexName(index.getName()))
                                        .and(scanComparisons(range("[[1, 4],[1, 4]]")))
                                        .and(reverse ? isReverse() : isNotReverse())
                                ))
                        ).where(comparisonKey(concatenateFields("num_value_unique", "str_value_indexed", "rec_no"))));

                assertEquals(reverse ? -1042918614 : -1042919608, plan.planHash(CURRENT_LEGACY));
                assertEquals(reverse ? -2120887025 : -1943392061, plan.planHash(CURRENT_FOR_CONTINUATION));
            }
        } else {
            // Cascades always uses an in-union, and it doesn't have trouble with the extra unordered columns, so it behaves like the old planner
            // when omitPrimaryKeyInOrdering is true and the mode is AS_UNION
            //     ∪(__corr_q70 IN (1, 2, 4)) Covering(Index(indexForInUnion [EQUALS 1, EQUALS $q70]) -> [num_value_2: KEY[0], num_value_3_indexed: KEY[1], num_value_unique: KEY[2], rec_no: KEY[4], str_value_indexed: KEY[3]])
            assertMatchesExactly(plan,
                    inUnionOnValuesPlan(
                            coveringIndexPlan().where(indexPlanOf(indexPlan()
                                    .where(indexName(index.getName()))
                                    .and(reverse ? isReverse() : isNotReverse())
                                    .and(scanComparisons(equalities(exactly(equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1)), anyValueComparison()))))
                            ))
                    ).where(inUnionComparisonValues(exactly(fieldValueWithFieldNames("num_value_unique"), fieldValueWithFieldNames("str_value_indexed"), fieldValueWithFieldNames("rec_no"), fieldValueWithFieldNames("num_value_3_indexed")))));

            assertEquals(reverse ? 1927283095 : 1927282134, plan.planHash(CURRENT_LEGACY));
            assertEquals(reverse ? 404904458 : 405083204, plan.planHash(CURRENT_FOR_CONTINUATION));

            if (inAsOrUnionMode != InAsOrUnionMode.AS_UNION) {
                // When the mode is not AS_UNION, the plan should fail to execute because the parameter list is
                // limited in size to 0 parameters. Potentially, this should chose an alternative plan
                final RecordCoreException rce = assertThrows(RecordCoreException.class, () -> executeQuery(plan));
                assertThat(rce.getMessage(), containsString("too many IN values"));
                return;
            }
        }

        assertEquals(21, querySimpleRecordStore(hook, plan,
                EvaluationContext::empty,
                rec -> {
                    assertEquals(1, rec.getNumValue2());
                    assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(2), is(4)));
                },
                rec -> Tuple.from(rec.getNumValueUnique()),
                reverse,
                (isUseCascadesPlanner() || (omitPrimaryKeyInOrdering && inAsOrUnionMode != InAsOrUnionMode.NONE)) ? TestHelpers::assertDiscardedNone : (cx -> TestHelpers.assertDiscardedAtMost(79, cx))));
    }

    /**
     * Verify that an IN query over a parameter with a sort keeps filter even if OR is allowed.
     */
    @ParameterizedTest
    @EnumSource(InAsOrUnionMode.class)
    @DualPlannerTest
    void inQueryWithSortAndParameter(InAsOrUnionMode inAsOrMode) throws Exception {
        RecordMetaDataHook hook = metaData ->
                metaData.addIndex("MySimpleRecord", "compoundIndex",
                        concat(field("num_value_3_indexed"), field("str_value_indexed")));
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").in("inList"),
                        Query.field("str_value_indexed").greaterThan("bar"),
                        Query.field("str_value_indexed").lessThan("foo")))
                .setSort(field("str_value_indexed"))
                .build();

        planner.setConfiguration(inAsOrMode.configure(planner.getConfiguration().asBuilder())
                .build());

        // Index(MySimpleRecord$str_value_indexed ([bar],[foo])) | num_value_3_indexed IN $inList
        // ∪(__in_num_value_3_indexed__0 IN $inList) Index(compoundIndex [EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]])
        RecordQueryPlan plan = planQuery(query);
        if (useCascadesPlanner) {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inUnionOnValuesPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(
                                                    indexPlan()
                                                            .where(indexName("compoundIndex"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison()))))
                                            ))
                            )
                    ));
            assertEquals(96196115, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(164261584, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

            Assumptions.assumeTrue(inAsOrMode == InAsOrUnionMode.AS_UNION);
        } else if (inAsOrMode == InAsOrUnionMode.AS_UNION) {
            assertMatchesExactly(plan,
                    inUnionOnExpressionPlan(indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]]"))))
                            .where(inUnionComparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))
                            .and(inUnionValuesSources(exactly(inUnionInParameter(equalsObject("inList"))
                                    .and(inUnionBindingName("__in_num_value_3_indexed__0"))))));
            assertEquals(599923314, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(952789996, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    filterPlan(indexPlan()
                            .where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([bar],[foo])")))
                    ).where(queryComponents(only(equalsObject(Query.field("num_value_3_indexed").in("inList"))))));
            assertEquals(1428066748, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1407869064, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(30, querySimpleRecordStore(hook, plan,
                () -> EvaluationContext.forBinding("inList", asList(1, 3, 4)),
                rec -> assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(3), is(4))),
                rec -> Tuple.from(rec.getStrValueIndexed()),
                false,
                context -> { }));
        assertEquals(0, querySimpleRecordStore(hook, plan,
                () -> EvaluationContext.forBinding("inList", ImmutableList.of()),
                rec -> fail("should not have any records"),
                context -> { }));
        assertEquals(10, querySimpleRecordStore(hook, plan,
                () -> EvaluationContext.forBinding("inList", ImmutableList.of(3)),
                rec -> assertThat(rec.getNumValue3Indexed(), is(3)),
                rec -> Tuple.from(rec.getStrValueIndexed()),
                false,
                context -> { }));
    }

    /**
     * Verify that an IN predicate that, when converted to an OR of equality predicates, would lead to a very large DNF
     * gets planned as a normal IN query rather than throwing an exception.
     */
    @Test
    void cnfAsInQuery() throws Exception {
        RecordMetaDataHook hook = metaData ->
                metaData.addIndex("MySimpleRecord", "compoundIndex",
                        concat(field("num_value_3_indexed"), field("str_value_indexed")));
        complexQuerySetup(hook);

        // A CNF whose DNF size doesn't fit in an int, expressed with IN predicates.
        List<QueryComponent> conjuncts = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            conjuncts.add(Query.field("num_value_3_indexed").in(ImmutableList.of(i * 100, i * 100 + 1)));
        }

        final QueryComponent filter = Query.and(conjuncts);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("str_value_indexed"))
                .build();
        RecordQueryPlan plan = planQuery(query);
        // Did not throw an exception
        assertMatchesExactly(plan,
                filterPlan(indexPlan()
                        .where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(unbounded()))
                ).where(queryComponents(exactly(conjuncts.stream().map(PrimitiveMatchers::equalsObject).collect(ImmutableList.toImmutableList())))));
    }

    /**
     * Verify that a query with an IN on the second nested field of a multi-index for which there is also a first nested
     * field is translated into an appropriate index scan.
     */
    @ParameterizedTest(name = "testInWithNesting[normalizeNestedFields={0}]")
    @DualPlannerTest
    @BooleanSource
    void testInWithNesting(boolean normalizeNestedFields) throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, rec) -> {
            rec.setStrValue("_" + i);
            rec.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });

        List<String> ls = asList("String6", "String1", "String25", "String11");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").equalsValue(1L),
                        Query.field("path").in(ls))))
                .build();

        // Index(ind [EQUALS 1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String6, String1, String25, String11]
        setNormalizeNestedFields(normalizeNestedFields);
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(indexPlan()
                            .where(indexName("ind"))
                            .and(RecordQueryPlanMatchers.scanComparisons(range("[EQUALS 1, EQUALS $__in_path__0]")))
                    ).where(inValuesList(equalsObject(ls))));
            assertEquals(1075889283, plan.planHash(PlanHashable.CURRENT_LEGACY));
            if (normalizeNestedFields) {
                assertEquals(1870256531, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(1864715405, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("ind"))
                                            .and(scanComparisons(equalities(exactly(equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)), anyValueComparison()))))))
                            ).where(inValuesList(equalsObject(ImmutableList.of("String6", "String1", "String25", "String11"))))));
            assertEquals(-932896977, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(771072275, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        queryRecordsWithHeader(recordMetaDataHook, plan, cursor ->
                        assertEquals(asList( "_56", "_6", "_1", "_51", "_11", "_61"),
                                cursor.map(TestRecordsWithHeaderProto.MyRecord.Builder::getStrValue).asList().get()),
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that a query with multiple INs is translated into an index scan within multiple IN joins.
     */
    @ParameterizedTest(name = "testMultipleInQueryIndex[normalizeNestedFields={0}]")
    @DualPlannerTest
    @BooleanSource
    void testMultipleInQueryIndex(boolean normalizeNestedFields) throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, rec) -> {
            rec.setStrValue("_" + i);
            rec.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });
        List<Long> longList = asList(1L, 4L);
        List<String> stringList = asList("String6", "String25", "String1", "String34");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").in(longList),
                        Query.field("path").in(stringList))))
                .build();

        // Index(ind [EQUALS $__in_rec_no__0, EQUALS $__in_path__1]) WHERE __in_path__1 IN [String6, String25, String1, String34] WHERE __in_rec_no__0 IN [1, 4]
        setNormalizeNestedFields(normalizeNestedFields);
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<RecordQueryIndexPlan> indexPlanMatcher =
                    indexPlan()
                            .where(indexName("ind"))
                            .and(scanComparisons(range("[EQUALS $__in_rec_no__0, EQUALS $__in_path__1]")));

            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            inValuesJoinPlan(indexPlanMatcher).where(inValuesList(equalsObject(stringList)))
                    ).where(inValuesList(equalsObject(longList))
                    ).or(inValuesJoinPlan(
                            inValuesJoinPlan(indexPlanMatcher).where(inValuesList(equalsObject(longList)))
                    ).where(inValuesList(equalsObject(stringList)))));

            assertEquals(-1869764109, plan.planHash(PlanHashable.CURRENT_LEGACY));
            if (normalizeNestedFields) {
                assertEquals(-1019951714, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(1234840472, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan()
                                                            .where(indexName("ind"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison(), anyValueComparison())))))))
                                            .where(inValuesList(equalsObject(longList))))
                            .where(inValuesList(equalsObject(stringList)))));

            assertEquals(1750857420, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-558676321, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        queryRecordsWithHeader(recordMetaDataHook, plan, cursor ->
                        assertEquals(asList("_56", "_6", "_1", "_51", "_34", "_84"),
                                cursor.map(TestRecordsWithHeaderProto.MyRecord.Builder::getStrValue).asList().get()),
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that a query with multiple INs is translated into an index scan within multiple IN joins, when the query
     * sort order is compatible with the nesting of the IN joins.
     */
    @ParameterizedTest(name = "testInWithNesting[normalizeNestedFields={0}]" )
    @DualPlannerTest
    @BooleanSource
    void testMultipleInQueryIndexSorted(boolean normalizeNestedFields) throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, rec) -> {
            rec.setStrValue("_" + i);
            rec.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("path").in(asList("String6", "String25", "String1", "String34")),
                        Query.field("rec_no").in(asList(4L, 1L)))))
                .setSort(field("header").nest(field("rec_no"), field("path")))
                .build();

        // Index(ind [EQUALS $__in_rec_no__1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String1, String25, String34, String6] SORTED WHERE __in_rec_no__1 IN [1, 4] SORTED
        setNormalizeNestedFields(normalizeNestedFields);
        RecordQueryPlan plan = planQuery(query);
        List<String> sortedStringList = asList("String1", "String25", "String34", "String6");
        List<Long> sortedLongList = asList(1L, 4L);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_rec_no__1, EQUALS $__in_path__0]")))
                            ).where(inValuesList(equalsObject(sortedStringList)))
                    ).where(inValuesList(equalsObject(sortedLongList))));
            assertEquals(303286809, plan.planHash(PlanHashable.CURRENT_LEGACY));
            if (normalizeNestedFields) {
                assertEquals(378183994, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(-1661991116, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan()
                                                            .where(indexName("ind"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison(), anyValueComparison())))))))
                                            .where(inValuesList(equalsObject(sortedStringList))))
                                    .where(inValuesList(equalsObject(sortedLongList)))));
            assertEquals(38185226, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1650923249, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        queryRecordsWithHeader(recordMetaDataHook, plan, cursor ->
                        assertEquals(asList("1:String1", "1:String1", "1:String6", "1:String6", "4:String34", "4:String34"),
                                cursor.map(m -> m.getHeader().getRecNo() + ":" + m.getHeader().getPath()).asList().get()),
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that an IN join is executed correctly when the number of records to retrieve is limited.
     */
    @ParameterizedTest(name = "testInWithLimit[normalizeNestedFields={0}]")
    @DualPlannerTest
    @BooleanSource
    void testInWithLimit(boolean normalizeNestedFields) throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, rec) -> {
            rec.setStrValue("_" + i);
            rec.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });

        List<String> ls = asList("String6", "String1", "String25", "String11");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").equalsValue(1L),
                        Query.field("path").in(ls))))
                .build();

        // Index(ind [EQUALS 1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String6, String1, String25, String11]
        setNormalizeNestedFields(normalizeNestedFields);
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS 1, EQUALS $__in_path__0]")))
                    ).where(inValuesList(equalsObject(ls))));
            assertEquals(1075889283, plan.planHash(PlanHashable.CURRENT_LEGACY));
            if (normalizeNestedFields) {
                assertEquals(1870256531, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(1864715405, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan()
                                                            .where(indexName("ind"))
                                                            .and(scanComparisons(equalities(exactly(equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)), anyValueComparison())))))))
                                            .where(inValuesList(equalsObject(ls)))));
            assertEquals(-932896977, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(771072275, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        queryRecordsWithHeader(recordMetaDataHook, plan, null, 3, cursor ->
                        assertEquals(asList( "_56", "_6", "_1"),
                                cursor.map(TestRecordsWithHeaderProto.MyRecord.Builder::getStrValue).asList().get()),
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that an IN join is executed correctly when continuations are used.
     */
    @ParameterizedTest(name = "testInWithContinuation[normalizeNestedFields={0}]")
    @DualPlannerTest
    @BooleanSource
    void testInWithContinuation(boolean normalizeNestedFields) throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, rec) -> {
            rec.setStrValue("_" + i);
            rec.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });

        List<String> ls = asList("String1", "String6", "String25", "String11");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").equalsValue(1L),
                        Query.field("path").in(ls))))
                .build();

        // Index(ind [EQUALS 1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String1, String6, String25, String11]
        setNormalizeNestedFields(normalizeNestedFields);
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS 1, EQUALS $__in_path__0]")))
                    ).where(inValuesList(equalsObject(ls))));
            assertEquals(1075745133, plan.planHash(PlanHashable.CURRENT_LEGACY));
            if (normalizeNestedFields) {
                assertEquals(1870112381, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(1864571255, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan()
                                                    .where(indexName("ind"))
                                                    .and(scanComparisons(equalities(exactly(equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)), anyValueComparison())))))))
                                    .where(inValuesList(equalsObject(ls)))));
            assertEquals(-964177527, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(766603625, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        // result: [ "_1", "_51", "_56", "_6", "_11", "_61"]
        final Holder<byte[]> continuation = new Holder<>();
        queryRecordsWithHeader(recordMetaDataHook, plan, null, 10,
                cursor -> {
                    RecordCursorResult<TestRecordsWithHeaderProto.MyRecord.Builder> result = cursor.getNext();
                    assertEquals("_1", Objects.requireNonNull(result.get()).getStrValue());
                    continuation.value = result.getContinuation().toBytes();
                },
                TestHelpers::assertDiscardedNone);
        queryRecordsWithHeader(recordMetaDataHook, planQuery(query),
                continuation.value, 10,
                cursor -> {
                    RecordCursorResult<TestRecordsWithHeaderProto.MyRecord.Builder> result = cursor.getNext();
                    assertEquals("_51", Objects.requireNonNull(result.get()).getStrValue());
                    result = cursor.getNext();
                    assertEquals("_56", Objects.requireNonNull(result.get()).getStrValue());
                    continuation.value = result.getContinuation().toBytes();
                },
                TestHelpers::assertDiscardedNone);
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").equalsValue(1L),
                        Query.field("path").in(asList("String6", "String11")))))
                .build();
        // we miss _6
        // Note, Since we have two equals operands, the continuation ends up being relative to that
        // and is just the id, so we want the id of the continuation point from before ("_56") to be greater than the
        // first id of the new continuation ("_11")
        queryRecordsWithHeader(recordMetaDataHook,
                planQuery(query2),
                continuation.value, 10,
                cursor -> {
                    RecordCursorResult<TestRecordsWithHeaderProto.MyRecord.Builder> result = cursor.getNext();
                    assertEquals("_11", Objects.requireNonNull(result.get()).getStrValue());
                    result = cursor.getNext();
                    assertEquals("_61", Objects.requireNonNull(result.get()).getStrValue());
                    result = cursor.getNext();
                    assertFalse(result.hasNext());
                },
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that one-of-them queries work with IN.
     */
    @Test
    void testOneOfThemIn() throws Exception {
        RecordMetaDataHook recordMetaDataHook = metadata ->
                metadata.addIndex("MySimpleRecord", "ind", field("repeater", FanType.FanOut));
        setupSimpleRecordStore(recordMetaDataHook,
                (i, builder) -> builder.setRecNo(i).addAllRepeater(Arrays.asList(10 + i % 4, 20 + i % 4)));
        List<Integer> ls = Arrays.asList(13, 22);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("repeater").oneOfThem().in(ls))
                .build();

        // Index(ind [EQUALS $__in_repeater__0]) | UnorderedPrimaryKeyDistinct() WHERE __in_repeater__0 IN [13, 22]
        RecordQueryPlan plan = planQuery(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_repeater__0]"))))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(503365581, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(77841121, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertEquals(50, querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getRecNo() % 4, anyOf(is(3L), is(2L))),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that one-of-them queries work with IN (with binding).
     */
    @Test
    void testOneOfThemInParameter() throws Exception {
        RecordMetaDataHook recordMetaDataHook = metadata ->
                metadata.addIndex("MySimpleRecord", "ind", field("repeater", FanType.FanOut));
        setupSimpleRecordStore(recordMetaDataHook,
                (i, builder) -> builder.setRecNo(i).addAllRepeater(Arrays.asList(10 + i % 4, 20 + i % 4)));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("repeater").oneOfThem().in("values"))
                .build();

        // Index(ind [EQUALS $__in_repeater__0]) | UnorderedPrimaryKeyDistinct() WHERE __in_repeater__0 IN $values
        RecordQueryPlan plan = planQuery(query);
        assertMatchesExactly(plan,
                inParameterJoinPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_repeater__0]"))))
                ).where(RecordQueryPlanMatchers.inParameter(equalsObject("values"))));
        assertEquals(-320448635, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(604626720, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertEquals(50, querySimpleRecordStore(recordMetaDataHook, plan,
                () -> EvaluationContext.forBinding("values", Arrays.asList(13L, 11L)),
                rec -> assertThat(rec.getRecNo() % 4, anyOf(is(3L), is(1L))),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that one-of-them queries work with IN when sorted on the repeated field.
     */
    @Test
    void testOneOfThemInSorted() throws Exception {
        RecordMetaDataHook recordMetaDataHook = metadata ->
                metadata.addIndex("MySimpleRecord", "ind", field("repeater", FanType.FanOut));
        setupSimpleRecordStore(recordMetaDataHook,
                (i, builder) -> builder.setRecNo(i).addAllRepeater(Arrays.asList(10 + i % 4, 20 + i % 4)));
        List<Integer> ls = Arrays.asList(13, 22);
        List<Integer> reversed = new ArrayList<>(ls);
        Collections.reverse(reversed);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("repeater").oneOfThem().in(reversed))
                .setSort(field("repeater", FanType.FanOut))
                .build();
        RecordQueryPlan plan = planQuery(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_repeater__0]"))))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(503365582, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(50, querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getRecNo() % 4, anyOf(is(3L), is(2L))),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that one-of-them queries work with IN when sorted on a second field, if union is allowed.
     */
    @ParameterizedTest
    @EnumSource(InAsOrUnionMode.class)
    void testOneOfThemInSortBySecondField(InAsOrUnionMode inAsOrMode) throws Exception {
        RecordMetaDataHook recordMetaDataHook = metadata ->
                metadata.addIndex("MySimpleRecord", "ind",
                        concat(field("repeater", FanType.FanOut), field("str_value_indexed")));
        setupSimpleRecordStore(recordMetaDataHook,
                (i, builder) -> builder.setRecNo(i)
                        .setStrValueIndexed((i & 1) == 1 ? "odd" : "even")
                        .addAllRepeater(Arrays.asList(10 + i % 4, 20 + i % 4)));
        List<Integer> inList = Arrays.asList(13, 22);
        QueryComponent filter = Query.field("repeater").oneOfThem().in(inList);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("str_value_indexed"))
                .build();
        assertTrue(planner instanceof RecordQueryPlanner);
        planner.setConfiguration(inAsOrMode.configure(planner.getConfiguration().asBuilder())
                .build());
        RecordQueryPlan plan = planQuery(query);
        // ∪(__in_repeater__0 IN [13, 22]) Index(ind [EQUALS $__in_repeater__0]) | UnorderedPrimaryKeyDistinct()
        // Index(MySimpleRecord$str_value_indexed <,>) | one of repeater IN [13, 22]
        boolean asUnion = inAsOrMode == InAsOrUnionMode.AS_UNION;
        assertMatchesExactly(plan,
                asUnion ?
                unorderedPrimaryKeyDistinctPlan(
                        inUnionOnExpressionPlan(indexPlan().where(indexName("ind"))
                                .and(scanComparisons(range("[EQUALS $__in_repeater__0]"))))
                                .where(inUnionComparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))
                                .and(inUnionValuesSources(exactly(inUnionInValues(equalsObject(inList))
                                        .and(inUnionBindingName("__in_repeater__0")))))) :
                filterPlan(indexPlan()
                        .where(indexName("MySimpleRecord$str_value_indexed"))
                        .and(scanComparisons(unbounded()))
                ).where(queryComponents(exactly(equalsObject(filter)))));
        assertEquals(asUnion ? 1408337059 : 324839336, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(50, querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getRecNo() % 4, anyOf(is(3L), is(2L))),
                asUnion ?
                TestHelpers::assertDiscardedNone :
                context -> TestHelpers.assertDiscardedExactly(50, context)));
    }

    /**
     * Verify that IN works with grouped rank indexes.
     */
    @Test
    void testRecordFunctionInGrouped() throws Exception {
        RecordMetaDataHook recordMetaDataHook = metadata ->
                metadata.addIndex("MySimpleRecord", new Index("rank_by_string", field("num_value_2").groupBy(field("str_value_indexed")),
                        IndexTypes.RANK));
        setupSimpleRecordStore(recordMetaDataHook,
                (i, builder) -> builder.setRecNo(i).setStrValueIndexed("str" + i % 4).setNumValue2(i + 100));

        List<Long> ls = Arrays.asList(1L, 3L, 5L);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("str0"),
                        Query.rank(Key.Expressions.field("num_value_2")
                                .groupBy(Key.Expressions.field("str_value_indexed")))
                                .in(ls)))
                .build();

        // Index(rank_by_string [EQUALS str0, EQUALS $__in_rank__0] BY_RANK) WHERE __in_rank__0 IN [1, 3, 5]
        RecordQueryPlan plan = planQuery(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        indexPlan().where(indexName("rank_by_string")).and(RecordQueryPlanMatchers.indexScanType(IndexScanType.BY_RANK))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(-1804746094, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(268875332, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        List<Long> recNos = new ArrayList<>();
        querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                rec -> recNos.add(rec.getRecNo()),
                TestHelpers::assertDiscardedNone);
        assertEquals(Arrays.asList(4L, 12L, 20L), recNos);
    }

    /**
     * Verify that IN works with ungrouped rank indexes.
     */
    @Test
    void testRecordFunctionInUngrouped() throws Exception {
        RecordMetaDataHook recordMetaDataHook = metadata ->
                metadata.addIndex("MySimpleRecord", new Index("rank", field("num_value_2").ungrouped(),
                        IndexTypes.RANK));
        setupSimpleRecordStore(recordMetaDataHook,
                (i, builder) -> builder.setRecNo(i).setStrValueIndexed("str" + i % 4).setNumValue2(i + 100));

        List<Long> ls = Arrays.asList(1L, 3L, 5L);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.rank("num_value_2").in(ls))
                .build();

        // Index(rank [EQUALS $__in_rank__0] BY_RANK) WHERE __in_rank__0 IN [1, 3, 5]
        RecordQueryPlan plan = planQuery(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        indexPlan().where(indexName("rank")).and(RecordQueryPlanMatchers.indexScanType(IndexScanType.BY_RANK))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(1334377108, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(1071050249, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        List<Long> recNos = new ArrayList<>();
        querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                rec -> recNos.add(rec.getRecNo()),
                TestHelpers::assertDiscardedNone);
        assertEquals(Arrays.asList(1L, 3L, 5L), recNos);
    }

    /**
     * Verify that IN queries can be planned using index scans, then used in a UNION to implement OR with an inequality
     * on the same field, and that the resulting union will be ordered by that field.
     */
    @DualPlannerTest
    void testInQueryOr() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_unique").in(Arrays.asList(903, 905, 901)),
                        Query.field("num_value_unique").greaterThan(950)))
                .build();

        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_unique [EQUALS $__in_num_value_unique__0]) WHERE __in_num_value_unique__0 IN [901, 903, 905] SORTED ∪[Field { 'num_value_unique' None}, Field { 'rec_no' None}] Index(MySimpleRecord$num_value_unique ([950],>)
            assertMatchesExactly(plan,
                    RecordQueryPlanMatchers.unionOnExpressionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_unique")),
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("[EQUALS $__in_num_value_unique__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))))
                            .where(comparisonKey(concat(field("num_value_unique"), primaryKey("MySimpleRecord")))));
            assertEquals(1116661716, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-924293640, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            RecordQueryPlanMatchers.unionOnValuesPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique")))),
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))))
                                    .where(comparisonKeyValues(exactly(fieldValueWithFieldNames("num_value_unique"), fieldValueWithFieldNames("rec_no"))))));
            assertEquals(1341091029, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-29700805, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        assertEquals(53, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> assertThat(rec.getNumValueUnique(), anyOf(is(901), is(903), is(905), greaterThan(950))),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that IN queries can be planned using index scans, then used in a UNION to implement an OR with IN whose
     * elements overlap, and that the union with that comparison key deduplicates the records in the overlap.
     */
    @DualPlannerTest
    void testInQueryOrOverlap() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_unique").in(Arrays.asList(903, 905, 901)),
                        Query.field("num_value_unique").in(Arrays.asList(906, 905, 904))))
                .build();

        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_unique [EQUALS $__in_num_value_unique__0]) WHERE __in_num_value_unique__0 IN [901, 903, 905] SORTED ∪[Field { 'num_value_unique' None}, Field { 'rec_no' None}] Index(MySimpleRecord$num_value_unique [EQUALS $__in_num_value_unique__0]) WHERE __in_num_value_unique__0 IN [904, 905, 906] SORTED
            // Ordinary equality comparisons would be ordered just by the primary key so that would be the union comparison key.
            // Must compare the IN field here; they are ordered, but not trivially (same value for each).
            assertMatchesExactly(plan,
                    RecordQueryPlanMatchers.unionOnExpressionPlan(
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("[EQUALS $__in_num_value_unique__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))),
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("[EQUALS $__in_num_value_unique__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(904, 905, 906))))));
            assertEquals(218263868, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(468995802, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            RecordQueryPlanMatchers.unionOnValuesPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))),
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(904, 905, 906)))))
                                    .where(comparisonKeyValues(exactly(fieldValueWithFieldNames("num_value_unique"), fieldValueWithFieldNames("rec_no"))))));
            assertEquals(1914194235, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1777494852, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        Set<Long> dupes = new HashSet<>();
        assertEquals(5, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> {
                    assertTrue(dupes.add(rec.getRecNo()), "should not have duplicated records");
                    assertThat(rec.getNumValueUnique(), anyOf(is(901), is(903), is(904), is(905), is(906)));
                }, context -> TestHelpers.assertDiscardedAtMost(1, context)));
    }

    /**
     * Verify that an IN requires an unordered union due to incompatible ordering.
     */
    @DualPlannerTest
    void testInQueryOrDifferentCondition() throws Exception {
        complexQuerySetup(NO_HOOK);
        planner.setConfiguration(planner.getConfiguration().asBuilder().setAttemptFailedInJoinAsUnionMaxSize(10).build());

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_unique").lessThan(910),
                        Query.and(
                                Query.field("num_value_unique").greaterThan(990),
                                Query.field("num_value_2").in(Arrays.asList(2, 0)))))
                .build();
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                            unionOnExpressionPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([null],[910])"))),
                                    filterPlan(
                                            indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>"))))
                                            .where(queryComponents(only(equalsObject(Query.field("num_value_2").in(Arrays.asList(2, 0))))))));
            assertEquals(224679143, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(2117979117, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    RecordQueryPlanMatchers.unionOnValuesPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([null],[910])"))),
                            inUnionOnValuesPlan(
                                    predicatesFilterPlan(indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>"))))
                                            .where(predicates(valuePredicate(fieldValueWithFieldNames("num_value_2"), anyValueComparison()))))
                                    .where(comparisonKeyValues(exactly(fieldValueWithFieldNames("num_value_unique"), fieldValueWithFieldNames("rec_no"), fieldValueWithFieldNames("num_value_2"))))));
            assertEquals(1521186153, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-455093906, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(16, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> {
                    assertThat(rec.getNumValueUnique(), anyOf(lessThan(910), greaterThan(990)));
                    if (rec.getNumValue3Indexed() > 990) {
                        assertThat(rec.getNumValue2(), anyOf(is(2), is(0)));
                    }
                }, context -> TestHelpers.assertDiscardedAtMost(13, context)));
    }

    /**
     * Verify that a complex query involving IN, AND, and OR is planned using a union of scans and joins on a
     * multi-field index, where the left subset has equality and the final field has an IN plus inequality on that same
     * field.
     */
    @DualPlannerTest
    void testInQueryOrCompound() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_2").equalsValue(0),
                        Query.or(
                                Query.field("num_value_3_indexed").in(Arrays.asList(1, 3)),
                                Query.field("num_value_3_indexed").greaterThanOrEquals(4))))
                .build();

        // Index(multi_index [EQUALS odd, EQUALS 0, EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN [1, 3] SORTED ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Index(multi_index [[odd, 0, 4],[odd, 0]])
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    RecordQueryPlanMatchers.unionOnExpressionPlan(
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[EQUALS odd, EQUALS 0, EQUALS $__in_num_value_3_indexed__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(1, 3)))),
                            indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[odd, 0, 4],[odd, 0]]")))));
            assertEquals(468569345, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1312398381, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            RecordQueryPlanMatchers.unionOnValuesPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("multi_index")))),
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("multi_index"))
                                                            .and(scanComparisons(equalities(exactly(
                                                                    equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "odd")),
                                                                    equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 0)),
                                                                    anyValueComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(1, 3)))))
                                    .where(comparisonKeyValues(exactly(fieldValueWithFieldNames("num_value_3_indexed"), fieldValueWithFieldNames("rec_no"))))));
            assertEquals(-761192240, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(876045548, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        assertEquals(3 + 4 + 4, querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                rec -> {
                    assertThat(rec.getStrValueIndexed(), is("odd"));
                    assertThat(rec.getNumValue2(), is(0));
                    assertThat(rec.getNumValue3Indexed(), anyOf(is(1), is(3), greaterThanOrEqualTo(4)));
                }, TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify an IN clause prevents index usage because the IN loop is not compatible with index ordering in the old
     * planner but causes an IN-UNION to be created in the new planner.
     */
    @DualPlannerTest
    void testInQueryOrMultipleIndexes() throws Exception {
        complexQuerySetup(NO_HOOK);
        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .build());

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").in(Arrays.asList(1, 3))))
                .build();

        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            // Two ordinary equals single-column index scans would be compatible on the following primary key, but
            // the IN loop inside one branch prevents that here. A regular filter would not.
            assertMatchesExactly(plan,
                    unorderedPrimaryKeyDistinctPlan(
                            unorderedUnionPlan(
                                    indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[odd],[odd]]"))),
                                    inValuesJoinPlan(
                                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                                    ).where(inValuesList(equalsObject(Arrays.asList(1, 3)))))));
            assertEquals(-1310248168, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1826025907, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            RecordQueryPlanMatchers.unionOnValuesPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                                    inUnionOnValuesPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))
                                                            .and(scanComparisons(equalities(exactly(anyValueComparison())))))))
                                            .where(comparisonKeyValues(exactly(fieldValueWithFieldNames("rec_no"), fieldValueWithFieldNames("num_value_3_indexed"))))
                                            .and(inUnionValuesSources(exactly(inUnionInValues(equalsObject(ImmutableList.of(1, 3)))))))));
            assertEquals(2084726425, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-865068999, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        Set<Long> dupes = new HashSet<>();
        assertEquals(50 + 10 + 10, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                rec -> {
                    assertTrue(dupes.add(rec.getRecNo()), "should not have duplicated records");
                    assertTrue(rec.getStrValueIndexed().equals("odd") ||
                               rec.getNumValue3Indexed() == 1 ||
                               rec.getNumValue3Indexed() == 3);
                }, context -> TestHelpers.assertDiscardedAtMost(20, context)));
    }

    /**
     * Verify that enum field indexes are used to implement IN clauses.
     */
    @DualPlannerTest
    void enumIn() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder type = metaData.getRecordType("MyShapeRecord");
            metaData.addIndex(type, new Index("color", field("color")));
        };
        setupEnumShapes(hook);

        final var redBlue =
                asList(TestRecordsEnumProto.MyShapeRecord.Color.RED,
                        TestRecordsEnumProto.MyShapeRecord.Color.BLUE);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyShapeRecord")
                .setFilter(Query.field("color").in(redBlue))
                .build();

        // Index(color [EQUALS $__in_color__0]) WHERE __in_color__0 IN [RED, BLUE]
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    descendantPlans(indexPlan().where(indexName("color"))));
            assertFalse(plan.hasRecordScan(), "should not use record scan");
            assertEquals(-520431454, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1447363737, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("color"))
                                                            .and(scanComparisons(equalities(exactly(
                                                                    anyValueComparison())))))))
                                            .where(inValuesList(equalsInList(redBlue)))));
            assertFalse(plan.hasRecordScan(), "should not use record scan");
            assertEquals(-349856378, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(421925187, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openEnumRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsEnumProto.MyShapeRecord.Builder shapeRec = TestRecordsEnumProto.MyShapeRecord.newBuilder();
                    shapeRec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertThat(shapeRec.getColor(), is(oneOf(TestRecordsEnumProto.MyShapeRecord.Color.RED, TestRecordsEnumProto.MyShapeRecord.Color.BLUE)));
                    i++;
                }
            }
            assertEquals(18, i);
            TestHelpers.assertDiscardedNone(context);
        }
    }

    /**
     * Verify that an IN with an empty list returns nothing.
     */
    @DualPlannerTest
    void testInQueryEmptyList() throws Exception {
        complexQuerySetup(NO_HOOK);
        List<Integer> ls = Collections.emptyList();
        final QueryComponent filter = Query.field("num_value_2").in(ls);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .build();

        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN []
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(selfOrDescendantPlans(scanPlan().where(scanComparisons(unbounded())))).where(queryComponents(only(equalsObject(filter)))));
            assertEquals(-1139440895, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1691830390, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(selfOrDescendantPlans(scanPlan().and(scanComparisons(unbounded()))))
                            .where(predicates(valuePredicate(fieldValueWithFieldNames("num_value_2"), new Comparisons.ListComparison(Comparisons.Type.IN, ImmutableList.of())))));
            assertEquals(738017460, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-2063024914, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty, (rec) -> {
        }));
    }

    @Test
    void testInQueryWithNonSargable() throws Exception {
        final Index numValue2RepeaterIndex = new Index("numValue2NumValue3", concatenateFields("num_value_2", "num_value_3_indexed"));
        final RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", numValue2RepeaterIndex);
            metaDataBuilder.removeIndex("MySimpleRecord$num_value_3_indexed");
        };
        complexQuerySetup(hook);

        final var inFilter = Query.field("num_value_3_indexed").in(List.of(1, 3, 5, 7, 9));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").greaterThan(42),
                        inFilter
                ))
                .build();

        RecordQueryPlan plan = planQuery(query);

        // Make sure we do not plan the IN as a JOIN and that we don't fall back to a primary scan but use an
        // index.
        assertMatchesExactly(plan,
                fetchFromPartialRecordPlan(
                        filterPlan(
                                coveringIndexPlan()
                                        .where(indexPlanOf(indexPlan()
                                                .where(indexName("numValue2NumValue3"))
                                                .and(scanComparisons(range("([42],>"))))))
                                                .where(queryComponents(only(equalsObject(inFilter))))));
        assertEquals(1306759254, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(1359269242, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @ParameterizedTest(name = "testInQueryWithNonSargableReplan [shouldReplan = {0}]")
    @BooleanSource
    void testInQueryWithNonSargableReplan(final boolean shouldReplan) throws Exception {
        complexQuerySetup(NO_HOOK);

        planner.setConfiguration(planner.getConfiguration().asBuilder().setMaxNumReplansForInToJoin(shouldReplan ? 1 : 0).build());
        final var inListNumValue3Indexed = List.of(1, 3, 5, 7, 9);
        final var inListNumValue2 = List.of(2, 4, 6, 8);
        final var filter = Query.and(
                Query.field("num_value_2").in(inListNumValue2),
                Query.field("num_value_3_indexed").in(inListNumValue3Indexed)
        );

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .build();

        RecordQueryPlan plan = planQuery(query);
        if (shouldReplan) {
            // With replanning we plan one IN as a JOIN over an index using that IN-bound equaltiy predicate
            // and the other one as a residual IN.
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            filterPlan(
                                    indexPlan()
                                            .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                            .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__1]"))))
                                    .where(queryComponents(only(equalsObject(Query.field("num_value_2").in(inListNumValue2)))))
                    ).where(inValuesList(equalsObject(inListNumValue3Indexed))));
            assertEquals(-440990190, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(529154549, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            // Without any replanning we plan with both IN predicates in a residual filter.
            assertMatchesExactly(plan,
                    filterPlan(
                            typeFilterPlan(
                                    scanPlan().where(scanComparisons(unbounded()))))
                            .where(queryComponents(only(equalsObject(filter)))));
            assertEquals(-898685565, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1176593054, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }
}
