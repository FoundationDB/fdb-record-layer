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
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.anyParameterComparison;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.equalities;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.temp.matchers.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.comparisonKey;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inParameterJoinPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inUnionBindingName;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inUnionComparisonKey;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inUnionInParameter;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inUnionInValues;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inUnionPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inUnionValuesSources;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inValuesJoinPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.inValuesList;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.selfOrDescendantPlans;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.unionPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.unorderedUnionPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ValueMatchers.fieldValue;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
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
        RecordQueryPlan plan = planner.plan(query);
        
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(queryComponents(exactly(equalsObject(filter)))));

            assertEquals(-1139367278, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1907300063, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1694772440, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                            .where(predicates(valuePredicate(fieldValue("num_value_2"), new Comparisons.ListComparison(Comparisons.Type.IN, ImmutableList.of(0, 2))))));

            assertEquals(997592219, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1107686929, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-895159306, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(67, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), anyOf(is(0), is(2))),
                context -> assertDiscardedAtMost(33, context)));
    }

    /**
     * Verify that an IN (with parameter) without an index is implemented as a filter on a scan.
     */
    @Test
    void testInQueryNoIndexWithParameter() throws Exception {
        complexQuerySetup(NO_HOOK);
        final QueryComponent filter = Query.field("num_value_2").in("valuesThree");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)    // num_value_2 is i%3
                .build();

        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN $valuesThree
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                filterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                        .where(queryComponents(exactly(equalsObject(filter)))));
        assertEquals(-1677754212, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-192829430, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-192829430, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(33, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valuesThree", asList(1, 3)),
                record -> assertThat(record.getNumValue2(), anyOf(is(1), is(3))),
                context -> assertDiscardedAtMost(67, context)));
    }

    /**
     * Verify that an IN with an index is implemented as an index scan, with an IN join.
     */
    @DualPlannerTest
    void testInQueryIndex() throws Exception {
        complexQuerySetup(NO_HOOK);
        List<Integer> ls = asList(1, 2, 4);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in(ls))
                .setSort(field("num_value_3_indexed"))
                .build();

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .build());

        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN [1, 2, 4]
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                    ).where(inValuesList(equalsObject(ls))));
            assertEquals(-2004060309, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(571226247, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(571195399, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                    inValuesJoinPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan()
                                            .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                            .and(scanComparisons(equalities(only(anyParameterComparison())))))))
                            .where(inValuesList(equalsObject(ls)))));
            assertEquals(-2068499040, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-992959779, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1998042418, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(60, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> assertThat(record.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
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

        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN $valueThrees
            assertMatchesExactly(plan,
                    inParameterJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                    ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees"))));
            assertEquals(883815022, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(514739864, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(514739864, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(inParameterJoinPlan(
                            coveringIndexPlan().where(indexPlanOf(indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(equalities(only(anyParameterComparison()))))))
                            ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees")))));
            assertEquals(1152354842, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1759971309, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1759971309, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        int count = querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", asList(1, 3, 4)),
                myrec -> assertThat(myrec.getNumValue3Indexed(), anyOf(is(1), is(3), is(4))),
                TestHelpers::assertDiscardedNone);
        assertEquals(60, count);
    }

    /**
     * Verify that an IN (with parameter) with an index is implemented as an index scan, with an IN union in the presence
     * of other equality-bound index parts.
     */
    @DualPlannerTest
    void testInQueryParameter2() throws Exception {
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
                .setSort(Key.Expressions.field("num_value_unique"))
                .build();

        planner.setConfiguration(InAsOrUnionMode.AS_UNION.configure(planner.getConfiguration().asBuilder())
                .build());

        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inUnionPlan(
                            indexPlan()
                                    .where(indexName("multi_index"))
                                    .and(scanComparisons(range("[EQUALS $p, EQUALS $__in_num_value_2__0]")))
                    ).where(inUnionValuesSources(exactly(RecordQueryPlanMatchers.inUnionInParameter(equalsObject("valueThrees"))))));
            assertEquals(1360223246, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2045945193, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2045945193, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inUnionPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("multi_index"))
                                            .and(scanComparisons(equalities(exactly(anyParameterComparison(), anyParameterComparison()))))))
                            ).where(RecordQueryPlanMatchers.inUnionValuesSources(exactly(inUnionInParameter(equalsObject("valueThrees")))))));
            assertEquals(1947663752, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1788373323, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1788373323, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that an in with a bad parameter plans correctly but fails upon execution.
     */
    @Test
    void testInQueryParameterBad() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in("valueThrees"))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN $valueThrees
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                inParameterJoinPlan(
                        indexPlan()
                                .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                ).where(RecordQueryPlanMatchers.inParameter(equalsObject("valueThrees"))));
        assertEquals(883815022, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(514739864, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(514739864, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", Collections.emptyList()),
                myrec -> fail("There should be no results")));
        assertThrows(RecordCoreException.class, TestHelpers.toCallable(() ->
                assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                        EvaluationContext::empty, /* no binding for valueThrees */
                        myrec -> fail("There should be no results")))));
        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", null), /* no binding for valueThrees */
                myrec -> fail("There should be no results")));
    }


    /**
     * Verify that NOT IN is planned correctly, and fails if no binding is provided.
     */
    @Test
    void testNotInQueryParameterBad() throws Exception {
        complexQuerySetup(NO_HOOK);
        final QueryComponent filter = Query.not(Query.field("num_value_3_indexed").in("valueThrees"));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .build();

        // Scan(<,>) | [MySimpleRecord] | Not(num_value_3_indexed IN $valueThrees)
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                filterPlan(descendantPlans(scanPlan().where(scanComparisons(unbounded()))))
                .where(queryComponents(exactly(equalsObject(filter)))));
        assertEquals(1667070490, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1804602975, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1804602975, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(100, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", Collections.emptyList()),
                myrec -> {
                },
                TestHelpers::assertDiscardedNone));
        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan,
                () -> EvaluationContext.forBinding("valueThrees", null), /* no binding for valueThrees */
                myrec -> fail("There should be no results")));
    }

    /**
     * Verify that an IN against an unsorted list with an index is implemented as an index scan, with an IN join on
     * a sorted copy of the list.
     */
    @DualPlannerTest
    void testInQueryIndexSorted() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").in(asList(1, 4, 2)))
                .setSort(field("num_value_3_indexed"))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN [1, 2, 4] SORTED
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]")))
                    ).where(inValuesList(equalsObject(asList(1, 2, 4)))));
            assertEquals(-2004060309, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(571226247, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(571195399, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("MySimpleRecord$num_value_3_indexed"))
                                            .and(scanComparisons(equalities(only(anyParameterComparison()))))))
                                    ).where(inValuesList(equalsObject(asList(1, 2, 4))))));
            assertEquals(-2068499040, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-992959779, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1998042418, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(60, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> assertThat(record.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that an IN against an unsorted list with an index is not implemented as an IN JOIN when the query sort is
     * not by the field with an IN filter.
     */
    @DualPlannerTest
    void testInQueryIndexSortedDifferently() throws Exception {
        complexQuerySetup(NO_HOOK);
        final QueryComponent filter = Query.field("num_value_3_indexed").in(asList(1, 4, 2));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("str_value_indexed"))
                .build();

        // Index(MySimpleRecord$str_value_indexed <,>) | num_value_3_indexed IN [1, 4, 2]
        RecordQueryPlan plan = planner.plan(query);
        // IN join is cancelled on account of incompatible sorting.
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(selfOrDescendantPlans(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(unbounded()))))
                            .where(queryComponents(exactly(equalsObject(filter)))));
            assertEquals(1775865786, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(972267, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(212572525, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(selfOrDescendantPlans(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(unbounded()))))
                            .where(predicates(valuePredicate(fieldValue("num_value_3_indexed"), new Comparisons.ListComparison(Comparisons.Type.IN, ImmutableList.of(1, 4, 2))))));
            assertEquals(1470982333, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(800585401, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1012185659, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(60, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> assertThat(record.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                context -> TestHelpers.assertDiscardedAtMost(40, context)));
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

        assertTrue(planner instanceof RecordQueryPlanner); // The configuration is planner-specific.
        planner.setConfiguration(inAsOrMode.configure(planner.getConfiguration().asBuilder())
                .build());

        // Index(MySimpleRecord$str_value_indexed <,>) | num_value_3_indexed IN [1, 4, 2]
        // Index(compoundIndex [[1],[1]]) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex [[4],[4]]) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex [[2],[2]])
        // ∪(__in_num_value_3_indexed__0 IN [1, 4, 2]) Index(compoundIndex [EQUALS $__in_num_value_3_indexed__0])
        RecordQueryPlan plan = planner.plan(query);
        if (inAsOrMode == InAsOrUnionMode.AS_OR) {
            assertMatchesExactly(plan,
                    unionPlan(inList.stream().map(number ->
                            indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range(String.format("[[%d],[%d]]", number, number)))))
                            .collect(ImmutableList.toImmutableList()))
                            .where(comparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord")))));
            assertEquals(-1813975352, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1188407258, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2089555085, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else if (inAsOrMode == InAsOrUnionMode.AS_UNION) {
            assertMatchesExactly(plan,
                    inUnionPlan(indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0]"))))
                            .where(inUnionComparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))
                            .and(inUnionValuesSources(exactly(inUnionInValues(equalsObject(inList))
                                    .and(inUnionBindingName("__in_num_value_3_indexed__0"))))));
            assertEquals(406364040, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(494706693, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-628125702, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    filterPlan(indexPlan()
                            .where(indexName("MySimpleRecord$str_value_indexed"))
                            .and(scanComparisons(unbounded()))
                    ).where(queryComponents(exactly(equalsObject(filter)))));

            assertEquals(1775865786, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(972267, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(212572525, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        assertEquals(60, querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                record -> assertThat(record.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                context -> TestHelpers.assertDiscardedAtMost(40, context)));
    }

    /**
     * Verify that an IN query with a sort and range predicate can be implemented as an ordered union of compound indexes
     * that can satisfy the sort once the equality predicates from the IN have been pushed onto the indexes.
     * @see com.apple.foundationdb.record.query.plan.planning.InExtractor#asOr()
     */
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

        assertTrue(planner instanceof RecordQueryPlanner); // The configuration is planner-specific.
        RecordQueryPlanner recordQueryPlanner = (RecordQueryPlanner)planner;
        recordQueryPlanner.setConfiguration(inAsOrMode.configure(recordQueryPlanner.getConfiguration().asBuilder())
                .build());

        // Index(MySimpleRecord$str_value_indexed ([bar],[foo])) | num_value_3_indexed IN [1, 4, 2]
        // Index(compoundIndex ([1, bar],[1, foo])) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex ([4, bar],[4, foo])) ∪[Field { 'str_value_indexed' None}, Field { 'rec_no' None}] Index(compoundIndex ([2, bar],[2, foo]))
        // ∪(__in_num_value_3_indexed__0 IN [1, 4, 2]) Index(compoundIndex [EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]])
        RecordQueryPlan plan = planner.plan(query);
        if (inAsOrMode == InAsOrUnionMode.AS_OR) {
            // IN join is impossible because of incompatible sorting, but we can still plan as an OR on the compound index.
            assertMatchesExactly(plan,
                    unionPlan(
                            inList.stream()
                                    .map(number -> indexPlan().where(indexName("compoundIndex")).and(scanComparisons(range(String.format("([%d, bar],[%d, foo])", number, number)))))
                                    .collect(ImmutableList.toImmutableList()))
                            .where(RecordQueryPlanMatchers.comparisonKey(equalsObject(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))));
            assertEquals(651476052, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-2091774924, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1421992908, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else if (inAsOrMode == InAsOrUnionMode.AS_UNION) {
            assertMatchesExactly(plan,
                    inUnionPlan(indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]]"))))
                            .where(inUnionComparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))
                            .and(inUnionValuesSources(exactly(inUnionInValues(equalsObject(inList))
                                    .and(inUnionBindingName("__in_num_value_3_indexed__0"))))));
            assertEquals(591462908, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(834697335, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1630766853, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    filterPlan(indexPlan()
                            .where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([bar],[foo])")))
                    ).where(queryComponents(only(equalsObject(Query.field("num_value_3_indexed").in(inList))))));
            assertEquals(-1681846586, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(340962909, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-790068626, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        assertEquals(30, querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                record -> assertThat(record.getNumValue3Indexed(), anyOf(is(1), is(2), is(4))),
                context -> { }));
    }

    /**
     * Verify that an IN query over a parameter with a sort keeps filter even if OR is allowed.
     */
    @ParameterizedTest
    @EnumSource(InAsOrUnionMode.class)
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

        assertTrue(planner instanceof RecordQueryPlanner); // The configuration is planner-specific.
        RecordQueryPlanner recordQueryPlanner = (RecordQueryPlanner)planner;
        recordQueryPlanner.setConfiguration(inAsOrMode.configure(recordQueryPlanner.getConfiguration().asBuilder())
                .build());

        // Index(MySimpleRecord$str_value_indexed ([bar],[foo])) | num_value_3_indexed IN $inList
        // ∪(__in_num_value_3_indexed__0 IN $inList) Index(compoundIndex [EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]])
        RecordQueryPlan plan = planner.plan(query);
        if (inAsOrMode == InAsOrUnionMode.AS_UNION) {
            assertMatchesExactly(plan,
                    inUnionPlan(indexPlan().where(indexName("compoundIndex"))
                                    .and(scanComparisons(range("[EQUALS $__in_num_value_3_indexed__0, [GREATER_THAN bar && LESS_THAN foo]]"))))
                            .where(inUnionComparisonKey(concat(field("str_value_indexed"), primaryKey("MySimpleRecord"))))
                            .and(inUnionValuesSources(exactly(inUnionInParameter(equalsObject("inList"))
                                    .and(inUnionBindingName("__in_num_value_3_indexed__0"))))));
            assertEquals(-625336982, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-382102555, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1724734348, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    filterPlan(indexPlan()
                            .where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([bar],[foo])")))
                    ).where(queryComponents(only(equalsObject(Query.field("num_value_3_indexed").in("inList"))))));
            assertEquals(1428066748, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1407869064, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(65237271, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(30, querySimpleRecordStore(hook, plan,
                () -> EvaluationContext.forBinding("inList", asList(1, 3, 4)),
                record -> assertThat(record.getNumValue3Indexed(), anyOf(is(1), is(3), is(4))),
                context -> { }));
        assertEquals(0, querySimpleRecordStore(hook, plan,
                () -> EvaluationContext.forBinding("inList", ImmutableList.of()),
                record -> fail("should not have any records"),
                context -> { }));
        assertEquals(10, querySimpleRecordStore(hook, plan,
                () -> EvaluationContext.forBinding("inList", ImmutableList.of(3)),
                record -> assertThat(record.getNumValue3Indexed(), is(3)),
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
        RecordQueryPlan plan = planner.plan(query);
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
    @DualPlannerTest
    void testInWithNesting() throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, record) -> {
            record.setStrValue("_" + i);
            record.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });

        List<String> ls = asList("String6", "String1", "String25", "String11");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").equalsValue(1L),
                        Query.field("path").in(ls))))
                .build();

        // Index(ind [EQUALS 1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String6, String1, String25, String11]
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(indexPlan()
                            .where(indexName("ind"))
                            .and(RecordQueryPlanMatchers.scanComparisons(range("[EQUALS 1, EQUALS $__in_path__0]")))
                    ).where(inValuesList(equalsObject(ls))));
            assertEquals(1075889283, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1864715405, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-847163347, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan()
                                            .where(indexName("ind"))
                                            .and(scanComparisons(equalities(exactly(equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)), anyParameterComparison()))))))
                            ).where(inValuesList(equalsObject(ImmutableList.of("String6", "String1", "String25", "String11"))))));
            assertEquals(590997643, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-740153893, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1523769992, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        queryRecordsWithHeader(recordMetaDataHook, plan, cursor ->
                        assertEquals(asList( "_56", "_6", "_1", "_51", "_11", "_61"),
                                cursor.map(TestRecordsWithHeaderProto.MyRecord.Builder::getStrValue).asList().get()),
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that a query with multiple INs is translated into an index scan within multiple IN joins.
     */
    @DualPlannerTest
    void testMultipleInQueryIndex() throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, record) -> {
            record.setStrValue("_" + i);
            record.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
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
        RecordQueryPlan plan = planner.plan(query);
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

            assertEquals(-1869764109, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1234840472, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(297055958, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan()
                                                            .where(indexName("ind"))
                                                            .and(scanComparisons(equalities(exactly(anyParameterComparison(), anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(stringList))))
                            .where(inValuesList(equalsObject(longList)))));

            assertEquals(-1217841460, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-2110068961, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1810804415, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
    @DualPlannerTest
    void testMultipleInQueryIndexSorted() throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, record) -> {
            record.setStrValue("_" + i);
            record.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("path").in(asList("String6", "String25", "String1", "String34")),
                        Query.field("rec_no").in(asList(4L, 1L)))))
                .setSort(field("header").nest(field("rec_no"), field("path")))
                .build();

        // Index(ind [EQUALS $__in_rec_no__1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String1, String25, String34, String6] SORTED WHERE __in_rec_no__1 IN [1, 4] SORTED
        RecordQueryPlan plan = planner.plan(query);
        List<String> sortedStringList = asList("String1", "String25", "String34", "String6");
        List<Long> sortedLongList = asList(1L, 4L);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_rec_no__1, EQUALS $__in_path__0]")))
                            ).where(inValuesList(equalsObject(sortedStringList)))
                    ).where(inValuesList(equalsObject(sortedLongList))));
            assertEquals(303286809, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1661991116, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1396696428, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan()
                                                            .where(indexName("ind"))
                                                            .and(scanComparisons(equalities(exactly(anyParameterComparison(), anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(sortedStringList))))
                                    .where(inValuesList(equalsObject(sortedLongList)))));
            assertEquals(457372810, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-30052367, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(959442763, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        queryRecordsWithHeader(recordMetaDataHook, plan, cursor ->
                        assertEquals(asList("1:String1", "1:String1", "1:String6", "1:String6", "4:String34", "4:String34"),
                                cursor.map(m -> m.getHeader().getRecNo() + ":" + m.getHeader().getPath()).asList().get()),
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that an IN join is executed correctly when the number of records to retrieve is limited.
     */
    @DualPlannerTest
    void testInWithLimit() throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, record) -> {
            record.setStrValue("_" + i);
            record.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });

        List<String> ls = asList("String6", "String1", "String25", "String11");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").equalsValue(1L),
                        Query.field("path").in(ls))))
                .build();

        // Index(ind [EQUALS 1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String6, String1, String25, String11]
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS 1, EQUALS $__in_path__0]")))
                    ).where(inValuesList(equalsObject(ls))));
            assertEquals(1075889283, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1864715405, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-847163347, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan()
                                                            .where(indexName("ind"))
                                                            .and(scanComparisons(equalities(exactly(equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)), anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(ls)))));
            assertEquals(590997643, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-740153893, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1523769992, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        queryRecordsWithHeader(recordMetaDataHook, plan, null, 3, cursor ->
                        assertEquals(asList( "_56", "_6", "_1"),
                                cursor.map(TestRecordsWithHeaderProto.MyRecord.Builder::getStrValue).asList().get()),
                TestHelpers::assertDiscardedNone);
    }

    /**
     * Verify that an IN join is executed correctly when continuations are used.
     */
    @DualPlannerTest
    void testInWithContinuation() throws Exception {
        final RecordMetaDataHook recordMetaDataHook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("str_value"));
            metaData.addIndex("MyRecord", "ind", field("header").nest(field("rec_no"), field("path")));
        };

        setupRecordsWithHeader(recordMetaDataHook, (i, record) -> {
            record.setStrValue("_" + i);
            record.getHeaderBuilder().setRecNo(i % 5).setPath("String" + i % 50).setNum(i);
        });

        List<String> ls = asList("String1", "String6", "String25", "String11");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.and(
                        Query.field("rec_no").equalsValue(1L),
                        Query.field("path").in(ls))))
                .build();

        // Index(ind [EQUALS 1, EQUALS $__in_path__0]) WHERE __in_path__0 IN [String1, String6, String25, String11]
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    inValuesJoinPlan(
                            indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS 1, EQUALS $__in_path__0]")))
                    ).where(inValuesList(equalsObject(ls))));
            assertEquals(1075745133, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1864571255, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-847163347, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            inValuesJoinPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan()
                                                    .where(indexName("ind"))
                                                    .and(scanComparisons(equalities(exactly(equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)), anyParameterComparison())))))))
                                    .where(inValuesList(equalsObject(ls)))));
            assertEquals(559717093, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-744622543, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1523769992, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
        queryRecordsWithHeader(recordMetaDataHook, planner.plan(query),
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
                planner.plan(query2),
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
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_repeater__0]"))))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(503365581, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(77841121, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(77839705, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(50, querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                record -> assertThat(record.getRecNo() % 4, anyOf(is(3L), is(2L))),
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
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                inParameterJoinPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_repeater__0]"))))
                ).where(RecordQueryPlanMatchers.inParameter(equalsObject("values"))));
        assertEquals(-320448635, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(604626720, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(604626720, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(50, querySimpleRecordStore(recordMetaDataHook, plan,
                () -> EvaluationContext.forBinding("values", Arrays.asList(13L, 11L)),
                record -> assertThat(record.getRecNo() % 4, anyOf(is(3L), is(1L))),
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
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        unorderedPrimaryKeyDistinctPlan(
                                indexPlan().where(indexName("ind")).and(scanComparisons(range("[EQUALS $__in_repeater__0]"))))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(503365582, plan.planHash());
        assertEquals(50, querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                record -> assertThat(record.getRecNo() % 4, anyOf(is(3L), is(2L))),
                TestHelpers::assertDiscardedNone));
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

        // Index(rank_by_string [EQUALS str0, EQUALS $__in_rank([Field { 'str_value_indexed' None}, Field { 'num_value_2' None}] group 1)__0] BY_RANK) WHERE __in_rank([Field { 'str_value_indexed' None}, Field { 'num_value_2' None}] group 1)__0 IN [1, 3, 5]
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        indexPlan().where(indexName("rank_by_string")).and(RecordQueryPlanMatchers.indexScanType(IndexScanType.BY_RANK))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(-778840248, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1474202802, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(2030164999, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        List<Long> recNos = new ArrayList<>();
        querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                record -> recNos.add(record.getRecNo()),
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

        // Index(rank [EQUALS $__in_rank(Field { 'num_value_2' None} group 1)__0] BY_RANK) WHERE __in_rank(Field { 'num_value_2' None} group 1)__0 IN [1, 3, 5]
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                inValuesJoinPlan(
                        indexPlan().where(indexName("rank")).and(RecordQueryPlanMatchers.indexScanType(IndexScanType.BY_RANK))
                ).where(inValuesList(equalsObject(ls))));
        assertEquals(1518925028, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1422629447, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1422660327, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        List<Long> recNos = new ArrayList<>();
        querySimpleRecordStore(recordMetaDataHook, plan, EvaluationContext::empty,
                record -> recNos.add(record.getRecNo()),
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

        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_unique [EQUALS $__in_num_value_unique__0]) WHERE __in_num_value_unique__0 IN [901, 903, 905] SORTED ∪[Field { 'num_value_unique' None}, Field { 'rec_no' None}] Index(MySimpleRecord$num_value_unique ([950],>)
            assertMatchesExactly(plan,
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_unique")),
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("[EQUALS $__in_num_value_unique__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))))
                            .where(comparisonKey(concat(field("num_value_unique"), primaryKey("MySimpleRecord")))));
            assertEquals(1116661716, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-924293640, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(713030732, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique")))),
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique"))
                                                            .and(scanComparisons(equalities(exactly(anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))))
                                    .where(comparisonKey(concat(field("num_value_unique"), primaryKey("MySimpleRecord"))))));
            assertEquals(874214575, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1101003320, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1557397237, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        assertEquals(53, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> assertThat(record.getNumValueUnique(), anyOf(is(901), is(903), is(905), greaterThan(950))),
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

        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            // Index(MySimpleRecord$num_value_unique [EQUALS $__in_num_value_unique__0]) WHERE __in_num_value_unique__0 IN [901, 903, 905] SORTED ∪[Field { 'num_value_unique' None}, Field { 'rec_no' None}] Index(MySimpleRecord$num_value_unique [EQUALS $__in_num_value_unique__0]) WHERE __in_num_value_unique__0 IN [904, 905, 906] SORTED
            // Ordinary equality comparisons would be ordered just by the primary key so that would be the union comparison key.
            // Must compare the IN field here; they are ordered, but not trivially (same value for each).
            assertMatchesExactly(plan,
                    unionPlan(
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("[EQUALS $__in_num_value_unique__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))),
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("[EQUALS $__in_num_value_unique__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(904, 905, 906))))));
            assertEquals(218263868, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(468995802, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2098251608, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique"))
                                                            .and(scanComparisons(equalities(exactly(anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(901, 903, 905)))),
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique"))
                                                            .and(scanComparisons(equalities(exactly(anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(904, 905, 906)))))
                                    .where(comparisonKey(concat(field("num_value_unique"), primaryKey("MySimpleRecord"))))));
            assertEquals(-1323754895, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(856768529, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1700358353, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        Set<Long> dupes = new HashSet<>();
        assertEquals(5, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> {
                    assertTrue(dupes.add(record.getRecNo()), "should not have duplicated records");
                    assertThat(record.getNumValueUnique(), anyOf(is(901), is(903), is(904), is(905), is(906)));
                }, context -> TestHelpers.assertDiscardedAtMost(1, context)));
    }

    /**
     * Verify that an IN requires an unordered union due to incompatible ordering.
     */
    @DualPlannerTest
    void testInQueryOrDifferentCondition() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_unique").lessThan(910),
                        Query.and(
                                Query.field("num_value_unique").greaterThan(990),
                                Query.field("num_value_2").in(Arrays.asList(2, 0)))))
                .build();
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            // Without the join, these would be using the same index and so compatible, even though inequalities.
            // TODO: IN join in filter can prevent index scan merging (https://github.com/FoundationDB/fdb-record-layer/issues/9)
            assertMatchesExactly(plan,
                    unorderedPrimaryKeyDistinctPlan(
                            unorderedUnionPlan(
                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([null],[910])"))),
                                    inValuesJoinPlan(
                                            filterPlan(
                                                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>"))))
                                    ).where(inValuesList(equalsObject(Arrays.asList(0, 2)))))));
            assertEquals(-97067043, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(942676960, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(417180157, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            // Cascades planner avoids IN-JOIN causing a primary scan and a UNION-ALL
            unionPlan(
                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([null],[910])"))),
                    predicatesFilterPlan(indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>"))))
                            .where(predicates(valuePredicate(fieldValue("num_value_2"), new Comparisons.ListComparison(Comparisons.Type.IN, ImmutableList.of(0, 2))))))
                    .where(comparisonKey(concat(field("num_value_unique"), primaryKey("MySimpleRecord"))));
            assertEquals(-1933328656, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1747054907, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1932097284, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(16, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> {
                    assertThat(record.getNumValueUnique(), anyOf(lessThan(910), greaterThan(990)));
                    if (record.getNumValue3Indexed() > 990) {
                        assertThat(record.getNumValue2(), anyOf(is(2), is(0)));
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
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    unionPlan(
                            inValuesJoinPlan(
                                    indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[EQUALS odd, EQUALS 0, EQUALS $__in_num_value_3_indexed__0]")))
                            ).where(inValuesList(equalsObject(Arrays.asList(1, 3)))),
                            indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[odd, 0, 4],[odd, 0]]")))));
            assertEquals(468569345, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1312398381, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1327693258, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("multi_index")))),
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("multi_index"))
                                                            .and(scanComparisons(equalities(exactly(
                                                                    equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "odd")),
                                                                    equalsObject(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 0)),
                                                                    anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(Arrays.asList(1, 3)))))
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))))));
            assertEquals(687944558, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-913554911, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-137232979, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(3 + 4 + 4, querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                record -> {
                    assertThat(record.getStrValueIndexed(), is("odd"));
                    assertThat(record.getNumValue2(), is(0));
                    assertThat(record.getNumValue3Indexed(), anyOf(is(1), is(3), greaterThanOrEqualTo(4)));
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

        RecordQueryPlan plan = planner.plan(query);

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
            assertEquals(-1310248168, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1826025907, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1395411845, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                                    inUnionPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))
                                                            .and(scanComparisons(equalities(exactly(anyParameterComparison())))))))
                                            .where(inUnionComparisonKey(primaryKey("MySimpleRecord")))
                                            .and(inUnionValuesSources(exactly(inUnionInValues(equalsObject(ImmutableList.of(1, 3)))))))));
            assertEquals(2086306995, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(527952105, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(626043938, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        Set<Long> dupes = new HashSet<>();
        assertEquals(50 + 10 + 10, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                record -> {
                    assertTrue(dupes.add(record.getRecNo()), "should not have duplicated records");
                    assertTrue(record.getStrValueIndexed().equals("odd") ||
                               record.getNumValue3Indexed() == 1 ||
                               record.getNumValue3Indexed() == 3);
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
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    descendantPlans(indexPlan().where(indexName("color"))));
            assertFalse(plan.hasRecordScan(), "should not use record scan");
            assertEquals(-520431454, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1447363737, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1442809521, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                                    inValuesJoinPlan(
                                            coveringIndexPlan().where(
                                                    indexPlanOf(indexPlan().where(indexName("color"))
                                                            .and(scanComparisons(equalities(exactly(
                                                                    anyParameterComparison())))))))
                                            .where(inValuesList(equalsObject(redBlue)))));
            assertFalse(plan.hasRecordScan(), "should not use record scan");
            assertEquals(1174038242, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1089300981, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1677589406, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openEnumRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
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
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(selfOrDescendantPlans(scanPlan().where(scanComparisons(unbounded())))).where(queryComponents(only(equalsObject(filter)))));
            assertEquals(-1139440895, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1907402540, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1694845095, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(selfOrDescendantPlans(scanPlan().and(scanComparisons(unbounded()))))
                            .where(predicates(valuePredicate(fieldValue("num_value_2"), new Comparisons.ListComparison(Comparisons.Type.IN, ImmutableList.of())))));
            assertEquals(997518602, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1107789406, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-895231961, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        assertEquals(0, querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty, (rec) -> {
        }));
    }

}
