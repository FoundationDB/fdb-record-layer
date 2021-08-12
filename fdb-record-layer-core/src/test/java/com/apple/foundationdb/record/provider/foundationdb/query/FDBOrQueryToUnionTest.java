/*
 * FDBOrQueryToUnionTest.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedExactly;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.temp.matchers.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.comparisonKey;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.intersectionPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.unionPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.unorderedUnionPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ValueMatchers.fieldValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests related to planning a query with an OR clause into a union plan.
 */
@Tag(Tags.RequiresFDB)
class FDBOrQueryToUnionTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that an OR of compatibly-ordered (up to reversal) indexed fields can be implemented as a union.
     */
    //@SuppressWarnings("rawtypes") // Bug with raw types and method references: https://bugs.openjdk.java.net/browse/JDK-8063054
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testComplexQuery6(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .setSort(null, true)
                .setRemoveDuplicates(true)
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch || planner instanceof CascadesPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[odd],[odd]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[0],[0]]")))))));
            assertMatchesExactly(plan, planMatcher);

            assertTrue(plan.getQueryPlanChildren().stream().allMatch(QueryPlan::isReverse));
            assertEquals(-1584186103, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-357068519, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(964023338, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[odd],[odd]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[0],[0]]"))));
            assertMatchesExactly(plan, planMatcher);

            assertTrue(plan.getQueryPlanChildren().stream().allMatch(QueryPlan::isReverse));
            assertEquals(-2067012572, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1784357954, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1189517485, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        Set<Long> seen = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0, "condition on record not met");
                    assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                    seen.add(myrec.getRecNo());
                    i++;
                }
            }
            assertEquals(60, i);
            assertDiscardedAtMost(10, context);
            if (shouldDeferFetch) {
                assertLoadRecord(60, context);
            }
        }
    }

    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testComplexQuery6Continuations(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .setRemoveDuplicates(true)
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
        RecordQueryPlan plan = planner.plan(query);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int limit = 1; limit <= 5; limit++) {
                clearStoreCounter(context);
                Set<Long> seen = new HashSet<>();
                int i = 0;
                byte[] continuation = null;
                do {
                    try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(limit)
                            .build()).asIterator()) {
                        while (cursor.hasNext()) {
                            FDBQueriedRecord<Message> rec = cursor.next();
                            TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                            myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                            assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0, "condition on record not met");
                            assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                            seen.add(myrec.getRecNo());
                            i++;
                        }
                        continuation = cursor.getContinuation();
                    }
                } while (continuation != null);
                assertEquals(60, i);
                assertDiscardedExactly(10, context);
                if (shouldDeferFetch) {
                    assertLoadRecord(60, context);
                }
            }
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on the same field are implemented using a union of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testOrQuery1(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(4)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[2],[2]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[4],[4]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch || planner instanceof CascadesPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[2],[2]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[4],[4]]"))))))
                                    .where(comparisonKey(primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1912003491, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1070595610, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-369851503, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[2],[2]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[4],[4]]"))))
                            .where(comparisonKey(primaryKey("MySimpleRecord")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(273143354, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1002901843, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1703645950, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 2 ||
                               myrec.getNumValue3Indexed() == 4);
                    i++;
                }
            }
            assertEquals(20 + 20 + 20, i);
            assertDiscardedNone(context);
            assertLoadRecord(60, context);
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on the same field are implemented using a union of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testOrQueryPlanEquals(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(4)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
        RecordQueryPlan plan = planner.plan(query);

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(4),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(1)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
        RecordQueryPlan plan2 = planner.plan(query2);

        // plan is physically different but returns the same result
        assertThat(plan.hashCode(), not(equalTo(plan2.hashCode())));
        assertThat(plan, not(equalTo(plan2)));
        assertTrue(plan.semanticEquals(plan2));
    }

    /**
     * Verify that queries with an OR of a mix of equality and inequality predicates on the same field are implemented
     * using a union of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testOrQuery2(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Covering(Index(MySimpleRecord$num_value_3_indexed [[2],[2]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Covering(Index(MySimpleRecord$num_value_3_indexed ([3],>) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch || planner instanceof CascadesPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[2],[2]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))))))
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(504228282, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1520996708, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2080970598, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[2],[2]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))))
                            .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1299166123, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-700473135, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-140499245, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 2 ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            assertEquals(20 + 20 + 20, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that queries with an OR of non-overlapping range inequalities on the same field are implemented using a union
     * of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testOrQuery3(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").lessThan(2),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();

        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed ([null],[2])) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Covering(Index(MySimpleRecord$num_value_3_indexed ([3],>) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);
        if (shouldDeferFetch || planner instanceof CascadesPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([null],[2])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))))))
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-627934247, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(502710007, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1718649364, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([null],[2])"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))))
                            .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1930405164, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1650830816, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-434891459, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValue3Indexed() < 2 ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            assertEquals(40 + 20, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on different fields are implemented using a union of indexes,
     * if all fields are indexed.
     */
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testOrQuery4(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[even],[even]]) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[3],[3]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch || planner instanceof CascadesPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[even],[even]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))))))
                                    .where(comparisonKey(primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-417814093, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1082480572, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(233155848, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[even],[even]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))))
                            .where(comparisonKey(primaryKey("MySimpleRecord")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-673254486, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(991016881, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1988313995, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") ||
                               myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 3);
                    i++;
                }
            }
            assertEquals(50 + 10 + 10, i);
            assertDiscardedAtMost(20, context);
            if (shouldDeferFetch) {
                assertLoadRecord(50 + 10 + 10, context);
            }
        }
    }

    /**
     * Verify that an OR of inequalities on different fields uses an unordered union, since there is no compatible ordering.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery5 [removesDuplicates = {0}]")
    @BooleanSource
    void testOrQuery5(boolean removesDuplicates) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .setRemoveDuplicates(removesDuplicates)
                .build();

        // Unordered(Index(MySimpleRecord$str_value_indexed ([null],[m])) ∪ Index(MySimpleRecord$num_value_3_indexed ([3],>))
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof CascadesPlanner) {
            BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unorderedUnionPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([null],[m])"))))),
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))))));
            if (removesDuplicates) {
                planMatcher =
                        fetchFromPartialRecordPlan(
                                unorderedPrimaryKeyDistinctPlan(planMatcher));
            } else {
                planMatcher =
                        fetchFromPartialRecordPlan(planMatcher);

            }
            assertMatchesExactly(plan, planMatcher);

            assertEquals(removesDuplicates ? 1898767693 : 1898767686, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(removesDuplicates ? -583062018 : 212117636, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(removesDuplicates ? 1864525478 : -1635262164, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unorderedUnionPlan(
                            indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([null],[m])"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))));

            if (removesDuplicates) {
                planMatcher = unorderedPrimaryKeyDistinctPlan(planMatcher);
            }
            assertMatchesExactly(plan, planMatcher);

            assertEquals(removesDuplicates ? -1569447744 : -1569447745, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(removesDuplicates ? 1558364455 : -1941423187, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(removesDuplicates ? -289015345 : 506164309, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            Objects.requireNonNull(context.getTimer()).reset();
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().compareTo("m") < 0 ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            if (removesDuplicates) {
                assertEquals(50 + 10, i);
                assertDiscardedAtMost(10, context);
            } else {
                assertEquals(70, i);
                assertDiscardedNone(context);
            }
        }
    }

    @Nonnull
    @SuppressWarnings("unused") // used by reflection
    private static Stream<Arguments> query5WithLimitsArgs() {
        return Stream.of(1, 2, 5, 7).flatMap(i -> Stream.of(Arguments.of(i, false), Arguments.of(i, true)));
    }

    @DualPlannerTest
    @MethodSource("query5WithLimitsArgs")
    @ParameterizedTest(name = "testOrQuery5WithLimits [limit = {0}, removesDuplicates = {1}]")
    void testOrQuery5WithLimits(int limit, boolean removesDuplicates) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        setDeferFetchAfterUnionAndIntersection(true);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .setRemoveDuplicates(removesDuplicates)
                .build();

        // Unordered(Index(MySimpleRecord$str_value_indexed ([null],[m])) ∪ Index(MySimpleRecord$num_value_3_indexed ([3],>))
        RecordQueryPlan plan = planner.plan(query);

        final BindingMatcher<RecordQueryUnorderedUnionPlan> unionPlanBindingMatcher = unorderedUnionPlan(
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")))));

        final BindingMatcher<? extends RecordQueryPlan> planMatcher;
        if (removesDuplicates) {
            planMatcher = fetchFromPartialRecordPlan(unorderedPrimaryKeyDistinctPlan(unionPlanBindingMatcher));
        } else {
            planMatcher = fetchFromPartialRecordPlan(unionPlanBindingMatcher);
        }

        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            assertEquals(removesDuplicates ? 1898767693 : 1898767686, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(removesDuplicates ? -583062018 : 212117636, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(removesDuplicates ? 1864525478 : -1635262164, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(removesDuplicates ? 1898767693 : 1898767686, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(removesDuplicates ? -583062018 : 212117636, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(removesDuplicates ? 1864525478 : -1635262164, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            boolean done = false;
            byte[] continuation = null;
            Set<Tuple> uniqueKeys = new HashSet<>();
            int itr = 0;
            while (!done) {
                ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(limit)
                        .build();
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties).asIterator()) {
                    int i = 0;
                    Set<Tuple> keysThisIteration = new HashSet<>();
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                        assertTrue(myrec.getStrValueIndexed().compareTo("m") < 0 ||
                                   myrec.getNumValue3Indexed() > 3);
                        uniqueKeys.add(rec.getPrimaryKey());
                        if (removesDuplicates) {
                            assertThat(keysThisIteration.add(rec.getPrimaryKey()), is(true));
                        }
                        i++;
                    }
                    continuation = cursor.getContinuation();
                    done = cursor.getNoNextReason().isSourceExhausted();
                    if (!done) {
                        assertEquals(limit, i);
                    }
                }
                itr++;
                assertThat("exceeded maximum iterations", itr, lessThan(500));
            }
            assertEquals(50 + 10, uniqueKeys.size());
        }
    }

    /**
     * Verify that a complex query with an OR of an AND produces a union plan if appropriate indexes are defined.
     * In particular, verify that it can use the last field of an index and does not require primary key ordering
     * compatibility.
     */
    @DualPlannerTest
    void testOrQuery6() throws Exception {
        RecordMetaDataHook hook = metaData ->
                metaData.addIndex("MySimpleRecord",
                        new Index("str_value_3_index",
                                "str_value_indexed",
                                "num_value_3_indexed"));
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.and(
                                Query.field("str_value_indexed").equalsValue("even"),
                                Query.field("num_value_3_indexed").greaterThan(3)),
                        Query.field("num_value_3_indexed").lessThan(1)))
                .build();

        // Index(str_value_3_index ([even, 3],[even]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Index(MySimpleRecord$num_value_3_indexed ([null],[1]))
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof CascadesPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("([even, 3],[even]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([null],[1])"))))))
                                    .where(comparisonKey(concat(Key.Expressions.field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-835124758, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(778876973, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1061354639, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("([even, 3],[even]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([null],[1])"))))
                            .where(comparisonKey(concat(Key.Expressions.field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1721396731, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1374663850, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1092186184, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue((myrec.getStrValueIndexed().equals("even") &&
                                myrec.getNumValue3Indexed() > 3) ||
                               myrec.getNumValue3Indexed() < 1);
                    i++;
                }
            }
            assertEquals(20 + 10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a complex query with an OR of an AND produces a union plan if the appropriate indexes are defined.
     * Unlike {@link #testOrQuery6()}, the legs of the union are not compatibly ordered, so this will revert to using
     * an unordered union.
     */
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testUnorderableOrQueryWithAnd(boolean removesDuplicates) throws Exception {
        RecordMetaDataHook hook = metaDataBuilder -> {
            complexQuerySetupHook().apply(metaDataBuilder);
            metaDataBuilder.addIndex("MySimpleRecord", new Index("multi_index_2", "str_value_indexed", "num_value_3_indexed"));
        };
        complexQuerySetup(hook);
        setDeferFetchAfterUnionAndIntersection(true);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(
                        Query.and(
                                Query.field("str_value_indexed").equalsValue("even"),
                                Query.or(Query.field("num_value_2").lessThanOrEquals(1), Query.field("num_value_3_indexed").greaterThanOrEquals(3))
                        )
                )
                .setRemoveDuplicates(removesDuplicates)
                .build();

        // Unordered(Index(multi_index ([even, null],[even, 1]]) ∪ Index(multi_index_2 [[even, 3],[even]]))
        RecordQueryPlan plan = planner.plan(query);

        BindingMatcher<? extends RecordQueryPlan> planMatcher =
                unorderedUnionPlan(
                        coveringIndexPlan()
                                .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([even, null],[even, 1]]"))))),
                        coveringIndexPlan()
                                .where(indexPlanOf(indexPlan().where(indexName("multi_index_2")).and(scanComparisons(range("[[even, 3],[even]]"))))));

        if (removesDuplicates) {
            planMatcher = fetchFromPartialRecordPlan(unorderedPrimaryKeyDistinctPlan(planMatcher));
        } else {
            planMatcher = fetchFromPartialRecordPlan(planMatcher);
        }
        assertMatchesExactly(plan, planMatcher);

        assertEquals(removesDuplicates ? -1216499257 : -1216499264, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(removesDuplicates ? 610131412 : 1405311066, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(removesDuplicates ? 1591758672 : -1908028970, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") &&
                               (myrec.getNumValue2() <= 1 || myrec.getNumValue3Indexed() >= 3));
                    i++;
                }
            }
            if (removesDuplicates) {
                assertEquals(40, i);
                assertDiscardedAtMost(13, context);
                assertLoadRecord(53, context);
            } else {
                assertEquals(53, i);
                assertDiscardedNone(context);
            }
        }
    }

    /**
     * Verify that an OR with complex limits is implemented as a union, where the comparison key is constructed
     * without repetition out of the index key and primary key (see note).
     */
    @DualPlannerTest
    void testOrQuery7() throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook(true);
        complexQuerySetup(hook);
        setDeferFetchAfterUnionAndIntersection(true);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.or(
                                Query.field("num_value_3_indexed").equalsValue(1),
                                Query.field("num_value_3_indexed").greaterThan(3))))
                .build();

        // Index(str_value_3_index [[even, 1],[even, 1]]) ∪[Field { 'str_value_indexed' None}, Field { 'num_value_3_indexed' None}, Field { 'num_value_unique' None}] Index(str_value_3_index ([even, 3],[even]])
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("[[even, 1],[even, 1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("([even, 3],[even]]"))))))
                                    .where(comparisonKey(concat(Key.Expressions.field("str_value_indexed"), field("num_value_3_indexed"), field("num_value_unique")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-664830657, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1572009327, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1251823795, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("[[even, 1],[even, 1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("([even, 3],[even]]"))))))
                                    // note that the comparison key is only on (num_value_3_indexed, num_value_unique) as str_value_indexed is equality-bound
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), field("num_value_unique")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-60058062, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1391842890, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(79291284, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") &&
                               (myrec.getNumValue3Indexed() == 1) ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            assertEquals(10 + 10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that an OR query on the same field with a sort on that field is implemented as a union of index scans,
     * where the union ordering is the field (and not the primary key, as it would normally be for equality predicates).
     * TODO The planner could be smarter here:
     * TODO: Add RecordQueryConcatenationPlan for non-overlapping unions (https://github.com/FoundationDB/fdb-record-layer/issues/13)
     * Note that the ordering planner property evaluation now understands that num_value_3_indexed is equality-bound
     * and does not need to partake in the ordering.
     */
    @DualPlannerTest
    void testOrQueryOrdered() throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook();
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .setSort(field("num_value_3_indexed"))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'str_value_indexed' None}, Field { 'num_value_unique' None}] Index(MySimpleRecord$num_value_3_indexed [[3],[3]])
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))))
                            .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1412961915, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(258619931, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1414232579, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))))))
                                    // note that the comparison key is only on the primary key
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1300798826, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1882806542, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(739308244, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    int numValue3 = myrec.getNumValue3Indexed();
                    assertTrue(numValue3 == 1 || numValue3 == 3, "should satisfy value condition");
                    assertTrue(numValue3 == 1 || i >= 20, "lower values should come first");
                    i++;
                }
            }
            assertEquals(20 + 20, i);
            assertDiscardedNone(context);
        }

        query = query.toBuilder()
                .setSort(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))
                .build();
        plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))))
                            .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1412961915, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(258435419, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1414417091, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))))))
                                    // note that the comparison key is only on the primary key
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1300798826, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1882991054, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(739123732, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testOrQueryChildReordering(boolean shouldPushFetchAboveUnionToIntersection) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .setSort(null, true)
                .setRemoveDuplicates(true)
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldPushFetchAboveUnionToIntersection);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan1 = planner.plan(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(0),
                        Query.field("str_value_indexed").equalsValue("odd")))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) ∪ Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]))
        RecordQueryPlan plan2 = planner.plan(query2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertNotEquals(plan1, plan2);
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());
        assertTrue(plan1.semanticEquals(plan2));
        if (shouldPushFetchAboveUnionToIntersection || planner instanceof CascadesPlanner) {
            assertEquals(-1584186103, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-357068519, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(964023338, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(-91575587, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1919956247, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(78160824, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-2067012572, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1784357954, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1189517485, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(600484528, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(221470226, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-2075379999, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        
        Set<Long> seen = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor1 = recordStore.executeQuery(plan1).asIterator();
                    RecordCursorIterator<FDBQueriedRecord<Message>> cursor2 = recordStore.executeQuery(plan2).asIterator()) {
                while (cursor1.hasNext()) {
                    assertThat(cursor2.hasNext(), is(true));
                    FDBQueriedRecord<Message> rec1 = cursor1.next();
                    FDBQueriedRecord<Message> rec2 = cursor2.next();
                    assertEquals(Objects.requireNonNull(rec1).getRecord(), Objects.requireNonNull(rec2).getRecord());
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec1.getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0, "condition on record not met");
                    assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                    seen.add(myrec.getRecNo());
                    i++;
                }
                assertThat(cursor2.hasNext(), is(false));
            }
            assertEquals(60, i);
            assertDiscardedAtMost(20, context);
            if (shouldPushFetchAboveUnionToIntersection) {
                assertLoadRecord(120, context);
            }
        }
    }

    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testOrQueryChildReordering2(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .setSort(null, true)
                .setRemoveDuplicates(true)
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan1 = planner.plan(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(Lists.reverse(Objects.requireNonNull((OrComponent)query1.getFilter()).getChildren())))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) ∪ Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]))
        RecordQueryPlan plan2 = planner.plan(query2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertNotEquals(plan1, plan2);
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());
        assertTrue(plan1.semanticEquals(plan2));
        
        if (shouldDeferFetch || planner instanceof CascadesPlanner) {
            assertEquals(770691035, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1890796442, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(55660884, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(1289607451, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-29394342, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1772831508, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(723665474, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-330673401, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2129158337, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(184229634, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2044103111, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-448638335, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        Set<Long> seen = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor1 = recordStore.executeQuery(plan1).asIterator();
                    RecordCursorIterator<FDBQueriedRecord<Message>> cursor2 = recordStore.executeQuery(plan2).asIterator()) {
                while (cursor1.hasNext()) {
                    assertThat(cursor2.hasNext(), is(true));
                    FDBQueriedRecord<Message> rec1 = cursor1.next();
                    FDBQueriedRecord<Message> rec2 = cursor2.next();
                    assertEquals(Objects.requireNonNull(rec1).getRecord(), Objects.requireNonNull(rec2).getRecord());
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec1.getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0 || myrec.getNumValue3Indexed() == 3, "condition on record not met");
                    assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                    seen.add(myrec.getRecNo());
                    i++;
                }
                assertThat(cursor2.hasNext(), is(false));
            }
            assertEquals(70, i);
            assertDiscardedAtMost(40, context);
            if (shouldDeferFetch) {
                assertLoadRecord(140, context);
            }
        }
    }

    /**
     * Verify that a query with an OR of an AND can be implemented as a union of an index scan with an intersection of index scans.
     */
    @DualPlannerTest
    void testOrderedOrQueryWithAnd() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", "str_2", concat(field("str_value_indexed"), field("num_value_2")));
            metaData.addIndex("MySimpleRecord", "nu_2", concat(field("num_value_unique"), field("num_value_2")));
            metaData.addIndex("MySimpleRecord", "n3_2", concat(field("num_value_3_indexed"), field("num_value_2")));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.and(
                                Query.field("num_value_3_indexed").equalsValue(1),
                                Query.field("num_value_unique").equalsValue(909))))
                .setSort(field("num_value_2"))
                .build();

        // Index(str_2 [[even],[even]]) ∪[Field { 'num_value_2' None}, Field { 'rec_no' None}] Index(nu_2 [[909],[909]]) ∩ Index(n3_2 [[1],[1]])
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("str_2")).and(scanComparisons(range("[[even],[even]]"))),
                            intersectionPlan(
                                    indexPlan().where(indexName("nu_2")).and(scanComparisons(range("[[909],[909]]"))),
                                    indexPlan().where(indexName("n3_2")).and(scanComparisons(range("[[1],[1]]")))))
                            .where(comparisonKey(concat(field("num_value_2"), primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1659601413, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1344221020, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1474039530, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("str_2")).and(scanComparisons(range("[[even],[even]]"))))),
                                    intersectionPlan(
                                            coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("nu_2")).and(scanComparisons(range("[[909],[909]]"))))),
                                            coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("n3_2")).and(scanComparisons(range("[[1],[1]]")))))))
                                    .where(comparisonKey(concat(field("num_value_2"), primaryKey("MySimpleRecord")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1254181870, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(156652779, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1906836981, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") || (myrec.getNumValue3Indexed() == 1 && myrec.getNumValueUnique() == 909));
                    i++;
                }
            }
            assertEquals(51, i);
        }
    }

    /**
     * Verify that an OR on two indexed fields with compatibly ordered indexes is implemented by a union, and that the
     * union cursors works properly with a returned record limit.
     */
    @DualPlannerTest
    @ParameterizedTest
    @BooleanSource
    void testComplexLimits5(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]]) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch || planner instanceof CascadesPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[odd],[odd]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[0],[0]]"))))))
                                    .where(comparisonKey(primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1584186334, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-351348461, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(969743396, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[odd],[odd]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[0],[0]]"))))
                            .where(comparisonKey(primaryKey("MySimpleRecord")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-2067012605, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1790078012, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1183797427, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(5).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());

                    if (myrec.getNumValue3Indexed() != 0) {
                        assertEquals("odd", myrec.getStrValueIndexed());
                    }
                    i += 1;
                }
            }
            assertEquals(5, i);
            assertDiscardedAtMost(1, context);
            if (shouldDeferFetch) {
                assertLoadRecord(5, context);
            }
        }
    }

    /**
     * Verify that boolean normalization of a complex AND/OR expression produces simple plans.
     * In particular, verify that an AND of OR still uses a union of index scans (an OR of AND).
     */
    @DualPlannerTest
    public void testOrQueryDenorm() throws Exception {
        // new Index("multi_index", "str_value_indexed", "num_value_2", "num_value_3_indexed")
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_2").equalsValue(0),
                        Query.or(
                                Query.field("num_value_3_indexed").equalsValue(0),
                                Query.and(
                                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                                        Query.field("num_value_3_indexed").lessThanOrEquals(3)))))
                .build();

        // Index(multi_index [[even, 0, 0],[even, 0, 0]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Index(multi_index [[even, 0, 2],[even, 0, 3]])
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[even, 0, 0],[even, 0, 0]]"))),
                            indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[even, 0, 2],[even, 0, 3]]"))))
                            .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));
            assertMatchesExactly(plan, planMatcher);
            assertEquals(-2074065439, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1146901452, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1940448631, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[even, 0, 0],[even, 0, 0]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[even, 0, 2],[even, 0, 3]]"))))))
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertMatchesExactly(plan, planMatcher);
            assertEquals(-1633556172, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1006639371, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-200977842, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertEquals(0, (myrec.getNumValue2() % 3));
                    assertThat(myrec.getNumValue3Indexed() % 5, anyOf(is(0), allOf(greaterThanOrEqualTo(2), lessThanOrEqualTo(3))));
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that OR plans work properly when executed with continuations, even when the continuation splits differ
     * in how they exhaust the union sources.
     */
    @DualPlannerTest
    void testOrQuerySplitContinuations() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            for (int i = 0; i < 100; i++) {
                recBuilder.setRecNo(i);
                recBuilder.setNumValue3Indexed(i / 10);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        // Each substream completes before the next one starts.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(5)))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) ∪ Index(MySimpleRecord$num_value_3_indexed [[3],[3]]) ∪ Index(MySimpleRecord$num_value_3_indexed [[5],[5]])
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[5],[5]]"))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(273143386, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1919034675, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1703645950, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[5],[5]]")))))
                            ));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1912003715, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-154462778, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-369851503, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int limit = 1; limit <= 5; limit++) {
                int i = 0;
                byte[] continuation = null;
                do {
                    try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(limit)
                            .build()).asIterator()) {
                        while (cursor.hasNext()) {
                            FDBQueriedRecord<Message> rec = cursor.next();
                            TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                            myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                            assertEquals((i / 10) * 2 + 1, myrec.getNumValue3Indexed());
                            i++;
                        }
                        continuation = cursor.getContinuation();
                    }
                } while (continuation != null);
                assertEquals(30, i);
            }
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that queries with an OR of predicates with a common scan and different filters does not bother with a Union.
     *
     * TODO [Cascades Planner] We have a predicate of the form a ^ (x v y v z). That is CNFed into:
     *      (a ^ x) v (a ^ y) v (a ^ z). As that is currently done per pre step before planning, we miss out on the
     *      optimal plan later as the originally written DNF is not preserved but crucial for finding the single
     *      index scan.
     *      (https://github.com/FoundationDB/fdb-record-layer/issues/1301)
     */
    @Test
    void testOrQueryNoIndex() throws Exception {
        RecordMetaDataHook hook = metadata -> metadata.removeIndex("MySimpleRecord$num_value_3_indexed");
        complexQuerySetup(hook);
        QueryComponent orComponent = Query.or(
                Query.field("num_value_3_indexed").equalsValue(1),
                Query.field("num_value_3_indexed").equalsValue(2),
                Query.field("num_value_3_indexed").equalsValue(4));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("str_value_indexed").equalsValue("even"), orComponent))
                .build();

        // Index(MySimpleRecord$str_value_indexed [[even],[even]]) | Or([num_value_3_indexed EQUALS 1, num_value_3_indexed EQUALS 2, num_value_3_indexed EQUALS 4])
        RecordQueryPlan plan = planner.plan(query);

        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                filterPlan(
                        indexPlan()
                                .where(indexName("MySimpleRecord$str_value_indexed"))
                                .and(scanComparisons(range("[[even],[even]]"))))
                        .where(queryComponents(exactly(equalsObject(orComponent))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-1553701984, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1108620348, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1573180943, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertTrue(myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 2 ||
                               myrec.getNumValue3Indexed() == 4);
                    i++;
                }
            }
            assertEquals(10 + 10 + 10, i);
            assertDiscardedExactly(10 + 10, context);
        }
    }

    /**
     * Verify that a union visitor won't defer a record fetch if the comparison key has fields that the index
     * entry doesn't.
     * This sort of plan is never produced by the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner},
     * so we have to test the visitor directly.
     */
    @Test
    void unionVisitorOnComplexComparisonKey() throws Exception {
        complexQuerySetup(null);

        RecordQueryPlan originalPlan1 = RecordQueryUnionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                primaryKey("MySimpleRecord"), true);

        RecordQueryPlan modifiedPlan1 = RecordQueryPlannerSubstitutionVisitor.applyVisitors(originalPlan1, recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                fetchFromPartialRecordPlan(
                        unionPlan(
                                coveringIndexPlan()
                                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                                coveringIndexPlan()
                                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))))
                        ));
        assertMatchesExactly(modifiedPlan1, planMatcher);

        RecordQueryPlan originalPlan2 = RecordQueryUnionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                concat(field("num_value_2"), primaryKey("MySimpleRecord")), true);
        RecordQueryPlan modifiedPlan2 = RecordQueryPlannerSubstitutionVisitor.applyVisitors(originalPlan2,  recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        // Visitor should not perform transformation because of comparison key on num_value_unique
        assertEquals(originalPlan2, modifiedPlan2);
    }

    @DualPlannerTest
    void deferFetchOnUnionWithInnerFilter() throws Exception {
        complexQuerySetup(metaData -> {
            // We don't prefer covering indexes over other indexes yet.
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.addIndex("MySimpleRecord", "coveringIndex", new KeyWithValueExpression(concat(field("num_value_2"), field("num_value_3_indexed")), 1));
        });

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").startsWith("foo"),
                        Query.and(
                                Query.field("num_value_2").greaterThanOrEquals(2),
                                Query.field("num_value_2").lessThanOrEquals(4)),
                        Query.and(
                                Query.field("num_value_3_indexed").lessThanOrEquals(18),
                                Query.field("num_value_2").greaterThanOrEquals(26))
                        ))
                .build();
        setDeferFetchAfterUnionAndIntersection(true);
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    unorderedUnionPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("{[foo],[foo]}"))))),
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[2],[4]]"))))),
                                            filterPlan(
                                                    coveringIndexPlan()
                                                            .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[26],>"))))))
                                                    .where(queryComponents(exactly(equalsObject(Query.field("num_value_3_indexed").lessThanOrEquals(18)))))
                                    )));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1829743477, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1168128533, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1840217393, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    unorderedUnionPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("{[foo],[foo]}"))))),
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[2],[4]]"))))),
                                            predicatesFilterPlan(
                                                    coveringIndexPlan()
                                                            .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[26],>"))))))
                                                    .where(predicates(only(valuePredicate(fieldValue("num_value_3_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 18)))))
                                    )));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(331039648, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1539052743, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1469293183, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    @DualPlannerTest
    void testOrQueryToDistinctUnion() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        setDeferFetchAfterUnionAndIntersection(true);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("m"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .setRemoveDuplicates(true)
                .build();

        final RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                fetchFromPartialRecordPlan(
                        unionPlan(
                                coveringIndexPlan()
                                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                                coveringIndexPlan()
                                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))))));

        assertMatchesExactly(plan, planMatcher);

        assertEquals(-1608004667, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-291254354, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(969743396, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @DualPlannerTest
    void testOrQueryToDistinctUnionWithPartialDefer() throws Exception {
        complexQuerySetup(null);
        setDeferFetchAfterUnionAndIntersection(true);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("m"),
                        Query.field("num_value_3_indexed").equalsValue(3),
                        Query.and(Query.field("num_value_2").equalsValue(3), Query.field("num_value_3_indexed").equalsValue(4))))
                .setRemoveDuplicates(true)
                .build();

        final RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")),
                            filterPlan(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))).where(queryComponents(exactly(equalsObject(Query.field("num_value_2").equalsValue(3))))));

            assertMatchesExactly(plan, planMatcher);
            assertEquals(-91578519, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1329489138, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1714281445, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionPlan(
                            fetchFromPartialRecordPlan(
                                    unionPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")))))),
                            predicatesFilterPlan(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")))
                                    .where(predicates(only(valuePredicate(fieldValue("num_value_2"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 3))))));

            assertMatchesExactly(plan, planMatcher);

            assertEquals(357806958, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-325811449, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-2072211590, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * TODO [Cascades Planner] We have a predicate of the form a ^ (x v y v z). That is CNFed into:
     *      (a ^ x) v (a ^ y) v (a ^ z). As that is currently done per pre step before planning, we miss out on the
     *      optimal plan later as the originally written DNF is not preserved but crucial for finding the single
     *      index scan. (https://github.com/FoundationDB/fdb-record-layer/issues/1301)
     *      We should then transform a ^ (x v y v z) to
     *      - SELECT * FROM (SELECT * FROM T WHERE a) WHERE x v y v z
     *      - SELECT * FROM (SELECT * FROM T WHERE x v y v z) WHERE a
     *      - SELECT * FROM T WHERE x v y v z INTERSECT SELECT * FROM T WHERE a
     */
    @DualPlannerTest
    void testComplexOrQueryToDistinctUnion() throws Exception {
        complexQuerySetup(null);
        setDeferFetchAfterUnionAndIntersection(true);

        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("outer"),
                        Query.or(
                                Query.and(Query.field("num_value_3_indexed").greaterThan(1),
                                        Query.field("num_value_3_indexed").lessThan(2)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(3),
                                        Query.field("num_value_3_indexed").lessThan(4)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(5),
                                        Query.field("num_value_3_indexed").lessThan(6)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(7),
                                        Query.field("num_value_3_indexed").lessThan(8)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(9),
                                        Query.field("num_value_3_indexed").lessThan(10)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(11),
                                        Query.field("num_value_3_indexed").lessThan(12)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(13),
                                        Query.field("num_value_3_indexed").lessThan(14)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(15),
                                        Query.field("num_value_3_indexed").lessThan(16)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(17),
                                        Query.field("num_value_3_indexed").lessThan(18)))))
                .setRemoveDuplicates(true)
                .build();

        final RecordQueryPlan plan = planner.plan(query);

        final BindingMatcher<? extends RecordQueryPlan> planMatcher;
        if (planner instanceof RecordQueryPlanner) {
            planMatcher = filterPlan(
                    fetchFromPartialRecordPlan(
                            unionPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([1],[2])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],[4])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([5],[6])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([7],[8])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([9],[10])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([11],[12])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([13],[14])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([15],[16])"))))),
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([17],[18])"))))))
                                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))))
                    ))
                    .where(queryComponents(exactly(equalsObject(Query.field("str_value_indexed").equalsValue("outer")))));

        } else {
            planMatcher = unionPlan(
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([1],[2])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],[4])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([5],[6])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([7],[8])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([9],[10])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([11],[12])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([13],[14])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([15],[16])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))),
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([17],[18])"))))
                            .where(predicates(only(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer"))))))
                    .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));

        }
        assertMatches(plan, planMatcher);
    }
}
