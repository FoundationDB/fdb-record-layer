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
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedExactly;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.everyLeaf;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.union;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unorderedUnion;
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
public class FDBOrQueryToUnionTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that an OR of compatibly-ordered (up to reversal) indexed fields can be implemented as a union.
     */
    @SuppressWarnings("rawtypes") // Bug with raw types and method references: https://bugs.openjdk.java.net/browse/JDK-8063054
    @ParameterizedTest
    @BooleanSource
    public void testComplexQuery6(boolean shouldDeferFetch) throws Exception {
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
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch) {
            assertThat(plan, fetch(union(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))))));
            assertTrue(plan.getQueryPlanChildren().stream().allMatch(QueryPlan::isReverse));
            assertEquals(-1584186103, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1194358007, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-459005480, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, union(
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))));
            assertTrue(plan.getQueryPlanChildren().stream().allMatch(QueryPlan::isReverse));
            assertEquals(-2067012572, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(947068466, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1682420993, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        Set<Long> seen = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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

    @ParameterizedTest
    @BooleanSource
    public void testComplexQuery6Continuations(boolean shouldPushFetchAboveUnionToIntersection) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .setRemoveDuplicates(true)
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldPushFetchAboveUnionToIntersection);
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
                            myrec.mergeFrom(rec.getRecord());
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
                if (shouldPushFetchAboveUnionToIntersection) {
                    assertLoadRecord(60, context);
                }
            }
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on the same field are implemented using a union of indexes.
     */
    @ParameterizedTest
    @BooleanSource
    public void testOrQuery1(boolean shouldDeferFetch) throws Exception {
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

        if (shouldDeferFetch) {
            assertThat(plan, fetch(union(Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[4],[4]]")))))
            ), equalTo(primaryKey("MySimpleRecord")))));
            assertEquals(1912003491, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-468939975, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-30693330, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, union(Arrays.asList(
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[4],[4]]"))))
            ), equalTo(primaryKey("MySimpleRecord"))));
            assertEquals(273143354, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1604557478, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2042804123, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
    @ParameterizedTest
    @BooleanSource
    public void testOrQueryPlanEquals(boolean shouldDeferFetch) throws Exception {
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
    @ParameterizedTest
    @BooleanSource
    public void testOrQuery2(boolean shouldDeferFetch) throws Exception {
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
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch) {
            assertThat(plan, fetch(union(Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([3],>")))))
            ), equalTo(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))))));
            assertEquals(504228282, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1512332881, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1355721875, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, union(Arrays.asList(
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([3],>"))))
            ), equalTo(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertEquals(1299166123, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(561164572, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(717775578, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
    @ParameterizedTest
    @BooleanSource
    public void testOrQuery3(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").lessThan(2),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();

        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
        RecordQueryPlan plan = planner.plan(query);
        if (shouldDeferFetch) {
            assertThat(plan, fetch(union(
                    coveringIndexScan(indexScan(allOf(
                            indexName("MySimpleRecord$num_value_3_indexed"),
                            bounds(hasTupleString("([null],[2])"))))),
                    coveringIndexScan(indexScan(allOf(
                            indexName("MySimpleRecord$num_value_3_indexed"),
                            bounds(hasTupleString("([3],>"))))),
                    equalTo(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))))));
            assertEquals(-627934247, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1185237037, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-314729194, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, union(
                    indexScan(allOf(
                            indexName("MySimpleRecord$num_value_3_indexed"),
                            bounds(hasTupleString("([null],[2])")))),
                    indexScan(allOf(
                            indexName("MySimpleRecord$num_value_3_indexed"),
                            bounds(hasTupleString("([3],>")))),
                    equalTo(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
            assertEquals(-1930405164, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(956189436, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1826697279, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
    @ParameterizedTest
    @BooleanSource
    public void testOrQuery4(boolean shouldDeferFetch) throws Exception {
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
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch) {
            assertThat(plan, fetch(union(Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]")))))
            ), equalTo(primaryKey("MySimpleRecord"))))); // ordered by primary key, since the fields are not the same.
            assertEquals(-417814093, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-607870693, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1119620137, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, union(Arrays.asList(
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]"))))
            ), equalTo(primaryKey("MySimpleRecord")))); // ordered by primary key, since the fields are not the same.
            assertEquals(-673254486, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1465626760, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(953877316, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
    public void testOrQuery5(boolean removesDuplicates) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .setRemoveDuplicates(removesDuplicates)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        Matcher<RecordQueryPlan> planMatcher = unorderedUnion(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("([null],[m])")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([3],>"))))
        );
        if (removesDuplicates) {
            planMatcher = primaryKeyDistinct(planMatcher);
        }
        assertThat(plan, planMatcher);
        assertEquals(removesDuplicates ? -1569447744 : -1569447745, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(removesDuplicates ? 315019783 : 1110199437, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(removesDuplicates ? 1086710879 : 1881890533, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            context.getTimer().reset();
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
    private static Stream<Arguments> query5WithLimitsArgs() {
        return Stream.of(1, 2, 5, 7).flatMap(i -> Stream.of(Arguments.of(i, false), Arguments.of(i, true)));
    }

    @MethodSource("query5WithLimitsArgs")
    @ParameterizedTest(name = "testOrQuery5WithLimits [limit = {0}, removesDuplicates = {1}]")
    @DualPlannerTest
    public void testOrQuery5WithLimits(int limit, boolean removesDuplicates) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .setRemoveDuplicates(removesDuplicates)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        Matcher<RecordQueryPlan> planMatcher = unorderedUnion(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("([null],[m])")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([3],>"))))
        );
        if (removesDuplicates) {
            planMatcher = primaryKeyDistinct(planMatcher);
        }
        assertThat(plan, planMatcher);
        assertEquals(removesDuplicates ? -1569447744 : -1569447745, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(removesDuplicates ? 315019783 : 1110199437, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(removesDuplicates ? 1086710879 : 1881890533, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
                        myrec.mergeFrom(rec.getRecord());
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
    @Test
    public void testOrQuery6() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("str_value_3_index",
                    "str_value_indexed", "num_value_3_indexed"));
        };
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.and(
                                Query.field("str_value_indexed").equalsValue("even"),
                                Query.field("num_value_3_indexed").greaterThan(3)),
                        Query.field("num_value_3_indexed").lessThan(1)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(
                indexScan(allOf(indexName("str_value_3_index"), bounds(hasTupleString("([even, 3],[even]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([null],[1])"))))));
        assertEquals(1721396731, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-974564602, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1796736412, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
    @ParameterizedTest
    @BooleanSource
    public void testUnorderableOrQueryWithAnd(boolean removesDuplicates) throws Exception {
        RecordMetaDataHook hook = metaDataBuilder -> {
            complexQuerySetupHook().apply(metaDataBuilder);
            metaDataBuilder.addIndex("MySimpleRecord", new Index("multi_index_2", "str_value_indexed", "num_value_3_indexed"));
        };
        complexQuerySetup(hook);
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
        RecordQueryPlan plan = planner.plan(query);
        Matcher<RecordQueryPlan> planMatcher = unorderedUnion(
                indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("([even, null],[even, 1]]")))),
                indexScan(allOf(indexName("multi_index_2"), bounds(hasTupleString("[[even, 3],[even]]"))))
        );
        if (removesDuplicates) {
            planMatcher = primaryKeyDistinct(planMatcher);
        }
        assertThat(plan, planMatcher);
        assertEquals(removesDuplicates ? -173785610 : -173785611, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(removesDuplicates ? -339226767 : 455952887, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(removesDuplicates ? 246627957 : 1041807611, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
    @Test
    public void testOrQuery7() throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.or(
                                Query.field("num_value_3_indexed").equalsValue(1),
                                Query.field("num_value_3_indexed").greaterThan(3))))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(
                indexScan(allOf(indexName("str_value_3_index"), bounds(hasTupleString("[[even, 1],[even, 1]]")))),
                indexScan(allOf(indexName("str_value_3_index"), bounds(hasTupleString("([even, 3],[even]]")))),
                // note that the primary key is (str_value_indexed, num_value_unique), but "str_value_indexed" is not repeated.
                equalTo(concat(field("str_value_indexed"), field("num_value_3_indexed"), field("num_value_unique")))));
        assertEquals(-94975810, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1385328582, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1892351448, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
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
     */
    @Test
    public void testOrQueryOrdered() throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .setSort(field("num_value_3_indexed"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]")))),
                equalTo(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
        assertEquals(1412961915, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(36691391, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1956934243, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    int numValue3 = myrec.getNumValue3Indexed();
                    assertTrue(numValue3 == 1 || numValue3 == 3, "should satisfy value condition");
                    assertTrue(numValue3 == 1 || i >= 20, "lower values should come first");
                    i++;
                }
            }
            assertEquals(20 + 20, i);
            assertDiscardedNone(context);
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void testOrQueryChildReordering(boolean shouldPushFetchAboveUnionToIntersection) throws Exception {
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
        RecordQueryPlan plan1 = planner.plan(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(0),
                        Query.field("str_value_indexed").equalsValue("odd")))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertNotEquals(plan1, plan2);
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());
        assertTrue(plan1.semanticEquals(plan2));
        if (shouldPushFetchAboveUnionToIntersection) {
            assertEquals(-1584186103, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1194358007, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-459005480, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(-91575587, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2152249, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-349034358, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-2067012572, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(947068466, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1682420993, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(600484528, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2143578722, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1792392115, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
                    assertEquals(rec1.getRecord(), rec2.getRecord());
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

    @ParameterizedTest
    @BooleanSource
    public void testOrQueryChildReordering2(boolean shouldDeferFetch) throws Exception {
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
        RecordQueryPlan plan1 = planner.plan(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(Lists.reverse(((OrComponent)query1.getFilter()).getChildren())))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertNotEquals(plan1, plan2);
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());
        assertTrue(plan1.semanticEquals(plan2));
        
        if (shouldDeferFetch) {
            assertEquals(770691035, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-234864047, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1125345961, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(1289607451, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2058498961, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1901237353, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(723665474, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1838633406, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(948151492, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(184229634, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-162970882, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(172260100, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
                    assertEquals(rec1.getRecord(), rec2.getRecord());
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
     * Verify that an OR on two indexed fields with compatibly ordered indexes is implemented by a union, and that the
     * union cursors works properly with a returned record limit.
     */
    @ParameterizedTest
    @BooleanSource
    public void testComplexLimits5(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch) {
            assertThat(plan, fetch(union(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))))));
            assertEquals(-1584186334, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1194173309, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-458820782, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, union(
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))));
            assertEquals(-2067012605, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(947253164, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1682605691, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(5).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());

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
    @Test
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
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(
                indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("[[even, 0, 0],[even, 0, 0]]")))),
                indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("[[even, 0, 2],[even, 0, 3]]"))))));
        assertEquals(-2074065439, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1105764760, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(2041868485, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertTrue((myrec.getNumValue2() % 3) == 0);
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
    @Test
    public void testOrQuerySplitContinuations() throws Exception {
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
        RecordQueryPlan plan = planner.plan(query);
        Matcher<RecordQueryPlan> leaf = indexScan(allOf(
                indexName("MySimpleRecord$num_value_3_indexed"),
                bounds(anyOf(hasTupleString("[[1],[1]]"), hasTupleString("[[3],[3]]"), hasTupleString("[[5],[5]]")))));
        assertThat(plan, union(everyLeaf(is(leaf)), everyLeaf(is(leaf))));
        assertEquals(273143386, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1634110150, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(2042804123, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
                            myrec.mergeFrom(rec.getRecord());
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
     */
    @Test
    public void testOrQueryNoIndex() throws Exception {
        RecordMetaDataHook hook = metadata -> {
            metadata.removeIndex("MySimpleRecord$num_value_3_indexed");
        };
        complexQuerySetup(hook);
        QueryComponent orComponent = Query.or(
                Query.field("num_value_3_indexed").equalsValue(1),
                Query.field("num_value_3_indexed").equalsValue(2),
                Query.field("num_value_3_indexed").equalsValue(4));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("str_value_indexed").equalsValue("even"), orComponent))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, filter(orComponent, indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]"))))));
        assertEquals(-1553701984, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(925115369, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1299086656, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even"));
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
    public void unionVisitorOnComplexComparisonKey() throws Exception {
        complexQuerySetup(null);

        RecordQueryPlan originalPlan1 = RecordQueryUnionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                primaryKey("MySimpleRecord"), true);

        RecordQueryPlan modifiedPlan1 = RecordQueryPlannerSubstitutionVisitor.applyVisitors(originalPlan1, recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        assertThat(modifiedPlan1, fetch(union(
                coveringIndexScan(indexScan("MySimpleRecord$str_value_indexed")), coveringIndexScan(indexScan("MySimpleRecord$num_value_3_indexed")))));

        RecordQueryPlan originalPlan2 = RecordQueryUnionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false),
                concat(field("num_value_2"), primaryKey("MySimpleRecord")), true);
        RecordQueryPlan modifiedPlan2 = RecordQueryPlannerSubstitutionVisitor.applyVisitors(originalPlan2,  recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        // Visitor should not perform transformation because of comparison key on num_value_unique
        assertEquals(originalPlan2, modifiedPlan2);
    }

    @Test
    public void deferFetchOnUnionWithInnerFilter() throws Exception {
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

        assertThat(plan, fetch(primaryKeyDistinct(unorderedUnion(ImmutableList.of(
                coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("{[foo],[foo]}"))))),
                coveringIndexScan(indexScan(allOf(indexName("coveringIndex"), bounds(hasTupleString("[[2],[4]]"))))),
                filter(Query.field("num_value_3_indexed").lessThanOrEquals(18),
                        coveringIndexScan(indexScan(allOf(indexName("coveringIndex"), bounds(hasTupleString("[[26],>")))))))))));
    }
}
