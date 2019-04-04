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
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedExactly;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.everyLeaf;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    @Test
    public void testComplexQuery6() throws Exception {
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
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))));
        assertTrue(plan.getQueryPlanChildren().stream().allMatch(QueryPlan::isReverse));
        assertEquals(-2067012572, plan.planHash());

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
        }
    }

    @Test
    public void testComplexQuery6Continuations() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .setRemoveDuplicates(true)
                .build();
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
            }
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on the same field are implemented using a union of indexes.
     */
    @Test
    public void testOrQuery1() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(4)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(Arrays.asList(
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[4],[4]]"))))
                ), equalTo(primaryKey("MySimpleRecord"))));
        assertEquals(273143354, plan.planHash());

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
        }
    }

    /**
     * Verify that queries with an OR of a mix of equality and inequality predicates on the same field are implemented
     * using a union of indexes.
     */
    @Test
    public void testOrQuery2() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        Matcher<RecordQueryPlan> leaf = indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"),
                bounds(anyOf(hasTupleString("[[1],[1]]"), hasTupleString("[[2],[2]]"), hasTupleString("([3],>")))));
        assertThat(plan, union(Arrays.asList(
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([3],>"))))
                ), equalTo(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
        assertEquals(1299166123, plan.planHash());

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
    @Test
    public void testOrQuery3() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").lessThan(2),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(
                indexScan(allOf(
                        indexName("MySimpleRecord$num_value_3_indexed"),
                        bounds(hasTupleString("([null],[2])")))),
                indexScan(allOf(
                        indexName("MySimpleRecord$num_value_3_indexed"),
                        bounds(hasTupleString("([3],>")))),
                equalTo(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")))));
        assertEquals(-1930405164, plan.planHash());

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
    @Test
    public void testOrQuery4() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(Arrays.asList(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]"))))
        ), equalTo(primaryKey("MySimpleRecord")))); // ordered by primary key, since the fields are not the same.
        assertEquals(-673254486, plan.planHash());

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
        }
    }

    /**
     * Verify that an OR of inequalities on different fields uses an unordered union, since there is no compatible ordering.
     */
    @Test
    public void testOrQuery5() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, primaryKeyDistinct(unorderedUnion(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("([null],[m])")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([3],>")))))));
        assertEquals(-1569447744, plan.planHash());

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
            assertEquals(50 + 10, i);
            assertDiscardedAtMost(10, context);
        }
    }

    @ValueSource(ints = {1, 2, 5, 7})
    @ParameterizedTest(name = "testOrQuery5WithLimits [limit = {0}]")
    public void testOrQuery5WithLimits(int limit) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, primaryKeyDistinct(unorderedUnion(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("([null],[m])")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("([3],>")))))));
        assertEquals(-1569447744, plan.planHash());

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
                        assertThat(keysThisIteration.add(rec.getPrimaryKey()), is(true));
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
     * Verify that a complex query with with an OR of an AND produces a union plan if appropriate indexes are defined.
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
        assertEquals(1721396731, plan.planHash());

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
        assertEquals(-94975810, plan.planHash());

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
        assertEquals(1412961915, plan.planHash());

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

    @Test
    public void testOrQueryChildReordering() throws Exception {
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
        RecordQueryPlan plan1 = planner.plan(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(0),
                        Query.field("str_value_indexed").equalsValue("odd")))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertEquals(plan1.hashCode(), plan2.hashCode());
        assertEquals(plan1, plan2);
        assertEquals(-2067012572, plan1.planHash());
        assertEquals(600484528, plan2.planHash());

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
        }
    }

    @Test
    public void testOrQueryChildReordering2() throws Exception {
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
        RecordQueryPlan plan1 = planner.plan(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(Lists.reverse(((OrComponent)query1.getFilter()).getChildren())))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertEquals(plan1.hashCode(), plan2.hashCode());
        assertEquals(plan1, plan2);
        assertEquals(723665474, plan1.planHash());
        assertEquals(184229634, plan2.planHash());

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
        }
    }

    /**
     * Verify that an OR on two indexed fields with compatibly ordered indexes is implemented by a union, and that the
     * union cursors works properly with a returned record limit.
     */
    @Test
    public void testComplexLimits5() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, union(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))));
        assertEquals(-2067012605, plan.planHash());

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
        assertEquals(1921174117, plan.planHash());

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
        assertEquals(273143386, plan.planHash());
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
}
