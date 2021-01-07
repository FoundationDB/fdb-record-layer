/*
 * FDBSortQueryIndexSelectionTest.java
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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexMaintainerFactory;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.predicates.match.PredicateMatchers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedExactly;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of how the planner selects indexes for queries with a defined sort order.
 */
@Tag(Tags.RequiresFDB)
public class FDBSortQueryIndexSelectionTest extends FDBRecordStoreQueryTestBase {
    private static Stream<Arguments> hooks() {
        return Stream.of(
                Arguments.of(NO_HOOK, PlannableIndexTypes.DEFAULT),
                Arguments.of((RecordMetaDataHook) metadata -> {
                    String indexName = "MySimpleRecord$num_value_3_indexed";
                    KeyExpression root = metadata.getIndex(indexName).getRootExpression();
                    metadata.removeIndex(indexName);
                    metadata.addIndex("MySimpleRecord", new Index(indexName, root, "FAKE_TYPE"));
                    metadata.getIndex(indexName).setSubspaceKey(indexName + "_2");
                }, new PlannableIndexTypes(Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION, "FAKE_TYPE"),
                        PlannableIndexTypes.DEFAULT.getRankTypes(), PlannableIndexTypes.DEFAULT.getTextTypes(),
                        PlannableIndexTypes.DEFAULT.getLuceneTypes())));
    }

    /**
     * An index maintainer factory for the "FAKE_TYPE" index which shadows the VALUE index type.
     * This allows us to test the logic for handling other index types that declare that they are "planner compatible"
     * with another index type.
     */
    @AutoService(IndexMaintainerFactory.class)
    public static class FakeIndexMaintainerFactory extends ValueIndexMaintainerFactory {
        @Override
        @Nonnull
        public Iterable<String> getIndexTypes() {
            return Collections.singletonList("FAKE_TYPE");
        }
    }

    /**
     * Verify that simple sorts are implemented using index scans.
     */
    @DualPlannerTest
    public void sortOnly() throws Exception {
        sortOnlyUnique(NO_HOOK);
    }

    private void sortOnlyUnique(RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo((1096 * i + 722) % 1289); // Carter-Wegman hash, with large enough prime
                recBuilder.setNumValueUnique(i);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(field("num_value_unique"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded())));
        assertEquals(-1130465929, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1401341445, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1401341445, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(i++, myrec.getNumValueUnique());
                }
            }
            assertEquals(100, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that if the sort matches an index that can satisfy a filter that the index is used.
     */
    @ParameterizedTest
    @MethodSource("hooks")
    public void sortWithScannableFilterOnIndex(RecordMetaDataHook hook, PlannableIndexTypes indexTypes) throws Exception {
        setupSimpleRecordStore(hook, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2).setNumValue3Indexed(i % 3));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").greaterThanOrEquals(2))
                .setSort(field("num_value_3_indexed"))
                .build();
        setupPlanner(indexTypes);
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],>")))));
        assertEquals(1008857205, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-482062850, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1290401822, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        AtomicInteger lastNumValue3 = new AtomicInteger(Integer.MIN_VALUE);
        int returned = querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                builder -> {
                    assertThat(builder.getNumValue3Indexed(), greaterThanOrEqualTo(2));
                    assertThat(builder.getNumValue3Indexed(), greaterThanOrEqualTo(lastNumValue3.get()));
                    lastNumValue3.set(builder.getNumValue3Indexed());
                },
                TestHelpers::assertDiscardedNone);
        assertEquals(33, returned);
        setupPlanner(null); // reset planner
    }

    /**
     * Verify that if there is an index on the sorted field but it does not satisfy the comparison
     * of a filter (because that comparison cannot be accomplished with a scan) that the index
     * is still used solely to accomplish sorting.
     */
    @ParameterizedTest
    @MethodSource("hooks")
    public void sortWithNonScannableFilterOnIndex(RecordMetaDataHook hook, PlannableIndexTypes indexTypes) throws Exception {
        setupSimpleRecordStore(hook, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2).setNumValue3Indexed(i % 3));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").notEquals(1))
                .setSort(field("num_value_3_indexed"))
                .build();
        setupPlanner(indexTypes);
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, fetch(filter(query.getFilter(),
                coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), unbounded()))))));
        assertEquals(-1303978120, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1679419154, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(210855933, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        AtomicInteger lastNumValue3 = new AtomicInteger(Integer.MIN_VALUE);
        int returned = querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                builder -> {
                    assertThat(builder.getNumValue3Indexed(), not(equalTo(1)));
                    assertThat(builder.getNumValue3Indexed(), greaterThanOrEqualTo(lastNumValue3.get()));
                    lastNumValue3.set(builder.getNumValue3Indexed());
                },
                context -> assertDiscardedAtMost(33, context));
        assertEquals(67, returned);
        setupPlanner(null); // reset planner
    }

    /**
     * Verify that if we have an "and" query where one of the filters matches the sort but we can't use a scan
     * that the index is used only for sorting and not to satisfy the predicate.
     */
    @ParameterizedTest
    @MethodSource("hooks")
    public void sortWithNonScannableFilterWithAnd(RecordMetaDataHook hook, PlannableIndexTypes indexTypes) throws Exception {
        setupSimpleRecordStore(hook, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2).setNumValue3Indexed(i % 3));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_3_indexed").notEquals(1),
                        Query.field("num_value_2").equalsValue(0))
                )
                .setSort(field("num_value_3_indexed"))
                .build();
        setupPlanner(indexTypes);
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan,
                filter(PredicateMatchers.equivalentTo(Query.field("num_value_2").equalsValue(0)),
                        fetch(
                                filter(PredicateMatchers.equivalentTo(Query.field("num_value_3_indexed").notEquals(1)),
                                        coveringIndexScan(
                                                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), unbounded())))))));
        assertEquals(-2013739934, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1585790658, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-2139479029, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        AtomicInteger lastNumValue3 = new AtomicInteger(Integer.MIN_VALUE);
        int returned = querySimpleRecordStore(hook, plan, EvaluationContext::empty,
                builder -> {
                    assertThat(builder.getNumValue3Indexed(), not(equalTo(1)));
                    assertThat(builder.getNumValue2(), equalTo(0));
                    assertThat(builder.getNumValue3Indexed(), greaterThanOrEqualTo(lastNumValue3.get()));
                    lastNumValue3.set(builder.getNumValue3Indexed());
                },
                context -> assertDiscardedAtMost(66, context));
        assertEquals(34, returned);
        setupPlanner(null); // reset the planner
    }

    /**
     * Verify that we can sort by the primary key if possible.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "sortByPrimaryKey() [{0}]")
    @BooleanSource
    public void sortByPrimaryKey(boolean reverse) throws Exception {
        setupSimpleRecordStore(NO_HOOK, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(field("rec_no"), reverse)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, typeFilter(contains("MySimpleRecord"), scan(unbounded())));

        AtomicLong lastId = new AtomicLong(reverse ? 99L : 0L);
        int returned = querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty,
                builder -> assertThat(builder.getRecNo(), equalTo(reverse ? lastId.getAndDecrement() : lastId.getAndIncrement())),
                TestHelpers::assertDiscardedNone);
        assertEquals(100, returned);
    }

    private void sortByPrimaryKeyWithFilter(@Nonnull QueryComponent filter,
                                            boolean reverse,
                                            int planHash,
                                            int expectedReturn,
                                            int maxDiscarded,
                                            @Nonnull Matcher<RecordQueryPlan> planMatcher,
                                            @Nonnull TestHelpers.DangerousConsumer<TestRecords1Proto.MySimpleRecord.Builder> checkRecord) throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("rec_no"), reverse)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat("unexpected plan for filter: " + filter.toString(), plan, planMatcher);
        assertEquals(planHash, plan.planHash(PlanHashable.PlanHashKind.LEGACY), "unexpected plan hash for filter: " + filter.toString());

        AtomicLong lastId = new AtomicLong(reverse ? Long.MAX_VALUE : Long.MIN_VALUE);
        int returned = querySimpleRecordStore(NO_HOOK, plan, EvaluationContext::empty, builder -> {
            checkRecord.accept(builder);
            if (reverse) {
                assertThat(builder.getRecNo(), lessThan(lastId.get()));
            } else {
                assertThat(builder.getRecNo(), greaterThan(lastId.get()));
            }
            lastId.set(builder.getRecNo());
        }, context -> assertDiscardedAtMost(maxDiscarded, context));

        assertEquals(expectedReturn, returned, "unexpected return count for filter: " + filter.toString());
    }

    // Overloaded version of the above method that assumes that the filter is satisfied by a scan
    private void sortByPrimaryKeyWithFilter(@Nonnull QueryComponent filter,
                                            boolean reverse,
                                            int planHash,
                                            int expectedReturn,
                                            @Nonnull TestHelpers.DangerousConsumer<TestRecords1Proto.MySimpleRecord.Builder> checkRecord) throws Exception {
        sortByPrimaryKeyWithFilter(
                filter,
                reverse,
                planHash,
                expectedReturn,
                100 - expectedReturn,
                filter(PredicateMatchers.equivalentTo(filter), typeFilter(contains("MySimpleRecord"), scan(unbounded()))),
                checkRecord
        );
    }

    /**
     * Verify that we can still sort by primary key without an index but with filters.
     */
    @ParameterizedTest(name = "sortByPrimaryKeyWithFilter() [{0}]")
    @BooleanSource
    public void sortByPrimaryKeyWithFilter(boolean reverse) throws Exception {
        setupSimpleRecordStore(NO_HOOK, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2).setNumValue3Indexed(i % 3));

        // Case 1: Equality filter on a single field with primary key as next field.
        // TODO: Queries with order-preserving filter sorting by primary key will not use index for filter (https://github.com/FoundationDB/fdb-record-layer/issues/5)
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_3_indexed").equalsValue(0),
                reverse,
                reverse ? 831975758 : 831975757, // with index: -1828364112
                34,
                // with index: indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]")))
                builder -> assertThat(builder.getNumValue3Indexed(), equalTo(0))
        );

        // Case 2: Inequality filter on that same field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_3_indexed").greaterThan(0),
                reverse,
                reverse ? -248685451 : -248685452,
                66,
                builder -> assertThat(builder.getNumValue3Indexed(), greaterThan(0))
        );


        // Case 3: Filter that cannot become a scan comparison on that field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_3_indexed").notEquals(1),
                reverse,
                reverse ? 409493595 : 409493594,
                67,
                builder -> assertThat(builder.getNumValue3Indexed(), not(equalTo(1)))
        );

        // Case 4: Equality filter on an unindexed field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_2").equalsValue(1),
                reverse,
                reverse ? 913370524 : 913370523,
                50,
                builder -> assertThat(builder.getNumValue2(), equalTo(1))
        );

        // Case 5: Inequality filter on an unindexed field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_2").lessThan(2),
                reverse,
                reverse ? 2042689125 : 2042689124,
                100,
                builder -> assertThat(builder.getNumValue2(), lessThan(2))
        );

        // Case 6: Filter that cannot be accomplished by a scan on an unindexed field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_2").notEquals(1),
                reverse,
                reverse ? 490888360 : 490888359,
                50,
                builder -> assertThat(builder.getNumValue2(), not(equalTo(2)))
        );

        // Case 7: And query with filter on one indexed and one unindexed field.
        // TODO: Queries with order-preserving filter sorting by primary key will not use index for filter (https://github.com/FoundationDB/fdb-record-layer/issues/5)
        sortByPrimaryKeyWithFilter(
                Query.and(
                    Query.field("num_value_2").equalsValue(1),
                    Query.field("num_value_3_indexed").equalsValue(0)
                ),
                reverse,
                reverse ? 304196996 : 304196995, // with index: 1756841371
                17,
                // with index:
                // filter(equalTo(Query.field("num_value_2").equalsValue(1)),
                //        indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]")))))
                builder -> assertThat(builder.getNumValue2(), not(equalTo(2)))
        );

        // Case 8: And query with non-scannable filter as a component
        sortByPrimaryKeyWithFilter(
                Query.and(
                    Query.field("num_value_2").notEquals(1),
                    Query.field("num_value_3_indexed").greaterThan(1)
                ),
                reverse,
                reverse ? -988509408 : -988509409,
                17,
                builder -> {
                    assertThat(builder.getNumValue2(), not(equalTo(1)));
                    assertThat(builder.getNumValue3Indexed(), greaterThan(1));
                }
        );
    }

    /**
     * Verify that a sort is implemented using an appropriate index.
     */
    @DualPlannerTest
    public void testComplexQuery5() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(field("num_value_unique"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded())));
        assertEquals(-1130465929, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1401341445, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1401341445, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(901 + i, myrec.getNumValueUnique());
                    i++;
                }
            }
            assertEquals(100, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a sort can be implemented by traversing an index in the reverse order.
     */
    @DualPlannerTest
    public void testComplexQuery5r() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(field("num_value_unique"), true)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded())));
        assertTrue(plan.isReverse(), "plan should have reversal");
        assertEquals(-1130465928, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1401341451, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1401341451, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(1000 - i, myrec.getNumValueUnique());
                    i++;
                }
            }
            assertEquals(100, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a query with a filter on one field and a sort on another uses the index of the sort preferentially,
     * and falls back to filtering to implement the filter if an appropriate multi-field index is not available.
     */
    @DualPlannerTest
    public void testComplexQuery8x() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .setSort(field("num_value_3_indexed"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, filter(query.getFilter(),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), unbounded()))));
        assertEquals(-1429997503, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        // TODO: Issue https://github.com/FoundationDB/fdb-record-layer/issues/1074
        // assertEquals(-1729416480, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    i++;
                }
            }
            assertEquals(50, i);
            assertDiscardedAtMost(50, context);
        }
    }

    /**
     * Verify that reverse sorts can be implemented by a reverse index scan.
     */
    @DualPlannerTest
    public void testComplexLimits1() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(field("str_value_indexed"), true)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), unbounded())));
        assertTrue(plan.isReverse(), "plan is reversed");
        assertEquals(324762955, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-2077573822, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-2077573822, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("odd", myrec.getStrValueIndexed());
                    i += 1;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that sorts on nested fields are implemented using nested record field indexes.
     */
    @Test
    public void sortNested() throws Exception {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
            builder.addIndex("MyRecord", "MyRecord$header_num", concat(field("header").nest("num"),
                    field("str_value")));
            RecordMetaData metaData = builder.getRecordMetaData();
            createOrOpenRecordStore(context, metaData);

            for (int i = 0; i < 100; i++) {
                TestRecordsWithHeaderProto.MyRecord.Builder recBuilder = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                TestRecordsWithHeaderProto.HeaderRecord.Builder headerBuilder = recBuilder.getHeaderBuilder();
                headerBuilder.setRecNo((1096 * i + 722) % 1289); // Carter-Wegman hash, with large enough prime
                headerBuilder.setPath("root");
                headerBuilder.setNum(i);
                recBuilder.setStrValue(Integer.toString(i));
                recordStore.saveRecord(recBuilder.build());
            }
            {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MyRecord")
                        .setSort(field("header").nest("num"))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), unbounded())));
                assertEquals(-1173952475, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(171090061, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(171090061, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                int i = 0;
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertEquals(i++, myrec.getHeader().getNum());
                    }
                }
                assertEquals(100, i);
                assertDiscardedNone(context);
            }
            {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MyRecord")
                        .setFilter(Query.field("header").matches(Query.field("num").lessThan(50)))
                        .setSort(field("header").nest("num"))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("([null],[50])")))));
                assertEquals(2008179964, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(-1042281727, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(1655970539, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                int i = 0;
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertEquals(i++, myrec.getHeader().getNum());
                    }
                }
                assertEquals(50, i);
                assertDiscardedNone(context);
            }
            {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MyRecord")
                        .setFilter(Query.field("header").matches(Query.field("num").equalsValue(1)))
                        .setSort(field("str_value"))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("[[1],[1]]")))));
                assertEquals(878861315, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(159640176, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(309295911, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                int i = 0;
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertEquals(1, myrec.getHeader().getNum());
                        i++;
                    }
                }
                assertEquals(1, i);
                assertDiscardedNone(context);
            }
            {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MyRecord")
                        .setFilter(Query.and(
                                Query.field("header").matches(Query.field("num").isNull()),
                                Query.field("str_value").greaterThan("middle")))
                        .setSort(field("str_value"))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("([null, middle],[null]]")))));
                assertEquals(1553479768, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(588769993, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(223759000, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            }
            {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MyRecord")
                        .setFilter(Query.field("header").matches(Query.field("rec_no").greaterThan(0L)))
                        .setSort(field("header").nest("num"))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, fetch(filter(query.getFilter(),
                        coveringIndexScan(indexScan(allOf(indexName("MyRecord$header_num"), unbounded()))))));
                assertEquals(673903077, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(-782158585, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(-621348627, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertTrue(myrec.hasHeader(), "Retrieved record missing header");
                        assertTrue(myrec.getHeader().hasRecNo(), "Retrieved record missing rec_no");

                        long recNo = myrec.getHeader().getRecNo();
                        assertTrue(recNo > 0L, "Record does not match filter (rec_no " + recNo + "<= 0)");
                    }
                }
                assertDiscardedExactly(0, context);
                clearStoreCounter(context);
            }
            {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MyRecord")
                        .setFilter(Query.field("header").matches(
                                Query.and(
                                        Query.field("rec_no").greaterThan(10L),
                                        Query.field("num").lessThan(50)
                                )))
                        .setSort(field("header").nest("num"))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, fetch(filter(Query.field("header").matches(Query.field("rec_no").greaterThan(10L)),
                        coveringIndexScan(indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("([null],[50])"))))))));
                assertEquals(1473993740, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(258021661, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(-1834694065, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertTrue(myrec.hasHeader(), "Retrieved record missing header");
                        assertTrue(myrec.getHeader().hasRecNo(), "Retrieved record missing rec_no");
                        assertTrue(myrec.getHeader().hasNum(), "Retrieved record missing num");

                        long recNo = myrec.getHeader().getRecNo();
                        int num = myrec.getHeader().getNum();
                        assertTrue(recNo > 10L && num < 50, "Retrieved record does not match filter (rec_no = " + recNo + " and num = " + num + ")");
                    }
                }
                assertDiscardedExactly(0, context);
            }
        }
    }

    /**
     * Verify that the planner does not accept sorts on multiple record types with uncommon primary keys.
     */
    @Test
    public void testUncommonPrimaryKeyWithSort() throws Exception {
        assertThrows(RecordCoreException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openMultiRecordStore(context);

                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordTypes(Arrays.asList("MultiRecordOne", "MultiRecordTwo"))
                        .setSort(field("element", FanType.FanOut))
                        .setRemoveDuplicates(true)
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                System.out.println("Uncommon key plan: " + plan);
            }
        });
    }

    /**
     * Verify that the planner does not accept sorts on multiple record types with uncommon primary keys, even when a
     * multi-field index exists.
     */
    @DualPlannerTest
    public void testUncommonMultiIndex() throws Exception {
        assertThrows(RecordCoreException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openMultiRecordStore(context);
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordTypes(Arrays.asList("MultiRecordOne", "MultiRecordThree", "MultiRecordTwo"))
                        .setSort(field("element", FanType.FanOut))
                        .setRemoveDuplicates(false)
                        .build();
                RecordQueryPlan plan = planner.plan(query);
            }
        });
    }

    @Test
    public void twoSortOneNestedFilter() throws Exception {
        final RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("RestaurantReviewer", "schoolNameEmail", concat(
                    field("stats").nest(field("school_name")),
                    field("name"),
                    field("email")));
        };
        nestedWithAndSetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.field("stats").matches(Query.field("school_name").equalsValue("Human University")))
                .setSort(concat(field("name"), field("email")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("schoolNameEmail"), bounds(hasTupleString("[[Human University],[Human University]]")))));
        assertEquals(387659205, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1346681682, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(915196357, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    static final RecordMetaDataHook NULL_UNIQUE_HOOK = md -> {
        md.removeIndex("MySimpleRecord$num_value_unique");
        final Index index = new Index("MySimpleRecord$num_value_unique",
                field("num_value_unique", FanType.None, Key.Evaluated.NullStandin.NULL_UNIQUE),
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        index.setSubspaceKey("MySimpleRecord$num_value_unique_2");  // Otherwise fails validation
        md.addIndex("MySimpleRecord", index);
    };

    /**
     * Verify that sort only works with different null interpretation on unique index.
     */
    @DualPlannerTest
    public void sortOnlyUniqueNull() throws Exception {
        sortOnlyUnique(NULL_UNIQUE_HOOK);
    }
    
    /**
     * Verify that sort with filter works with different null interpretation on unique index.
     */
    @DualPlannerTest
    public void sortUniqueNull() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NULL_UNIQUE_HOOK);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo((1096 * i + 722) % 1289);
                recBuilder.setNumValueUnique(i);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThanOrEquals(20))
                .setSort(field("num_value_unique"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), bounds(hasTupleString("[[20],>")))));
        assertEquals(-535398101, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1997712120, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1488915646, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NULL_UNIQUE_HOOK);
            int i = 20;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(i++, myrec.getNumValueUnique());
                }
            }
            assertEquals(100, i);
            assertDiscardedNone(context);
        }
    }

}
