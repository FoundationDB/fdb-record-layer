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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
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
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortAdapter;
import com.apple.foundationdb.record.sorting.SortEvents;
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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
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
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.sort;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of how the planner selects indexes for queries with a defined sort order.
 */
@Tag(Tags.RequiresFDB)
class FDBSortQueryIndexSelectionTest extends FDBRecordStoreQueryTestBase {
    @SuppressWarnings("unused") // used through reflection
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
                        PlannableIndexTypes.DEFAULT.getRankTypes(), PlannableIndexTypes.DEFAULT.getTextTypes())));
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
    public void sortOnly() {
        sortOnlyUnique(NO_HOOK);
    }

    private void sortOnlyUnique(RecordMetaDataHook hook) {
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

        // Index(MySimpleRecord$num_value_unique <,>)
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded())));
        assertEquals(-1130465929, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-491910604, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-491910604, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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
    void sortWithScannableFilterOnIndex(RecordMetaDataHook hook, PlannableIndexTypes indexTypes) throws Exception {
        setupSimpleRecordStore(hook, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2).setNumValue3Indexed(i % 3));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").greaterThanOrEquals(2))
                .setSort(field("num_value_3_indexed"))
                .build();
        setupPlanner(indexTypes);

        // Index(MySimpleRecord$num_value_3_indexed [[2],>)
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],>")))));
        assertEquals(1008857205, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-2059045225, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1347749581, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
    void sortWithNonScannableFilterOnIndex(RecordMetaDataHook hook, PlannableIndexTypes indexTypes) throws Exception {
        setupSimpleRecordStore(hook, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2).setNumValue3Indexed(i % 3));

        final QueryComponent filter = Query.field("num_value_3_indexed").notEquals(1);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("num_value_3_indexed"))
                .build();
        setupPlanner(indexTypes);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed <,>) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) | num_value_3_indexed NOT_EQUALS 1)
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, fetch(filter(filter,
                coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), unbounded()))))));
        assertEquals(-1303978120, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1548200279, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-856491930, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
    void sortWithNonScannableFilterWithAnd(RecordMetaDataHook hook, PlannableIndexTypes indexTypes) throws Exception {
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

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed <,>) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) | num_value_3_indexed NOT_EQUALS 1) | num_value_2 EQUALS 0
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan,
                filter(Query.field("num_value_2").equalsValue(0),
                        fetch(
                                filter(Objects.requireNonNull(Query.field("num_value_3_indexed").notEquals(1)),
                                        coveringIndexScan(
                                                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), unbounded())))))));
        assertEquals(-2013739934, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1437222023, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-867524414, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
    void sortByPrimaryKey(boolean reverse) throws Exception {
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
        assertThat("unexpected plan for filter: " + filter, plan, planMatcher);
        assertEquals(planHash, plan.planHash(PlanHashable.PlanHashKind.LEGACY), "unexpected plan hash for filter: " + filter);

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

        assertEquals(expectedReturn, returned, "unexpected return count for filter: " + filter);
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
                filter(filter, typeFilter(contains("MySimpleRecord"), scan(unbounded()))),
                checkRecord
        );
    }

    /**
     * Verify that we can still sort by primary key without an index but with filters.
     */
    @ParameterizedTest(name = "sortByPrimaryKeyWithFilter() [{0}]")
    @BooleanSource
    void sortByPrimaryKeyWithFilter(boolean reverse) throws Exception {
        setupSimpleRecordStore(NO_HOOK, (i, builder) -> builder.setRecNo(i).setNumValue2(i % 2).setNumValue3Indexed(i % 3));

        // Case 1: Equality filter on a single field with primary key as next field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_3_indexed").equalsValue(0),
                reverse,
                reverse ? -1828364111 : -1828364112,
                34, 0,
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]")))),
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

        // Case 4: Equality filter on an un-indexed field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_2").equalsValue(1),
                reverse,
                reverse ? 913370524 : 913370523,
                50,
                builder -> assertThat(builder.getNumValue2(), equalTo(1))
        );

        // Case 5: Inequality filter on an un-indexed field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_2").lessThan(2),
                reverse,
                reverse ? 2042689125 : 2042689124,
                100,
                builder -> assertThat(builder.getNumValue2(), lessThan(2))
        );

        // Case 6: Filter that cannot be accomplished by a scan on an un-indexed field.
        sortByPrimaryKeyWithFilter(
                Query.field("num_value_2").notEquals(1),
                reverse,
                reverse ? 490888360 : 490888359,
                50,
                builder -> assertThat(builder.getNumValue2(), not(equalTo(2)))
        );

        // Case 7: And query with filter on one indexed and one un-indexed field.
        sortByPrimaryKeyWithFilter(
                Query.and(
                    Query.field("num_value_2").equalsValue(1),
                    Query.field("num_value_3_indexed").equalsValue(0)
                ),
                reverse,
                reverse ? 1756841372 : 1756841371,
                17, 20,
                filter(Query.field("num_value_2").equalsValue(1),
                        indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))),
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

        // Index(MySimpleRecord$num_value_unique <,>)
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded())));
        assertEquals(-1130465929, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-491910604, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-491910604, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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

        // Index(MySimpleRecord$num_value_unique <,> REVERSE)
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded())));
        assertTrue(plan.isReverse(), "plan should have reversal");
        assertEquals(-1130465928, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-491910790, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-491910790, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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
        final QueryComponent filter = Query.field("str_value_indexed").equalsValue("even");
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("num_value_3_indexed"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, filter(filter,
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), unbounded()))));
        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-1429997503, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        // TODO: Issue https://github.com/FoundationDB/fdb-record-layer/issues/1074
        // assertEquals(-1729416480, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(952181942, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
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

        // Index(MySimpleRecord$str_value_indexed <,> REVERSE)
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), unbounded())));
        assertTrue(plan.isReverse(), "plan is reversed");
        assertEquals(324762955, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(19722195, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(19722195, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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
    @SuppressWarnings("java:S5961")
    @Test
    void sortNested() {
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

                // Index(MyRecord$header_num <,>)
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), unbounded())));
                assertEquals(-1173952475, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(1008825832, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(1008825832, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                int i = 0;
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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

                // Index(MyRecord$header_num ([null],[50]))
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("([null],[50])")))));
                assertEquals(2008179964, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(2049006062, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(-204519612, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                int i = 0;
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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

                // Index(MyRecord$header_num [[1],[1]])
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("[[1],[1]]")))));
                assertEquals(878861315, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(653879397, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(998239886, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                int i = 0;
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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

                // Index(MyRecord$header_num ([null, middle],[null]])
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("([null, middle],[null]]")))));
                assertEquals(1553479768, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(1072001836, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(-1653404355, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            }
            {
                final QueryComponent filter = Query.field("header").matches(Query.field("rec_no").greaterThan(0L));
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MyRecord")
                        .setFilter(filter)
                        .setSort(field("header").nest("num"))
                        .build();

                // Fetch(Covering(Index(MyRecord$header_num <,>) -> [str_value: KEY[1], header: [num: KEY[0], path: KEY[2], rec_no: KEY[3]]]) | header/{rec_no GREATER_THAN 0})
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, fetch(filter(filter,
                        coveringIndexScan(indexScan(allOf(indexName("MyRecord$header_num"), unbounded()))))));
                assertEquals(673903077, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(-582153460, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(-421343502, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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

                // Fetch(Covering(Index(MyRecord$header_num ([null],[50])) -> [str_value: KEY[1], header: [num: KEY[0], path: KEY[2], rec_no: KEY[3]]]) | header/{rec_no GREATER_THAN 10})
                RecordQueryPlan plan = planner.plan(query);
                assertThat(plan, fetch(filter(Query.field("header").matches(Query.field("rec_no").greaterThan(10L)),
                        coveringIndexScan(indexScan(allOf(indexName("MyRecord$header_num"), bounds(hasTupleString("([null],[50])"))))))));
                assertEquals(1473993740, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(1598662608, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(619653398, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
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
    @SuppressWarnings("java:S5778")
    @Test
    void testUncommonPrimaryKeyWithSort() {
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
    @SuppressWarnings("java:S5778")
    @DualPlannerTest
    void testUncommonMultiIndex() {
        assertThrows(RecordCoreException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openMultiRecordStore(context);
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordTypes(Arrays.asList("MultiRecordOne", "MultiRecordThree", "MultiRecordTwo"))
                        .setSort(field("element", FanType.FanOut))
                        .setRemoveDuplicates(false)
                        .build();
                planner.plan(query);
            }
        });
    }

    @SuppressWarnings("java:S5778")
    @Test
    void twoSortOneNestedFilter() throws Exception {
        final RecordMetaDataHook hook = metaData ->
                metaData.addIndex("RestaurantReviewer", "schoolNameEmail", concat(
                        field("stats").nest(field("school_name")),
                        field("name"),
                        field("email")));
        nestedWithAndSetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.field("stats").matches(Query.field("school_name").equalsValue("Human University")))
                .setSort(concat(field("name"), field("email")))
                .build();

        // Index(schoolNameEmail [[Human University],[Human University]])
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("schoolNameEmail"), bounds(hasTupleString("[[Human University],[Human University]]")))));
        assertEquals(387659205, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1202542055, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1693682768, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
    void sortOnlyUniqueNull() {
        sortOnlyUnique(NULL_UNIQUE_HOOK);
    }
    
    /**
     * Verify that sort with filter works with different null interpretation on unique index.
     */
    @DualPlannerTest
    void sortUniqueNull() {
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

        // Index(MySimpleRecord$num_value_unique [[20],>)
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), bounds(hasTupleString("[[20],>")))));

        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-535398101, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1799532339, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1088253993, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-535398101, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1799532345, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1088253999, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, NULL_UNIQUE_HOOK);
            int i = 20;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals(i++, myrec.getNumValueUnique());
                }
            }
            assertEquals(100, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that sort that cannot be done with any index can be enabled for in-memory sort.
     */
    public enum SortWithoutIndexMode {
        DISALLOWED, MEMORY, FILE
    }

    @EnumSource(SortWithoutIndexMode.class)
    @ParameterizedTest(name = "sortWithoutIndex [mode = {0}]")
    public void sortWithoutIndex(SortWithoutIndexMode mode) throws Exception {
        final int numberOfRecordsToSave = 2000;
        final int numberOfResultsToReturn = 5;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (int i = 0; i < numberOfRecordsToSave; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo((2244 * i + 1649) % 2357); // Slightly larger prime.
                recBuilder.setNumValue2(i);
                recBuilder.setNumValue3Indexed(i % 5);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").greaterThanOrEquals(2))
                .setSort(field("num_value_2"), true)
                .build();

        if (mode == SortWithoutIndexMode.DISALLOWED) {
            assertThrows(RecordCoreException.class, () -> {
                planner.plan(query);
            });
            return;
        }

        ((RecordQueryPlanner)planner).setConfiguration(((RecordQueryPlanner)planner).getConfiguration().asBuilder().setAllowNonIndexSort(true).build());

        // Index(MySimpleRecord$num_value_3_indexed [[2],>) ORDER BY num_value_2 DESC
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, sort(allOf(hasProperty("reverse", equalTo(true))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],>"))))));
        assertEquals(256365917, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(172993081, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(748321565, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        // Skip + limit is enough to overflow memory buffer. Skips into middle of section.
        final int skip = mode == SortWithoutIndexMode.FILE ? numberOfRecordsToSave / 2 + 1 : 0;
        ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder().setSkip(skip).setReturnedRowLimit(numberOfResultsToReturn);
        final PrimitiveIterator.OfInt sortedInts = IntStream.iterate(numberOfRecordsToSave - 1, i -> i - 1)
                .filter(i -> i % 5 >= 2)
                .skip(skip).limit(numberOfResultsToReturn)
                .iterator();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            timer.reset();
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties.build())) {
                while (true) {
                    RecordCursorResult<FDBQueriedRecord<Message>> next = cursor.getNext();
                    if (!next.hasNext()) {
                        break;
                    }
                    FDBQueriedRecord<Message> rec = next.get();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(sortedInts.hasNext());
                    assertEquals(sortedInts.nextInt(), myrec.getNumValue2());
                }
            }
            assertFalse(sortedInts.hasNext());
            assertDiscardedNone(context);
            int countBeforeSorting = (numberOfRecordsToSave / 5) * 3;
            if (mode == SortWithoutIndexMode.MEMORY) {
                assertEquals(0, timer.getCount(SortEvents.Events.FILE_SORT_OPEN_FILE));
                assertEquals(countBeforeSorting, timer.getCount(SortEvents.Events.MEMORY_SORT_STORE_RECORD));
                assertEquals(numberOfResultsToReturn, timer.getCount(SortEvents.Events.MEMORY_SORT_LOAD_RECORD));
            } else {
                int nfiles = numberOfRecordsToSave / RecordQuerySortAdapter.DEFAULT_MAX_RECORD_COUNT_IN_MEMORY;
                assertEquals(nfiles, timer.getCount(SortEvents.Events.FILE_SORT_OPEN_FILE));
                assertEquals(nfiles - 1, timer.getCount(SortEvents.Events.FILE_SORT_MERGE_FILES));
                assertEquals(countBeforeSorting, timer.getCount(SortEvents.Events.FILE_SORT_SAVE_RECORD));
                assertEquals(numberOfResultsToReturn, timer.getCount(SortEvents.Events.FILE_SORT_LOAD_RECORD));
                assertEquals(skip / RecordQuerySortAdapter.DEFAULT_RECORD_COUNT_PER_SECTION, timer.getCount(SortEvents.Events.FILE_SORT_SKIP_SECTION));
                assertEquals(skip % RecordQuerySortAdapter.DEFAULT_RECORD_COUNT_PER_SECTION, timer.getCount(SortEvents.Events.FILE_SORT_SKIP_RECORD));
                assertThat(timer.getCount(SortEvents.Counts.FILE_SORT_FILE_BYTES), allOf(greaterThan(1000), lessThan(100000)));
            }
        }
    }

}
