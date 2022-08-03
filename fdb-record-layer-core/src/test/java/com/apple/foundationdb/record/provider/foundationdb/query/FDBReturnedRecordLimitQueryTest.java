/*
 * FDBReturnedRecordLimitQueryTest.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.descendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of query execution with a limit on the number of records that can be returned. These limits are used mostly
 * as a way to test that various queries execute properly with continuations.
 */
@Tag(Tags.RequiresFDB)
class FDBReturnedRecordLimitQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that a returned record limit works properly against a query with a filter on one field and a sort on another,
     * when the filter field is un-indexed and the sort is in reverse order.
     */
    @ParameterizedTest
    @BooleanSource
    void testComplexLimits2(final boolean shouldOptimizeForIndexFilters) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        final QueryComponent filter = Query.field("num_value_2").equalsValue(0);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setSort(field("str_value_indexed"), true)
                .build();
        setOptimizeForIndexFilters(shouldOptimizeForIndexFilters);

        // Index(MySimpleRecord$str_value_indexed <,> REVERSE) | num_value_2 EQUALS 0
        // Fetch(Covering(Index(multi_index <,> REVERSE) -> [num_value_2: KEY[1], num_value_3_indexed: KEY[2], rec_no: KEY[3], str_value_indexed: KEY[0]]) | num_value_2 EQUALS 0)
        RecordQueryPlan plan = planner.plan(query);

        if (shouldOptimizeForIndexFilters) {
            assertThat(plan, fetch(filter(filter,
                    coveringIndexScan(indexScan(allOf(indexName("multi_index"), unbounded()))))));
            assertTrue(plan.isReverse());
            assertEquals(-1143466156, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(915163788, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1279091452, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, filter(filter,
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), unbounded()))));
            assertTrue(plan.isReverse());
            assertEquals(-384998859, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1575402371, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(525309685, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals(0, myrec.getNumValue2());
                    assertEquals("odd", myrec.getStrValueIndexed());
                    i += 1;
                }
            }
            assertEquals(10, i);
            assertDiscardedAtMost(34, context);
        }
    }

    /**
     * Verify that a returned record limit works properly against a filter on an un-indexed field.
     */
    @DualPlannerTest
    void testComplexLimits3() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(0))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, descendant(scan(unbounded())));
        if (planner instanceof RecordQueryPlanner) {
            assertEquals(913370522, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        // TODO: Issue https://github.com/FoundationDB/fdb-record-layer/issues/1074
        // assertEquals(389700036, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-1244637277, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals(0, myrec.getNumValue2());
                    i += 1;
                }
            }
            assertEquals(10, i);
            assertDiscardedAtMost(18, context);
        }
    }

    /**
     * Verify that a returned record limit works properly with type filters.
     */
    @DualPlannerTest
    void testComplexLimits6() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord").setAllowedIndexes(Collections.emptyList()).build();

        // Scan(<,>) | [MySimpleRecord]
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertThat(plan, typeFilter(contains("MySimpleRecord"), scan(unbounded())));
            assertEquals(1623132336, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1229687959, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1229687959, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, typeFilter(contains("MySimpleRecord"), scan(unbounded())));
            assertEquals(1623132336, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(846114080, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(846114080, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    i += 1;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a returned record limit works properly when no filter is needed.
     * Verify that a type filter that is trivially satisfied is elided from the final plan.
     */
    @DualPlannerTest
    void testComplexLimits7() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder().setRecordTypes(Arrays.asList("MySimpleRecord", "MyOtherRecord")).build();

        // Scan(<,>)
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertThat(plan, scan(unbounded()));
            assertEquals(2, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1305688464, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1305688464, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, scan(unbounded()));
            assertEquals(2, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-268895783, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-268895783, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator()) {
                while (cursor.hasNext()) {
                    cursor.next();
                    i += 1;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /*
     * Verify that a returned record limit works properly with a distinct filter.
     */
    @Test
    void testComplexLimits8() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("repeater").oneOfThem().equalsValue(2))
                .setRemoveDuplicates(true)
                .build();

        // Index(repeater$fanout [[2],[2]]) | UnorderedPrimaryKeyDistinct()
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("repeater$fanout"), bounds(hasTupleString("[[2],[2]]"))))));
        assertEquals(-784887967, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-173504614, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(170826084, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getRepeaterList().contains(2), "Repeater does not contain 2");
                    i += 1;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }
}
