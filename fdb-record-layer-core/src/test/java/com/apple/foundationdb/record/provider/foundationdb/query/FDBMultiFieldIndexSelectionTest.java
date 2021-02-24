/*
 * FDBMultiFieldIndexSelectionTest.java
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the planner's use of multi-field indexes.
 */
@Tag(Tags.RequiresFDB)
public class FDBMultiFieldIndexSelectionTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that a two field index can be used for queries on both the first field alone and both fields.
     */
    @DualPlannerTest
    public void testPrefixScalar() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", "prefix_scalar", concat(field("num_value_2"), field("num_value_3_indexed")));
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            TestRecords1Proto.MySimpleRecord.Builder recordBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();

            recordBuilder.setRecNo(1).setNumValue2(1).setNumValue3Indexed(1);
            recordStore.saveRecord(recordBuilder.build());

            recordBuilder.setRecNo(2).setNumValue2(2).setNumValue3Indexed(2);
            recordStore.saveRecord(recordBuilder.build());

            recordBuilder.setRecNo(3).setNumValue2(1).clearNumValue3Indexed();
            recordStore.saveRecord(recordBuilder.build());

            commit(context);
        }

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQueryPlan plan1 = planner.plan(query1);
        assertThat(plan1, indexScan(allOf(indexName("prefix_scalar"), bounds(hasTupleString("[[1],[1]]")))));
        assertEquals(339959201, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(314514450, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(464170185, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(1)))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertThat(plan2, indexScan(allOf(indexName("prefix_scalar"), bounds(hasTupleString("[[1, 1],[1, 1]]")))));
        assertEquals(-447322749, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-40431985, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(453584239, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            List<Long> recnos = recordStore.executeQuery(plan1)
                    .map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getRecNo())
                    .asList().join();
            assertEquals(Arrays.asList(3L, 1L), recnos);
            TestHelpers.assertDiscardedNone(context);
            clearStoreCounter(context);
            recnos = recordStore.executeQuery(plan2)
                    .map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getRecNo())
                    .asList().join();
            assertEquals(Arrays.asList(1L), recnos);
            TestHelpers.assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a complex query with an appropriate multi-field index uses the index.
     */
    @DualPlannerTest
    public void testComplexQuery2() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(3),
                        Query.field("num_value_2").equalsValue(0)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("multi_index"),
                bounds(hasTupleString("[[even, 0, 3],[even, 0, 3]]")))));
        assertEquals(657537200, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1649013125, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(656626838, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
                    assertTrue((myrec.getNumValue3Indexed() % 5) == 3);
                    i++;
                }
            }
            assertEquals(3, i);
            TestHelpers.assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a complex query with an appropriate multi-field index uses the index.
     */
    @DualPlannerTest
    public void testComplexQuery3() throws Exception {
        // new Index("multi_index", "str_value_indexed", "num_value_2", "num_value_3_indexed")
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_2").equalsValue(0),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("[[even, 0, 2],[even, 0, 3]]")))));
        assertEquals(2137890746, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(2076121538, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-720747267, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
                    assertTrue((myrec.getNumValue3Indexed() % 5) >= 2);
                    assertTrue((myrec.getNumValue3Indexed() % 5) <= 3);
                    i++;
                }
            }
            assertEquals(6, i);
            TestHelpers.assertDiscardedNone(context);
        }
    }

    @DualPlannerTest
    public void testComplexQueryDenorm() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_2").equalsValue(0),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)))
                .build();

        RecordQueryPlan plan = planner.plan(query);
        RecordQuery queryDenorm = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.and(
                                Query.field("str_value_indexed").equalsValue("even"),
                                Query.field("num_value_2").equalsValue(0)),
                        Query.and(
                                Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                                Query.field("num_value_3_indexed").lessThanOrEquals(3))))
                .build();
        RecordQueryPlan planDenorm = planner.plan(queryDenorm);
        assertEquals(plan, planDenorm);
    }

    /**
     * Verify that a complex query with an appropriate multi-field index uses the index, even when bounds are complex.
     */
    @DualPlannerTest
    public void testComplexQuery4() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_2").equalsValue(0),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2)))
                .setSort(field("num_value_3_indexed"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(
                indexName("multi_index"),
                bounds(hasTupleString("[[even, 0, 2],[even, 0]]")))));
        assertEquals(1276767038, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1482243327, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-419919041, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
                    assertTrue((myrec.getNumValue3Indexed() % 5) >= 2);
                    i++;
                }
            }
            assertEquals(9, i);
            TestHelpers.assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a query with an equality condition filter on one field and a sort on another can make use of an
     * appropriate multi-field index that includes both of them.
     */
    @Test
    public void testComplexQuery8() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .setSort(field("num_value_2"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("[[even],[even]]")))));
        assertEquals(1375215309, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-445612552, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(995377110, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
            TestHelpers.assertDiscardedNone(context);
        }
    }
}
