/*
 * FDBCrossRecordQueryTest.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of query planning and execution on cross-record indexes.
 */
@Tag(Tags.RequiresFDB)
public class FDBCrossRecordQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that sorting by a common field on a universal index returns all types of records in an index scan.
     */
    @DualPlannerTest
    public void testCrossRecordTypeQuery() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            saveSimpleRecord(100, "first", 1);
            saveSimpleRecord(110, "second", 2);
            saveSimpleRecord2("third", 3);
            saveSimpleRecord2("fourth", 4);
            saveSimpleRecord(80, "fifth", 5);
            saveSimpleRecord2("sixth", 6);
            saveSimpleRecord2("seventh", 7);
            saveSimpleRecord(60, "seventh", 7);
            saveSimpleRecord2("seventh again", 7);
            saveSimpleRecord3("eighth", 8);

            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setSort(field("etag"))
                .build();

        // Index(versions <,>)
        RecordQueryPlan plan = planner.plan(query);
        MatcherAssert.assertThat(plan, indexScan(allOf(indexName("versions"), unbounded())));
        assertEquals(1555932709, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(974856845, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(974856845, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        List<String> names = new ArrayList<>();
        List<Integer> etags = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    final Message record = cursor.next().getRecord();
                    names.add((String)record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                    etags.add((int)record.getField(record.getDescriptorForType().findFieldByName("etag")));
                }
            }
            assertDiscardedNone(context);
        }
        assertEquals(Arrays.asList("first", "second", "third", "fourth", "fifth", "sixth"), names.subList(0, 6));
        assertThat(names.subList(6, 9), containsInAnyOrder("seventh", "seventh", "seventh again"));
        assertEquals(1555932709, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(974856845, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(974856845, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals("eighth", names.get(9));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 7, 7, 8), etags);
    }

    /**
     * Verify that multi-type record queries can scan multi-type indexes.
     * Verify that single-type record queries can scan multi-type indexes with a type filter.
     */
    @Test
    public void testMultiRecordTypeIndexScan() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            saveSimpleRecord(100, "first", 1);
            saveSimpleRecord(110, "second", 2);
            saveSimpleRecord2("third", 3);
            saveSimpleRecord2("fourth", 4);
            saveSimpleRecord(80, "fifth", 5);
            saveSimpleRecord2("sixth", 6);
            saveSimpleRecord2("seventh", 7);
            saveSimpleRecord(60, "seventh", 7);
            saveSimpleRecord2("seventh again", 7);

            saveSimpleRecord3("t3 second", 2);
            saveSimpleRecord3("t3 sixth", 6);
            saveSimpleRecord3("t3 seventh", 7);

            commit(context);
        }

        List<String> names = new ArrayList<>();
        List<Integer> etags = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            try (RecordCursorIterator<FDBIndexedRecord<Message>> cursor = recordStore.scanIndexRecords("partial_versions",
                    IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asIterator()) {
                while (cursor.hasNext()) {
                    final Message record = cursor.next().getRecord();
                    names.add((String) record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                    etags.add((int) record.getField(record.getDescriptorForType().findFieldByName("etag")));
                }
            }
            assertDiscardedNone(context);
        }
        assertEquals(Arrays.asList("first", "second", "third", "fourth", "fifth", "sixth"), names.subList(0, 6));
        assertThat(names.subList(6, 9), containsInAnyOrder("seventh", "seventh", "seventh again"));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 7, 7), etags);

        {
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordTypes(Arrays.asList("MySimpleRecord", "MySimpleRecord2"))
                    .setFilter(Query.field("etag").equalsValue(7))
                    .build();

            // Index(partial_versions [[7],[7]])
            RecordQueryPlan plan = planner.plan(query);
            MatcherAssert.assertThat(plan, indexScan(allOf(indexName("partial_versions"), bounds(hasTupleString("[[7],[7]]")))));
            assertEquals(-501898489, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1154059976, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1004410007, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            names.clear();
            etags.clear();
            try (FDBRecordContext context = openContext()) {
                openUnionRecordStore(context);
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        final Message record = cursor.next().getRecord();
                        names.add((String)record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                        etags.add((int)record.getField(record.getDescriptorForType().findFieldByName("etag")));
                    }
                }
                assertDiscardedNone(context);
            }
            assertThat(names, containsInAnyOrder("seventh", "seventh", "seventh again"));
            assertEquals(Arrays.asList(7, 7, 7), etags);
        }
        {
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord2")
                    .setFilter(Query.field("etag").equalsValue(7))
                    .build();

            // Index(partial_versions [[7],[7]]) | [MySimpleRecord2]
            RecordQueryPlan plan = planner.plan(query);
            MatcherAssert.assertThat(plan, typeFilter(contains("MySimpleRecord2"), indexScan(allOf(indexName("partial_versions"), bounds(hasTupleString("[[7],[7]]"))))));
            assertEquals(-1724404567, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(625156757, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(969338500, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            names.clear();
            etags.clear();
            try (FDBRecordContext context = openContext()) {
                openUnionRecordStore(context);
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        final Message record = cursor.next().getRecord();
                        names.add((String)record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                        etags.add((int)record.getField(record.getDescriptorForType().findFieldByName("etag")));
                    }
                }
                assertDiscardedAtMost(1, context);
            }
            assertThat(names, containsInAnyOrder("seventh", "seventh again"));
            assertEquals(Arrays.asList(7, 7), etags);
        }
        {
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord3")
                    .setFilter(Query.field("etag").equalsValue(7))
                    .build();

            // Index(versions [[7],[7]]) | [MySimpleRecord3]
            RecordQueryPlan plan = planner.plan(query);
            MatcherAssert.assertThat(plan, typeFilter(contains("MySimpleRecord3"), indexScan(allOf(indexName("versions"), bounds(hasTupleString("[[7],[7]]"))))));
            assertEquals(-1908726868, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1842301080, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-2108484473, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            names.clear();
            etags.clear();
            try (FDBRecordContext context = openContext()) {
                clearStoreCounter(context);
                openUnionRecordStore(context);
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        final Message record = cursor.next().getRecord();
                        names.add((String)record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                        etags.add((int)record.getField(record.getDescriptorForType().findFieldByName("etag")));
                    }
                }
                assertDiscardedAtMost(3, context);
            }
            assertEquals(Arrays.asList("t3 seventh"), names);
            assertEquals(Arrays.asList(7), etags);
        }
        {
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordTypes(Arrays.asList("MySimpleRecord2", "MySimpleRecord3"))
                    .setFilter(Query.field("etag").equalsValue(7))
                    .build();

            // Index(versions [[7],[7]]) | [MySimpleRecord2, MySimpleRecord3]
            RecordQueryPlan plan = planner.plan(query);
            MatcherAssert.assertThat(plan, typeFilter(containsInAnyOrder("MySimpleRecord2", "MySimpleRecord3"),
                    indexScan(allOf(indexName("versions"), bounds(hasTupleString("[[7],[7]]"))))));
            assertEquals(-1151709653, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1695649001, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1351467258, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            names.clear();
            etags.clear();
            try (FDBRecordContext context = openContext()) {
                clearStoreCounter(context);
                openUnionRecordStore(context);
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                    while (cursor.hasNext()) {
                        final Message record = cursor.next().getRecord();
                        names.add((String)record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                        etags.add((int)record.getField(record.getDescriptorForType().findFieldByName("etag")));
                    }
                }
                assertDiscardedAtMost(1, context);
            }
            assertThat(names, containsInAnyOrder("seventh", "seventh again", "t3 seventh"));
            assertEquals(Arrays.asList(7, 7, 7), etags);
        }
    }

    /**
     * Verify that filtering by a field on a universal index works.
     */
    @DualPlannerTest
    public void testCrossRecordTypeQueryFiltered() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            saveSimpleRecord(100, "first", 1);
            saveSimpleRecord(110, "second", 2);
            saveSimpleRecord2("third", 3);
            saveSimpleRecord2("fourth", 4);
            saveSimpleRecord(80, "fifth", 5);
            saveSimpleRecord2("sixth", 6);
            saveSimpleRecord2("seventh", 7);
            saveSimpleRecord(60, "seventh", 7);
            saveSimpleRecord2("seventh again", 7);

            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setFilter(Query.field("etag").greaterThan(3))
                .setSort(field("etag"))
                .build();

        // Index(versions ([3],>)
        RecordQueryPlan plan = planner.plan(query);
        MatcherAssert.assertThat(plan, indexScan(allOf(indexName("versions"), bounds(hasTupleString("([3],>")))));
        assertEquals(-1766882004, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1979698671, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1625127316, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        List<String> names = new ArrayList<>();
        List<Integer> etags = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    final Message record = cursor.next().getRecord();
                    names.add((String)record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                    etags.add((int)record.getField(record.getDescriptorForType().findFieldByName("etag")));
                }
            }
            assertDiscardedNone(context);
        }
        assertEquals(Arrays.asList("fourth", "fifth", "sixth"), names.subList(0, 3));
        assertThat(names.subList(3, 6), containsInAnyOrder("seventh", "seventh", "seventh again"));
        assertEquals(Arrays.asList(4, 5, 6, 7, 7, 7), etags);
    }

    /**
     * Verify that querying on a nested field can scan an appropriate universal index.
     */
    @DualPlannerTest
    public void testNestedCrossRecordTypeQueryFilteredAndSorted() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord.newBuilder().setRecNo(80).setStrValueIndexed("box").setNested(TestRecordsWithUnionProto.Nested.newBuilder().setEtag(1)).build());
            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder().setStrValueIndexed("of").setNested(TestRecordsWithUnionProto.Nested.newBuilder().setEtag(2)).build());
            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord3.newBuilder().setStrValueIndexed("fox").setNested(TestRecordsWithUnionProto.Nested.newBuilder().setEtag(3)).build());

            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setFilter(Query.field("nested").matches(Query.field("etag").greaterThan(1)))
                .setSort(field("nested").nest("etag"))
                .build();

        // Index(cross_versions ([1],>)
        RecordQueryPlan plan = planner.plan(query);
        MatcherAssert.assertThat(plan, indexScan(allOf(indexName("cross_versions"), bounds(hasTupleString("([1],>")))));
        assertEquals(552822345, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-2026740686, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1336599315, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        List<String> names = new ArrayList<>();
        List<Integer> etags = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    final Message record = cursor.next().getRecord();
                    names.add((String)record.getField(record.getDescriptorForType().findFieldByName("str_value_indexed")));
                    final Message nested = ((Message)record.getField(record.getDescriptorForType().findFieldByName("nested")));
                    etags.add((int)nested.getField(nested.getDescriptorForType().findFieldByName("etag")));
                }
            }
            assertDiscardedNone(context);
        }
        assertEquals(Arrays.asList("of", "fox"), names);
        assertEquals(Arrays.asList(2, 3), etags);
    }

    /**
     * Verify that a query against some record types can use an index on more record types, even when comparing against
     * a nested field.
     */
    @Test
    public void testNestedPartialCrossRecordTypeQuery() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord.newBuilder().setRecNo(80).setStrValueIndexed("box").setEtag(1).setNested(TestRecordsWithUnionProto.Nested.newBuilder().setEtag(1)).build());
            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder().setStrValueIndexed("of").setEtag(1).setNested(TestRecordsWithUnionProto.Nested.newBuilder().setEtag(2)).build());
            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord3.newBuilder().setStrValueIndexed("fox").setEtag(2).setNested(TestRecordsWithUnionProto.Nested.newBuilder().setEtag(3)).build());

            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordTypes(Arrays.asList("MySimpleRecord2", "MySimpleRecord3"))
                .setFilter(Query.and(
                        Query.field("etag").equalsValue(1),
                        Query.field("nested").matches(Query.field("etag").equalsValue(2))))
                .build();

        // Index(partial_nested_versions [[2, 1],[2, 1]]) | [MySimpleRecord2, MySimpleRecord3]
        RecordQueryPlan plan = planner.plan(query);
        MatcherAssert.assertThat(plan, typeFilter(containsInAnyOrder("MySimpleRecord2", "MySimpleRecord3"),
                indexScan(allOf(indexName("partial_nested_versions"), bounds(hasTupleString("[[2, 1],[2, 1]]"))))));
        assertEquals(-1448785488, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(482298891, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1383990870, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    final Message record = cursor.next().getRecord();
                    final Message nested = ((Message)record.getField(record.getDescriptorForType().findFieldByName("nested")));
                    assertEquals(2, nested.getField(nested.getDescriptorForType().findFieldByName("etag")));
                }
            }
            assertDiscardedNone(context);
        }
    }
}
