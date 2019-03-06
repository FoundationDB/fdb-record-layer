/*
 * FDBAndQueryToIntersectionTest.java
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

import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.apple.foundationdb.record.ExecuteProperties.newBuilder;
import static com.apple.foundationdb.record.TestHelpers.RealAnythingMatcher.anything;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedExactly;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.descendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasNoDescendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.intersection;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests related to planning a query with an AND clause into an intersection plan.
 */
@Tag(Tags.RequiresFDB)
public class FDBAndQueryToIntersectionTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that a complex query with an AND of fields with compatibly ordered indexes generates an intersection plan.
     */
    @Test
    public void testComplexQueryAndWithTwoChildren() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, intersection(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]")))),
                equalTo(field("rec_no"))));
        assertEquals(-1973527173, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertTrue((myrec.getNumValue3Indexed() % 5) == 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedExactly(50, context);
        }
    }

    /**
     * Verify that a complex query with an AND of more than two fields with compatibly ordered indexes generates an intersection plan.
     */
    @Test
    public void testComplexQueryAndWithMultipleChildren() throws Exception {
        // Add an additional index to use for additional filtering
        RecordMetaDataHook hook = (metaDataBuilder) -> {
            complexQuerySetupHook().apply(metaDataBuilder);
            metaDataBuilder.removeIndex("multi_index");
            metaDataBuilder.addIndex("MySimpleRecord", "MySimpleRecord$num_value_2", field("num_value_2"));
        };
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_2").equalsValue(1)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, intersection(Arrays.asList(
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_2"), bounds(hasTupleString("[[1],[1]]"))))),
                equalTo(field("rec_no"))));
        assertEquals(-478358039, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("odd", myrec.getStrValueIndexed());
                    assertEquals(2, myrec.getNumValue3Indexed());
                    assertEquals(1, myrec.getNumValue2());
                    i++;
                }
            }
            assertEquals(4, i);
            assertDiscardedAtMost(90, context);
        }
    }

    /**
     * Verify that a complex query with an AND of fields that are _not_ compatibly ordered generates a plan without
     * an intersection (uses filter instead).
     */
    @Test
    public void testComplexQueryAndWithIncompatibleFilters() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").startsWith("e"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        // Not an intersection plan, since not compatibly ordered.
        assertThat(plan, allOf(
                hasNoDescendant(intersection(anything(), anything())),
                descendant(indexScan(anyOf(indexName("MySimpleRecord$str_value_indexed"), indexName("MySimpleRecord$num_value_3_indexed"))))));
        assertFalse(plan.hasRecordScan(), "should not use record scan");
        assertEquals(746853985, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertTrue((myrec.getNumValue3Indexed() % 5) == 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedExactly(40, context);
        }
    }

    /**
     * Verify that a complex query with an AND of fields where some of them are compatibly ordered uses an intersection
     * for only those filters.
     */
    @Test
    public void testComplexQueryAndWithSomeIncompatibleFilters() throws Exception {
        // Add an additional index to use for additional filtering
        RecordMetaDataHook hook = (metaDataBuilder) -> {
            complexQuerySetupHook().apply(metaDataBuilder);
            metaDataBuilder.removeIndex("multi_index");
            metaDataBuilder.addIndex("MySimpleRecord", "MySimpleRecord$num_value_2", field("num_value_2"));
        };
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").startsWith("e"),
                        Query.field("num_value_3_indexed").equalsValue(0),
                        Query.field("num_value_2").equalsValue(2)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        // Should only include compatibly-ordered things in the intersection
        assertThat(plan, filter(equalTo(Query.field("str_value_indexed").startsWith("e")), intersection(
                indexScan(allOf(indexName(equalTo("MySimpleRecord$num_value_3_indexed")), bounds(hasTupleString("[[0],[0]]")))),
                indexScan(allOf(indexName(equalTo("MySimpleRecord$num_value_2")), bounds(hasTupleString("[[2],[2]]")))),
                equalTo(field("rec_no")))));
        assertEquals(1095867174, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertEquals(0, myrec.getNumValue3Indexed());
                    assertEquals(2, myrec.getNumValue2());
                    i++;
                }
            }
            assertEquals(3, i);
            assertDiscardedAtMost(42, context);
        }
    }

    /**
     * Verify that the planner does not use aggregate indexes to implement ANDs as intersections.
     */
    @Test
    public void testComplexQuery1g() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
            metaData.addIndex("MySimpleRecord", new Index("grouped_index",
                    concatenateFields("str_value_indexed", "num_value_2")
                            .group(1),
                    IndexTypes.RANK));
        };
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        planner.setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_SCAN);
        RecordQueryPlan plan = planner.plan(query);
        // Would get Intersection didn't have identical continuations if it did
        assertThat("Should not use grouped index", plan, hasNoDescendant(indexScan(indexName("grouped_index"))));
        assertEquals(622816289, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertTrue((myrec.getNumValue3Indexed() % 5) == 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedExactly(10, context);
        }
    }

    /**
     * Verify that an AND on two indexed fields with compatibly ordered indexes is implemented by an intersection, and
     * that the intersection cursor works properly with a returned record limit.
     */
    @Test
    public void testComplexLimits4() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, intersection(
                indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))));
        assertEquals(-2067012605, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, newBuilder().setReturnedRowLimit(5).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(0, myrec.getNumValue3Indexed());
                    assertEquals("odd", myrec.getStrValueIndexed());
                    i += 1;
                }
            }
            assertEquals(5, i);
            assertDiscardedAtMost(23, context);
        }
    }

    /**
     * Verify that a complex AND is implemented as an intersection of two multi-field indexes, where the first field of
     * the primary key is also the first field of the two indexes for which there are additional equality predicates.
     */
    @Test
    public void testAndQuery7() throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, intersection(
                indexScan(allOf(indexName("str_value_2_index"), bounds(hasTupleString("[[even, 1],[even, 1]]")))),
                indexScan(allOf(indexName("str_value_3_index"), bounds(hasTupleString("[[even, 3],[even, 3]]")))),
                equalTo(primaryKey("MySimpleRecord"))));
        assertEquals(-1785751672, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") &&
                               myrec.getNumValue2() == 1 &&
                               myrec.getNumValue3Indexed() == 3);
                    i++;
                }
            }
            assertEquals(3, i);
            assertDiscardedAtMost(19, context);
        }
    }

    /**
     * Verify that very complex AND clauses are implemented as combinations of range and index scans.
     * The possible plans are an index scan for one field or an intersection for the other, in each case with the other
     * condition as a filter. This checks that the intersection is only preferred when it accomplishes more.
     */
    @Test
    public void intersectionVersusRange() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData -> {
                metaData.addIndex("MySimpleRecord", "num_value_2");
                metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
                metaData.addIndex("MySimpleRecord", new Index("index_2_3", "num_value_2", "num_value_3_indexed"));
            });
        }
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat("should have range scan in " + plan, plan, descendant(indexScan(indexName("index_2_3"))));
        assertFalse(plan.hasRecordScan(), "should not use record scan");
        assertEquals(1840965325, plan.planHash());
    }

    /**
     * Verify that a query with a sort by primary key and AND clause is implemented as an intersection of index scans,
     * when the primary key is unbounded.
     */
    @Test
    public void sortedIntersectionUnbounded() throws Exception {
        RecordMetaDataHook hook = sortingShapesHook();
        setupEnumShapes(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyShapeRecord")
                .setFilter(Query.and(
                        Query.field("shape").equalsValue(TestRecordsEnumProto.MyShapeRecord.Shape.CIRCLE),
                        Query.field("color").equalsValue(TestRecordsEnumProto.MyShapeRecord.Color.RED)))
                .setSort(field("rec_no"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, intersection(
                indexScan(allOf(indexName("color"), bounds(hasTupleString("[[10],[10]]")))),
                indexScan(allOf(indexName("shape"), bounds(hasTupleString("[[200],[200]]"))))));
        assertEquals(-296022647, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openEnumRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsEnumProto.MyShapeRecord.Builder shapeRec = TestRecordsEnumProto.MyShapeRecord.newBuilder();
                    shapeRec.mergeFrom(rec.getRecord());
                    assertThat(shapeRec.getRecName(), endsWith("-RED-CIRCLE"));
                    assertThat(shapeRec.getShape(), equalTo(TestRecordsEnumProto.MyShapeRecord.Shape.CIRCLE));
                    assertThat(shapeRec.getColor(), equalTo(TestRecordsEnumProto.MyShapeRecord.Color.RED));
                    i++;
                }
            }
            assertEquals(3, i);
            assertDiscardedAtMost(10, context);
        }
    }

    /**
     * Verify that a query with a sort by primary key and AND clause (with complex limits) is implemented as an
     * intersection of index scans, when the primary key is bounded.
     */
    @Test
    public void sortedIntersectionBounded() throws Exception {
        RecordMetaDataHook hook = sortingShapesHook();
        setupEnumShapes(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyShapeRecord")
                .setFilter(Query.and(
                        Query.field("shape").equalsValue(TestRecordsEnumProto.MyShapeRecord.Shape.CIRCLE),
                        Query.field("color").equalsValue(TestRecordsEnumProto.MyShapeRecord.Color.RED),
                        Query.field("rec_no").greaterThanOrEquals(2),
                        Query.field("rec_no").lessThanOrEquals(11)))
                .setSort(field("rec_no"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, intersection(
                indexScan(allOf(indexName("color"), bounds(hasTupleString("[[10, 2],[10, 11]]")))),
                indexScan(allOf(indexName("shape"), bounds(hasTupleString("[[200, 2],[200, 11]]"))))));
        assertEquals(-1943887159, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openEnumRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsEnumProto.MyShapeRecord.Builder shapeRec = TestRecordsEnumProto.MyShapeRecord.newBuilder();
                    shapeRec.mergeFrom(rec.getRecord());
                    assertThat(shapeRec.getRecName(), anyOf(is("SMALL-RED-CIRCLE"), is("MEDIUM-RED-CIRCLE")));
                    assertThat(shapeRec.getShape(), equalTo(TestRecordsEnumProto.MyShapeRecord.Shape.CIRCLE));
                    assertThat(shapeRec.getColor(), equalTo(TestRecordsEnumProto.MyShapeRecord.Color.RED));
                    assertThat(shapeRec.getRecNo(), allOf(greaterThanOrEqualTo(2), lessThanOrEqualTo(11)));
                    i++;
                }
            }
            assertEquals(2, i);
            assertDiscardedAtMost(4, context);
        }
    }

    private RecordMetaDataHook sortingShapesHook() {
        return metaData -> {
            final RecordTypeBuilder type = metaData.getRecordType("MyShapeRecord");
            metaData.addIndex(type, new Index("size", concatenateFields("size", "rec_no")));
            metaData.addIndex(type, new Index("color", concatenateFields("color", "rec_no")));
            metaData.addIndex(type, new Index("shape", concatenateFields("shape", "rec_no")));
        };
    }

}

