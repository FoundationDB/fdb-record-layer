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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;

import static com.apple.foundationdb.record.ExecuteProperties.newBuilder;
import static com.apple.foundationdb.record.TestHelpers.RealAnythingMatcher.anything;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedExactly;
import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.descendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasNoDescendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.intersection;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.selfOrDescendantPlans;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ValueMatchers.fieldValue;
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
    @ParameterizedTest
    @BooleanSource
    public void testComplexQueryAndWithTwoChildren(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();

        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Index(MySimpleRecord$str_value_indexed [[even],[even]]) ∩ Index(MySimpleRecord$num_value_3_indexed [[3],[3]])
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[even],[even]]) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∩ Covering(Index(MySimpleRecord$num_value_3_indexed [[3],[3]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch) {
            assertThat(plan, fetch(intersection(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]"))))),
                    equalTo(field("rec_no")))));
            assertEquals(-929788310, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1914172894, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-271606869, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, intersection(
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]")))),
                    equalTo(field("rec_no"))));
            assertEquals(-1973527173, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(227253579, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1869819604, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            if (shouldDeferFetch) {
                assertLoadRecord(10, context);
            }
        }
    }

    /**
     * Verify that a complex query with an AND of fields with compatibly ordered indexes generates an intersection plan.
     */
    @DualPlannerTest
    public void testComplexQueryAndWithTwoChildren2() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();

        setDeferFetchAfterUnionAndIntersection(true);
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, fetch(intersection(
                coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]"))))),
                coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[3],[3]]"))))),
                equalTo(field("rec_no")))));
        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-929788310, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1914172894, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-271606869, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-70465554, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2126183202, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1157469383, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            assertLoadRecord(10, context);
        }
    }

    /**
     * Verify that a complex query with an AND of fields with compatibly ordered indexes generates an intersection plan.
     */
    @DualPlannerTest
    public void testComplexQueryAndWithTwoChildren3() throws Exception {
        RecordMetaDataHook hook = (metaDataBuilder) -> {
            complexQuerySetupHook().apply(metaDataBuilder);
            metaDataBuilder.removeIndex("multi_index");
            metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
            metaDataBuilder.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaDataBuilder.addIndex("MySimpleRecord", "multi_val_val_3", concat(field("str_value_indexed"), field("num_value_2"), field("num_value_3_indexed")));
            metaDataBuilder.addIndex("MySimpleRecord", "multi_val_3_val", concat(field("num_value_3_indexed"), field("num_value_2"), field("str_value_indexed")));
        };
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .setRequiredResults(ImmutableList.of(field("str_value_indexed"), field("num_value_3_indexed")))
                .build();

        setDeferFetchAfterUnionAndIntersection(true);
        RecordQueryPlan plan = planner.plan(query);
        System.out.println(plan);
    }
    
    /**
     * Verify that a complex query with an AND of more than two fields with compatibly ordered indexes generates an intersection plan.
     */
    @ParameterizedTest
    @BooleanSource
    public void testComplexQueryAndWithMultipleChildren(boolean shouldDeferFetch) throws Exception {
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
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]]) ∩ Index(MySimpleRecord$num_value_3_indexed [[2],[2]]) ∩ Index(MySimpleRecord$num_value_2 [[1],[1]])
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]]) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∩ Covering(Index(MySimpleRecord$num_value_3_indexed [[2],[2]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∩ Covering(Index(MySimpleRecord$num_value_2 [[1],[1]]) -> [num_value_2: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);
        if (shouldDeferFetch) {
            assertThat(plan, fetch(intersection(Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_2"), bounds(hasTupleString("[[1],[1]]")))))),
                    equalTo(field("rec_no")))));
            assertEquals(946461036, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-625341018, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(116741660, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, intersection(Arrays.asList(
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[2],[2]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_2"), bounds(hasTupleString("[[1],[1]]"))))),
                    equalTo(field("rec_no"))));
            assertEquals(-478358039, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1448156435, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-2104728183, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            if (shouldDeferFetch) {
                assertLoadRecord(4, context);
            }
        }
    }
    
    /**
     * Verify that a complex query with an AND of fields that are _not_ compatibly ordered generates a plan without
     * an intersection (uses filter instead).
     */
    @ParameterizedTest
    @BooleanSource
    public void testComplexQueryAndWithIncompatibleFilters(final boolean shouldOptimizeForIndexFilters) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").startsWith("e"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        setOptimizeForIndexFilters(shouldOptimizeForIndexFilters);

        // Index(MySimpleRecord$str_value_indexed {[e],[e]}) | num_value_3_indexed EQUALS 3
        // Fetch(Covering(Index(multi_index {[e],[e]}) -> [num_value_2: KEY[1], num_value_3_indexed: KEY[2], rec_no: KEY[3], str_value_indexed: KEY[0]]) | num_value_3_indexed EQUALS 3)
        RecordQueryPlan plan = planner.plan(query);
        // Not an intersection plan, since not compatibly ordered.
        if (shouldOptimizeForIndexFilters) {
            assertThat(plan, allOf(
                    hasNoDescendant(intersection(anything(), anything())),
                    descendant(filter(Query.field("num_value_3_indexed").equalsValue(3), coveringIndexScan(indexScan(anyOf(indexName("multi_index"), bounds(hasTupleString("[[e],[e]]")))))))));
            assertFalse(plan.hasRecordScan(), "should not use record scan");
            assertEquals(-1810430840, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1492232944, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1442514296, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, allOf(
                    hasNoDescendant(intersection(anything(), anything())),
                    descendant(indexScan(anyOf(indexName("MySimpleRecord$str_value_indexed"), indexName("MySimpleRecord$num_value_3_indexed"))))));
            assertFalse(plan.hasRecordScan(), "should not use record scan");
            assertEquals(746853985, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(312168193, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(361886841, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            assertLoadRecord(50, context);
        }
    }

    /**
     * Verify that a complex query with an AND of fields where some of them are compatibly ordered uses an intersection
     * for only those filters.
     */
    @ParameterizedTest
    @BooleanSource
    public void testComplexQueryAndWithSomeIncompatibleFilters(boolean shouldDeferFetch) throws Exception {
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
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∩ Covering(Index(MySimpleRecord$num_value_2 [[2],[2]]) -> [num_value_2: KEY[0], rec_no: KEY[1]])) | str_value_indexed STARTS_WITH e
        RecordQueryPlan plan = planner.plan(query);
        // Should only include compatibly-ordered things in the intersection

        if (shouldDeferFetch) {
            assertThat(plan, filter(Query.field("str_value_indexed").startsWith("e"), fetch(intersection(
                    coveringIndexScan(indexScan(allOf(indexName(equalTo("MySimpleRecord$num_value_3_indexed")), bounds(hasTupleString("[[0],[0]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName(equalTo("MySimpleRecord$num_value_2")), bounds(hasTupleString("[[2],[2]]"))))),
                    equalTo(field("rec_no"))))));
            assertEquals(-1979861885, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1271604119, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1737180988, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, filter(Query.field("str_value_indexed").startsWith("e"), intersection(
                    indexScan(allOf(indexName(equalTo("MySimpleRecord$num_value_3_indexed")), bounds(hasTupleString("[[0],[0]]")))),
                    indexScan(allOf(indexName(equalTo("MySimpleRecord$num_value_2")), bounds(hasTupleString("[[2],[2]]")))),
                    equalTo(field("rec_no")))));
            assertEquals(1095867174, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(688107104, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(222530235, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            if (shouldDeferFetch) {
                assertLoadRecord(7, context);
            }
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

        // Index(MySimpleRecord$num_value_3_indexed [[3],[3]]) | str_value_indexed EQUALS even
        RecordQueryPlan plan = planner.plan(query);
        // Would get Intersection didn't have identical continuations if it did
        assertThat("Should not use grouped index", plan, hasNoDescendant(indexScan("grouped_index")));
        assertEquals(622816289, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1284025903, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1170038658, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

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
            assertLoadRecord(20, context);
        }
    }

    /**
     * Verify that an AND on two indexed fields with compatibly ordered indexes is implemented by an intersection, and
     * that the intersection cursor works properly with a returned record limit.
     */
    @ParameterizedTest
    @BooleanSource
    public void testComplexLimits4(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]]) ∩ Index(MySimpleRecord$num_value_3_indexed [[0],[0]])
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]]) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∩ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planner.plan(query);
        if (shouldDeferFetch) {
            assertThat(plan, fetch(intersection(
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))))));
            assertEquals(-1584186334, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1592698726, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-271606869, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, intersection(
                    indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[odd],[odd]]")))),
                    indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[0]]"))))));
            assertEquals(-2067012605, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(548727747, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1869819604, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }


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
            if (shouldDeferFetch) {
                assertLoadRecord(5, context);
            }
        }
    }

    /**
     * Verify that a complex AND is implemented as an intersection of two multi-field indexes, where the first field of
     * the primary key is also the first field of the two indexes for which there are additional equality predicates.
     */
    @ParameterizedTest
    @BooleanSource
    public void testAndQuery7(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook(true);
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(str_value_2_index [[even, 1],[even, 1]]) -> [num_value_2: KEY[1], num_value_unique: KEY[2], str_value_indexed: KEY[0]]) ∩ Covering(Index(str_value_3_index [[even, 3],[even, 3]]) -> [num_value_3_indexed: KEY[1], num_value_unique: KEY[2], str_value_indexed: KEY[0]]))
        RecordQueryPlan plan = planner.plan(query);

        if (shouldDeferFetch) {
            assertThat(plan, fetch(intersection(
                    coveringIndexScan(indexScan(allOf(indexName("str_value_2_index"), bounds(hasTupleString("[[even, 1],[even, 1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("str_value_3_index"), bounds(hasTupleString("[[even, 3],[even, 3]]"))))),
                    equalTo(primaryKey("MySimpleRecord")))));
            assertEquals(384640197, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-230590024, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1728044710, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, intersection(
                    indexScan(allOf(indexName("str_value_2_index"), bounds(hasTupleString("[[even, 1],[even, 1]]")))),
                    indexScan(allOf(indexName("str_value_3_index"), bounds(hasTupleString("[[even, 3],[even, 3]]")))),
                    equalTo(primaryKey("MySimpleRecord"))));
            assertEquals(-1785751672, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1910836449, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(413381763, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            }
        }
    }

    /**
     * Verify that very complex AND clauses are implemented as combinations of range and index scans.
     * The possible plans are an index scan for one field or an intersection for the other, in each case with the other
     * condition as a filter. This checks that the intersection is only preferred when it accomplishes more.
     */
    @DualPlannerTest
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

        // Index(index_2_3 [[1, 2],[1, 3]]) | And([str_value_indexed EQUALS even, num_value_unique EQUALS 0])
        RecordQueryPlan plan = planner.plan(query);
        assertThat("should have range scan in " + plan, plan, descendant(indexScan("index_2_3")));

        assertFalse(plan.hasRecordScan(), "should not use record scan");
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    selfOrDescendantPlans(indexPlan().where(RecordQueryPlanMatchers.indexName("index_2_3"))));

            assertEquals(2140693065, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1517851081, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1716318889, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(
                            indexPlan()
                                    .where(RecordQueryPlanMatchers.indexName("index_2_3"))
                                    .and(scanComparisons(range("[[1, 2],[1, 3]]"))))
                            .where(predicates(valuePredicate(fieldValue("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "even")),
                                    valuePredicate(fieldValue("num_value_unique"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 0)))));

            assertEquals(-476608798, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-119924960, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(409745570, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that a query with a sort by primary key and AND clause is implemented as an intersection of index scans,
     * when the primary key is unbounded.
     */
    @ParameterizedTest
    @BooleanSource
    public void sortedIntersectionUnbounded(boolean shouldDeferFetch) throws Exception {
        RecordMetaDataHook hook = sortingShapesHook();
        setupEnumShapes(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyShapeRecord")
                .setFilter(Query.and(
                        Query.field("shape").equalsValue(TestRecordsEnumProto.MyShapeRecord.Shape.CIRCLE),
                        Query.field("color").equalsValue(TestRecordsEnumProto.MyShapeRecord.Color.RED)))
                .setSort(field("rec_no"))
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(color [[10],[10]]) -> [color: KEY[0], rec_name: KEY[2], rec_no: KEY[1]]) ∩ Covering(Index(shape [[200],[200]]) -> [rec_name: KEY[2], rec_no: KEY[1], shape: KEY[0]]))
        RecordQueryPlan plan = planner.plan(query);
        if (shouldDeferFetch) {
            assertThat(plan, fetch(intersection(
                    coveringIndexScan(indexScan(allOf(indexName("color"), bounds(hasTupleString("[[10],[10]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("shape"), bounds(hasTupleString("[[200],[200]]"))))))));
            assertEquals(-2072158516, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1707812033, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1828796254, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, intersection(
                    indexScan(allOf(indexName("color"), bounds(hasTupleString("[[10],[10]]")))),
                    indexScan(allOf(indexName("shape"), bounds(hasTupleString("[[200],[200]]"))))));
            assertEquals(-296022647, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(433614440, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-324744569, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            }
        }
    }

    /**
     * Verify that a query with a sort by primary key and AND clause (with complex limits) is implemented as an
     * intersection of index scans, when the primary key is bounded.
     */
    @ParameterizedTest
    @BooleanSource
    public void sortedIntersectionBounded(boolean shouldDeferFetch) throws Exception {
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
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

        // Fetch(Covering(Index(color [[10, 2],[10, 11]]) -> [color: KEY[0], rec_name: KEY[2], rec_no: KEY[1]]) ∩ Covering(Index(shape [[200, 2],[200, 11]]) -> [rec_name: KEY[2], rec_no: KEY[1], shape: KEY[0]]))
        // Fetch(Covering(Index(color [[10, 2],[10, 11]]) -> [color: KEY[0], rec_name: KEY[2], rec_no: KEY[1]]) ∩ Covering(Index(shape [[200, 2],[200, 11]]) -> [rec_name: KEY[2], rec_no: KEY[1], shape: KEY[0]]))
        RecordQueryPlan plan = planner.plan(query);
        if (shouldDeferFetch) {
            assertThat(plan, fetch(intersection(
                    coveringIndexScan(indexScan(allOf(indexName("color"), bounds(hasTupleString("[[10, 2],[10, 11]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("shape"), bounds(hasTupleString("[[200, 2],[200, 11]]"))))))));
            assertEquals(1992249868, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(625673791, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1309396162, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, intersection(
                    indexScan(allOf(indexName("color"), bounds(hasTupleString("[[10, 2],[10, 11]]")))),
                    indexScan(allOf(indexName("shape"), bounds(hasTupleString("[[200, 2],[200, 11]]"))))));
            assertEquals(-942526391, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1527867032, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(832030311, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

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
            if (shouldDeferFetch) {
                assertLoadRecord(2, context);
            }
        }
    }

    /**
     * Verify that an intersection visitor won't defer a record fetch if the comparison key has fields that the index
     * entry doesn't.
     * This sort of plan is never produced by the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner},
     * so we have to test the visitor directly.
     */
    @Test
    public void intersectionVisitorOnComplexComparisonKey() throws Exception {
        complexQuerySetup(null);

        IndexScanParameters fullValueScan = IndexScanComparisons.byValue();

        RecordQueryPlan originalPlan1 = RecordQueryIntersectionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", fullValueScan, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", fullValueScan, false),
                primaryKey("MySimpleRecord"));

        RecordQueryPlan modifiedPlan1 = RecordQueryPlannerSubstitutionVisitor.applyVisitors(originalPlan1, recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        assertThat(modifiedPlan1, fetch(intersection(
                coveringIndexScan(indexScan("MySimpleRecord$str_value_indexed")), coveringIndexScan(indexScan("MySimpleRecord$num_value_3_indexed")))));

        RecordQueryPlan originalPlan2 = RecordQueryIntersectionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", fullValueScan, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", fullValueScan, false),
                concat(field("num_value_2"), primaryKey("MySimpleRecord")));
        RecordQueryPlan modifiedPlan2 = RecordQueryPlannerSubstitutionVisitor.applyVisitors(originalPlan2, recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        // Visitor should not perform transformation because of comparison key on num_value_unique
        assertEquals(originalPlan2, modifiedPlan2);
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

