/*
 * FDBNestedFieldQueryTest.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords5Proto;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.predicates.match.PredicateMatchers;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.queryPredicateDescendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the planner's ability to handle nested fields.
 */
@Tag(Tags.RequiresFDB)
public class FDBNestedFieldQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that simple queries on nested fields can use bounds on a record scan.
     */
    @DualPlannerTest
    public void hierarchical() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openHierarchicalRecordStore(context);

            TestRecords3Proto.MyHierarchicalRecord.Builder recBuilder = TestRecords3Proto.MyHierarchicalRecord.newBuilder();
            recBuilder.setChildName("photos");
            recBuilder.setNumValueIndexed(1);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setChildName("music");
            recBuilder.setNumValueIndexed(2);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setParentPath("photos");
            recBuilder.setChildName("vacations");
            recBuilder.setNumValueIndexed(11);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setChildName("pets");
            recBuilder.setNumValueIndexed(12);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setParentPath("photos/vacations");
            recBuilder.setChildName("paris");
            recBuilder.setNumValueIndexed(111);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setChildName("london");
            recBuilder.setNumValueIndexed(112);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setParentPath("photos/vacations/paris");
            recBuilder.setChildName("seine");
            recBuilder.setNumValueIndexed(1111);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openHierarchicalRecordStore(context);

            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(null, "photos"));
            assertNotNull(rec);
            TestRecords3Proto.MyHierarchicalRecord.Builder myrec = TestRecords3Proto.MyHierarchicalRecord.newBuilder();
            myrec.mergeFrom(rec.getRecord());
            assertEquals(1, myrec.getNumValueIndexed());
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyHierarchicalRecord")
                .setFilter(Query.field("parent_path").equalsValue("photos"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, scan(bounds(hasTupleString("[[photos],[photos]]"))));
        assertEquals(1063779424, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-623055281, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(568511736, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Arrays.asList(12, 11), fetchResultValues(plan, TestRecords3Proto.MyHierarchicalRecord.NUM_VALUE_INDEXED_FIELD_NUMBER,
                this::openHierarchicalRecordStore,
                TestHelpers::assertDiscardedNone));

        query = RecordQuery.newBuilder()
                .setRecordType("MyHierarchicalRecord")
                .setFilter(Query.field("parent_path").startsWith("photos"))
                .build();
        plan = planner.plan(query);
        assertThat(plan, scan(bounds(hasTupleString("{[photos],[photos]}"))));
        assertEquals(224213141, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1663787616, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1347957217, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Arrays.asList(12, 11, 112, 111, 1111), fetchResultValues(plan, TestRecords3Proto.MyHierarchicalRecord.NUM_VALUE_INDEXED_FIELD_NUMBER,
                this::openHierarchicalRecordStore,
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that nested field comparisons with fanout can scan indexes.
     */
    @DualPlannerTest
    public void nested() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("RestaurantRecord", "complex", concat(field("name"), field("rest_no"), field("reviews", KeyExpression.FanType.FanOut).nest(concat(field("reviewer"), field("rating")))));
            metaData.addIndex("RestaurantRecord", "composite", concat(field("name"), field("rest_no")));
            metaData.addIndex("RestaurantRecord", "duplicates", concat(field("name"), field("name")));
        };

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context, hook);

            TestRecords4Proto.RestaurantReviewer.Builder reviewerBuilder = TestRecords4Proto.RestaurantReviewer.newBuilder();
            reviewerBuilder.setId(1);
            reviewerBuilder.setName("Lemuel");
            recordStore.saveRecord(reviewerBuilder.build());

            reviewerBuilder.setId(2);
            reviewerBuilder.setName("Gulliver");
            recordStore.saveRecord(reviewerBuilder.build());

            TestRecords4Proto.RestaurantRecord.Builder recBuilder = TestRecords4Proto.RestaurantRecord.newBuilder();
            recBuilder.setRestNo(101);
            recBuilder.setName("The Emperor's Three Tables");
            TestRecords4Proto.RestaurantReview.Builder reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(1);
            reviewBuilder.setRating(10);
            reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(2);
            reviewBuilder.setRating(3);
            TestRecords4Proto.RestaurantTag.Builder tagBuilder = recBuilder.addTagsBuilder();
            tagBuilder.setValue("Lilliput");
            tagBuilder.setWeight(5);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = TestRecords4Proto.RestaurantRecord.newBuilder();
            recBuilder.setRestNo(102);
            recBuilder.setName("Small Fry's Fried Victuals");
            reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(1);
            reviewBuilder.setRating(5);
            reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(2);
            reviewBuilder.setRating(5);
            tagBuilder = recBuilder.addTagsBuilder();
            tagBuilder.setValue("Lilliput");
            tagBuilder.setWeight(1);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }

        // TODO this was originally:
        // QueryExpression.field("reviews").matches(QueryExpression.field("rating").greaterThan(5)),
        // which should have failed validate
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(Query.and(
                        Query.field("name").equalsValue("something"),
                        Query.field("name").equalsValue("something1"),
                        Query.field("rest_no").equalsValue(1L),
                        //Query.field("reviews").oneOfThem().matches(Query.field("reviewer").equalsValue(10L))))
                        //Query.field("reviews").oneOfThem().matches(Query.field("rating").equalsValue(20))))
                        Query.field("reviews").oneOfThem().matches(Query.and(Query.field("reviewer").equalsValue(10L), Query.field("rating").equalsValue(20)))))
                .build();
        RecordQueryPlan plan = planner.plan(query);

        System.out.println(plan.toString());

        /*
        assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("review_rating"), bounds(hasTupleString("([5],>"))))));
        assertEquals(1378568952, plan.planHash());
        assertEquals(Arrays.asList(101L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                this::openNestedRecordStore,
                TestHelpers::assertDiscardedNone));

        query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(Query.field("tags").oneOfThem().matches(
                        Query.and(
                                Query.field("value").equalsValue("Lilliput"),
                                Query.field("weight").greaterThanOrEquals(5))))
                .build();
        plan = planner.plan(query);
        assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("tag"), bounds(hasTupleString("[[Lilliput, 5],[Lilliput]]"))))));
        assertEquals(-1197819382, plan.planHash());
        assertEquals(Collections.singletonList(101L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                this::openNestedRecordStore,
                TestHelpers::assertDiscardedNone)); */
    }

    /**
     * Verify that AND clauses in queries on nested record stores are implemented so that the AND is expressed as a
     * condition on the parent field, rather than as an AND of separate nested conditions.
     */
    @DualPlannerTest
    public void nestedWithAnd() throws Exception {
        nestedWithAndSetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.field("stats").matches(
                        Query.and(
                                Query.field("start_date").greaterThan(0L),
                                Query.field("school_name").equalsValue("Human University"))))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, filter(Query.field("stats").matches(Query.field("school_name").equalsValue("Human University")),
                indexScan(allOf(indexName("stats$school"), bounds(hasTupleString("([0],>"))))));
        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-417538532, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1086699829, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1053293170, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-2139547699, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1632824291, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1666230950, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(Collections.singletonList(2L), fetchResultValues(plan, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                this::openNestedRecordStore,
                TestHelpers::assertDiscardedNone));

        query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.field("stats").matches(
                        Query.and(
                                Query.field("start_date").lessThanOrEquals(1000L),
                                Query.field("school_name").lessThan("University of Procrastination"),
                                Query.field("hometown").startsWith("H")
                        )
                ))
                .build();
        plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertThat(plan, filter(Query.field("stats").matches(
                    Query.and(Query.field("school_name").lessThan("University of Procrastination"),
                            Query.field("hometown").startsWith("H"))),
                    indexScan(allOf(indexName("stats$school"), bounds(hasTupleString("([null],[1000]]"))))));

            assertEquals(1700959433, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1026881662, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(129783739, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertThat(plan, filter(
                    allOf(queryPredicateDescendant(PredicateMatchers.field("stats", "school_name").lessThan("University of Procrastination")),
                            queryPredicateDescendant(PredicateMatchers.field("stats", "hometown").startsWith("H"))),
                    indexScan(allOf(indexName("stats$school"), bounds(hasTupleString("([null],[1000]]"))))));

            assertEquals(-1842706543, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-661089614, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-955666247, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(Collections.singletonList(1L), fetchResultValues(plan, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                this::openNestedRecordStore,
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that an AND query on a nested record store that can be mostly implemented by a scan of a concatenated index
     * still filters on predicates that are not satisfied by scanning that index.
     * Specifically, verify that an AND query with a predicate on an outer record and a predicate on an inner, map-like
     * record that can be satisfied by scanning a particular index, and a predicate on the inner record that cannot be
     * satisfied by scanning that index, is planned as an index scan followed by a filter with the unsatisfied predicate.
     */
    @DualPlannerTest
    public void nestedAndOnNestedMap() throws Exception {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsNestedMapProto.getDescriptor());
            metaDataBuilder.addIndex("OuterRecord", "key_index", concat(
                    field("other_id"),
                    field("map").nest(field("entry", KeyExpression.FanType.FanOut).nest("key"))));
            createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("OuterRecord")
                .setFilter(Query.and(
                        Query.field("other_id").equalsValue(1L),
                        Query.field("map").matches(
                                Query.field("entry").oneOfThem().matches(
                                        Query.and(
                                                Query.field("key").equalsValue("alpha"),
                                                Query.field("value").notEquals("test"))))))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        // verify that the value filter that can't be satisfied by the index isn't dropped from the filter expression

        assertThat(plan, filter(
                Query.field("map").matches(
                        Query.field("entry").oneOfThem().matches(
                                Query.and(
                                        Query.field("key").equalsValue("alpha"),
                                        Query.field("value").notEquals("test")))),
                primaryKeyDistinct(indexScan(allOf(indexName("key_index"), bounds(hasTupleString("[[1, alpha],[1, alpha]]")))))));

        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-1406660101, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1017790003, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1231067764, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-1406660101, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1926448708, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1713170947, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that AND clauses in queries on nested record stores with concatenated repeats are implemented properly.
     */
    @DualPlannerTest
    public void nestedWithAndConcat() throws Exception {
        final RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("stats$school");
            metaData.addIndex("RestaurantReviewer", "stats$school", field("stats").nest(concatenateFields("school_name", "start_date")));
            metaData.getIndex("stats$school").setSubspaceKey("stats$school_2");
        };
        nestedWithAndSetup(hook);

        // Same query expressed two ways.

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.field("stats").matches(
                        Query.and(
                                Query.field("start_date").greaterThan(0L),
                                Query.field("school_name").equalsValue("Human University"))))
                .build();
        RecordQueryPlan plan1 = planner.plan(query1);
        assertThat(plan1, indexScan(allOf(indexName("stats$school"), bounds(hasTupleString("([Human University, 0],[Human University]]")))));
        assertEquals(-1854785243, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1516102185, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1435917256, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Collections.singletonList(2L), fetchResultValues(plan1, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                ctx -> openNestedRecordStore(ctx, hook),
                TestHelpers::assertDiscardedNone));

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.and(
                        Query.field("stats").matches(
                                Query.field("start_date").greaterThan(0L)),
                        Query.field("stats").matches(
                                Query.field("school_name").equalsValue("Human University"))))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertThat(plan2, indexScan(allOf(indexName("stats$school"), bounds(hasTupleString("([Human University, 0],[Human University]]")))));
        assertEquals(-1854785243, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1516102185, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1435917256, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Collections.singletonList(2L), fetchResultValues(plan2, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                ctx -> openNestedRecordStore(ctx, hook),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that BETWEEN-style AND on nested fields merge properly.
     */
    @DualPlannerTest
    public void nestedWithBetween() throws Exception {
        final RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("stats$school");
            metaData.addIndex("RestaurantReviewer", "stats$school", concat(field("name"), field("stats").nest(field("start_date"))));
            metaData.getIndex("stats$school").setSubspaceKey("stats$school_2");
        };
        nestedWithAndSetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.and(
                        Query.field("name").equalsValue("Newt A. Robot"),
                        Query.field("stats").matches(
                                Query.field("start_date").greaterThan(100L)),
                        Query.field("stats").matches(
                                Query.field("start_date").lessThan(2000L))))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("stats$school"), bounds(hasTupleString("([Newt A. Robot, 100],[Newt A. Robot, 2000])")))));
        assertEquals(1355996214, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(852895263, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(222571818, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Collections.singletonList(2L), fetchResultValues(plan, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                ctx -> openNestedRecordStore(ctx, hook),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that queries on doubly nested records with fanout on the inner field work properly.
     */
    @DualPlannerTest
    public void doublyNested() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openDoublyNestedRecordStore(context);

            TestRecords5Proto.CalendarEvent.Builder eventBuilder = TestRecords5Proto.CalendarEvent.newBuilder();
            eventBuilder.setPath("ev1");
            TestRecords5Proto.CalendarEventIndex.Builder indexBuilder = eventBuilder.getEventIndexBuilder();
            TestRecords5Proto.Recurrence.Builder occurBuilder = indexBuilder.addRecurrenceBuilder();
            occurBuilder.setStart(2);
            occurBuilder.setEnd(2);
            occurBuilder = indexBuilder.addRecurrenceBuilder();
            occurBuilder.setStart(12);
            occurBuilder.setEnd(12);
            recordStore.saveRecord(eventBuilder.build());

            eventBuilder = TestRecords5Proto.CalendarEvent.newBuilder();
            eventBuilder.setPath("ev2");
            indexBuilder = eventBuilder.getEventIndexBuilder();
            occurBuilder = indexBuilder.addRecurrenceBuilder();
            occurBuilder.setStart(5);
            occurBuilder.setEnd(5);
            recordStore.saveRecord(eventBuilder.build());

            eventBuilder = TestRecords5Proto.CalendarEvent.newBuilder();
            eventBuilder.setPath("ev3");
            indexBuilder = eventBuilder.getEventIndexBuilder();
            occurBuilder = indexBuilder.addRecurrenceBuilder();
            occurBuilder.setStart(15);
            occurBuilder.setEnd(15);
            occurBuilder = indexBuilder.addRecurrenceBuilder();
            occurBuilder.setStart(25);
            occurBuilder.setEnd(25);
            recordStore.saveRecord(eventBuilder.build());

            commit(context);
        }

        // TODO this was originally:
        // QueryExpression.field("eventIndex").matches(
        //     QueryExpression.field("recurrence").matches(
        //         QueryExpression.field("start").greaterThan(10L))),
        // which should have failed validate
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("CalendarEvent")
                .setFilter(Query.field("eventIndex").matches(
                        Query.field("recurrence").oneOfThem().matches(
                                Query.field("start").greaterThan(10L))))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("event_start"), bounds(hasTupleString("([10],>"))))));
        assertEquals(667993366, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(853766432, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1543907524, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Arrays.asList("ev1", "ev3"),
                fetchResultValues(plan, TestRecords5Proto.CalendarEvent.PATH_FIELD_NUMBER,
                        this::openDoublyNestedRecordStore,
                        context -> TestHelpers.assertDiscardedAtMost(1, context)));
    }

    private Message createVersionedCalendarEvent(String path, int alarmIndexVersion, int calendarEventIndexVersion) {
        final TestRecords5Proto.CalendarEvent.Builder builder = TestRecords5Proto.CalendarEvent.newBuilder();
        builder.setPath(path);
        builder.setAlarmIndex(TestRecords5Proto.AlarmIndex.newBuilder().setVersion(alarmIndexVersion));
        builder.setEventIndex(TestRecords5Proto.CalendarEventIndex.newBuilder().setVersion(calendarEventIndexVersion));
        return builder.build();
    }

    /**
     * Verify that complex queries on nested messages with concatenated repeats are implemented using index scans.
     */
    @Test
    public void testConcatNested() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openConcatNestedRecordStore(context);
            recordStore.saveRecord(createVersionedCalendarEvent("ev1", 1, 1));
            recordStore.saveRecord(createVersionedCalendarEvent("ev2", 1, 2));
            recordStore.saveRecord(createVersionedCalendarEvent("ev3", 2, 4));
            recordStore.saveRecord(createVersionedCalendarEvent("ev4", 2, 6));
            recordStore.saveRecord(createVersionedCalendarEvent("ev5", 3, 1));
            recordStore.saveRecord(createVersionedCalendarEvent("ev6", 3, 2));

            commit(context);
        }
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("CalendarEvent")
                .setFilter(Query.field("alarmIndex").matches(Query.field("version").equalsValue(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("versions"), bounds(hasTupleString("[[3],[3]]")))));
        assertEquals(-686220795, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(963408882, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1113062695, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Arrays.asList("ev5", "ev6"),
                fetchResultValues(plan, TestRecords5Proto.CalendarEvent.PATH_FIELD_NUMBER,
                        this::openConcatNestedRecordStore,
                        TestHelpers::assertDiscardedNone));

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("CalendarEvent")
                .setFilter(Query.and(
                        Query.field("alarmIndex").matches(Query.field("version").equalsValue(2)),
                        Query.field("eventIndex").matches(Query.field("version").equalsValue(6))))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertThat(plan2, indexScan(allOf(indexName("versions"), bounds(hasTupleString("[[2, 6],[2, 6]]")))));
        assertEquals(-686220795, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(963408882, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1113062695, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Arrays.asList("ev4"),
                fetchResultValues(plan2, TestRecords5Proto.CalendarEvent.PATH_FIELD_NUMBER,
                        this::openConcatNestedRecordStore,
                        TestHelpers::assertDiscardedNone));

        RecordQuery query3 = RecordQuery.newBuilder()
                .setRecordType("CalendarEvent")
                .setFilter(Query.field("alarmIndex").matches(Query.field("version").greaterThan(1)))
                .setSort(Key.Expressions.field("alarmIndex").nest("version"))
                .build();
        RecordQueryPlan plan3 = planner.plan(query3);
        assertThat(plan3, indexScan(allOf(indexName("versions"), bounds(hasTupleString("([1],>")))));
        assertEquals(-686220795, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(963408882, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1113062695, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note: ev3/ev4 can be switched, as can ev5/ev6.
        // The important thing is that ev3 and ev4 are before both ev5 and ev6
        assertEquals(Arrays.asList("ev3", "ev4", "ev5", "ev6"),
                fetchResultValues(plan3, TestRecords5Proto.CalendarEvent.PATH_FIELD_NUMBER,
                        this::openConcatNestedRecordStore,
                        TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that record scans with nested primary keys works properly.
     * Specifically, verify that a filter is implemented as a record scan in the case where there is a two-field
     * primary key both of whose fields are nested in some header subrecord.
     */
    @DualPlannerTest
    public void testNestedPrimaryKeyQuery() throws Exception {
        final RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(concat(
                            field("header").nest(field("path")),
                            field("header").nest(field("rec_no"))));
        };
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            saveHeaderRecord(1, "a", 0, "able");
            saveHeaderRecord(2, "a", 3, "baker");
            commit(context);
        }
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.and(
                        Query.field("header").matches(Query.field("path").equalsValue("a")),
                        Query.field("header").matches(Query.field("rec_no").equalsValue(2L))))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, scan(bounds(hasTupleString("[[a, 2],[a, 2]]"))));
        assertEquals(1265534819, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(136710600, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1817343447, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                assertTrue(result.hasNext());
                TestRecordsWithHeaderProto.MyRecord record = parseMyRecord(result.get().getRecord());
                assertEquals("baker", record.getStrValue());
                assertFalse(cursor.getNext().hasNext());
            }
            TestHelpers.assertDiscardedNone(context);
        }
    }
}
