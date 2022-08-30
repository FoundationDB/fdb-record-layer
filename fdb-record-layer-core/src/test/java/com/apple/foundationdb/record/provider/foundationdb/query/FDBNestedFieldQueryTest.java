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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryRecordFunction;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.SetMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.flatMapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexScanType;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the planner's ability to handle nested fields.
 */
@Tag(Tags.RequiresFDB)
class FDBNestedFieldQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that simple queries on nested fields can use bounds on a record scan.
     */
    @DualPlannerTest
    void hierarchical() throws Exception {
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

        // Scan([[photos],[photos]])
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    scanPlan().where(scanComparisons(range("[[photos],[photos]]"))));
            assertEquals(1063779424, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-845866877, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1788735724, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    scanPlan().where(scanComparisons(range("[[photos],[photos]]"))));
            assertEquals(1063779424, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1949336585, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(288971890, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(Arrays.asList(12, 11), fetchResultValues(plan, TestRecords3Proto.MyHierarchicalRecord.NUM_VALUE_INDEXED_FIELD_NUMBER,
                this::openHierarchicalRecordStore,
                TestHelpers::assertDiscardedNone));

        query = RecordQuery.newBuilder()
                .setRecordType("MyHierarchicalRecord")
                .setFilter(Query.field("parent_path").startsWith("photos"))
                .build();

        // Scan({[photos],[photos]})
        plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    scanPlan().where(scanComparisons(range("{[photos],[photos]}"))));
            assertEquals(224213141, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2081868884, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-783433835, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    scanPlan().where(scanComparisons(range("{[photos],[photos]}"))));
            assertEquals(224213141, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(582105050, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2011769627, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(Arrays.asList(12, 11, 112, 111, 1111), fetchResultValues(plan, TestRecords3Proto.MyHierarchicalRecord.NUM_VALUE_INDEXED_FIELD_NUMBER,
                this::openHierarchicalRecordStore,
                TestHelpers::assertDiscardedNone));
    }

    private void addDataForNested() throws Exception {
        addDataForNested(null);
    }

    private void addDataForNested(@Nullable final RecordMetaDataHook hook) throws Exception {
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
    }

    /**
     * Verify that nested field comparisons with fanout can scan indexes.
     */
    @DualPlannerTest
    void nested() throws Exception {
        addDataForNested();

        final var query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(Query.field("reviews").oneOfThem().matches(Query.field("rating").greaterThan(5)))
                .build();

        // Index(review_rating ([5],>) | UnorderedPrimaryKeyDistinct()
        final var plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    unorderedPrimaryKeyDistinctPlan(
                            indexPlan()
                                    .where(indexName("review_rating"))
                                    .and(scanComparisons(range("([5],>")))));
            assertEquals(1378568952, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-2085209333, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2129300140, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("review_rating"))
                                                    .and(scanComparisons(range("([5],>"))))))));
            assertEquals(1060048085, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1589243362, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1669701185, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        assertEquals(Collections.singletonList(101L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                this::openNestedRecordStore,
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that nested field comparisons with fanout can scan indexes.
     */
    @DualPlannerTest
    void nested2() throws Exception {
        addDataForNested();

        final var query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(Query.field("tags").oneOfThem().matches(
                        Query.and(
                                Query.field("value").equalsValue("Lilliput"),
                                Query.field("weight").greaterThanOrEquals(5))))
                .build();

        // Index(tag [[Lilliput, 5],[Lilliput]]) | UnorderedPrimaryKeyDistinct()
        final var plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    unorderedPrimaryKeyDistinctPlan(
                            indexPlan()
                                    .where(indexName("tag"))
                                    .and(scanComparisons(range("[[Lilliput, 5],[Lilliput]]")))));
            assertEquals(-1197819382, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1570485504, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1584619812, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("tag"))
                                                    .and(scanComparisons(range("[[Lilliput, 5],[Lilliput]]"))))))));
            assertEquals(205198931, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2066451475, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2080585783, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(Collections.singletonList(101L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                this::openNestedRecordStore,
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that nested field comparisons with fanout can scan indexes.
     */
    @DualPlannerTest
    void nested3() throws Exception {
        addDataForNested();

        final var reviewFilter = Query.field("reviews").oneOfThem().matches(Query.and(
                Query.field("rating").equalsValue(5),
                Query.field("reviewer").equalsValue(1L)));
        final var query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(reviewFilter)
                .build();
        final var plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    indexPlan()
                                            .where(indexName("review_rating"))
                                            .and(scanComparisons(range("[[5],[5]]")))))
                            .where(queryComponents(only(equalsObject(reviewFilter)))));
            assertEquals(1252155441, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-2056078191, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1471222808, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(Collections.singletonList(102L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));
        } else {
            // Note that in cascades the index plan has a "higher cost" than the primary scan plan (per conf flag to
            // prefer primary scans when comparing equal-ish index and scan plans).
            assertMatchesExactly(plan,
                    unorderedPrimaryKeyDistinctPlan(
                            flatMapPlan(
                                    indexPlan()
                                            .where(indexName("review_rating"))
                                            .and(scanComparisons(range("[[5],[5]]"))),
                                    descendantPlans(
                                            predicatesFilterPlan(anyPlan())
                                                    .where(predicates(
                                                            SetMatcher.exactlyInAnyOrder(
                                                                    valuePredicate(fieldValue("rating"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5)),
                                                                    valuePredicate(fieldValue("reviewer"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)))))))));
            assertEquals(-83822384, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2065397699, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(446113990, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertEquals(Collections.singletonList(102L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    context -> TestHelpers.assertDiscardedAtMost(5, context)));
        }
    }

    /**
     * Verify that nested field comparisons with fanout can scan indexes.
     */
    @DualPlannerTest
    void nested4() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("RestaurantRecord", "complex", concat(field("name"), field("rest_no"), field("reviews", KeyExpression.FanType.FanOut).nest(concat(field("reviewer"), field("rating")))));
            metaData.addIndex("RestaurantRecord", "composite", concat(field("name"), field("rest_no")));
            metaData.addIndex("RestaurantRecord", "duplicates", concat(field("name"), field("name")));
        };

        addDataForNested(hook);

        final QueryComponent nestedComponent =
                Query.field("reviews").oneOfThem().matches(Query.and(Query.field("reviewer").equalsValue(10L), Query.field("rating").equalsValue(20)));
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(Query.and(
                        Query.field("name").equalsValue("something"),
                        Query.field("name").equalsValue("something"),
                        Query.field("rest_no").equalsValue(1L),
                        nestedComponent))
                .build();
        RecordQueryPlan plan = planner.plan(query);

        if (planner instanceof RecordQueryPlanner) {
            // Does not understand duplicate condition
            assertMatchesExactly(plan,
                    filterPlan(
                            indexPlan()
                                    .where(indexName("duplicates"))
                                    .and(scanComparisons(range("[[something, something, 1],[something, something, 1]]"))))
                            .where(queryComponents(only(equalsObject(nestedComponent)))));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan().where(indexName("complex"))
                                                    .and(scanComparisons(range("[[something, 1, 10, 20],[something, 1, 10, 20]]"))))))));

        }
    }

    /**
     * Verify that AND clauses in queries on nested record stores are implemented so that the AND is expressed as a
     * condition on the parent field, rather than as an AND of separate nested conditions.
     */
    @DualPlannerTest
    void nestedWithAnd() throws Exception {
        nestedWithAndSetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.field("stats").matches(
                        Query.and(
                                Query.field("start_date").greaterThan(0L),
                                Query.field("school_name").equalsValue("Human University"))))
                .build();

        // Index(stats$school ([0],>) | stats/{school_name EQUALS Human University}
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(
                            indexPlan()
                                    .where(indexName("stats$school"))
                                    .and(scanComparisons(range("([0],>"))))
                            .where(queryComponents(only(equalsObject(Query.field("stats").matches(Query.field("school_name").equalsValue("Human University")))))));

            assertEquals(-417538532, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-808841176, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1039128921, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(
                            indexPlan()
                                    .where(indexName("stats$school"))
                                    .and(scanComparisons(range("([0],>"))))
                            .where(predicates(
                                    valuePredicate(fieldValue("stats.school_name"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Human University")))));

            assertEquals(-2139547699, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(766602000, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1680395199, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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

        // Index(stats$school ([null],[1000]]) | stats/{And([school_name LESS_THAN University of Procrastination, hometown STARTS_WITH H])}
        // Index(stats$school ([null],[1000]]) | And([$85876e0f-5bbb-4a78-baaf-b3b0eae60423/stats.hometown STARTS_WITH H, $85876e0f-5bbb-4a78-baaf-b3b0eae60423/stats.school_name LESS_THAN University of Procrastination])
        plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(
                            indexPlan()
                                    .where(indexName("stats$school"))
                                    .and(scanComparisons(range("([null],[1000]]"))))
                            .where(queryComponents(only(equalsObject(Query.field("stats").matches(
                                            Query.and(Query.field("school_name").lessThan("University of Procrastination"),
                                                    Query.field("hometown").startsWith("H"))))))));

            assertEquals(1700959433, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(336906555, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-400610616, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(
                            indexPlan()
                                    .where(indexName("stats$school"))
                                    .and(scanComparisons(range("([null],[1000]]"))))
                            .where(predicates(
                                    valuePredicate(fieldValue("stats.hometown"), new Comparisons.SimpleComparison(Comparisons.Type.STARTS_WITH, "H")),
                                    valuePredicate(fieldValue("stats.school_name"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "University of Procrastination")))));

            assertEquals(-1842706543, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1351064721, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1486060602, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(Collections.singletonList(1L), fetchResultValues(plan, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                this::openNestedRecordStore,
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that AND conditions involving nested non-repeated fields still work when index has non-nested fields,
     * no matter which way the nested conditions are expressed.
     */
    @DualPlannerTest
    void nestedThenWithAnd() throws Exception {
        final RecordMetaDataHook hook = metaData ->
                metaData.addIndex("RestaurantReviewer", "emailHometown", concat(field("email"), field("stats").nest(concatenateFields("hometown", "start_date"))));
        nestedWithAndSetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.and(
                        Query.field("stats").matches(Query.and(
                                Query.field("start_date").lessThanOrEquals(0L),
                                Query.field("hometown").equalsValue("Home Town"))),
                        Query.field("email").equalsValue("pmp@example.com")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                        indexPlan()
                                .where(indexName("emailHometown"))
                                .and(scanComparisons(range("([pmp@example.com, Home Town, null],[pmp@example.com, Home Town, 0]]"))));
        
        assertEquals(-688450117, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1159885451, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(608425592, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Collections.singletonList(1L), fetchResultValues(plan, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                context -> openNestedRecordStore(context, hook),
                TestHelpers::assertDiscardedNone));

        query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.and(
                        Query.field("stats").matches(Query.field("start_date").lessThanOrEquals(0L)),
                        Query.field("email").equalsValue("pmp@example.com"),
                        Query.field("stats").matches(Query.field("hometown").equalsValue("Home Town"))))
                .build();
        assertEquals(plan, planner.plan(query));
    }

    /**
     * Verify that matching part of a nested field only uses part of the index.
     */
    @DualPlannerTest
    public void nestedThenWithAndPartial() throws Exception {
        final RecordMetaDataHook hook = metaData ->
                metaData.addIndex("RestaurantReviewer", "hometownEmail", concat(field("stats").nest(concatenateFields("hometown", "school_name", "start_date")), field("email")));
        nestedWithAndSetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantReviewer")
                .setFilter(Query.and(
                        Query.field("stats").matches(Query.and(
                                Query.field("hometown").equalsValue("Home Town"),
                                Query.field("school_name").equalsValue("University of Learning"))),
                        Query.field("email").equalsValue("pmp@example.com")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(
                            indexPlan()
                                    .where(indexName("hometownEmail"))
                                    .and(scanComparisons(range("[[Home Town, University of Learning],[Home Town, University of Learning]]"))))
                            .where(queryComponents(only(equalsObject(Query.field("email").equalsValue("pmp@example.com"))))));

            assertEquals(895882018, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(146214617, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1382626015, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    predicatesFilterPlan(
                            indexPlan()
                                    .where(indexName("hometownEmail"))
                                    .and(scanComparisons(range("[[Home Town, University of Learning],[Home Town, University of Learning]]"))))
                            .where(predicates(
                                    valuePredicate(fieldValue("email"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "pmp@example.com")))));

            assertEquals(-1385621911, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(945827751, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-2112728147, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        assertEquals(Collections.singletonList(1L), fetchResultValues(plan, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                context -> openNestedRecordStore(context, hook),
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
    void nestedAndOnNestedMap() {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsNestedMapProto.getDescriptor());
            metaDataBuilder.addIndex("OuterRecord", "key_index", concat(
                    field("other_id"),
                    field("map").nest(field("entry", KeyExpression.FanType.FanOut).nest("key"))));
            createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
            commit(context);
        }

        final var mapFilterComponent = Query.field("map").matches(
                Query.field("entry").oneOfThem().matches(
                        Query.and(
                                Query.field("key").equalsValue("alpha"),
                                Query.field("value").notEquals("test"))));
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("OuterRecord")
                .setFilter(Query.and(
                        Query.field("other_id").equalsValue(1L),
                        mapFilterComponent))
                .build();

        // Index(key_index [[1, alpha],[1, alpha]]) | UnorderedPrimaryKeyDistinct() | map/{one of entry/{And([key EQUALS alpha, value NOT_EQUALS test])}}
        RecordQueryPlan plan = planner.plan(query);
        // verify that the value filter that can't be satisfied by the index isn't dropped from the filter expression
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    filterPlan(
                            unorderedPrimaryKeyDistinctPlan(indexPlan()
                                    .where(indexName("key_index"))
                                    .and(scanComparisons(range("[[1, alpha],[1, alpha]]")))))
                            .where(queryComponents(ListMatcher.exactly(equalsObject(mapFilterComponent)))));

            assertEquals(-1406660101, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-16989308, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1707510741, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            // Note that in cascades the index plan has a "higher cost" than the primary scan plan (per conf flag to
            // prefer primary scans when comparing equal-ish index and scan plans).
            assertMatchesExactly(plan,
                    unorderedPrimaryKeyDistinctPlan(
                            flatMapPlan(
                                    indexPlan()
                                            .where(indexName("key_index"))
                                            .and(scanComparisons(range("[[1, alpha],[1, alpha]]"))),
                                    descendantPlans(
                                            predicatesFilterPlan(anyPlan())
                                                    .where(predicates(
                                                            SetMatcher.exactlyInAnyOrder(
                                                                    valuePredicate(fieldValue("key"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "alpha")),
                                                                    valuePredicate(fieldValue("value"), new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, "test")))))))));

            assertEquals(1553162232, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1581979808, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(417122259, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that AND clauses in queries on nested record stores with concatenated repeats are implemented properly.
     */
    @DualPlannerTest
    void nestedWithAndConcat() throws Exception {
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

        // Index(stats$school ([Human University, 0],[Human University]])
        RecordQueryPlan plan1 = planner.plan(query1);
        assertMatchesExactly(plan1,
                indexPlan()
                        .where(indexName("stats$school"))
                        .and(scanComparisons(range("([Human University, 0],[Human University]]"))));
        assertEquals(-1854785243, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(245473758, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1563763213, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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

        // Index(stats$school ([Human University, 0],[Human University]])
        RecordQueryPlan plan2 = planner.plan(query2);
        assertMatchesExactly(plan2,
                indexPlan()
                        .where(indexName("stats$school"))
                        .and(scanComparisons(range("([Human University, 0],[Human University]]"))));
        assertEquals(-1854785243, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(245473758, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1563763213, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Collections.singletonList(2L), fetchResultValues(plan2, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                ctx -> openNestedRecordStore(ctx, hook),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that BETWEEN-style AND on nested fields merge properly.
     */
    @DualPlannerTest
    void nestedWithBetween() throws Exception {
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

        // Index(stats$school ([Newt A. Robot, 100],[Newt A. Robot, 2000]))
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                indexPlan()
                        .where(indexName("stats$school"))
                        .and(scanComparisons(range("([Newt A. Robot, 100],[Newt A. Robot, 2000])"))));
        assertEquals(1355996214, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(669950614, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1690206997, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Collections.singletonList(2L), fetchResultValues(plan, TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER,
                ctx -> openNestedRecordStore(ctx, hook),
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that queries on doubly nested records with fanout on the inner field work properly.
     */
    @DualPlannerTest
    void doublyNested() throws Exception {
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

        // Index(event_start ([10],>) | UnorderedPrimaryKeyDistinct()
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    unorderedPrimaryKeyDistinctPlan(
                            indexPlan()
                                    .where(indexName("event_start"))
                                    .and(scanComparisons(range("([10],>")))));

            assertEquals(667993366, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1217457303, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1297919931, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    fetchFromPartialRecordPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    coveringIndexPlan()
                                            .where(indexPlanOf(indexPlan()
                                                    .where(indexName("event_start"))
                                                    .and(scanComparisons(range("([10],>"))))))));
            assertEquals(380986279, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-721491332, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-801953960, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
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
    @DualPlannerTest
    void testConcatNested() throws Exception {
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

        // Index(versions [[3],[3]])
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                indexPlan()
                        .where(indexName("versions"))
                        .and(scanComparisons(range("[[3],[3]]"))));
        assertEquals(-686220795, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-199094493, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(145206414, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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

        // Index(versions [[2, 6],[2, 6]])
        RecordQueryPlan plan2 = planner.plan(query2);
        assertMatchesExactly(plan2,
                indexPlan()
                        .where(indexName("versions"))
                        .and(scanComparisons(range("[[2, 6],[2, 6]]"))));
        assertEquals(-686220795, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-199094493, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(145206414, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(Collections.singletonList("ev4"),
                fetchResultValues(plan2, TestRecords5Proto.CalendarEvent.PATH_FIELD_NUMBER,
                        this::openConcatNestedRecordStore,
                        TestHelpers::assertDiscardedNone));

        RecordQuery query3 = RecordQuery.newBuilder()
                .setRecordType("CalendarEvent")
                .setFilter(Query.field("alarmIndex").matches(Query.field("version").greaterThan(1)))
                .setSort(Key.Expressions.field("alarmIndex").nest("version"))
                .build();

        // Index(versions ([1],>)
        RecordQueryPlan plan3 = planner.plan(query3);
        assertMatchesExactly(plan3,
                indexPlan()
                        .where(indexName("versions"))
                        .and(scanComparisons(range("([1],>"))));
        assertEquals(-686220795, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-199094493, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(145206414, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
    void testNestedPrimaryKeyQuery() throws Exception {
        final RecordMetaDataHook hook = metaData ->
                metaData.getRecordType("MyRecord")
                        .setPrimaryKey(concat(
                                field("header").nest(field("path")),
                                field("header").nest(field("rec_no"))));
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

        // Scan([[a, 2],[a, 2]])
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            assertMatchesExactly(plan,
                    scanPlan()
                            .where(scanComparisons(range("[[a, 2],[a, 2]]"))));
            assertEquals(1265534819, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1094245019, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-2039475834, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertMatchesExactly(plan,
                    scanPlan()
                            .where(scanComparisons(range("[[a, 2],[a, 2]]"))));
            assertEquals(1265534819, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-2081530776, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1268205705, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                assertTrue(result.hasNext());
                TestRecordsWithHeaderProto.MyRecord record = parseMyRecord(Objects.requireNonNull(result.get()).getRecord());
                assertEquals("baker", record.getStrValue());
                assertFalse(cursor.getNext().hasNext());
            }
            TestHelpers.assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a rank index on a map-like repeated nested message can be scanned for rank comparisons.
     */
    @Test
    void nestedRankMap() throws Exception {
        final GroupingKeyExpression rankGroup = new GroupingKeyExpression(concat(
                field("other_id"),
                field("map").nest(field("entry", KeyExpression.FanType.FanOut).nest(concatenateFields("key", "value")))), 1);
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsNestedMapProto.getDescriptor());
        metaDataBuilder.addIndex("OuterRecord", new Index("rank_value_by_key", rankGroup, IndexTypes.RANK));
        // TODO: This is not a very obvious way to specify this. But we don't have correlation names.
        final QueryComponent keyCondition = Query.field("map").matches(Query.field("entry").oneOfThem().matches(Query.field("key").equalsValue("alpha")));
        final QueryRecordFunction<Long> rank = Query.rank(rankGroup).withAdditionalCondition(keyCondition);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
            TestRecordsNestedMapProto.OuterRecord.Builder builder = TestRecordsNestedMapProto.OuterRecord.newBuilder().setOtherId(1);
            TestRecordsNestedMapProto.MapRecord.Builder mapBuilder = builder.getMapBuilder();
            builder.setRecId(1);
            mapBuilder.addEntryBuilder().setKey("alpha").setValue("abc");
            mapBuilder.addEntryBuilder().setKey("beta").setValue("bcd");
            recordStore.saveRecord(builder.build());
            builder.setRecId(2);
            mapBuilder.clear();
            mapBuilder.addEntryBuilder().setKey("alpha").setValue("aaa");
            mapBuilder.addEntryBuilder().setKey("beta").setValue("bbb");
            recordStore.saveRecord(builder.build());
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("OuterRecord")
                .setFilter(Query.and(
                        Query.field("other_id").equalsValue(1L),
                        Query.rank(rankGroup).lessThan(10L),
                        keyCondition))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertMatchesExactly(plan,
                unorderedPrimaryKeyDistinctPlan(
                        indexPlan()
                                .where(indexName("rank_value_by_key"))
                                .and(indexScanType(IndexScanType.BY_RANK))
                                .and(scanComparisons(range("([1, alpha, null],[1, alpha, 10])")))));
        assertEquals(1307013946, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1725407749, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(825274646, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                assertTrue(result.hasNext());
                var resultRecord = Objects.requireNonNull(result.get());
                assertEquals(Tuple.from(2), resultRecord.getPrimaryKey());
                result = cursor.getNext();
                assertTrue(result.hasNext());
                resultRecord = Objects.requireNonNull(result.get());
                assertEquals(Tuple.from(1), resultRecord.getPrimaryKey());
                assertEquals(1, rank.eval(recordStore, EvaluationContext.EMPTY, resultRecord.getStoredRecord()).get());
                result = cursor.getNext();
                assertFalse(result.hasNext());
            }
        }
    }
}
