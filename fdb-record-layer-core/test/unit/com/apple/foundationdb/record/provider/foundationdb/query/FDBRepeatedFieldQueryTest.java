/*
 * FDBRepeatedFieldQueryTest.java
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

import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords6Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.TestHelpers.RealAnythingMatcher.anything;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.union;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of query planning and execution for queries on records with repeated fields.
 */
@Tag(Tags.RequiresFDB)
public class FDBRepeatedFieldQueryTest extends FDBRecordStoreQueryTestBase {
    private void openDoublyRepeatedRecordStore(FDBRecordContext context) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords6Proto.getDescriptor());
        metaDataBuilder.addIndex(null, COUNT_INDEX);
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("MyRepeatedRecord"),
                new Index("rep_strings",
                        concat(field("s1", FanType.Concatenate), field("s2", FanType.Concatenate))));
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("MyRepeatedRecord"),
                new Index("s1$concat", field("s1", FanType.Concatenate)));
        createRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    /**
     * Verify that equality checks against repeated fields can scan an index scan with a FanType of Concatenate.
     */
    @Test
    public void doublyRepeated() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openDoublyRepeatedRecordStore(context);
            recordStore.deleteAllRecords();

            TestRecords6Proto.MyRepeatedRecord.Builder recBuilder = TestRecords6Proto.MyRepeatedRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.clearS1();
            recBuilder.addAllS1(Arrays.asList("aaa", "bbb"));
            recBuilder.clearS2();
            recBuilder.addAllS2(Arrays.asList("ccc", "ddd"));
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setRecNo(2);
            recBuilder.clearS1();
            recBuilder.addAllS1(Arrays.asList("aaa", "bbb", "ccc"));
            recBuilder.clearS2();
            recBuilder.addAllS2(Arrays.asList("ddd"));
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRepeatedRecord")
                .setFilter(Query.field("s1").equalsValue(Arrays.asList("aaa", "bbb")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("s1$concat"), bounds(hasTupleString("[[[aaa, bbb]],[[aaa, bbb]]]")))));
        assertEquals(2088320916, plan.planHash());
        assertEquals(Arrays.asList(1L), fetchResultValues(plan,
                TestRecords6Proto.MyRepeatedRecord.REC_NO_FIELD_NUMBER,
                this::openDoublyRepeatedRecordStore,
                TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that <code>oneOfThem()</code> does not try to scan an index with <code>FanType.Concatenate</code>.
     * Verify that <code>notEmpty()</code> or list <code>equals()</code> queries scan an index with
     * <code>FanType.Concatenate</code> and can apply a union to the result.
     */
    @Test
    public void doublyRepeatedComparison() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openDoublyRepeatedRecordStore(context);
            recordStore.deleteAllRecords();

            TestRecords6Proto.MyRepeatedRecord.Builder recBuilder = TestRecords6Proto.MyRepeatedRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.addS1("aaa");
            recBuilder.addS1("bbb");
            recBuilder.addS2("ccc");
            recordStore.saveRecord(recBuilder.build());

            recBuilder.clear();
            recBuilder.setRecNo(2);
            recBuilder.addS1("aba");
            recBuilder.addS2("ddd");
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRepeatedRecord")
                .setFilter(Query.field("s1").oneOfThem().greaterThan("b"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, filter(equalTo(query.getFilter()), scan(bounds(unbounded()))));
        assertEquals(972152650, plan.planHash());
        assertEquals(Arrays.asList(1L), fetchResultValues(plan, TestRecords6Proto.MyRepeatedRecord.REC_NO_FIELD_NUMBER,
                this::openDoublyRepeatedRecordStore,
                context -> assertDiscardedAtMost(1, context)));

        query = RecordQuery.newBuilder()
                .setRecordType("MyRepeatedRecord")
                .setFilter(Query.or(
                        Query.and(Query.field("s2").notEmpty(), Query.field("s1").equalsValue(Arrays.asList("aba"))),
                        Query.field("s1").equalsValue(Arrays.asList("aaa", "bbb"))
                ))
                .build();
        plan = planner.plan(query);
        assertThat(plan, union(
                filter(equalTo(Query.field("s2").notEmpty()),
                        indexScan(allOf(indexName("s1$concat"), bounds(hasTupleString("[[[aba]],[[aba]]]"))))),
                indexScan(allOf(indexName("s1$concat"), bounds(hasTupleString("[[[aaa, bbb]],[[aaa, bbb]]]"))))));
        assertEquals(1376647244, plan.planHash());
        assertEquals(Arrays.asList(1L, 2L), fetchResultValues(plan, TestRecords6Proto.MyRepeatedRecord.REC_NO_FIELD_NUMBER,
                this::openDoublyRepeatedRecordStore,
                context -> assertDiscardedAtMost(1, context)));
    }

    /**
     * Verify that sorts on repeated fields are implemented with fanout indexes.
     * Verify that they include distinctness filters and value filters where necessary.
     */
    @Test
    public void sortRepeated() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);
            recordStore.deleteAllRecords();

            TestRecords4Proto.RestaurantReviewer reviewer = TestRecords4Proto.RestaurantReviewer.newBuilder()
                    .setId(1L)
                    .setName("Javert")
                    .setEmail("anton@ego.com")
                    .setStats(TestRecords4Proto.ReviewerStats.newBuilder()
                            .setStartDate(100L)
                            .setHometown("Toulon")
                    )
                    .build();
            recordStore.saveRecord(reviewer);

            reviewer = TestRecords4Proto.RestaurantReviewer.newBuilder()
                    .setId(2L)
                    .setName("M. le Maire")
                    .setStats(TestRecords4Proto.ReviewerStats.newBuilder()
                            .setStartDate(120L)
                            .setHometown("Montreuil-sur-mer")
                    )
                    .build();
            recordStore.saveRecord(reviewer);

            TestRecords4Proto.RestaurantRecord restaurant = TestRecords4Proto.RestaurantRecord.newBuilder()
                    .setRestNo(1000L)
                    .setName("Chez Thénardier")
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(1L)
                                    .setRating(100)
                    )
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(2L)
                                    .setRating(0)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("l'atmosphère")
                                    .setWeight(10)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("les aliments")
                                    .setWeight(70)
                    )
                    .addCustomer("jean")
                    .addCustomer("fantine")
                    .addCustomer("cosette")
                    .addCustomer("éponine")
                    .build();
            recordStore.saveRecord(restaurant);

            restaurant = TestRecords4Proto.RestaurantRecord.newBuilder()
                    .setRestNo(1001L)
                    .setName("ABC")
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(1L)
                                    .setRating(34)
                    )
                    .addReviews(
                            TestRecords4Proto.RestaurantReview.newBuilder()
                                    .setReviewer(2L)
                                    .setRating(110)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("l'atmosphère")
                                    .setWeight(40)
                    )
                    .addTags(
                            TestRecords4Proto.RestaurantTag.newBuilder()
                                    .setValue("les aliments")
                                    .setWeight(20)
                    )
                    .addCustomer("gavroche")
                    .addCustomer("enjolras")
                    .addCustomer("éponine")
                    .build();
            recordStore.saveRecord(restaurant);

            commit(context);
        }
        {
            RecordQuery.Builder builder = RecordQuery.newBuilder()
                    .setRecordType("RestaurantRecord")
                    .setSort(field("reviews", FanType.FanOut).nest("rating"));
            RecordQuery query = builder.setRemoveDuplicates(false).build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName("review_rating"), bounds(unbounded()))));
            assertEquals(406416366, plan.planHash());
            assertEquals(Arrays.asList(1000L, 1001L, 1000L, 1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));

            query = builder.setRemoveDuplicates(true).build();
            plan = planner.plan(query);
            assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("review_rating"), bounds(unbounded())))));
            assertEquals(406416367, plan.planHash());
            assertEquals(Arrays.asList(1000L, 1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    context -> assertDiscardedAtMost(2, context)));
        }
        {
            RecordQuery.Builder builder = RecordQuery.newBuilder()
                    .setRecordType("RestaurantRecord")
                    .setSort(field("reviews", FanType.FanOut).nest("rating"), true);
            RecordQuery query = builder.setRemoveDuplicates(false).build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName("review_rating"), bounds(unbounded()))));
            assertTrue(plan.isReverse());
            assertEquals(406416367, plan.planHash());
            assertEquals(Arrays.asList(1001L, 1000L, 1001L, 1000L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));

            query = builder.setRemoveDuplicates(true).build();
            plan = planner.plan(query);
            assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("review_rating"), bounds(unbounded())))));
            assertTrue(plan.isReverse());
            assertEquals(406416368, plan.planHash());
            assertEquals(Arrays.asList(1001L, 1000L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    context -> assertDiscardedAtMost(2, context)));
        }
        {
            RecordQuery.Builder builder = RecordQuery.newBuilder()
                    .setRecordType("RestaurantRecord")
                    .setSort(field("reviews", FanType.FanOut).nest("rating"))
                    .setFilter(Query.field("name").greaterThan("A"));
            RecordQuery query = builder.setRemoveDuplicates(false).build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, filter(equalTo(query.getFilter()), indexScan(allOf(indexName("review_rating"), bounds(unbounded())))));
            assertEquals(1381942688, plan.planHash());
            assertEquals(Arrays.asList(1000L, 1001L, 1000L, 1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));

            query = builder.setRemoveDuplicates(true).build();
            plan = planner.plan(query);
            assertThat(plan, filter(equalTo(query.getFilter()), primaryKeyDistinct(
                    indexScan(allOf(indexName("review_rating"), bounds(unbounded()))))));
            assertEquals(1381942689, plan.planHash());
            assertEquals(Arrays.asList(1000L, 1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    context -> assertDiscardedAtMost(2, context)));
        }
        {
            RecordQuery.Builder builder = RecordQuery.newBuilder()
                    .setRecordType("RestaurantRecord")
                    .setSort(field("customer", FanType.FanOut))
                    .setFilter(Query.field("name").greaterThan("A"));
            RecordQuery query = builder.setRemoveDuplicates(false).build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, filter(equalTo(query.getFilter()), indexScan(allOf(indexName("customers"), bounds(unbounded())))));
            assertEquals(1833106833, plan.planHash());
            assertEquals(Arrays.asList(1000L, 1001L, 1000L, 1001L, 1000L, 1000L, 1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));

            query = builder.setRemoveDuplicates(true).build();
            plan = planner.plan(query);
            assertThat(plan, filter(equalTo(query.getFilter()), primaryKeyDistinct(
                    indexScan(allOf(indexName("customers"), bounds(unbounded()))))));
            assertEquals(1833106834, plan.planHash());
            assertEquals(Arrays.asList(1000L, 1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    context -> assertDiscardedAtMost(5, context)));
        }
        {
            RecordQuery.Builder builder = RecordQuery.newBuilder()
                    .setRecordType("RestaurantRecord")
                    .setFilter(Query.field("customer").oneOfThem().equalsValue("éponine"))
                    .setSort(field("name"));
            RecordQuery query = builder.setRemoveDuplicates(false).build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName("customers-name"), bounds(hasTupleString("[[éponine],[éponine]]")))));
            assertEquals(-574773820, plan.planHash());
            assertEquals(Arrays.asList(1001L, 1000L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));

            query = builder.setRemoveDuplicates(true).build();
            plan = planner.plan(query);
            assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("customers-name"), bounds(hasTupleString("[[éponine],[éponine]]"))))));
            assertEquals(-574773819, plan.planHash());
            assertEquals(Arrays.asList(1001L, 1000L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));
        }
        {
            RecordQuery.Builder builder = RecordQuery.newBuilder()
                    .setRecordType("RestaurantRecord")
                    .setFilter(Query.field("customer").oneOfThem().equalsValue("gavroche"))
                    .setSort(field("name"));
            RecordQuery query = builder.setRemoveDuplicates(false).build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName("customers-name"), bounds(hasTupleString("[[gavroche],[gavroche]]")))));
            assertEquals(-1720782767, plan.planHash());
            assertEquals(Collections.singletonList(1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));

            query = builder.setRemoveDuplicates(true).build();
            plan = planner.plan(query);
            assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("customers-name"), bounds(hasTupleString("[[gavroche],[gavroche]]"))))));
            assertEquals(-1720782766, plan.planHash());
            assertEquals(Collections.singletonList(1001L), fetchResultValues(plan, TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER,
                    this::openNestedRecordStore,
                    TestHelpers::assertDiscardedNone));
        }
    }

    /**
     * Verify that repeated fields can be retrieved using indexes.
     * Verify that demanding unique values forces a distinctness plan at the end.
     */
    @Test
    public void testComplexQuery7() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("repeater").oneOfThem().equalsValue(100))
                .build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1337)
                    .addRepeater(100)
                    .addRepeater(100)
                    .build());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1337)
                    .addRepeater(100)
                    .build()
            );
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder();
            Message byPrimary = recordStore.loadRecord(Tuple.from(1337)).getRecord();
            TestRecords1Proto.MySimpleRecord simplePrimary = builder.mergeFrom(byPrimary).build();
            assertEquals(1337, simplePrimary.getRecNo());
            assertEquals(Collections.singletonList(100), simplePrimary.getRepeaterList());

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, primaryKeyDistinct(indexScan(allOf(indexName("repeater$fanout"), bounds(hasTupleString("[[100],[100]]"))))));
            assertEquals(-784887869, plan.planHash());
            List<Message> byQuery = plan.execute(evaluationContext).map(FDBQueriedRecord::getRecord).asList().get();
            assertEquals(1, byQuery.size());
            assertDiscardedNone(context);
            TestRecords1Proto.MySimpleRecord simpleByQuery = builder.clear().mergeFrom(byQuery.get(0)).build();
            assertEquals(1337, simpleByQuery.getRecNo());
            assertEquals(Collections.singletonList(100), simpleByQuery.getRepeaterList());
        }
    }

    /**
     * Verifies that a query of a non-repeated field can use an index that starts with that field only if the query also
     * includes the repeated field, because repeated introduces duplicates and index entries are ordered by second field
     * and so cannot be deduplicated without additional space.
     */
    @Test
    public void testPrefixRepeated() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex(metaData.getRecordType("MySimpleRecord"),
                    new Index("prefix_repeated",
                            concat(field("num_value_2"), field("repeater", FanType.FanOut))));
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();

            TestRecords1Proto.MySimpleRecord.Builder recordBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();

            recordBuilder.setRecNo(1).setNumValue2(1).addRepeater(1).addRepeater(2);
            recordStore.saveRecord(recordBuilder.build());

            recordBuilder.setRecNo(2).setNumValue2(2).clearRepeater().addRepeater(2);
            recordStore.saveRecord(recordBuilder.build());

            recordBuilder.setRecNo(3).setNumValue2(1).clearRepeater();
            recordStore.saveRecord(recordBuilder.build());

            commit(context);
        }

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQueryPlan plan1 = planner.plan(query1);
        assertThat(plan1, filter(anything(), typeFilter(anything(), scan(bounds(unbounded())))));
        assertEquals(913370492, plan1.planHash());

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("repeater").oneOfThem().equalsValue(1)))
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertThat(plan2, primaryKeyDistinct(indexScan(allOf(indexName("prefix_repeated"),
                bounds(hasTupleString("[[1, 1],[1, 1]]"))))));
        assertEquals(-1387256366, plan2.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            List<Long> recnos = plan1.execute(evaluationContext)
                    .map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getRecNo())
                    .asList().join();
            assertEquals(Arrays.asList(1L, 3L), recnos);
            assertDiscardedAtMost(1, context);
            clearStoreCounter(context);
            recnos = plan2.execute(evaluationContext)
                    .map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getRecNo())
                    .asList().join();
            assertEquals(Arrays.asList(1L), recnos);
            assertDiscardedNone(context);
        }
    }
}
