/*
 * FDBCoveringIndexQueryTest.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleSet;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.temp.matchers.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ValueMatchers.fieldValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests related to planning queries that can use covering indexes.
 */
@Tag(Tags.RequiresFDB)
class FDBCoveringIndexQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that a covering index is used when possible.
     */
    @DualPlannerTest
    void coveringSimple() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Collections.singletonList(field("num_value_unique")))
                .build();

        // Covering(Index(MySimpleRecord$num_value_unique ([990],>) -> [num_value_unique: KEY[0], rec_no: KEY[1]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-158312359, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1293351441, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1374755849, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValueUnique() > 990);
                    assertFalse(myrec.hasNumValue2());
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a covering index is used when possible.
     */
    @SuppressWarnings("unchecked")
    @DualPlannerTest
    void coveringOff() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Collections.singletonList(field("num_value_unique")))
                .build();

        // Covering(Index(MySimpleRecord$num_value_unique ([990],>) -> [num_value_unique: KEY[0], rec_no: KEY[1]])
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setDisabledTransformationRuleNames(ImmutableSet.of(
                        "PushFilterThroughFetchRule",
                        "PushDistinctThroughFetchRule",
                        "PushSetOperationThroughFetchRule",
                        "MergeProjectionAndFetchRule"), PlannerRuleSet.DEFAULT).build());
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312359, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1293351441, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1374755849, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312359, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(594363437, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(512959029, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValueUnique() > 990);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a covering index is used with a compatible sort on the query.
     */
    @DualPlannerTest
    void coveringSortNoFilter() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(field("num_value_3_indexed"))
                .setRequiredResults(Collections.singletonList(field("num_value_3_indexed")))
                .build();

        // Covering(Index(MySimpleRecord$num_value_3_indexed <,>) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(unbounded()))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(413789395, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1655846226, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1655846226, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    /**
     * Verify that a covering index is not used when it does not include enough fields; a regular index is used instead.
     */
    @DualPlannerTest
    void coveringSimpleInsufficient() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"), true)
                .setRequiredResults(Arrays.asList(
                        field("num_value_unique"),
                        field("num_value_3_indexed")))
                .build();

        // Index(MySimpleRecord$num_value_unique ([990],> REVERSE)
        RecordQueryPlan plan = planner.plan(query);
        assertTrue(plan.isReverse());
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312358, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(594363257, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(512958849, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312358, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(594363251, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(512958843, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that some other index scan is used when there is no appropriate index for the returned fields.
     */
    @DualPlannerTest
    void notCoveringRecordScan() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setRequiredResults(Collections.singletonList(field("num_value_3_indexed")))
                .build();

        // Index(MySimpleRecord$str_value_indexed <,>)
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(unbounded()));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(324762954, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(19722381, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(19722381, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(unbounded()))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(413789395, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1655846226, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1655846226, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that if the filter contains additional parameters not satisfiable by an otherwise
     * covering index that it is not used.
     */
    @DualPlannerTest
    void notCoveringWithAdditionalFilter() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").equalsValue(1), Query.field("num_value_2").lessThan(2)))
                .setRequiredResults(Collections.singletonList(field("num_value_3_indexed")))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) | num_value_2 LESS_THAN 2
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    filterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))
                    .where(queryComponents(exactly(equalsObject(Query.field("num_value_2").lessThan(2)))));
            assertMatchesExactly(plan, planMatcher);
            
            assertEquals(-1408807323, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1474845065, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1103679372, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))
                            .where(predicates(only(valuePredicate(fieldValue("num_value_2"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 2)))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(728152174, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-675231931, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-304066238, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that a filter not satisfied by the index scan itself but using fields present in the index
     * can still allow a covering scan with the filter on the partial records.
     */
    @DualPlannerTest
    void coveringWithAdditionalFilter() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_3_indexed", "num_value_2"));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").lessThan(1), Query.field("num_value_2").lessThan(2)))
                .setRequiredResults(Collections.singletonList(field("num_value_3_indexed")))
                .build();

        // Covering(Index(multi_index ([null],[1])) -> [num_value_2: KEY[1], num_value_3_indexed: KEY[0], rec_no: KEY[2]]) | num_value_2 LESS_THAN 2
        RecordQueryPlan plan = planner.plan(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    filterPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([null],[1])"))))))
                            .where(queryComponents(exactly(equalsObject(Query.field("num_value_2").lessThan(2)))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1374002128, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1359983418, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1492450855, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    predicatesFilterPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([null],[1])"))))))
                            .where(predicates(only(valuePredicate(fieldValue("num_value_2"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 2)))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(762957369, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-2135370744, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-692837721, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        }
    }

    /**
     * Verify that an extra covering filter can use a nested field.
     */
    @DualPlannerTest
    void coveringWithAdditionalNestedFilter() {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord").setPrimaryKey(field("header").nest(field("rec_no")));
            builder.addIndex("MyRecord", "multi", concat(field("str_value"), field("header").nest(concatenateFields("path", "num"))));
            RecordMetaData metaData = builder.getRecordMetaData();
            createOrOpenRecordStore(context, metaData);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MyRecord")
                    .setFilter(Query.and(Query.field("str_value").equalsValue("abc"), Query.field("header").matches(Query.field("num").equalsValue(1))))
                    .build();

            // Fetch(Covering(Index(multi [[abc],[abc]]) -> [str_value: KEY[0], header: [num: KEY[2], path: KEY[1], rec_no: KEY[3]]]) | header/{num EQUALS 1})
            RecordQueryPlan plan = planner.plan(query);
            if (planner instanceof RecordQueryPlanner) {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        fetchFromPartialRecordPlan(
                                filterPlan(
                                        coveringIndexPlan()
                                                .where(indexPlanOf(indexPlan().where(indexName("multi")).and(scanComparisons(range("[[abc],[abc]]"))))))
                                        .where(queryComponents(exactly(equalsObject(Query.field("header").matches(Query.field("num").equalsValue(1)))))));
                assertMatchesExactly(plan, planMatcher);

                assertEquals(-1536005152, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(1350035332, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(-1843652335, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            } else {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        fetchFromPartialRecordPlan(
                                predicatesFilterPlan(
                                        coveringIndexPlan()
                                                .where(indexPlanOf(indexPlan().where(indexName("multi")).and(scanComparisons(range("[[abc],[abc]]"))))))
                                        .where(predicates(only(valuePredicate(fieldValue("header.num"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1)))))
                        );
                assertMatchesExactly(plan, planMatcher);
                
                assertEquals(1623341655, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
                assertEquals(2019556616, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
                assertEquals(-1174131051, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            }
        }
    }

    /**
     * Verify that an index can be covering if more than one field is required and they are in the key.
     */
    @DualPlannerTest
    void coveringMulti() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_unique", "num_value_2"));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Arrays.asList(
                        field("num_value_unique"),
                        field("num_value_2")))
                .build();

        // Covering(Index(multi_index ([990],>) -> [num_value_2: KEY[1], num_value_unique: KEY[0], rec_no: KEY[2]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([990],>")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(291429560, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1065678, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-80338730, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValueUnique() > 990);
                    assertEquals(myrec.getNumValue2(), (999 - i) % 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that an index can be covering if some of the required fields are in the value part of the index.
     */
    @DualPlannerTest
    void coveringMultiValue() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index(
                    "multi_index_value",
                    field("num_value_unique"),
                    field("num_value_2"),
                    IndexTypes.VALUE,
                    IndexOptions.UNIQUE_OPTIONS));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Arrays.asList(
                        field("num_value_unique"),
                        field("num_value_2")))
                .build();

        // Covering(Index(multi_index_value ([990],>) -> [num_value_2: VALUE[0], num_value_unique: KEY[0], rec_no: KEY[1]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("multi_index_value")).and(scanComparisons(range("([990],>")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-782505942, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(450250048, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(368845640, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValueUnique() > 990);
                    assertEquals(myrec.getNumValue2(), (999 - i) % 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that an index can be covering for nested fields if the field is in the value of the index.
     */
    @DualPlannerTest
    void coveringWithHeaderValue() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(field("rec_no")));
            metaData.addIndex("MyRecord", new Index("MyRecord$str_value", field("str_value"), field("header").nest(field("path")),
                    IndexTypes.VALUE, Collections.emptyMap()));
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("lion"))
                .setRequiredResults(Collections.singletonList(field("header").nest("path")))
                .build();

        // Covering(Index(MyRecord$str_value [[lion],[lion]]) -> [str_value: KEY[0], header: [path: VALUE[0], rec_no: KEY[1]]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[lion],[lion]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-629018945, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(177826375, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(344218219, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    /**
     * Verify that an index can be covering for concatenated nested fields in the value of the index.
     */
    @DualPlannerTest
    void coveringWithHeaderConcatenatedValue() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(field("rec_no")));
            metaData.addIndex("MyRecord", new Index("MyRecord$str_value", field("str_value"), field("header").nest(concatenateFields("path", "num")),
                    IndexTypes.VALUE, Collections.emptyMap()));
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("leopard"))
                .setRequiredResults(Collections.singletonList(field("header").nest(concatenateFields("num", "path"))))
                .build();

        // Covering(Index(MyRecord$str_value [[leopard],[leopard]]) -> [str_value: KEY[0], header: [num: VALUE[1], path: VALUE[0], rec_no: KEY[1]]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[leopard],[leopard]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-568702564, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1766803018, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(344218219, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    /**
     * Verify that an index can be covering if the required fields are in the primary key.
     */
    @DualPlannerTest
    void coveringWithHeader() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
            metaData.addIndex("MyRecord", "str_value");
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            saveHeaderRecord(1, "a", 0, "lynx");
            saveHeaderRecord(2, "a", 1, "bobcat");
            saveHeaderRecord(3, "a", 2, "panther");

            saveHeaderRecord(1, "b", 3, "jaguar");
            saveHeaderRecord(2, "b", 4, "leopard");
            saveHeaderRecord(3, "b", 5, "lion");
            saveHeaderRecord(4, "b", 6, "tiger");
            context.commit();
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("lion"))
                .setRequiredResults(Collections.singletonList(field("header").nest("rec_no")))
                .build();

        // Covering(Index(MyRecord$str_value [[lion],[lion]]) -> [str_value: KEY[0], header: [path: KEY[1], rec_no: KEY[2]]])
        RecordQueryPlan plan = planner.plan(query);
        BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[lion],[lion]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-629018945, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(177826375, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(344218219, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals(3, myrec.getHeader().getRecNo());
                }
            }
            context.commit();
            assertDiscardedNone(context);
        }

        query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").startsWith("l"))
                .setRequiredResults(Arrays.asList(field("header").nest(concatenateFields("path", "rec_no")), field("str_value")))
                .build();

        // Covering(Index(MyRecord$str_value {[l],[l]}) -> [str_value: KEY[0], header: [path: KEY[1], rec_no: KEY[2]]])
        plan = planner.plan(query);
        planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("{[l],[l]}")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-1471907004, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1581115138, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1123663700, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            List<Pair<String, Long>> results = new ArrayList<>();
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertThat(myrec.getStrValue(), startsWith("l"));
                    assertThat(myrec.getHeader().hasPath(), is(true));
                    assertThat(myrec.getHeader().hasRecNo(), is(true));
                    results.add(Pair.of(myrec.getHeader().getPath(), myrec.getHeader().getRecNo()));
                }
            }
            assertEquals(Arrays.asList(Pair.of("b", 2L), Pair.of("b", 3L), Pair.of("a", 1L)), results);
            context.commit();
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that if given a concatenated required-results field that a covering index is returned.
     */
    @DualPlannerTest
    void coveringConcatenatedFields() throws Exception {
        RecordMetaDataHook hook = metaData ->
                metaData.addIndex("MySimpleRecord", "MySimpleRecord$2+3", concatenateFields("num_value_2", "num_value_3_indexed"));
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_2").greaterThan(0), Query.field("num_value_2").lessThan(10)))
                .setRequiredResults(Collections.singletonList(concatenateFields("num_value_2", "num_value_3_indexed")))
                .build();

        // Covering(Index(MySimpleRecord$2+3 ([0],[10])) -> [num_value_2: KEY[0], num_value_3_indexed: KEY[1], rec_no: KEY[2]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$2+3")).and(scanComparisons(range("([0],[10])")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(1722836804, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-992322107, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(2083564653, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertThat(myrec.getNumValue2(), greaterThan(0));
                    assertThat(myrec.getNumValue2(), lessThan(10));
                    assertThat(myrec.hasNumValue3Indexed(), is(true));
                }
            }
            commit(context);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that covering indexes are not used when the an outer "header" field is missing from the primary key,
     * even though the index has all of the fields that the query actually asks for.
     */
    @DualPlannerTest
    void notCoveringWithRequiredFieldsNotAvailable() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    // Even though path is required, it isn't part of the primary key, so won't be in the index,
                    // so won't be covering.
                    .setPrimaryKey(field("header").nest(field("rec_no")));
            metaData.addIndex("MyRecord", "str_value");
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("lion"))
                .setRequiredResults(Collections.singletonList(field("header").nest("rec_no")))
                .build();

        // Index(MyRecord$str_value [[lion],[lion]])
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[lion],[lion]]")));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-629018945, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(2065541259, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-2063034193, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    /**
     * Verify that selecting the group key and the aggregate function from a grouped aggregate index can be planned
     * by a covering aggregate index.
     */
    @Test
    void queryCoveringAggregate() {
        Index sumIndex = new Index("value3sum", field("num_value_3_indexed").groupBy(Key.Expressions.concatenateFields("str_value_indexed", "num_value_2")), IndexTypes.SUM);
        RecordMetaDataHook hook = metaData -> metaData.addIndex("MySimpleRecord", sumIndex);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int i = 0; i < 20; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValue2(i % 3);
                recBuilder.setNumValue3Indexed(i % 5);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .setRequiredResults(Arrays.asList(field("str_value_indexed"), field("num_value_2")))
                .build();
        // This is here since the main planner doesn't currently support planning aggregates, so it's basically a
        // separate "mini-planner".
        // TODO: Support aggregate planning in the main query planner (https://github.com/FoundationDB/fdb-record-layer/issues/14)
        RecordQueryPlan plan = ((RecordQueryPlanner) planner).planCoveringAggregateIndex(query, "value3sum");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(Objects.requireNonNull(plan)).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    int sum = 0;
                    for (int j = 0; j < 20; j += 2) {
                        if (j % 3 == myrec.getNumValue2()) {
                            sum += j % 5;
                        }
                    }
                    assertEquals(sum, Objects.requireNonNull(rec.getIndexEntry()).getValue().getLong(0));
                    i++;
                }
            }
            assertEquals(3, i);
            assertDiscardedNone(context);
        }
    }

    @Test
    @Disabled
    void nestedRepeatedSplitCoveringIndex() throws Exception {
        nestedWithAndSetup(metaData -> {
            metaData.removeIndex("review_rating");
            metaData.addIndex("RestaurantRecord", "splitCoveringIndex",
                    keyWithValue(field("reviews", KeyExpression.FanType.FanOut).nest(field("rating"), field("reviewer")), 1));
        });

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("RestaurantRecord")
                .setFilter(Query.field("reviews").oneOfThem().matches(Query.field("rating").greaterThan(2)))
                .setRequiredResults(ImmutableList.of(field("reviews", KeyExpression.FanType.FanOut).nest(field("rating"))))
                .setRemoveDuplicates(false)
                .build();

        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("splitCoveringIndex")).and(scanComparisons(range("([0],>")))));
        assertMatchesExactly(plan, planMatcher);
    }

    /**
     * Verify that a covering index can have redundant duplicated fields.
     */
    @DualPlannerTest
    void coveringRedundant() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_2", "num_value_unique", "num_value_2"));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Arrays.asList(
                        field("num_value_unique"),
                        field("num_value_2")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[1],[1]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(1372089780, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1440154798, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1095794309, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }
    
}
