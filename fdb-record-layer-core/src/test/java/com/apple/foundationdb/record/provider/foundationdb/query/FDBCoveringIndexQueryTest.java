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

import com.apple.foundationdb.record.ObjectPlanHash;
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
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-158312359, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-1293351441, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

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
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312359, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1293351441, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312359, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(594363437, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(unbounded()))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(413789395, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(1655846226, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        assertTrue(plan.isReverse());
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312358, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(594363257, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$num_value_unique")).and(scanComparisons(range("([990],>")));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-158312358, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(594363251, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(unbounded()));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(324762954, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(19722381, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(unbounded()))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(413789395, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1655846226, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    filterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))
                    .where(queryComponents(exactly(equalsObject(Query.field("num_value_2").lessThan(2)))));
            assertMatchesExactly(plan, planMatcher);
            
            assertEquals(-1408807323, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1474845065, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    predicatesFilterPlan(
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))))
                            .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("num_value_2"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 2)))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(468651032, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-934733073, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    filterPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([null],[1])"))))))
                            .where(queryComponents(exactly(equalsObject(Query.field("num_value_2").lessThan(2)))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1374002128, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1359983418, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    predicatesFilterPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([null],[1])"))))))
                            .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("num_value_2"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 2)))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(503456227, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1900095410, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
            RecordQueryPlan plan = planQuery(query);
            if (planner instanceof RecordQueryPlanner) {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        fetchFromPartialRecordPlan(
                                filterPlan(
                                        coveringIndexPlan()
                                                .where(indexPlanOf(indexPlan().where(indexName("multi")).and(scanComparisons(range("[[abc],[abc]]"))))))
                                        .where(queryComponents(exactly(equalsObject(Query.field("header").matches(Query.field("num").equalsValue(1)))))));
                assertMatchesExactly(plan, planMatcher);

                assertEquals(-1536005152, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1350035332, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        fetchFromPartialRecordPlan(
                                predicatesFilterPlan(
                                        coveringIndexPlan()
                                                .where(indexPlanOf(indexPlan().where(indexName("multi")).and(scanComparisons(range("[[abc],[abc]]"))))))
                                        .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("header.num"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1)))))
                        );
                assertMatchesExactly(plan, planMatcher);
                
                assertEquals(816714623, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1163509600, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([990],>")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(291429560, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(1065678, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("multi_index_value")).and(scanComparisons(range("([990],>")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-782505942, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(450250048, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[lion],[lion]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-629018945, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(177826375, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[leopard],[leopard]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-568702564, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(1766803018, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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
        RecordQueryPlan plan = planQuery(query);
        BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[lion],[lion]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-629018945, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(177826375, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

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
        plan = planQuery(query);
        planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("{[l],[l]}")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-1471907004, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-1581115138, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$2+3")).and(scanComparisons(range("([0],[10])")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(1722836804, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-992322107, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                indexPlan().where(indexName("MyRecord$str_value")).and(scanComparisons(range("[[lion],[lion]]")));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(-629018945, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(2065541259, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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

        RecordQueryPlan plan = planQuery(query);
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
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[1],[1]]")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(1372089780, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-1440154798, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @DualPlannerTest
    void coveringPrimaryKey() throws Exception {
        final Index primaryKeyIndex = new Index("primayKeyIndex", keyWithValue(concatenateFields("rec_no", "num_value_2"), 1));
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", primaryKeyIndex);
        complexQuerySetup(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("rec_no").greaterThanOrEquals(1000L))
                .setRequiredResults(List.of(field("num_value_2")))
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName(primaryKeyIndex.getName())).and(scanComparisons(range("[[1000],>")))));
        assertMatchesExactly(plan, planMatcher);

        assertEquals(1339142211, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(284123809, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void coveringWideFunctionKey() throws Exception {
        final Index nonFlattenedIndex = new Index("nonFlattenedIndex", concat(
                field("num_value_2"),
                function("non_flattened", concatenateFields("num_value_3_indexed", "str_value_indexed"))
        ));
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", nonFlattenedIndex);
        complexQuerySetup(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").greaterThanOrEquals(1))
                .setRequiredResults(List.of(field("num_value_2")))
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan().where(indexName(nonFlattenedIndex.getName())).and(scanComparisons(range("[[1],>")))));
        assertMatchesExactly(plan, planMatcher);
        
        assertEquals(782401607, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-1308646683, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(Objects.requireNonNull(plan)).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertThat(myrec.getNumValue2(), greaterThanOrEqualTo(1));
                    i++;
                }
            }
            assertEquals(66, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Function registry for {@link NonFlattenedFunction}.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class TestFunctionRegistry implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return Collections.singletonList(new FunctionKeyExpression.BiFunctionBuilder("non_flattened",
                    NonFlattenedFunction::new));
        }
    }

    private static class NonFlattenedFunction extends FunctionKeyExpression implements QueryableKeyExpression {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("NonFlattened-Function");

        public NonFlattenedFunction(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 0;
        }

        @Override
        public int getMaxArguments() {
            return Integer.MAX_VALUE;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                        @Nullable Message message,
                                                                        @Nonnull Key.Evaluated arguments) {
            return Collections.singletonList(arguments);
        }

        @Override
        public boolean createsDuplicates() {
            return getArguments().createsDuplicates();
        }

        @Override
        public int getColumnSize() {
            return getArguments().getColumnSize();
        }

        @Nonnull
        @Override
        public Value toValue(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final Type baseType, @Nonnull final List<String> fieldNamePrefix) {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public int planHash(@Nonnull final PlanHashable.PlanHashMode mode) {
            return super.basePlanHash(mode, BASE_HASH);
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return super.baseQueryHash(hashKind, BASE_HASH);
        }
    }
}

