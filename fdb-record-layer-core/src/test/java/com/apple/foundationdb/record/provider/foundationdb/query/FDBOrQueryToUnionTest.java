/*
 * FDBOrQueryToUnionTest.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.PlanHashable.CURRENT_FOR_CONTINUATION;
import static com.apple.foundationdb.record.PlanHashable.CURRENT_LEGACY;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedExactly;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.containsAll;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.orPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.comparisonKey;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.comparisonKeyValues;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.intersectionOnExpressionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.intersectionOnValuesPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unionOnExpressionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unionOnValuesPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedUnionPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValueWithFieldNames;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests related to planning a query with an OR clause into a union plan.
 */
@Tag(Tags.RequiresFDB)
class FDBOrQueryToUnionTest extends FDBRecordStoreQueryTestBase {
    private static Stream<Boolean> booleanArgs() {
        return Stream.of(false, true);
    }

    static class OrQueryParams {
        private final boolean deferFetch;
        private final boolean omitPrimaryKeyInOrderingKey;
        private final boolean sortReverse;
        private final boolean removeDuplicates;
        private final boolean normalizeNestedFields;

        OrQueryParams(boolean deferFetch,
                      boolean omitPrimaryKeyInOrderingKey,
                      boolean sortReverse,
                      boolean removeDuplicates,
                      boolean normalizeNestedFields) {
            this.deferFetch = deferFetch;
            this.omitPrimaryKeyInOrderingKey = omitPrimaryKeyInOrderingKey;
            this.sortReverse = sortReverse;
            this.removeDuplicates = removeDuplicates;
            this.normalizeNestedFields = normalizeNestedFields;
        }

        public boolean shouldDeferFetch() {
            return deferFetch;
        }

        public boolean shouldOmitPrimaryKeyInOrderingKey() {
            return omitPrimaryKeyInOrderingKey;
        }

        public boolean shouldNormalizeNestedFields() {
            return normalizeNestedFields;
        }

        public boolean isRemoveDuplicates() {
            return removeDuplicates;
        }

        public boolean isSortReverse() {
            return sortReverse;
        }

        public RecordQuery.Builder queryBuilder(@Nullable KeyExpression sort) {
            return RecordQuery.newBuilder()
                    .setRemoveDuplicates(removeDuplicates)
                    .setSort(sort, sortReverse);
        }

        public RecordQuery.Builder queryBuilder() {
            return queryBuilder(null);
        }

        public void setPlannerConfiguration(FDBRecordStoreQueryTestBase testBase) {
            testBase.setDeferFetchAfterUnionAndIntersection(deferFetch);
            testBase.setOmitPrimaryKeyInUnionOrderingKey(omitPrimaryKeyInOrderingKey);
            testBase.setNormalizeNestedFields(normalizeNestedFields);
        }

        @Override
        public String toString() {
            return "OrQueryParams{" +
                   "deferFetch=" + deferFetch +
                   ", omitPrimaryKeyInOrderingKey=" + omitPrimaryKeyInOrderingKey +
                   ", sortReverse=" + sortReverse +
                   ", removeDuplicates=" + removeDuplicates +
                    ", normalizeNestedFields=" + normalizeNestedFields +
                   '}';
        }

        @Nonnull
        public OrQueryParams withSortReverse(boolean newSort) {
            if (newSort == sortReverse) {
                return this;
            }
            return new OrQueryParams(deferFetch, omitPrimaryKeyInOrderingKey, newSort, removeDuplicates, normalizeNestedFields);
        }

        @Nonnull
        public OrQueryParams withRemoveDuplicates(boolean newRemoveDuplicates) {
            if (newRemoveDuplicates == removeDuplicates) {
                return this;
            }
            return new OrQueryParams(deferFetch, omitPrimaryKeyInOrderingKey, sortReverse, newRemoveDuplicates, normalizeNestedFields);
        }

        @Nonnull
        public OrQueryParams withDeferFetch(boolean newDeferFetch) {
            if (newDeferFetch == deferFetch) {
                return this;
            }
            return new OrQueryParams(newDeferFetch, omitPrimaryKeyInOrderingKey, sortReverse, removeDuplicates, normalizeNestedFields);
        }

        @Nonnull
        public OrQueryParams withNormalizeNestedFields(boolean newNormalizeNestedFields) {
            if (newNormalizeNestedFields == normalizeNestedFields) {
                return this;
            }
            return new OrQueryParams(deferFetch, omitPrimaryKeyInOrderingKey, sortReverse, removeDuplicates, newNormalizeNestedFields);
        }
    }

    static Stream<OrQueryParams> baseParams() {
        return booleanArgs().flatMap(deferFetch ->
                booleanArgs().map(omitPrimaryKeyInOrderingKey ->
                        new OrQueryParams(deferFetch, omitPrimaryKeyInOrderingKey, false, true, false)));
    }

    static Stream<OrQueryParams> paramsWithAndWithoutRemovesDuplicates() {
        return baseParams().flatMap(params ->
                booleanArgs().map(params::withRemoveDuplicates));
    }

    static Stream<OrQueryParams> reverseParams() {
        return baseParams().map(baseParam -> baseParam.withSortReverse(true));
    }

    static Stream<OrQueryParams> baseForwardAndReverseParams() {
        return baseParams().flatMap(p -> Stream.of(p.withSortReverse(false), p.withSortReverse(true)));
    }

    static Stream<OrQueryParams> nestedFieldTestParams() {
        return baseForwardAndReverseParams().flatMap(p -> Stream.of(p.withNormalizeNestedFields(false), p.withNormalizeNestedFields(true)));
    }

    /**
     * Verify that an OR of compatibly-ordered (up to reversal) indexed fields can be implemented as a union.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testComplexQuery6[{0}]")
    @MethodSource("reverseParams")
    void testComplexQuery6(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planQuery(query);

        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[odd],[odd]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[0],[0]]")))
        ), null);
        assertMatchesExactly(plan, planMatcher);

        if (orQueryParams.shouldDeferFetch() && !(planner instanceof CascadesPlanner)) {
            assertTrue(plan.getQueryPlanChildren().stream().allMatch(QueryPlan::isReverse));
            assertEquals(-1584186103, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-357068519, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (planner instanceof CascadesPlanner) {
            // Cascades does not mark everything as reversed in this case, but validate that all of the children go the same way
            boolean planReverseness = plan.getQueryPlanChildren().get(0).isReverse();
            plan.getQueryPlanChildren().forEach(child ->
                    assertEquals(planReverseness, child.isReverse(), () -> "expected child " + child + " to have same reverseness as first child"));
            assertEquals(725509027, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1507585422, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertTrue(plan.getQueryPlanChildren().stream().allMatch(QueryPlan::isReverse));
            assertEquals(-2067012572, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1784357954, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        Set<Long> seen = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0, "condition on record not met");
                    assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                    seen.add(myrec.getRecNo());
                    i++;
                }
            }
            assertEquals(60, i);
            assertDiscardedAtMost(10, context);
            if (orQueryParams.shouldDeferFetch()) {
                assertLoadRecord(60, context);
            }
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testComplexQuery6Continuations[{0}]")
    @MethodSource("baseParams")
    void testComplexQuery6Continuations(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        orQueryParams.setPlannerConfiguration(this);
        RecordQueryPlan plan = planQuery(query);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int limit = 1; limit <= 5; limit++) {
                clearStoreCounter(context);
                Set<Long> seen = new HashSet<>();
                int i = 0;
                byte[] continuation = null;
                do {
                    try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(limit)
                            .build()).asIterator()) {
                        while (cursor.hasNext()) {
                            FDBQueriedRecord<Message> rec = cursor.next();
                            TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                            myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                            assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0, "condition on record not met");
                            assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                            seen.add(myrec.getRecNo());
                            i++;
                        }
                        continuation = cursor.getContinuation();
                    }
                } while (continuation != null);
                assertEquals(60, i);
                assertDiscardedExactly(10, context);
                if (orQueryParams.shouldDeferFetch()) {
                    assertLoadRecord(60, context);
                }
            }
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on the same field are implemented using a union of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery1[{0}]")
    @MethodSource("baseParams")
    void testOrQuery1(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(4)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[2],[2]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[4],[4]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planQuery(query);
        final KeyExpression comparisonKey = planner instanceof CascadesPlanner ? concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")) : primaryKey("MySimpleRecord");
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[2],[2]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[4],[4]]")))
        ), comparisonKey);
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof CascadesPlanner) {
            assertEquals(-1974121674, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1183017507, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(orQueryParams.shouldDeferFetch() ? 1912003491 : 273143354, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(orQueryParams.shouldDeferFetch() ? -1070595610 : 1002901843, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 2 ||
                               myrec.getNumValue3Indexed() == 4);
                    i++;
                }
            }
            assertEquals(20 + 20 + 20, i);
            assertDiscardedNone(context);
            assertLoadRecord(60, context);
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on the same field are implemented using a union of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryPlanEquals[{0}]")
    @MethodSource("baseParams")
    void testOrQueryPlanEquals(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(4)))
                .build();
        orQueryParams.setPlannerConfiguration(this);
        RecordQueryPlan plan = planQuery(query);

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(4),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(1)))
                .build();
        orQueryParams.setPlannerConfiguration(this);
        RecordQueryPlan plan2 = planQuery(query2);

        // plan is physically different but returns the same result
        assertThat(plan.hashCode(), not(equalTo(plan2.hashCode())));
        assertThat(plan, not(equalTo(plan2)));
        assertTrue(plan.semanticEquals(plan2));
    }

    /**
     * Verify that queries with an OR of a mix of equality and inequality predicates on the same field are implemented
     * using a union of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery2[{0}]")
    @MethodSource("baseParams")
    void testOrQuery2(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Covering(Index(MySimpleRecord$num_value_3_indexed [[2],[2]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Covering(Index(MySimpleRecord$num_value_3_indexed ([3],>) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[2],[2]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>")))
        ), concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")));
        assertMatchesExactly(plan, planMatcher);

        if (orQueryParams.shouldDeferFetch() && !(planner instanceof CascadesPlanner)) {
            assertEquals(504228282, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1520996708, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (planner instanceof CascadesPlanner) {
            assertEquals(-948815552, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-2075944345, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(1299166123, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-700473135, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 2 ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            assertEquals(20 + 20 + 20, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that queries with an OR of non-overlapping range inequalities on the same field are implemented using a union
     * of indexes.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery3[{0}]")
    @MethodSource("baseParams")
    void testOrQuery3(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").lessThan(2),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();

        orQueryParams.setPlannerConfiguration(this);

        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed ([null],[2])) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Covering(Index(MySimpleRecord$num_value_3_indexed ([3],>) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([null],[2])"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>")))
        ), concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")));
        assertMatchesExactly(plan, planMatcher);

        if (orQueryParams.shouldDeferFetch() && !(planner instanceof CascadesPlanner)) {
            assertEquals(-627934247, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(502710007, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (planner instanceof CascadesPlanner) {
            assertEquals(-2080978081, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1200736250, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(-1930405164, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1650830816, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getNumValue3Indexed() < 2 ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            assertEquals(40 + 20, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that queries with an OR of equality predicates on different fields are implemented using a union of indexes,
     * if all fields are indexed.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery4[{0}]")
    @MethodSource("baseParams")
    void testOrQuery4(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[even],[even]]) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[3],[3]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[even],[even]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]")))
        ), primaryKey("MySimpleRecord"));
        assertMatchesExactly(plan, planMatcher);

        if (orQueryParams.shouldDeferFetch() && !(planner instanceof CascadesPlanner)) {
            assertEquals(-417814093, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1082480572, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (planner instanceof CascadesPlanner) {
            assertEquals(1891881268, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(2056249763, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(-673254486, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(991016881, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") ||
                               myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 3);
                    i++;
                }
            }
            assertEquals(50 + 10 + 10, i);
            assertDiscardedAtMost(20, context);
            if (orQueryParams.shouldDeferFetch()) {
                assertLoadRecord(50 + 10 + 10, context);
            }
        }
    }

    /**
     * Verify that an OR of inequalities on different fields uses an unordered union, since there is no compatible ordering.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery5[{0}]")
    @MethodSource("paramsWithAndWithoutRemovesDuplicates")
    void testOrQuery5(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Unordered(Index(MySimpleRecord$str_value_indexed ([null],[m])) ∪ Index(MySimpleRecord$num_value_3_indexed ([3],>))
        RecordQueryPlan plan = planQuery(query);

        // Cascades planner always removes duplicates
        boolean planRemovesDuplicates = orQueryParams.isRemoveDuplicates() || planner instanceof CascadesPlanner;
        if (orQueryParams.shouldDeferFetch() || planner instanceof CascadesPlanner) {
            BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unorderedUnionPlan(
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([null],[m])"))))),
                            coveringIndexPlan()
                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))))));
            if (planRemovesDuplicates) {
                planMatcher =
                        fetchFromPartialRecordPlan(
                                unorderedPrimaryKeyDistinctPlan(planMatcher));
            } else {
                planMatcher =
                        fetchFromPartialRecordPlan(planMatcher);

            }
            assertMatchesExactly(plan, planMatcher);

            assertEquals(planRemovesDuplicates ? 1898767693 : 1898767686, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(planRemovesDuplicates ? -583062018 : 212117636, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unorderedUnionPlan(
                            indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("([null],[m])"))),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],>"))));

            if (planRemovesDuplicates) {
                planMatcher = unorderedPrimaryKeyDistinctPlan(planMatcher);
            }
            assertMatchesExactly(plan, planMatcher);

            assertEquals(planRemovesDuplicates ? -1569447744 : -1569447745, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(planRemovesDuplicates ? 1558364455 : -1941423187, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            Objects.requireNonNull(context.getTimer()).reset();
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().compareTo("m") < 0 ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            if (planRemovesDuplicates) {
                assertEquals(50 + 10, i);
                assertDiscardedAtMost(10, context);
            } else {
                assertEquals(70, i);
                assertDiscardedNone(context);
            }
        }
    }

    @Nonnull
    @SuppressWarnings("unused") // used by reflection
    private static Stream<Arguments> query5WithLimitsArgs() {
        return Stream.of(1, 2, 5, 7).flatMap(i ->
                paramsWithAndWithoutRemovesDuplicates().map(params -> Arguments.of(i, params)));
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery5WithLimits[limit = {0}, {1}]")
    @MethodSource("query5WithLimitsArgs")
    void testOrQuery5WithLimits(int limit, OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);

        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").lessThan("m"),
                        Query.field("num_value_3_indexed").greaterThan(3)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Unordered(Index(MySimpleRecord$str_value_indexed ([null],[m])) ∪ Index(MySimpleRecord$num_value_3_indexed ([3],>))
        RecordQueryPlan plan = planQuery(query);
        boolean planRemovesDuplicates = orQueryParams.isRemoveDuplicates() || planner instanceof CascadesPlanner;

        final BindingMatcher<? extends RecordQueryPlan> planMatcher;
        if (orQueryParams.shouldDeferFetch() || planner instanceof CascadesPlanner) {
            final BindingMatcher<RecordQueryUnorderedUnionPlan> unionPlanBindingMatcher = unorderedUnionPlan(
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")))));

            if (planRemovesDuplicates) {
                planMatcher = fetchFromPartialRecordPlan(unorderedPrimaryKeyDistinctPlan(unionPlanBindingMatcher));
            } else {
                planMatcher = fetchFromPartialRecordPlan(unionPlanBindingMatcher);
            }
        } else {
            final BindingMatcher<RecordQueryUnorderedUnionPlan> unionPlanBindingMatcher = unorderedUnionPlan(
                            indexPlan().where(indexName("MySimpleRecord$str_value_indexed")),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")));
            if (planRemovesDuplicates) {
                planMatcher = unorderedPrimaryKeyDistinctPlan(unionPlanBindingMatcher);
            } else {
                planMatcher = unionPlanBindingMatcher;
            }
        }

        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(planRemovesDuplicates ? 1898767693 : 1898767686, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(planRemovesDuplicates ? -583062018 : 212117636, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(planRemovesDuplicates ? -1569447744 : -1569447745, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(planRemovesDuplicates ? 1558364455 : -1941423187, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(planRemovesDuplicates ? 1898767693 : 1898767686, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(planRemovesDuplicates ? -583062018 : 212117636, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            boolean done = false;
            byte[] continuation = null;
            Set<Tuple> uniqueKeys = new HashSet<>();
            int itr = 0;
            while (!done) {
                ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(limit)
                        .build();
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties).asIterator()) {
                    int i = 0;
                    Set<Tuple> keysThisIteration = new HashSet<>();
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                        assertTrue(myrec.getStrValueIndexed().compareTo("m") < 0 ||
                                   myrec.getNumValue3Indexed() > 3);
                        uniqueKeys.add(rec.getPrimaryKey());
                        if (planRemovesDuplicates) {
                            assertThat(keysThisIteration.add(rec.getPrimaryKey()), is(true));
                        }
                        i++;
                    }
                    continuation = cursor.getContinuation();
                    done = cursor.getNoNextReason().isSourceExhausted();
                    if (!done) {
                        assertEquals(limit, i);
                    }
                }
                itr++;
                assertThat("exceeded maximum iterations", itr, lessThan(500));
            }
            assertEquals(50 + 10, uniqueKeys.size());
        }
    }

    /**
     * Verify that a complex query with an OR of an AND produces a union plan if appropriate indexes are defined.
     * In particular, verify that it can use the last field of an index and does not require primary key ordering
     * compatibility.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery6[{0}]")
    @MethodSource("baseParams")
    void testOrQuery6(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = metaData ->
                metaData.addIndex("MySimpleRecord",
                        new Index("str_value_3_index",
                                "str_value_indexed",
                                "num_value_3_indexed"));
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.and(
                                Query.field("str_value_indexed").equalsValue("even"),
                                Query.field("num_value_3_indexed").greaterThan(3)),
                        Query.field("num_value_3_indexed").lessThan(1)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(str_value_3_index ([even, 3],[even]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Index(MySimpleRecord$num_value_3_indexed ([null],[1]))
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("([even, 3],[even]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([null],[1])")))
        ), concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")));
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof CascadesPlanner) {
            assertEquals(2006798704, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1476903216, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (orQueryParams.shouldDeferFetch()) {
            assertEquals(-835124758, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(778876973, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(1721396731, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1374663850, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue((myrec.getStrValueIndexed().equals("even") &&
                                myrec.getNumValue3Indexed() > 3) ||
                               myrec.getNumValue3Indexed() < 1);
                    i++;
                }
            }
            assertEquals(20 + 10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a complex query with an OR of an AND produces a union plan if the appropriate indexes are defined.
     * Unlike {@link #testOrQuery6(OrQueryParams)}, the legs of the union are not compatibly ordered, so this will revert to using
     * an unordered union.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testUnorderableOrQueryWithAnd[{0}]")
    @MethodSource("paramsWithAndWithoutRemovesDuplicates")
    void testUnorderableOrQueryWithAnd(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", new Index("multi_index_2", "str_value_indexed", "num_value_3_indexed")));
        complexQuerySetup(hook);

        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(
                        Query.and(
                                Query.field("str_value_indexed").equalsValue("even"),
                                Query.or(Query.field("num_value_2").lessThanOrEquals(1), Query.field("num_value_3_indexed").greaterThanOrEquals(3))
                        )
                )
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Unordered(Index(multi_index ([even, null],[even, 1]]) ∪ Index(multi_index_2 [[even, 3],[even]]))
        final RecordQueryPlan plan = planQuery(query);

        // The Cascades planner always removes duplicates and defers fetches, even when not specified on the query
        final boolean planRemovesDuplicates = orQueryParams.isRemoveDuplicates() || (planner instanceof CascadesPlanner);
        final boolean planDefersFetches = orQueryParams.shouldDeferFetch() || (planner instanceof CascadesPlanner);
        List<BindingMatcher<? extends RecordQueryIndexPlan>> indexScanMatchers = List.of(
                indexPlan().where(indexName("multi_index")).and(scanComparisons(range("([even, null],[even, 1]]"))),
                indexPlan().where(indexName("multi_index_2")).and(scanComparisons(range("[[even, 3],[even]]")))
        );
        final List<? extends BindingMatcher<? extends RecordQueryPlan>> childPlanMatchers;
        if (planDefersFetches) {
            childPlanMatchers = indexScanMatchers.stream()
                    .map(indexScanMatcher -> coveringIndexPlan().where(indexPlanOf(indexScanMatcher)))
                    .collect(Collectors.toList());
        } else {
            childPlanMatchers = indexScanMatchers;
        }
        BindingMatcher<? extends RecordQueryPlan> planMatcher = unorderedUnionPlan(childPlanMatchers);

        if (planRemovesDuplicates) {
            planMatcher = unorderedPrimaryKeyDistinctPlan(planMatcher);
        }
        if (planDefersFetches) {
            planMatcher = fetchFromPartialRecordPlan(planMatcher);
        }
        assertMatchesExactly(plan, planMatcher);

        if (planRemovesDuplicates) {
            if (planDefersFetches) {
                assertEquals(-1216499257, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(610131412, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(-173785610, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1543409411, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            if (planDefersFetches) {
                assertEquals(-1216499264, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1405311066, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(-173785611, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-748229757, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") &&
                               (myrec.getNumValue2() <= 1 || myrec.getNumValue3Indexed() >= 3));
                    i++;
                }
            }
            if (planRemovesDuplicates) {
                assertEquals(40, i);
                assertDiscardedAtMost(13, context);
                assertLoadRecord(53, context);
            } else {
                assertEquals(53, i);
                assertDiscardedNone(context);
            }
        }
    }

    /**
     * Verify that an OR with complex limits is implemented as a union, where the comparison key is constructed
     * without repetition out of the index key and primary key (see note).
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuery7[{0}]")
    @MethodSource("baseParams")
    void testOrQuery7(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook(true);
        complexQuerySetup(hook);

        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.or(
                                Query.field("num_value_3_indexed").equalsValue(1),
                                Query.field("num_value_3_indexed").greaterThan(3))))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(str_value_3_index [[even, 1],[even, 1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'num_value_unique' None}] Index(str_value_3_index ([even, 3],[even]])
        RecordQueryPlan plan = planQuery(query);
        final KeyExpression comparisonKey = (orQueryParams.shouldOmitPrimaryKeyInOrderingKey() || planner instanceof CascadesPlanner)
                ? concatenateFields("num_value_3_indexed", "num_value_unique")
                : concatenateFields("str_value_indexed", "num_value_3_indexed", "num_value_unique");
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("[[even, 1],[even, 1]]"))),
                indexPlan().where(indexName("str_value_3_index")).and(scanComparisons(range("([even, 3],[even]]")))
        ), comparisonKey);
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                if (orQueryParams.shouldOmitPrimaryKeyInOrderingKey()) {
                    assertEquals(-60058062, plan.planHash(PlanHashable.CURRENT_LEGACY));
                    assertEquals(-1391842890, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                } else {
                    assertEquals(-664830657, plan.planHash(PlanHashable.CURRENT_LEGACY));
                    assertEquals(1572009327, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                }
            } else {
                if (orQueryParams.shouldOmitPrimaryKeyInOrderingKey()) {
                    assertEquals(-8579725, plan.planHash(PlanHashable.CURRENT_LEGACY));
                    assertEquals(749583583, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                } else {
                    assertEquals(-94975810, plan.planHash(PlanHashable.CURRENT_LEGACY));
                    assertEquals(-581531496, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                }
            }
        } else {
            assertEquals(-3043538, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-142082973, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") &&
                               (myrec.getNumValue3Indexed() == 1) ||
                               myrec.getNumValue3Indexed() > 3);
                    i++;
                }
            }
            assertEquals(10 + 10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that an OR query on the same field with a sort on that field is implemented as a union of index scans,
     * where the union ordering is the field (and not the primary key, as it would normally be for equality predicates).
     * TODO The planner could be smarter here:
     * TODO:(<a href="https://github.com/FoundationDB/fdb-record-layer/issues/13">Add RecordQueryConcatenationPlan for non-overlapping unions</a>)
     * Note that the ordering planner property evaluation now understands that num_value_3_indexed is equality-bound
     * and does not need to partake in the ordering.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryOrdered[{0}]")
    @MethodSource("baseParams")
    void testOrQueryOrdered(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook();
        complexQuerySetup(hook);

        RecordQuery query = orQueryParams.queryBuilder(field("num_value_3_indexed"))
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'str_value_indexed' None}, Field { 'num_value_unique' None}] Index(MySimpleRecord$num_value_3_indexed [[3],[3]])
        RecordQueryPlan plan = planQuery(query);
        BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]")))
        ), concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")));
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(1300798826, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1882806542, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(1412961915, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(258619931, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(2142773918, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(21338461, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    int numValue3 = myrec.getNumValue3Indexed();
                    assertTrue(numValue3 == 1 || numValue3 == 3, "should satisfy value condition");
                    assertTrue(numValue3 == 1 || i >= 20, "lower values should come first");
                    i++;
                }
            }
            assertEquals(20 + 20, i);
            assertDiscardedNone(context);
        }

        query = query.toBuilder()
                .setSort(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")), orQueryParams.isSortReverse())
                .build();
        orQueryParams.setPlannerConfiguration(this);
        plan = planQuery(query);
        planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]")))
        ), concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")));
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(1300798826, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1882991054, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(1412961915, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(258435419, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(2142773918, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(21153949, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }

    /**
     * Verify that the same index can be matched for multiple legs of an OR query, even if there are (compatible) ordering
     * constraints.
     * @throws Exception from test set up
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrderedOrQueryWithMultipleValuesForEarlierColumn[{0}]")
    @MethodSource("baseParams")
    void testOrderedOrQueryWithMultipleValuesForEarlierColumn(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);

        final RecordQuery query = orQueryParams.queryBuilder(field("num_value_3_indexed"))
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.or(
                                Query.field("num_value_2").equalsValue(0),
                                Query.field("num_value_2").equalsValue(2)
                        )
                ))
                .build();

        // Collect all of the expected values into a list of matchers
        final List<Matcher<? super TestRecords1Proto.MySimpleRecord>> expected;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final RecordType recordType = recordStore.getRecordMetaData().getRecordType("MySimpleRecord");
            expected = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .filter(rec -> recordType.equals(rec.getRecordType()))
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .filter(msg -> "odd".equals(msg.getStrValueIndexed()) && (msg.getNumValue2() == 0 || msg.getNumValue2() == 2))
                    .asList()
                    .join()
                    .stream()
                    .map(Matchers::equalTo) // unfortunately, cannot use RecordCursor.map here due to how generics are handled
                    .collect(Collectors.toList());
            commit(context);
        }

        // Index(multi_index [[odd, 0],[odd, 0]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Index(multi_index [[odd, 2],[odd, 2]])
        orQueryParams.setPlannerConfiguration(this);
        RecordQueryPlan plan = planQuery(query);
        final KeyExpression comparisonKey = planner instanceof CascadesPlanner ? concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"), field("num_value_2")) : concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"));
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[odd, 0],[odd, 0]]"))),
                indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[odd, 2],[odd, 2]]")))
        ), comparisonKey);
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(-1754668377, plan.planHash(CURRENT_LEGACY));
                assertEquals(1632504435, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(-1477800426, plan.planHash(CURRENT_LEGACY));
                assertEquals(-521036388, plan.planHash(CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(-2004621044, plan.planHash(CURRENT_LEGACY));
            assertEquals(661700575, plan.planHash(CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final List<TestRecords1Proto.MySimpleRecord> queried = recordStore.executeQuery(plan)
                    .map(FDBRecord::getRecord)
                    .map(msg -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(msg).build())
                    .asList()
                    .join();

            // Validate that the queried records contain all expected values
            assertThat(queried, containsInAnyOrder(expected));

            // Validate sort
            Integer maxNumValue3 = null;
            for (TestRecords1Proto.MySimpleRecord queriedRec : queried) {
                if (maxNumValue3 != null) {
                    assertThat(maxNumValue3, lessThanOrEqualTo(queriedRec.getNumValue3Indexed()));
                }
                maxNumValue3 = queriedRec.getNumValue3Indexed();
            }
        }
    }

    /**
     * Check on "OR" queries on an index with additional columns after the columns required for ordering.
     * For example, if we have an index on fields {@code (a, b, c)}, then we should be able to use it to
     * answer a query of the form:
     *
     * <pre>{@code
     * SELECT * FROM type WHERE a = ?foo OR a = ?bar ORDER BY b
     * }</pre>
     *
     * <p>
     * The thing that makes this case a little tricky is that the planner has to notice that that extra {@code c}
     * field doesn't get in the way. Note that an index on {@code (a, c, b)} would <em>not</em> be okay, because now
     * the {@code c} field is interfering with the order. Likewise, if {@code c} is a repeated field, then we're also
     * in trouble, because results may not show up the correct number of times in the index (or may be missing
     * completely if {@code c} is empty).
     * </p>
     *
     * @throws Exception from underlying test set up
     * @see <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2336">Issue #2336</a>
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrderedOrQueryWithIntermediateUnorderedColumn[{0}]")
    @MethodSource("baseParams")
    void testOrderedOrQueryWithIntermediateUnorderedColumn(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);

        final RecordQuery query = orQueryParams.queryBuilder(field("num_value_2"))
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("str_value_indexed").equalsValue("even")
                ))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(multi_index [[odd],[odd]]) ∪[Field { 'num_value_2' None}, Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Index(multi_index [[even],[even]])
        final RecordQueryPlan plan;
        if (planner instanceof RecordQueryPlanner && !orQueryParams.shouldOmitPrimaryKeyInOrderingKey()) {
            RecordCoreException err = assertThrows(RecordCoreException.class, () -> planQuery(query));
            assertThat(err.getMessage(), Matchers.containsString("Cannot sort without appropriate index"));
            return;
        } else {
            plan = planQuery(query);
        }

        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[odd],[odd]]"))),
                indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[even],[even]]")))
        ), planner instanceof CascadesPlanner ? concat(field("num_value_2"), field("num_value_3_indexed"), primaryKey("MySimpleRecord"), field("str_value_indexed")) : concat(field("num_value_2"), field("num_value_3_indexed"), primaryKey("MySimpleRecord")));
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(1751233613, plan.planHash(CURRENT_LEGACY));
                assertEquals(-846928499, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(-363390528, plan.planHash(CURRENT_LEGACY));
                assertEquals(1294497974, plan.planHash(CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(471565558, plan.planHash(CURRENT_LEGACY));
            assertEquals(1167320211, plan.planHash(CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            Integer numValue2 = null;
            int queriedCount = 0;
            try (RecordCursor<TestRecords1Proto.MySimpleRecord> cursor = recordStore.executeQuery(plan)
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())) {
                for (RecordCursorResult<TestRecords1Proto.MySimpleRecord> result = cursor.getNext(); result.hasNext(); result = cursor.getNext()) {
                    TestRecords1Proto.MySimpleRecord msg = Objects.requireNonNull(result.get());
                    if (numValue2 != null) {
                        assertThat(msg.getNumValue2(), greaterThanOrEqualTo(numValue2));
                    }
                    numValue2 = msg.getNumValue2();
                    assertThat(msg.getStrValueIndexed(), either(equalTo("even")).or(equalTo("odd")));
                    queriedCount++;
                }
            }
            final RecordType type = recordStore.getRecordMetaData().getRecordType("MySimpleRecord");
            int expectedCount = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .filter(rec -> type.equals(rec.getRecordType()))
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .filter(msg -> "even".equals(msg.getStrValueIndexed()) || "odd".equals(msg.getStrValueIndexed()))
                    .getCount()
                    .join();
            assertEquals(expectedCount, queriedCount, "Incorrect number of records returned");
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testNestedPredicates[{0}]")
    @MethodSource("nestedFieldTestParams")
    void testNestedPredicates(OrQueryParams orQueryParams) throws Exception {
        final Index index = new Index("Reviewer$catgory_stats", concat(field("category"), field("stats").nest(concatenateFields("hometown", "start_date")), field("name")));
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("RestaurantReviewer", index);

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context, hook);
            orQueryParams.setPlannerConfiguration(this);
            final RecordQuery query = orQueryParams.queryBuilder(concat(field("stats").nest("start_date"), field("name")))
                    .setRecordType("RestaurantReviewer")
                    .setFilter(Query.and(
                            Query.field("category").equalsValue(0),
                            Query.field("stats").matches(
                                    Query.or(
                                            Query.field("hometown").equalsParameter("town1"),
                                            Query.field("hometown").equalsParameter("town2")
                                    )
                            )
                    ))
                    .setRequiredResults(List.of(field("stats").nest("start_date"), field("stats").nest("hometown"), field("id"), field("name")))
                    .build();
            if (!isUseCascadesPlanner() && !orQueryParams.shouldNormalizeNestedFields()) {
                RecordCoreException rce = assertThrows(RecordCoreException.class, () -> planQuery(query));
                assertThat(rce.getMessage(), containsString("Cannot sort without appropriate index"));
                return;
            }
            final RecordQueryPlan plan = planQuery(query);
            final BindingMatcher<? extends RecordQueryPlan> planMatcher = unionPlanMatcher(orQueryParams,
                        List.of(
                                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS 0, EQUALS $town1]"))),
                                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS 0, EQUALS $town2]")))
                        ),
                        isUseCascadesPlanner() ? concat(field("stats").nest("start_date"), field("name"), field("id"), field("stats").nest("hometown")) : concat(field("stats").nest("start_date"), field("name"), primaryKey("RestaurantReviewer")));
            assertMatchesExactly(plan, planMatcher);
            assertEquals(orQueryParams.isSortReverse(), plan.isReverse());
            if (isUseCascadesPlanner()) {
                assertEquals(orQueryParams.isSortReverse() ? 1539206407 : 1539206374, plan.planHash(CURRENT_LEGACY));
                assertEquals(orQueryParams.isSortReverse() ? 1716842733 : 1722562791, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(orQueryParams.isSortReverse() ? 1766220 : 1766187, plan.planHash(CURRENT_LEGACY));
                if (orQueryParams.shouldDeferFetch()) {
                    assertEquals(orQueryParams.isSortReverse() ? -207163713 : -201443655, plan.planHash(CURRENT_FOR_CONTINUATION));
                } else {
                    assertEquals(orQueryParams.isSortReverse() ? 22976319 : 28696377, plan.planHash(CURRENT_FOR_CONTINUATION));
                }
            }
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testNestedPredicatesWithExtraUnorderedColumns[{0}]")
    @MethodSource("nestedFieldTestParams")
    void testNestedPredicatesWithExtraUnorderedColumns(OrQueryParams orQueryParams) throws Exception {
        final Index index = new Index("Reviewer$catgory_stats", concat(field("category"), field("stats").nest(concatenateFields("hometown", "start_date")), field("name")));
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("RestaurantReviewer", index);

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context, hook);
            orQueryParams.setPlannerConfiguration(this);
            final RecordQuery query = orQueryParams.queryBuilder(field("stats").nest("start_date"))
                    .setRecordType("RestaurantReviewer")
                    .setFilter(Query.and(
                            Query.field("category").equalsValue(0),
                            Query.field("stats").matches(
                                    Query.or(
                                            Query.field("hometown").equalsParameter("town1"),
                                            Query.field("hometown").equalsParameter("town2")
                                    )
                            )
                    ))
                    .setRequiredResults(List.of(field("stats").nest("start_date"), field("stats").nest("hometown"), field("id"), field("name")))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            final BindingMatcher<? extends RecordQueryPlan> planMatcher;
            if (isUseCascadesPlanner() || (orQueryParams.shouldOmitPrimaryKeyInOrderingKey() && orQueryParams.shouldNormalizeNestedFields())) {
                planMatcher = unionPlanMatcher(orQueryParams,
                        List.of(
                                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS 0, EQUALS $town1]"))),
                                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS 0, EQUALS $town2]")))
                        ),
                        isUseCascadesPlanner() ? concat(field("stats").nest("start_date"), field("name"), field("id"), field("stats.hometown")) : concat(field("stats").nest("start_date"), field("name"), primaryKey("RestaurantReviewer")));
            } else if (orQueryParams.shouldNormalizeNestedFields()) {
                planMatcher = filterPlan(
                        indexPlan()
                                .where(indexName("stats$school"))
                                .and(scanComparisons(unbounded()))
                ).where(queryComponents(containsAll(Set.of(
                        Query.or(
                                Query.and(Query.field("category").equalsValue(0), Query.field("stats").matches(Query.field("hometown").equalsParameter("town1"))),
                                Query.and(Query.field("category").equalsValue(0), Query.field("stats").matches(Query.field("hometown").equalsParameter("town2")))
                        )
                ))));
            } else {
                planMatcher = filterPlan(
                        indexPlan()
                                .where(indexName("stats$school"))
                                .and(scanComparisons(unbounded()))
                ).where(queryComponents(containsAll(Set.of(
                        Query.field("category").equalsValue(0),
                        Query.field("stats").matches(
                                Query.or(Query.field("hometown").equalsParameter("town1"), Query.field("hometown").equalsParameter("town2"))
                        )
                ))));
            }
            assertMatchesExactly(plan, planMatcher);
            assertEquals(orQueryParams.isSortReverse(), plan.isReverse());
            if (isUseCascadesPlanner()) {
                assertEquals(orQueryParams.isSortReverse() ? 1539206407 : 1539206374, plan.planHash(CURRENT_LEGACY));
                assertEquals(orQueryParams.isSortReverse() ? 1716842733 : 1722562791, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                if (orQueryParams.shouldNormalizeNestedFields()) {
                    if (orQueryParams.shouldOmitPrimaryKeyInOrderingKey()) {
                        assertEquals(orQueryParams.isSortReverse() ? 1766220 : 1766187, plan.planHash(CURRENT_LEGACY));
                        if (orQueryParams.shouldDeferFetch()) {
                            assertEquals(orQueryParams.isSortReverse() ? -207163713 : -201443655, plan.planHash(CURRENT_FOR_CONTINUATION));
                        } else {
                            assertEquals(orQueryParams.isSortReverse() ? 22976319 : 28696377, plan.planHash(CURRENT_FOR_CONTINUATION));
                        }
                    } else {
                        assertEquals(orQueryParams.isSortReverse() ? -2116497242 : -2116497243, plan.planHash(CURRENT_LEGACY));
                        if (orQueryParams.shouldDeferFetch()) {
                            assertEquals(orQueryParams.isSortReverse() ? -1600469760 : -1600463994, plan.planHash(CURRENT_FOR_CONTINUATION));
                        } else {
                            assertEquals(orQueryParams.isSortReverse() ? -1600469760 : -1600463994, plan.planHash(CURRENT_FOR_CONTINUATION));
                        }
                    }
                } else {
                    assertEquals(orQueryParams.isSortReverse() ? 410359689 : 410359688, plan.planHash(CURRENT_LEGACY));
                    assertEquals(orQueryParams.isSortReverse() ? -1407920643 : -1407914877, plan.planHash(CURRENT_FOR_CONTINUATION));
                }
            }
        }
    }

    /**
     * Test that an index with extra columns is rejected if one of those columns is repeated. This test case is
     * similar to {@link #testOrderedOrQueryWithIntermediateUnorderedColumn}, but the index should be rejected because
     * one of the unmatched columns is repeated. This has to be rejected because results can be missing (if the
     * repeated field is empty).
     *
     * @throws Exception from test set up
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrderedUnionDoesNotUseIndexWithExtraRepeatedColumn[{0}]")
    @MethodSource("baseParams")
    void testOrderedUnionDoesNotUseIndexWithExtraRepeatedColumn(OrQueryParams orQueryParams) throws Exception {
        final Index index = new Index("str_3_repeater", concat(field("str_value_indexed"), field("num_value_3_indexed"), field("repeater", KeyExpression.FanType.FanOut)));
        RecordMetaDataHook hook = complexQuerySetupHook()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));
        complexQuerySetup(hook);

        final RecordQuery query = orQueryParams.queryBuilder(field("num_value_3_indexed"))
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("str_value_indexed").equalsValue("odd")
                ))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(MySimpleRecord$num_value_3_indexed <,>) | Or([str_value_indexed EQUALS even, str_value_indexed EQUALS odd])
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> indexPlanMatcher = indexPlan()
                .where(indexName("MySimpleRecord$num_value_3_indexed"))
                .and(scanComparisons(unbounded()));
        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher = filterPlan(indexPlanMatcher)
                    .where(queryComponents(only(PrimitiveMatchers.equalsObject(query.getFilter()))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1539136105, plan.planHash(CURRENT_LEGACY));
            assertEquals(-1993337462, plan.planHash(CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher = predicatesFilterPlan(indexPlanMatcher).where(predicates(
                    only(QueryPredicateMatchers.orPredicate(exactly(
                            valuePredicate(ValueMatchers.fieldValueWithFieldNames("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "even")),
                            valuePredicate(ValueMatchers.fieldValueWithFieldNames("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "odd"))
                    )))
            ));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-732740118, plan.planHash(CURRENT_LEGACY));
            assertEquals(-559232537, plan.planHash(CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            int queriedCount = 0;
            try (RecordCursor<TestRecords1Proto.MySimpleRecord> cursor = recordStore.executeQuery(plan)
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())) {
                Integer maxNumValue3 = null;
                for (RecordCursorResult<TestRecords1Proto.MySimpleRecord> result = cursor.getNext(); result.hasNext(); result = cursor.getNext()) {
                    TestRecords1Proto.MySimpleRecord msg = Objects.requireNonNull(result.get());
                    assertThat(msg.getStrValueIndexed(), either(equalTo("even")).or(equalTo("odd")));
                    if (maxNumValue3 != null) {
                        assertThat("Results should be sorted by num_value_3_indexed", msg.getNumValue3Indexed(), greaterThanOrEqualTo(maxNumValue3));
                    }
                    maxNumValue3 = msg.getNumValue3Indexed();
                    queriedCount++;
                }
            }

            final RecordType type = recordStore.getRecordMetaData().getRecordType("MySimpleRecord");
            int expectedCount = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .filter(rec -> type.equals(rec.getRecordType()))
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .filter(msg -> "even".equals(msg.getStrValueIndexed()) || "odd".equals(msg.getStrValueIndexed()))
                    .getCount()
                    .join();
            assertEquals(expectedCount, queriedCount, "Incorrect number of records returned");
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testOrderedUnionHasRepeatedColumnsInPrefixAndOrdering[{0}]")
    @MethodSource("baseForwardAndReverseParams")
    void testOrderedUnionHasRepeatedColumnsInPrefixAndOrdering(OrQueryParams orQueryParams) throws Exception {
        final Index index = new Index("index_with_two_num_value_2s", concatenateFields("num_value_2", "str_value_indexed", "num_value_3_indexed", "num_value_2"));
        RecordMetaDataHook hook = complexQuerySetupHook()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));
        complexQuerySetup(hook);

        final String value2Param = "value2";
        final String strValue1Param = "strParam1";
        final String strValue2Param = "strParam2";
        final RecordQuery query = orQueryParams.queryBuilder(field("num_value_3_indexed"))
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsParameter(value2Param),
                        Query.or(
                                Query.field("str_value_indexed").equalsParameter(strValue1Param),
                                Query.field("str_value_indexed").equalsParameter(strValue2Param)
                        )
                ))
                .setRequiredResults(List.of(field("str_value_indexed"), field("num_value_3_indexed")))
                .setAllowedIndex(index.getName())
                .build();
        orQueryParams.setPlannerConfiguration(this);

        final RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = unionPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS $" + value2Param + ", EQUALS $" + strValue1Param + "]"))),
                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS $" + value2Param + ", EQUALS $" + strValue2Param + "]")))
        ), isUseCascadesPlanner() ? concatenateFields("num_value_3_indexed", "rec_no", "str_value_indexed") : concatenateFields("num_value_3_indexed", "rec_no"));
        assertMatchesExactly(plan, planMatcher);
        assertEquals(orQueryParams.isSortReverse(), plan.isReverse());
        if (isUseCascadesPlanner()) {
            assertEquals(orQueryParams.isSortReverse() ? 973132071L : 973132038L, plan.planHash(CURRENT_LEGACY));
            assertEquals(orQueryParams.isSortReverse() ? 1977118957L : 1982839015L, plan.planHash(CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(orQueryParams.isSortReverse() ? -2058994186L : -2058994219L, plan.planHash(CURRENT_LEGACY));
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(orQueryParams.isSortReverse() ? -1347044477L : -1341324419L, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(orQueryParams.isSortReverse() ? -1116904445L : -1111184387L, plan.planHash(CURRENT_FOR_CONTINUATION));
            }
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Execute the query. Validate the sort order is followed
            @Nullable Integer numValue3 = null;
            final Bindings bindings = Bindings.newBuilder()
                    .set(value2Param, 1)
                    .set(strValue1Param, "even")
                    .set(strValue2Param, "odd")
                    .build();
            final Set<Long> ids = new HashSet<>();
            try (RecordCursorIterator<FDBQueriedRecord<Message>> iterator = executeQuery(plan, bindings)) {
                while (iterator.hasNext()) {
                    final FDBQueriedRecord<Message> queriedRecord = iterator.next();
                    TestRecords1Proto.MySimpleRecord simpleRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                            .mergeFrom(queriedRecord.getRecord())
                            .build();
                    assertEquals(1, simpleRecord.getNumValue2());
                    assertThat(simpleRecord.getStrValueIndexed(), either(equalTo("even")).or(equalTo("odd")));
                    int queriedNumValue3 = simpleRecord.getNumValue3Indexed();
                    if (numValue3 != null) {
                        assertThat(queriedNumValue3, orQueryParams.isSortReverse() ? lessThanOrEqualTo(numValue3) : greaterThanOrEqualTo(numValue3));
                    }
                    numValue3 = queriedNumValue3;
                    assertTrue(ids.add(simpleRecord.getRecNo()), () -> "set should not have already contained ID " + simpleRecord.getRecNo());
                }
            }

            // Make sure the correct IDs are collected
            assertThat(ids, not(empty()));
            final RecordType type = recordStore.getRecordMetaData().getRecordType("MySimpleRecord");
            final Set<Long> expectedIds = new HashSet<>();
            recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .filter(rec -> type.equals(rec.getRecordType()))
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .filter(msg -> msg.getNumValue2() == 1 && ("even".equals(msg.getStrValueIndexed()) || "odd".equals(msg.getStrValueIndexed())))
                    .map(TestRecords1Proto.MySimpleRecord::getRecNo)
                    .forEach(expectedIds::add)
                    .join();
            assertEquals(expectedIds, ids);
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testOrderedUnionHasRepeatedColumnsNotInPrefix[{0}]")
    @MethodSource("baseForwardAndReverseParams")
    void testOrderedUnionHasRepeatedColumnsNotInPrefix(OrQueryParams orQueryParams) throws Exception {
        final Index index = new Index("index_with_two_num_value_2s", concatenateFields("str_value_indexed", "num_value_2", "num_value_3_indexed", "num_value_2"));
        RecordMetaDataHook hook = complexQuerySetupHook()
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));
        complexQuerySetup(hook);

        final String strValue1Param = "strParam1";
        final String strValue2Param = "strParam2";
        final RecordQuery query = orQueryParams.queryBuilder(field("num_value_2"))
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsParameter(strValue1Param),
                        Query.field("str_value_indexed").equalsParameter(strValue2Param))
                )
                .setRequiredResults(List.of(field("num_value_2"), field("num_value_3_indexed")))
                .setAllowedIndex(index.getName())
                .build();
        orQueryParams.setPlannerConfiguration(this);

        if (!isUseCascadesPlanner() && !orQueryParams.shouldOmitPrimaryKeyInOrderingKey()) {
            RecordCoreException rce = assertThrows(RecordCoreException.class, () -> planQuery(query));
            assertThat(rce.getMessage(), containsString("Cannot sort without appropriate index"));
            return;
        }
        final RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = unionPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS $" + strValue1Param + "]"))),
                indexPlan().where(indexName(index.getName())).and(scanComparisons(range("[EQUALS $" + strValue2Param + "]")))
        ), isUseCascadesPlanner() ? concatenateFields("num_value_2", "num_value_3_indexed", "rec_no", "str_value_indexed") : concatenateFields("num_value_2", "num_value_3_indexed", "rec_no"));
        assertMatchesExactly(plan, planMatcher);
        assertEquals(orQueryParams.isSortReverse(), plan.isReverse());
        if (isUseCascadesPlanner()) {
            assertEquals(orQueryParams.isSortReverse() ? 1380805606 : 1380805573, plan.planHash(CURRENT_LEGACY));
            assertEquals(orQueryParams.isSortReverse() ? -1854527892 : -1848807834, plan.planHash(CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(orQueryParams.isSortReverse() ? 336481815 : 336481782, plan.planHash(CURRENT_LEGACY));
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(orQueryParams.isSortReverse() ? 426190694 : 431910752, plan.planHash(CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(orQueryParams.isSortReverse() ? 656330726 : 662050784, plan.planHash(CURRENT_FOR_CONTINUATION));
            }
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Execute the query. Validate the sort order is followed
            @Nullable Integer numValue2 = null;
            final Bindings bindings = Bindings.newBuilder()
                    .set(strValue1Param, "even")
                    .set(strValue2Param, "odd")
                    .build();
            final Set<Long> ids = new HashSet<>();
            try (RecordCursorIterator<FDBQueriedRecord<Message>> iterator = executeQuery(plan, bindings)) {
                while (iterator.hasNext()) {
                    final FDBQueriedRecord<Message> queriedRecord = iterator.next();
                    TestRecords1Proto.MySimpleRecord simpleRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                            .mergeFrom(queriedRecord.getRecord())
                            .build();
                    assertThat(simpleRecord.getStrValueIndexed(), either(equalTo("even")).or(equalTo("odd")));
                    int queriedNumValue2 = simpleRecord.getNumValue2();
                    if (numValue2 != null) {
                        assertThat(queriedNumValue2, orQueryParams.isSortReverse() ? lessThanOrEqualTo(numValue2) : greaterThanOrEqualTo(numValue2));
                    }
                    numValue2 = queriedNumValue2;
                    assertTrue(ids.add(simpleRecord.getRecNo()), () -> "set should not have already contained ID " + simpleRecord.getRecNo());
                }
            }

            // Make sure the correct IDs are collected
            assertThat(ids, not(empty()));
            final RecordType type = recordStore.getRecordMetaData().getRecordType("MySimpleRecord");
            final Set<Long> expectedIds = new HashSet<>();
            recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .filter(rec -> type.equals(rec.getRecordType()))
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .filter(msg -> "even".equals(msg.getStrValueIndexed()) || "odd".equals(msg.getStrValueIndexed()))
                    .map(TestRecords1Proto.MySimpleRecord::getRecNo)
                    .forEach(expectedIds::add)
                    .join();
            assertEquals(expectedIds, ids);
        }
    }

    @SuppressWarnings("unused") // used as parameter source for parameterized test
    static Stream<Arguments> orderByIncludingValuePortion() {
        final List<KeyExpression> sortKeys = List.of(
                field("num_value_2"),
                concatenateFields("num_value_2", "rec_no")
        );
        return baseForwardAndReverseParams().flatMap(orQueryParams ->
                sortKeys.stream().map(sortKey ->
                        Arguments.of(orQueryParams, sortKey)));
    }

    @DualPlannerTest
    @ParameterizedTest(name = "orderByIncludingValuePortion[orQueryParams={0}, sortKey={1}]")
    @MethodSource
    void orderByIncludingValuePortion(@Nonnull OrQueryParams orQueryParams, @Nonnull KeyExpression sortKey) throws Exception {
        final Index keyWithValueIndex = new Index("simple$keyWithValue", keyWithValue(
                concatenateFields("num_value_3_indexed", "num_value_2", "num_value_unique"), 2));
        final RecordMetaDataHook hook = metaData -> metaData.addIndex("MySimpleRecord", keyWithValueIndex);
        complexQuerySetup(hook);
        boolean sortKeyContainsPrimaryKey = sortKey.getColumnSize() == 2;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final String param1 = "p1";
            final String param2 = "p2";
            final RecordQuery query = orQueryParams.queryBuilder(sortKey)
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.or(Query.field("num_value_3_indexed").equalsParameter(param1), Query.field("num_value_3_indexed").equalsParameter(param2)))
                    .setRequiredResults(List.of(field("num_value_unique"), field("num_value_3_indexed")))
                    .build();
            orQueryParams.setPlannerConfiguration(this);
            final RecordQueryPlan plan = planQuery(query);
            final KeyExpression expectedComparisonKey;
            if (isUseCascadesPlanner()) {
                if (sortKeyContainsPrimaryKey) {
                    expectedComparisonKey = concatenateFields("num_value_2", "rec_no", "num_value_3_indexed");
                } else {
                    expectedComparisonKey = concatenateFields("num_value_2", "rec_no", "num_value_3_indexed");
                }
            } else {
                expectedComparisonKey = concatenateFields("num_value_2", "rec_no");
            }
            final BindingMatcher<? extends RecordQueryPlan> planMatcher = unionPlanMatcher(orQueryParams, List.of(
                    indexPlan().where(indexName(keyWithValueIndex.getName())).and(scanComparisons(range("[EQUALS $" + param1 + "]"))),
                    indexPlan().where(indexName(keyWithValueIndex.getName())).and(scanComparisons(range("[EQUALS $" + param2 + "]")))
            ), expectedComparisonKey);
            assertMatchesExactly(plan, planMatcher);
            assertEquals(orQueryParams.isSortReverse(), plan.isReverse());

            final int legacyHash = plan.planHash(CURRENT_LEGACY);
            final int continuationHash = plan.planHash(CURRENT_FOR_CONTINUATION);
            if (isUseCascadesPlanner()) {
                if (sortKeyContainsPrimaryKey) {
                    assertEquals(orQueryParams.isSortReverse() ? 951372169 : 951372136, legacyHash);
                    assertEquals(orQueryParams.isSortReverse() ? 477401359 : 483121417, continuationHash);
                } else {
                    assertEquals(orQueryParams.isSortReverse() ? 951372169 : 951372136, legacyHash);
                    assertEquals(orQueryParams.isSortReverse() ? 477401359 : 483121417, continuationHash);
                }
            } else {
                if (orQueryParams.shouldDeferFetch()) {
                    assertEquals(orQueryParams.isSortReverse() ? 442484585L : 442484552L, legacyHash);
                    // The planHash can differ here because the choice of comparison key can result in the index plan
                    // either being "strictly sorted" or not
                    if (sortKeyContainsPrimaryKey) {
                        assertEquals(orQueryParams.isSortReverse() ? -1935987792L : -1930267734L, continuationHash);
                    } else {
                        assertEquals(orQueryParams.isSortReverse() ? -1935803280L : -1930083222L, continuationHash);
                    }
                } else {
                    assertEquals(orQueryParams.isSortReverse() ? 442484585L : 442484552L, legacyHash);
                    if (sortKeyContainsPrimaryKey) {
                        assertEquals(orQueryParams.isSortReverse() ? -1705847760L : -1700127702L, continuationHash);
                    } else {
                        assertEquals(orQueryParams.isSortReverse() ? -1705663248L : -1699943190L, continuationHash);
                    }
                }
            }

            @Nullable Tuple lastSortValue = null;
            final Bindings bindings = Bindings.newBuilder()
                    .set(param1, 0)
                    .set(param2, 2)
                    .build();
            try (RecordCursorIterator<FDBQueriedRecord<Message>> iter = executeQuery(plan, bindings)) {
                while (iter.hasNext()) {
                    TestRecords1Proto.MySimpleRecord simpleRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                            .mergeFrom(iter.next().getRecord())
                            .build();
                    assertThat(simpleRecord.getNumValue3Indexed(), either(equalTo(0)).or(equalTo(2)));
                    Tuple sortValue = sortKeyContainsPrimaryKey ? Tuple.from(simpleRecord.getNumValue2(), simpleRecord.getRecNo()) : Tuple.from(simpleRecord.getNumValue2());
                    if (lastSortValue != null) {
                        if (orQueryParams.isSortReverse()) {
                            assertThat(lastSortValue.compareTo(sortValue), greaterThanOrEqualTo(0));
                        } else {
                            assertThat(lastSortValue.compareTo(sortValue), lessThanOrEqualTo(0));
                        }
                    }
                    lastSortValue = sortValue;
                }
            }
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryChildReordering[{0}]")
    @MethodSource("baseParams")
    void testOrQueryChildReordering(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .setSort(null, true)
                .setRemoveDuplicates(true)
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan1 = planQuery(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(0),
                        Query.field("str_value_indexed").equalsValue("odd")))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) ∪ Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]))
        RecordQueryPlan plan2 = planQuery(query2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertNotEquals(plan1, plan2);
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());
        assertTrue(plan1.semanticEquals(plan2));
        if (orQueryParams.shouldDeferFetch() && !(planner instanceof CascadesPlanner)) {
            assertEquals(-1584186103, plan1.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-357068519, plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            assertEquals(-91575587, plan2.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1919956247, plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (planner instanceof CascadesPlanner) {
            assertEquals(725509027, plan1.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1507585422, plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            assertEquals(-2076847753, plan2.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1224494146, plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(-2067012572, plan1.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1784357954, plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            assertEquals(600484528, plan2.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(221470226, plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
        
        Set<Long> seen = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor1 = executeQuery(plan1);
                     RecordCursorIterator<FDBQueriedRecord<Message>> cursor2 = executeQuery(plan2)) {
                while (cursor1.hasNext()) {
                    assertThat(cursor2.hasNext(), is(true));
                    FDBQueriedRecord<Message> rec1 = cursor1.next();
                    FDBQueriedRecord<Message> rec2 = cursor2.next();
                    assertEquals(Objects.requireNonNull(rec1).getRecord(), Objects.requireNonNull(rec2).getRecord());
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec1.getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0, "condition on record not met");
                    assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                    seen.add(myrec.getRecNo());
                    i++;
                }
                assertThat(cursor2.hasNext(), is(false));
            }
            assertEquals(60, i);
            assertDiscardedAtMost(20, context);
            if (orQueryParams.shouldDeferFetch()) {
                assertLoadRecord(120, context);
            }
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryChildReordering2[{0}]")
    @MethodSource("reverseParams")
    void testOrQueryChildReordering2(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query1 = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan1 = planQuery(query1);
        RecordQuery query2 = query1.toBuilder()
                .setFilter(Query.or(Lists.reverse(Objects.requireNonNull((OrComponent)query1.getFilter()).getChildren())))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE) ∪ Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) ∪ Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE)
        // Fetch(Covering(Index(MySimpleRecord$num_value_3_indexed [[3],[3]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]] REVERSE) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]) ∪ Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]] REVERSE) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]))
        RecordQueryPlan plan2 = planQuery(query2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertNotEquals(plan1, plan2);
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());
        assertTrue(plan1.semanticEquals(plan2));
        
        if (orQueryParams.shouldDeferFetch() && !(planner instanceof CascadesPlanner)) {
            assertEquals(770691035, plan1.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1890796442, plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            assertEquals(1289607451, plan2.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-29394342, plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (planner instanceof CascadesPlanner) {
            assertEquals(-1214587858, plan1.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(912054445, plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            assertEquals(-695671442, plan2.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1008136339, plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(723665474, plan1.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-330673401, plan1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            assertEquals(184229634, plan2.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(2044103111, plan2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        Set<Long> seen = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor1 = executeQuery(plan1);
                    RecordCursorIterator<FDBQueriedRecord<Message>> cursor2 = executeQuery(plan2)) {
                while (cursor1.hasNext()) {
                    assertThat(cursor2.hasNext(), is(true));
                    FDBQueriedRecord<Message> rec1 = cursor1.next();
                    FDBQueriedRecord<Message> rec2 = cursor2.next();
                    assertEquals(Objects.requireNonNull(rec1).getRecord(), Objects.requireNonNull(rec2).getRecord());
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec1.getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("odd") || myrec.getNumValue3Indexed() == 0 || myrec.getNumValue3Indexed() == 3, "condition on record not met");
                    assertFalse(seen.contains(myrec.getRecNo()), "Already saw a record!");
                    seen.add(myrec.getRecNo());
                    i++;
                }
                assertThat(cursor2.hasNext(), is(false));
            }
            assertEquals(70, i);
            assertDiscardedAtMost(40, context);
            if (orQueryParams.shouldDeferFetch()) {
                assertLoadRecord(140, context);
            }
        }
    }

    /**
     * Verify that a query with an OR of an AND can be implemented as a union of an index scan with an intersection of index scans.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrderedOrQueryWithAnd[{0}]")
    @MethodSource("baseParams")
    void testOrderedOrQueryWithAnd(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", "str_2", concat(field("str_value_indexed"), field("num_value_2")));
            metaData.addIndex("MySimpleRecord", "nu_2", concat(field("num_value_unique"), field("num_value_2")));
            metaData.addIndex("MySimpleRecord", "n3_2", concat(field("num_value_3_indexed"), field("num_value_2")));
        };
        complexQuerySetup(hook);

        RecordQuery query = orQueryParams.queryBuilder(field("num_value_2"))
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.and(
                                Query.field("num_value_3_indexed").equalsValue(1),
                                Query.field("num_value_unique").equalsValue(909))))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(str_2 [[even],[even]]) ∪[Field { 'num_value_2' None}, Field { 'rec_no' None}] Index(nu_2 [[909],[909]]) ∩ Index(n3_2 [[1],[1]])
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        unionOnExpressionPlan(
                                indexPlan().where(indexName("str_2")).and(scanComparisons(range("[[even],[even]]"))),
                                fetchFromPartialRecordPlan(
                                        intersectionOnExpressionPlan(
                                                coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("nu_2")).and(scanComparisons(range("[[909],[909]]"))))),
                                                coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("n3_2")).and(scanComparisons(range("[[1],[1]]"))))))
                                ))
                                .where(comparisonKey(concat(field("num_value_2"), primaryKey("MySimpleRecord"))));
                assertMatchesExactly(plan, planMatcher);

                assertEquals(-31022114, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1965726789, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        unionOnExpressionPlan(
                                indexPlan().where(indexName("str_2")).and(scanComparisons(range("[[even],[even]]"))),
                                intersectionOnExpressionPlan(
                                        indexPlan().where(indexName("nu_2")).and(scanComparisons(range("[[909],[909]]"))),
                                        indexPlan().where(indexName("n3_2")).and(scanComparisons(range("[[1],[1]]")))))
                                .where(comparisonKey(concat(field("num_value_2"), primaryKey("MySimpleRecord"))));
                assertMatchesExactly(plan, planMatcher);

                assertEquals(-1659601413, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1344221020, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            RecordQueryPlanMatchers.unionOnValuesPlan(
                                            coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("str_2")).and(scanComparisons(range("[[even],[even]]"))))),
                                            intersectionOnValuesPlan(
                                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("nu_2")).and(scanComparisons(range("[[909],[909]]"))))),
                                                    coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("n3_2")).and(scanComparisons(range("[[1],[1]]")))))))
                                    .where(comparisonKeyValues(exactly(fieldValueWithFieldNames("num_value_2"), fieldValueWithFieldNames("rec_no")))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(1677471422, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(164376971, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertTrue(myrec.getStrValueIndexed().equals("even") || (myrec.getNumValue3Indexed() == 1 && myrec.getNumValueUnique() == 909));
                    i++;
                }
            }
            assertEquals(51, i);
        }
    }

    /**
     * Verify that an OR on two indexed fields with compatibly ordered indexes is implemented by a union, and that the
     * union cursors works properly with a returned record limit.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testComplexLimits5[{0}]")
    @MethodSource("baseParams")
    void testComplexLimits5(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("odd"),
                        Query.field("num_value_3_indexed").equalsValue(0)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Fetch(Covering(Index(MySimpleRecord$str_value_indexed [[odd],[odd]]) -> [rec_no: KEY[1], str_value_indexed: KEY[0]]) ∪ Covering(Index(MySimpleRecord$num_value_3_indexed [[0],[0]]) -> [num_value_3_indexed: KEY[0], rec_no: KEY[1]]))
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("[[odd],[odd]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[0],[0]]")))
        ), primaryKey("MySimpleRecord"));
        assertMatchesExactly(plan, planMatcher);

        if (orQueryParams.shouldDeferFetch() && !(planner instanceof CascadesPlanner)) {
            assertEquals(-1584186334, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-351348461, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else if (planner instanceof CascadesPlanner) {
            assertEquals(725509027, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1507585422, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertEquals(-2067012605, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1790078012, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(5).build()).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());

                    if (myrec.getNumValue3Indexed() != 0) {
                        assertEquals("odd", myrec.getStrValueIndexed());
                    }
                    i += 1;
                }
            }
            assertEquals(5, i);
            assertDiscardedAtMost(1, context);
            if (orQueryParams.shouldDeferFetch()) {
                assertLoadRecord(5, context);
            }
        }
    }

    /**
     * Verify that boolean normalization of a complex AND/OR expression produces simple plans.
     * In particular, verify that an AND of OR still uses a union of index scans (an OR of AND).
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryDenorm[{0}]")
    @MethodSource("baseParams")
    void testOrQueryDenorm(OrQueryParams orQueryParams) throws Exception {
        // new Index("multi_index", "str_value_indexed", "num_value_2", "num_value_3_indexed")
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_2").equalsValue(0),
                        Query.or(
                                Query.field("num_value_3_indexed").equalsValue(0),
                                Query.and(
                                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                                        Query.field("num_value_3_indexed").lessThanOrEquals(3)))))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(multi_index [[even, 0, 0],[even, 0, 0]]) ∪[Field { 'num_value_3_indexed' None}, Field { 'rec_no' None}] Index(multi_index [[even, 0, 2],[even, 0, 3]])
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[even, 0, 0],[even, 0, 0]]"))),
                indexPlan().where(indexName("multi_index")).and(scanComparisons(range("[[even, 0, 2],[even, 0, 3]]")))
        ), concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")));
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(-1633556172, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1006639371, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(-2074065439, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1146901452, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(1208367290, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1704665614, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertEquals(0, (myrec.getNumValue2() % 3));
                    assertThat(myrec.getNumValue3Indexed() % 5, anyOf(is(0), allOf(greaterThanOrEqualTo(2), lessThanOrEqualTo(3))));
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that OR plans work properly when executed with continuations, even when the continuation splits differ
     * in how they exhaust the union sources.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQuerySplitContinuations[{0}]")
    @MethodSource("baseParams")
    void testOrQuerySplitContinuations(OrQueryParams orQueryParams) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            for (int i = 0; i < 100; i++) {
                recBuilder.setRecNo(i);
                recBuilder.setNumValue3Indexed(i / 10);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        // Each substream completes before the next one starts.
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("num_value_3_indexed").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(5)))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(MySimpleRecord$num_value_3_indexed [[1],[1]]) ∪ Index(MySimpleRecord$num_value_3_indexed [[3],[3]]) ∪ Index(MySimpleRecord$num_value_3_indexed [[5],[5]])
        RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[1],[1]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[3],[3]]"))),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("[[5],[5]]")))
        ), planner instanceof CascadesPlanner ? concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord")) : primaryKey("MySimpleRecord"));
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(1912003715, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-154462778, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(273143386, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1919034675, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(-1974121450, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(2099150339, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int limit = 1; limit <= 5; limit++) {
                int i = 0;
                byte[] continuation = null;
                do {
                    try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(limit)
                            .build()).asIterator()) {
                        while (cursor.hasNext()) {
                            FDBQueriedRecord<Message> rec = cursor.next();
                            TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                            myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                            assertEquals((i / 10) * 2 + 1, myrec.getNumValue3Indexed());
                            i++;
                        }
                        continuation = cursor.getContinuation();
                    }
                } while (continuation != null);
                assertEquals(30, i);
            }
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that queries with an OR of predicates with a common scan and different filters does not bother with a Union.
     */
    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryNoIndex[{0}]")
    @MethodSource("baseParams")
    void testOrQueryNoIndex(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = metadata -> metadata.removeIndex("MySimpleRecord$num_value_3_indexed");
        complexQuerySetup(hook);
        QueryComponent orComponent = Query.or(
                Query.field("num_value_3_indexed").equalsValue(1),
                Query.field("num_value_3_indexed").equalsValue(2),
                Query.field("num_value_3_indexed").equalsValue(4));
        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("str_value_indexed").equalsValue("even"), orComponent))
                .build();
        orQueryParams.setPlannerConfiguration(this);

        // Index(MySimpleRecord$str_value_indexed [[even],[even]]) | Or([num_value_3_indexed EQUALS 1, num_value_3_indexed EQUALS 2, num_value_3_indexed EQUALS 4])
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    filterPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$str_value_indexed"))
                                    .and(scanComparisons(range("[[even],[even]]"))))
                            .where(queryComponents(exactly(equalsObject(orComponent))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1553701984, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1108620348, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    predicatesFilterPlan(
                            indexPlan()
                                    .where(indexName("MySimpleRecord$str_value_indexed"))
                                    .and(scanComparisons(range("[[even],[even]]"))))
                            .where(predicates(only(orPredicate(
                                    exactly(valuePredicate(ValueMatchers.fieldValueWithFieldNames("num_value_3_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1)),
                                            valuePredicate(ValueMatchers.fieldValueWithFieldNames("num_value_3_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 2)),
                                            valuePredicate(ValueMatchers.fieldValueWithFieldNames("num_value_3_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 4)))))));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-2131629164, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-715191475, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertTrue(myrec.getNumValue3Indexed() == 1 ||
                               myrec.getNumValue3Indexed() == 2 ||
                               myrec.getNumValue3Indexed() == 4);
                    i++;
                }
            }
            assertEquals(10 + 10 + 10, i);
            assertDiscardedExactly(10 + 10, context);
        }
    }

    /**
     * Verify that a union visitor won't defer a record fetch if the comparison key has fields that the index
     * entry doesn't.
     * This sort of plan is never produced by the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner},
     * so we have to test the visitor directly.
     */
    @Test
    void unionVisitorOnComplexComparisonKey() throws Exception {
        complexQuerySetup(null);

        final IndexScanParameters fullValueScan = IndexScanComparisons.byValue();

        RecordQueryPlan originalPlan1 = RecordQueryUnionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", fullValueScan, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", fullValueScan, false),
                primaryKey("MySimpleRecord"), true);

        RecordQueryPlan modifiedPlan1 = RecordQueryPlannerSubstitutionVisitor.applyRegularVisitors(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(), originalPlan1, recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                fetchFromPartialRecordPlan(
                        unionOnExpressionPlan(
                                coveringIndexPlan()
                                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                                coveringIndexPlan()
                                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))))
                        ));
        assertMatchesExactly(modifiedPlan1, planMatcher);

        RecordQueryPlan originalPlan2 = RecordQueryUnionPlan.from(
                new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", fullValueScan, false),
                new RecordQueryIndexPlan("MySimpleRecord$num_value_3_indexed", fullValueScan, false),
                concat(field("num_value_2"), primaryKey("MySimpleRecord")), true);
        RecordQueryPlan modifiedPlan2 = RecordQueryPlannerSubstitutionVisitor.applyRegularVisitors(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(), originalPlan2, recordStore.getRecordMetaData(), PlannableIndexTypes.DEFAULT, primaryKey("MySimpleRecord"));
        // Visitor should not perform transformation because of comparison key on num_value_unique
        assertEquals(originalPlan2, modifiedPlan2);
    }

    @DualPlannerTest
    @ParameterizedTest(name = "deferFetchOnUnionWithInnerFilter[{0}]")
    @MethodSource("baseParams")
    void deferFetchOnUnionWithInnerFilter(OrQueryParams orQueryParams) throws Exception {
        complexQuerySetup(metaData -> {
            // We don't prefer covering indexes over other indexes yet.
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.addIndex("MySimpleRecord", "coveringIndex", new KeyWithValueExpression(concat(field("num_value_2"), field("num_value_3_indexed")), 1));
        });

        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").startsWith("foo"),
                        Query.and(
                                Query.field("num_value_2").greaterThanOrEquals(2),
                                Query.field("num_value_2").lessThanOrEquals(4)),
                        Query.and(
                                Query.field("num_value_3_indexed").lessThanOrEquals(18),
                                Query.field("num_value_2").greaterThanOrEquals(26))
                ))
                .build();
        orQueryParams.setPlannerConfiguration(this);
        RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        fetchFromPartialRecordPlan(
                                unorderedPrimaryKeyDistinctPlan(
                                        unorderedUnionPlan(
                                                coveringIndexPlan()
                                                        .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("{[foo],[foo]}"))))),
                                                coveringIndexPlan()
                                                        .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[2],[4]]"))))),
                                                filterPlan(
                                                        coveringIndexPlan()
                                                                .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[26],>"))))))
                                                        .where(queryComponents(exactly(equalsObject(Query.field("num_value_3_indexed").lessThanOrEquals(18)))))
                                        )));
                assertMatchesExactly(plan, planMatcher);

                assertEquals(-1829743477, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-1168128533, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                        unorderedPrimaryKeyDistinctPlan(
                                unorderedUnionPlan(
                                        indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("{[foo],[foo]}"))),
                                        indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[2],[4]]"))),
                                        fetchFromPartialRecordPlan(
                                                filterPlan(
                                                        coveringIndexPlan()
                                                                .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[26],>"))))))
                                                        .where(queryComponents(exactly(equalsObject(Query.field("num_value_3_indexed").lessThanOrEquals(18)))))
                                        )));
                assertMatchesExactly(plan, planMatcher);

                assertEquals(-1023404717, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(787297195, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    fetchFromPartialRecordPlan(
                            unorderedPrimaryKeyDistinctPlan(
                                    unorderedUnionPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")).and(scanComparisons(range("{[foo],[foo]}"))))),
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[2],[4]]"))))),
                                            predicatesFilterPlan(
                                                    coveringIndexPlan()
                                                            .where(indexPlanOf(indexPlan().where(indexName("coveringIndex")).and(scanComparisons(range("[[26],>"))))))
                                                    .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("num_value_3_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 18)))))
                                    )));
            assertMatchesExactly(plan, planMatcher);

            assertEquals(-1002673308, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(658371925, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryToDistinctUnion[{0}]")
    @MethodSource("baseParams")
    void testOrQueryToDistinctUnion(OrQueryParams orQueryParams) throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);

        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("m"),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();

        orQueryParams.setPlannerConfiguration(this);
        final RecordQueryPlan plan = planQuery(query);
        final BindingMatcher<? extends RecordQueryPlan> planMatcher = queryPlanMatcher(orQueryParams, List.of(
                indexPlan().where(indexName("MySimpleRecord$str_value_indexed")),
                indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))
        ), null);
        assertMatchesExactly(plan, planMatcher);

        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                assertEquals(-1608004667, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-291254354, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertEquals(-2070415224, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1850172119, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        } else {
            assertEquals(701690694, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1447491315, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }

    @DualPlannerTest
    @ParameterizedTest(name = "testOrQueryToDistinctUnionWithPartialDefer[{0}]")
    @MethodSource("baseParams")
    void testOrQueryToDistinctUnionWithPartialDefer(OrQueryParams orQueryParams) throws Exception {
        complexQuerySetup(null);

        RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(
                        Query.field("str_value_indexed").equalsValue("m"),
                        Query.field("num_value_3_indexed").equalsValue(3),
                        Query.and(Query.field("num_value_2").equalsValue(3), Query.field("num_value_3_indexed").equalsValue(4))))
                .build();

        orQueryParams.setPlannerConfiguration(this);
        final RecordQueryPlan plan = planQuery(query);

        if (planner instanceof RecordQueryPlanner) {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    unionOnExpressionPlan(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")),
                            indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")),
                            filterPlan(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed"))).where(queryComponents(exactly(equalsObject(Query.field("num_value_2").equalsValue(3))))));

            assertMatchesExactly(plan, planMatcher);
            assertEquals(-91578519, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(1329489138, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            final BindingMatcher<? extends RecordQueryPlan> planMatcher =
                    RecordQueryPlanMatchers.unionOnValuesPlan(
                            fetchFromPartialRecordPlan(
                                    RecordQueryPlanMatchers.unionOnValuesPlan(
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$str_value_indexed")))),
                                            coveringIndexPlan()
                                                    .where(indexPlanOf(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")))))),
                            predicatesFilterPlan(indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")))
                                    .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("num_value_2"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 3))))));

            assertMatchesExactly(plan, planMatcher);

            assertEquals(2082208238, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1572133935, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }

    /**
     * TODO [Cascades Planner] We have a UNION DISTINCT of identical arms all containing the same filter. If the filter
     *      is pulled above the UNION the fetch can also be deferred until after the UNION which may be beneficial
     *      (according to the current cost model which is probably correct).
     */
    @Tag(Tags.Slow)
    @DualPlannerTest
    @ParameterizedTest(name = "testComplexOrQueryToDistinctUnion[{0}]")
    @MethodSource("baseParams")
    void testComplexOrQueryToDistinctUnion(OrQueryParams orQueryParams) throws Exception {
        complexQuerySetup(null);

        final RecordQuery query = orQueryParams.queryBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("outer"),
                        Query.or(
                                Query.and(Query.field("num_value_3_indexed").greaterThan(1),
                                        Query.field("num_value_3_indexed").lessThan(2)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(3),
                                        Query.field("num_value_3_indexed").lessThan(4)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(5),
                                        Query.field("num_value_3_indexed").lessThan(6)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(7),
                                        Query.field("num_value_3_indexed").lessThan(8)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(9),
                                        Query.field("num_value_3_indexed").lessThan(10)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(11),
                                        Query.field("num_value_3_indexed").lessThan(12)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(13),
                                        Query.field("num_value_3_indexed").lessThan(14)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(15),
                                        Query.field("num_value_3_indexed").lessThan(16)),
                                Query.and(Query.field("num_value_3_indexed").greaterThan(17),
                                        Query.field("num_value_3_indexed").lessThan(18)))))
                .build();

        orQueryParams.setPlannerConfiguration(this);
        final RecordQueryPlan plan = planQuery(query);

        final List<BindingMatcher<RecordQueryIndexPlan>> indexMatchers = List.of(
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([1],[2])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([3],[4])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([5],[6])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([7],[8])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([9],[10])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([11],[12])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([13],[14])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([15],[16])"))),
                        indexPlan().where(indexName("MySimpleRecord$num_value_3_indexed")).and(scanComparisons(range("([17],[18])")))
        );

        final BindingMatcher<? extends RecordQueryPlan> planMatcher;
        if (planner instanceof RecordQueryPlanner) {
            if (orQueryParams.shouldDeferFetch()) {
                planMatcher = filterPlan(
                        fetchFromPartialRecordPlan(
                                unionOnExpressionPlan(
                                        indexMatchers.stream()
                                                .map(indexPlanMatcher -> coveringIndexPlan().where(indexPlanOf(indexPlanMatcher)))
                                                .collect(Collectors.toList()))
                                        .where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))))
                        ))
                        .where(queryComponents(exactly(equalsObject(Query.field("str_value_indexed").equalsValue("outer")))));
            } else {
                planMatcher = unionOnExpressionPlan(
                        indexMatchers
                                .stream()
                                .map(indexPlanMatcher -> filterPlan(indexPlanMatcher).where(queryComponents(exactly(equalsObject(Query.field("str_value_indexed").equalsValue("outer"))))))
                                .collect(Collectors.toList())
                ).where(comparisonKey(concat(field("num_value_3_indexed"), primaryKey("MySimpleRecord"))));
            }
        } else {
            planMatcher = RecordQueryPlanMatchers.unionOnValuesPlan(
                    indexMatchers.stream().map(indexPlanMatcher -> predicatesFilterPlan(indexPlanMatcher)
                            .where(predicates(only(valuePredicate(ValueMatchers.fieldValueWithFieldNames("str_value_indexed"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "outer")))))
                    ).collect(Collectors.toList())
            ).where(comparisonKeyValues(exactly(fieldValueWithFieldNames("num_value_3_indexed"), fieldValueWithFieldNames("rec_no"))));
        }
        assertMatches(plan, planMatcher);
    }

    @Nonnull
    private BindingMatcher<? extends RecordQueryPlan> queryPlanMatcher(@Nonnull OrQueryParams orQueryParams, @Nonnull List<BindingMatcher<RecordQueryIndexPlan>> indexPlanMatchers, @Nullable KeyExpression comparisonKey) {
        BindingMatcher<? extends RecordQueryUnionPlan> unionPlanMatcher = unionPlanMatcher(orQueryParams, indexPlanMatchers, comparisonKey);
        if (planner instanceof CascadesPlanner || orQueryParams.shouldDeferFetch()) {
            return fetchFromPartialRecordPlan(unionPlanMatcher);
        } else {
            return unionPlanMatcher;
        }
    }

    @Nonnull
    private BindingMatcher<? extends RecordQueryUnionPlan> unionPlanMatcher(@Nonnull OrQueryParams orQueryParams, @Nonnull List<BindingMatcher<RecordQueryIndexPlan>> indexPlanMatchers, @Nullable KeyExpression comparisonKey) {
        if (planner instanceof RecordQueryPlanner) {
            BindingMatcher<RecordQueryUnionOnKeyExpressionPlan> unionPlanMatcher;
            if (orQueryParams.shouldDeferFetch()) {
                unionPlanMatcher = unionOnExpressionPlan(
                        indexPlanMatchers.stream().map(indexPlanMatcher -> coveringIndexPlan().where(indexPlanOf(indexPlanMatcher))).collect(Collectors.toList())
                );
            } else {
                unionPlanMatcher = unionOnExpressionPlan(indexPlanMatchers);
            }
            if (comparisonKey != null) {
                unionPlanMatcher = unionPlanMatcher.where(comparisonKey(comparisonKey));
            }
            return unionPlanMatcher;
        } else {
            BindingMatcher<RecordQueryUnionOnValuesPlan> unionPlanMatcher = unionOnValuesPlan(
                    indexPlanMatchers.stream().map(indexPlanMatcher -> coveringIndexPlan().where(indexPlanOf(indexPlanMatcher))).collect(Collectors.toList())
            );
            if (comparisonKey != null) {
                List<BindingMatcher<? extends Value>> fieldValueMatchers = comparisonKey.normalizeKeyForPositions().stream()
                        .flatMap(childExpr -> {
                            if (childExpr instanceof FieldKeyExpression) {
                                return Stream.of(fieldValueWithFieldNames(((FieldKeyExpression)childExpr).getFieldName()));
                            } else if (childExpr instanceof NestingKeyExpression) {
                                String parent = ((NestingKeyExpression)childExpr).getParent().getFieldName();
                                return ((NestingKeyExpression)childExpr).getChildren().stream()
                                        .map(innerChild -> {
                                            if (innerChild instanceof FieldKeyExpression) {
                                                return fieldValueWithFieldNames(parent + "." + ((FieldKeyExpression)innerChild).getFieldName());
                                            } else {
                                                return fail("unexpected comparison key type: " + childExpr);
                                            }
                                        });
                            } else {
                                return fail("unexpected comparison key type: " + childExpr);
                            }
                        })
                        .collect(Collectors.toList());
                unionPlanMatcher = unionPlanMatcher.where(comparisonKeyValues(exactly(fieldValueMatchers)));
            }
            return unionPlanMatcher;
        }
    }
}
