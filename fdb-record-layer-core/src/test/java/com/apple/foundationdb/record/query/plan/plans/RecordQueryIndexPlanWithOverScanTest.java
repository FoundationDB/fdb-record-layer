/*
 * RecordQueryIndexPlanWithOverScanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursorBase;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of {@link RecordQueryIndexPlan} with {@link com.apple.foundationdb.record.IndexScanType#BY_VALUE_OVER_SCAN}.
 * This scan type should have identical semantics to the {@link com.apple.foundationdb.record.IndexScanType#BY_VALUE},
 * but it may scan additional ranges to load a cache. The primary thing that these tests are designed to verify is that
 * the behavior of the two different scan types are the same, including that continuations are compatible.
 */
@Tag(Tags.RequiresFDB)
class RecordQueryIndexPlanWithOverScanTest extends FDBRecordStoreQueryTestBase {
    private static final RecordQueryPlannerConfiguration plannerConfiguration = RecordQueryPlannerConfiguration.builder()
            .setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_INDEX)
            .setAttemptFailedInJoinAsOr(true)
            .setComplexityThreshold(RecordQueryPlanner.DEFAULT_COMPLEXITY_THRESHOLD)
            .addValueIndexOverScanNeeded("MySimpleRecord$str_value_indexed")
            .addValueIndexOverScanNeeded("compoundIndex")
            .build();

    @ParameterizedTest(name = "basicScanTest[reverse={0}]")
    @BooleanSource
    void basicScanTest(boolean reverse) throws Exception {
        setupSimpleRecordStore(NO_HOOK, (i, builder) ->
                builder.setRecNo(i).setStrValueIndexed((char)('a' + (i % 26)) + "_suffix"));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(Query.field("str_value_indexed").greaterThan("bar"), Query.field("str_value_indexed").lessThan("foo")))
                    .setSort(Key.Expressions.field("str_value_indexed"), reverse)
                    .build();
            final Matcher<RecordQueryIndexPlan> indexPlanMatcher = allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("([bar],[foo])")));
            RecordQueryPlan indexPlan = recordStore.planQuery(query);
            assertThat(indexPlan, indexScan(indexPlanMatcher));
            RecordQueryPlan indexPlanWithOverScan = recordStore.planQuery(query, ParameterRelationshipGraph.empty(), plannerConfiguration);

            timer.reset();
            assertTrue(assertSameResults(recordStore, indexPlan, indexPlanWithOverScan, EvaluationContext.EMPTY, ExecuteProperties.SERIAL_EXECUTE, null).isEnd());
            // There should be 32 load records, because these values of str_value_indexed should be read:
            //    c_suffix, d_suffix, e_suffix, f_suffix
            // There are 100 records, so that corresponds to records: 3, 4, 5, 6, 29, 30, 31, 32, 55, 56, 57, 58, 81, 82, 83 and 84
            // Those 16 records are read twice, once by each plan
            assertEquals(32, timer.getCount(FDBStoreTimer.Events.LOAD_RECORD));

            commit(context);
        }
    }

    @ParameterizedTest(name = "coveringIndexScanTest[reverse={0}]")
    @BooleanSource
    void coveringIndexScanTest(boolean reverse) throws Exception {
        setupSimpleRecordStore(NO_HOOK, (i, builder) ->
                builder.setRecNo(i).setStrValueIndexed(i % 2 == 0 ? "foo" : "bar"));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setRequiredResults(List.of(Key.Expressions.field("str_value_indexed"), Key.Expressions.field("rec_no")))
                    .setFilter(Query.field("str_value_indexed").equalsValue("foo"))
                    .setSort(Key.Expressions.field("str_value_indexed"), reverse)
                    .build();
            final Matcher<RecordQueryIndexPlan> indexPlanMatcher = allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[foo],[foo]]")));
            RecordQueryPlan indexPlan = recordStore.planQuery(query);
            assertThat(indexPlan, coveringIndexScan(indexScan(indexPlanMatcher)));
            RecordQueryPlan indexPlanWithOverScan = recordStore.planQuery(query, ParameterRelationshipGraph.empty(), plannerConfiguration);

            timer.reset();
            assertTrue(assertSameResults(recordStore, indexPlan, indexPlanWithOverScan, EvaluationContext.EMPTY, ExecuteProperties.SERIAL_EXECUTE, null).isEnd());
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.LOAD_RECORD), "covering plans should load 0 records");

            commit(context);
        }
    }

    /**
     * This simulates a case that might actually benefit from using the overscan to load more data into the cache.
     * In this case, there is an index on {@code (str_value_indexed, num_value_3_indexed)}, and this wants to
     * load the record(s) for a bunch of different values of {@code num_value_3_indexed} for a given value of
     * {@code str_value_indexed}, some of which will have an associated record and some of which won't. When using
     * the overscan, some empty ranges will be put into the cache, so if a request comes in for an adjacent
     * value, the index can be looked up in the cache.
     *
     * @param reverse whether to execute individual scans in reverse
     * @throws Exception rethrown from store set up and opening
     */
    @ParameterizedTest(name = "evaluateMultipleParameters[reverse={0}]")
    @BooleanSource
    void evaluateMultipleParameters(boolean reverse) throws Exception {
        final Index compoundIndex = new Index("compoundIndex", Key.Expressions.concat(Key.Expressions.field("str_value_indexed"), Key.Expressions.field("num_value_3_indexed")));
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", compoundIndex);
        setupSimpleRecordStore(hook, (i, builder) ->
                builder.setRecNo(i + 100L).setNumValue3Indexed(i).setStrValueIndexed(i % 2 == 0 ? "even" : "odd"));

        Random r = new Random();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(Query.field("str_value_indexed").equalsValue("even"), Query.field("num_value_3_indexed").equalsParameter("val")))
                    .setSort(compoundIndex.getRootExpression(), reverse)
                    .build();
            final Matcher<RecordQueryIndexPlan> indexPlanMatcher = allOf(indexName(compoundIndex.getName()), bounds(hasTupleString("[EQUALS even, EQUALS $val]")));
            RecordQueryPlan indexPlan = recordStore.planQuery(query);
            assertThat(indexPlan, indexScan(indexPlanMatcher));
            RecordQueryPlan indexPlanWithOverScan = recordStore.planQuery(query, ParameterRelationshipGraph.empty(), plannerConfiguration);

            for (int i = 0; i < 50; i++) {
                EvaluationContext evaluationContext = EvaluationContext.EMPTY.withBinding("val", r.nextInt(100));
                assertSameResults(recordStore, indexPlan, indexPlanWithOverScan, evaluationContext, ExecuteProperties.SERIAL_EXECUTE, null);
            }

            commit(context);
        }
    }

    @SuppressWarnings("unused") // used as parameter source for parameterized test
    static Stream<Arguments> withLimits() {
        List<Integer> limits = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        return Stream.of(false, true).flatMap(reverse ->
                limits.stream().map(limit ->
                        Arguments.of(reverse, limit)));
    }

    @ParameterizedTest(name = "withLimits[reverse={0}, limit={1}]")
    @MethodSource
    void withLimits(boolean reverse, int limit) throws Exception {
        final Index compoundIndex = new Index("compoundIndex",
                Key.Expressions.concat(Key.Expressions.field("num_value_2"),
                        Key.Expressions.field("str_value_indexed"),
                Key.Expressions.field("num_value_unique")));
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", compoundIndex);
        setupSimpleRecordStore(hook, (i, builder) ->
                builder.setRecNo(i).setNumValue2(i % 7).setStrValueIndexed(i % 3 == 0 ? "threven" : "throdd").setNumValueUnique(i));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(Query.field("num_value_2").equalsParameter("num_val"), Query.field("str_value_indexed").equalsParameter("str_val")))
                    .setSort(compoundIndex.getRootExpression(), reverse)
                    .setRequiredResults(List.of(Key.Expressions.field("num_value_unique")))
                    .build();
            final Matcher<RecordQueryIndexPlan> indexPlanMatcher = allOf(indexName(compoundIndex.getName()), bounds(hasTupleString("[EQUALS $num_val, EQUALS $str_val]")));
            RecordQueryPlan indexPlan = recordStore.planQuery(query);
            assertThat(indexPlan, coveringIndexScan(indexScan(indexPlanMatcher)));
            RecordQueryPlan indexPlanWithOverScan = recordStore.planQuery(query, ParameterRelationshipGraph.empty(), plannerConfiguration);

            ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                    .setDefaultCursorStreamingMode(CursorStreamingMode.WANT_ALL)
                    .setReturnedRowLimit(limit)
                    .build();
            EvaluationContext evaluationContext = EvaluationContext.EMPTY
                    .withBinding("str_val", "threven")
                    .withBinding("num_val", 2);

            RecordCursorContinuation continuation = RecordCursorStartContinuation.START;
            do {
                continuation = assertSameResults(recordStore, indexPlan, indexPlanWithOverScan, evaluationContext, executeProperties, continuation.toBytes());
            } while (!continuation.isEnd());

            commit(context);
        }
    }

    // Validates that the index plan and the overscan index plan return identical results, including continuations
    private static RecordCursorContinuation assertSameResults(@Nonnull FDBRecordStore recordStore,
                                                              @Nonnull RecordQueryPlan indexPlan,
                                                              @Nonnull RecordQueryPlan overscanIndexPlan,
                                                              @Nonnull EvaluationContext evaluationContext,
                                                              @Nonnull ExecuteProperties executeProperties,
                                                              @Nullable byte[] continuation) {
        // Ensure the scan properties for each have their own execute state
        ExecuteProperties indexExecuteProperties = executeProperties.setState(new ExecuteState());
        ExecuteProperties overscanExecuteProperties = executeProperties.setState(new ExecuteState());
        try (RecordCursor<FDBQueriedRecord<Message>> indexCursor = indexPlan.execute(recordStore, evaluationContext, continuation, indexExecuteProperties);
                RecordCursor<FDBQueriedRecord<Message>> overscanCursor = overscanIndexPlan.execute(recordStore, evaluationContext, continuation, overscanExecuteProperties)) {
            RecordCursorResult<FDBQueriedRecord<Message>> indexResult;
            RecordCursorResult<FDBQueriedRecord<Message>> overscanResult;
            do {
                indexResult = indexCursor.getNext();
                overscanResult = overscanCursor.getNext();

                ByteString indexResultContinuationInByteString = indexResult.getContinuation() instanceof KeyValueCursorBase.Continuation ? ((KeyValueCursorBase.Continuation) indexResult.getContinuation()).getInnerContinuationInByteString() : indexResult.getContinuation().toByteString();
                byte[] indexResultContinuationInBytes = indexResult.getContinuation() instanceof KeyValueCursorBase.Continuation ? ((KeyValueCursorBase.Continuation) indexResult.getContinuation()).getInnerContinuationInBytes() : indexResult.getContinuation().toBytes();

                assertEquals(indexResultContinuationInByteString, overscanResult.getContinuation().toByteString(), "Continuation byte strings should match");
                assertArrayEquals(indexResultContinuationInBytes, overscanResult.getContinuation().toBytes(), "Continuation byte arrays should match");

                assertEquals(indexResult.hasNext(), overscanResult.hasNext(), "Overscan cursor should have next if index result has next");
                if (indexResult.hasNext()) {
                    assertEquals(indexResult.get().getRecord(), overscanResult.get().getRecord(), "Result returned via overscan cursor should match regular cursor");
                } else {
                    assertEquals(indexResult.getNoNextReason(), overscanResult.getNoNextReason(), "Overscan cursor should have same no next reason as index result");
                }
            } while (indexResult.hasNext());

            return indexResult.getContinuation();
        }
    }
}
