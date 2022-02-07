/*
 * BitmapValueIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsBitmapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexAggregateFunctionCall;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexAggregate;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType.FanOut;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.compositeBitmap;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScanType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@code BITMAP_VALUE} type indexes.
 */
@Tag(Tags.RequiresFDB)
class BitmapValueIndexTest extends FDBRecordStoreTestBase {

    @Test
    void basic() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("rec_no_by_str_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from("odd", 1)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 1)
                            .collect(Collectors.toList())));
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("rec_no_by_str_num3"), IndexScanType.BY_GROUP,
                            TupleRange.between(Tuple.from("odd", 1, 150), Tuple.from("odd", 1, 175)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(IntStream.range(150, 175).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 1)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void aggregateFunction() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            final IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.BITMAP_VALUE, REC_NO_BY_STR_NUM3, null);
            assertThat(
                    collectOnBits(recordStore.evaluateAggregateFunction(
                            Collections.singletonList("MySimpleRecord"), aggregateFunction,
                            TupleRange.allOf(Tuple.from("odd", 3)),
                            IsolationLevel.SERIALIZABLE).join().getBytes(0), 0),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 3)
                            .collect(Collectors.toList())));
            assertThat(
                    collectOnBits(recordStore.evaluateAggregateFunction(
                            Collections.singletonList("MySimpleRecord"), aggregateFunction,
                            TupleRange.between(Tuple.from("odd", 3, 160), Tuple.from("odd", 3, 180)),
                            IsolationLevel.SERIALIZABLE).join().getBytes(0), 160),
                    equalTo(IntStream.range(160, 180).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 3)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void nonPrimaryKey() {
        final RecordMetaDataHook num_by_num3_hook = metadata -> {
            metadata.addIndex(metadata.getRecordType("MySimpleRecord"),
                              new Index("num_by_num3",
                                        concatenateFields("num_value_3", "num_value_unique").group(1),
                                        IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        };
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook));
            saveRecords(0, 100);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook));
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(IntStream.range(1000, 1100).boxed()
                            .filter(i -> (i % 5) == 2)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void uniquenessViolationChecked() {
        final RecordMetaDataHook num_by_num3_hook_not_unique = metadata -> {
            metadata.removeIndex("MySimpleRecord$num_value_unique");
            metadata.addIndex(metadata.getRecordType("MySimpleRecord"),
                    new Index("num_by_num3",
                            concatenateFields("num_value_3", "num_value_unique").group(1),
                            IndexTypes.BITMAP_VALUE, ImmutableMap.of(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16", IndexOptions.UNIQUE_OPTION, "true")));
        };
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            saveRecords(0, 10);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            assertThrows(RecordIndexUniquenessViolation.class, () -> {
                // This is a duplicate of record #2.
                recordStore.saveRecord(TestRecordsBitmapProto.MySimpleRecord.newBuilder()
                        .setRecNo(1002)
                        .setStrValue("even")
                        .setNumValueUnique(1002)
                        .setNumValue3(2)
                        .build());
            });
        }
    }

    @Test
    void uniquenessViolationNotChecked() {
        final RecordMetaDataHook num_by_num3_hook_not_unique = metadata -> {
            metadata.removeIndex("MySimpleRecord$num_value_unique");
            metadata.addIndex(metadata.getRecordType("MySimpleRecord"),
                    new Index("num_by_num3",
                            concatenateFields("num_value_3", "num_value_unique").group(1),
                            IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        };
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            saveRecords(0, 10);
            commit(context);
        }
        final List<Integer> expected = IntStream.range(1000, 1010).boxed()
                .filter(i -> (i % 5) == 2)
                .collect(Collectors.toList());
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(expected));
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            // This is a duplicate of record #2.
            recordStore.saveRecord(TestRecordsBitmapProto.MySimpleRecord.newBuilder()
                    .setRecNo(1002)
                    .setStrValue("even")
                    .setNumValueUnique(1002)
                    .setNumValue3(2)
                    .build());
            commit(context);
        }
        // Bitmap unchanged.
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(expected));
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            // Removing the duplicate removes the shared bit.
            recordStore.deleteRecord(Tuple.from(1002));
            commit(context);
        }
        final List<Integer> expected2 = new ArrayList<>(expected);
        expected2.remove(Integer.valueOf(1002));
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(num_by_num3_hook_not_unique));
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(expected2));
        }
    }

    @Test
    void andQuery() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            // Covering(Index(rec_no_by_str_num2 [[odd, 3],[odd, 3]] BY_GROUP) -> [rec_no: KEY[2]]) BITAND Covering(Index(rec_no_by_str_num3 [[odd, 4],[odd, 4]] BY_GROUP) -> [rec_no: KEY[2]])
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.field("num_value_3").equalsValue(4)));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 4],[odd, 4]]"))))))));
            assertEquals(1339577615, queryPlan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1022755654, queryPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(944107193, queryPlan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && (i % 5) == 4)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void andQueryPosition() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            // Covering(Index(rec_no_by_str_num2 ([odd, 3, 150],[odd, 3]] BY_GROUP) -> [rec_no: KEY[2]]) BITAND Covering(Index(rec_no_by_str_num3 ([odd, 4, 150],[odd, 4]] BY_GROUP) -> [rec_no: KEY[2]])
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.field("num_value_3").equalsValue(4),
                    Query.field("rec_no").greaterThan(150)));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("([odd, 3, 150],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("([odd, 4, 150],[odd, 4]]"))))))));
            assertEquals(-1911273393, queryPlan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2018486938, queryPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1342370457, queryPlan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(151, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && (i % 5) == 4)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void andOrQuery() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            // Covering(Index(rec_no_by_str_num2 [[odd, 3],[odd, 3]] BY_GROUP) -> [rec_no: KEY[2]]) BITAND Covering(Index(rec_no_by_str_num3 [[odd, 2],[odd, 2]] BY_GROUP) -> [rec_no: KEY[2]]) BITOR Covering(Index(rec_no_by_str_num3 [[odd, 4],[odd, 4]] BY_GROUP) -> [rec_no: KEY[2]])
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.or(Query.field("num_value_3").equalsValue(2),
                             Query.field("num_value_3").equalsValue(4))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1] BITOR [2]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 2],[odd, 2]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 4],[odd, 4]]"))))))));
            assertEquals(1173292541, queryPlan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1559227819, queryPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(72895039, queryPlan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && ((i % 5) == 2 || (i % 5) == 4))
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void andOrQueryWithContinuation() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.or(Query.field("num_value_3").equalsValue(1),
                             Query.field("num_value_3").equalsValue(4))));
            List<Integer> onBits = new ArrayList<>();
            int ntimes = 0;
            byte[] continuation = null;
            do {
                RecordCursor<IndexEntry> cursor = queryPlan.execute(recordStore, EvaluationContext.EMPTY, continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(2).build())
                        .map(FDBQueriedRecord::getIndexEntry);
                RecordCursorResult<IndexEntry> cursorResult = cursor.forEachResult(i -> onBits.addAll(collectOnBits(i.get()))).join();
                ntimes++;
                continuation = cursorResult.getContinuation().toBytes();
            } while (continuation != null);
            assertThat(
                    onBits,
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && ((i % 5) == 1 || (i % 5) == 4))
                            .collect(Collectors.toList())));
            assertThat(ntimes, equalTo(4));
        }
    }

    @Test
    void andOrQueryWithDuplicate() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            // Covering(Index(rec_no_by_str_num2 [[odd, 3],[odd, 3]] BY_GROUP) -> [rec_no: KEY[2]]) BITAND Covering(Index(rec_no_by_str_num3 [[odd, 0],[odd, 0]] BY_GROUP) -> [rec_no: KEY[2]]) BITOR Covering(Index(rec_no_by_str_num2 [[odd, 3],[odd, 3]] BY_GROUP) -> [rec_no: KEY[2]]) BITAND Covering(Index(rec_no_by_str_num3 [[odd, 4],[odd, 4]] BY_GROUP) -> [rec_no: KEY[2]])
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.or(
                            Query.and(
                                    Query.field("num_value_2").equalsValue(3),
                                    Query.field("num_value_3").equalsValue(0)),
                            Query.and(
                                    Query.field("num_value_2").equalsValue(3),
                                    Query.field("num_value_3").equalsValue(4)))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1] BITOR [0] BITAND [2]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 0],[odd, 0]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 4],[odd, 4]]"))))))));
            assertEquals(1788540340, queryPlan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1021904334, queryPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1583681802, queryPlan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> ((i % 7) == 3 && (i % 5) == 0) || ((i % 7) == 3 && (i % 5) == 4))
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void andNotQuery() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            // Covering(Index(rec_no_by_str_num2 [[odd, 1],[odd, 1]] BY_GROUP) -> [rec_no: KEY[2]]) BITAND BITNOT Covering(Index(rec_no_by_str_num3 [[odd, 2],[odd, 2]] BY_GROUP) -> [rec_no: KEY[2]])
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(1),
                    Query.not(Query.field("num_value_3").equalsValue(2))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND BITNOT [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 1],[odd, 1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 2],[odd, 2]]"))))))));
            assertEquals(1339577551, queryPlan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(17236339, queryPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(2043204530, queryPlan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 1 && !((i % 5) == 2))
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void nonOverlappingOrQuery() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            for (int recNo = 100; recNo < 200; recNo++) {
                recordStore.saveRecord(TestRecordsBitmapProto.MySimpleRecord.newBuilder()
                        .setRecNo(recNo)
                        .setStrValue((recNo & 1) == 1 ? "odd" : "even")
                        .setNumValue2(1)
                        .build());
            }
            for (int recNo = 500; recNo < 600; recNo++) {
                recordStore.saveRecord(TestRecordsBitmapProto.MySimpleRecord.newBuilder()
                        .setRecNo(recNo)
                        .setStrValue((recNo & 1) == 1 ? "odd" : "even")
                        .setNumValue3(1)
                        .build());
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            // Covering(Index(rec_no_by_str_num2 [[odd, 1],[odd, 1]] BY_GROUP) -> [rec_no: KEY[2]]) BITOR Covering(Index(rec_no_by_str_num3 [[odd, 1],[odd, 1]] BY_GROUP) -> [rec_no: KEY[2]])
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.or(Query.field("num_value_2").equalsValue(1),
                             Query.field("num_value_3").equalsValue(1))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITOR [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 1],[odd, 1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 1],[odd, 1]]"))))))));
            assertEquals(-556720460, queryPlan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1315884767, queryPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-952190817, queryPlan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.concat(IntStream.range(100, 200), IntStream.range(500, 600)).boxed()
                            .filter(i -> (i & 1) == 1)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void nestedAndQuery() {
        final KeyExpression num_by_str = field("nested").nest(field("entry", FanOut).nest(concatenateFields("str_value", "num_value")));
        final GroupingKeyExpression nested_num_by_str = concat(field("num_value_1"), num_by_str).group(1);
        final KeyExpression nested_num_by_str_num2 = concat(field("num_value_1"), field("num_value_2"), num_by_str).group(1);
        final KeyExpression nested_num_by_str_num3 = concat(field("num_value_1"), field("num_value_3"), num_by_str).group(1);
        final RecordMetaDataHook nested_rec_no_by_str_nums_hook = metadata -> {
            final RecordTypeBuilder recordType = metadata.getRecordType("MyNestedRecord");
            metadata.addIndex(recordType, new Index("nested_num_by_str_num2", nested_num_by_str_num2, IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
            metadata.addIndex(recordType, new Index("nested_num_by_str_num3", nested_num_by_str_num3, IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        };
        final IndexAggregateFunctionCall bitmap_value_nested_num_by_str = new IndexAggregateFunctionCall(FunctionNames.BITMAP_VALUE, nested_num_by_str);
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(nested_rec_no_by_str_nums_hook));
            for (int recNo = 100; recNo < 200; recNo++) {
                recordStore.saveRecord(TestRecordsBitmapProto.MyNestedRecord.newBuilder()
                        .setRecNo(recNo)
                        .setNumValue1(1)
                        .setNested(TestRecordsBitmapProto.MyNestedRecord.Nested.newBuilder()
                                .addEntry(TestRecordsBitmapProto.MyNestedRecord.Nested.Entry.newBuilder()
                                        .setStrValue((recNo & 1) == 1 ? "odd" : "even")
                                        .setNumValue(recNo + 1000)))
                        .setNumValue2(recNo % 7)
                        .setNumValue3(recNo % 5)
                        .build());
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(nested_rec_no_by_str_nums_hook));
            setupPlanner(null);
            final RecordQuery recordQuery = RecordQuery.newBuilder()
                    .setRecordType("MyNestedRecord")
                    .setFilter(Query.and(
                            Query.field("num_value_1").equalsValue(1),
                            Query.field("nested").matches(Query.field("entry").oneOfThem().matches(Query.field("str_value").equalsValue("odd"))),
                            Query.field("num_value_2").equalsValue(3),
                            Query.field("num_value_3").equalsValue(4)))
                    .setRequiredResults(Collections.singletonList(field("nested").nest(field("entry", FanOut).nest("num_value"))))
                    .build();
            final RecordQueryPlan queryPlan =  ComposedBitmapIndexAggregate.tryPlan((RecordQueryPlanner)planner,
                            recordQuery, bitmap_value_nested_num_by_str, IndexQueryabilityFilter.DEFAULT)
                    .orElseGet(() -> fail("Cannot plan query"));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("nested_num_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[1, 3, odd],[1, 3, odd]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("nested_num_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[1, 4, odd],[1, 4, odd]]"))))))));
            assertEquals(1000204717, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && (i % 5) == 4)
                            .map(i -> i + 1000)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void singleQuery() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3)));
            assertThat(queryPlan, coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))));
            assertEquals(1188586655, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    void negatedQuery() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            assertThrows(AssertionFailedError.class, () -> {
                plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                        Query.field("str_value").equalsValue("odd"),
                        Query.not(Query.field("num_value_2").equalsValue(3))));
            });
        }
    }

    @Test
    void filterIndexSelection() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData(REC_NO_BY_STR_NUMS_HOOK));
            setupPlanner(null);
            final RecordQuery recordQuery = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(
                        Query.field("str_value").equalsValue("odd"),
                        Query.field("num_value_2").equalsValue(3)))
                    .setRequiredResults(Collections.singletonList(field("rec_no")))
                    .build();
            final RecordQueryPlan queryPlan = ComposedBitmapIndexAggregate.tryPlan((RecordQueryPlanner)planner,
                            recordQuery, BITMAP_VALUE_REC_NO_BY_STR, IndexQueryabilityFilter.TRUE)
                    .orElseGet(() -> fail("Cannot plan query"));
            assertThat(queryPlan,
                    coveringIndexScan(indexScan(allOf(
                            indexName("rec_no_by_str_num2"),
                            indexScanType(IndexScanType.BY_GROUP),
                            bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))));
            assertEquals(1188586655, queryPlan.planHash());
            assertEquals(Optional.empty(),
                    ComposedBitmapIndexAggregate.tryPlan((RecordQueryPlanner)planner,
                            recordQuery, BITMAP_VALUE_REC_NO_BY_STR, IndexQueryabilityFilter.FALSE));
        }
    }

    protected static final GroupingKeyExpression REC_NO_BY_STR = concatenateFields("str_value", "rec_no").group(1);
    protected static final GroupingKeyExpression REC_NO_BY_STR_NUM2 = concatenateFields("str_value", "num_value_2", "rec_no").group(1);
    protected static final GroupingKeyExpression REC_NO_BY_STR_NUM3 = concatenateFields("str_value", "num_value_3", "rec_no").group(1);
    protected static final Map<String, String> SMALL_BITMAP_OPTIONS = Collections.singletonMap(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16");
    protected static final RecordMetaDataHook REC_NO_BY_STR_NUMS_HOOK = metadata -> {
        final RecordTypeBuilder recordType = metadata.getRecordType("MySimpleRecord");
        metadata.addIndex(recordType, new Index("rec_no_by_str_num2", REC_NO_BY_STR_NUM2, IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        metadata.addIndex(recordType, new Index("rec_no_by_str_num3", REC_NO_BY_STR_NUM3, IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
    };

    protected static final IndexAggregateFunctionCall BITMAP_VALUE_REC_NO_BY_STR = new IndexAggregateFunctionCall(FunctionNames.BITMAP_VALUE, REC_NO_BY_STR);

    protected RecordMetaData metaData(@Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsBitmapProto.getDescriptor());
        hook.apply(metaData);
        return metaData.getRecordMetaData();
    }

    protected void saveRecords(int start, int end) {
        for (int recNo = start; recNo < end; recNo++) {
            recordStore.saveRecord(TestRecordsBitmapProto.MySimpleRecord.newBuilder()
                    .setRecNo(recNo)
                    .setStrValue((recNo & 1) == 1 ? "odd" : "even")
                    .setNumValueUnique(recNo + 1000)
                    .setNumValue2(recNo % 7)
                    .setNumValue3(recNo % 5)
                    .build());
        }
    }

    protected List<Integer> collectOnBits(@Nonnull RecordCursor<IndexEntry> indexEntries) {
        return indexEntries.reduce(new ArrayList<Integer>(), (list, entries) -> {
            list.addAll(collectOnBits(entries));
            return list;
        }).join();
    }

    protected List<Integer> collectOnBits(@Nonnull IndexEntry indexEntry) {
        return collectOnBits(indexEntry.getValue().getBytes(0), (int)indexEntry.getKey().getLong(indexEntry.getKeySize() - 1));
    }

    protected List<Integer> collectOnBits(@Nonnull byte[] bitmap, int offset) {
        final List<Integer> result = new ArrayList<>();
        for (int i = 0; i < bitmap.length; i++) {
            if (bitmap[i] != 0) {
                for (int j = 0; j < 8; j++) {
                    if ((bitmap[i] & (1 << j)) != 0) {
                        result.add(offset + i * 8 + j);
                    }
                }
            }
        }
        return result;
    }

    protected RecordQueryPlan plan(@Nonnull IndexAggregateFunctionCall functionCall, @Nonnull QueryComponent filter) {
        final RecordQuery recordQuery = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setRequiredResults(Collections.singletonList(field("rec_no")))
                .build();
        return ComposedBitmapIndexAggregate.tryPlan((RecordQueryPlanner)planner, recordQuery, functionCall, IndexQueryabilityFilter.DEFAULT)
                .orElseGet(() -> fail("Cannot plan query"));
    }

}
