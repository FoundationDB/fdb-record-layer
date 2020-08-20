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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
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

/**
 * Tests for {@code BITMAP_VALUE} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class BitmapValueIndexTest extends FDBRecordStoreTestBase {

    @Test
    public void basic() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
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
    public void aggregateFunction() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
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
    public void nonPrimaryKey() {
        final RecordMetaDataHook num_by_num3_hook = metadata -> {
            metadata.addIndex(metadata.getRecordType("MySimpleRecord"),
                              new Index("num_by_num3",
                                        concatenateFields("num_value_3_indexed", "num_value_unique").group(1),
                                        IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook);
            saveRecords(0, 100);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook);
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
    public void uniquenessViolationChecked() {
        final RecordMetaDataHook num_by_num3_hook_not_unique = metadata -> {
            metadata.removeIndex("MySimpleRecord$num_value_unique");
            metadata.addIndex(metadata.getRecordType("MySimpleRecord"),
                    new Index("num_by_num3",
                            concatenateFields("num_value_3_indexed", "num_value_unique").group(1),
                            IndexTypes.BITMAP_VALUE, ImmutableMap.of(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16", IndexOptions.UNIQUE_OPTION, "true")));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            saveRecords(0, 10);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            assertThrows(RecordIndexUniquenessViolation.class, () -> {
                // This is a duplicate of record #2.
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1002)
                        .setStrValueIndexed("even")
                        .setNumValueUnique(1002)
                        .setNumValue3Indexed(2)
                        .build());
            });
        }
    }

    @Test
    public void uniquenessViolationNotChecked() {
        final RecordMetaDataHook num_by_num3_hook_not_unique = metadata -> {
            metadata.removeIndex("MySimpleRecord$num_value_unique");
            metadata.addIndex(metadata.getRecordType("MySimpleRecord"),
                    new Index("num_by_num3",
                            concatenateFields("num_value_3_indexed", "num_value_unique").group(1),
                            IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            saveRecords(0, 10);
            commit(context);
        }
        final List<Integer> expected = IntStream.range(1000, 1010).boxed()
                .filter(i -> (i % 5) == 2)
                .collect(Collectors.toList());
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(expected));
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            // This is a duplicate of record #2.
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1002)
                    .setStrValueIndexed("even")
                    .setNumValueUnique(1002)
                    .setNumValue3Indexed(2)
                    .build());
            commit(context);
        }
        // Bitmap unchanged.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(expected));
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            // Removing the duplicate removes the shared bit.
            recordStore.deleteRecord(Tuple.from(1002));
            commit(context);
        }
        final List<Integer> expected2 = new ArrayList<>(expected);
        expected2.remove(Integer.valueOf(1002));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook_not_unique);
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(expected2));
        }
    }

    @Test
    public void andQuery() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value_indexed").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.field("num_value_3_indexed").equalsValue(4)));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 4],[odd, 4]]"))))))));
            assertEquals(1339577615, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && (i % 5) == 4)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    public void andQueryPosition() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value_indexed").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.field("num_value_3_indexed").equalsValue(4),
                    Query.field("rec_no").greaterThan(150)));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("([odd, 3, 150],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("([odd, 4, 150],[odd, 4]]"))))))));
            assertEquals(-1911273393, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(151, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && (i % 5) == 4)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    public void andOrQuery() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value_indexed").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.or(Query.field("num_value_3_indexed").equalsValue(2),
                             Query.field("num_value_3_indexed").equalsValue(4))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1] BITOR [2]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 2],[odd, 2]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 4],[odd, 4]]"))))))));
            assertEquals(1173292541, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 3 && ((i % 5) == 2 || (i % 5) == 4))
                            .collect(Collectors.toList())));
        }
    }

    @Test
    public void andOrQueryWithContinuation() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value_indexed").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(3),
                    Query.or(Query.field("num_value_3_indexed").equalsValue(1),
                             Query.field("num_value_3_indexed").equalsValue(4))));
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
    public void andOrQueryWithDuplicate() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value_indexed").equalsValue("odd"),
                    Query.or(
                            Query.and(
                                    Query.field("num_value_2").equalsValue(3),
                                    Query.field("num_value_3_indexed").equalsValue(0)),
                            Query.and(
                                    Query.field("num_value_2").equalsValue(3),
                                    Query.field("num_value_3_indexed").equalsValue(4)))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND [1] BITOR [0] BITAND [2]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 3],[odd, 3]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 0],[odd, 0]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 4],[odd, 4]]"))))))));
            assertEquals(1788540340, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> ((i % 7) == 3 && (i % 5) == 0) || ((i % 7) == 3 && (i % 5) == 4))
                            .collect(Collectors.toList())));
        }
    }

    @Test
    public void andNotQuery() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value_indexed").equalsValue("odd"),
                    Query.field("num_value_2").equalsValue(1),
                    Query.not(Query.field("num_value_3_indexed").equalsValue(2))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITAND BITNOT [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 1],[odd, 1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 2],[odd, 2]]"))))))));
            assertEquals(1339577551, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 7) == 1 && !((i % 5) == 2))
                            .collect(Collectors.toList())));
        }
    }

    @Test
    public void nonOverlappingOrQuery() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            for (int recNo = 100; recNo < 200; recNo++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(recNo)
                        .setStrValueIndexed((recNo & 1) == 1 ? "odd" : "even")
                        .setNumValue2(1)
                        .build());
            }
            for (int recNo = 500; recNo < 600; recNo++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(recNo)
                        .setStrValueIndexed((recNo & 1) == 1 ? "odd" : "even")
                        .setNumValue3Indexed(1)
                        .build());
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUMS_HOOK);
            setupPlanner(null);
            final RecordQueryPlan queryPlan = plan(BITMAP_VALUE_REC_NO_BY_STR, Query.and(
                    Query.field("str_value_indexed").equalsValue("odd"),
                    Query.or(Query.field("num_value_2").equalsValue(1),
                             Query.field("num_value_3_indexed").equalsValue(1))));
            assertThat(queryPlan, compositeBitmap(hasToString("[0] BITOR [1]"), Arrays.asList(
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num2"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 1],[odd, 1]]"))))),
                    coveringIndexScan(indexScan(allOf(indexName("rec_no_by_str_num3"), indexScanType(IndexScanType.BY_GROUP), bounds(hasTupleString("[[odd, 1],[odd, 1]]"))))))));
            assertEquals(-556720460, queryPlan.planHash());
            assertThat(
                    collectOnBits(queryPlan.execute(recordStore).map(FDBQueriedRecord::getIndexEntry)),
                    equalTo(IntStream.concat(IntStream.range(100, 200), IntStream.range(500, 600)).boxed()
                            .filter(i -> (i & 1) == 1)
                            .collect(Collectors.toList())));
        }
    }

    protected static final KeyExpression REC_NO_BY_STR = concatenateFields("str_value_indexed", "rec_no").group(1);
    protected static final KeyExpression REC_NO_BY_STR_NUM2 = concatenateFields("str_value_indexed", "num_value_2", "rec_no").group(1);
    protected static final KeyExpression REC_NO_BY_STR_NUM3 = concatenateFields("str_value_indexed", "num_value_3_indexed", "rec_no").group(1);
    protected static final Map<String, String> SMALL_BITMAP_OPTIONS = Collections.singletonMap(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16");
    protected static final RecordMetaDataHook REC_NO_BY_STR_NUMS_HOOK = metadata -> {
        final RecordTypeBuilder recordType = metadata.getRecordType("MySimpleRecord");
        metadata.addIndex(recordType, new Index("rec_no_by_str_num2", REC_NO_BY_STR_NUM2, IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        metadata.addIndex(recordType, new Index("rec_no_by_str_num3", REC_NO_BY_STR_NUM3, IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
    };
    protected static final IndexAggregateFunction BITMAP_VALUE_REC_NO_BY_STR = new IndexAggregateFunction(FunctionNames.BITMAP_VALUE, REC_NO_BY_STR, null);

    protected void saveRecords(int start, int end) {
        for (int recNo = start; recNo < end; recNo++) {
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(recNo)
                    .setStrValueIndexed((recNo & 1) == 1 ? "odd" : "even")
                    .setNumValueUnique(recNo + 1000)
                    .setNumValue2(recNo % 7)
                    .setNumValue3Indexed(recNo % 5)
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

    protected RecordQueryPlan plan(@Nonnull IndexAggregateFunction function, @Nonnull QueryComponent filter) {
        final RecordQuery recordQuery = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(filter)
                .setRequiredResults(Collections.singletonList(Key.Expressions.field("rec_no")))
                .build();
        return ComposedBitmapIndexAggregate.tryPlan((RecordQueryPlanner)planner, recordQuery, function)
                .orElseThrow(() -> new IllegalStateException("Cannot plan query"));
    }

}
