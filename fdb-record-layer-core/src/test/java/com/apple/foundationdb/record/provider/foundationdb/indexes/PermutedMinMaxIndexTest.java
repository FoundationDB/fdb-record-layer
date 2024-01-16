/*
 * PermutedMinMaxIndexTest.java
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
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for permuted min / max type indexes.
 */
@Tag(Tags.RequiresFDB)
public class PermutedMinMaxIndexTest extends FDBRecordStoreTestBase {

    protected static final String INDEX_NAME = "permuted";

    protected static RecordMetaDataHook hook(boolean min) {
        return md -> {
            md.addIndex("MySimpleRecord", new Index(INDEX_NAME,
                    Key.Expressions.concatenateFields("str_value_indexed", "num_value_2", "num_value_3_indexed").group(1),
                    min ? IndexTypes.PERMUTED_MIN : IndexTypes.PERMUTED_MAX,
                    Collections.singletonMap(IndexOptions.PERMUTED_SIZE_OPTION, "1")));
        };
    }

    @Test
    public void min() {
        final RecordMetaDataHook hook = hook(true);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 222, 200);
            saveRecord(99, "no", 66, 0);

            assertEquals(Arrays.asList(
                    Tuple.from(100, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(3, "yes", 111, 50);
            assertEquals(Arrays.asList(
                    Tuple.from(50, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            saveRecord(3, "yes", 111, 150);
            assertEquals(Arrays.asList(
                    Tuple.from(100, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(150, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));
        }
    }

    @Test
    public void max() {
        final RecordMetaDataHook hook = hook(false);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 222, 200);
            saveRecord(99, "no", 666, 0);

            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(100, 111)
            ), scanGroup(Tuple.from("yes"), true));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(3, "yes", 111, 250);
            assertEquals(Arrays.asList(
                    Tuple.from(250, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), true));

            saveRecord(3, "yes", 111, 50);
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(100, 111)
            ), scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(50, 111)
            ), scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), true));
        }
    }

    @Test
    public void tie() {
        final RecordMetaDataHook hook = hook(false);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 111, 50);
            saveRecord(3, "yes", 111, 100);

            assertEquals(Arrays.asList(
                    Tuple.from(100, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(100, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(50, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));
        }
    }

    @ParameterizedTest(name = "evaluateAggregateFunction[min={0}]")
    @BooleanSource
    void evaluateAggregateFunction(boolean min) {
        final String functionName = min ? FunctionNames.MIN : FunctionNames.MAX;
        final List<String> recordTypeNames = List.of("MySimpleRecord");
        final RecordMetaDataHook hook = hook(min);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 1, 1);
            saveRecord(2, "yes", 1, 2);
            saveRecord(3, "yes", 1, 3);
            saveRecord(4, "yes", 2, 4);
            saveRecord(5, "yes", 2, 5);
            saveRecord(6, "yes", 2, 6);
            saveRecord(7, "no", 3, 7);
            saveRecord(8, "no", 3, 8);
            saveRecord(9, "no", 3, 9);
            saveRecord(10, "no", 4, 10);
            saveRecord(11, "no", 4, 11);
            saveRecord(12, "no", 4, 12);

            // Evaluate across the complete index
            final IndexAggregateFunction ungrouped = new IndexAggregateFunction(
                    functionName,
                    Key.Expressions.field("num_value_3_indexed").ungrouped(),
                    INDEX_NAME);
            Tuple ungroupedExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, ungrouped, Key.Evaluated.EMPTY, IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(1L) : Tuple.from(12L), ungroupedExtremum);

            // Evaluate across the first column of the grouping key. This requires rolling up values across the different groups
            IndexAggregateFunction stringValueGrouped = new IndexAggregateFunction(
                    functionName,
                    Key.Expressions.field("num_value_3_indexed").groupBy(Key.Expressions.field("str_value_indexed")),
                    INDEX_NAME);
            Tuple yesExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, stringValueGrouped, Key.Evaluated.scalar("yes"), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(1L) : Tuple.from(6L), yesExtremum);
            Tuple noExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, stringValueGrouped, Key.Evaluated.scalar("no"), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(7L) : Tuple.from(12L), noExtremum);
            Tuple maybeExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, stringValueGrouped, Key.Evaluated.scalar("maybe"), IsolationLevel.SERIALIZABLE).join();
            assertNull(maybeExtremum);

            // Evaluate when limited to a single group. This requires scanning through the keys of the larger group and filtering out unrelated groups
            IndexAggregateFunction mostSpecificallyGrouped = new IndexAggregateFunction(
                    functionName,
                    Key.Expressions.field("num_value_3_indexed").groupBy(Key.Expressions.concatenateFields("str_value_indexed", "num_value_2")),
                    INDEX_NAME);
            Tuple yes0Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("yes", 0L), IsolationLevel.SERIALIZABLE).join();
            assertNull(yes0Extremum);
            Tuple yes1Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("yes", 1L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(1L) : Tuple.from(3L), yes1Extremum);
            Tuple yes2Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("yes", 2L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(4L) : Tuple.from(6L), yes2Extremum);
            Tuple no3Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("no", 3L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(7L) : Tuple.from(9L), no3Extremum);
            Tuple no4Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("no", 4L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(10L) : Tuple.from(12L), no4Extremum);
            Tuple no5Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("no", 5L), IsolationLevel.SERIALIZABLE).join();
            assertNull(no5Extremum);

            commit(context);
        }
    }

    @ParameterizedTest(name = "coveringIndexScan[min={0}]")
    @BooleanSource
    void coveringIndexScan(boolean min) {
        final String functionName = min ? FunctionNames.MIN : FunctionNames.MAX;
        final RecordMetaDataHook hook = hook(min);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String strValueParam = "str_value";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("str_value_indexed").equalsParameter(strValueParam))
                    .setRequiredResults(List.of(Key.Expressions.field("num_value_2")))
                    .build();
            RecordQueryPlan plan = ((RecordQueryPlanner)planner).planCoveringAggregateIndex(query, INDEX_NAME);
            assertNotNull(plan);
            assertTrue(plan.hasIndexScan(INDEX_NAME));

            saveRecord(1, "yes", 1, 1);
            saveRecord(2, "yes", 1, 2);
            saveRecord(3, "yes", 1, 3);
            saveRecord(4, "yes", 2, 4);
            saveRecord(5, "yes", 2, 5);
            saveRecord(6, "yes", 2, 6);
            saveRecord(7, "no", 3, 7);
            saveRecord(8, "no", 3, 8);
            saveRecord(9, "no", 3, 9);
            saveRecord(10, "no", 4, 10);
            saveRecord(11, "no", 4, 11);
            saveRecord(12, "no", 4, 12);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            List<Pair<Long, Long>> results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "yes"));
            assertThat(results, hasSize(2));
            assertThat(results, containsInAnyOrder(Pair.of(1L, min ? 1L : 3L), Pair.of(2L, min ? 4L : 6L)));

            results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "no"));
            assertThat(results, hasSize(2));
            assertThat(results, containsInAnyOrder(Pair.of(3L, min ? 7L : 9L), Pair.of(4L, min ? 10L : 12L)));

            results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "maybe"));
            assertThat(results, empty());

            commit(context);
        }
    }

    @Nonnull
    private List<Pair<Long, Long>> executePermutedIndexScan(@Nonnull RecordQueryPlan plan, @Nonnull Index index, @Nullable EvaluationContext evaluationContext) {
        return plan.execute(recordStore, evaluationContext == null ? EvaluationContext.EMPTY : evaluationContext)
                .map(rec -> {
                    assertEquals(index, rec.getIndex());
                    TestRecords1Proto.MySimpleRecord simple = TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build();
                    assertTrue(simple.hasNumValue2());
                    long numValue2 = simple.getNumValue2();
                    Tuple indexKey = rec.getIndexEntry().getKey();
                    assertNotNull(indexKey);
                    assertEquals(numValue2, indexKey.getLong(2));
                    return Pair.of(numValue2, indexKey.getLong(1));
                })
                .asList()
                .join();
    }

    @Test
    public void deleteWhere() {
        final RecordMetaDataHook hook = md -> {
            final KeyExpression pkey = Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed", "rec_no");
            md.getRecordType("MySimpleRecord").setPrimaryKey(pkey);
            md.getRecordType("MyOtherRecord").setPrimaryKey(pkey);
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.removeIndex("MySimpleRecord$num_value_3_indexed");
            md.removeIndex("MySimpleRecord$num_value_unique");
            md.removeIndex(COUNT_INDEX.getName());
            md.removeIndex(COUNT_UPDATES_INDEX.getName());
            md.addIndex("MySimpleRecord", new Index(INDEX_NAME,
                    Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed", "str_value_indexed", "num_value_unique").group(1),
                    IndexTypes.PERMUTED_MAX, Collections.singletonMap(IndexOptions.PERMUTED_SIZE_OPTION, "2")));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(100, "yes", 1, 1);
            saveRecord(150, "yes", 1, 1);
            saveRecord(200, "no", 1, 1);
            saveRecord(300, "yes", 1, 2);
            saveRecord(400, "no", 1, 2);
            saveRecord(500, "maybe", 2, 1);

            assertEquals(Arrays.asList(
                    Tuple.from(1, 150, 1, "yes"),
                    Tuple.from(1, 200, 1, "no"),
                    Tuple.from(1, 300, 2, "yes"),
                    Tuple.from(1, 400, 2, "no"),
                    Tuple.from(2, 500, 1, "maybe")
            ), scanGroup(Tuple.from(), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.deleteRecordsWhere(Query.field("num_value_2").equalsValue(2));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 150, 1, "yes"),
                    Tuple.from(1, 200, 1, "no"),
                    Tuple.from(1, 300, 2, "yes"),
                    Tuple.from(1, 400, 2, "no")
            ), scanGroup(Tuple.from(), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertThrows(Query.InvalidExpressionException.class, () -> {
                recordStore.deleteRecordsWhere(Query.and(
                        Query.field("num_value_2").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(1)));
            });
        }
    }

    private void saveRecord(int recNo, String strValue, int value2, int value3) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed(strValue)
                .setNumValue2(value2)
                .setNumValue3Indexed(value3)
                .setNumValueUnique(recNo)
                .build());
    }

    private List<Tuple> scanGroup(Tuple group, boolean reverse) {
        return recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(INDEX_NAME), IndexScanType.BY_GROUP,
                TupleRange.allOf(group), null, reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN)
                .map(entry -> TupleHelpers.subTuple(entry.getKey(), group.size(), entry.getKeySize()))
                .asList()
                .join();
    }

}
