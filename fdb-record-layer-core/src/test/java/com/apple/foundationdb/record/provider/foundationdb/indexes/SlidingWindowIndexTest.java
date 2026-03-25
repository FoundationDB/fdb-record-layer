/*
 * SlidingWindowIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.SlidingWindowKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for sliding window index maintainer.
 *
 * <p>Records are created with {@link #rec(int, int, int)} and named by convention:
 * {@code rec<recNo>_<windowKeyValue>}. For example, {@code rec(1, 10, 100)} is called
 * {@code rec1_100} because its window key (num_value_3_indexed) is 100.
 * For multi-column window key tests, the name uses both fields:
 * {@code rec1_10_100} for window key (num_value_2=10, num_value_3_indexed=100).
 * The assertion helper {@link #assertWindowContains(long...)} verifies exactly which
 * records (by recNo) are present in the index.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowIndexTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sliding_window_index";

    /**
     * Single-column window key: window key = num_value_3_indexed, whole key = num_value_2.
     * Records named rec{recNo}_{num_value_3_indexed}.
     */
    @Nonnull
    private static RecordMetaDataHook hook(int windowSize, @Nonnull String order) {
        final KeyExpression wholeKey = Key.Expressions.field("num_value_2");
        final KeyExpression windowKey = Key.Expressions.field("num_value_3_indexed");
        return hookWith(wholeKey, windowKey, windowSize, order);
    }

    /**
     * Multi-column window key: window key = (num_value_2, num_value_3_indexed), whole key = num_value_unique.
     * Records named rec{recNo}_{num_value_2}_{num_value_3_indexed}.
     */
    @Nonnull
    private static RecordMetaDataHook multiColumnWindowHook(int windowSize, @Nonnull String order) {
        final KeyExpression wholeKey = Key.Expressions.field("num_value_unique");
        final KeyExpression windowKey = Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed");
        return hookWith(wholeKey, windowKey, windowSize, order);
    }

    @Nonnull
    private static RecordMetaDataHook hookWith(@Nonnull KeyExpression wholeKey,
                                               @Nonnull KeyExpression windowKey,
                                               int windowSize,
                                               @Nonnull String order) {
        final SlidingWindowKeyExpression slidingWindow = new SlidingWindowKeyExpression(wholeKey, windowKey);
        final Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.SLIDING_WINDOW_SIZE_OPTION, Integer.toString(windowSize));
        options.put(IndexOptions.SLIDING_WINDOW_ORDER_OPTION, order);
        return md -> {
            md.addIndex("MySimpleRecord", new Index(INDEX_NAME, slidingWindow, "value", options));
        };
    }

    private void rec(int recNo, int value2, int value3) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed("s" + recNo)
                .setNumValue2(value2)
                .setNumValue3Indexed(value3)
                .setNumValueUnique(recNo)
                .build());
    }

    private void deleteRec(int recNo) {
        recordStore.deleteRecord(Tuple.from((long) recNo));
    }

    private void assertWindowContains(long... expectedRecNos) {
        final Set<Long> actual = recordStore
                .scanIndex(recordStore.getRecordMetaData().getIndex(INDEX_NAME),
                        IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                .asList()
                .join()
                .stream()
                .map(e -> e.getPrimaryKey().getLong(0))
                .collect(Collectors.toSet());
        final Set<Long> expected = java.util.Arrays.stream(expectedRecNos)
                .boxed()
                .collect(Collectors.toSet());
        assertEquals(expected, actual,
                "Window should contain recNos " + expected + " but was " + actual);
    }

    @Test
    void maxInsertBelowWindowSize() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(5, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300

            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void maxEvictsLowest() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300
            assertWindowContains(1, 2, 3);

            rec(4, 40, 400);  // rec4_400 → evicts rec1_100 (lowest)
            assertWindowContains(2, 3, 4);

            commit(context);
        }
    }

    @Test
    void maxSkipsLowerValue() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300

            rec(4, 40, 50);   // rec4_50 → worse than worst (rec1_100) → skipped
            assertWindowContains(1, 2, 3);

            commit(context);
        }
    }

    @Test
    void maxMultipleEvictions() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            assertWindowContains(1, 2);

            rec(3, 30, 300);  // rec3_300 → evicts rec1_100
            assertWindowContains(2, 3);

            rec(4, 40, 400);  // rec4_400 → evicts rec2_200
            assertWindowContains(3, 4);

            rec(5, 50, 500);  // rec5_500 → evicts rec3_300
            assertWindowContains(4, 5);

            commit(context);
        }
    }

    @Test
    void minInsertBelowWindowSize() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(5, "MIN"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300

            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void minEvictsHighest() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, "MIN"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300
            assertWindowContains(1, 2, 3);

            rec(4, 40, 50);   // rec4_50 → evicts rec3_300 (highest)
            assertWindowContains(1, 2, 4);

            commit(context);
        }
    }

    @Test
    void minSkipsHigherValue() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, "MIN"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300

            rec(4, 40, 400);  // rec4_400 → worse than worst (rec3_300) → skipped
            assertWindowContains(1, 2, 3);

            commit(context);
        }
    }

    @Test
    void minMultipleEvictions() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, "MIN"));

            rec(1, 10, 300);  // rec1_300
            rec(2, 20, 200);  // rec2_200
            assertWindowContains(1, 2);

            rec(3, 30, 100);  // rec3_100 → evicts rec1_300
            assertWindowContains(2, 3);

            rec(4, 40, 50);   // rec4_50 → evicts rec2_200
            assertWindowContains(3, 4);

            rec(5, 50, 10);   // rec5_10 → evicts rec3_100
            assertWindowContains(4, 5);

            commit(context);
        }
    }

    @Test
    void deleteRecordInWindow() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(5, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300
            assertWindowContains(1, 2, 3);

            deleteRec(2);      // remove rec2_200
            assertWindowContains(1, 3);

            commit(context);
        }
    }

    @Test
    void deleteRecordNotInWindow() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 50);   // rec3_50 → skipped (MAX, worse than rec1_100)
            assertWindowContains(1, 2);

            deleteRec(3);      // rec3_50 was never in window → no-op
            assertWindowContains(1, 2);

            commit(context);
        }
    }

    @Test
    void deleteAndReinsert() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            assertWindowContains(1, 2);

            deleteRec(1);      // remove rec1_100 → count drops to 1
            assertWindowContains(2);

            rec(3, 30, 300);  // rec3_300 → window has space
            assertWindowContains(2, 3);

            commit(context);
        }
    }

    @Test
    void deleteAllAndRefill() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            assertWindowContains(1, 2);

            deleteRec(1);
            deleteRec(2);
            assertWindowContains();  // empty

            rec(3, 30, 300);  // rec3_300
            rec(4, 40, 400);  // rec4_400
            assertWindowContains(3, 4);

            commit(context);
        }
    }

    @Test
    void deleteFromFullWindowThenInsertLow() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300
            assertWindowContains(1, 2, 3);

            deleteRec(2);      // remove rec2_200 → count = 2
            assertWindowContains(1, 3);

            // Window is below capacity → even a low window key enters
            rec(4, 40, 50);   // rec4_50 → fills the gap
            assertWindowContains(1, 3, 4);

            commit(context);
        }
    }

    @Test
    void multiColumnMaxEvictsLowest() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, multiColumnWindowHook(2, "MAX"));

            rec(1, 10, 100);  // rec1_10_100
            rec(2, 20, 200);  // rec2_20_200
            assertWindowContains(1, 2);

            rec(3, 30, 300);  // rec3_30_300 → evicts rec1_10_100
            assertWindowContains(2, 3);

            commit(context);
        }
    }

    @Test
    void multiColumnMinEvictsHighest() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, multiColumnWindowHook(2, "MIN"));

            rec(1, 10, 100);  // rec1_10_100
            rec(2, 20, 200);  // rec2_20_200
            assertWindowContains(1, 2);

            rec(3, 5, 50);    // rec3_5_50 → evicts rec2_20_200
            assertWindowContains(1, 3);

            commit(context);
        }
    }

    @Test
    void multiColumnSkipsWhenNotBetter() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, multiColumnWindowHook(2, "MAX"));

            rec(1, 20, 200);  // rec1_20_200
            rec(2, 30, 300);  // rec2_30_300
            assertWindowContains(1, 2);

            rec(3, 10, 100);  // rec3_10_100 → worse than rec1_20_200 → skipped
            assertWindowContains(1, 2);

            commit(context);
        }
    }

    @Test
    void multiColumnSecondFieldBreaksTie() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, multiColumnWindowHook(2, "MAX"));

            rec(1, 10, 100);  // rec1_10_100
            rec(2, 10, 200);  // rec2_10_200
            assertWindowContains(1, 2);

            // Same first field (10), second field 300 > 100 → evicts rec1_10_100
            rec(3, 10, 300);  // rec3_10_300
            assertWindowContains(2, 3);

            commit(context);
        }
    }

    @Test
    void windowSizeOne() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(1, "MAX"));

            rec(1, 10, 100);  // rec1_100
            assertWindowContains(1);

            rec(2, 20, 200);  // rec2_200 → evicts rec1_100
            assertWindowContains(2);

            rec(3, 30, 300);  // rec3_300 → evicts rec2_200
            assertWindowContains(3);

            rec(4, 40, 50);   // rec4_50 → worse than rec3_300 → skipped
            assertWindowContains(3);

            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValues() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 100);  // rec2_100
            rec(3, 30, 100);  // rec3_100
            assertWindowContains(1, 2, 3);

            rec(4, 40, 200);  // rec4_200 → evicts one of the _100s
            Set<Long> window = scanIndexRecNos();
            assertEquals(3, window.size());
            assertTrue(window.contains(4L), "rec4_200 should be in window");

            commit(context);
        }
    }

    @Test
    void exactBoundaryValue() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, "MAX"));

            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200

            // rec3_100 has same window key as worst (rec1_100) → NOT evicted (strict >)
            rec(3, 30, 100);
            assertWindowContains(1, 2);

            commit(context);
        }
    }

    @Test
    void rebuildIndexMax() {
        final RecordMetaDataHook hook = hook(3, "MAX");
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            rec(1, 10, 100);  // rec1_100
            rec(2, 20, 200);  // rec2_200
            rec(3, 30, 300);  // rec3_300
            rec(4, 40, 400);  // rec4_400
            rec(5, 50, 50);   // rec5_50
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.rebuildAllIndexes().join();
            // Rebuild in pk order: rec1_100, rec2_200, rec3_300, rec4_400, rec5_50
            // MAX window=3: ends with {rec2_200, rec3_300, rec4_400}
            assertWindowContains(2, 3, 4);
            commit(context);
        }
    }

    @Test
    void rebuildIndexMin() {
        final RecordMetaDataHook hook = hook(3, "MIN");
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            rec(1, 10, 500);  // rec1_500
            rec(2, 20, 400);  // rec2_400
            rec(3, 30, 300);  // rec3_300
            rec(4, 40, 200);  // rec4_200
            rec(5, 50, 100);  // rec5_100
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.rebuildAllIndexes().join();
            // Rebuild in pk order: rec1_500, rec2_400, rec3_300, rec4_200, rec5_100
            // MIN window=3: ends with {rec3_300, rec4_200, rec5_100}
            assertWindowContains(3, 4, 5);
            commit(context);
        }
    }

    @Test
    void protoSerializationRoundTrip() {
        final KeyExpression wholeKey = Key.Expressions.concatenateFields("num_value_2", "str_value_indexed");
        final KeyExpression windowKey = Key.Expressions.field("num_value_3_indexed");
        final SlidingWindowKeyExpression original = new SlidingWindowKeyExpression(wholeKey, windowKey);

        final RecordKeyExpressionProto.KeyExpression proto = original.toKeyExpression();
        assertTrue(proto.hasSlidingWindow());

        final KeyExpression deserialized = KeyExpression.fromProto(proto);
        assertTrue(deserialized instanceof SlidingWindowKeyExpression);
        final SlidingWindowKeyExpression deserializedSW = (SlidingWindowKeyExpression) deserialized;
        assertEquals(original.getWholeKey(), deserializedSW.getWholeKey());
        assertEquals(original.getWindowKey(), deserializedSW.getWindowKey());
        assertEquals(original, deserializedSW);
    }

    @Test
    void protoSerializationMultiColumnWindowKey() {
        final KeyExpression wholeKey = Key.Expressions.field("num_value_unique");
        final KeyExpression windowKey = Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed");
        final SlidingWindowKeyExpression original = new SlidingWindowKeyExpression(wholeKey, windowKey);

        final RecordKeyExpressionProto.KeyExpression proto = original.toKeyExpression();
        assertTrue(proto.hasSlidingWindow());

        final KeyExpression deserialized = KeyExpression.fromProto(proto);
        assertTrue(deserialized instanceof SlidingWindowKeyExpression);
        final SlidingWindowKeyExpression deserializedSW = (SlidingWindowKeyExpression) deserialized;
        assertEquals(original.getWholeKey(), deserializedSW.getWholeKey());
        assertEquals(original.getWindowKey(), deserializedSW.getWindowKey());
        assertEquals(original, deserializedSW);
    }

    @Nonnull
    private Set<Long> scanIndexRecNos() {
        return recordStore
                .scanIndex(recordStore.getRecordMetaData().getIndex(INDEX_NAME),
                        IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                .asList()
                .join()
                .stream()
                .map(e -> e.getPrimaryKey().getLong(0))
                .collect(Collectors.toSet());
    }
}
