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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for sliding window index maintainer using {@code RowNumberWindow} predicate.
 *
 * <p>Records are created with {@link #rec(int, int, int)} and named by convention:
 * {@code rec<recNo>_<windowKeyValue>}. For example, {@code rec(1, 10, 100)} is called
 * {@code rec1_100} because its window key (num_value_3_indexed) is 100.
 * The assertion helper {@link #assertWindowContains(long...)} verifies exactly which
 * records (by recNo) are present in the index.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowIndexTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sliding_window_index";

    /**
     * Creates a hook with: RowNumberWindow(num_value_3_indexed, direction) <= windowSize.
     * Index root expression = num_value_2.
     */
    @Nonnull
    private static RecordMetaDataHook hook(int windowSize, @Nonnull Direction direction) {
        final KeyExpression wholeKey = Key.Expressions.field("num_value_2");
        final IndexPredicate.RowNumberWindowPredicate predicate =
                new IndexPredicate.RowNumberWindowPredicate("num_value_3_indexed", direction, windowSize);
        return md -> md.addIndex("MySimpleRecord",
                new Index(INDEX_NAME, wholeKey, "value", IndexOptions.EMPTY_OPTIONS, predicate));
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
        final Set<Long> actual = scanIndexRecNos();
        final Set<Long> expected = java.util.Arrays.stream(expectedRecNos)
                .boxed()
                .collect(Collectors.toSet());
        assertEquals(expected, actual,
                "Window should contain recNos " + expected + " but was " + actual);
    }

    // ===== DESC tests (keep highest = old MAX) =====

    @Test
    void descInsertBelowWindowSize() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(5, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);

            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void descEvictsLowest() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);
            assertWindowContains(1, 2, 3);

            rec(4, 40, 400);  // evicts rec1_100 (lowest)
            assertWindowContains(2, 3, 4);

            commit(context);
        }
    }

    @Test
    void descSkipsLowerValue() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);

            rec(4, 40, 50);   // worse than worst (rec1_100) → overflow
            assertWindowContains(1, 2, 3);

            commit(context);
        }
    }

    @Test
    void descMultipleEvictions() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            assertWindowContains(1, 2);

            rec(3, 30, 300);  // evicts rec1_100
            assertWindowContains(2, 3);

            rec(4, 40, 400);  // evicts rec2_200
            assertWindowContains(3, 4);

            rec(5, 50, 500);  // evicts rec3_300
            assertWindowContains(4, 5);

            commit(context);
        }
    }

    // ===== ASC tests (keep lowest = old MIN) =====

    @Test
    void ascInsertBelowWindowSize() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(5, Direction.ASC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);

            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void ascEvictsHighest() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, Direction.ASC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);
            assertWindowContains(1, 2, 3);

            rec(4, 40, 50);   // evicts rec3_300 (highest)
            assertWindowContains(1, 2, 4);

            commit(context);
        }
    }

    @Test
    void ascSkipsHigherValue() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, Direction.ASC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);

            rec(4, 40, 400);  // worse than worst (rec3_300) → overflow
            assertWindowContains(1, 2, 3);

            commit(context);
        }
    }

    @Test
    void ascMultipleEvictions() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.ASC));

            rec(1, 10, 300);
            rec(2, 20, 200);
            assertWindowContains(1, 2);

            rec(3, 30, 100);  // evicts rec1_300
            assertWindowContains(2, 3);

            rec(4, 40, 50);   // evicts rec2_200
            assertWindowContains(3, 4);

            rec(5, 50, 10);   // evicts rec3_100
            assertWindowContains(4, 5);

            commit(context);
        }
    }

    // ===== Delete tests =====

    @Test
    void deleteRecordInWindow() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(5, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);
            assertWindowContains(1, 2, 3);

            deleteRec(2);
            assertWindowContains(1, 3);

            commit(context);
        }
    }

    @Test
    void deleteRecordNotInWindowOrOverflow() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(5, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            assertWindowContains(1, 2);

            deleteRec(1);
            assertWindowContains(2);

            commit(context);
        }
    }

    @Test
    void deleteAndReinsert() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            assertWindowContains(1, 2);

            deleteRec(1);
            assertWindowContains(2);

            rec(3, 30, 300);
            assertWindowContains(2, 3);

            commit(context);
        }
    }

    @Test
    void deleteAllAndRefill() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            assertWindowContains(1, 2);

            deleteRec(1);
            deleteRec(2);
            assertWindowContains();

            rec(3, 30, 300);
            rec(4, 40, 400);
            assertWindowContains(3, 4);

            commit(context);
        }
    }

    @Test
    void deleteFromFullWindowThenInsertLow() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);
            assertWindowContains(1, 2, 3);

            deleteRec(2);
            assertWindowContains(1, 3);

            rec(4, 40, 50);   // window below capacity → enters
            assertWindowContains(1, 3, 4);

            commit(context);
        }
    }

    // ===== Re-election tests =====

    @Test
    void deleteFromWindowPromotesOverflowDesc() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);  // window
            rec(2, 20, 200);  // window
            rec(3, 30, 50);   // overflow (worse for DESC)
            assertWindowContains(1, 2);

            deleteRec(1);      // promotes rec3_50 from overflow
            assertWindowContains(2, 3);

            commit(context);
        }
    }

    @Test
    void deleteFromWindowPromotesOverflowAsc() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.ASC));

            rec(1, 10, 100);  // window
            rec(2, 20, 200);  // window
            rec(3, 30, 300);  // overflow (worse for ASC)
            assertWindowContains(1, 2);

            deleteRec(2);      // promotes rec3_300 from overflow
            assertWindowContains(1, 3);

            commit(context);
        }
    }

    @Test
    void deleteFromOverflowNoChange() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);  // window
            rec(2, 20, 200);  // window
            rec(3, 30, 50);   // overflow
            assertWindowContains(1, 2);

            deleteRec(3);      // overflow delete → window unchanged
            assertWindowContains(1, 2);

            commit(context);
        }
    }

    @Test
    void cascadingReElectionDesc() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);  // window
            rec(2, 20, 200);  // window
            rec(3, 30, 50);   // overflow
            rec(4, 40, 30);   // overflow
            assertWindowContains(1, 2);

            deleteRec(1);
            assertWindowContains(2, 3);

            deleteRec(2);
            assertWindowContains(3, 4);

            commit(context);
        }
    }

    @Test
    void cascadingReElectionAsc() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.ASC));

            rec(1, 10, 100);  // window
            rec(2, 20, 200);  // window
            rec(3, 30, 300);  // overflow
            rec(4, 40, 400);  // overflow
            assertWindowContains(1, 2);

            deleteRec(2);
            assertWindowContains(1, 3);

            deleteRec(3);
            assertWindowContains(1, 4);

            commit(context);
        }
    }

    @Test
    void reElectionWithEmptyOverflow() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            assertWindowContains(1, 2);

            deleteRec(1);
            assertWindowContains(2);

            commit(context);
        }
    }

    // ===== Edge cases =====

    @Test
    void windowSizeOne() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(1, Direction.DESC));

            rec(1, 10, 100);
            assertWindowContains(1);

            rec(2, 20, 200);  // evicts rec1_100
            assertWindowContains(2);

            rec(3, 30, 300);  // evicts rec2_200
            assertWindowContains(3);

            rec(4, 40, 50);   // worse → overflow
            assertWindowContains(3);

            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValues() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 100);
            rec(3, 30, 100);
            assertWindowContains(1, 2, 3);

            rec(4, 40, 200);  // evicts one of the _100s
            Set<Long> window = scanIndexRecNos();
            assertEquals(3, window.size());
            assertTrue(window.contains(4L), "rec4_200 should be in window");

            commit(context);
        }
    }

    @Test
    void exactBoundaryValue() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);

            // Same window key as worst → NOT better (strict) → overflow
            rec(3, 30, 100);
            assertWindowContains(1, 2);

            commit(context);
        }
    }

    // ===== deleteWhere tests =====
    // deleteWhere requires a partitioned sliding window. The partition prefix comes from
    // the PARTITION BY clause in the RowNumberWindowPredicate. deleteWhere(prefix) clears
    // the entire partition group from both the sliding window subspace and the delegate.

    /**
     * Hook with a partitioned window: PARTITION BY num_value_2, ORDER BY num_value_3_indexed.
     * Each distinct num_value_2 value gets its own window of the given size.
     * Index root expression = num_value_2.
     */
    @Nonnull
    private static RecordMetaDataHook partitionedHook(int windowSize, @Nonnull Direction direction) {
        final KeyExpression wholeKey = Key.Expressions.field("num_value_2");
        final IndexPredicate.RowNumberWindowPredicate predicate =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("num_value_3_indexed"), direction, windowSize,
                        ImmutableList.of(ImmutableList.of("num_value_2")));
        return md -> md.addIndex("MySimpleRecord",
                new Index(INDEX_NAME, wholeKey, "value", IndexOptions.EMPTY_OPTIONS, predicate));
    }

    @Test
    void deleteWhereClearsEntirePartition() {
        try (FDBRecordContext context = openContext()) {
            // Each num_value_2 partition gets its own window of size 3, DESC
            openSimpleRecordStore(context, partitionedHook(3, Direction.DESC));

            // Partition 10: rec1, rec2 (window has room)
            rec(1, 10, 100);
            rec(2, 10, 200);
            // Partition 20: rec3, rec4
            rec(3, 20, 300);
            rec(4, 20, 400);
            assertWindowContains(1, 2, 3, 4);

            // deleteWhere(10) clears partition 10 entirely
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from(10L)).join();

            // Only partition 20 remains
            assertWindowContains(3, 4);
            commit(context);
        }
    }

    @Test
    void deleteWhereOnNonExistentPartition() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, partitionedHook(3, Direction.DESC));

            rec(1, 10, 100);
            rec(2, 20, 200);
            assertWindowContains(1, 2);

            // deleteWhere(999) — partition doesn't exist → no-op
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from(999L)).join();

            assertWindowContains(1, 2);
            commit(context);
        }
    }

    @Test
    void deleteWherePartitionWithOverflow() {
        try (FDBRecordContext context = openContext()) {
            // Each partition gets a window of 2, DESC
            openSimpleRecordStore(context, partitionedHook(2, Direction.DESC));

            // Partition 10: rec1(100), rec2(200) in window; rec5(50) in overflow
            rec(1, 10, 100);
            rec(2, 10, 200);
            rec(5, 10, 50);   // overflow for partition 10

            // Partition 20: rec3(300), rec4(400) in window
            rec(3, 20, 300);
            rec(4, 20, 400);
            assertWindowContains(1, 2, 3, 4);

            // deleteWhere(10) clears partition 10 — both window and overflow entries
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from(10L)).join();

            // Only partition 20 remains
            assertWindowContains(3, 4);
            commit(context);
        }
    }

    @Test
    void deleteWherePreservesOtherPartitions() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, partitionedHook(2, Direction.DESC));

            // Three partitions
            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);
            assertWindowContains(1, 2, 3);

            // Delete partition 20
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from(20L)).join();

            // Partitions 10 and 30 remain
            assertWindowContains(1, 3);

            // Can still insert into other partitions
            rec(5, 10, 500);
            assertWindowContains(1, 3, 5);

            commit(context);
        }
    }

    /**
     * Hook with a two-column partition: PARTITION BY (str_value_indexed, num_value_2),
     * ORDER BY num_value_3_indexed.
     * Index root expression = concat(str_value_indexed, num_value_2).
     */
    @Nonnull
    private static RecordMetaDataHook twoColumnPartitionHook(int windowSize, @Nonnull Direction direction) {
        final KeyExpression wholeKey = Key.Expressions.concat(
                Key.Expressions.field("str_value_indexed"),
                Key.Expressions.field("num_value_2"));
        final IndexPredicate.RowNumberWindowPredicate predicate =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("num_value_3_indexed"), direction, windowSize,
                        ImmutableList.of(
                                ImmutableList.of("str_value_indexed"),
                                ImmutableList.of("num_value_2")));
        return md -> md.addIndex("MySimpleRecord",
                new Index(INDEX_NAME, wholeKey, "value", IndexOptions.EMPTY_OPTIONS, predicate));
    }

    private void recWithStr(int recNo, String strVal, int value2, int value3) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed(strVal)
                .setNumValue2(value2)
                .setNumValue3Indexed(value3)
                .setNumValueUnique(recNo)
                .build());
    }

    @Test
    void twoColumnPartitionWindowAndDeleteWhere() {
        try (FDBRecordContext context = openContext()) {
            // Each (str_value_indexed, num_value_2) pair gets its own window of size 2, DESC
            openSimpleRecordStore(context, twoColumnPartitionHook(2, Direction.DESC));

            // Partition ("A", 10): rec1(v3=100), rec2(v3=200) — fills window
            recWithStr(1, "A", 10, 100);
            recWithStr(2, "A", 10, 200);
            // Partition ("A", 20): rec3(v3=300)
            recWithStr(3, "A", 20, 300);
            // Partition ("B", 10): rec4(v3=400), rec5(v3=500) — fills window
            recWithStr(4, "B", 10, 400);
            recWithStr(5, "B", 10, 500);
            // Partition ("A", 10): rec6(v3=50) — overflow (worse than rec1's 100 in DESC)
            recWithStr(6, "A", 10, 50);

            assertWindowContains(1, 2, 3, 4, 5);

            // deleteWhere with full partition prefix ("A", 10) — clears that partition only
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A", 10L)).join();

            // Partition ("A", 10) gone — rec1, rec2, rec6 all removed
            // Partitions ("A", 20) and ("B", 10) remain
            assertWindowContains(3, 4, 5);

            // deleteWhere with partial prefix ("A") — clears all partitions starting with "A"
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A")).join();

            // Only partition ("B", 10) remains
            assertWindowContains(4, 5);

            commit(context);
        }
    }

    // ===== Rebuild tests =====

    @Test
    void rebuildIndexDesc() {
        final RecordMetaDataHook hook = hook(3, Direction.DESC);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            rec(1, 10, 100);
            rec(2, 20, 200);
            rec(3, 30, 300);
            rec(4, 40, 400);
            rec(5, 50, 50);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.rebuildAllIndexes().join();
            // DESC window=3: keeps {rec2_200, rec3_300, rec4_400}
            assertWindowContains(2, 3, 4);
            commit(context);
        }
    }

    @Test
    void rebuildIndexAsc() {
        final RecordMetaDataHook hook = hook(3, Direction.ASC);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            rec(1, 10, 500);
            rec(2, 20, 400);
            rec(3, 30, 300);
            rec(4, 40, 200);
            rec(5, 50, 100);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.rebuildAllIndexes().join();
            // ASC window=3: keeps {rec3_300, rec4_200, rec5_100}
            assertWindowContains(3, 4, 5);
            commit(context);
        }
    }

    // ===== Serialization tests =====

    @Test
    void rowNumberWindowProtoRoundTrip() {
        final IndexPredicate.RowNumberWindowPredicate original =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 100);

        final RecordMetaDataProto.Predicate proto = original.toProto();
        assertTrue(proto.hasRowNumberWindowPredicate());
        assertEquals(100, proto.getRowNumberWindowPredicate().getSize());
        assertEquals("score", proto.getRowNumberWindowPredicate().getOrderingField(0));
        assertEquals(RecordMetaDataProto.RowNumberWindowPredicate.Direction.DESC,
                proto.getRowNumberWindowPredicate().getDirection());

        final IndexPredicate deserialized = IndexPredicate.fromProto(proto);
        assertTrue(deserialized instanceof IndexPredicate.RowNumberWindowPredicate);
        final IndexPredicate.RowNumberWindowPredicate deserializedP =
                (IndexPredicate.RowNumberWindowPredicate) deserialized;
        assertEquals(original.getOrderingField(), deserializedP.getOrderingField());
        assertEquals(original.getDirection(), deserializedP.getDirection());
        assertEquals(original.getSize(), deserializedP.getSize());
        assertEquals(original, deserializedP);
    }

    @Test
    void rowNumberWindowProtoRoundTripAsc() {
        final IndexPredicate.RowNumberWindowPredicate original =
                new IndexPredicate.RowNumberWindowPredicate("timestamp", Direction.ASC, 50);

        final RecordMetaDataProto.Predicate proto = original.toProto();
        assertTrue(proto.hasRowNumberWindowPredicate());
        assertEquals(RecordMetaDataProto.RowNumberWindowPredicate.Direction.ASC,
                proto.getRowNumberWindowPredicate().getDirection());

        final IndexPredicate deserialized = IndexPredicate.fromProto(proto);
        assertEquals(original, deserialized);
    }

    // ===== Validation tests =====

    @Test
    void qualifyInAndIsValid() {
        final IndexPredicate.RowNumberWindowPredicate qualify =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 10);
        final IndexPredicate.ConstantPredicate constant =
                new IndexPredicate.ConstantPredicate(IndexPredicate.ConstantPredicate.ConstantValue.TRUE);
        final IndexPredicate and = new IndexPredicate.AndPredicate(ImmutableList.of(qualify, constant));

        // Should not throw
        IndexPredicate.validateRowNumberWindowPlacement(and);
    }

    @Test
    void qualifyUnderOrIsInvalid() {
        final IndexPredicate.RowNumberWindowPredicate qualify =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 10);
        final IndexPredicate.ConstantPredicate constant =
                new IndexPredicate.ConstantPredicate(IndexPredicate.ConstantPredicate.ConstantValue.TRUE);
        final IndexPredicate or = new IndexPredicate.OrPredicate(ImmutableList.of(qualify, constant));

        org.junit.jupiter.api.Assertions.assertThrows(
                com.apple.foundationdb.record.RecordCoreException.class,
                () -> IndexPredicate.validateRowNumberWindowPlacement(or));
    }

    @Test
    void qualifyUnderAndInsideOrIsInvalid() {
        final IndexPredicate.RowNumberWindowPredicate qualify =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 10);
        final IndexPredicate.ConstantPredicate constant =
                new IndexPredicate.ConstantPredicate(IndexPredicate.ConstantPredicate.ConstantValue.TRUE);
        // AND(qualify) inside OR
        final IndexPredicate andWithQualify = new IndexPredicate.AndPredicate(ImmutableList.of(qualify));
        final IndexPredicate or = new IndexPredicate.OrPredicate(ImmutableList.of(andWithQualify, constant));

        org.junit.jupiter.api.Assertions.assertThrows(
                com.apple.foundationdb.record.RecordCoreException.class,
                () -> IndexPredicate.validateRowNumberWindowPlacement(or));
    }

    // ===== Vector index wrapping tests =====

    private static final String VECTOR_INDEX_NAME = "sliding_window_vector_index";
    private static final int VECTOR_DIMENSIONS = 4;

    private static HalfRealVector makeVector(float... values) {
        final Half[] components = new Half[values.length];
        for (int i = 0; i < values.length; i++) {
            components[i] = Half.valueOf(values[i]);
        }
        return new HalfRealVector(components);
    }

    private void openVectorRecordStore(FDBRecordContext context, int windowSize,
                                       @Nonnull Direction direction) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsVectorsProto.getDescriptor());
        metaDataBuilder.getRecordType("VectorRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields("group_id", "rec_no"));

        final IndexPredicate.RowNumberWindowPredicate predicate =
                new IndexPredicate.RowNumberWindowPredicate("group_id", direction, windowSize);

        final Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name());
        options.put(IndexOptions.HNSW_NUM_DIMENSIONS, Integer.toString(VECTOR_DIMENSIONS));

        metaDataBuilder.addIndex("VectorRecord",
                new Index(VECTOR_INDEX_NAME,
                        new KeyWithValueExpression(Key.Expressions.field("vector_data"), 0),
                        IndexTypes.VECTOR, options, predicate));

        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    private void saveVectorRecord(long recNo, int groupId, float... vectorValues) {
        final HalfRealVector vector = makeVector(vectorValues);
        recordStore.saveRecord(VectorRecord.newBuilder()
                .setRecNo(recNo)
                .setGroupId(groupId)
                .setVectorData(ByteString.copyFrom(vector.getRawData()))
                .build());
    }

    private void deleteVectorRecord(long recNo, int groupId) {
        recordStore.deleteRecord(Tuple.from(groupId, recNo));
    }

    @Nonnull
    private Set<Long> scanVectorIndexRecNos(@Nonnull HalfRealVector queryVector) {
        final Index index = recordStore.getRecordMetaData().getIndex(VECTOR_INDEX_NAME);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        final VectorIndexScanBounds bounds = new VectorIndexScanBounds(
                TupleRange.ALL,
                Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                queryVector,
                100,
                VectorIndexScanOptions.empty());
        return maintainer.scan(bounds, null, ScanProperties.FORWARD_SCAN)
                .asList()
                .join()
                .stream()
                .map(e -> e.getPrimaryKey().getLong(1))  // primary key is (group_id, rec_no)
                .collect(Collectors.toSet());
    }

    @Test
    void vectorIndexWrappedWithSlidingWindow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openVectorRecordStore(context, 2, Direction.DESC);

            saveVectorRecord(1, 10, 0.1f, 0.2f, 0.3f, 0.4f);
            saveVectorRecord(2, 20, 0.5f, 0.6f, 0.7f, 0.8f);

            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(
                    recordStore.getRecordMetaData().getIndex(VECTOR_INDEX_NAME));
            assertTrue(maintainer instanceof SlidingWindowIndexMaintainer,
                    "Expected SlidingWindowIndexMaintainer wrapping vector index");

            final HalfRealVector queryVector = makeVector(0.1f, 0.2f, 0.3f, 0.4f);
            assertEquals(Set.of(1L, 2L), scanVectorIndexRecNos(queryVector));

            commit(context);
        }
    }

    @Test
    void vectorIndexSlidingWindowEviction() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openVectorRecordStore(context, 2, Direction.DESC);

            saveVectorRecord(1, 10, 0.1f, 0.2f, 0.3f, 0.4f);  // group_id=10 → window
            saveVectorRecord(2, 20, 0.5f, 0.6f, 0.7f, 0.8f);  // group_id=20 → window
            saveVectorRecord(3, 30, 0.9f, 0.1f, 0.2f, 0.3f);  // group_id=30 → evicts rec(1, 10)

            final HalfRealVector queryVector = makeVector(0.1f, 0.2f, 0.3f, 0.4f);
            assertEquals(Set.of(2L, 3L), scanVectorIndexRecNos(queryVector),
                    "Vector index should contain only windowed records after eviction");

            assertNotNull(recordStore.loadRecord(Tuple.from(10, 1L)));
            assertNotNull(recordStore.loadRecord(Tuple.from(20, 2L)));
            assertNotNull(recordStore.loadRecord(Tuple.from(30, 3L)));

            commit(context);
        }
    }

    @Test
    void vectorIndexSlidingWindowDeleteAndReElect() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openVectorRecordStore(context, 2, Direction.DESC);

            saveVectorRecord(1, 10, 0.1f, 0.2f, 0.3f, 0.4f);  // group_id=10 → window
            saveVectorRecord(2, 20, 0.5f, 0.6f, 0.7f, 0.8f);  // group_id=20 → window
            saveVectorRecord(4, 8, 0.5f, 0.6f, 0.7f, 0.8f);   // group_id=8 → overflow
            saveVectorRecord(3, 5, 0.9f, 0.1f, 0.2f, 0.3f);   // group_id=5 → overflow

            final HalfRealVector queryVector = makeVector(0.5f, 0.5f, 0.5f, 0.5f);
            assertEquals(Set.of(1L, 2L), scanVectorIndexRecNos(queryVector));

            deleteVectorRecord(1, 10);
            assertEquals(Set.of(2L, 4L), scanVectorIndexRecNos(queryVector),
                    "Vector index should promote best overflow (group_id=8) after re-election");

            deleteVectorRecord(4, 8);
            assertEquals(Set.of(2L, 3L), scanVectorIndexRecNos(queryVector),
                    "Vector index should promote next best overflow (group_id=5) after re-election");

            commit(context);
        }
    }

    @Test
    void vectorIndexSlidingWindowOverflowDelete() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openVectorRecordStore(context, 2, Direction.DESC);

            saveVectorRecord(1, 10, 0.1f, 0.2f, 0.3f, 0.4f);  // window
            saveVectorRecord(2, 20, 0.5f, 0.6f, 0.7f, 0.8f);  // window
            saveVectorRecord(3, 5, 0.9f, 0.1f, 0.2f, 0.3f);   // overflow

            final HalfRealVector queryVector = makeVector(0.5f, 0.5f, 0.5f, 0.5f);
            assertEquals(Set.of(1L, 2L), scanVectorIndexRecNos(queryVector));

            deleteVectorRecord(3, 5);
            assertEquals(Set.of(1L, 2L), scanVectorIndexRecNos(queryVector),
                    "Vector index should be unchanged after overflow delete");

            commit(context);
        }
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
