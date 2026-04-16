/*
 * SlidingWindowIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the sliding window index maintainer with an HNSW vector delegate.
 *
 * <p>Each test creates an HNSW index decorated with a sliding window. The window key is
 * {@code relevance} (lower = more relevant for ASC, higher for DESC). The vector field
 * is {@code vector_data}. Records are identified by {@code rec_no}.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowIndexTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_vector_index";
    private static final int VECTOR_DIMS = 4;

    // ===== Helpers =====

    private static HalfRealVector makeVector(float... values) {
        final Half[] components = new Half[values.length];
        for (int i = 0; i < values.length; i++) {
            components[i] = Half.valueOf(values[i]);
        }
        return new HalfRealVector(components);
    }

    /**
     * Opens a store with a sliding window HNSW index. No partitioning.
     * Window key = relevance, index key = vector_data.
     */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction) throws Exception {
        openStore(context, windowSize, direction, ImmutableList.of());
    }

    /**
     * Opens a store with a partitioned sliding window HNSW index.
     */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction,
                           @Nonnull List<List<String>> partitionFields) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsSlidingWindowVectorProto.getDescriptor());
        metaDataBuilder.getRecordType("SlidingWindowVectorRecord")
                .setPrimaryKey(Key.Expressions.field("rec_no"));

        final IndexPredicate.RowNumberWindowPredicate windowPredicate =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("relevance"), direction, windowSize, partitionFields);

        final Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name());
        options.put(IndexOptions.HNSW_NUM_DIMENSIONS, Integer.toString(VECTOR_DIMS));

        // Build key expression: for partitioned indexes, prefix with partition columns
        // e.g. PARTITION BY(zone) → KeyWithValue(concat(zone, vector_data), 1)
        // e.g. PARTITION BY(zone, category) → KeyWithValue(concat(zone, category, vector_data), 2)
        final com.apple.foundationdb.record.metadata.expressions.KeyExpression keyExpr;
        if (partitionFields.isEmpty()) {
            keyExpr = new KeyWithValueExpression(Key.Expressions.field("vector_data"), 0);
        } else {
            com.apple.foundationdb.record.metadata.expressions.KeyExpression prefix =
                    Key.Expressions.field(partitionFields.get(0).get(0));
            for (int i = 1; i < partitionFields.size(); i++) {
                prefix = Key.Expressions.concat(prefix, Key.Expressions.field(partitionFields.get(i).get(0)));
            }
            keyExpr = new KeyWithValueExpression(
                    Key.Expressions.concat(prefix, Key.Expressions.field("vector_data")),
                    partitionFields.size());
        }

        metaDataBuilder.addIndex("SlidingWindowVectorRecord",
                new Index(INDEX_NAME, keyExpr, IndexTypes.VECTOR, options, windowPredicate));

        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    /**
     * Inserts a record with the given fields. Vector is arbitrary (unique per record).
     */
    private void rec(long recNo, long relevance) {
        rec(recNo, "z", "c", relevance, 0, uniqueVector(recNo));
    }

    private void rec(long recNo, String zone, String category, long relevance, long score, float... vec) {
        final HalfRealVector vector = makeVector(vec);
        recordStore.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(recNo)
                .setZone(zone)
                .setCategory(category)
                .setRelevance(relevance)
                .setScore(score)
                .setVectorData(ByteString.copyFrom(vector.getRawData()))
                .build());
    }

    private void deleteRec(long recNo) {
        recordStore.deleteRecord(Tuple.from(recNo));
    }

    /**
     * Creates a unique vector for a given recNo so HNSW can distinguish records.
     */
    private static float[] uniqueVector(long recNo) {
        float v = (float)(recNo % 100) / 100.0f;
        return new float[]{v, 1.0f - v, v * 0.5f, 0.1f};
    }

    /**
     * Scans the HNSW index with a broad query to find all indexed records.
     */
    @Nonnull
    private Set<Long> scanIndexRecNos() {
        final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        final HalfRealVector queryVector = makeVector(0.5f, 0.5f, 0.5f, 0.5f);
        final VectorIndexScanBounds bounds = new VectorIndexScanBounds(
                TupleRange.ALL,
                Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                queryVector,
                100,
                VectorIndexScanOptions.empty());
        return maintainer.scan(bounds, null, com.apple.foundationdb.record.ScanProperties.FORWARD_SCAN)
                .asList()
                .join()
                .stream()
                .map(e -> e.getPrimaryKey().getLong(0))
                .collect(Collectors.toSet());
    }

    private void assertWindowContains(long... expectedRecNos) {
        final Set<Long> actual = scanIndexRecNos();
        final Set<Long> expected = java.util.Arrays.stream(expectedRecNos)
                .boxed()
                .collect(Collectors.toSet());
        assertEquals(expected, actual,
                "Window should contain recNos " + expected + " but was " + actual);
    }

    private long readWindowCount() {
        final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
        final Subspace swSubspace = recordStore.indexSlidingWindowSubspace(index);
        final Subspace metaSubspace = swSubspace.subspace(Tuple.from()).subspace(Tuple.from(1));
        final byte[] counterKey = metaSubspace.pack(Tuple.from(3));
        final byte[] counterBytes = recordStore.ensureContextActive().get(counterKey).join();
        if (counterBytes == null) {
            return 0L;
        }
        return Tuple.fromBytes(counterBytes).getLong(0);
    }

    // ===== DESC tests (keep highest relevance) =====

    @Test
    void descInsertBelowWindowSize() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void descEvictsLowest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertWindowContains(1, 2, 3);

            rec(4, 400);  // evicts rec1 (lowest relevance for DESC)
            assertWindowContains(2, 3, 4);
            commit(context);
        }
    }

    @Test
    void descSkipsLowerValue() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);

            rec(4, 50);  // worse than worst (100) → overflow
            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void descMultipleEvictions() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);

            rec(3, 300);  // evicts rec1
            assertWindowContains(2, 3);

            rec(4, 400);  // evicts rec2
            assertWindowContains(3, 4);

            rec(5, 500);  // evicts rec3
            assertWindowContains(4, 5);
            commit(context);
        }
    }

    // ===== ASC tests (keep lowest relevance) =====

    @Test
    void ascInsertBelowWindowSize() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.ASC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void ascEvictsHighest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.ASC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertWindowContains(1, 2, 3);

            rec(4, 50);  // evicts rec3 (highest relevance for ASC)
            assertWindowContains(1, 2, 4);
            commit(context);
        }
    }

    @Test
    void ascSkipsHigherValue() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.ASC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);

            rec(4, 400);  // worse than worst (300) → overflow
            assertWindowContains(1, 2, 3);
            commit(context);
        }
    }

    @Test
    void ascMultipleEvictions() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.ASC);
            rec(1, 300);
            rec(2, 200);
            assertWindowContains(1, 2);

            rec(3, 100);  // evicts rec1 (300)
            assertWindowContains(2, 3);

            rec(4, 50);   // evicts rec2 (200)
            assertWindowContains(3, 4);

            rec(5, 10);   // evicts rec3 (100)
            assertWindowContains(4, 5);
            commit(context);
        }
    }

    // ===== Delete tests =====

    @Test
    void deleteRecordInWindow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertWindowContains(1, 2, 3);

            deleteRec(2);
            assertWindowContains(1, 3);
            commit(context);
        }
    }

    @Test
    void deleteAndReinsert() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);

            deleteRec(1);
            assertWindowContains(2);

            rec(3, 300);
            assertWindowContains(2, 3);
            commit(context);
        }
    }

    @Test
    void deleteAllAndRefill() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);

            deleteRec(1);
            deleteRec(2);
            assertWindowContains();

            rec(3, 300);
            rec(4, 400);
            assertWindowContains(3, 4);
            commit(context);
        }
    }

    // ===== Re-election tests =====

    @Test
    void deleteFromWindowPromotesOverflowDesc() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);  // window
            rec(2, 200);  // window
            rec(3, 50);   // overflow
            assertWindowContains(1, 2);

            deleteRec(1);  // promotes rec3 from overflow
            assertWindowContains(2, 3);
            commit(context);
        }
    }

    @Test
    void deleteFromWindowPromotesOverflowAsc() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.ASC);
            rec(1, 100);  // window
            rec(2, 200);  // window
            rec(3, 300);  // overflow
            assertWindowContains(1, 2);

            deleteRec(2);  // promotes rec3 from overflow
            assertWindowContains(1, 3);
            commit(context);
        }
    }

    @Test
    void deleteFromOverflowNoChange() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 50);   // overflow
            assertWindowContains(1, 2);

            deleteRec(3);  // overflow delete
            assertWindowContains(1, 2);
            commit(context);
        }
    }

    @Test
    void cascadingReElectionDesc() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 50);   // overflow
            rec(4, 30);   // overflow
            assertWindowContains(1, 2);

            deleteRec(1);
            assertWindowContains(2, 3);

            deleteRec(2);
            assertWindowContains(3, 4);
            commit(context);
        }
    }

    @Test
    void cascadingReElectionAsc() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.ASC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);  // overflow
            rec(4, 400);  // overflow
            assertWindowContains(1, 2);

            deleteRec(2);
            assertWindowContains(1, 3);

            deleteRec(3);
            assertWindowContains(1, 4);
            commit(context);
        }
    }

    @Test
    void reElectionWithEmptyOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);

            deleteRec(1);
            assertWindowContains(2);
            commit(context);
        }
    }

    // ===== Edge cases =====

    @Test
    void windowSizeOne() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 1, Direction.DESC);
            rec(1, 100);
            assertWindowContains(1);

            rec(2, 200);  // evicts rec1
            assertWindowContains(2);

            rec(3, 300);  // evicts rec2
            assertWindowContains(3);

            rec(4, 50);   // worse → overflow
            assertWindowContains(3);
            commit(context);
        }
    }

    @Test
    void windowSizeOneEvictionSetsBoundaryToNewEntry() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 1, Direction.DESC);
            rec(1, 100);
            assertWindowContains(1);
            assertEquals(1, readWindowCount());

            // Evicts rec1 — getNewBoundaryAfterEviction returns null (no inward entry),
            // so the new entry (rec2) itself becomes the boundary.
            rec(2, 200);
            assertWindowContains(2);
            assertEquals(1, readWindowCount());

            // Verify boundary was correctly set: rec3(50) is worse than rec2(200)
            // for DESC, so it should go to overflow, not evict.
            rec(3, 50);
            assertWindowContains(2);
            assertEquals(1, readWindowCount());
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValues() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 100);
            rec(3, 100);
            assertWindowContains(1, 2, 3);

            rec(4, 200);  // evicts one of the 100s
            Set<Long> window = scanIndexRecNos();
            assertEquals(3, window.size());
            assertTrue(window.contains(4L), "rec4 should be in window");
            commit(context);
        }
    }

    @Test
    void exactBoundaryValue() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);

            // Same window key as worst → NOT better (strict) → overflow
            rec(3, 100);
            assertWindowContains(1, 2);
            commit(context);
        }
    }

    // ===== Record update (same primary key) tests =====

    @Test
    void updateRecordWithBetterWindowKey() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);

            rec(1, 300);  // update: 100 → 300
            assertWindowContains(1, 2);
            commit(context);
        }
    }

    @Test
    void updateRecordFromWindowToOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 200);
            rec(2, 300);
            rec(3, 100);  // overflow
            assertWindowContains(1, 2);

            rec(1, 50);   // update: 200 → 50 (worse for DESC) → leaves window, rec3 re-elected
            assertWindowContains(2, 3);
            commit(context);
        }
    }

    @Test
    void updateRecordFromOverflowToWindow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 200);
            rec(2, 300);
            rec(3, 50);   // overflow
            assertWindowContains(1, 2);

            rec(3, 400);  // update: 50 → 400 → enters window, evicts rec1
            assertWindowContains(2, 3);
            commit(context);
        }
    }

    @Test
    void updateRecordMultipleTimes() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 50);   // overflow
            assertWindowContains(1, 2);

            rec(3, 150);  // 150 > boundary(100) → evicts rec1
            assertWindowContains(2, 3);

            rec(3, 250);  // still in window, update
            assertWindowContains(2, 3);

            rec(3, 350);  // still in window, update
            assertWindowContains(2, 3);
            commit(context);
        }
    }

    // ===== deleteWhere tests (partitioned) =====

    @Test
    void deleteWhereClearsEntirePartition() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));

            rec(1, "A", "c", 100, 0, uniqueVector(1));
            rec(2, "A", "c", 200, 0, uniqueVector(2));
            rec(3, "B", "c", 300, 0, uniqueVector(3));
            rec(4, "B", "c", 400, 0, uniqueVector(4));
            assertWindowContains(1, 2, 3, 4);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A")).join();

            assertWindowContains(3, 4);
            commit(context);
        }
    }

    @Test
    void deleteWhereOnNonExistentPartition() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));

            rec(1, "A", "c", 100, 0, uniqueVector(1));
            rec(2, "B", "c", 200, 0, uniqueVector(2));
            assertWindowContains(1, 2);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("Z")).join();

            assertWindowContains(1, 2);
            commit(context);
        }
    }

    @Test
    void deleteWherePartitionWithOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));

            rec(1, "A", "c", 100, 0, uniqueVector(1));
            rec(2, "A", "c", 200, 0, uniqueVector(2));
            rec(5, "A", "c", 50, 0, uniqueVector(5));   // overflow in partition A
            rec(3, "B", "c", 300, 0, uniqueVector(3));
            rec(4, "B", "c", 400, 0, uniqueVector(4));
            assertWindowContains(1, 2, 3, 4);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A")).join();

            assertWindowContains(3, 4);
            commit(context);
        }
    }

    @Test
    void twoColumnPartitionDeleteWhere() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC,
                    ImmutableList.of(ImmutableList.of("zone"), ImmutableList.of("category")));

            rec(1, "A", "x", 100, 0, uniqueVector(1));
            rec(2, "A", "x", 200, 0, uniqueVector(2));
            rec(3, "A", "y", 300, 0, uniqueVector(3));
            rec(4, "B", "x", 400, 0, uniqueVector(4));
            rec(5, "B", "x", 500, 0, uniqueVector(5));
            assertWindowContains(1, 2, 3, 4, 5);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            // Delete partition ("A", "x")
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A", "x")).join();
            assertWindowContains(3, 4, 5);

            // Delete partial prefix ("A") — clears remaining partition ("A", "y")
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A")).join();
            assertWindowContains(4, 5);
            commit(context);
        }
    }

    // ===== updateWhileWriteOnly tests =====

    @Test
    void updateWhileWriteOnlyInsertNewRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);
            assertEquals(2, readWindowCount());

            final var newRec = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(3).setZone("z").setCategory("c").setRelevance(300).setScore(0)
                    .setVectorData(ByteString.copyFrom(makeVector(uniqueVector(3)).getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(null, recordStore.saveRecord(newRec)).join();

            assertWindowContains(1, 2, 3);
            assertEquals(3, readWindowCount());
            commit(context);
        }
    }

    @Test
    void updateWhileWriteOnlyInsertExistingRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);
            assertEquals(2, readWindowCount());

            // rec2 already exists. Should remove then re-insert. Counter stays at 2.
            final var rec2Again = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(2).setZone("z").setCategory("c").setRelevance(200).setScore(0)
                    .setVectorData(ByteString.copyFrom(makeVector(uniqueVector(2)).getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(null, recordStore.saveRecord(rec2Again)).join();

            assertWindowContains(1, 2);
            assertEquals(2, readWindowCount());
            commit(context);
        }
    }

    @Test
    void updateWhileWriteOnlyUpdateExistingRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertWindowContains(1, 2, 3);
            assertEquals(3, readWindowCount());

            final var oldRec2 = recordStore.loadRecord(Tuple.from(2L));
            final var newRec2 = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(2).setZone("z").setCategory("c").setRelevance(250).setScore(0)
                    .setVectorData(ByteString.copyFrom(makeVector(uniqueVector(2)).getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(oldRec2, recordStore.saveRecord(newRec2)).join();

            assertWindowContains(1, 2, 3);
            assertEquals(3, readWindowCount());
            commit(context);
        }
    }

    @Test
    void updateWhileWriteOnlyUpdateNonExistingOldRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertWindowContains(1, 2);
            assertEquals(2, readWindowCount());

            final var oldRec5 = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(5).setZone("z").setCategory("c").setRelevance(500).setScore(0)
                    .setVectorData(ByteString.copyFrom(makeVector(uniqueVector(5)).getRawData()))
                    .build();
            final var savedOld = recordStore.saveRecord(oldRec5);
            final var newRec5 = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(5).setZone("z").setCategory("c").setRelevance(500).setScore(0)
                    .setVectorData(ByteString.copyFrom(makeVector(uniqueVector(5)).getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(savedOld, recordStore.saveRecord(newRec5)).join();

            assertWindowContains(1, 2, 5);
            assertEquals(3, readWindowCount());
            commit(context);
        }
    }

    // ===== Rebuild tests =====

    @Test
    void rebuildIndexDesc() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 400);
            rec(5, 50);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            recordStore.rebuildAllIndexes().join();
            // DESC window=3: keeps {rec2(200), rec3(300), rec4(400)}
            assertWindowContains(2, 3, 4);
            commit(context);
        }
    }

    @Test
    void rebuildIndexAsc() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.ASC);
            rec(1, 500);
            rec(2, 400);
            rec(3, 300);
            rec(4, 200);
            rec(5, 100);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.ASC);
            recordStore.rebuildAllIndexes().join();
            // ASC window=3: keeps {rec3(300), rec4(200), rec5(100)}
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
        final IndexPredicate and = new IndexPredicate.AndPredicate(List.of(qualify, constant));
        IndexPredicate.validateRowNumberWindowPlacement(and);
    }

    @Test
    void qualifyUnderOrIsInvalid() {
        final IndexPredicate.RowNumberWindowPredicate qualify =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 10);
        final IndexPredicate.ConstantPredicate constant =
                new IndexPredicate.ConstantPredicate(IndexPredicate.ConstantPredicate.ConstantValue.TRUE);
        final IndexPredicate or = new IndexPredicate.OrPredicate(List.of(qualify, constant));
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
        final IndexPredicate andWithQualify = new IndexPredicate.AndPredicate(List.of(qualify));
        final IndexPredicate or = new IndexPredicate.OrPredicate(List.of(andWithQualify, constant));
        org.junit.jupiter.api.Assertions.assertThrows(
                com.apple.foundationdb.record.RecordCoreException.class,
                () -> IndexPredicate.validateRowNumberWindowPlacement(or));
    }

    // ===== RowNumberWindowPredicate unit tests =====

    @Test
    void rowNumberWindowPredicateToString() {
        final IndexPredicate.RowNumberWindowPredicate simple =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 100);
        assertEquals("QualifyRowNumber(score, DESC) <= 100", simple.toString());

        final IndexPredicate.RowNumberWindowPredicate partitioned =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("score"), Direction.ASC, 50,
                        ImmutableList.of(ImmutableList.of("zone"), ImmutableList.of("category")));
        assertEquals("QualifyRowNumber(PARTITION BY zone, category ORDER BY score, ASC) <= 50",
                partitioned.toString());
    }

    @Test
    void rowNumberWindowPredicateGetFieldName() {
        final IndexPredicate.RowNumberWindowPredicate pred =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 100);
        assertEquals("score", pred.getFieldName());
    }

    @Test
    void rowNumberWindowPredicateGetFieldNameMultiElement() {
        final IndexPredicate.RowNumberWindowPredicate pred =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("nested", "score"), Direction.ASC, 10);
        assertThrows(com.google.common.base.VerifyException.class, pred::getFieldName);
    }

    @Test
    void rowNumberWindowPredicateHashCode() {
        final IndexPredicate.RowNumberWindowPredicate a =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 100);
        final IndexPredicate.RowNumberWindowPredicate b =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.DESC, 100);
        final IndexPredicate.RowNumberWindowPredicate c =
                new IndexPredicate.RowNumberWindowPredicate("score", Direction.ASC, 100);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a, b);
        assertFalse(a.equals(c));
    }

    // ===== Factory tests =====

    @Test
    void factoryGetIndexTypesIsEmpty() {
        final SlidingWindowIndexMaintainerFactory factory =
                new SlidingWindowIndexMaintainerFactory(new VectorIndexMaintainerFactory());
        assertFalse(factory.getIndexTypes().iterator().hasNext());
    }

    // ===== Delegate method coverage tests =====

    @Test
    void delegateMethodsScan() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            // scan(IndexScanType, TupleRange, continuation, ScanProperties) is used by scanIndexRecNos
            assertWindowContains(1, 2);

            // canDeleteWhere — no partition key → should return false
            final com.apple.foundationdb.record.query.QueryToKeyMatcher matcher =
                    new com.apple.foundationdb.record.query.QueryToKeyMatcher(
                            com.apple.foundationdb.record.query.expressions.Query.field("zone")
                                    .equalsValue("A"));
            assertFalse(maintainer.canDeleteWhere(matcher, Key.Evaluated.scalar("A")));

            // canEvaluateRecordFunction
            assertFalse(maintainer.canEvaluateRecordFunction(
                    new IndexRecordFunction<>("test",
                            Key.Expressions.field("rec_no").groupBy(Key.Expressions.empty()),
                            index.getName())));

            // canEvaluateAggregateFunction
            assertFalse(maintainer.canEvaluateAggregateFunction(
                    new com.apple.foundationdb.record.metadata.IndexAggregateFunction(
                            "test", Key.Expressions.field("rec_no"), index.getName())));

            // evaluateIndex
            final var rec = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec);
            final List<com.apple.foundationdb.record.IndexEntry> entries = maintainer.evaluateIndex(rec);
            assertNotNull(entries);

            // filteredIndexEntries
            final List<com.apple.foundationdb.record.IndexEntry> filtered =
                    maintainer.filteredIndexEntries(recordStore.loadRecord(Tuple.from(1L)));
            assertNotNull(filtered);

            // addedRangeWithKey
            maintainer.addedRangeWithKey(Tuple.from(1L)).join();

            // isIdempotent (delegate forwards)
            maintainer.isIdempotent();

            commit(context);
        }
    }

    @Test
    void canDeleteWhereWithPartition() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));
            rec(1, "A", "c", 100, 0, uniqueVector(1));

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            final com.apple.foundationdb.record.query.QueryToKeyMatcher matcher =
                    new com.apple.foundationdb.record.query.QueryToKeyMatcher(
                            com.apple.foundationdb.record.query.expressions.Query.field("zone")
                                    .equalsValue("A"));
            assertTrue(maintainer.canDeleteWhere(matcher, Key.Evaluated.scalar("A")));
            commit(context);
        }
    }
}
