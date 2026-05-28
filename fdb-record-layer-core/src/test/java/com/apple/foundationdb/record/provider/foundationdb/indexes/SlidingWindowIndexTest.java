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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.SlidingWindow;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.SlidingWindowAssert.assertThat;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.makeVector;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.sampleVector;
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

    /**
     * Opens a store with a sliding window HNSW index. No grouping.
     * Window key = relevance, index key = vector_data.
     */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction) throws Exception {
        openStore(context, windowSize, direction, ImmutableList.of());
    }

    /**
     * Opens a store with a grouped sliding window HNSW index.
     */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction,
                           @Nonnull List<List<String>> groupingFields) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsSlidingWindowVectorProto.getDescriptor());
        metaDataBuilder.getRecordType("SlidingWindowVectorRecord")
                .setPrimaryKey(Key.Expressions.field("rec_no"));

        final IndexPredicate.RowNumberWindowPredicate windowPredicate =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("relevance"), direction, windowSize, groupingFields);

        final Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name());
        options.put(IndexOptions.HNSW_NUM_DIMENSIONS, Integer.toString(VECTOR_DIMS));

        // Build key expression: for grouped indexes, prefix with group columns
        // e.g. one grouping column (zone) → KeyWithValue(concat(zone, vector_data), 1)
        // e.g. two grouping columns (zone, category) → KeyWithValue(concat(zone, category, vector_data), 2)
        final com.apple.foundationdb.record.metadata.expressions.KeyExpression keyExpr;
        if (groupingFields.isEmpty()) {
            keyExpr = new KeyWithValueExpression(Key.Expressions.field("vector_data"), 0);
        } else {
            com.apple.foundationdb.record.metadata.expressions.KeyExpression prefix =
                    Key.Expressions.field(groupingFields.get(0).get(0));
            for (int i = 1; i < groupingFields.size(); i++) {
                prefix = Key.Expressions.concat(prefix, Key.Expressions.field(groupingFields.get(i).get(0)));
            }
            keyExpr = new KeyWithValueExpression(
                    Key.Expressions.concat(prefix, Key.Expressions.field("vector_data")),
                    groupingFields.size());
        }

        metaDataBuilder.addIndex("SlidingWindowVectorRecord",
                new Index(INDEX_NAME, keyExpr, IndexTypes.VECTOR, options, windowPredicate));

        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    /**
     * Inserts a record with the given fields. Vector is arbitrary (unique per record).
     */
    private void rec(long recNo, long relevance) {
        rec(recNo, "z", "c", relevance, 0, sampleVector());
    }

    private void rec(long recNo, String zone, String category, long relevance, long score, HalfRealVector vector) {
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
     * Probe: snapshot of the sliding-window state for the ungrouped index.
     */
    @Nonnull
    private SlidingWindow slidingWindow() {
        return SlidingWindowTestHelpers.slidingWindow(recordStore, INDEX_NAME);
    }

    /**
     * Probe: snapshot of the sliding-window state for the given group.
     */
    @Nonnull
    private SlidingWindow groupedSlidingWindow(@Nullable final Tuple groupingKey) {
        return SlidingWindowTestHelpers.groupedSlidingWindow(recordStore, INDEX_NAME, groupingKey);
    }

    // ===== DESC tests (keep highest relevance) =====

    @Test
    void descInsertBelowWindowSize() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);

            rec(4, 400);  // evicts rec1 (lowest relevance for DESC)
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L, 4L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);
            commit(context);
        }
    }

    @Test
    void descMultipleEvictions() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            rec(3, 300);  // evicts rec1
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);

            rec(4, 400);  // evicts rec2
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L);

            rec(5, 500);  // evicts rec3
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(4L, 5L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);

            rec(4, 50);  // evicts rec3 (highest relevance for ASC)
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 4L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);
            commit(context);
        }
    }

    @Test
    void ascMultipleEvictions() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.ASC);
            rec(1, 300);
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            rec(3, 100);  // evicts rec1 (300)
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);

            rec(4, 50);   // evicts rec2 (200)
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L);

            rec(5, 10);   // evicts rec3 (100)
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(4L, 5L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);

            deleteRec(2);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 3L);
            commit(context);
        }
    }

    @Test
    void deleteAndReinsert() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(1);
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L);

            rec(3, 300);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);
            commit(context);
        }
    }

    @Test
    void deleteAllAndRefill() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(1);
            deleteRec(2);
            assertThat(slidingWindow())
                    .hasSizeOf(0)
                    .underlyingHnsw()
                    .isEmpty();

            rec(3, 300);
            rec(4, 400);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(1);  // promotes rec3 from overflow
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(2);  // promotes rec3 from overflow
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 3L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(3);  // overflow delete
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(1);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);

            deleteRec(2);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(2);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 3L);

            deleteRec(3);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 4L);
            commit(context);
        }
    }

    @Test
    void reElectionWithEmptyOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            deleteRec(1);
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L);
            commit(context);
        }
    }

    // ===== Edge cases =====

    @Test
    void windowSizeOne() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 1, Direction.DESC);
            rec(1, 100);
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L);

            rec(2, 200);  // evicts rec1
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L);

            rec(3, 300);  // evicts rec2
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L);

            rec(4, 50);   // worse → overflow
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L);
            commit(context);
        }
    }

    @Test
    void windowSizeOneEvictionSetsBoundaryToNewEntry() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 1, Direction.DESC);
            rec(1, 100);
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L);

            // Evicts rec1 — getNewBoundaryAfterEviction returns null (no inward entry),
            // so the new entry (rec2) itself becomes the boundary.
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L);

            // Verify boundary was correctly set: rec3(50) is worse than rec2(200)
            // for DESC, so it should go to overflow, not evict.
            rec(3, 50);
            assertThat(slidingWindow())
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L);
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValuesCase1() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 100);
            rec(3, 100);
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);

            rec(4, 200);  // evicts one of the 100s
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .contains(4L);
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValuesCase2() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.ASC);
            rec(1, 100);
            rec(2, 100);
            rec(3, 100);
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);

            rec(4, 200);  // must be ignored, pk used here as tiebreaker, it is worse than the boundary
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);
            commit(context);
        }
    }



    @Test
    void duplicateWindowKeyValuesCase3() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(10, 100);
            rec(33, 100);
            rec(5, 100);
            rec(6, 40);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(10L, 33L);
            deleteRec(33);

            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(10L, 5L);
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValuesCase4() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.ASC);
            rec(10, 100);
            rec(33, 100);
            rec(5, 100);
            rec(6, 40);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(5L, 6L);
            deleteRec(6);

            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(10L, 5L);
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValuesCase5() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 25);
            rec(2, 100);
            rec(3, 100);
            rec(4, 100);
            rec(5, 500);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(4L, 5L);
            deleteRec(5);

            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L);
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValuesCase6() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.ASC);
            rec(1, 25);
            rec(2, 100);
            rec(3, 100);
            rec(4, 100);
            rec(5, 500);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);
            deleteRec(1);

            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValuesCase7() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.ASC);
            rec(10, 100);
            rec(11, 100);
            rec(3, 100); // causes eviction
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 10L);
            deleteRec(10);

            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 11L);
            commit(context);
        }
    }

    @Test
    void duplicateWindowKeyValuesCase8() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(3, 100);
            rec(5, 100);
            rec(10, 100); // causes eviction
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(10L, 5L);
            deleteRec(10);

            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 5L);
            commit(context);
        }
    }

    @Test
    void exactBoundaryValue() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);

            // pk is used a tie-breaker using the DESC sort semantics, the window must slide
            rec(3, 100);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            rec(1, 300);  // update: 100 → 300
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            rec(1, 50);   // update: 200 → 50 (worse for DESC) → leaves window, rec3 re-elected
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            rec(3, 400);  // update: 50 → 400 → enters window, evicts rec1
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            rec(3, 150);  // 150 > boundary(100) → evicts rec1
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);

            rec(3, 250);  // still in window, update
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);

            rec(3, 350);  // still in window, update
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L);
            commit(context);
        }
    }

    // ===== deleteWhere tests (grouped) =====

    @Test
    void deleteWhereClearsEntireGroup() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));

            rec(1, "A", "c", 100, 0, sampleVector());
            rec(2, "A", "c", 200, 0, sampleVector());
            rec(3, "B", "c", 300, 0, sampleVector());
            rec(4, "B", "c", 400, 0, sampleVector());
            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L, 4L); // hnsw skip scan

            assertThat(groupedSlidingWindow(Tuple.from("A")))
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            assertThat(groupedSlidingWindow(Tuple.from("B")))
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A")).join();

            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L); // hnsw skip scan

            assertThat(groupedSlidingWindow(Tuple.from("B")))
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L);

            commit(context);
        }
    }

    @Test
    void deleteWhereOnNonExistentGroup() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));

            rec(1, "A", "c", 100, 0, sampleVector());
            rec(2, "B", "c", 200, 0, sampleVector());
            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L); // hnsw skip scan

            assertThat(groupedSlidingWindow(Tuple.from("A")))
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L);

            assertThat(groupedSlidingWindow(Tuple.from("B")))
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L);


            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("Z")).join();

            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L); // hnsw skip scan

            assertThat(groupedSlidingWindow(Tuple.from("A")))
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L);

            assertThat(groupedSlidingWindow(Tuple.from("B")))
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L);

            commit(context);
        }
    }

    @Test
    void deleteWhereGroupWithOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));

            rec(1, "A", "c", 100, 0, sampleVector());
            rec(2, "A", "c", 200, 0, sampleVector());
            rec(5, "A", "c", 50, 0, sampleVector());   // overflow in group A
            rec(3, "B", "c", 300, 0, sampleVector());
            rec(4, "B", "c", 400, 0, sampleVector());
            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L, 4L);

            assertThat(groupedSlidingWindow(Tuple.from("A")))
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L); // hnsw skip scan

            assertThat(groupedSlidingWindow(Tuple.from("B")))
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L); // hnsw skip scan

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A")).join();

            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L); // hnsw skip scan

            assertThat(groupedSlidingWindow(Tuple.from("B")))
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L); // hnsw skip scan

            commit(context);
        }
    }

    @Test
    void twoColumnGroupDeleteWhere() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC,
                    ImmutableList.of(ImmutableList.of("zone"), ImmutableList.of("category")));

            rec(1, "A", "x", 100, 0, sampleVector());
            rec(2, "A", "x", 200, 0, sampleVector());
            rec(3, "A", "y", 300, 0, sampleVector());
            rec(4, "B", "x", 400, 0, sampleVector());
            rec(5, "B", "x", 500, 0, sampleVector());
            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L, 4L, 5L); // hnsw skip scan

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            // Delete group ("A", "x")
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A", "x")).join();
            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L, 5L);

            assertThat(groupedSlidingWindow(Tuple.from("A", "y")))
                    .hasSizeOf(1)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L);

            assertThat(groupedSlidingWindow(Tuple.from("B", "x")))
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(4L, 5L);

            // Delete partial prefix ("A") — clears remaining group ("A", "y")
            maintainer.deleteWhere(context.ensureActive(), Tuple.from("A")).join();
            assertThat(slidingWindow())
                    .underlyingHnsw()
                    .containsInAnyOrder(4L, 5L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            final var newRec = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(3).setZone("z").setCategory("c").setRelevance(300).setScore(0)
                    .setVectorData(ByteString.copyFrom(sampleVector().getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(null, recordStore.saveRecord(newRec)).join();

            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);
            commit(context);
        }
    }

    @Test
    void updateWhileWriteOnlyInsertExistingRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            // rec2 already exists. Should remove then re-insert. Counter stays at 2.
            final var rec2Again = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(2).setZone("z").setCategory("c").setRelevance(200).setScore(0)
                    .setVectorData(ByteString.copyFrom(sampleVector().getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(null, recordStore.saveRecord(rec2Again)).join();

            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);

            final var oldRec2 = recordStore.loadRecord(Tuple.from(2L));
            final var newRec2 = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(2).setZone("z").setCategory("c").setRelevance(250).setScore(0)
                    .setVectorData(ByteString.copyFrom(sampleVector().getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(oldRec2, recordStore.saveRecord(newRec2)).join();

            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 3L);
            commit(context);
        }
    }

    @Test
    void updateWhileWriteOnlyUpdateNonExistingOldRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            final var oldRec5 = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(5).setZone("z").setCategory("c").setRelevance(500).setScore(0)
                    .setVectorData(ByteString.copyFrom(sampleVector().getRawData()))
                    .build();
            final var savedOld = recordStore.saveRecord(oldRec5);
            final var newRec5 = SlidingWindowVectorRecord.newBuilder()
                    .setRecNo(5).setZone("z").setCategory("c").setRelevance(500).setScore(0)
                    .setVectorData(ByteString.copyFrom(sampleVector().getRawData()))
                    .build();
            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
            maintainer.updateWhileWriteOnly(savedOld, recordStore.saveRecord(newRec5)).join();

            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L, 5L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(2L, 3L, 4L);
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
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingHnsw()
                    .containsInAnyOrder(3L, 4L, 5L);
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

        final IndexPredicate.RowNumberWindowPredicate grouped =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("score"), Direction.ASC, 50,
                        ImmutableList.of(ImmutableList.of("zone"), ImmutableList.of("category")));
        assertEquals("QualifyRowNumber(PARTITION BY zone, category ORDER BY score, ASC) <= 50",
                grouped.toString());
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
            assertThat(slidingWindow())
                    .hasSizeOf(2)
                    .underlyingHnsw()
                    .containsInAnyOrder(1L, 2L);

            // canDeleteWhere — no group key → should return false
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
    void canDeleteWhereWithGroup() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));
            rec(1, "A", "c", 100, 0, sampleVector());

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

    // ===== Mock-based delegate coverage tests =====

    /**
     * A minimal IndexMaintainer stub whose delegate methods return controlled values.
     * Used to verify that SlidingWindowIndexMaintainer forwards every delegate method.
     */
    private static class StubIndexMaintainer extends IndexMaintainer {
        private static final Tuple SENTINEL_TUPLE = Tuple.from(42L);
        private static final IndexEntry SENTINEL_ENTRY =
                new IndexEntry(null, Tuple.from(1L), Tuple.from());
        private static final IndexOperationResult SENTINEL_OP_RESULT = new IndexOperationResult() { };

        StubIndexMaintainer(@Nonnull IndexMaintainerState state) {
            super(state);
        }

        @Nonnull
        @Override
        public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range,
                                              @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
            return RecordCursor.fromList(List.of(SENTINEL_ENTRY));
        }

        @Nonnull
        @Override
        public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> o,
                                                                   @Nullable FDBIndexableRecord<M> n) {
            return AsyncUtil.DONE;
        }

        @Nonnull
        @Override
        public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable FDBIndexableRecord<M> o,
                                                                                 @Nullable FDBIndexableRecord<M> n) {
            return AsyncUtil.DONE;
        }

        @Nonnull
        @Override
        public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range,
                                                                  @Nullable byte[] continuation,
                                                                  @Nonnull ScanProperties scanProperties) {
            return RecordCursor.fromList(List.of(SENTINEL_ENTRY));
        }

        @Nonnull
        @Override
        public CompletableFuture<Void> clearUniquenessViolations() {
            return AsyncUtil.DONE;
        }

        @Nonnull
        @Override
        public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation,
                                                                @Nullable ScanProperties scanProperties) {
            return RecordCursor.empty();
        }

        @Override
        public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
            return true;
        }

        @Nullable
        @Override
        public <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull FDBRecord<M> record) {
            return List.of(SENTINEL_ENTRY);
        }

        @Nullable
        @Override
        public <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable FDBIndexableRecord<M> r) {
            return List.of(SENTINEL_ENTRY);
        }

        @Nonnull
        @Override
        @SuppressWarnings("unchecked")
        public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(
                @Nonnull EvaluationContext ctx, @Nonnull IndexRecordFunction<T> function,
                @Nonnull FDBRecord<M> record) {
            return CompletableFuture.completedFuture((T) SENTINEL_TUPLE);
        }

        @Override
        public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
            return true;
        }

        @Nonnull
        @Override
        public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                                   @Nonnull TupleRange range,
                                                                   @Nonnull IsolationLevel isolationLevel) {
            return CompletableFuture.completedFuture(SENTINEL_TUPLE);
        }

        @Override
        public boolean isIdempotent() {
            return true;
        }

        @Nonnull
        @Override
        public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
            return true;
        }

        @Nonnull
        @Override
        public CompletableFuture<Void> deleteWhere(@Nonnull Transaction tr, @Nonnull Tuple prefix) {
            return AsyncUtil.DONE;
        }

        @Nonnull
        @Override
        public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
            return CompletableFuture.completedFuture(SENTINEL_OP_RESULT);
        }

        @Nonnull
        @Override
        public RecordCursor<FDBIndexedRawRecord> scanRemoteFetch(@Nonnull IndexScanBounds scanBounds,
                                                                  @Nullable byte[] continuation,
                                                                  @Nonnull ScanProperties scanProperties,
                                                                  int commonPrimaryKeyLength) {
            return RecordCursor.empty();
        }

        @Nonnull
        @Override
        public CompletableFuture<Void> mergeIndex() {
            return AsyncUtil.DONE;
        }
    }

    @Test
    void delegateMethodsWithMock() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final IndexMaintainerState mockState = new IndexMaintainerState(
                    recordStore, index, IndexMaintenanceFilter.NORMAL);
            final StubIndexMaintainer stub = new StubIndexMaintainer(mockState);
            final SlidingWindowIndexMaintainer sw = new SlidingWindowIndexMaintainer(mockState, stub);

            // scan (4-arg)
            final List<IndexEntry> scanResult = sw.scan(
                    IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList().join();
            assertEquals(1, scanResult.size());
            assertEquals(StubIndexMaintainer.SENTINEL_ENTRY, scanResult.get(0));

            // scanUniquenessViolations
            final List<IndexEntry> violations = sw.scanUniquenessViolations(
                    TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(1, violations.size());

            // clearUniquenessViolations
            sw.clearUniquenessViolations().join();

            // validateEntries
            final List<InvalidIndexEntry> invalid = sw.validateEntries(
                    null, ScanProperties.FORWARD_SCAN).asList().join();
            assertTrue(invalid.isEmpty());

            // evaluateRecordFunction
            rec(1, 100);
            final var loadedRec = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(loadedRec);
            final Tuple evalResult = (Tuple) sw.evaluateRecordFunction(EvaluationContext.EMPTY,
                    new IndexRecordFunction<>("test",
                            Key.Expressions.field("rec_no").groupBy(Key.Expressions.empty()),
                            index.getName()),
                    loadedRec).join();
            assertEquals(StubIndexMaintainer.SENTINEL_TUPLE, evalResult);

            // evaluateAggregateFunction
            final Tuple aggResult = sw.evaluateAggregateFunction(
                    new IndexAggregateFunction("test",
                            Key.Expressions.field("rec_no"), index.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
            assertEquals(StubIndexMaintainer.SENTINEL_TUPLE, aggResult);

            // performOperation
            final IndexOperationResult opResult = sw.performOperation(
                    new IndexOperation() { }).join();
            assertEquals(StubIndexMaintainer.SENTINEL_OP_RESULT, opResult);

            // scanRemoteFetch
            final var remoteFetchResult = sw.scanRemoteFetch(
                    new VectorIndexScanBounds(TupleRange.ALL,
                            Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                            makeVector(0.5f, 0.5f, 0.5f, 0.5f), 100,
                            VectorIndexScanOptions.empty()),
                    null, ScanProperties.FORWARD_SCAN, 1).asList().join();
            assertTrue(remoteFetchResult.isEmpty());

            // mergeIndex
            sw.mergeIndex().join();

            commit(context);
        }
    }
}
