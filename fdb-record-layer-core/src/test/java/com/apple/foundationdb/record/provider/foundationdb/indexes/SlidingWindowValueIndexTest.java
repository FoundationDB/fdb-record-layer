/*
 * SlidingWindowValueIndexTest.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.SlidingWindowAssert.assertThat;

/**
 * Tests for the sliding window index maintainer with a value-index delegate.
 *
 * <p>Companion to {@link SlidingWindowIndexTest} (HNSW delegate) and
 * {@link SlidingWindowIndexConcurrencyTest} (value-index delegate, concurrency-focused).
 * The value-index path was added primarily to make concurrent-transaction assumptions
 * easier to test, but the basic single-transaction behaviors still need coverage so the
 * delegate wiring is exercised independently of the concurrency harness — that's what
 * lives here.</p>
 *
 * <p>Every test wires a value index decorated with a sliding window. The window key is
 * {@code relevance} (lower = more relevant for ASC, higher for DESC) and the index key
 * is {@code score} (left as the proto default — the delegate is enumerated by the
 * primary-key tiebreak). Records are identified by {@code rec_no}.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowValueIndexTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_value_index";

    /** Opens a store with a sliding-window value index ordered by {@code relevance}. */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsSlidingWindowVectorProto.getDescriptor());
        metaDataBuilder.getRecordType("SlidingWindowVectorRecord")
                .setPrimaryKey(Key.Expressions.field("rec_no"));
        final IndexPredicate windowPredicate =
                new IndexPredicate.RowNumberWindowPredicate("relevance", direction, windowSize);
        metaDataBuilder.addIndex("SlidingWindowVectorRecord",
                new Index(INDEX_NAME, Key.Expressions.field("score"), IndexTypes.VALUE,
                        Map.of(), windowPredicate));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    private void rec(long recNo, long relevance) {
        recordStore.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(recNo)
                .setRelevance(relevance)
                .build());
    }

    private void deleteRec(long recNo) {
        recordStore.deleteRecord(Tuple.from(recNo));
    }

    /**
     * Asserts the post-state of the window: the size, the entries-subspace window-side
     * recNos, the value-index recNos, and the full maintainer invariant — all at once.
     */
    private void assertWindow(int windowSize, @Nonnull Direction direction,
                              long... expectedRecNos) {
        assertThat(SlidingWindowTestHelpers.slidingWindowViaValueIndex(recordStore, INDEX_NAME))
                .hasSizeOf(expectedRecNos.length)
                .hasEntriesOf(expectedRecNos)
                .underlyingValueIndex()
                .containsInAnyOrder(expectedRecNos);
        SlidingWindowTestHelpers.verifySlidingWindowInvariant(
                recordStore, INDEX_NAME, windowSize, direction, windowSize);
    }

    // ===== Basic insert / evict =====

    @Test
    void descInsertBelowWindowSize() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertWindow(5, Direction.DESC, 1L, 2L, 3L);
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
            rec(4, 400);  // evicts rec1 (lowest relevance for DESC)
            assertWindow(3, Direction.DESC, 2L, 3L, 4L);
            commit(context);
        }
    }

    @Test
    void ascEvictsHighest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.ASC);
            rec(1, 300);
            rec(2, 200);
            rec(3, 100);
            rec(4, 50);   // evicts rec1 (highest relevance for ASC)
            assertWindow(3, Direction.ASC, 2L, 3L, 4L);
            commit(context);
        }
    }

    @Test
    void overflowSkipsDelegate() throws Exception {
        // The new entry is worse than the boundary, so it lives only in the entries
        // subspace (overflow side) and never reaches the value-index delegate.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 50);   // worse than (100, 1) for DESC → overflow only
            assertWindow(3, Direction.DESC, 1L, 2L, 3L);
            commit(context);
        }
    }

    // ===== Delete + re-election =====

    @Test
    void deleteFromWindowPromotesOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);  // window
            rec(2, 200);  // window
            rec(3, 50);   // overflow

            deleteRec(1);  // boundary-delete: re-elect rec3 from overflow
            assertWindow(2, Direction.DESC, 2L, 3L);
            commit(context);
        }
    }

    @Test
    void deleteFromOverflowLeavesWindowUnchanged() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 50);   // overflow

            deleteRec(3);  // overflow-delete: short-circuit, no re-election
            assertWindow(2, Direction.DESC, 1L, 2L);
            commit(context);
        }
    }

    // ===== Update path =====

    @Test
    void updateMovingFromWindowToOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 200);  // window
            rec(2, 300);  // window, boundary (200, 1) for DESC
            rec(3, 100);  // overflow

            // Update rec1: 200 → 50 (worse for DESC). rec1 leaves the delegate;
            // rec3 gets promoted from overflow.
            rec(1, 50);
            assertWindow(2, Direction.DESC, 2L, 3L);
            commit(context);
        }
    }

    // ===== Edge case =====

    @Test
    void windowSizeOneEvictionAndOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 1, Direction.DESC);
            rec(1, 100);
            rec(2, 200);  // evicts rec1, rec2 becomes the boundary
            rec(3, 50);   // worse than rec2 → overflow only
            assertWindow(1, Direction.DESC, 2L);
            commit(context);
        }
    }
}
