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
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.SlidingWindow;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.SlidingWindowAssert.assertThat;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.concurrent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the sliding window index maintainer with a value-index delegate.
 *
 * <p>Each test creates a value index decorated with a sliding window. The window key is
 * {@code relevance} (lower = more relevant for ASC, higher for DESC). The value index
 * key is {@code score}. Records are identified by {@code rec_no}.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowIndexConcurrencyTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_value_index";

    /**
     * Opens a store with a sliding window value index. No grouping.
     * Window key = relevance, index key = score.
     */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction) throws Exception {
        openStore(context, windowSize, direction, ImmutableList.of());
    }

    /**
     * Opens a store with a (possibly grouped) sliding window value index.
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

        // Build key expression: for grouped indexes, prefix with group columns
        // e.g. one grouping column (zone) → concat(zone, score)
        // e.g. two grouping columns (zone, category) → concat(zone, category, score)
        final com.apple.foundationdb.record.metadata.expressions.KeyExpression keyExpr;
        if (groupingFields.isEmpty()) {
            keyExpr = Key.Expressions.field("score");
        } else {
            com.apple.foundationdb.record.metadata.expressions.KeyExpression expr =
                    Key.Expressions.field(groupingFields.get(0).get(0));
            for (int i = 1; i < groupingFields.size(); i++) {
                expr = Key.Expressions.concat(expr, Key.Expressions.field(groupingFields.get(i).get(0)));
            }
            keyExpr = Key.Expressions.concat(expr, Key.Expressions.field("score"));
        }

        metaDataBuilder.addIndex("SlidingWindowVectorRecord",
                new Index(INDEX_NAME, keyExpr, IndexTypes.VALUE,
                        java.util.Collections.emptyMap(), windowPredicate));

        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    /**
     * Inserts a record with the given fields. Score defaults to 0.
     */
    private void rec(long recNo, long relevance) {
        rec(recNo, "z", "c", relevance, 0);
    }

    private void rec(long recNo, String zone, String category, long relevance, long score) {
        recordStore.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(recNo)
                .setZone(zone)
                .setCategory(category)
                .setRelevance(relevance)
                .setScore(score)
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
        return SlidingWindowTestHelpers.slidingWindowViaValueIndex(recordStore, INDEX_NAME);
    }

    /**
     * Probe: snapshot of the sliding-window state for the given group.
     */
    @Nonnull
    private SlidingWindow groupedSlidingWindow(@Nullable final Tuple groupingKey) {
        return SlidingWindowTestHelpers.groupedSlidingWindowViaValueIndex(recordStore, INDEX_NAME, groupingKey);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void descConcurrentInsertsBelowWindowSizeDoNotConflict(long seed) throws Exception {
        // Seed the window with three records (window size 5, so it stays under-full).
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L);
            commit(context);
        }

        // Two concurrent transactions each insert a distinct record into the still-not-full
        // window. Because the maintainer bumps the count with an atomic ADD mutation and reads
        // the count/boundary through tr.snapshot() (and neither new key is worse than the seeded
        // boundary of (100, 1), so neither rewrites the boundary), the two transactions touch no
        // shared read-conflict key. Both commits must therefore succeed in any order without a
        // conflict.
        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(4, 400))
                .tx("B", () -> rec(5, 500))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        // Both records landed in the window and the count reflects all five.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            assertThat(slidingWindow())
                    .hasSizeOf(5)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L, 4L, 5L);
            commit(context);
        }
    }

    @Test
    void descConcurrentInsertsAboveWindowSizeConflict() throws Exception {
        // Seed the window with three records (window size 5, so it stays under-full).
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertThat(slidingWindow())
                    .hasSizeOf(3)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L);
            commit(context);
        }

        // Two concurrent transactions each insert a record that is WORSE than the
        // seeded boundary (100, 1) for DESC: A=(50, 4) and B=(75, 5). Both go through
        // the not-full branch (count=3 < 5), but because each new entry replaces the
        // boundary, the maintainer pairs its tr.set(boundaryMetaKey, ...) with an
        // explicit tr.addReadConflictKey(boundaryMetaKey) — turning the boundary
        // update into a proper read-modify-write under FDB's optimistic concurrency.
        // The two transactions therefore share a read-conflict range on the boundary
        // key and cannot both commit: A wins (declared order), B fails. Final window
        // contains rec 4 (from A) but not rec 5.
        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(4, 50))
                .tx("B", () -> rec(5, 75))
                .commitAll()
                .expectConflictOn("B");

        // Only rec 4 (from A) made it in; B was rolled back on conflict.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            assertThat(slidingWindow())
                    .hasSizeOf(4)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L, 4L);
            commit(context);
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void concurrentInsertsOutsideBoundariesOfFullWindowDoNotConflict(long seed) throws Exception {
        // Fill the window to capacity (size=5) with relevances 100..500.
        // For DESC the boundary is the worst entry: (100, 1).
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 400);
            rec(5, 500);
            assertThat(slidingWindow())
                    .hasSizeOf(5)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L, 4L, 5L);
            commit(context);
        }

        // Six concurrent transactions each insert a record that is WORSE than the
        // boundary (100, 1) for DESC — i.e. each new entry belongs in overflow,
        // outside the window. The window is full (count=5=size), so every tx takes
        // the full branch, snapshot-reads the boundary, finds isBetter(entry,
        // boundary) is false, and short-circuits: no delegate.update, no
        // tr.addReadConflictKey(boundaryMetaKey), no boundary rewrite. Each tx
        // only writes its own entry into the entries subspace at a disjoint key,
        // so the six transactions share no read-conflict range and all commit
        // cleanly regardless of order — the seeded shuffle exercises that.
        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(6, 70))
                .tx("B", () -> rec(7, 80))
                .tx("C", () -> rec(8, 90))
                .tx("D", () -> rec(9, 10))
                .tx("E", () -> rec(10, 11))
                .tx("F", () -> rec(11, 12))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        // The delegate is untouched — the six new records all live in overflow,
        // and the window still holds the original five.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            assertThat(slidingWindow())
                    .hasSizeOf(5)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L, 4L, 5L);
            commit(context);
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void concurrentInsertsOutsideBoundariesOfFullWindowDoNotConflictWithSingleMaintainingTx(long seed) throws Exception {
        // Fill the window to capacity (size=5) with relevances 100..500.
        // For DESC the boundary is the worst entry: (100, 1).
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 400);
            rec(5, 500);
            assertThat(slidingWindow())
                    .hasSizeOf(5)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L, 4L, 5L);
            commit(context);
        }

        // Six concurrent transactions, but exactly one (E) actually maintains the
        // window: rec(10, 430) is BETTER than the boundary (100, 1) for DESC, so
        // E evicts rec 1 from the delegate and reseats the boundary. The other
        // five carry overflow-only inserts (relevance < 100) and short-circuit
        // out of the maintainer — no delegate touch, no boundaryMetaKey
        // read-conflict, no boundary rewrite.
        //
        // E does take a read-conflict on boundaryMetaKey and scans the entries
        // subspace via keyAfter((100, 1))→end to pick the new boundary, but that
        // range only covers the original better-than-boundary entries (200..500)
        // — not the overflow keys the other txns are appending below 100. So
        // E's read set is disjoint from every other tx's write set, and all six
        // commit cleanly in any order; the seeded shuffle exercises that.
        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(6, 70))
                .tx("B", () -> rec(7, 80))
                .tx("C", () -> rec(8, 90))
                .tx("D", () -> rec(9, 10))
                .tx("E", () -> rec(10, 430)) // maintenance tx
                .tx("F", () -> rec(11, 12))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        // The delegate is untouched — the six new records all live in overflow,
        // and the window still holds the original five.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            assertThat(slidingWindow())
                    .hasSizeOf(5)
                    .underlyingValueIndex()
                    .containsInAnyOrder(2L, 3L, 4L, 5L, 10L);
            commit(context);
        }
    }

    @Test
    void concurrentInsertionsWithinBoundariesOfFullWindowConflict() throws Exception {
        // Fill the window to capacity (size=5) with relevances 100..500.
        // For DESC the boundary is the worst entry: (100, 1).
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 400);
            rec(5, 500);
            assertThat(slidingWindow())
                    .hasSizeOf(5)
                    .underlyingValueIndex()
                    .containsInAnyOrder(1L, 2L, 3L, 4L, 5L);
            commit(context);
        }

        // Two concurrent transactions both insert into a FULL window with values
        // strictly BETTER than the boundary (100, 1) for DESC: A=(6, 5000),
        // B=(7, 8000). Both take the eviction path — each snapshot-reads the
        // boundary, finds isBetter(entry, boundary)=true, and calls
        // evictBoundaryAndReplace, which pairs a tr.addReadConflictKey on
        // boundaryMetaKey with a tr.set rewriting it. With both A and B reading
        // AND writing the same boundaryMetaKey, FDB's optimistic concurrency
        // forces one to lose: A commits first (declared order), so B's read
        // overlaps A's write and B fails.
        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(6, 5000))
                .tx("B", () -> rec(7, 8000))
                .commitAll()
                .expectConflictOn("B")
                .expectCommitted("A");

        // A's eviction landed: rec 1 (the previous boundary) is out, rec 6 is in,
        // and the boundary has advanced to (200, 2). B was rolled back, so rec 7
        // never made it to the delegate.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            assertThat(slidingWindow())
                    .hasSizeOf(5)
                    .underlyingValueIndex()
                    .containsInAnyOrder(2L, 3L, 4L, 5L, 6L);
            commit(context);
        }
    }
}
