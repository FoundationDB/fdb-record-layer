/*
 * SlidingWindowIndexConcurrencyTest.java
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
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Random;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.SlidingWindowAssert.assertThat;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.concurrent;

/**
 * Concurrency tests for the sliding window index maintainer with a value-index delegate.
 *
 * <p>Every test wires a value index decorated with a sliding window. The window key is
 * {@code relevance} (lower = more relevant for ASC, higher for DESC) and the index key
 * is {@code score} (left as the proto default 0 — the delegate is enumerated by primary
 * key tiebreak). Records are identified by {@code rec_no}.</p>
 *
 * <p>Each test exercises one corner of the maintainer's transactional contract:
 * does a given concurrent shape of inserts produce a commit conflict on the
 * sliding-window meta keys, and does the post-state match expectations?</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowIndexConcurrencyTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_value_index";

    /** Opens a store with a sliding-window value index ordered by {@code relevance}. */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction) throws Exception {
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

    /** Saves a record with the given primary key and window key. */
    private void rec(long recNo, long relevance) {
        recordStore.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(recNo)
                .setRelevance(relevance)
                .build());
    }

    /**
     * Opens a fresh transaction, seeds records 1..N with the given relevances, asserts
     * the window contains all of them, and commits. The post-seed boundary is the
     * lowest-sorting entry under {@code direction} — i.e. {@code (relevances[0], 1)}
     * for DESC if the inputs are presented in increasing order.
     */
    private void seedWindow(int windowSize, @Nonnull Direction direction,
                            long... relevances) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, windowSize, direction);
            final long[] expectedRecNos = new long[relevances.length];
            for (int i = 0; i < relevances.length; i++) {
                final long recNo = i + 1;
                rec(recNo, relevances[i]);
                expectedRecNos[i] = recNo;
            }
            assertThat(SlidingWindowTestHelpers.slidingWindowViaValueIndex(recordStore, INDEX_NAME))
                    .hasSizeOf(relevances.length)
                    .underlyingValueIndex()
                    .containsInAnyOrder(expectedRecNos);
            commit(context);
        }
    }

    /**
     * Opens a fresh transaction and asserts that the delegate index contains exactly
     * {@code expectedRecNos} (in any order) and the window count matches.
     */
    private void assertFinalWindow(int windowSize, @Nonnull Direction direction,
                                   long... expectedRecNos) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, windowSize, direction);
            assertThat(SlidingWindowTestHelpers.slidingWindowViaValueIndex(recordStore, INDEX_NAME))
                    .hasSizeOf(expectedRecNos.length)
                    .underlyingValueIndex()
                    .containsInAnyOrder(expectedRecNos);
            commit(context);
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void descBetterInsertsIntoNonFullWindowDoNotConflict(long seed) throws Exception {
        // Window has spare capacity (3 of 5). Both new relevances (400, 500) are
        // better than the seeded boundary (100, 1) for DESC, so neither rewrites
        // the boundary key — the maintainer just ADD-1s the counter (atomic) and
        // appends to the entries subspace at distinct keys. With no shared
        // read-conflict range, both commits succeed in any order; the seeded
        // shuffle exercises that.
        seedWindow(5, Direction.DESC, 100, 200, 300);

        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(4, 400))
                .tx("B", () -> rec(5, 500))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        assertFinalWindow(5, Direction.DESC, 1L, 2L, 3L, 4L, 5L);
    }

    @Test
    void descWorseInsertsIntoNonFullWindowConflictOnBoundary() throws Exception {
        // Window has spare capacity (3 of 5), but A=(50, 4) and B=(75, 5) are both
        // worse than the seeded boundary (100, 1) for DESC, so each tx rewrites
        // the boundary. The maintainer pairs that tr.set(boundaryMetaKey, ...)
        // with an explicit tr.addReadConflictKey(boundaryMetaKey), turning the
        // boundary update into a proper read-modify-write. The two transactions
        // therefore share a read-conflict range on the boundary key and only
        // one can commit: A wins (declared order), B fails.
        seedWindow(5, Direction.DESC, 100, 200, 300);

        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(4, 50))
                .tx("B", () -> rec(5, 75))
                .commitAll()
                .expectConflictOn("B")
                .expectCommitted("A");

        assertFinalWindow(5, Direction.DESC, 1L, 2L, 3L, 4L);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void descOverflowInsertsIntoFullWindowDoNotConflict(long seed) throws Exception {
        // Window is full (5 of 5). Every new relevance is below the seeded
        // boundary (100, 1) for DESC, so each tx takes the full-branch overflow
        // short-circuit: no delegate touch, no read-conflict on boundaryMetaKey,
        // no boundary rewrite. Each tx only appends its entry to the entries
        // subspace at a disjoint key — no two transactions share a read-conflict
        // range and all six commit in any order.
        seedWindow(5, Direction.DESC, 100, 200, 300, 400, 500);

        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(6, 70))
                .tx("B", () -> rec(7, 80))
                .tx("C", () -> rec(8, 90))
                .tx("D", () -> rec(9, 10))
                .tx("E", () -> rec(10, 11))
                .tx("F", () -> rec(11, 12))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        // The delegate is untouched — the six new records all live in overflow.
        assertFinalWindow(5, Direction.DESC, 1L, 2L, 3L, 4L, 5L);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void descOneEvictionAlongsideOverflowInsertsIntoFullWindowDoNotConflict(long seed) throws Exception {
        // Window is full. Five txns insert below the boundary (100, 1) and short-
        // circuit out of the full branch — no delegate touch, no read-conflict on
        // boundaryMetaKey. The sixth (E, relevance 430) is better than the
        // boundary, so it goes through evictBoundaryAndReplace: it removes rec 1
        // from the delegate, takes a read-conflict on boundaryMetaKey, and picks
        // the new boundary by scanning entries via keyAfter((100, 1))→end. That
        // scan only covers the original better-than-boundary entries (200..500)
        // — disjoint from the overflow keys the others append below 100. So E's
        // read set never overlaps anyone's write set; all six commit in any
        // order.
        seedWindow(5, Direction.DESC, 100, 200, 300, 400, 500);

        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(6, 70))
                .tx("B", () -> rec(7, 80))
                .tx("C", () -> rec(8, 90))
                .tx("D", () -> rec(9, 10))
                .tx("E", () -> rec(10, 430)) // maintenance tx
                .tx("F", () -> rec(11, 12))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        // E's eviction landed: rec 1 (the previous boundary) is out, rec 10 is
        // in, the boundary advanced to (200, 2). The five overflow records
        // (rec 6, 7, 8, 9, 11) live in the entries subspace but not the delegate.
        assertFinalWindow(5, Direction.DESC, 2L, 3L, 4L, 5L, 10L);
    }

    @Test
    void descCompetingEvictionsIntoFullWindowConflict() throws Exception {
        // Window is full. Both A=(6, 5000) and B=(7, 8000) are better than the
        // boundary (100, 1) for DESC, so both take the eviction path. Each calls
        // evictBoundaryAndReplace, which adds a read-conflict on boundaryMetaKey
        // and rewrites it. With both A and B reading AND writing the same
        // boundaryMetaKey, FDB's optimistic concurrency forces one to lose: A
        // commits first (declared order), so B's read overlaps A's write and B
        // fails.
        seedWindow(5, Direction.DESC, 100, 200, 300, 400, 500);

        concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
                .tx("A", () -> rec(6, 5000))
                .tx("B", () -> rec(7, 8000))
                .commitAll()
                .expectConflictOn("B")
                .expectCommitted("A");

        // A's eviction landed: rec 1 (the previous boundary) is out, rec 6 is
        // in, the boundary advanced to (200, 2). B was rolled back, so rec 7
        // never reached the delegate.
        assertFinalWindow(5, Direction.DESC, 2L, 3L, 4L, 5L, 6L);
    }
}
