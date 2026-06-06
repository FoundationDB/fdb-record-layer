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
import com.apple.foundationdb.tuple.Tuple;
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

    /** Deletes the record with the given primary key. */
    private void deleteRec(long recNo) {
        recordStore.deleteRecord(Tuple.from(recNo));
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
     * Opens a fresh transaction and asserts the maintainer's post-state is consistent.
     *
     * <p>Three checks run in one fluent chain:</p>
     * <ul>
     *     <li>the window count matches {@code expectedRecNos.length},</li>
     *     <li>the entries subspace's window-side recNos match {@code expectedRecNos}
     *         — a maintainer invariant, divergence means the entries subspace and
     *         the delegate fell out of sync,</li>
     *     <li>the value index contains exactly {@code expectedRecNos}.</li>
     * </ul>
     */
    private void assertFinalWindow(int windowSize, @Nonnull Direction direction,
                                   long... expectedRecNos) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, windowSize, direction);
            assertThat(SlidingWindowTestHelpers.slidingWindowViaValueIndex(recordStore, INDEX_NAME))
                    .hasSizeOf(expectedRecNos.length)
                    .hasEntriesOf(expectedRecNos)
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

    @Test
    void ascOverflowInsertConflictsWithConcurrentBoundaryDelete() throws Exception {
        // Without the conflict-tracking fix, an overflow insert racing a
        // boundary delete used to leave the new entry orphaned in the entries
        // subspace — better than the post-race boundary, but absent from the
        // delegate. The fix lives in two places:
        //   - Type.getBestInOverflow now declares write- AND read-conflict
        //     ranges over the entries-subspace slice it scanned during
        //     re-election, instead of only the slice's read.
        //   - handleInsert adds a read-conflict on its own (windowValue,
        //     primaryKey) entry key before writing it.
        // Together those ranges intersect, so the optimistic resolver aborts
        // the insert (TX A) when it commits after the re-electing TX B.
        //
        // Initial state (ASC, window = 5):
        //   Window:   rec 1..5 with relevances 10, 20, 30, 40, 50 — boundary
        //             is (50, 5).
        //   Overflow: rec 6, 7, 8 with relevances 60, 70, 80.
        //
        // TX A inserts rec(9, 55). The window is full and 55 is not better
        // than the boundary 50 for ASC, so handleInsert short-circuits the
        // full branch with only the entries-subspace write at (55, 9) — but
        // it also declares a read-conflict on (55, 9) before writing.
        //
        // TX B deletes rec 5 — the boundary record. handleDelete clears
        // (50, 5), decrements the counter, picks (40, 4) as the post-eviction
        // boundary, then re-elects from overflow via getBestInOverflow's
        // forward scan from keyAfter((40, 4)). That scan now also declares
        // both a read-conflict and a write-conflict range over
        // [keyAfter((40, 4)), keyAfter((60, 6))) — the slice it actually
        // visited — and promotes rec 6 into the delegate (final boundary
        // (60, 6)).
        //
        // Commit order: B then A. B commits cleanly. A's read-conflict on
        // (55, 9) sits inside the slice B declared as a write range, so the
        // resolver aborts A. Only B's effects survive.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.ASC);
            rec(1, 10);
            rec(2, 20);
            rec(3, 30);
            rec(4, 40);
            rec(5, 50);  // boundary
            rec(6, 60);  // overflow
            rec(7, 70);  // overflow
            rec(8, 80);  // overflow
            commit(context);
        }

        concurrent(this, ctx -> openStore(ctx, 5, Direction.ASC))
                .tx("B", () -> deleteRec(5))
                .tx("A", () -> rec(9, 55))
                .commitAll()  // B commits first (declared order); A is aborted
                .expectCommitted("B")
                .expectConflictOn("A");

        // B's effects landed: rec 5 evicted, rec 6 promoted, boundary advanced
        // to (60, 6). A was rolled back, so rec 9 is gone everywhere — the
        // entries subspace, the delegate, and the records subspace. The
        // maintainer invariant holds: window-side entries == delegate.
        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 4L, 6L);
    }

    @Test
    void ascBoundaryDeleteConflictsWithConcurrentOverflowDelete() throws Exception {
        // Same family of race as the insert/delete one: two concurrent deletes
        // — one of an overflow record, one of the boundary itself — used to
        // both commit even though the boundary-delete's re-election promoted
        // the very record the other tx was deleting. The result was a
        // delegate entry whose backing entries-subspace key was gone (boundary
        // dangling, delegate over-counting the window).
        //
        // The same fix that closes the insert/delete race closes this one
        // too: Type.getBestInOverflow now declares write- AND read-conflict
        // ranges over the entries-subspace slice it scanned during
        // re-election. Any concurrent transaction whose read or write set
        // touches that slice (here, T1's get/clear of the would-be promotion
        // target) will lose the resolver race.
        //
        // Initial state (ASC, window = 5):
        //   Window:   rec 1..5 with relevances 10, 20, 30, 40, 50 — boundary
        //             is (50, 5).
        //   Overflow: rec 6, 7 with relevances 60, 70.
        //
        // T1 deletes rec 6 (an overflow record). handleDelete reads and
        // clears (60, 6) — both go through the non-snapshot `tr.get` /
        // `tr.clear`, so the read-conflict and write sets cover (60, 6). It
        // snapshot-reads the boundary, finds isInWindow false, and short-
        // circuits.
        //
        // T2 deletes rec 5 — the boundary record. handleDelete clears
        // (50, 5), decrements the counter, picks (40, 4) as the post-eviction
        // boundary, then re-elects from overflow via getBestInOverflow's
        // forward scan from keyAfter((40, 4)). That scan now declares both a
        // read- and a write-conflict range over [keyAfter((40, 4)),
        // keyAfter((60, 6))) — the slice it actually visited — and promotes
        // rec 6 into the delegate.
        //
        // Commit order: T1 then T2. T1 commits cleanly. T2's read range over
        // the overflow slice includes (60, 6); T1's committed write at
        // (60, 6) sits inside it, so the resolver aborts T2. Only T1's
        // effects survive.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.ASC);
            rec(1, 10);
            rec(2, 20);
            rec(3, 30);
            rec(4, 40);
            rec(5, 50);  // boundary
            rec(6, 60);  // overflow
            rec(7, 70);  // overflow
            commit(context);
        }

        concurrent(this, ctx -> openStore(ctx, 5, Direction.ASC))
                .tx("T1", () -> deleteRec(6))  // delete an overflow record
                .tx("T2", () -> deleteRec(5))  // delete the boundary
                .commitAll()  // T1 commits first (declared order); T2 is aborted
                .expectCommitted("T1")
                .expectConflictOn("T2");

        // T1's effects landed: rec 6 is gone everywhere (record, entries
        // subspace). T2 was rolled back, so the window remains {1..5} with
        // boundary (50, 5) and rec 5 still in the delegate. The maintainer
        // invariant holds: window-side entries == delegate == {1..5}.
        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 4L, 5L);
    }
}
