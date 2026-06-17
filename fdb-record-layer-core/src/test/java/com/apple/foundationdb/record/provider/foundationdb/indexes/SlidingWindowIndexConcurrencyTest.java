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
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.record.util.ConcurrentTransactionHarness;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Random;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.SlidingWindowAssert.assertThat;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.verifySlidingWindowInvariant;
import static org.junit.jupiter.api.Assertions.fail;

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
     * Opens a fresh {@link ConcurrentTransactionHarness.ConcurrentScenario} pre-wired
     * to this test's {@code openStore(ctx, windowSize, direction)}, so call sites
     * can write {@code concurrent(5, Direction.DESC).tx(...)} instead of
     * threading the lambda by hand.
     */
    @Nonnull
    private ConcurrentTransactionHarness.ConcurrentScenario concurrent(int windowSize, @Nonnull Direction direction) {
        return ConcurrentTransactionHarness.concurrent(this::openContext, ctx -> openStore(ctx, windowSize, direction));
    }

    /**
     * Opens a fresh transaction, seeds records 1..N with the given relevances,
     * asserts the post-state, and commits. Inputs are presumed to be in
     * increasing order. When {@code N > windowSize}, ASC keeps the first
     * {@code windowSize} recNos in the delegate and the rest land in overflow;
     * DESC keeps the last {@code windowSize}.
     */
    private void seedWindow(int windowSize, @Nonnull Direction direction,
                            long... relevances) throws Exception {
        final int expectedInWindow = Math.min(windowSize, relevances.length);
        try (FDBRecordContext context = openContext()) {
            openStore(context, windowSize, direction);
            for (int i = 0; i < relevances.length; i++) {
                rec(i + 1, relevances[i]);
            }
            final long[] expectedInWindowRecNos = new long[expectedInWindow];
            final int offset = direction == Direction.ASC ? 0 : relevances.length - expectedInWindow;
            for (int i = 0; i < expectedInWindow; i++) {
                expectedInWindowRecNos[i] = offset + i + 1;
            }
            assertThat(SlidingWindowTestHelpers.slidingWindowViaValueIndex(recordStore, INDEX_NAME))
                    .hasSizeOf(expectedInWindow)
                    .underlyingValueIndex()
                    .containsInAnyOrder(expectedInWindowRecNos);
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

            verifySlidingWindowInvariant(recordStore,
                    INDEX_NAME,
                    windowSize,
                    direction,
                    expectedRecNos.length,
                    expectedRecNos.length);

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

        concurrent(5, Direction.DESC)
                .tx("A", () -> rec(4, 400))
                .tx("B", () -> rec(5, 500))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        assertFinalWindow(5, Direction.DESC, 1L, 2L, 3L, 4L, 5L);
    }

    @Test
    void descWorseInsertsIntoNonFullWindowConflictOnBoundaryCase1() throws Exception {
        // Window has spare capacity (3 of 5), but A=(50, 4) and B=(75, 5) are both
        // worse than the seeded boundary (100, 1) for DESC, so each tx rewrites
        // the boundary. The maintainer pairs that tr.set(boundaryMetaKey, ...)
        // with an explicit tr.addReadConflictKey(boundaryMetaKey), turning the
        // boundary update into a proper read-modify-write. The two transactions
        // therefore share a read-conflict range on the boundary key and only
        // one can commit: A wins (declared order), B fails.
        seedWindow(5, Direction.DESC, 100, 200, 300);

        concurrent(5, Direction.DESC)
                .tx("A", () -> rec(4, 50))
                .tx("B", () -> rec(5, 75))
                .commitAll()
                .expectConflictOn("B")
                .expectCommitted("A");

        assertFinalWindow(5, Direction.DESC, 1L, 2L, 3L, 4L);
    }

    @Test
    void descWorseInsertsIntoNonFullWindowConflictOnBoundaryCase2() throws Exception {
        // This is the same setup in descWorseInsertsIntoNonFullWindowConflictOnBoundaryCase1
        // but in this test B wins, and so, A fails
        seedWindow(5, Direction.DESC, 100, 200, 300);

        concurrent(5, Direction.DESC)
                .tx("B", () -> rec(5, 75))
                .tx("A", () -> rec(4, 50))
                .commitAll()
                .expectConflictOn("A")
                .expectCommitted("B");

        assertFinalWindow(5, Direction.DESC, 1L, 2L, 3L, 5L);
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

        concurrent(5, Direction.DESC)
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

        concurrent(5, Direction.DESC)
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

        concurrent(5, Direction.DESC)
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

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void ascBetterInsertsIntoNonFullWindowDoNotConflict(long seed) throws Exception {
        // ASC mirror of descBetterInsertsIntoNonFullWindowDoNotConflict.
        // Window has spare capacity (3 of 5). Both new relevances (50, 25) are
        // better than the seeded boundary (300, 1) for ASC, so neither rewrites
        // the boundary key — the maintainer just ADD-1s the counter (atomic) and
        // appends to the entries subspace at distinct keys. With no shared
        // read-conflict range, both commits succeed in any order; the seeded
        // shuffle exercises that.
        seedWindow(5, Direction.ASC, 300, 200, 100);

        concurrent(5, Direction.ASC)
                .tx("A", () -> rec(4, 50))
                .tx("B", () -> rec(5, 25))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 4L, 5L);
    }

    @Test
    void ascWorseInsertsIntoNonFullWindowConflictOnBoundaryCase1() throws Exception {
        // ASC mirror of descWorseInsertsIntoNonFullWindowConflictOnBoundaryCase1.
        // Window has spare capacity (3 of 5), but A=(400, 4) and B=(500, 5) are
        // both worse than the seeded boundary (300, 1) for ASC, so each tx
        // rewrites the boundary. The maintainer pairs that
        // tr.set(boundaryMetaKey, ...) with an explicit
        // tr.addReadConflictKey(boundaryMetaKey), turning the boundary update
        // into a proper read-modify-write. The two transactions therefore share
        // a read-conflict range on the boundary key and only one can commit:
        // A wins (declared order), B fails.
        seedWindow(5, Direction.ASC, 300, 200, 100);

        concurrent(5, Direction.ASC)
                .tx("A", () -> rec(4, 400))
                .tx("B", () -> rec(5, 500))
                .commitAll()
                .expectConflictOn("B")
                .expectCommitted("A");

        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 4L);
    }

    @Test
    void ascWorseInsertsIntoNonFullWindowConflictOnBoundaryCase2() throws Exception {
        // This is the same setup as ascWorseInsertsIntoNonFullWindowConflictOnBoundaryCase1
        // but in this test B wins, and so, A fails.
        seedWindow(5, Direction.ASC, 300, 200, 100);

        concurrent(5, Direction.ASC)
                .tx("B", () -> rec(5, 500))
                .tx("A", () -> rec(4, 400))
                .commitAll()
                .expectConflictOn("A")
                .expectCommitted("B");

        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 5L);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void ascOverflowInsertsIntoFullWindowDoNotConflict(long seed) throws Exception {
        // ASC mirror of descOverflowInsertsIntoFullWindowDoNotConflict.
        // Window is full (5 of 5). Every new relevance is above the seeded
        // boundary (500, 5) for ASC, so each tx takes the full-branch overflow
        // short-circuit: no delegate touch, no read-conflict on boundaryMetaKey,
        // no boundary rewrite. Each tx only appends its entry to the entries
        // subspace at a disjoint key — no two transactions share a read-conflict
        // range and all six commit in any order.
        seedWindow(5, Direction.ASC, 100, 200, 300, 400, 500);

        concurrent(5, Direction.ASC)
                .tx("A", () -> rec(6, 700))
                .tx("B", () -> rec(7, 800))
                .tx("C", () -> rec(8, 900))
                .tx("D", () -> rec(9, 1100))
                .tx("E", () -> rec(10, 1200))
                .tx("F", () -> rec(11, 1300))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        // The delegate is untouched — the six new records all live in overflow.
        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 4L, 5L);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void ascOneEvictionAlongsideOverflowInsertsIntoFullWindowDoNotConflict(long seed) throws Exception {
        // ASC mirror of descOneEvictionAlongsideOverflowInsertsIntoFullWindowDoNotConflict.
        // Window is full. Five txns insert above the boundary (500, 5) and
        // short-circuit out of the full branch — no delegate touch, no
        // read-conflict on boundaryMetaKey. The sixth (E, relevance 70) is
        // better than the boundary, so it goes through evictBoundaryAndReplace:
        // it removes rec 5 from the delegate, takes a read-conflict on
        // boundaryMetaKey, and picks the new boundary by scanning entries via
        // a reverse scan from begin to packed((500, 5)). That scan only covers
        // the original better-than-boundary entries (100..400) — disjoint from
        // the overflow keys the others append above 500. So E's read set never
        // overlaps anyone's write set; all six commit in any order.
        seedWindow(5, Direction.ASC, 100, 200, 300, 400, 500);

        concurrent(5, Direction.ASC)
                .tx("A", () -> rec(6, 700))
                .tx("B", () -> rec(7, 800))
                .tx("C", () -> rec(8, 900))
                .tx("D", () -> rec(9, 1100))
                .tx("E", () -> rec(10, 70)) // maintenance tx
                .tx("F", () -> rec(11, 1200))
                .commitInAnyOrder(new Random(seed))
                .expectNoConflicts();

        // E's eviction landed: rec 5 (the previous boundary) is out, rec 10 is
        // in, the boundary advanced to (400, 4). The five overflow records
        // (rec 6, 7, 8, 9, 11) live in the entries subspace but not the delegate.
        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 4L, 10L);
    }

    @Test
    void ascCompetingEvictionsIntoFullWindowConflict() throws Exception {
        // ASC mirror of descCompetingEvictionsIntoFullWindowConflict.
        // Window is full. Both A=(6, 50) and B=(7, 25) are better than the
        // boundary (500, 5) for ASC, so both take the eviction path. Each calls
        // evictBoundaryAndReplace, which adds a read-conflict on boundaryMetaKey
        // and rewrites it. With both A and B reading AND writing the same
        // boundaryMetaKey, FDB's optimistic concurrency forces one to lose: A
        // commits first (declared order), so B's read overlaps A's write and B
        // fails.
        seedWindow(5, Direction.ASC, 100, 200, 300, 400, 500);

        concurrent(5, Direction.ASC)
                .tx("A", () -> rec(6, 50))
                .tx("B", () -> rec(7, 25))
                .commitAll()
                .expectConflictOn("B")
                .expectCommitted("A");

        // A's eviction landed: rec 5 (the previous boundary) is out, rec 6 is
        // in, the boundary advanced to (400, 4). B was rolled back, so rec 7
        // never reached the delegate.
        assertFinalWindow(5, Direction.ASC, 1L, 2L, 3L, 4L, 6L);
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
        seedWindow(5, Direction.ASC, 10, 20, 30, 40, 50, 60, 70, 80);

        concurrent(5, Direction.ASC)
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
        seedWindow(5, Direction.ASC, 10, 20, 30, 40, 50, 60, 70);

        concurrent(5, Direction.ASC)
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

    @Test
    void ascBetterInsertRacingWithLastInWindowDeleteOrphansEntry() throws Exception {
        // Regression for the symmetric counterpart of
        // ascOverflowInsertConflictsWithConcurrentBoundaryDelete. The
        // companion fix in Type.getBestInOverflow already declares explicit
        // read+write conflict ranges over the slice it scans during
        // re-election, which covers most of the gap. But reElectFromOverflow
        // short-circuits when the post-eviction boundary is null — i.e. when
        // the deleted entry was the *last* in-window entry — so
        // getBestInOverflow never runs in that path, and the conflict
        // tracking has to come from getNewBoundaryAfterEviction itself.
        // Without that, a concurrent better-than-boundary insert could
        // commit cleanly alongside the delete and end up with rec in the
        // delegate but boundary = null, breaking the
        // "window-side == delegate" invariant.
        //
        // Initial state (ASC, window = 5, count = 1):
        //   Window:   rec 1 with relevance 50 — boundary (50, 1).
        //   No overflow.
        //
        // TX A inserts rec(2, 30). count=1 < 5 → not-full branch:
        //   delegate.update(null, rec2), addReadConflictKey on (30, 2),
        //   ADD-1 on counter, snapshot-reads boundary (50, 1). (30, 2) is
        //   better than (50, 1) for ASC, so NO boundary write, NO
        //   addReadConflictKey on boundaryMetaKey.
        //
        // TX B deletes rec 1 (the only in-window entry, also the boundary).
        // handleDelete clears (50, 1), addReadConflictKey on boundaryMetaKey,
        // decrements counter, then re-elects via updateBoundaryAfterDelete →
        // getNewBoundaryAfterEviction's reverse scan returns null (no entries
        // remain). updateBoundaryAfterDelete clears boundaryMetaKey and
        // returns null. reElectFromOverflow then sees
        // currentBoundaryPacked == null and short-circuits — getBestInOverflow
        // never runs.
        //
        // The fix lives inside getNewBoundaryAfterEviction itself: it now
        // declares explicit read+write conflict ranges over the slice it
        // scanned (or the full requested range when the scan returned null).
        // For MIN that's [scannedKey, oldBoundary) — here, [begin,
        // packed((50, 1))) — which covers A's write at (30, 2). So A's
        // read-conflict on (30, 2) intersects B's declared write-conflict
        // slice, and the resolver aborts A.
        //
        // Commit order: B then A. B commits cleanly (no prior committed
        // writes); A's commit sees B's declared write range covering A's
        // read of (30, 2) and is aborted.
        seedWindow(5, Direction.ASC, 50);

        concurrent(5, Direction.ASC)
                .tx("B", () -> deleteRec(1))
                .tx("A", () -> rec(2, 30))
                .commitAll()  // B commits first; A is aborted by the resolver
                .expectCommitted("B")
                .expectConflictOn("A");

        // After the fix: getNewBoundaryAfterEviction declares an explicit
        // read+write conflict range over [begin, oldBoundary) for MIN
        // (covering the full requested range when the scan returns null,
        // as it does here — the entries subspace is empty in B's view
        // after clearing (50, 1)). A's read-conflict on (30, 2) sits
        // inside that range, so the resolver aborts A. B's effects land
        // alone: rec 1 is gone, the window is empty, and the maintainer
        // invariant holds (window-side == delegate == ∅).
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.ASC);
            assertThat(SlidingWindowTestHelpers.slidingWindowViaValueIndex(recordStore, INDEX_NAME))
                    .hasSizeOf(0)
                    .hasEntriesOf()
                    .underlyingValueIndex()
                    .isEmpty();
            commit(context);
        }
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void competingInsertsIntoEmptyWindowConflictOnBoundary(Direction direction) throws Exception {
        // Initial state (window = 5): the window is empty — no entries,
        // boundary unset, count = 0. Direction-agnostic: the conflict pattern
        // here is the same for ASC and DESC.
        //
        // Both A=(1, 100) and B=(2, 200) take the not-full branch of
        // handleInsert (count = 0 < 5). Each snapshot-reads boundaryMetaKey
        // and sees null; the `boundaryBytes == null` branch then both
        // addReadConflictKey on boundaryMetaKey AND tr.set it to its own
        // entry key — every first-insert is forced to establish the boundary.
        // With both A and B reading AND writing the same boundaryMetaKey,
        // FDB's resolver picks one: A commits (declared order), B fails.
        seedWindow(5, direction);

        concurrent(5, direction)
                .tx("A", () -> rec(1, 100))
                .tx("B", () -> rec(2, 200))
                .commitAll()
                .expectCommitted("A")
                .expectConflictOn("B");

        // Only A's insert survives: rec 1 in the delegate, boundary (100, 1),
        // count = 1.
        assertFinalWindow(5, direction, 1L);
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void competingDeletesOfOnlyEntryConflict(Direction direction) throws Exception {
        // Initial state (window = 5): a single in-window entry rec 1 with
        // relevance 50 — it is also the boundary (50, 1). Direction-agnostic:
        // the conflict pattern here is the same for ASC and DESC.
        //
        // Both A and B delete rec 1. Each handleDelete tr.get's (50, 1)
        // — non-snapshot, so a read-conflict on (50, 1) — sees it exists,
        // and clears it. Then each addReadConflictKey on boundaryMetaKey
        // and runs updateBoundaryAfterDelete: entryKey == boundaryEntryKey,
        // so getNewBoundaryAfterEviction scans for a replacement, finds
        // none (only entry is the one being deleted), and clears
        // boundaryMetaKey. reElectFromOverflow short-circuits on the null
        // boundary and ADD-1s the counter.
        //
        // Both transactions therefore write to the same (50, 1) entry key
        // and to boundaryMetaKey. A commits (declared order); B's read of
        // (50, 1) overlaps A's committed write at (50, 1), so the resolver
        // aborts B.
        seedWindow(5, direction, 50);

        concurrent(5, direction)
                .tx("A", () -> deleteRec(1))
                .tx("B", () -> deleteRec(1))
                .commitAll()
                .expectCommitted("A")
                .expectConflictOn("B");

        // A's delete drained the window: no entries, no boundary, count = 0,
        // delegate empty. B was rolled back. The maintainer invariant holds
        // (window-side == delegate == ∅).
        assertFinalWindow(5, direction);
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void competingDeletesOfBoundaryAndInWindowEntryConflict(Direction direction) throws Exception {
        // Initial state (window = 5): two in-window entries — rec 1 (relevance
        // 50) and rec 2 (relevance 100), count = 2. The "worst" entry is the
        // boundary, so the boundary key differs by direction:
        //   ASC/MIN  → boundary = (100, 2) (highest relevance)
        //   DESC/MAX → boundary = (50, 1)  (lowest relevance)
        //
        // Each direction has one tx deleting the boundary and one tx deleting
        // the other in-window entry. The roles flip between ASC and DESC, but
        // the outcome is symmetric: A commits and B aborts.
        //
        // ASC: A deletes rec 1 (not boundary), B deletes rec 2 (boundary).
        //   B's handleDelete runs updateBoundaryAfterDelete because the
        //   deleted entry IS the boundary; getNewBoundaryAfterEviction's
        //   reverse scan finds (50, 1) and declares an explicit
        //   write-conflict range over [(50, 1), packed((100, 2))) — the slice
        //   it actually visited. A's clear of (50, 1) lands inside that
        //   slice; B also has a non-snapshot getRange read over it (from
        //   getRange's implicit read-conflict). A commits first, so B's read
        //   intersects A's committed write at (50, 1) and the resolver
        //   aborts B.
        //
        // DESC: A deletes rec 1 (boundary), B deletes rec 2 (not boundary).
        //   A's handleDelete runs updateBoundaryAfterDelete:
        //   getNewBoundaryAfterEviction's forward scan finds (100, 2),
        //   declares write-conflict over [keyAfter((50, 1)), keyAfter((100,
        //   2))), and rewrites boundaryMetaKey to (100, 2). B's read of
        //   (100, 2) (non-snapshot tr.get inside handleDelete) sits inside
        //   A's declared write slice, and B's addReadConflictKey on
        //   boundaryMetaKey overlaps A's write to it. A commits first, so
        //   the resolver aborts B on either intersection.
        seedWindow(5, direction, 50, 100);

        concurrent(5, direction)
                .tx("A", () -> deleteRec(1))
                .tx("B", () -> deleteRec(2))
                .commitAll()
                .expectCommitted("A")
                .expectConflictOn("B");

        // Only A's delete survives: rec 2 remains in the delegate, count = 1,
        // boundary = (100, 2) for both directions (the only remaining entry,
        // either untouched in ASC or rewritten by A's re-election in DESC).
        assertFinalWindow(5, direction, 2L);
    }

    @Test
    void descNonFullBoundaryDownConflictsWithConcurrentOverflowInFlipSlice() throws Exception {
        // Reproduces the counter-undercount race fixed by the boundary-flip
        // write-conflict in handleInsert's non-full-worse-or-equal branch.
        //
        // Two transactions race against each other with DIFFERENT read versions:
        //
        //   TX-B (R_B taken when count=1, boundary=(35, 1))
        //     saves pk=7 V=9.  At its R, count<windowSize → non-full path.
        //     (9, 7) is worse than (35, 1) for DESC, so it sets boundary=(9, 7).
        //
        //   TX-A (R_A taken when count=5 (full), boundary=(35, 1))
        //     saves pk=6 V=26. Count≥windowSize → full path. (26, 6) is NOT
        //     better than (35, 1), so it takes the silent overflow short-circuit:
        //     it writes entries[(26, 6)] at the top of handleInsert and returns
        //     done. No counter, no delegate, no read-conflict on boundaryMetaKey.
        //
        // With TX-B committing first, the live boundary becomes (9, 7) and
        // (26, 6) — which TX-A wrote into entries-subspace under the
        // assumption it'd be overflow per (35, 1) — is now window-side per
        // (9, 7). It's not in delegate. Counter wasn't bumped. Bug.
        //
        // The fix: when TX-B sets the boundary down, it declares a write-conflict
        // over the slice (9, 7) ≤ X < (35, 1) — exactly the entries that flipped
        // from overflow to window. (26, 6).packedKey sits in that slice, and
        // TX-A has a read-conflict on it (added at the top of handleInsert). The
        // resolver therefore aborts TX-A on commit, preventing the bug.
        final int windowSize = 5;

        // [1] Seed pk=1 with relevance=35 → boundary=(35, 1), count=1.
        try (FDBRecordContext ctx = openContext()) {
            openStore(ctx, windowSize, Direction.DESC);
            rec(1, 35);
            commit(ctx);
        }

        // [2] Open ctxB and pin its read version (R_B) at count=1, boundary=(35, 1).
        final FDBRecordContext ctxB = openContext();
        openStore(ctxB, windowSize, Direction.DESC);
        ctxB.getReadVersion();
        final FDBRecordStore storeB = recordStore;

        // [3] In a separate, independently-committed context, fill the window
        //     with 4 records whose relevance > 35. After this commits, count=5
        //     and the boundary is still (35, 1) (none of these saves rewrites
        //     it because each new entry is BETTER than the boundary — non-full
        //     path with isBetter=true skips the boundary write).
        try (FDBRecordContext ctx = openContext()) {
            openStore(ctx, windowSize, Direction.DESC);
            rec(2, 40);
            rec(3, 50);
            rec(4, 60);
            rec(5, 70);
            commit(ctx);
        }

        // [4] Open ctxA. R_A sees count=5 (full), boundary=(35, 1).
        final FDBRecordContext ctxA = openContext();
        openStore(ctxA, windowSize, Direction.DESC);
        ctxA.getReadVersion();
        final FDBRecordStore storeA = recordStore;

        // [5] TX-A: save pk=6 V=26. Full-path-overflow branch. Silent.
        storeA.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(6)
                .setRelevance(26)
                .build());

        // [6] TX-B: save pk=7 V=9. Non-full-worse-or-equal branch. Sets boundary=(9, 7).
        storeB.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(7)
                .setRelevance(9)
                .build());

        // [7] Commit ctxB. With or without the fix this succeeds — TX-B has
        //     read+write on boundaryMetaKey but no other tx has written it.
        commit(ctxB);

        // [8] Commit ctxA. After the fix, TX-B's flip-slice declared write-range
        //     [(9, 7).packed, (35, 1).packed) covers (26, 6).packedKey, and
        //     TX-A's read-conflict on entries[(26, 6)] makes the resolver
        //     abort it. Without the fix, this commit succeeds and the bug
        //     manifests as window-side > delegate (assertion 1 in
        //     verifySlidingWindowInvariant).
        try {
            commit(ctxA);
            fail("expected ctxA to abort: TX-B's flip-slice write-conflict over "
                    + "[(9, 7), (35, 1)) should intersect TX-A's read-conflict "
                    + "on entries[(26, 6)]");
        } catch (FDBExceptions.FDBStoreTransactionConflictException expected) {
            // Expected: the fix engaged.
        } finally {
            ctxA.close();
        }

        // [9] Verify final state. Only TX-B's effects landed. The maintainer
        //     invariant holds (delegate == window-side, boundary points at a
        //     present entry, counter matches window-side count). After TX-B,
        //     counter went from 5 → 6 (atomic ADD on snapshot=1 applied to
        //     live=5), so countUpperBound = windowSize + 1.
        try (FDBRecordContext ctx = openContext()) {
            openStore(ctx, windowSize, Direction.DESC);
            verifySlidingWindowInvariant(recordStore, INDEX_NAME, windowSize,
                    Direction.DESC, windowSize + 1, windowSize + 1);
            commit(ctx);
        }
    }
}
