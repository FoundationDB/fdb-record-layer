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

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic sanity tests for {@link SlidingWindowIndexMaintainer} when the underlying
 * delegate is a {@link com.apple.foundationdb.record.metadata.IndexTypes#VALUE} index rather
 * than the usual HNSW vector index.
 *
 * <p>The VALUE-delegate path is exercised here to support the concurrency stress tests in
 * {@link SlidingWindowIndexConcurrencyTest}, which need a delegate that can sustain a high
 * mutation rate. These tests guard against regressions in the basic eviction / re-election
 * paths when the delegate is a value index, independently of any concurrent workload.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowValueIndexTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_value_index";
    private static final int WINDOW_SIZE = 5;

    /**
     * Adds a value-delegate sliding-window index on {@code MySimpleRecord.num_value_2}.
     * Window key = num_value_2. Index key = num_value_2 (so the value index orders entries
     * by the same field that drives windowing).
     */
    private void openStore(@Nonnull FDBRecordContext context, @Nonnull Direction direction) {
        openSimpleRecordStore(context, metaDataBuilder -> {
            final IndexPredicate.RowNumberWindowPredicate predicate =
                    new IndexPredicate.RowNumberWindowPredicate(
                            ImmutableList.of("num_value_2"), direction, WINDOW_SIZE, ImmutableList.of());
            metaDataBuilder.addIndex("MySimpleRecord",
                    new Index(INDEX_NAME, Key.Expressions.field("num_value_2"), IndexTypes.VALUE,
                            java.util.Collections.emptyMap(), predicate));
        });
    }

    private void save(long recNo, int numValue2) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed("x")
                .setNumValue2(numValue2)
                .setNumValue3Indexed(0)
                .build());
    }

    private void delete(long recNo) {
        recordStore.deleteRecord(Tuple.from(recNo));
    }

    private Set<Long> delegatePks() {
        return SlidingWindowTestHelpers.scanValueIndexRecNos(recordStore, INDEX_NAME);
    }

    private long count() {
        return SlidingWindowTestHelpers.readWindowCount(recordStore, INDEX_NAME, null);
    }

    private void verify(@Nonnull Direction direction) {
        // No concurrency in this test, so the count must equal windowSize at most.
        SlidingWindowTestHelpers.verifySlidingWindowInvariant(recordStore, INDEX_NAME, WINDOW_SIZE, direction, WINDOW_SIZE);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Direction.class)
    void insertBelowWindowSize(@Nonnull Direction direction) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, direction);
            save(1, 100);
            save(2, 200);
            save(3, 300);

            assertEquals(3L, count());
            assertEquals(Set.of(1L, 2L, 3L), delegatePks());
            verify(direction);
            commit(context);
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Direction.class)
    void insertEvictsBoundary(@Nonnull Direction direction) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, direction);
            // Fill window with widely-spaced relevances so the boundary is unambiguous.
            for (int i = 1; i <= WINDOW_SIZE; i++) {
                save(i, i * 100);
            }
            assertEquals(WINDOW_SIZE, count());
            assertEquals(Set.of(1L, 2L, 3L, 4L, 5L), delegatePks());

            // Insert a record that is strictly better than every current window entry.
            // ASC keeps the smallest, DESC keeps the largest.
            final int betterValue = direction == Direction.ASC ? 1 : 9999;
            save(99, betterValue);

            // The new record must be in the delegate.
            assertEquals(WINDOW_SIZE, count());
            final Set<Long> pks = delegatePks();
            assertTrue(pks.contains(99L),
                    "evicted-by-better insert: new record not present in delegate, got " + pks);
            // Exactly one of the original five must have been evicted.
            assertEquals(WINDOW_SIZE, pks.size());
            verify(direction);
            commit(context);
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Direction.class)
    void insertBelowBoundaryWhenFull(@Nonnull Direction direction) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, direction);
            for (int i = 1; i <= WINDOW_SIZE; i++) {
                save(i, i * 100);
            }
            final Set<Long> before = delegatePks();

            // Insert a worse record: should land in overflow only, not in delegate.
            final int worseValue = direction == Direction.ASC ? 9999 : -1;
            save(99, worseValue);

            assertEquals(WINDOW_SIZE, count());
            assertEquals(before, delegatePks(),
                    "delegate must be unchanged after inserting a worse record into a full window");
            verify(direction);
            commit(context);
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Direction.class)
    void deleteFromOverflow(@Nonnull Direction direction) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, direction);
            for (int i = 1; i <= WINDOW_SIZE; i++) {
                save(i, i * 100);
            }
            // Add an overflow record.
            final int worseValue = direction == Direction.ASC ? 9999 : -1;
            save(99, worseValue);
            final Set<Long> beforeDelegate = delegatePks();
            final long beforeCount = count();

            // Delete the overflow record.
            delete(99);

            assertEquals(beforeCount, count(), "deleting an overflow entry must not change the count");
            assertEquals(beforeDelegate, delegatePks(), "deleting an overflow entry must not change the delegate");
            verify(direction);
            commit(context);
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Direction.class)
    void deleteBoundaryReElects(@Nonnull Direction direction) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, direction);
            // Fill the window.
            for (int i = 1; i <= WINDOW_SIZE; i++) {
                save(i, i * 100);
            }
            // Add an overflow record adjacent to the boundary (slightly worse than it).
            // ASC: boundary is the largest (rec 5 @ 500). Overflow record at 600.
            // DESC: boundary is the smallest (rec 1 @ 100). Overflow record at 50.
            final int overflowValue = direction == Direction.ASC ? 600 : 50;
            save(99, overflowValue);
            assertEquals(WINDOW_SIZE, count());

            // The boundary record is the worst in-window record. For our wide spacing it's
            // the one with the largest value (ASC) or smallest value (DESC).
            final long boundaryPk = direction == Direction.ASC ? 5L : 1L;

            // Delete the boundary: the overflow record (recNo 99) should be promoted into
            // the window and become the new boundary.
            delete(boundaryPk);

            assertEquals(WINDOW_SIZE, count(), "count must remain windowSize after re-election from overflow");
            final Set<Long> pks = delegatePks();
            assertTrue(pks.contains(99L),
                    "promoted overflow record must be in the delegate after re-election, got " + pks);
            assertFalse(pks.contains(boundaryPk),
                    "deleted boundary record must not be in the delegate, got " + pks);
            verify(direction);
            commit(context);
        }
    }

    /**
     * When the window count is below {@code windowSize}, every in-pool record must be in the
     * delegate regardless of how its window key compares to the others.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(Direction.class)
    void underfilledWindowKeepsEverything(@Nonnull Direction direction) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, direction);
            // Mix order of inserts to exercise boundary-updates while underfilled.
            save(3, 300);
            save(1, 100);
            save(2, 200);

            assertEquals(3L, count());
            assertEquals(Set.of(1L, 2L, 3L), delegatePks());
            verify(direction);
            commit(context);
        }
    }
}
