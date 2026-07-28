/*
 * SlidingWindowIndexMetricsTest.java
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

import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainer.SlidingWindowCounter;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainer.SlidingWindowEvent;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainer.SlidingWindowSizeEvent;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.sampleVector;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that the {@link SlidingWindowIndexMaintainer} reports the expected counters
 * and timing events through {@link FDBStoreTimer} for each operational path.
 *
 * <p>Each test exercises one code path and asserts that the corresponding
 * {@link SlidingWindowCounter} or {@link SlidingWindowEvent} fires the expected
 * number of times. Counters that require a corruption scenario to fire
 * ({@code EVICTED_RECORD_MISSING}, {@code PROMOTED_RECORD_MISSING}) and
 * {@code DELETE_UNTRACKED} (which requires window-key divergence between an indexed
 * entry and its later delete) are not exercised here because they are not reachable
 * via the public API without manual subspace manipulation.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowIndexMetricsTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_vector_index";
    private static final int VECTOR_DIMS = 4;

    @BeforeEach
    void resetTimer() {
        timer.reset();
    }

    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction) {
        openStore(context, windowSize, direction, ImmutableList.of());
    }

    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction,
                           @Nonnull List<List<String>> groupingFields) {
        createOrOpenRecordStore(context,
                SlidingWindowTestHelpers.buildSlidingWindowVectorMetaData(
                        INDEX_NAME, windowSize, VECTOR_DIMS, direction, groupingFields));
    }

    private void rec(long recNo, long relevance) {
        rec(recNo, "z", "c", relevance, sampleVector());
    }

    private void rec(long recNo, String zone, String category, long relevance, HalfRealVector vector) {
        recordStore.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(recNo)
                .setZone(zone)
                .setCategory(category)
                .setRelevance(relevance)
                .setScore(0)
                .setVectorData(ByteString.copyFrom(vector.getRawData()))
                .build());
    }

    private void deleteRec(long recNo) {
        recordStore.deleteRecord(Tuple.from(recNo));
    }

    @Nonnull
    private IndexMaintainer maintainer() {
        final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
        return recordStore.getIndexMaintainer(index);
    }

    private long count(@Nonnull SlidingWindowCounter counter) {
        return timer.getCount(counter);
    }

    private long eventCount(@Nonnull SlidingWindowEvent event) {
        return timer.getCount(event);
    }

    private long timeNanos(@Nonnull SlidingWindowEvent event) {
        return timer.getTimeNanos(event);
    }

    private long sizeEventCount(@Nonnull SlidingWindowSizeEvent event) {
        return timer.getCount(event);
    }

    private long cumulativeSize(@Nonnull SlidingWindowSizeEvent event) {
        return timer.getSize(event);
    }

    // ===== Insert path counters =====

    @Test
    void itemAddedToWindowFillingFiresOnEachInsertWhileFilling() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);

            assertEquals(3, count(SlidingWindowCounter.SW_ITEM_ADDED_TO_WINDOW_FILLING));
            assertEquals(0, count(SlidingWindowCounter.SW_ITEM_ADDED_TO_ENTRIES_ONLY));
            assertEquals(0, eventCount(SlidingWindowEvent.SW_EVICT_AND_REPLACE));
            commit(context);
        }
    }

    @Test
    void itemAddedToEntriesOnlyFiresWhenOverflowed() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertEquals(3, count(SlidingWindowCounter.SW_ITEM_ADDED_TO_WINDOW_FILLING));

            rec(4, 50);  // worse than boundary (100) for DESC → overflow

            assertEquals(1, count(SlidingWindowCounter.SW_ITEM_ADDED_TO_ENTRIES_ONLY));
            assertEquals(0, eventCount(SlidingWindowEvent.SW_EVICT_AND_REPLACE));
            commit(context);
        }
    }

    // ===== Delete path counters =====

    @Test
    void overflowEntryDeletedFiresWhenDeletingOverflowRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 50);  // overflow

            deleteRec(4);

            assertEquals(1, count(SlidingWindowCounter.SW_OVERFLOW_ENTRY_DELETED));
            assertEquals(0, count(SlidingWindowCounter.SW_WINDOW_ENTRY_DELETED));
            assertEquals(0, eventCount(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_DELETE));
            assertEquals(0, count(SlidingWindowCounter.SW_ITEM_PROMOTED_FROM_OVERFLOW));
            commit(context);
        }
    }

    @Test
    void windowEntryDeletedFiresWhenDeletingNonBoundaryInWindow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);  // boundary for DESC (lowest)
            rec(2, 200);
            rec(3, 300);

            deleteRec(2);  // in window, not boundary

            assertEquals(1, count(SlidingWindowCounter.SW_WINDOW_ENTRY_DELETED));
            assertEquals(0, eventCount(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_DELETE));
            commit(context);
        }
    }

    @Test
    void boundaryDeleteTriggersRescanAfterDeleteEvent() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);  // boundary for DESC (lowest)
            rec(2, 200);
            rec(3, 300);

            deleteRec(1);  // delete the boundary

            assertEquals(0, count(SlidingWindowCounter.SW_WINDOW_ENTRY_DELETED));
            assertEquals(1, eventCount(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_DELETE));
            assertTrue(timeNanos(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_DELETE) > 0,
                    "SW_BOUNDARY_RESCAN_AFTER_DELETE duration should be positive");
            commit(context);
        }
    }

    @Test
    void itemPromotedFromOverflowFiresAfterWindowDeleteWhenOverflowAvailable() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 50);  // overflow

            deleteRec(2);  // in-window delete; should promote rec4 from overflow

            assertEquals(1, count(SlidingWindowCounter.SW_WINDOW_ENTRY_DELETED));
            assertEquals(1, count(SlidingWindowCounter.SW_ITEM_PROMOTED_FROM_OVERFLOW));
            assertEquals(0, count(SlidingWindowCounter.SW_WINDOW_SHRUNK_NO_OVERFLOW));
            commit(context);
        }
    }

    @Test
    void windowShrunkNoOverflowFiresWhenNothingToPromote() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);

            deleteRec(2);  // no overflow exists; window simply shrinks

            assertEquals(1, count(SlidingWindowCounter.SW_WINDOW_ENTRY_DELETED));
            assertEquals(1, count(SlidingWindowCounter.SW_WINDOW_SHRUNK_NO_OVERFLOW));
            assertEquals(0, count(SlidingWindowCounter.SW_ITEM_PROMOTED_FROM_OVERFLOW));
            commit(context);
        }
    }

    @Test
    void partitionEmptiedFiresWhenLastEntryRemoved() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);

            deleteRec(1);  // boundary delete with no other entries

            assertEquals(1, count(SlidingWindowCounter.SW_PARTITION_EMPTIED));
            assertEquals(1, eventCount(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_DELETE));
            commit(context);
        }
    }

    // ===== Special operations =====

    @Test
    void preemptiveDeleteWriteOnlyFiresOnUpdateWhileWriteOnly() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            final FDBStoredRecord<Message> stored = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(stored);

            timer.reset();

            maintainer().updateWhileWriteOnly(null, stored).join();

            assertEquals(1, count(SlidingWindowCounter.SW_PREEMPTIVE_DELETE_WRITE_ONLY));
            commit(context);
        }
    }

    @Test
    void partitionClearedFiresOnDeleteWhere() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));
            rec(1, "A", "c", 100, sampleVector());
            rec(2, "A", "c", 200, sampleVector());
            rec(3, "B", "c", 300, sampleVector());

            maintainer().deleteWhere(context.ensureActive(), Tuple.from("A")).join();

            assertEquals(1, count(SlidingWindowCounter.SW_PARTITION_CLEARED));
            commit(context);
        }
    }

    @Test
    void preemptiveDeleteWriteOnlyFiresOncePerUpdateFromQueue() throws Exception {
        // Draining pending writes queue must increment the preemptive-delete counter exactly once.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            final FDBStoredRecord<Message> stored = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(stored);
            final Any entry = maintainer().serializePendingWriteQueue(null, stored);

            timer.reset();
            maintainer().updateFromQueue(entry).join();

            assertEquals(1, count(SlidingWindowCounter.SW_PREEMPTIVE_DELETE_WRITE_ONLY),
                    "updateFromQueue must not double-count the preemptive-delete counter");
            commit(context);
        }
    }

    // ===== Timer events =====

    @Test
    void evictAndReplaceEventIsTimed() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);

            rec(3, 300);  // evicts rec1

            assertEquals(1, eventCount(SlidingWindowEvent.SW_EVICT_AND_REPLACE));
            assertTrue(timeNanos(SlidingWindowEvent.SW_EVICT_AND_REPLACE) > 0,
                    "SW_EVICT_AND_REPLACE duration should be positive");
            commit(context);
        }
    }

    @Test
    void reElectFromOverflowEventIsTimed() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 50);  // overflow (worse than boundary 100)

            deleteRec(1);  // window delete → re-elect from overflow

            assertEquals(1, eventCount(SlidingWindowEvent.SW_RE_ELECT_FROM_OVERFLOW));
            assertTrue(timeNanos(SlidingWindowEvent.SW_RE_ELECT_FROM_OVERFLOW) > 0,
                    "SW_RE_ELECT_FROM_OVERFLOW duration should be positive");
            commit(context);
        }
    }

    @Test
    void evictionTriggersRescanAfterEvictEvent() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);

            rec(3, 300);  // eviction → rescan from the eviction path only

            assertEquals(1, eventCount(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_EVICT));
            assertEquals(0, eventCount(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_DELETE));
            assertTrue(timeNanos(SlidingWindowEvent.SW_BOUNDARY_RESCAN_AFTER_EVICT) > 0,
                    "SW_BOUNDARY_RESCAN_AFTER_EVICT duration should be positive");
            commit(context);
        }
    }

    @Test
    void delegateInsertEventFiresOnFillAndOnEvictionAndOnPromotion() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);

            // Fill phase: each record goes into the delegate (2 inserts).
            rec(1, 100);
            rec(2, 200);
            assertEquals(2, eventCount(SlidingWindowEvent.SW_DELEGATE_INSERT));

            // Eviction: insert rec3 into the delegate (3 inserts total).
            rec(3, 300);
            assertEquals(3, eventCount(SlidingWindowEvent.SW_DELEGATE_INSERT));

            // Overflow add does NOT touch the delegate.
            rec(4, 50);
            assertEquals(3, eventCount(SlidingWindowEvent.SW_DELEGATE_INSERT));

            // Window delete + promotion from overflow inserts rec4 into the delegate (4 total).
            deleteRec(2);
            assertEquals(4, eventCount(SlidingWindowEvent.SW_DELEGATE_INSERT));

            assertTrue(timeNanos(SlidingWindowEvent.SW_DELEGATE_INSERT) > 0,
                    "SW_DELEGATE_INSERT duration should be positive");
            commit(context);
        }
    }

    @Test
    void delegateDeleteEventFiresOnEvictionAndOnWindowDelete() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            assertEquals(0, eventCount(SlidingWindowEvent.SW_DELEGATE_DELETE));

            // Eviction removes rec1 from the delegate.
            rec(3, 300);
            assertEquals(1, eventCount(SlidingWindowEvent.SW_DELEGATE_DELETE));

            // Overflow add does NOT touch the delegate, neither for insert nor for delete.
            rec(4, 50);
            assertEquals(1, eventCount(SlidingWindowEvent.SW_DELEGATE_DELETE));

            // Overflow delete does NOT touch the delegate either.
            deleteRec(4);
            assertEquals(1, eventCount(SlidingWindowEvent.SW_DELEGATE_DELETE));

            // Window delete removes rec2 from the delegate.
            deleteRec(2);
            assertEquals(2, eventCount(SlidingWindowEvent.SW_DELEGATE_DELETE));

            assertTrue(timeNanos(SlidingWindowEvent.SW_DELEGATE_DELETE) > 0,
                    "SW_DELEGATE_DELETE duration should be positive");
            commit(context);
        }
    }

    // ===== Robustness =====

    @Test
    void windowCountSizeEventTracksSizeMutations() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC);

            rec(1, 100);  // count → 1
            rec(2, 200);  // count → 2
            rec(3, 300);  // count → 3
            // Window full; eviction does not change count, so no size event recorded.
            rec(4, 400);
            // Re-election bumps count back to 3 after a window delete.
            rec(5, 50);   // overflow (no count change)
            deleteRec(2); // count → 2, then promotion bumps to 3

            // Three fills (1,2,3) + delete (2) + promotion (3) = 5 size events.
            // Cumulative sum = 1 + 2 + 3 + 2 + 3 = 11.
            assertEquals(5, sizeEventCount(SlidingWindowSizeEvent.SW_WINDOW_COUNT));
            assertEquals(11, cumulativeSize(SlidingWindowSizeEvent.SW_WINDOW_COUNT));
            commit(context);
        }
    }
}
