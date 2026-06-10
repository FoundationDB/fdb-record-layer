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
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainer.SlidingWindowCounter;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainer.SlidingWindowEvent;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowTestHelpers.sampleVector;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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

    private FDBStoreTimer timer;

    /**
     * Opens an FDB context with a fresh timer attached so each test can read counters
     * without bleed-over from the parent test base.
     */
    @Nonnull
    private FDBRecordContext openTimerContext() {
        timer = new FDBStoreTimer();
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setTimer(timer)
                .build();
        return fdb.openContext(config);
    }

    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction) throws Exception {
        openStore(context, windowSize, direction, ImmutableList.of());
    }

    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction,
                           @Nonnull List<List<String>> groupingFields) throws Exception {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsSlidingWindowVectorProto.getDescriptor());
        metaDataBuilder.getRecordType("SlidingWindowVectorRecord")
                .setPrimaryKey(Key.Expressions.field("rec_no"));

        final IndexPredicate.RowNumberWindowPredicate windowPredicate =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("relevance"), direction, windowSize, groupingFields);

        final Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name());
        options.put(IndexOptions.HNSW_NUM_DIMENSIONS, Integer.toString(VECTOR_DIMS));

        final KeyExpression keyExpr;
        if (groupingFields.isEmpty()) {
            keyExpr = new KeyWithValueExpression(Key.Expressions.field("vector_data"), 0);
        } else {
            KeyExpression prefix = Key.Expressions.field(groupingFields.get(0).get(0));
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

    // ===== Insert path counters =====

    @Test
    void itemAddedToWindowFillingFiresOnEachInsertWhileFilling() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);

            assertEquals(3, count(SlidingWindowCounter.ITEM_ADDED_TO_WINDOW_FILLING));
            assertEquals(0, count(SlidingWindowCounter.ITEM_ADDED_TO_ENTRIES_ONLY));
            assertEquals(0, count(SlidingWindowCounter.BOUNDARY_EVICTED_AND_REPLACED));
            commit(context);
        }
    }

    @Test
    void itemAddedToEntriesOnlyFiresWhenOverflowed() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            assertEquals(3, count(SlidingWindowCounter.ITEM_ADDED_TO_WINDOW_FILLING));

            rec(4, 50);  // worse than boundary (100) for DESC → overflow

            assertEquals(1, count(SlidingWindowCounter.ITEM_ADDED_TO_ENTRIES_ONLY));
            assertEquals(0, count(SlidingWindowCounter.BOUNDARY_EVICTED_AND_REPLACED));
            commit(context);
        }
    }

    @Test
    void boundaryEvictedAndReplacedFiresWhenWindowFullAndBetter() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);

            rec(4, 400);  // better than boundary (100) for DESC → eviction

            assertEquals(1, count(SlidingWindowCounter.BOUNDARY_EVICTED_AND_REPLACED));
            assertEquals(0, count(SlidingWindowCounter.ITEM_ADDED_TO_ENTRIES_ONLY));
            commit(context);
        }
    }

    // ===== Delete path counters =====

    @Test
    void overflowEntryDeletedFiresWhenDeletingOverflowRecord() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 50);  // overflow

            deleteRec(4);

            assertEquals(1, count(SlidingWindowCounter.OVERFLOW_ENTRY_DELETED));
            assertEquals(0, count(SlidingWindowCounter.WINDOW_ENTRY_DELETED));
            assertEquals(0, count(SlidingWindowCounter.BOUNDARY_ENTRY_DELETED));
            assertEquals(0, count(SlidingWindowCounter.ITEM_PROMOTED_FROM_OVERFLOW));
            commit(context);
        }
    }

    @Test
    void windowEntryDeletedFiresWhenDeletingNonBoundaryInWindow() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);  // boundary for DESC (lowest)
            rec(2, 200);
            rec(3, 300);

            deleteRec(2);  // in window, not boundary

            assertEquals(1, count(SlidingWindowCounter.WINDOW_ENTRY_DELETED));
            assertEquals(0, count(SlidingWindowCounter.BOUNDARY_ENTRY_DELETED));
            commit(context);
        }
    }

    @Test
    void boundaryEntryDeletedFiresWhenDeletingTheBoundary() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);  // boundary for DESC (lowest)
            rec(2, 200);
            rec(3, 300);

            deleteRec(1);  // delete the boundary

            assertEquals(1, count(SlidingWindowCounter.BOUNDARY_ENTRY_DELETED));
            assertEquals(0, count(SlidingWindowCounter.WINDOW_ENTRY_DELETED));
            commit(context);
        }
    }

    @Test
    void itemPromotedFromOverflowFiresAfterWindowDeleteWhenOverflowAvailable() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);
            rec(4, 50);  // overflow

            deleteRec(2);  // in-window delete; should promote rec4 from overflow

            assertEquals(1, count(SlidingWindowCounter.WINDOW_ENTRY_DELETED));
            assertEquals(1, count(SlidingWindowCounter.ITEM_PROMOTED_FROM_OVERFLOW));
            assertEquals(0, count(SlidingWindowCounter.WINDOW_SHRUNK_NO_OVERFLOW));
            commit(context);
        }
    }

    @Test
    void windowShrunkNoOverflowFiresWhenNothingToPromote() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 5, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 300);

            deleteRec(2);  // no overflow exists; window simply shrinks

            assertEquals(1, count(SlidingWindowCounter.WINDOW_ENTRY_DELETED));
            assertEquals(1, count(SlidingWindowCounter.WINDOW_SHRUNK_NO_OVERFLOW));
            assertEquals(0, count(SlidingWindowCounter.ITEM_PROMOTED_FROM_OVERFLOW));
            commit(context);
        }
    }

    @Test
    void partitionEmptiedFiresWhenLastEntryRemoved() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);

            deleteRec(1);  // boundary delete with no other entries

            assertEquals(1, count(SlidingWindowCounter.BOUNDARY_ENTRY_DELETED));
            assertEquals(1, count(SlidingWindowCounter.PARTITION_EMPTIED));
            commit(context);
        }
    }

    // ===== Special operations =====

    @Test
    void preemptiveDeleteWriteOnlyFiresOnUpdateWhileWriteOnly() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC);
            rec(1, 100);
            final FDBStoredRecord<Message> stored = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(stored);

            timer.reset();

            maintainer().updateWhileWriteOnly(null, stored).join();

            assertEquals(1, count(SlidingWindowCounter.PREEMPTIVE_DELETE_WRITE_ONLY));
            commit(context);
        }
    }

    @Test
    void partitionClearedFiresOnDeleteWhere() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 3, Direction.DESC, ImmutableList.of(ImmutableList.of("zone")));
            rec(1, "A", "c", 100, sampleVector());
            rec(2, "A", "c", 200, sampleVector());
            rec(3, "B", "c", 300, sampleVector());

            maintainer().deleteWhere(context.ensureActive(), Tuple.from("A")).join();

            assertEquals(1, count(SlidingWindowCounter.PARTITION_CLEARED));
            commit(context);
        }
    }

    // ===== Timer events =====

    @Test
    void evictAndReplaceEventIsTimed() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);

            rec(3, 300);  // evicts rec1

            assertEquals(1, eventCount(SlidingWindowEvent.EVICT_AND_REPLACE));
            assertTrue(timeNanos(SlidingWindowEvent.EVICT_AND_REPLACE) > 0,
                    "EVICT_AND_REPLACE duration should be positive");
            commit(context);
        }
    }

    @Test
    void reElectFromOverflowEventIsTimed() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);
            rec(3, 50);  // overflow (worse than boundary 100)

            deleteRec(1);  // window delete → re-elect from overflow

            assertEquals(1, eventCount(SlidingWindowEvent.RE_ELECT_FROM_OVERFLOW));
            assertTrue(timeNanos(SlidingWindowEvent.RE_ELECT_FROM_OVERFLOW) > 0,
                    "RE_ELECT_FROM_OVERFLOW duration should be positive");
            commit(context);
        }
    }

    @Test
    void boundaryRescanEventFiresOnEvictionAndOnBoundaryDelete() throws Exception {
        try (FDBRecordContext context = openTimerContext()) {
            openStore(context, 2, Direction.DESC);
            rec(1, 100);
            rec(2, 200);

            // Eviction path: triggers one BOUNDARY_RESCAN.
            rec(3, 300);
            assertEquals(1, eventCount(SlidingWindowEvent.BOUNDARY_RESCAN));

            // Boundary-delete path: triggers a second BOUNDARY_RESCAN.
            // After the eviction above, window holds {2,3}; boundary for DESC is rec2 (lowest).
            deleteRec(2);
            assertEquals(2, eventCount(SlidingWindowEvent.BOUNDARY_RESCAN));
            assertTrue(timeNanos(SlidingWindowEvent.BOUNDARY_RESCAN) > 0,
                    "BOUNDARY_RESCAN duration should be positive");
            commit(context);
        }
    }

    // ===== Robustness =====

    @Test
    void operationsDoNotFailWhenContextHasNoTimer() throws Exception {
        // Opens via the parent test base, which does not attach an FDBStoreTimer.
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC);

            assertDoesNotThrow(() -> {
                rec(1, 100);
                rec(2, 200);
                rec(3, 300);  // eviction
                rec(4, 50);   // overflow
                deleteRec(2); // re-election
                deleteRec(4); // overflow delete
            });
            commit(context);
        }
    }
}
