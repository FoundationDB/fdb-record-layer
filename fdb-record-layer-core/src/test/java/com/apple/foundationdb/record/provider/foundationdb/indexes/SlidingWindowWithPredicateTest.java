/*
 * SlidingWindowWithPredicateTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexComparison;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the interaction between sliding window semantics and base index predicates.
 *
 * <p>These tests use an AND predicate combining a value predicate (e.g. {@code num_value_2 > threshold})
 * with a {@link IndexPredicate.RowNumberWindowPredicate}. Records that fail the value predicate
 * should be completely invisible to the sliding window: they should not occupy window slots,
 * not enter overflow, and not be candidates for re-election.</p>
 *
 * <p>Assertions verify complete index entries: both the index key ({@code num_value_2}) and the
 * primary key ({@code recNo}), not just presence/absence.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowWithPredicateTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_predicate_index";

    /**
     * Creates a hook with the following configuration.
     * <ul>
     *   <li>Index root expression: {@code num_value_2}</li>
     *   <li>Predicate: AND(num_value_2 > threshold, RowNumberWindow(num_value_3_indexed, direction) <= windowSize)</li>
     * </ul>
     */
    @Nonnull
    private static RecordMetaDataHook hook(int windowSize, @Nonnull Direction direction, int threshold) {
        final KeyExpression wholeKey = Key.Expressions.field("num_value_2");
        final IndexPredicate valuePredicate = new IndexPredicate.ValuePredicate(
                List.of("num_value_2"),
                IndexComparison.fromComparison(
                        new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, threshold)));
        final IndexPredicate.RowNumberWindowPredicate windowPredicate =
                new IndexPredicate.RowNumberWindowPredicate("num_value_3_indexed", direction, windowSize);
        final IndexPredicate andPredicate = new IndexPredicate.AndPredicate(List.of(valuePredicate, windowPredicate));
        return md -> md.addIndex("MySimpleRecord",
                new Index(INDEX_NAME, wholeKey, "value", IndexOptions.EMPTY_OPTIONS, andPredicate));
    }

    /**
     * rec(recNo, numValue2, numValue3Indexed).
     * <ul>
     *   <li>{@code numValue2} — the indexed value AND the value predicate field</li>
     *   <li>{@code numValue3Indexed} — the window key (ranking field)</li>
     * </ul>
     */
    private void rec(int recNo, int value2, int value3) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed("s" + recNo)
                .setNumValue2(value2)
                .setNumValue3Indexed(value3)
                .setNumValueUnique(recNo)
                .build());
    }

    private void deleteRec(int recNo) {
        recordStore.deleteRecord(Tuple.from((long) recNo));
    }

    /**
     * Scans the index and returns a map of recNo → indexKey (num_value_2) for each entry.
     * This verifies both the primary key and the indexed value.
     */
    @Nonnull
    private Map<Long, Long> scanIndexEntries() {
        final List<IndexEntry> entries = recordStore
                .scanIndex(recordStore.getRecordMetaData().getIndex(INDEX_NAME),
                        IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                .asList()
                .join();
        return entries.stream().collect(Collectors.toMap(
                e -> e.getPrimaryKey().getLong(0),   // recNo
                e -> e.getKey().getLong(0)            // num_value_2 (index key)
        ));
    }

    /**
     * Asserts the index contains exactly the expected entries as {recNo → num_value_2} pairs.
     */
    private void assertIndexEntries(@Nonnull Map<Long, Long> expected) {
        final Map<Long, Long> actual = scanIndexEntries();
        assertEquals(expected, actual,
                "Index entries (recNo → num_value_2) should be " + expected + " but was " + actual);
    }

    // ===== Records failing the value predicate should be invisible to the window =====

    @Test
    void filteredRecordsDoNotOccupyWindowSlots() {
        // Window size 2, DESC (keep highest num_value_3_indexed), filter: num_value_2 > 5
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC, 5));

            // These pass the filter (num_value_2 > 5)
            rec(1, 10, 100);  // window
            rec(2, 20, 200);  // window
            assertIndexEntries(Map.of(1L, 10L, 2L, 20L));

            // This does NOT pass the filter (num_value_2 = 3 <= 5) → completely invisible
            rec(3, 3, 999);
            // rec3 should NOT take a window slot even though its window key (999) is highest
            assertIndexEntries(Map.of(1L, 10L, 2L, 20L));

            commit(context);
        }
    }

    @Test
    void filteredRecordsDoNotEvictWindowEntries() {
        // Window size 2, DESC, filter: num_value_2 > 5
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC, 5));

            rec(1, 10, 100);  // window
            rec(2, 20, 200);  // window
            assertIndexEntries(Map.of(1L, 10L, 2L, 20L));

            // rec3 fails filter, should NOT cause eviction even though 999 > 100
            rec(3, 2, 999);
            assertIndexEntries(Map.of(1L, 10L, 2L, 20L));

            // rec4 passes filter and evicts rec1 (lowest window key in DESC)
            rec(4, 30, 300);
            assertIndexEntries(Map.of(2L, 20L, 4L, 30L));

            commit(context);
        }
    }

    @Test
    void filteredRecordsDoNotEnterOverflow() {
        // Window size 2, DESC, filter: num_value_2 > 5
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC, 5));

            rec(1, 10, 200);  // window
            rec(2, 20, 300);  // window

            // rec3 fails filter → should NOT go to overflow
            rec(3, 1, 50);

            assertIndexEntries(Map.of(1L, 10L, 2L, 20L));

            // Delete rec1 → if rec3 were in overflow, it would be promoted (wrong)
            deleteRec(1);
            // Only rec2 should remain — no re-election from overflow
            assertIndexEntries(Map.of(2L, 20L));

            commit(context);
        }
    }

    @Test
    void onlyFilteredInRecordsFillWindow() {
        // Window size 3, DESC, filter: num_value_2 > 5
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(3, Direction.DESC, 5));

            // Mix of passing and failing records
            rec(1, 3, 100);   // fails filter
            rec(2, 10, 200);  // passes → window
            rec(3, 1, 300);   // fails filter
            rec(4, 20, 400);  // passes → window
            rec(5, 4, 500);   // fails filter
            rec(6, 30, 600);  // passes → window (fills window to 3)

            // Only the 3 records that pass the filter should be in the window
            assertIndexEntries(Map.of(2L, 10L, 4L, 20L, 6L, 30L));

            commit(context);
        }
    }

    @Test
    void evictionOnlyAmongFilteredRecords() {
        // Window size 2, DESC, filter: num_value_2 > 5
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC, 5));

            rec(1, 10, 100);  // passes → window
            rec(2, 20, 200);  // passes → window (full)

            // Fails filter → ignored
            rec(3, 2, 999);
            assertIndexEntries(Map.of(1L, 10L, 2L, 20L));

            // Passes filter, evicts rec1 (lowest window key in DESC)
            rec(4, 15, 300);
            assertIndexEntries(Map.of(2L, 20L, 4L, 15L));

            // Passes filter but worse than worst in window (200) → overflow
            rec(5, 25, 150);
            assertIndexEntries(Map.of(2L, 20L, 4L, 15L));

            // Delete rec4 → re-elect rec5 from overflow
            deleteRec(4);
            assertIndexEntries(Map.of(2L, 20L, 5L, 25L));

            commit(context);
        }
    }

    @Test
    void reElectionOnlyConsidersFilteredRecords() {
        // Window size 2, DESC, filter: num_value_2 > 5
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook(2, Direction.DESC, 5));

            rec(1, 10, 300);  // passes → window
            rec(2, 20, 200);  // passes → window (full)
            rec(3, 15, 100);  // passes → overflow (100 < 200 in DESC)

            // rec4 fails filter → should NOT be in overflow for re-election
            rec(4, 3, 150);

            assertIndexEntries(Map.of(1L, 10L, 2L, 20L));

            // Delete rec1 → should promote rec3 (not rec4 even though 150 > 100)
            deleteRec(1);
            assertIndexEntries(Map.of(2L, 20L, 3L, 15L));

            commit(context);
        }
    }

    @Test
    void rebuildWithMixedFilteredRecords() {
        final RecordMetaDataHook theHook = hook(2, Direction.DESC, 5);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, theHook);
            rec(1, 3, 500);   // fails filter
            rec(2, 10, 200);  // passes
            rec(3, 1, 400);   // fails filter
            rec(4, 20, 300);  // passes
            rec(5, 4, 600);   // fails filter
            rec(6, 30, 100);  // passes (but worse window key for DESC)
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, theHook);
            recordStore.rebuildAllIndexes().join();
            // DESC window=2 among passing records {rec2(v2=10,v3=200), rec4(v2=20,v3=300), rec6(v2=30,v3=100)}:
            // keeps highest v3 → {rec2, rec4}
            assertIndexEntries(Map.of(2L, 10L, 4L, 20L));
            commit(context);
        }
    }
}
