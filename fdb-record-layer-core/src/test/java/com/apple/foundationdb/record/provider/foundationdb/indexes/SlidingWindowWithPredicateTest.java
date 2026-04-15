/*
 * SlidingWindowWithPredicateTest.java
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
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexComparison;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto;
import com.apple.foundationdb.record.slidingwindowvector.TestRecordsSlidingWindowVectorProto.SlidingWindowVectorRecord;
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the interaction between sliding window semantics and base index predicates,
 * using an HNSW vector delegate.
 *
 * <p>These tests use an AND predicate combining a value predicate (e.g. {@code score > threshold})
 * with a {@link IndexPredicate.RowNumberWindowPredicate}. Records that fail the value predicate
 * should be completely invisible to the sliding window.</p>
 */
@Tag(Tags.RequiresFDB)
class SlidingWindowWithPredicateTest extends FDBRecordStoreTestBase {

    private static final String INDEX_NAME = "sw_predicate_vector_index";
    private static final int VECTOR_DIMS = 4;

    private static HalfRealVector makeVector(float... values) {
        final Half[] components = new Half[values.length];
        for (int i = 0; i < values.length; i++) {
            components[i] = Half.valueOf(values[i]);
        }
        return new HalfRealVector(components);
    }

    /**
     * Opens a store with a sliding window HNSW index AND a value predicate on score.
     * Predicate: AND(score > threshold, RowNumberWindow(relevance, direction) <= windowSize).
     */
    private void openStore(@Nonnull FDBRecordContext context, int windowSize,
                           @Nonnull Direction direction, int threshold) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsSlidingWindowVectorProto.getDescriptor());
        metaDataBuilder.getRecordType("SlidingWindowVectorRecord")
                .setPrimaryKey(Key.Expressions.field("rec_no"));

        final IndexPredicate valuePredicate = new IndexPredicate.ValuePredicate(
                List.of("score"),
                IndexComparison.fromComparison(
                        new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, (long) threshold)));
        final IndexPredicate.RowNumberWindowPredicate windowPredicate =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("relevance"), direction, windowSize);
        final IndexPredicate andPredicate = new IndexPredicate.AndPredicate(
                List.of(valuePredicate, windowPredicate));

        final Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name());
        options.put(IndexOptions.HNSW_NUM_DIMENSIONS, Integer.toString(VECTOR_DIMS));

        metaDataBuilder.addIndex("SlidingWindowVectorRecord",
                new Index(INDEX_NAME,
                        new KeyWithValueExpression(Key.Expressions.field("vector_data"), 0),
                        IndexTypes.VECTOR, options, andPredicate));

        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    private void rec(long recNo, long relevance, long score) {
        float v = (float)(recNo % 100) / 100.0f;
        final HalfRealVector vector = makeVector(v, 1.0f - v, v * 0.5f, 0.1f);
        recordStore.saveRecord(SlidingWindowVectorRecord.newBuilder()
                .setRecNo(recNo)
                .setZone("z")
                .setCategory("c")
                .setRelevance(relevance)
                .setScore(score)
                .setVectorData(ByteString.copyFrom(vector.getRawData()))
                .build());
    }

    private void deleteRec(long recNo) {
        recordStore.deleteRecord(Tuple.from(recNo));
    }

    @Nonnull
    private Set<Long> scanIndexRecNos() {
        final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        final HalfRealVector queryVector = makeVector(0.5f, 0.5f, 0.5f, 0.5f);
        final VectorIndexScanBounds bounds = new VectorIndexScanBounds(
                TupleRange.ALL,
                Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                queryVector,
                100,
                VectorIndexScanOptions.empty());
        return maintainer.scan(bounds, null, com.apple.foundationdb.record.ScanProperties.FORWARD_SCAN)
                .asList()
                .join()
                .stream()
                .map(e -> e.getPrimaryKey().getLong(0))
                .collect(Collectors.toSet());
    }

    private void assertWindowContains(long... expectedRecNos) {
        final Set<Long> actual = scanIndexRecNos();
        final Set<Long> expected = java.util.Arrays.stream(expectedRecNos)
                .boxed()
                .collect(Collectors.toSet());
        assertEquals(expected, actual,
                "Window should contain recNos " + expected + " but was " + actual);
    }

    // ===== Records failing the value predicate should be invisible to the window =====

    @Test
    void filteredRecordsDoNotOccupyWindowSlots() throws Exception {
        // Window size 2, DESC (keep highest relevance), filter: score > 5
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, 5);

            rec(1, 100, 10);  // passes (score=10 > 5)
            rec(2, 200, 20);  // passes
            assertWindowContains(1, 2);

            rec(3, 999, 3);   // fails filter (score=3 <= 5) → invisible
            assertWindowContains(1, 2);
            commit(context);
        }
    }

    @Test
    void filteredRecordsDoNotEvictWindowEntries() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, 5);

            rec(1, 100, 10);
            rec(2, 200, 20);
            assertWindowContains(1, 2);

            rec(3, 999, 2);   // fails filter, should NOT evict even though 999 > 100
            assertWindowContains(1, 2);

            rec(4, 300, 30);  // passes, evicts rec1 (lowest relevance in DESC)
            assertWindowContains(2, 4);
            commit(context);
        }
    }

    @Test
    void filteredRecordsDoNotEnterOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, 5);

            rec(1, 200, 10);
            rec(2, 300, 20);
            rec(3, 50, 1);    // fails filter → not in overflow

            assertWindowContains(1, 2);

            deleteRec(1);     // if rec3 were in overflow it would be promoted
            assertWindowContains(2);
            commit(context);
        }
    }

    @Test
    void onlyFilteredInRecordsFillWindow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 3, Direction.DESC, 5);

            rec(1, 100, 3);   // fails
            rec(2, 200, 10);  // passes → window
            rec(3, 300, 1);   // fails
            rec(4, 400, 20);  // passes → window
            rec(5, 500, 4);   // fails
            rec(6, 600, 30);  // passes → window (fills to 3)

            assertWindowContains(2, 4, 6);
            commit(context);
        }
    }

    @Test
    void evictionOnlyAmongFilteredRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, 5);

            rec(1, 100, 10);  // window
            rec(2, 200, 20);  // window (full)
            rec(3, 999, 2);   // fails → ignored
            assertWindowContains(1, 2);

            rec(4, 300, 15);  // passes, evicts rec1
            assertWindowContains(2, 4);

            rec(5, 150, 25);  // passes but worse than boundary → overflow
            assertWindowContains(2, 4);

            deleteRec(4);     // re-elect rec5
            assertWindowContains(2, 5);
            commit(context);
        }
    }

    @Test
    void reElectionOnlyConsidersFilteredRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, 5);

            rec(1, 300, 10);  // window
            rec(2, 200, 20);  // window (full)
            rec(3, 100, 15);  // passes → overflow
            rec(4, 150, 3);   // fails → not in overflow

            assertWindowContains(1, 2);

            deleteRec(1);     // should promote rec3 (not rec4)
            assertWindowContains(2, 3);
            commit(context);
        }
    }

    @Test
    void rebuildWithMixedFilteredRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, 5);
            rec(1, 500, 3);   // fails
            rec(2, 200, 10);  // passes
            rec(3, 400, 1);   // fails
            rec(4, 300, 20);  // passes
            rec(5, 600, 4);   // fails
            rec(6, 100, 30);  // passes (worst relevance for DESC)
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openStore(context, 2, Direction.DESC, 5);
            recordStore.rebuildAllIndexes().join();
            // DESC window=2 among passing: {rec2(200), rec4(300), rec6(100)} → keeps {rec2, rec4}
            assertWindowContains(2, 4);
            commit(context);
        }
    }
}
