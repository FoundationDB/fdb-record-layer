/*
 * SlidingWindowIndexDefinition.java
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

import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A sliding-window index over the scenario schema.
 * <p>
 * A sliding window is not its own index type: the maintainer registry wraps an index in the
 * sliding-window maintainer when it is a {@link IndexTypes#VECTOR} index that carries an
 * {@link IndexPredicate.RowNumberWindowPredicate}. The window size, the ordering key and the
 * partitioning all come from that predicate, and the delegate has to be a vector index, so the
 * records carry a real vector as well as the ordering key.
 * <p>
 * The window keeps the best {@code windowSize} records per partition by the ordering key; the rest
 * are held in an overflow tail that is invisible to a scan. So a scan returns
 * {@code min(windowSize, recordsInPartition)} entries.
 */
class SlidingWindowIndexDefinition implements IndexDefinition {
    private final int windowSize;
    // A fixed query vector (all zeros) shared by every scan so that the before/after scans are comparable.
    private final HalfRealVector queryVector = new HalfRealVector(zeroComponents());

    SlidingWindowIndexDefinition(final int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public TestRecordsIndexScenariosProto.IndexedMessage generateIndexedMessage(final int ordinal) {
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                // The vector the delegate index is built over; distinct distance per record.
                .setBytesValue(ScenarioRecords.vectorBytes(ordinal + 1))
                // The window's ordering key. Strictly monotonic and distinct, so eviction order is
                // fully determined and the window never has to break a tie.
                .setLongValue(ordinal)
                .build();
    }

    @Override
    public Index buildIndex(final IndexTarget target) {
        final int prefixSize = target.groupingPrefix().getColumnSize();
        final KeyExpression root = new KeyWithValueExpression(
                target.prefixed(target.indexedField(ScenarioRecords.BYTES_VALUE)), prefixSize);
        // The predicate's partition paths have to mirror the key-with-value prefix.
        final List<List<String>> partitionPaths = prefixSize > 0
                ? ImmutableList.of(ImmutableList.of(ScenarioRecords.GROUP))
                : ImmutableList.of();
        final IndexPredicate predicate = new IndexPredicate.RowNumberWindowPredicate(
                ImmutableList.of(ScenarioRecords.INDEXED, ScenarioRecords.LONG_VALUE),
                IndexPredicate.RowNumberWindowPredicate.Direction.DESC,
                windowSize,
                partitionPaths);
        return new Index(ScenarioRecords.INDEX_NAME, root, IndexTypes.VECTOR,
                ImmutableMap.of(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                        IndexOptions.VECTOR_NUM_DIMENSIONS, String.valueOf(ScenarioRecords.VECTOR_DIMENSIONS)),
                predicate);
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final Index index,
                                              final ScanProperties scanProperties) {
        // A vector index cannot be scanned through the TupleRange API; the range here is the partition
        // prefix and a large limit makes the nearest-neighbour search effectively exhaustive.
        final VectorIndexScanBounds bounds = new VectorIndexScanBounds(
                TupleRange.ALL,
                Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                queryVector,
                1000,
                VectorIndexScanOptions.empty());
        return store.getIndexMaintainer(index).scan(bounds, null, scanProperties);
    }

    @Override
    public boolean supportsSynthetic() {
        // The sliding-window factory rejects synthetic types outright: "sliding window index is on
        // synthetic record types". Supporting them would be a maintainer change.
        return false;
    }

    @Nonnull
    private static Half[] zeroComponents() {
        final Half[] components = new Half[ScenarioRecords.VECTOR_DIMENSIONS];
        for (int i = 0; i < components.length; i++) {
            components[i] = Half.valueOf(0.0f);
        }
        return components;
    }
}
