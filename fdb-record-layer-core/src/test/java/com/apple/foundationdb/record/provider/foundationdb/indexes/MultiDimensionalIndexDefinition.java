/*
 * MultiDimensionalIndexDefinition.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;

import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_NODE;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;

class MultiDimensionalIndexDefinition implements IndexDefinition {
    @Override
    public TestRecordsIndexScenariosProto.IndexedMessage generateIndexedMessage(final int ordinal) {
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                .setLongValue(100L * ordinal)
                .setLongValue2(100L * ordinal + 50L)
                .build();
    }

    @Override
    public Index buildIndex(final IndexTarget target) {
        final KeyExpression dimensions = concat(
                target.indexedField(ScenarioRecords.LONG_VALUE),
                target.indexedField(ScenarioRecords.LONG_VALUE_2));
        return new Index(ScenarioRecords.INDEX_NAME,
                DimensionsKeyExpression.of(target.groupingPrefix(), dimensions),
                IndexTypes.MULTIDIMENSIONAL,
                Map.of(IndexOptions.RTREE_STORAGE, BY_NODE.toString(),
                        IndexOptions.RTREE_STORE_HILBERT_VALUES, "true"));
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final Index index,
                                              final ScanProperties scanProperties) {
        // Exhaustive scan: every dimension unbounded, so the R-tree returns all entries
        // in (deterministic) Hilbert-value order.
        final MultidimensionalIndexScanBounds.Hypercube hypercube =
                new MultidimensionalIndexScanBounds.Hypercube(List.of(
                        TupleRange.betweenInclusive(null, null),
                        TupleRange.betweenInclusive(null, null)));
        final MultidimensionalIndexScanBounds bounds =
                new MultidimensionalIndexScanBounds(TupleRange.ALL, hypercube, TupleRange.ALL);
        return store.scanIndex(index, bounds, null, scanProperties);
    }
}
