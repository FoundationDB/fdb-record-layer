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
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioMetaData;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_NODE;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

class MultiDimensionalIndexDefinition implements IndexDefinition {
    private final String indexName = "EventIntervals";

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getIndexedTypeName() {
        return ScenarioRecords.SCENARIO_RECORD;
    }

    @Override
    public TestRecordsIndexScenariosProto.IndexedMessage generateIndexedMessage(final int index) {
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                .setStringValue("group")
                .setLongValue(100L * index)
                .setLongValue2(100L * index + 50L)
                .build();
    }

    @Override
    public Index buildIndex(final KeyExpression groupingPrefix) {
        // The R-tree always needs a (non-empty) dimensions prefix; use string_value as the base and prepend
        // the group prefix when grouped, so deleteRecordsWhere can clear a whole group (whose column is at
        // the front of, and no longer than, the dimensions prefix).
        final KeyExpression prefix = IndexScenarioMetaData.prefixed(groupingPrefix,
                field(ScenarioRecords.INDEXED).nest(ScenarioRecords.STRING_VALUE));
        final KeyExpression dimensions = concat(
                field(ScenarioRecords.INDEXED).nest(ScenarioRecords.LONG_VALUE),
                field(ScenarioRecords.INDEXED).nest(ScenarioRecords.LONG_VALUE_2));
        return new Index(indexName,
                DimensionsKeyExpression.of(prefix, dimensions),
                IndexTypes.MULTIDIMENSIONAL,
                ImmutableMap.of(IndexOptions.RTREE_STORAGE, BY_NODE.toString(),
                        IndexOptions.RTREE_STORE_HILBERT_VALUES, "true"));
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        // Exhaustive scan: every dimension unbounded, so the R-tree returns all entries
        // in (deterministic) Hilbert-value order.
        final MultidimensionalIndexScanBounds.Hypercube hypercube =
                new MultidimensionalIndexScanBounds.Hypercube(ImmutableList.of(
                        TupleRange.betweenInclusive(null, null),
                        TupleRange.betweenInclusive(null, null)));
        final MultidimensionalIndexScanBounds bounds =
                new MultidimensionalIndexScanBounds(TupleRange.ALL, hypercube, TupleRange.ALL);
        return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                bounds, null, scanProperties);
    }
}
