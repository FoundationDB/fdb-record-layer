/*
 * VectorIndexDefinition.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;

/**
 * A vector index over the scenario schema. The engine-specific index options (engine kind, metric,
 * dimensionality) are supplied by the test that owns the engine — see
 * {@link VectorIndexEngineTestSuite} — so the same definition exercises every vector engine.
 */
class VectorIndexDefinition implements IndexDefinition {
    private final String indexName = "vectorIndex";
    @Nonnull
    private final Map<String, String> indexOptions;
    // A fixed query vector (all zeros) shared by every scan so that the before/after scans are comparable.
    private final HalfRealVector queryVector = new HalfRealVector(constantHalfComponents(0.0f));

    VectorIndexDefinition(@Nonnull final Map<String, String> indexOptions) {
        this.indexOptions = indexOptions;
    }

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
        // Distinct distance-to-origin per record, so distance-sorted vector scans are deterministic.
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                .setBytesValue(ScenarioRecords.vectorBytes(index + 1))
                .build();
    }

    @Override
    public Index buildIndex(final IndexTarget target) {
        final KeyExpression vectorField = target.indexedField(ScenarioRecords.BYTES_VALUE);
        final KeyExpression groupingPrefix = target.groupingPrefix();
        final KeyExpression root = groupingPrefix.getColumnSize() == 0
                ? new KeyWithValueExpression(vectorField, 0)
                : new KeyWithValueExpression(concat(groupingPrefix, vectorField), groupingPrefix.getColumnSize());
        return new Index(indexName, root, IndexTypes.VECTOR, indexOptions);
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        final Index index = store.getRecordMetaData().getIndex(indexName);
        // A large k makes the (approximate) nearest-neighbor search effectively exhaustive for the small
        // number of records the scenarios use. The options are deliberately engine-neutral.
        final VectorIndexScanOptions options = VectorIndexScanOptions.builder()
                .putOption(VectorIndexScanOptions.VECTOR_RETURN_VECTORS, false)
                .build();
        final VectorIndexScanComparisons comparisons =
                VectorIndexTestBase.createVectorIndexScanComparisons(queryVector, 1000, options);
        return store.scanIndex(index, comparisons.bind(store, index, EvaluationContext.empty()),
                null, scanProperties);
    }

    @Nonnull
    private static Half[] constantHalfComponents(final float value) {
        final Half[] components = new Half[ScenarioRecords.VECTOR_DIMENSIONS];
        for (int i = 0; i < components.length; i++) {
            components[i] = Half.valueOf(value);
        }
        return components;
    }
}
