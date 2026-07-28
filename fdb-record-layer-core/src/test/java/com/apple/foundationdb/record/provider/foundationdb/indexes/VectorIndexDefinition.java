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
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;

class VectorIndexDefinition implements IndexDefinition {
    private final String indexName = "vectorIndex";
    // A fixed query vector (all zeros) shared by every scan so that the before/after scans are comparable.
    private final HalfRealVector queryVector = new HalfRealVector(constantHalfComponents(0.0f));

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
        final Half[] components = constantHalfComponents(0.0f);
        components[0] = Half.valueOf((float)(index + 1));
        final HalfRealVector vector = new HalfRealVector(components);
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                .setBytesValue(ByteString.copyFrom(vector.getRawData()))
                .build();
    }

    @Override
    public Index buildIndex(final IndexTarget target) {
        final KeyExpression vectorField = target.indexedField(ScenarioRecords.BYTES_VALUE);
        final KeyExpression groupingPrefix = target.groupingPrefix();
        final KeyExpression root = groupingPrefix.getColumnSize() == 0
                ? new KeyWithValueExpression(vectorField, 0)
                : new KeyWithValueExpression(concat(groupingPrefix, vectorField), groupingPrefix.getColumnSize());
        return new Index(indexName, root, IndexTypes.VECTOR,
                ImmutableMap.of(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                        IndexOptions.HNSW_NUM_DIMENSIONS, String.valueOf(ScenarioRecords.VECTOR_DIMENSIONS)));
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        final Index index = store.getRecordMetaData().getIndex(indexName);
        // A large efSearch and k make the (approximate) HNSW search effectively exhaustive
        // for the small number of records used by the scenarios.
        final VectorIndexScanOptions options = VectorIndexScanOptions.builder()
                .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 1000)
                .putOption(VectorIndexScanOptions.HNSW_RETURN_VECTORS, false)
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
