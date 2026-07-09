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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.IntStream;

class VectorIndexDefinition implements IndexDefinition {
    private final String indexName;
    private final int numDimensions;
    private final HalfRealVector queryVector;

    public VectorIndexDefinition(final int numDimensions) {
        this.numDimensions = numDimensions;
        indexName = "UngroupedVectorIndex";
        // A fixed query vector (all zeros) shared by every scan so that the before/after scans are comparable.
        this.queryVector = new HalfRealVector(constantHalfComponents(numDimensions, 0.0f));
    }

    @Override
    public RecordMetaData getMetaData() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsVectorsProto.getDescriptor());
        VectorIndexTestBase.addUngroupedVectorIndex(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Override
    public List<Message> generateRecords(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    // Distinct distance-to-query per record (component 0 == i + 1), so the
                    // distance-sorted result order is fully determined by the data.
                    final Half[] components = constantHalfComponents(numDimensions, 0.0f);
                    components[0] = Half.valueOf((float)(i + 1));
                    final HalfRealVector vector = new HalfRealVector(components);
                    return (Message)TestRecordsVectorsProto.VectorRecord.newBuilder()
                            .setRecNo(i)
                            .setGroupId(0)
                            .setVectorData(ByteString.copyFrom(vector.getRawData()))
                            .build();
                })
                .toList();
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

    @Override
    public List<Message> generateOtherRecords(final int count) {
        // A vector record with no vector_data is skipped by the HNSW index, so it is not covered.
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsVectorsProto.VectorRecord.newBuilder()
                        .setRecNo(1000 + i)
                        .setGroupId(0)
                        .build())
                .toList();
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    private static Half[] constantHalfComponents(final int numDimensions, final float value) {
        final Half[] components = new Half[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            components[i] = Half.valueOf(value);
        }
        return components;
    }
}
