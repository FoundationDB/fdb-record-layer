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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsMultidimensionalProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import java.util.List;
import java.util.stream.IntStream;

import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_NODE;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

class MultiDimensionalIndexDefinition implements IndexDefinition {
    private final String indexName = "EventIntervals";

    @Override
    public RecordMetaData getMetaData() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsMultidimensionalProto.getDescriptor());
        metaDataBuilder.getRecordType("MyMultidimensionalRecord")
                .setPrimaryKey(concat(field("info").nest("rec_domain"), field("rec_no")));
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index(indexName,
                        DimensionsKeyExpression.of(field("calendar_name"),
                                concat(field("start_epoch"), field("end_epoch"))),
                        IndexTypes.MULTIDIMENSIONAL,
                        ImmutableMap.of(IndexOptions.RTREE_STORAGE, BY_NODE.toString(),
                                IndexOptions.RTREE_STORE_HILBERT_VALUES, "true")));
        return metaDataBuilder.build();
    }

    @Override
    public List<Message> generateRecords(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                        .setRecNo(i)
                        .setCalendarName("calendar")
                        .setStartEpoch(100L * i)
                        .setEndEpoch(100L * i + 50L)
                        .build())
                .toList();
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

    @Override
    public List<Message> generateOtherRecords(final int count) {
        // Records with unset dimension fields (start/end epoch) are not added to the R-tree.
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                        .setRecNo(1000 + i)
                        .setCalendarName("other")
                        .build())
                .toList();
    }

    @Override
    public String getIndexName() {
        return indexName;
    }
}
