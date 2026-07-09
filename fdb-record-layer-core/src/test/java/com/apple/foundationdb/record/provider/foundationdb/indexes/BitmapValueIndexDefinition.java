/*
 * BitmapValueIndexDefinition.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsBitmapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.google.protobuf.Message;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

class BitmapValueIndexDefinition implements IndexDefinition {
    private final String indexName = "bitmapIndex";

    @Override
    public RecordMetaData getMetaData() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsBitmapProto.getDescriptor());
        // Grouped by an empty expression: the only grouped column is the bit position.
        metaDataBuilder.addIndex("MySimpleRecord",
                new Index(indexName, field("num_value_unique").ungrouped(),
                        IndexTypes.BITMAP_VALUE, BitmapValueIndexTest.SMALL_BITMAP_OPTIONS));
        return metaDataBuilder.build();
    }

    @Override
    public List<Message> generateRecords(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsBitmapProto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setNumValueUnique(i)
                        .build())
                .collect(Collectors.toList());
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                new IndexScanRange(IndexScanType.BY_GROUP, TupleRange.ALL), null, scanProperties);
    }

    @Override
    public List<Message> generateOtherRecords(final int count) {
        // MyNestedRecord is not covered by the bitmap index on MySimpleRecord.
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsBitmapProto.MyNestedRecord.newBuilder()
                        .setRecNo(1000 + i)
                        .build())
                .collect(Collectors.toList());
    }

    @Override
    public String getIndexName() {
        return indexName;
    }
}
