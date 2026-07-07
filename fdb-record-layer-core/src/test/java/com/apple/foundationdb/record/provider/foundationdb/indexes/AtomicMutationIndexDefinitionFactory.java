/*
 * AtomicMutationIndexDefinitionFactory.java
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
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinitionFactory;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

class AtomicMutationIndexDefinitionFactory implements IndexDefinitionFactory {
    private final String indexType;

    public AtomicMutationIndexDefinitionFactory(final String indexType) {
        this.indexType = indexType;
    }

    @Override
    public IndexDefinition getDefinition(final int groupingLength, @Nullable final Class<? extends SyntheticRecordType<?>> syntheticType) {
        return new IndexDefinition() {
            String indexName = "myIndex";

            @Override
            public RecordMetaData getMetaData() {
                RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                        .setRecords(TestRecords1Proto.getDescriptor());
                indexName = "myIndex";
                final GroupingKeyExpression keyExpression;
                if (Objects.equals(indexType, IndexTypes.COUNT) || Objects.equals(indexType, IndexTypes.COUNT_UPDATES)) {
                    keyExpression = Key.Expressions.empty().groupBy(Key.Expressions.empty());
                } else {
                    keyExpression = Key.Expressions.field("num_value_2").groupBy(Key.Expressions.empty());
                }
                metaDataBuilder.addIndex("MySimpleRecord",
                        new Index(indexName, keyExpression, indexType));
                return metaDataBuilder.build();
            }

            @Override
            public List<Message> generateRecords(final int count) {
                return IntStream.range(0, count)
                        .mapToObj(i -> (Message)TestRecords1Proto.MySimpleRecord.newBuilder()
                                .setRecNo(i)
                                // Don't insert 0
                                .setNumValue2(3 * i + 1)
                                .build())
                        .toList();
            }

            @Override
            public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, ScanProperties scanProperties) {
                return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                        new IndexScanRange(IndexScanType.BY_GROUP, TupleRange.ALL), null, scanProperties);
            }

            @Override
            public String getIndexName() {
                return indexName;
            }
        };
    }
}
