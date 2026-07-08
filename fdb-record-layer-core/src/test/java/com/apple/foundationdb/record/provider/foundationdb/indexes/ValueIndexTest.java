/*
 * ValueIndexTest.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenario;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Tests for the {@link ValueIndexMaintainer} using the shared index scenario framework.
 */
@Tag(Tags.RequiresFDB)
public class ValueIndexTest {

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    private KeySpacePath path;

    @BeforeEach
    void setUp() {
        path = pathManager.createPath();
    }

    @ParameterizedTest
    @IndexScenarios
    void indexScenariosTest(IndexScenario scenario) throws Exception {
        scenario.runTest(
                (groupingLength, syntheticType) -> new IndexDefinition() {
                    private final String indexName = "valueIndex";

                    @Override
                    public RecordMetaData getMetaData() {
                        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                                .setRecords(TestRecords1Proto.getDescriptor());
                        metaDataBuilder.addIndex("MySimpleRecord",
                                new Index(indexName, Key.Expressions.field("num_value_2"), IndexTypes.VALUE));
                        return metaDataBuilder.build();
                    }

                    @Override
                    public List<Message> generateRecords(final int count) {
                        return IntStream.range(0, count)
                                .mapToObj(i -> (Message)TestRecords1Proto.MySimpleRecord.newBuilder()
                                        .setRecNo(i)
                                        .setNumValue2(3 * i + 1)
                                        .build())
                                .toList();
                    }

                    @Override
                    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
                        return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                                new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL), null, scanProperties);
                    }

                    @Override
                    public List<Message> generateOtherRecords(final int count) {
                        // MyOtherRecord is not covered by the value index on MySimpleRecord.
                        return IntStream.range(0, count)
                                .mapToObj(i -> (Message)TestRecords1Proto.MyOtherRecord.newBuilder()
                                        .setRecNo(1000 + i)
                                        .build())
                                .toList();
                    }

                    @Override
                    public String getIndexName() {
                        return indexName;
                    }
                },
                () -> {
                    FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                            .setTimer(new FDBStoreTimer())
                            .build();
                    return dbExtension.getDatabase().openContext(config);
                },
                FDBRecordStore.newBuilder()
                        .setKeySpacePath(path));
    }
}
