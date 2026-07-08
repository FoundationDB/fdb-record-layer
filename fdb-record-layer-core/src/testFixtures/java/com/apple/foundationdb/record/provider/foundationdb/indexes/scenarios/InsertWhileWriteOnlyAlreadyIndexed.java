/*
 * InsertWhileWriteOnly.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

@AutoService(IndexScenario.class)
public class InsertWhileWriteOnlyAlreadyIndexed implements IndexScenario {
    @Override
    public void runTest(final IndexDefinitionFactory definitionFactory,
                        final Supplier<FDBRecordContext> openContext,
                        final FDBRecordStore.Builder storeBuilder) {
        final IndexDefinition definition = definitionFactory.getDefinition(0, null);
        final IndexScenarioModel model = new IndexScenarioModel(definition, openContext, storeBuilder);
        final List<Message> records = definition.generateRecords(10);

        model.setupIndex();
        model.saveRecords(records);
        model.markIndexDisabled();

        try (OnlineIndexer indexer = model.getIndexerBuilder()
                .build()) {
            indexer.buildIndex(false);
        }

        assertEquals(IndexState.WRITE_ONLY, model.getIndexState());

        // already indexed
        model.saveRecords(records);
        // should just be marking it readable
        try (OnlineIndexer indexer = model.getIndexerBuilder()
                .build()) {
            indexer.buildIndex(true);
        }
        final List<IndexEntry> actual = model.scanIndex();
        model.markIndexDisabled();
        model.buildIndex();
        // TODO: this is not ideal. Perhaps an early symptom of the eventual challenges ahead
        if (!definition.getMetaData().getIndex(definition.getIndexName()).getType().equals(IndexTypes.COUNT_UPDATES)) {
            model.assertScanResultsEqual(model.scanIndex(), actual);
        }
    }
}
