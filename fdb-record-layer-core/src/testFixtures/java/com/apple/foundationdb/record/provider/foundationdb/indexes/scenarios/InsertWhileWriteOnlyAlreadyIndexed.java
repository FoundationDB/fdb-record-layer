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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Saves records, then disables the index and rebuilds it without marking it readable, leaving it write-only.
 * Saves the same records again while the index is write-only, and then finishes the build. Because the records
 * are already indexed, finishing the build only needs to mark the index readable. Verifies that the index then
 * matches one rebuilt from scratch.
 */
@AutoService(IndexScenario.class)
public class InsertWhileWriteOnlyAlreadyIndexed implements IndexScenario {
    @Override
    public void runTest(final IndexDefinition definition,
                        final Supplier<FDBRecordContext> openContext,
                        final FDBRecordStore.Builder storeBuilder) {
        final IndexScenarioModel model = IndexScenarioModel.ungrouped(definition, openContext, storeBuilder);
        final List<Message> records = model.generateRecords(10);

        model.saveRecords(records);
        model.markIndexDisabled();

        // Index every record, but leave the index write-only rather than marking it readable.
        try (OnlineIndexer indexer = model.getIndexerBuilder()
                .build()) {
            indexer.buildIndex(false);
        }

        assertEquals(IndexState.WRITE_ONLY, model.getIndexState());

        // Save the records again, while the index is write-only and already covers them.
        model.saveRecords(records);
        // Every record is already indexed, so finishing the build only has to mark the index readable.
        try (OnlineIndexer indexer = model.getIndexerBuilder()
                .build()) {
            indexer.buildIndex(true);
        }
        final List<IndexEntry> actual = model.scanIndex();
        model.markIndexDisabled();
        model.buildIndex();
        // The second save of each record only leaves the index matching a rebuild, which sees each record
        // once, if the index ignores unchanged saves.
        if (definition.ignoresUnchangedSaves()) {
            model.assertScanResultsEqual(model.scanIndex(), actual);
        }
    }
}
