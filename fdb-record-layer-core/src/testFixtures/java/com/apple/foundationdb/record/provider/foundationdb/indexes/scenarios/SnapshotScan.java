/*
 * SnapshotScan.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assumptions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@AutoService(IndexScenario.class)
public class SnapshotScan implements IndexScenario {

    @Override
    public void runTest(final IndexDefinitionFactory definitionFactory, final Supplier<FDBRecordContext> openContext, final FDBRecordStore.Builder storeBuilder) throws Exception {
        final IndexDefinition definition = definitionFactory.getDefinition(0, null);
        Assumptions.assumeTrue(definition.supportsSnapshotIsolation(),
                "index does not support snapshot-isolated scans in the presence of concurrent same-transaction writes");
        final IndexScenarioModel model = new IndexScenarioModel(definition, openContext, storeBuilder);
        final List<Message> records = definition.generateRecords(10);

        model.setupIndex();
        model.saveRecords(records.subList(3, 6));

        final List<IndexEntry> original = model.scanIndex();

        final List<IndexEntry> snapshotScan = model.applyToStore(store -> {
            final RecordCursor<IndexEntry> snapshotResult = definition.scanIndex(store, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setIsolationLevel(IsolationLevel.SNAPSHOT).build()
            ));
            final List<IndexEntry> scanResults = snapshotResult.asList().join();
            // Change the index from a different transaction. Include records with values both below
            // and above the initial set so that min/max-style aggregate indexes change too.
            final List<Message> indexChangingRecords = new ArrayList<>(records.subList(0, 2));
            indexChangingRecords.addAll(records.subList(6, 10));
            model.saveRecords(indexChangingRecords);
            // Give this transaction writes too, but writes that do not touch the index (so that the
            // snapshot scan, not a write conflict, is what is being exercised).
            definition.generateOtherRecords(4).forEach(store::saveRecord);
            return scanResults;
        });

        model.assertScanResultsEqual(original, snapshotScan);

        model.assertScanResultsNotEqual(original, model.scanIndex());
    }
}
