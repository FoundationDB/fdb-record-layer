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
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

@AutoService(IndexScenario.class)
public class InsertWhileWriteOnlyNotIndexed implements IndexScenario {
    @Override
    public void runTest(final IndexDefinitionFactory definitionFactory,
                        final Supplier<FDBRecordContext> openContext,
                        final FDBRecordStore.Builder storeBuilder) {
        final IndexDefinition definition = definitionFactory.getDefinition();
        final IndexScenarioModel model = new IndexScenarioModel(definition, openContext, storeBuilder);
        final List<Message> records = model.generateRecords(10);
        model.setupIndex();
        model.markIndexDisabled();
        model.markIndexWriteOnly();

        assertEquals(IndexState.WRITE_ONLY, model.getIndexState());

        model.saveRecords(records);


        model.buildIndex();

        final List<IndexEntry> actual = model.scanIndex();
        model.markIndexDisabled();
        model.buildIndex();
        model.assertScanResultsEqual(model.scanIndex(), actual);
    }
}
