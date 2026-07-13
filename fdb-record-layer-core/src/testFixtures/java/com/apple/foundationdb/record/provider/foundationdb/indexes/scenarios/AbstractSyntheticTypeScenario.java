/*
 * AbstractSyntheticTypeScenario.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioMetaData.SyntheticKind;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assumptions;

import java.util.List;
import java.util.function.Supplier;

/**
 * Builds the index under test over a synthetic record type (joined or unnested), saves the
 * constituent/parent records, and verifies that the index rebuilds identically (from-scratch
 * maintenance vs a bulk {@code OnlineIndexer} rebuild) over the synthetic type.
 */
abstract class AbstractSyntheticTypeScenario implements IndexScenario {
    protected abstract SyntheticKind kind();

    @Override
    public void runTest(final IndexDefinitionFactory definitionFactory,
                        final Supplier<FDBRecordContext> openContext,
                        final FDBRecordStore.Builder storeBuilder) {
        final IndexDefinition definition = definitionFactory.getDefinition();
        Assumptions.assumeTrue(definition.supportsSynthetic(),
                "index does not support synthetic record types");
        final IndexScenarioModel model = IndexScenarioModel.synthetic(definition, kind(), openContext, storeBuilder);
        final List<Message> records = model.generateRecords(10);

        model.setupIndex();
        model.saveRecords(records);

        final List<IndexEntry> fromScratch = model.scanIndex();
        model.markIndexDisabled();
        model.buildIndex();
        model.assertScanResultsEqual(fromScratch, model.scanIndex());
    }
}
