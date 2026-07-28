/*
 * indexScenarioModel.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioMetaData.GroupingMode;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioMetaData.SyntheticKind;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndexScenarioModel {
    private enum RecordSource {
        SCENARIO, JOINED_CONSTITUENTS, UNNESTED_PARENTS
    }

    private final IndexDefinition definition;
    private final RecordMetaData metaData;
    private final RecordSource recordSource;
    private final Supplier<FDBRecordContext> openContext;
    private final FDBRecordStore.Builder storeBuilder;

    private IndexScenarioModel(final IndexDefinition definition, final RecordMetaData metaData,
                               final RecordSource recordSource, final Supplier<FDBRecordContext> openContext,
                               final FDBRecordStore.Builder storeBuilder) {
        this.definition = definition;
        this.metaData = metaData;
        this.recordSource = recordSource;
        this.openContext = openContext;
        this.storeBuilder = storeBuilder;
        storeBuilder.setMetaDataProvider(metaData);
    }

    /** Ungrouped normal scenario (the default). */
    public IndexScenarioModel(final IndexDefinition definition, final Supplier<FDBRecordContext> openContext,
                              final FDBRecordStore.Builder storeBuilder) {
        this(definition, IndexScenarioMetaData.forScenario(definition, GroupingMode.UNGROUPED),
                RecordSource.SCENARIO, openContext, storeBuilder);
    }

    /** Grouped normal scenario: the index is grouped by {@code group} and primary keys are aligned. */
    public static IndexScenarioModel grouped(final IndexDefinition definition,
                                             final Supplier<FDBRecordContext> openContext,
                                             final FDBRecordStore.Builder storeBuilder) {
        return new IndexScenarioModel(definition,
                IndexScenarioMetaData.forScenario(definition, GroupingMode.GROUPED),
                RecordSource.SCENARIO, openContext, storeBuilder);
    }

    /** Synthetic-type scenario: the index is over a joined or unnested record type. */
    public static IndexScenarioModel synthetic(final IndexDefinition definition, final SyntheticKind kind,
                                               final Supplier<FDBRecordContext> openContext,
                                               final FDBRecordStore.Builder storeBuilder) {
        final RecordSource source = kind == SyntheticKind.JOINED
                ? RecordSource.JOINED_CONSTITUENTS : RecordSource.UNNESTED_PARENTS;
        return new IndexScenarioModel(definition, IndexScenarioMetaData.forSynthetic(definition, kind),
                source, openContext, storeBuilder);
    }

    public RecordMetaData getMetaData() {
        return metaData;
    }

    /** Generate the records to save for this scenario's configuration. */
    public List<Message> generateRecords(final int count) {
        switch (recordSource) {
            case JOINED_CONSTITUENTS:
                return ScenarioRecords.joinedConstituents(count, definition);
            case UNNESTED_PARENTS:
                return ScenarioRecords.unnestedParents(count, definition);
            case SCENARIO:
            default:
                return ScenarioRecords.scenarioRecords(count, definition);
        }
    }

    /** Generate records that do not contribute an entry to the index under test. */
    public List<Message> generateOtherRecords(final int count) {
        return ScenarioRecords.otherRecords(count);
    }

    public void setupIndex() {
        runAgainstStore(definition::setupIndex);
    }

    public void assertScanResultsEqual(final List<IndexEntry> expected, final List<IndexEntry> actual) {
        assertTrue(definition.scanResultsEqual(expected, actual),
                () -> "index scan results differ:\n  expected=" + expected + "\n  actual=" + actual);
    }

    public void assertScanResultsNotEqual(final List<IndexEntry> unexpected, final List<IndexEntry> actual) {
        assertFalse(definition.scanResultsEqual(unexpected, actual),
                () -> "index scan results unexpectedly equal:\n  both=" + actual);
    }

    public void saveRecords(List<Message> records) {
        runAgainstStore(store1 -> records.forEach(store1::saveRecord));
    }

    public List<IndexEntry> scanIndex() {
        return applyToStore(store -> definition.scanIndex(store, ScanProperties.FORWARD_SCAN).asList().join());
    }

    public IndexState getIndexState() {
        return applyToStore(store -> store.getIndexState(definition.getIndexName()));
    }

    public void markIndexDisabled() {
        runAgainstStore(store -> store.markIndexDisabled(definition.getIndexName()).join());
    }

    public void markIndexWriteOnly() {
        runAgainstStore(store -> store.markIndexWriteOnly(definition.getIndexName()).join());
    }

    public void buildIndex() {
        try (OnlineIndexer indexer = getIndexerBuilder()
                .build()) {
            indexer.buildIndex();
        }
    }

    public OnlineIndexer.Builder getIndexerBuilder() {
        return OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .addTargetIndex(definition.getIndexName());
    }

    public <T> T applyToStore(Function<FDBRecordStore, T> consumer) {
        T result;
        try (FDBRecordContext context = openContext.get()) {
            final FDBRecordStore store = storeBuilder.setContext(context).createOrOpen();
            result = consumer.apply(store);
            context.commit();
        }
        return result;
    }

    public void runAgainstStore(Consumer<FDBRecordStore> consumer) {
        try (FDBRecordContext context = openContext.get()) {
            final FDBRecordStore store = storeBuilder.setContext(context).createOrOpen();
            consumer.accept(store);
            context.commit();
        }
    }
}
