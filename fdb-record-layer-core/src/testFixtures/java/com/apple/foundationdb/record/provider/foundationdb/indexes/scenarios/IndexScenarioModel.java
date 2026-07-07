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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class IndexScenarioModel {
    private final IndexDefinition definition;
    private final Supplier<FDBRecordContext> openContext;
    private final FDBRecordStore.Builder storeBuilder;

    public IndexScenarioModel(final IndexDefinition definition, final Supplier<FDBRecordContext> openContext, final FDBRecordStore.Builder storeBuilder) {
        this.definition = definition;
        this.openContext = openContext;
        this.storeBuilder = storeBuilder;
        storeBuilder.setMetaDataProvider(definition.getMetaData());
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
