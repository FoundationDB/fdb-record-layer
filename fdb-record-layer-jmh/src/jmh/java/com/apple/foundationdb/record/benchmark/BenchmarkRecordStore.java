/*
 * BenchmarkRecordStore.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark;

import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore.DEFAULT_PIPELINE_SIZE;

/**
 * Per benchmark state for accessing record store.
 * State includes store location, meta-data, and some performance parameters.
 * This is meant to simulate use in a mostly stateless server, so the basic operation is to open a transaction and connect
 * to a specific store.
 */
public class BenchmarkRecordStore {
    @Nonnull
    private FDBDatabase database;
    @Nonnull
    private FDBRecordStore.Builder recordStoreBuilder;
    @Nullable
    private KeySpacePath dataKeySpacePath;
    @Nullable
    private KeySpacePath metaDataKeySpacePath;
    @Nonnull
    private final PipelineSizer pipelineSizer;

    private static final KeySpace KEY_SPACE = new KeySpace(
            new DirectoryLayerDirectory("record-layer-benchmark", "record-layer-benchmark")
                    .addSubdirectory(new DirectoryLayerDirectory("data", "data")
                            .addSubdirectory(new KeySpaceDirectory("name", KeySpaceDirectory.KeyType.STRING)))
                    .addSubdirectory(new DirectoryLayerDirectory("metadata", "metadata")
                            .addSubdirectory(new KeySpaceDirectory("name", KeySpaceDirectory.KeyType.STRING)))
    );

    static class PipelineSizer implements FDBRecordStoreBase.PipelineSizer {
        @Nonnull
        final Map<PipelineOperation, Integer> pipelineSizes = new HashMap<>();
        int defaultPipelineSize = DEFAULT_PIPELINE_SIZE;

        @Override
        public int getPipelineSize(@Nonnull final PipelineOperation pipelineOperation) {
            return pipelineSizes.getOrDefault(pipelineOperation, defaultPipelineSize);
        }
    }

    public BenchmarkRecordStore() {
        database = FDBDatabaseFactory.instance().getDatabase();
        recordStoreBuilder = FDBRecordStore.newBuilder();
        pipelineSizer = new PipelineSizer();
        recordStoreBuilder.setPipelineSizer(pipelineSizer);
    }

    public FDBDatabase getDatabase() {
        return database;
    }

    public BenchmarkRecordStore setDatabase(final FDBDatabase database) {
        this.database = database;
        return this;
    }

    public FDBRecordStore.Builder getRecordStoreBuilder() {
        return recordStoreBuilder;
    }

    public KeySpacePath getDataKeySpacePath(@Nonnull String name) {
        return KEY_SPACE.path("record-layer-benchmark").add("data").add("name", name);
    }

    public KeySpacePath getMetaDataKeySpacePath(@Nonnull String name) {
        return KEY_SPACE.path("record-layer-benchmark").add("metadata").add("name", name);
    }

    public void setKeySpacePath(@Nonnull KeySpacePath keySpacePath) {
        this.dataKeySpacePath = keySpacePath;
        recordStoreBuilder.setKeySpacePath(keySpacePath);
    }

    public void setName(@Nonnull String name) {
        setKeySpacePath(getDataKeySpacePath(name));
    }

    public void setMetaData(@Nonnull RecordMetaDataProvider metaData) {
        recordStoreBuilder.setMetaDataProvider(metaData);
    }

    public void setMetaDataKeySpacePath(@Nonnull KeySpacePath metaDataKeySpacePath) {
        this.metaDataKeySpacePath = metaDataKeySpacePath;
    }

    public void setMetaDataName(@Nonnull String name) {
        setMetaDataKeySpacePath(getMetaDataKeySpacePath(name));
    }

    public void setDefaultPipelineSize(int defaultPipelineSize) {
        pipelineSizer.defaultPipelineSize = defaultPipelineSize;
    }

    public void setPipelineSize(PipelineOperation operation, int size) {
        pipelineSizer.pipelineSizes.put(operation, size);
    }

    public void deleteAllData(@Nonnull BenchmarkTimer timer) {
        database.run(timer.getTimer(), timer.getMdc(), recordContext -> {
            dataKeySpacePath.deleteAllData(recordContext);
            return null;
        });
    }

    public FDBRecordStore openRecordStore(@Nonnull FDBRecordContext recordContext) {
        FDBRecordStore.Builder builder = recordStoreBuilder.copyBuilder();
        builder.setContext(recordContext);
        if (metaDataKeySpacePath != null) {
            builder.setMetaDataStore(new FDBMetaDataStore(recordContext, metaDataKeySpacePath));
        }
        return builder.createOrOpen();
    }

    public void run(@Nonnull BenchmarkTimer timer, @Nonnull Consumer<FDBRecordStore> body) {
        database.run(timer.getTimer(), timer.getMdc(), recordContext -> {
            final FDBRecordStore recordStore = openRecordStore(recordContext);
            body.accept(recordStore);
            return null;
        });
    }

    public QueryPlanner planner(@Nonnull BenchmarkTimer timer, boolean cascades) {
        final RecordMetaData recordMetaData = recordStoreBuilder.getMetaDataProvider().getRecordMetaData();
        final RecordStoreState recordStoreState = new RecordStoreState(null, null);
        if (cascades) {
            return new CascadesPlanner(recordMetaData, recordStoreState, recordStoreBuilder.getIndexMaintainerRegistry());
        } else {
            return new RecordQueryPlanner(recordMetaData, recordStoreState, timer.getTimer());
        }
    }

}
