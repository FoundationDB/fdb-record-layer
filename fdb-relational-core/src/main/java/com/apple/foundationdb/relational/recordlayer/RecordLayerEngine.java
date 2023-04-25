/*
 * RecordLayerEngine.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class RecordLayerEngine {

    public static EmbeddedRelationalEngine makeEngine(@Nonnull RecordLayerConfig cfg,
                                                    @Nonnull List<FDBDatabase> databases,
                                                    @Nonnull KeySpace baseKeySpace,
                                                    @Nonnull StoreCatalog schemaCatalog,
                                                    @Nullable MetricRegistry metricsEngine,
                                                    @Nonnull RecordLayerMetadataOperationsFactory ddlFactory,
                                                    @Nullable RelationalPlanCache planCache
                                                    ) {

        MetricRegistry mEngine = convertToRecordLayerEngine(metricsEngine);

        List<StorageCluster> clusters = databases.stream().map(db ->
                new RecordLayerStorageCluster(new DirectFdbConnection(db, mEngine), baseKeySpace, cfg, schemaCatalog, planCache, ddlFactory)).collect(Collectors.toList());

        return new EmbeddedRelationalEngine(clusters, mEngine);
    }

    private static MetricRegistry convertToRecordLayerEngine(@Nullable MetricRegistry metricsEngine) {
        //metrics are disabled for this engine, so no-op it
        return Objects.requireNonNullElse(metricsEngine, NoOpMetricRegistry.INSTANCE);

    }

    private RecordLayerEngine() {
    }
}
