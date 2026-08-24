/*
 * RecordLayerEngine.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.OfflineStoredQueriesProcessor;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;

import com.codahale.metrics.MetricRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public final class RecordLayerEngine {

    private static final Logger logger = LogManager.getLogger(RecordLayerEngine.class);

    public static EmbeddedRelationalEngine makeEngine(@Nonnull RecordLayerConfig cfg,
                                                    @Nonnull List<FDBDatabase> databases,
                                                    @Nonnull KeySpace baseKeySpace,
                                                    @Nonnull StoreCatalog schemaCatalog,
                                                    @Nullable MetricRegistry metricsEngine,
                                                    @Nonnull RecordLayerMetadataOperationsFactory ddlFactory,
                                                    @Nullable RelationalPlanCache planCache) {

        MetricRegistry mEngine = convertToRecordLayerEngine(metricsEngine);

        List<FdbConnection> connections = databases.stream()
                .map(db -> new DirectFdbConnection(db, mEngine))
                .collect(Collectors.toList());

        List<StorageCluster> clusters = connections.stream()
                .map(conn -> new RecordLayerStorageCluster(conn, baseKeySpace, cfg, schemaCatalog, planCache, ddlFactory))
                .collect(Collectors.toList());

        if (planCache != null && !connections.isEmpty()) {
            List<RecordLayerSchemaTemplate> templates = List.of();
            try (Transaction txn = connections.get(0).getTransactionManager().createTransaction(Options.NONE)) {
                templates = OfflineStoredQueriesProcessor.getSchemaTemplates(schemaCatalog, txn);
                txn.commit();
            } catch (RelationalException e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to open catalog transaction for offline stored-query pre-warm", e);
                }
            }
            OfflineStoredQueriesProcessor.planStoredQueriesForSchemaTemplates(planCache, mEngine, templates);
        }

        return new EmbeddedRelationalEngine(clusters, mEngine);
    }

    private static MetricRegistry convertToRecordLayerEngine(@Nullable MetricRegistry metricsEngine) {
        //metrics are disabled for this engine, so no-op it
        return Objects.requireNonNullElse(metricsEngine, NoOpMetricRegistry.INSTANCE);

    }

    private RecordLayerEngine() {
    }
}
