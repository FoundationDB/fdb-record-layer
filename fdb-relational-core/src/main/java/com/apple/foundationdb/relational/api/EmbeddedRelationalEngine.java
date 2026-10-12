/*
 * EmbeddedRelationalEngine.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.OfflineStoredQueriesProcessor;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * The Engine that drives and manages an Embedded Relational instance.
 */
@API(API.Status.EXPERIMENTAL)
public class EmbeddedRelationalEngine {
    private final List<StorageCluster> clusters;

    private final MetricRegistry metricRegistry;

    @Nullable
    private final RelationalPlanCache planCache;

    //TODO(bfines) eventually we need to move StoreCatalog into StorageCluster
    @SpotBugsSuppressWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "May be refactored as embedded takes over transaction lifetime")
    public EmbeddedRelationalEngine(List<StorageCluster> fdbClusters,
                                  @Nonnull MetricRegistry metricRegistry,
                                  @Nullable RelationalPlanCache planCache) {
        if (fdbClusters.isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one FDB cluster to connect to");
        }
        this.clusters = fdbClusters;
        this.metricRegistry = metricRegistry;
        this.planCache = planCache;
    }

    public Collection<StorageCluster> getStorageClusters() {
        return clusters;
    }

    public MetricSet getEngineMetrics() {
        return metricRegistry;
    }

    @Nullable
    public RelationalPlanCache getPlanCache() {
        return planCache;
    }

    /**
     * Plans the stored queries of a schema template and puts the plans into the plan cache. Takes the schema
     * template, the record metadata, a metric collector and the planner options. Does nothing if the engine has no
     * plan cache.
     *
     * <p>The record metadata and the options must be the ones queries get at runtime.</p>
     */
    public void planStoredQueries(@Nonnull RecordLayerSchemaTemplate schemaTemplate,
                                  @Nonnull RecordMetaData recordMetaData,
                                  @Nonnull MetricCollector metricCollector,
                                  @Nonnull Options options) {
        final RelationalPlanCache cache = getPlanCache();
        if (cache != null) {
            OfflineStoredQueriesProcessor.planStoredQueries(cache, schemaTemplate, recordMetaData, metricCollector, options);
        }
    }
}
