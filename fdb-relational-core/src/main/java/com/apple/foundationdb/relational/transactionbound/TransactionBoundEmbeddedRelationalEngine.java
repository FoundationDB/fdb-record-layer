/*
 * TransactionBoundEmbeddedRelationalEngine.java
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

package com.apple.foundationdb.relational.transactionbound;

import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.query.cache.ChainedPlanCache;
import com.apple.foundationdb.relational.recordlayer.query.cache.PlanCache;

import java.util.Collection;
import java.util.List;

/**
 * A specialized derived class of {@link EmbeddedRelationalEngine} that creates a {@link com.apple.foundationdb.relational.recordlayer.catalog.TransactionBoundDatabase}
 * for every connection.
 */
public class TransactionBoundEmbeddedRelationalEngine extends EmbeddedRelationalEngine {
    private final PlanCache planCache;
    private final List<StorageCluster> clusters;

    public TransactionBoundEmbeddedRelationalEngine() {
        this(Options.NONE);
    }

    public TransactionBoundEmbeddedRelationalEngine(Options engineOptions) {
        super(List.of(new TransactionBoundStorageCluster(null)), NoOpMetricRegistry.INSTANCE);
        Integer cacheEntries = engineOptions.getOption(Options.Name.PLAN_CACHE_MAX_ENTRIES);
        if (cacheEntries == null || cacheEntries < 0) {
            this.planCache = null;
        } else {
            this.planCache = new ChainedPlanCache(cacheEntries);
        }
        this.clusters = List.of(new TransactionBoundStorageCluster(planCache));
    }

    @Override
    public Collection<StorageCluster> getStorageClusters() {
        return clusters;
    }

    public PlanCache getPlanCache() {
        return planCache;
    }
}
