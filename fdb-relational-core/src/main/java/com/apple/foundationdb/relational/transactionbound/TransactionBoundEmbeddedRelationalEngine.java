/*
 * TransactionBoundEmbeddedRelationalEngine.java
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

package com.apple.foundationdb.relational.transactionbound;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * A specialized derived class of {@link EmbeddedRelationalEngine} that creates a {@link com.apple.foundationdb.relational.recordlayer.catalog.TransactionBoundDatabase}
 * for every connection.
 */
@API(API.Status.EXPERIMENTAL)
public class TransactionBoundEmbeddedRelationalEngine extends EmbeddedRelationalEngine {
    private final RelationalPlanCache planCache;
    private final List<StorageCluster> clusters;

    public TransactionBoundEmbeddedRelationalEngine() {
        this(Options.NONE);
    }

    public TransactionBoundEmbeddedRelationalEngine(@Nonnull Options engineOptions) {
        this(engineOptions, null);
    }

    public TransactionBoundEmbeddedRelationalEngine(@Nonnull Options engineOptions, @Nullable KeySpace keySpace) {
        super(List.of(new TransactionBoundStorageCluster(null, keySpace)), NoOpMetricRegistry.INSTANCE);
        final Integer primaryCacheSize = engineOptions.getOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES);
        final Integer secondaryCacheSize = engineOptions.getOption(Options.Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES);
        final Integer tertiaryCacheSize = engineOptions.getOption(Options.Name.PLAN_CACHE_TERTIARY_MAX_ENTRIES);
        final Long primaryCacheTtlMillis = engineOptions.getOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS);
        final Long secondaryCacheTtlMillis = engineOptions.getOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS);
        final Long tertiaryCacheTtlMillis = engineOptions.getOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS);
        if (primaryCacheSize == null || primaryCacheSize <= 0) {
            this.planCache = null;
        } else {
            this.planCache = RelationalPlanCache.newRelationalCacheBuilder()
                    .setSize(primaryCacheSize)
                    .setSecondarySize(secondaryCacheSize)
                    .setTertiarySize(tertiaryCacheSize)
                    .setTtl(primaryCacheTtlMillis)
                    .setSecondaryTtl(secondaryCacheTtlMillis)
                    .setTertiaryTtl(tertiaryCacheTtlMillis)
                    .build();
        }

        this.clusters = List.of(new TransactionBoundStorageCluster(planCache, keySpace));
    }

    @Override
    public Collection<StorageCluster> getStorageClusters() {
        return clusters;
    }

    public RelationalPlanCache getPlanCache() {
        return planCache;
    }
}
