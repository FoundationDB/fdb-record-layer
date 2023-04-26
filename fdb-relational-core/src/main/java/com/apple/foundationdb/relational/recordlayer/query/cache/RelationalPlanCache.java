/*
 * RelationalPlanCache.java
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.common.base.Ticker;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * This is just a specialization of {@link MultiStageCache} with concrete types specific to plan caching.
 */
@ExcludeFromJacocoGeneratedReport
public final class RelationalPlanCache extends MultiStageCache<CachedQuery, PhysicalPlanEquivalence, Plan<?>> {

    /**
     * The default time-to-live for a primary cache entry.
     */
    private static final int DEFAULT_TTL = 5000;

    @Nonnull
    private static final TimeUnit DEFAULT_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * The default time-to-live for a secondary cache entry.
     */
    private static final int DEFAULT_SECONDARY_TTL = 3000;

    @Nonnull
    private static final TimeUnit DEFAULT_SECONDARY_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * The default size of the primary cache.
     */
    private static final int DEFAULT_SIZE = 128;

    /**
     * The default size of the secondary cache.
     */
    private static final int DEFAULT_SECONDARY_SIZE = 8;

    private RelationalPlanCache(int size,
                              int secondarySize,
                              int ttl,
                              @Nonnull final TimeUnit ttlTimeUnit,
                              int secondaryTtl,
                              @Nonnull final TimeUnit secondaryTtlTimeUnit,
                              @Nullable final Executor executor,
                              @Nullable final Executor secondaryExecutor,
                              @Nullable final Ticker ticker) {
        super(size, secondarySize, ttl, ttlTimeUnit, secondaryTtl, secondaryTtlTimeUnit, executor, secondaryExecutor, ticker);
    }

    @ExcludeFromJacocoGeneratedReport // this is just a mechanical builder.
    public static final class RelationalCacheBuilder extends MultiStageCache.Builder<CachedQuery, PhysicalPlanEquivalence, Plan<?>, RelationalCacheBuilder> {

        public RelationalCacheBuilder() {
            size = DEFAULT_SIZE;
            secondarySize = DEFAULT_SECONDARY_SIZE;
            ttl = DEFAULT_TTL;
            ttlTimeUnit = DEFAULT_TTL_TIME_UNIT;
            secondaryTtl = DEFAULT_SECONDARY_TTL;
            secondaryTtlTimeUnit = DEFAULT_SECONDARY_TTL_TIME_UNIT;
            executor = null;
            secondaryExecutor = null;
            ticker = null;
        }

        @Nonnull
        @Override
        public RelationalPlanCache build() {
            return new RelationalPlanCache(size, secondarySize, ttl, ttlTimeUnit, secondaryTtl, secondaryTtlTimeUnit, executor, secondaryExecutor, ticker);
        }

        @Nonnull
        @Override
        protected RelationalCacheBuilder self() {
            return this;
        }
    }

    @Nonnull
    public static RelationalCacheBuilder newRelationalCacheBuilder() {
        return new RelationalCacheBuilder();
    }

    @Nonnull
    public static RelationalPlanCache buildWithDefaults() {
        return newRelationalCacheBuilder().build();
    }

}
