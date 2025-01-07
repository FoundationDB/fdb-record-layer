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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.Options;
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
@API(API.Status.EXPERIMENTAL)
public class RelationalPlanCache extends MultiStageCache<String, QueryCacheKey, PhysicalPlanEquivalence, Plan<?>> {

    @Nonnull
    private static final TimeUnit DEFAULT_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

    @Nonnull
    private static final TimeUnit DEFAULT_SECONDARY_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

    @Nonnull
    private static final TimeUnit DEFAULT_TERTIARY_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

    private RelationalPlanCache(int size,
                              int secondarySize,
                              int tertiarySize,
                              long ttl,
                              @Nonnull final TimeUnit ttlTimeUnit,
                              long secondaryTtl,
                              @Nonnull final TimeUnit secondaryTtlTimeUnit,
                              long tertiaryTtl,
                              @Nonnull final TimeUnit tertiaryTtlTimeUnit,
                              @Nullable final Executor executor,
                              @Nullable final Executor secondaryExecutor,
                              @Nullable final Executor tertiaryExecutor,
                              @Nullable final Ticker ticker) {
        super(size, secondarySize, tertiarySize, ttl, ttlTimeUnit, secondaryTtl, secondaryTtlTimeUnit, tertiaryTtl, tertiaryTtlTimeUnit, executor, secondaryExecutor, tertiaryExecutor, ticker);
    }

    @ExcludeFromJacocoGeneratedReport // this is just a mechanical builder.
    public static final class RelationalCacheBuilder extends MultiStageCache.Builder<String, QueryCacheKey, PhysicalPlanEquivalence, Plan<?>, RelationalCacheBuilder> {

        public RelationalCacheBuilder() {
            size = (Integer) (Options.defaultOptions().get(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES));
            secondarySize = (Integer) (Options.defaultOptions().get(Options.Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES));
            tertiarySize = (Integer) (Options.defaultOptions().get(Options.Name.PLAN_CACHE_TERTIARY_MAX_ENTRIES));
            ttl = (Long) (Options.defaultOptions().get(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS));
            ttlTimeUnit = DEFAULT_TTL_TIME_UNIT;
            secondaryTtl = (Long) (Options.defaultOptions().get(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS));
            secondaryTtlTimeUnit = DEFAULT_SECONDARY_TTL_TIME_UNIT;
            tertiaryTtl = (Long) (Options.defaultOptions().get(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS));
            tertiaryTtlTimeUnit = DEFAULT_TERTIARY_TTL_TIME_UNIT;
            executor = null;
            secondaryExecutor = null;
            tertiaryExecutor = null;
            ticker = null;
        }

        @Nonnull
        @Override
        public RelationalPlanCache build() {
            return new RelationalPlanCache(size, secondarySize, tertiarySize, ttl, ttlTimeUnit, secondaryTtl, secondaryTtlTimeUnit, tertiaryTtl, tertiaryTtlTimeUnit, executor, secondaryExecutor, tertiaryExecutor, ticker);
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
