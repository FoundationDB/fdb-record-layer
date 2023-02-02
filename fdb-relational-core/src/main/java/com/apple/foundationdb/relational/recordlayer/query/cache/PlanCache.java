/*
 * PlanCache.java
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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.LogicalQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A Cache for Query Plans.
 *
 * Implementations of this interface <em>must</em> be thread-safe (and some implementations will probably
 * be process-safe as well, depending on their structure).
 *
 * In Relational, a PlanCache is a complicated beast. Because we have many schemas which have identical (
 * or near identical) structure, we are going to see a lot of different copies of the same logical query applied
 * to different databases, but which will fundamentally plan the same way. Thus, it is critical that a cache
 * be applicable across Schema structures.
 */
@ThreadSafe
public interface PlanCache {
    @Nullable
    RecordQueryPlan getPlan(@Nonnull LogicalQuery query,  //TODO(bfines) replace this with a STABLE logical query representation
                            @Nonnull SchemaState schemaState);

    void cacheEntry(@Nonnull LogicalQuery query,
                    @Nonnull RecordQueryPlan plan);

    CacheStatistics getStats();

    interface CacheValidityCondition {
        boolean test(@Nonnull LogicalQuery query,
                     @Nonnull PlanState cachedState,
                     @Nonnull SchemaState schemaState);
    }
}
