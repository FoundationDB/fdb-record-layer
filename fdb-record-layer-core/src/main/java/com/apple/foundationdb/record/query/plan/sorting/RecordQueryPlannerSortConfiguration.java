/*
 * RecordQueryPlannerSortConfiguration.java
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

package com.apple.foundationdb.record.query.plan.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordPlannerConfigurationProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;

import javax.annotation.Nonnull;

/**
 * Configuration for planning of non-index sort queries.
 *
 * @see RecordQueryPlannerConfiguration#getSortConfiguration
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQueryPlannerSortConfiguration {
    private static final RecordQueryPlannerSortConfiguration DEFAULT_INSTANCE = new RecordQueryPlannerSortConfiguration();

    @Nonnull
    private final RecordPlannerConfigurationProto.PlannerConfiguration.SortConfiguration proto;

    protected RecordQueryPlannerSortConfiguration() {
        this(RecordPlannerConfigurationProto.PlannerConfiguration.SortConfiguration.newBuilder()
                .setShouldAllowNonIndexSort(true)
                .build());
    }

    private RecordQueryPlannerSortConfiguration(@Nonnull RecordPlannerConfigurationProto.PlannerConfiguration.SortConfiguration proto) {
        this.proto = proto;
    }

    /**
     * Get whether the planner is allowed to use an in-memory sort plan.
     *
     * This method returns {@code true}.
     *
     * A subclass can override this method to use more complex criteria to decide.
     * @return whether to allow non-index sorting
     */
    public boolean shouldAllowNonIndexSort() {
        return proto.getShouldAllowNonIndexSort();
    }

    @Nonnull
    public RecordPlannerConfigurationProto.PlannerConfiguration.SortConfiguration toProto() {
        return proto;
    }

    /**
     * Get the sort key for the given key expression and direction.
     *
     * A subclass can override this method to return a subclass of {@link RecordQuerySortKey} and thereby affect
     * the {@link RecordQuerySortAdapter} used.
     * @param key required sort key
     * @param reverse whether to sort in reverse
     * @return a new sort key instance
     * @see RecordQuerySortKey#getAdapter
     */
    @Nonnull
    public RecordQuerySortKey getSortKey(@Nonnull KeyExpression key, boolean reverse) {
        return new RecordQuerySortKey(key, reverse);
    }

    @Nonnull
    public static RecordQueryPlannerSortConfiguration getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    @Nonnull
    public static RecordQueryPlannerSortConfiguration fromProto(@Nonnull RecordPlannerConfigurationProto.PlannerConfiguration.SortConfiguration proto) {
        return new RecordQueryPlannerSortConfiguration(proto);
    }
}
