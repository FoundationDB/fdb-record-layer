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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
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

    public static RecordQueryPlannerSortConfiguration getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    protected RecordQueryPlannerSortConfiguration() {
    }

    /**
     * Get whether the planner is allowed to use an in-memory sort plan.
     *
     * This method returns {@code true}.
     *
     * A subclass can override this method to use more complex criteria to decide.
     * @param query the query that cannot be planned without a sort
     * @return whether to allow non-index sorting
     */
    public boolean shouldAllowNonIndexSort(@Nonnull RecordQuery query) {
        return true;
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
}
