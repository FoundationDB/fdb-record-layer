/*
 * RecordQueryPlannerConfiguration.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryPlannerSortConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A set of configuration options for the {@link RecordQueryPlanner}.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryPlannerConfiguration {
    @Nonnull
    private final QueryPlanner.IndexScanPreference indexScanPreference;
    private final boolean attemptFailedInJoinAsOr;
    private final int attemptFailedInJoinAsUnionMaxSize;
    private final int complexityThreshold;
    private final boolean checkForDuplicateConditions;
    private final boolean deferFetchAfterUnionAndIntersection;
    private final boolean optimizeForIndexFilters;
    private final int maxTaskQueueSize;
    private final int maxTotalTaskCount;
    private final boolean useFullKeyForValueIndex;
    private final int maxNumMatchesPerRuleCall;
    @Nullable
    private final RecordQueryPlannerSortConfiguration sortConfiguration;

    private RecordQueryPlannerConfiguration(@Nonnull QueryPlanner.IndexScanPreference indexScanPreference,
                                            boolean attemptFailedInJoinAsOr,
                                            int attemptFailedInJoinAsUnionMaxSize,
                                            int complexityThreshold,
                                            boolean checkForDuplicateConditions,
                                            boolean deferFetchAfterUnionAndIntersection,
                                            boolean optimizeForIndexFilters,
                                            int maxTaskQueueSize,
                                            int maxTotalTaskCount,
                                            boolean useFullKeyForValueIndex,
                                            int maxNumMatchesPerRuleCall,
                                            @Nullable RecordQueryPlannerSortConfiguration sortConfiguration) {
        this.indexScanPreference = indexScanPreference;
        this.attemptFailedInJoinAsOr = attemptFailedInJoinAsOr;
        this.attemptFailedInJoinAsUnionMaxSize = attemptFailedInJoinAsUnionMaxSize;
        this.complexityThreshold = complexityThreshold;
        this.checkForDuplicateConditions = checkForDuplicateConditions;
        this.deferFetchAfterUnionAndIntersection = deferFetchAfterUnionAndIntersection;
        this.optimizeForIndexFilters = optimizeForIndexFilters;
        this.maxTaskQueueSize = maxTaskQueueSize;
        this.maxTotalTaskCount = maxTotalTaskCount;
        this.useFullKeyForValueIndex = useFullKeyForValueIndex;
        this.maxNumMatchesPerRuleCall = maxNumMatchesPerRuleCall;
        this.sortConfiguration = sortConfiguration;
    }

    /**
     * Get whether {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} is preferred over
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan} even when it does not satisfy any
     * additional conditions.
     * Scanning without an index is more efficient, but will have to skip over unrelated record types.
     * For that reason, it is safer to use an index, except when there is only one record type.
     * If the meta-data has more than one record type but the record store does not, this can be overridden.
     * @return the index scan preference
     */
    @Nonnull
    public QueryPlanner.IndexScanPreference getIndexScanPreference() {
        return indexScanPreference;
    }

    /**
     * Get whether the query planner should attempt to transform IN predicates that can't be implemented using a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan} into an equivalent OR of
     * equality predicates, which might be plannable as a union.
     * @return whether the planner will transform IN predicates into ORs when they can't be planned as in-joins
     */
    public boolean shouldAttemptFailedInJoinAsOr() {
        return attemptFailedInJoinAsOr;
    }

    /**
     * Get whether the query planner should attempt to transform IN predicates that can't be implemented using a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan} into a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan}.
     * @return whether the planner will transform IN predicates into a dynamic union when they can't be planned as in-joins
     */
    public boolean shouldAttemptFailedInJoinAsUnion() {
        return attemptFailedInJoinAsUnionMaxSize > 0;
    }

    /**
     * Get whether the query planner should attempt to transform IN predicates that can't be implemented using a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan} into a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan}.
     * @return the maximum number of total branches in the union allowed at execution time
     * or {@code 0} if this transformation is not allowed.
     */
    public int getAttemptFailedInJoinAsUnionMaxSize() {
        return attemptFailedInJoinAsUnionMaxSize;
    }

    /**
     * A limit on the complexity of the plans generated by the planner.
     * If the planner generates a query plan that exceeds this complexity, an exception will be thrown.
     * @return the complexity limit
     * See {@link QueryPlan#getComplexity} for a description of plan complexity.
     */
    public int getComplexityThreshold() {
        return complexityThreshold;
    }

    /**
     * Get whether normalization of query conditions should check for redundant conditions.
     * This check is not free, so it is appropriate only to enable this when the queries
     * come from a source known to include disjuncted conditions that imply one another.
     * @return whether normalization should check for redundant conditions
     * @see com.apple.foundationdb.record.query.plan.planning.BooleanNormalizer#isCheckForDuplicateConditions
     */
    public boolean shouldCheckForDuplicateConditions() {
        return checkForDuplicateConditions;
    }

    /**
     * Get whether the query planner should attempt to delay the fetch of the whole record until after union,
     * intersection, and primary key distinct operators, as implemented in the various
     * {@link com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor}s.
     * @return whether the planner should delay the fetch of the whole record until after union, intersection, and primary key distinct operators
     */
    public boolean shouldDeferFetchAfterUnionAndIntersection() {
        return deferFetchAfterUnionAndIntersection;
    }

    /**
     * Get whether the query planner should attempt to consider the applicability of filters that could then be
     * evaluated on index entries into the planning process.
     * @return whether the planner should optimize for index filters
     */
    public boolean shouldOptimizeForIndexFilters() {
        return optimizeForIndexFilters;
    }

    /**
     * Return the size limit of the cascades planner task queue.
     * @return the maximum size of the queue. 0 means "unbound" (the default). Trying to add a task beyond the maximum size will fail the planning.
     */
    public int getMaxTaskQueueSize() {
        return maxTaskQueueSize;
    }

    /**
     * Return the limit on the number of tasks that can be executed as part of the cascades planner planning.
     * @return the maximum number of tasks. 0 means "unbound" (the default). Trying to execute a task after the maximum number was exceeded will fail the planning.
     */
    public int getMaxTotalTaskCount() {
        return maxTotalTaskCount;
    }

    /**
     * Get whether the planner uses the entire key, including the primary key, for value indexes.
     * @return whether to include primary key in planning
     */
    public boolean shouldUseFullKeyForValueIndex() {
        return useFullKeyForValueIndex;
    }

    /**
     * Get the maximum number of matches that are permitted per rule call within the Cascades planner.
     * @return the maximum number of matches that are permitted per rule call within the Cascades planner
     */
    public int getMaxNumMatchesPerRuleCall() {
        return maxNumMatchesPerRuleCall;
    }

    /**
     * Get configuration for planning sorting, including whether the planner is allowed to use an in-memory sort plan.
     * @return configuration to use for planning non-index sorting, or {@code null} to never allow it
     */
    @Nullable
    public RecordQueryPlannerSortConfiguration getSortConfiguration() {
        return sortConfiguration;
    }

    @Nonnull
    public Builder asBuilder() {
        return new Builder(this);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link RecordQueryPlannerConfiguration}.
     */
    public static class Builder {
        @Nonnull
        private QueryPlanner.IndexScanPreference indexScanPreference = QueryPlanner.IndexScanPreference.PREFER_SCAN;
        private boolean attemptFailedInJoinAsOr = false;
        private int attemptFailedInJoinAsUnionMaxSize = 0;
        private int complexityThreshold = RecordQueryPlanner.DEFAULT_COMPLEXITY_THRESHOLD;
        private boolean checkForDuplicateConditions = false;
        private boolean deferFetchAfterUnionAndIntersection = false;
        private boolean optimizeForIndexFilters = false;
        private int maxTaskQueueSize = 0;
        private int maxTotalTaskCount = 0;
        private boolean useFullKeyForValueIndex = true;
        private int maxNumMatchesPerRuleCall = 0;
        @Nullable
        private RecordQueryPlannerSortConfiguration sortConfiguration;

        public Builder(@Nonnull RecordQueryPlannerConfiguration configuration) {
            this.indexScanPreference = configuration.indexScanPreference;
            this.attemptFailedInJoinAsOr = configuration.attemptFailedInJoinAsOr;
            this.attemptFailedInJoinAsUnionMaxSize = configuration.attemptFailedInJoinAsUnionMaxSize;
            this.complexityThreshold = configuration.complexityThreshold;
            this.checkForDuplicateConditions = configuration.checkForDuplicateConditions;
            this.deferFetchAfterUnionAndIntersection = configuration.deferFetchAfterUnionAndIntersection;
            this.optimizeForIndexFilters = configuration.optimizeForIndexFilters;
            this.maxTaskQueueSize = configuration.maxTaskQueueSize;
            this.maxTotalTaskCount = configuration.maxTotalTaskCount;
            this.useFullKeyForValueIndex = configuration.useFullKeyForValueIndex;
            this.maxNumMatchesPerRuleCall = configuration.maxNumMatchesPerRuleCall;
            this.sortConfiguration = configuration.sortConfiguration;
        }

        public Builder() {
        }

        public Builder setIndexScanPreference(@Nonnull QueryPlanner.IndexScanPreference indexScanPreference) {
            this.indexScanPreference = indexScanPreference;
            return this;
        }

        public Builder setAttemptFailedInJoinAsOr(boolean attemptFailedInJoinAsOr) {
            this.attemptFailedInJoinAsOr = attemptFailedInJoinAsOr;
            return this;
        }

        public Builder setAttemptFailedInJoinAsUnionMaxSize(int attemptFailedInJoinAsUnionMaxSize) {
            this.attemptFailedInJoinAsUnionMaxSize = attemptFailedInJoinAsUnionMaxSize;
            return this;
        }

        public Builder setComplexityThreshold(final int complexityThreshold) {
            this.complexityThreshold = complexityThreshold;
            return this;
        }

        public Builder setCheckForDuplicateConditions(final boolean checkForDuplicateConditions) {
            this.checkForDuplicateConditions = checkForDuplicateConditions;
            return this;
        }

        public Builder setDeferFetchAfterUnionAndIntersection(boolean deferFetchAfterUnionAndIntersection) {
            this.deferFetchAfterUnionAndIntersection = deferFetchAfterUnionAndIntersection;
            return this;
        }

        public Builder setOptimizeForIndexFilters(final boolean optimizeForIndexFilters) {
            this.optimizeForIndexFilters = optimizeForIndexFilters;
            return this;
        }

        /**
         * Set the size limit of the Cascades planner task queue.
         * If the planner tries to add a task to the queue beyond the maximum size, planning will fail.
         * Default value is 0, which means "unbound".
         * @param maxTaskQueueSize the maximum size of the queue.
         * @return this builder
         */
        public Builder setMaxTaskQueueSize(final int maxTaskQueueSize) {
            this.maxTaskQueueSize = maxTaskQueueSize;
            return this;
        }

        /**
         * Set a limit on the number of tasks that can be executed as part of the Cascades planner planning.
         * If the planner tries to execute a task after the maximum number was exceeded, planning will fail.
         * Default value is 0, which means "unbound".
         * @param maxTotalTaskCount the maximum number of tasks.
         * @return this builder
         */
        public Builder setMaxTotalTaskCount(final int maxTotalTaskCount) {
            this.maxTotalTaskCount = maxTotalTaskCount;
            return this;
        }

        /**
         * Set whether the planner uses the entire key, including the primary key, for value indexes.
         * @param useFullKeyForValueIndex whether to include primary key in planning
         * @return this builder
         */
        public Builder setUseFullKeyForValueIndex(final boolean useFullKeyForValueIndex) {
            this.useFullKeyForValueIndex = useFullKeyForValueIndex;
            return this;
        }

        /**
         * Set the maximum number of matches that are permitted per rule call within the Cascades planner.
         * Default value is 0, which means "unbound".
         * @param maxNumMatchesPerRuleCall the desired maximum number of matches that are permitted per rule call
         * @return {@code this}
         */
        public Builder setMaxNumMatchesPerRuleCall(final int maxNumMatchesPerRuleCall) {
            this.maxNumMatchesPerRuleCall = maxNumMatchesPerRuleCall;
            return this;
        }

        /**
         * Set configuration for planning sorting, including whether the planner is allowed to use an in-memory sort plan.
         * @param sortConfiguration configuration to use for planning non-index sorting, or {@code null} to never allow it
         * @return this builder
         */
        public Builder setSortConfiguration(final RecordQueryPlannerSortConfiguration sortConfiguration) {
            this.sortConfiguration = sortConfiguration;
            return this;
        }

        /**
         * Set whether the planner is allowed to use an in-memory sort plan.
         * @param allowNonIndexSort whether to allow non-index sorting
         * @return this builder
         */
        public Builder setAllowNonIndexSort(final boolean allowNonIndexSort) {
            setSortConfiguration(allowNonIndexSort ? RecordQueryPlannerSortConfiguration.getDefaultInstance() : null);
            return this;
        }
 
        public RecordQueryPlannerConfiguration build() {
            return new RecordQueryPlannerConfiguration(indexScanPreference, attemptFailedInJoinAsOr, attemptFailedInJoinAsUnionMaxSize, complexityThreshold, checkForDuplicateConditions, deferFetchAfterUnionAndIntersection, optimizeForIndexFilters, maxTaskQueueSize, maxTotalTaskCount, useFullKeyForValueIndex, maxNumMatchesPerRuleCall, sortConfiguration);
        }
    }
}
