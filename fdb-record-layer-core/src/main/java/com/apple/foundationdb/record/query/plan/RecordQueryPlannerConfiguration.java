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
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.rules.PredicateToLogicalUnionRule;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryPlannerSortConfiguration;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A set of configuration options for the {@link RecordQueryPlanner}.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryPlannerConfiguration {
    @Nonnull
    private static final RecordQueryPlannerConfiguration DEFAULT_PLANNER_CONFIGURATION = builder().build();

    @Nonnull
    private final QueryPlanner.IndexScanPreference indexScanPreference;
    private final boolean attemptFailedInJoinAsOr;
    private final int attemptFailedInJoinAsUnionMaxSize;
    private final int complexityThreshold;
    private final boolean checkForDuplicateConditions;
    private final boolean deferFetchAfterUnionAndIntersection;
    private final boolean deferFetchAfterInJoinAndInUnion;
    private final boolean omitPrimaryKeyInUnionOrderingKey;
    private final boolean optimizeForIndexFilters;
    private final boolean optimizeForRequiredResults;
    private final int maxTaskQueueSize;
    private final int maxTotalTaskCount;
    private final boolean useFullKeyForValueIndex;
    private final int maxNumMatchesPerRuleCall;
    @Nullable
    private final RecordQueryPlannerSortConfiguration sortConfiguration;
    @Nonnull
    private final Set<Class<? extends CascadesRule<?>>> disabledTransformationRules;
    private final boolean deferCrossProducts;
    private final IndexFetchMethod indexFetchMethod;

    /**
     * The value index's names that {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} with
     * {@link com.apple.foundationdb.record.IndexScanType#BY_VALUE_OVER_SCAN} is preferred to use.
     */
    private final Set<String> valueIndexesOverScanNeeded;
    private final boolean planOtherAttemptWholeFilter;
    private final int maxNumReplansForInToJoin;
    private final int orToUnionMaxNumConjuncts;

    private RecordQueryPlannerConfiguration(@Nonnull RecordQueryPlannerConfiguration.Builder builder) {
        this.indexScanPreference = builder.indexScanPreference;
        this.attemptFailedInJoinAsOr = builder.attemptFailedInJoinAsOr;
        this.attemptFailedInJoinAsUnionMaxSize = builder.attemptFailedInJoinAsUnionMaxSize;
        this.complexityThreshold = builder.complexityThreshold;
        this.checkForDuplicateConditions = builder.checkForDuplicateConditions;
        this.deferFetchAfterUnionAndIntersection = builder.deferFetchAfterUnionAndIntersection;
        this.deferFetchAfterInJoinAndInUnion = builder.deferFetchAfterInJoinAndInUnion;
        this.omitPrimaryKeyInUnionOrderingKey = builder.omitPrimaryKeyInUnionOrderingKey;
        this.optimizeForIndexFilters = builder.optimizeForIndexFilters;
        this.optimizeForRequiredResults = builder.optimizeForRequiredResults;
        this.maxTaskQueueSize = builder.maxTaskQueueSize;
        this.maxTotalTaskCount = builder.maxTotalTaskCount;
        this.useFullKeyForValueIndex = builder.useFullKeyForValueIndex;
        this.maxNumMatchesPerRuleCall = builder.maxNumMatchesPerRuleCall;
        this.sortConfiguration = builder.sortConfiguration;
        this.disabledTransformationRules = ImmutableSet.copyOf(builder.disabledTransformationRules);
        this.deferCrossProducts = builder.deferCrossProducts;
        this.indexFetchMethod = builder.indexFetchMethod;
        this.valueIndexesOverScanNeeded = builder.valueIndexesOverScanNeeded;
        this.planOtherAttemptWholeFilter = builder.planOtherAttemptWholeFilter;
        this.maxNumReplansForInToJoin = builder.maxNumReplansForInToJoin;
        this.orToUnionMaxNumConjuncts = builder.orToUnionMaxNumConjuncts;
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
     * Get whether the query planner should attempt to delay the fetch of the whole record until after in-join or in union operators.
     * @return whether the planner should delay the fetch of the whole record until after in-join or in union operators.
     */
    public boolean shouldDeferFetchAfterInJoinAndInUnion() {
        return deferFetchAfterInJoinAndInUnion;
    }

    public boolean shouldOmitPrimaryKeyInUnionOrderingKey() {
        return omitPrimaryKeyInUnionOrderingKey;
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
     * Get whether the query planner should attempt to consider the set of required result fields while finding the
     * best index for a record type access. If a query does not set the set of required results
     * ({@link RecordQuery#getRequiredResults()}), enabling this switch is meaningless.
     * @return whether the planner should optimize for the set of required results
     */
    public boolean shouldOptimizeForRequiredResults() {
        return optimizeForRequiredResults;
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

    /**
     * Method to return if a particular rule is enabled per this configuration.
     * @param rule in question
     * @return {@code true} is enabled, {@code false} otherwise
     */
    public boolean isRuleEnabled(@Nonnull CascadesRule<?> rule) {
        return !disabledTransformationRules.contains(rule.getClass());
    }

    /**
     * Return whether the query planner should defer cross products during join enumeration.
     * <br>
     * Cross products are joins of data sources that are completely independent of one another, i.e., the data sources
     * are not <em>connected</em> via a join predicate, nor is one correlated to the other. It is usually better to
     * plan a cross product as the last join in a join tree as a cross product tends to increase the cardinality.
     * Thus, such a join does not need to be planned among all the other joins and can therefore avoid useless
     * exploration and over-enumeration.
     * <br>
     * Unfortunately, there are cases, however, where it may be beneficial to treat a cross product like any other
     * join in a query. For instance, in star-like schemas, the dimension table cross products are usually of a low
     * cardinality due to some highly-selective predicates and can be joined upfront before joining the result with
     * the fact table.
     * @return {@code true} if the planner should defer cross products or {@code false}, if the planner should plan
     *         them like other joins.
     */
    public boolean shouldDeferCrossProducts() {
        return deferCrossProducts;
    }

    /**
     * Whether the planner should use IndexPrefetch operations for the index scan plans. IndexPrefetch operations
     * use the DB's API to fetch records from the index, rather than return the index entries, followed
     * by record fetches.
     * @return Whether the planner should use index prefetch in the plans
     */
    @Nonnull
    public IndexFetchMethod getIndexFetchMethod() {
        return indexFetchMethod;
    }

    public boolean valueIndexOverScanNeeded(@Nonnull String indexName) {
        return valueIndexesOverScanNeeded.contains(indexName);
    }

    public boolean shouldPlanOtherAttemptWholeFilter() {
        return planOtherAttemptWholeFilter;
    }

    public int getMaxNumReplansForInToJoin() {
        return maxNumReplansForInToJoin;
    }

    /**
     * Returns the maximum number of conjuncts whose combinations are enumerated when {@link PredicateToLogicalUnionRule} is
     * applied.
     * @return the maximum number of conjuncts
     */
    public int getOrToUnionMaxNumConjuncts() {
        return orToUnionMaxNumConjuncts;
    }

    @Nonnull
    public Builder asBuilder() {
        return new Builder(this);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    public static RecordQueryPlannerConfiguration defaultPlannerConfiguration() {
        return DEFAULT_PLANNER_CONFIGURATION;
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
        private boolean deferFetchAfterInJoinAndInUnion = false;
        private boolean omitPrimaryKeyInUnionOrderingKey = false;
        private boolean optimizeForIndexFilters = false;
        private boolean optimizeForRequiredResults = false;
        private int maxTaskQueueSize = 0;
        private int maxTotalTaskCount = 0;
        private boolean useFullKeyForValueIndex = true;
        private int maxNumMatchesPerRuleCall = 0;
        @Nullable
        private RecordQueryPlannerSortConfiguration sortConfiguration;
        private boolean deferCrossProducts = true;
        @Nonnull
        private Set<Class<? extends CascadesRule<?>>> disabledTransformationRules = Sets.newHashSet();
        @Nonnull
        private IndexFetchMethod indexFetchMethod = IndexFetchMethod.SCAN_AND_FETCH;

        @Nonnull
        private Set<String> valueIndexesOverScanNeeded = Sets.newHashSet();
        private boolean planOtherAttemptWholeFilter;
        private int maxNumReplansForInToJoin = 0;

        private int orToUnionMaxNumConjuncts = PredicateToLogicalUnionRule.DEFAULT_MAX_NUM_CONJUNCTS;

        public Builder(@Nonnull RecordQueryPlannerConfiguration configuration) {
            this.indexScanPreference = configuration.indexScanPreference;
            this.attemptFailedInJoinAsOr = configuration.attemptFailedInJoinAsOr;
            this.attemptFailedInJoinAsUnionMaxSize = configuration.attemptFailedInJoinAsUnionMaxSize;
            this.complexityThreshold = configuration.complexityThreshold;
            this.checkForDuplicateConditions = configuration.checkForDuplicateConditions;
            this.deferFetchAfterUnionAndIntersection = configuration.deferFetchAfterUnionAndIntersection;
            this.deferFetchAfterInJoinAndInUnion = configuration.deferFetchAfterInJoinAndInUnion;
            this.omitPrimaryKeyInUnionOrderingKey = configuration.omitPrimaryKeyInUnionOrderingKey;
            this.optimizeForIndexFilters = configuration.optimizeForIndexFilters;
            this.optimizeForRequiredResults = configuration.optimizeForRequiredResults;
            this.maxTaskQueueSize = configuration.maxTaskQueueSize;
            this.maxTotalTaskCount = configuration.maxTotalTaskCount;
            this.useFullKeyForValueIndex = configuration.useFullKeyForValueIndex;
            this.maxNumMatchesPerRuleCall = configuration.maxNumMatchesPerRuleCall;
            this.sortConfiguration = configuration.sortConfiguration;
            this.disabledTransformationRules = configuration.disabledTransformationRules;
            this.indexFetchMethod = configuration.indexFetchMethod;
            this.valueIndexesOverScanNeeded = configuration.valueIndexesOverScanNeeded;
            this.orToUnionMaxNumConjuncts = configuration.orToUnionMaxNumConjuncts;
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

        public Builder setDeferFetchAfterInJoinAndInUnion(boolean deferFetchAfterInJoinAndInUnion) {
            this.deferFetchAfterInJoinAndInUnion = deferFetchAfterInJoinAndInUnion;
            return this;
        }

        public Builder setOmitPrimaryKeyInUnionOrderingKey(boolean omitPrimaryKeyInUnionOrderingKey) {
            this.omitPrimaryKeyInUnionOrderingKey = omitPrimaryKeyInUnionOrderingKey;
            return this;
        }

        public Builder setOptimizeForIndexFilters(final boolean optimizeForIndexFilters) {
            this.optimizeForIndexFilters = optimizeForIndexFilters;
            return this;
        }

        /**
         * Set whether the query planner should attempt to consider the set of required result fields while finding the
         * best index for a record type access. If a query does not set the set of required results
         * ({@link RecordQuery#getRequiredResults()}), enabling this switch is meaningless.
         * @param optimizeForRequiredResults set the optimizeForRequiredResults parameter
         * @return this builder
         */
        public Builder setOptimizeForRequiredResults(final boolean optimizeForRequiredResults) {
            this.optimizeForRequiredResults = optimizeForRequiredResults;
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

        /**
         * Set a set of planner transformation rules that should be considered disabled for any planning effort.
         * @param disabledTransformationRules a set of disabled rules.
         * @return this builder
         */
        @Nonnull
        public Builder setDisabledTransformationRules(@Nonnull final Set<Class<? extends CascadesRule<?>>> disabledTransformationRules) {
            this.disabledTransformationRules = Sets.newHashSet(disabledTransformationRules);
            return this;
        }

        /**
         * Set a set of rules names that identify planner transformation rules that should be considered disabled
         * for any planning effort.
         * @param disabledTransformationRuleNames a set of rule names identifying (via simple class name)
         *        transformation rules
         * @param plannerRuleSet a {@link PlannerRuleSet} that is used to resolve the rule name to a rule class
         * @return this builder
         */
        @SuppressWarnings("unchecked")
        @Nonnull
        public Builder setDisabledTransformationRuleNames(@Nonnull final Set<String> disabledTransformationRuleNames, @Nonnull PlannerRuleSet plannerRuleSet) {
            final Stream<? extends CascadesRule<?>> allRules = plannerRuleSet.getAllRules();
            this.disabledTransformationRules =
                    allRules.map(rule -> (Class<? extends CascadesRule<?>>)rule.getClass())
                            .filter(ruleClass -> disabledTransformationRuleNames.contains(ruleClass.getSimpleName()))
                            .collect(Collectors.toSet());
            return this;
        }

        /**
         * Helper method to disable the planner transformation rule class passed in.
         * @param ruleClass a rule class that should be disabled
         * @return this builder
         */
        @Nonnull
        public Builder disableTransformationRule(@Nonnull Class<? extends CascadesRule<?>> ruleClass) {
            this.disabledTransformationRules.add(ruleClass);
            return this;
        }

        /**
         * Set whether the query planner should defer cross products during join enumeration.
         * <br>
         * Cross products are joins of data sources that are completely independent of one another, i.e., the data sources
         * are not <em>connected</em> via a join predicate, nor is one correlated to the other. It is usually better to
         * plan a cross product as the last join in a join tree as a cross product tends to increase the cardinality.
         * Thus, such a join does not need to be planned among all the other joins and can therefore avoid useless
         * exploration and over-enumeration.
         * <br>
         * Unfortunately, there are cases, however, where it may be beneficial to treat a cross product like any other
         * join in a query.
         * @param deferCrossProducts indicator if the planner should defer cross products
         * @return whether the planner will defer cross products or plan them like other joins
         */
        public Builder setDeferCrossProducts(final boolean deferCrossProducts) {
            this.deferCrossProducts = deferCrossProducts;
            return this;
        }

        /**
         * Set whether the planner should use FDB remote fetch operations for the index scan plans. Remote fetch operations
         * use the DB's API to fetch records from the index, rather than return the index entries, followed
         * by record fetches.
         * @param indexFetchMethod whether to use IndexFetch in the scan plans
         * @return this builder
         */
        @API(API.Status.EXPERIMENTAL)
        public Builder setIndexFetchMethod(@Nonnull final IndexFetchMethod indexFetchMethod) {
            this.indexFetchMethod = indexFetchMethod;
            return this;
        }

        @API(API.Status.EXPERIMENTAL)
        public Builder addValueIndexOverScanNeeded(@Nonnull final String indexName) {
            this.valueIndexesOverScanNeeded.add(indexName);
            return this;
        }

        /**
         * Set whether the planner attempts to plan a complex filter using non-VALUE indexes before splitting it up.
         * @param planOtherAttemptWholeFilter whether to attempt planning the whole filter
         * @return this builder
         */
        @API(API.Status.EXPERIMENTAL)
        public Builder setPlanOtherAttemptWholeFilter(final boolean planOtherAttemptWholeFilter) {
            this.planOtherAttemptWholeFilter = planOtherAttemptWholeFilter;
            return this;
        }

        /**
         * Set the maximum number of replanning-attempts during IN-to-JOIN transformations in the planner. This
         * option only applies to {@link RecordQueryPlanner}.
         * @param maxNumReplansForInToJoin the number of replanning attempts; defaults to {@code 0} for no
         *        replanning attempts
         * @return this builder
         */
        public Builder setMaxNumReplansForInToJoin(final int maxNumReplansForInToJoin) {
            this.maxNumReplansForInToJoin = maxNumReplansForInToJoin;
            return this;
        }

        /**
         * Set the maximum number of conjuncts whose combinations are enumerated when {@link PredicateToLogicalUnionRule} is
         * applied. This option only applies to {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
         * @param orToUnionMaxNumConjuncts the maximum number of conjuncts
         * @return this builder
         */
        public Builder setOrToUnionMaxNumConjuncts(final int orToUnionMaxNumConjuncts) {
            this.orToUnionMaxNumConjuncts = orToUnionMaxNumConjuncts;
            return this;
        }

        public RecordQueryPlannerConfiguration build() {
            return new RecordQueryPlannerConfiguration(this);
        }
    }
}
