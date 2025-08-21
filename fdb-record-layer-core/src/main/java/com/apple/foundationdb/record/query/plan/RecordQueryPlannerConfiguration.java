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
import com.apple.foundationdb.record.RecordPlannerConfigurationProto;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlanningRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.RewritingRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.rules.PredicateToLogicalUnionRule;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryPlannerSortConfiguration;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * A set of configuration options for the {@link RecordQueryPlanner}.
 */
@API(API.Status.UNSTABLE)
public class RecordQueryPlannerConfiguration {
    @Nonnull
    private static final BiMap<QueryPlanner.IndexScanPreference, RecordPlannerConfigurationProto.PlannerConfiguration.IndexScanPreference> SCAN_PREFERENCE_BI_MAP =
            PlanSerialization.protoEnumBiMap(QueryPlanner.IndexScanPreference.class, RecordPlannerConfigurationProto.PlannerConfiguration.IndexScanPreference.class);
    @Nonnull
    private static final BiMap<IndexFetchMethod, RecordPlannerConfigurationProto.PlannerConfiguration.IndexFetchMethod> FETCH_METHOD_BI_MAP =
            PlanSerialization.protoEnumBiMap(IndexFetchMethod.class, RecordPlannerConfigurationProto.PlannerConfiguration.IndexFetchMethod.class);
    @Nonnull
    private static final RecordQueryPlannerConfiguration DEFAULT_PLANNER_CONFIGURATION = builder().build();

    // Masks used for encoding multiple Boolean flags into a single long
    private static final long ATTEMPT_FAILED_IN_JOIN_AS_OR_MASK = 1L;
    private static final long CHECK_FOR_DUPLICATE_CONDITIONS_MASK = 1L << 1;
    private static final long DEFER_FETCH_AFTER_UNION_AND_INTERSECTION_MASK = 1L << 2;
    private static final long DEFER_FETCH_AFTER_IN_JOIN_AND_IN_UNION_MASK = 1L << 3;
    private static final long OMIT_PRIMARY_KEY_IN_UNION_ORDERING_KEY_MASK = 1L << 4;
    private static final long OPTIMIZE_FOR_INDEX_FILTERS_MASK = 1L << 5;
    private static final long OPTIMIZE_FOR_REQUIRED_RESULTS_MASK = 1L << 6;
    private static final long DONT_USE_FULL_KEY_FOR_VALUE_INDEX_MASK = 1L << 7;
    private static final long DONT_DEFER_CROSS_PRODUCTS_MASK = 1L << 8;
    private static final long PLAN_OTHER_ATTEMPT_FULL_FILTER_MASK = 1L << 9;
    private static final long NORMALIZE_NESTED_FIELDS_MASK = 1L << 10;
    private static final long OMIT_PRIMARY_KEY_IN_ORDERING_KEY_FOR_IN_UNION_MASK = 1L << 11;

    @Nonnull
    private final RecordPlannerConfigurationProto.PlannerConfiguration proto;
    @Nonnull
    private final QueryPlanner.IndexScanPreference indexScanPreference;
    @Nonnull
    private final IndexFetchMethod indexFetchMethod;
    @Nonnull
    private final Set<String> disabledTransformationRules;
    /**
     * The value index's names that {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} with
     * {@link com.apple.foundationdb.record.IndexScanType#BY_VALUE_OVER_SCAN} is preferred to use.
     */
    @Nonnull
    private final Set<String> valueIndexesOverScanNeeded;
    @Nullable
    private final RecordQueryPlannerSortConfiguration sortConfiguration;


    private RecordQueryPlannerConfiguration(@Nonnull RecordPlannerConfigurationProto.PlannerConfiguration proto, @Nullable RecordQueryPlannerSortConfiguration sortConfiguration) {
        this.proto = proto;
        this.indexScanPreference = SCAN_PREFERENCE_BI_MAP.inverse().get(proto.getIndexScanPreference());
        this.indexFetchMethod = FETCH_METHOD_BI_MAP.inverse().get(proto.getIndexFetchMethod());
        this.disabledTransformationRules = ImmutableSet.copyOf(proto.getDisabledTransformationRulesList());
        this.valueIndexesOverScanNeeded = ImmutableSet.copyOf(proto.getValueIndexesOverScanNeededList());
        this.sortConfiguration = sortConfiguration;
    }

    private boolean flagSet(long mask) {
        return (proto.getFlags() & mask) != 0;
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
        return flagSet(ATTEMPT_FAILED_IN_JOIN_AS_OR_MASK);
    }

    /**
     * Get whether the query planner should attempt to transform IN predicates that can't be implemented using a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan} into a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan}.
     * @return whether the planner will transform IN predicates into a dynamic union when they can't be planned as in-joins
     */
    public boolean shouldAttemptFailedInJoinAsUnion() {
        return getAttemptFailedInJoinAsUnionMaxSize() > 0;
    }

    /**
     * Get whether the query planner should attempt to transform IN predicates that can't be implemented using a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan} into a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan}.
     * @return the maximum number of total branches in the union allowed at execution time
     * or {@code 0} if this transformation is not allowed.
     */
    public int getAttemptFailedInJoinAsUnionMaxSize() {
        return proto.getAttemptFailedInJoinAsUnionMaxSize();
    }

    /**
     * A limit on the complexity of the plans generated by the planner.
     * If the planner generates a query plan that exceeds this complexity, an exception will be thrown.
     * @return the complexity limit
     * See {@link QueryPlan#getComplexity} for a description of plan complexity.
     */
    public int getComplexityThreshold() {
        return proto.hasComplexityThreshold() ? proto.getComplexityThreshold() : RecordQueryPlanner.DEFAULT_COMPLEXITY_THRESHOLD;
    }

    /**
     * Get whether normalization of query conditions should check for redundant conditions.
     * This check is not free, so it is appropriate only to enable this when the queries
     * come from a source known to include disjuncted conditions that imply one another.
     * @return whether normalization should check for redundant conditions
     * @see com.apple.foundationdb.record.query.plan.planning.BooleanNormalizer#isCheckForDuplicateConditions
     */
    public boolean shouldCheckForDuplicateConditions() {
        return flagSet(CHECK_FOR_DUPLICATE_CONDITIONS_MASK);
    }

    /**
     * Get whether the query planner should attempt to delay the fetch of the whole record until after union,
     * intersection, and primary key distinct operators, as implemented in the various
     * {@link com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor}s.
     * @return whether the planner should delay the fetch of the whole record until after union, intersection, and primary key distinct operators
     */
    public boolean shouldDeferFetchAfterUnionAndIntersection() {
        return flagSet(DEFER_FETCH_AFTER_UNION_AND_INTERSECTION_MASK);
    }

    /**
     * Get whether the query planner should attempt to delay the fetch of the whole record until after in-join or in union operators.
     * @return whether the planner should delay the fetch of the whole record until after in-join or in union operators.
     */
    public boolean shouldDeferFetchAfterInJoinAndInUnion() {
        return flagSet(DEFER_FETCH_AFTER_IN_JOIN_AND_IN_UNION_MASK);
    }

    /**
     * Get whether the query planner should omit the common primary key from the ordering key for {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan}s.
     * By default, the query planner always requires that all legs of an ordered union plan are ordered first by the requested
     * sort and then by the common primary key (if one is present between queried types). This can restrict the indexes
     * that can be matched for queries with {@link com.apple.foundationdb.record.query.expressions.OrComponent}s, and so the
     * suggestion is to set this parameter to be more permissive. This property exists mainly for backwards compatibility
     * reasons, as flipping this value can result in plans changing.
     *
     * @return whether to allow union ordering keys that do not contain the primary key
     */
    public boolean shouldOmitPrimaryKeyInUnionOrderingKey() {
        return flagSet(OMIT_PRIMARY_KEY_IN_UNION_ORDERING_KEY_MASK);
    }

    /**
     * Get whether the query planner should omit the common primary key from the ordering key for {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan}s.
     * By default, the query planner always requires that all legs of an in-union plan are ordered first by the requested
     * sort and then by the common primary key (if one is present between queried types). This can restrict the indexes
     * that can be matched for queries with that have both {@code IN} predicates and a specified sort constraint, and so the
     * suggestion is to set this parameter to be more permissive. This property exists mainly for backwards compatibility
     * reasons, as flipping this value can result in plans changing.
     *
     * @return whether to allow in-union ordering keys that do not contain the primary key
     */
    public boolean shouldOmitPrimaryKeyInOrderingKeyForInUnion() {
        return flagSet(OMIT_PRIMARY_KEY_IN_ORDERING_KEY_FOR_IN_UNION_MASK);
    }

    /**
     * Get whether the query planner should attempt to consider the applicability of filters that could then be
     * evaluated on index entries into the planning process.
     * @return whether the planner should optimize for index filters
     */
    public boolean shouldOptimizeForIndexFilters() {
        return flagSet(OPTIMIZE_FOR_INDEX_FILTERS_MASK);
    }

    /**
     * Get whether the query planner should attempt to consider the set of required result fields while finding the
     * best index for a record type access. If a query does not set the set of required results
     * ({@link RecordQuery#getRequiredResults()}), enabling this switch is meaningless.
     * @return whether the planner should optimize for the set of required results
     */
    public boolean shouldOptimizeForRequiredResults() {
        return flagSet(OPTIMIZE_FOR_REQUIRED_RESULTS_MASK);
    }

    /**
     * Return the size limit of the cascades planner task queue.
     * @return the maximum size of the queue. 0 means "unbound" (the default). Trying to add a task beyond the maximum size will fail the planning.
     */
    public int getMaxTaskQueueSize() {
        return proto.getMaxTaskQueueSize();
    }

    /**
     * Return the limit on the number of tasks that can be executed as part of the cascades planner planning.
     * @return the maximum number of tasks. 0 means "unbound" (the default). Trying to execute a task after the maximum number was exceeded will fail the planning.
     */
    public int getMaxTotalTaskCount() {
        return proto.getMaxTotalTaskCount();
    }

    /**
     * Get whether the planner uses the entire key, including the primary key, for value indexes.
     * @return whether to include primary key in planning
     */
    public boolean shouldUseFullKeyForValueIndex() {
        // Value defaults to true, so return *false* if the flag is set
        return !flagSet(DONT_USE_FULL_KEY_FOR_VALUE_INDEX_MASK);
    }

    /**
     * Get the maximum number of matches that are permitted per rule call within the Cascades planner.
     * @return the maximum number of matches that are permitted per rule call within the Cascades planner
     */
    public int getMaxNumMatchesPerRuleCall() {
        return proto.getMaxNumMatchesPerRuleCall();
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
    public boolean isRuleEnabled(@Nonnull PlannerRule<?, ?> rule) {
        return !disabledTransformationRules.contains(rule.getClass().getSimpleName());
    }

    /**
     * Returns the set of disabled transformation rules.
     * @return set of disabled transformation rules.
     */
    @Nonnull
    public Set<String> getDisabledTransformationRules() {
        return disabledTransformationRules;
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
        // Value defaults to true, so return *false* if the flag is set
        return !flagSet(DONT_DEFER_CROSS_PRODUCTS_MASK);
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
        return flagSet(PLAN_OTHER_ATTEMPT_FULL_FILTER_MASK);
    }

    /**
     * The number of times the {@link RecordQueryPlanner} will attempt to replan during IN-to-JOIN planning.
     * During IN-to-JOIN planning, the plan will initially try to push as many IN-predicates as possible
     * into index scans. However, if any IN-predicates cannot be pushed into the index, this can result
     * in poor performance as the same index scans may be executed multiple times with different elements
     * selected. Setting this value to a number greater than 0 allows the planner to try again, limiting the
     * number of extracted predicates. If the configuration value is greater than or equal to 0, it will
     * attempt to avoid non-sargable IN-predicates by planning all INs as a residual filter. If the
     * configuration value is less than 0, it will allow un-sargable IN-predicates to planned as an IN-join.
     *
     * <p>
     * The recommended value for this parameter is 1. The default is 0 for backwards compatibility reasons.
     * This parameter is only relevant for the {@link RecordQueryPlanner}.
     * </p>
     *
     * @return the max number of replans to perform during IN-to-JOIN planning
     */
    public int getMaxNumReplansForInToJoin() {
        return proto.getMaxNumReplansForInToJoin();
    }

    /**
     * The number of times the {@link RecordQueryPlanner} will attempt to replan during IN-to-union
     * planning. This is analogous to the configuration parameter {@link #getMaxNumReplansForInToJoin()},
     * but this is relevant for IN-union planning instead of IN-to-JOIN planning. In general, this property
     * should only be relevant (instead of {@link #getMaxNumReplansForInToJoin()}) if the query results are
     * sorted.
     *
     * <p>
     * The recommended value for this parameter is 1. The default is -1 for backwards compatbility reasons.
     * This parameter is only relevant for the {@link RecordQueryPlanner}.
     * </p>
     *
     * @return the max number of replans to perform during IN-union planning
     * @see #shouldAttemptFailedInJoinAsUnion()
     */
    public int getMaxNumReplansForInUnion() {
        return proto.hasMaxNumReplansForInUnion() ? proto.getMaxNumReplansForInUnion() : -1;
    }

    /**
     * Returns the maximum number of conjuncts whose combinations are enumerated when {@link PredicateToLogicalUnionRule} is
     * applied.
     * @return the maximum number of conjuncts
     */
    public int getOrToUnionMaxNumConjuncts() {
        return proto.hasOrToUnionMaxNumConjuncts() ? proto.getOrToUnionMaxNumConjuncts() : PredicateToLogicalUnionRule.DEFAULT_MAX_NUM_CONJUNCTS;
    }

    public boolean shouldNormalizeNestedFields() {
        return flagSet(NORMALIZE_NESTED_FIELDS_MASK);
    }

    /**
     * Return a protobuf representation of this configuration object. This can then be serialized and
     * returned along with, say, a plan continuation. If the original query is re-planned, the serialized
     * planner configuration object can be used to ensure (as much as possible) that the same plan is
     * generated for the same query.
     *
     * @return a protobuf representation of this configuration object
     */
    @Nonnull
    public RecordPlannerConfigurationProto.PlannerConfiguration toProto() {
        return proto;
    }

    @Nonnull
    public Builder asBuilder() {
        return new Builder(this);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RecordQueryPlannerConfiguration that = (RecordQueryPlannerConfiguration)o;
        return attemptFailedInJoinAsOr == that.attemptFailedInJoinAsOr
               && attemptFailedInJoinAsUnionMaxSize == that.attemptFailedInJoinAsUnionMaxSize
               && complexityThreshold == that.complexityThreshold
               && checkForDuplicateConditions == that.checkForDuplicateConditions
               && deferFetchAfterUnionAndIntersection == that.deferFetchAfterUnionAndIntersection
               && optimizeForIndexFilters == that.optimizeForIndexFilters
               && maxTaskQueueSize == that.maxTaskQueueSize
               && maxTotalTaskCount == that.maxTotalTaskCount
               && useFullKeyForValueIndex == that.useFullKeyForValueIndex
               && maxNumMatchesPerRuleCall == that.maxNumMatchesPerRuleCall
               && deferCrossProducts == that.deferCrossProducts
               && planOtherAttemptWholeFilter == that.planOtherAttemptWholeFilter
               && maxNumReplansForInToJoin == that.maxNumReplansForInToJoin
               && orToUnionMaxNumConjuncts == that.orToUnionMaxNumConjuncts
               && indexScanPreference == that.indexScanPreference
               && Objects.equals(sortConfiguration, that.sortConfiguration)
               && Objects.equals(disabledTransformationRules, that.disabledTransformationRules)
               && indexFetchMethod == that.indexFetchMethod
               && Objects.equals(valueIndexesOverScanNeeded, that.valueIndexesOverScanNeeded);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexScanPreference,
                attemptFailedInJoinAsOr,
                attemptFailedInJoinAsUnionMaxSize,
                complexityThreshold,
                checkForDuplicateConditions,
                deferFetchAfterUnionAndIntersection,
                optimizeForIndexFilters,
                maxTaskQueueSize,
                maxTotalTaskCount,
                useFullKeyForValueIndex,
                maxNumMatchesPerRuleCall,
                sortConfiguration,
                disabledTransformationRules,
                deferCrossProducts,
                indexFetchMethod,
                valueIndexesOverScanNeeded,
                planOtherAttemptWholeFilter,
                maxNumReplansForInToJoin,
                orToUnionMaxNumConjuncts);
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
     * Deserialize a configuration object from its protobuf representation. This can be used (along with
     * {@link #toProto()}) to ensure that the same configuration is used for the same query, potentially
     * across different JVMs.
     *
     * @param proto a protobuf representation of a {@code RecordQueryPlannerConfiguration}
     * @return a {@code RecordQueryPlannerConfiguration} with the same configuration as the proto object
     */
    @Nonnull
    public static RecordQueryPlannerConfiguration fromProto(@Nonnull RecordPlannerConfigurationProto.PlannerConfiguration proto) {
        @Nullable RecordQueryPlannerSortConfiguration sortConfiguration = proto.hasSortConfiguration() ? RecordQueryPlannerSortConfiguration.fromProto(proto.getSortConfiguration()) : null;
        return new RecordQueryPlannerConfiguration(proto, sortConfiguration);
    }

    /**
     * A builder for {@link RecordQueryPlannerConfiguration}.
     */
    public static class Builder {
        @Nonnull
        private final RecordPlannerConfigurationProto.PlannerConfiguration.Builder protoBuilder;
        @Nullable
        private RecordQueryPlannerSortConfiguration sortConfiguration;
        private long flags;

        public Builder(@Nonnull RecordQueryPlannerConfiguration configuration) {
            this.protoBuilder = configuration.toProto().toBuilder();
            this.sortConfiguration = configuration.sortConfiguration;
            this.flags = protoBuilder.getFlags();
        }

        public Builder() {
            this.protoBuilder = RecordPlannerConfigurationProto.PlannerConfiguration.newBuilder();
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setIndexScanPreference(@Nonnull QueryPlanner.IndexScanPreference indexScanPreference) {
            protoBuilder.setIndexScanPreference(SCAN_PREFERENCE_BI_MAP.get(indexScanPreference));
            return this;
        }

        private void updateFlags(boolean value, long mask) {
            flags = value ? (flags | mask) : (flags & ~mask);
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setAttemptFailedInJoinAsOr(boolean attemptFailedInJoinAsOr) {
            updateFlags(attemptFailedInJoinAsOr, ATTEMPT_FAILED_IN_JOIN_AS_OR_MASK);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setAttemptFailedInJoinAsUnionMaxSize(int attemptFailedInJoinAsUnionMaxSize) {
            protoBuilder.setAttemptFailedInJoinAsUnionMaxSize(attemptFailedInJoinAsUnionMaxSize);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setComplexityThreshold(final int complexityThreshold) {
            protoBuilder.setComplexityThreshold(complexityThreshold);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setCheckForDuplicateConditions(final boolean checkForDuplicateConditions) {
            updateFlags(checkForDuplicateConditions, CHECK_FOR_DUPLICATE_CONDITIONS_MASK);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setDeferFetchAfterUnionAndIntersection(boolean deferFetchAfterUnionAndIntersection) {
            updateFlags(deferFetchAfterUnionAndIntersection, DEFER_FETCH_AFTER_UNION_AND_INTERSECTION_MASK);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setDeferFetchAfterInJoinAndInUnion(boolean deferFetchAfterInJoinAndInUnion) {
            updateFlags(deferFetchAfterInJoinAndInUnion, DEFER_FETCH_AFTER_IN_JOIN_AND_IN_UNION_MASK);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setOmitPrimaryKeyInUnionOrderingKey(boolean omitPrimaryKeyInUnionOrderingKey) {
            updateFlags(omitPrimaryKeyInUnionOrderingKey, OMIT_PRIMARY_KEY_IN_UNION_ORDERING_KEY_MASK);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setOmitPrimaryKeyInOrderingKeyForInUnion(boolean omitPrimaryKeyInOrderingKeyForInUnion) {
            updateFlags(omitPrimaryKeyInOrderingKeyForInUnion, OMIT_PRIMARY_KEY_IN_ORDERING_KEY_FOR_IN_UNION_MASK);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setOptimizeForIndexFilters(final boolean optimizeForIndexFilters) {
            updateFlags(optimizeForIndexFilters, OPTIMIZE_FOR_INDEX_FILTERS_MASK);
            return this;
        }

        /**
         * Set whether the query planner should attempt to consider the set of required result fields while finding the
         * best index for a record type access. If a query does not set the set of required results
         * ({@link RecordQuery#getRequiredResults()}), enabling this switch is meaningless.
         * @param optimizeForRequiredResults set the optimizeForRequiredResults parameter
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setOptimizeForRequiredResults(final boolean optimizeForRequiredResults) {
            updateFlags(optimizeForRequiredResults, OPTIMIZE_FOR_REQUIRED_RESULTS_MASK);
            return this;
        }

        /**
         * Set the size limit of the Cascades planner task queue.
         * If the planner tries to add a task to the queue beyond the maximum size, planning will fail.
         * Default value is 0, which means "unbound".
         * @param maxTaskQueueSize the maximum size of the queue.
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setMaxTaskQueueSize(final int maxTaskQueueSize) {
            protoBuilder.setMaxTaskQueueSize(maxTaskQueueSize);
            return this;
        }

        /**
         * Set a limit on the number of tasks that can be executed as part of the Cascades planner planning.
         * If the planner tries to execute a task after the maximum number was exceeded, planning will fail.
         * Default value is 0, which means "unbound".
         * @param maxTotalTaskCount the maximum number of tasks.
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setMaxTotalTaskCount(final int maxTotalTaskCount) {
            protoBuilder.setMaxTotalTaskCount(maxTotalTaskCount);
            return this;
        }

        /**
         * Set whether the planner uses the entire key, including the primary key, for value indexes.
         * @param useFullKeyForValueIndex whether to include primary key in planning
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setUseFullKeyForValueIndex(final boolean useFullKeyForValueIndex) {
            updateFlags(!useFullKeyForValueIndex, DONT_USE_FULL_KEY_FOR_VALUE_INDEX_MASK);
            return this;
        }

        /**
         * Set the maximum number of matches that are permitted per rule call within the Cascades planner.
         * Default value is 0, which means "unbound".
         * @param maxNumMatchesPerRuleCall the desired maximum number of matches that are permitted per rule call
         * @return {@code this}
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setMaxNumMatchesPerRuleCall(final int maxNumMatchesPerRuleCall) {
            protoBuilder.setMaxNumMatchesPerRuleCall(maxNumMatchesPerRuleCall);
            return this;
        }

        /**
         * Set configuration for planning sorting, including whether the planner is allowed to use an in-memory sort plan.
         * @param sortConfiguration configuration to use for planning non-index sorting, or {@code null} to never allow it
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setSortConfiguration(@Nullable final RecordQueryPlannerSortConfiguration sortConfiguration) {
            if (sortConfiguration == null) {
                protoBuilder.clearSortConfiguration();
            } else {
                protoBuilder.setSortConfiguration(sortConfiguration.toProto());
            }
            this.sortConfiguration = sortConfiguration;
            return this;
        }

        /**
         * Set whether the planner is allowed to use an in-memory sort plan.
         * @param allowNonIndexSort whether to allow non-index sorting
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setAllowNonIndexSort(final boolean allowNonIndexSort) {
            setSortConfiguration(allowNonIndexSort ? RecordQueryPlannerSortConfiguration.getDefaultInstance() : null);
            return this;
        }

        /**
         * Set a set of planner transformation rules that should be considered disabled for any planning effort.
         * @param disabledTransformationRules a set of disabled rules.
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setDisabledTransformationRules(@Nonnull final Set<Class<? extends CascadesRule<?>>> disabledTransformationRules) {
            protoBuilder.clearDisabledTransformationRules();
            for (Class<? extends CascadesRule<?>> rule : disabledTransformationRules) {
                protoBuilder.addDisabledTransformationRules(rule.getSimpleName());
            }
            return this;
        }

        /**
         * Set a set of rules names that identify planner transformation rules that should be considered disabled
         * for any planning effort.
         * @param disabledTransformationRuleNames a set of rule names identifying (via simple class name)
         *        transformation rules
         * @param planningRuleSet a {@link PlanningRuleSet} that is used to resolve the rule name to a rule class
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setDisabledTransformationRuleNames(@Nonnull final Set<String> disabledTransformationRuleNames, @Nonnull PlanningRuleSet planningRuleSet) {
            protoBuilder.clearDisabledTransformationRules()
                    .addAllDisabledTransformationRules(disabledTransformationRuleNames);
            return this;
        }

        /**
         * Helper method that disables all rewriting rules. These are the rules that
         * are applied to the logical query and are designed to produce a canonical
         * version of the original query. However, these rules can add to the planning
         * time if the process of rewriting the query ends up over-enumerating.
         * If a query encounters problems during this stage of planning, this can
         * be used as a stop-gap to disable all rewrites and proceed directly to
         * the planning stage.
         *
         * @return this builder
         * @see RewritingRuleSet
         */
        @API(API.Status.EXPERIMENTAL)
        @SuppressWarnings("unchecked")
        @CanIgnoreReturnValue
        @Nonnull
        public Builder disableRewritingRules() {
            for (CascadesRule<?> rule : RewritingRuleSet.OPTIONAL_RULES) {
                disableTransformationRule((Class<? extends CascadesRule<?>>) rule.getClass());
            }
            return this;
        }

        /**
         * Helper method to disable the planner transformation rule class passed in.
         * @param ruleClass a rule class that should be disabled
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder disableTransformationRule(@Nonnull Class<? extends CascadesRule<?>> ruleClass) {
            protoBuilder.addDisabledTransformationRules(ruleClass.getSimpleName());
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
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setDeferCrossProducts(final boolean deferCrossProducts) {
            updateFlags(!deferCrossProducts, DONT_DEFER_CROSS_PRODUCTS_MASK);
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
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setIndexFetchMethod(@Nonnull final IndexFetchMethod indexFetchMethod) {
            protoBuilder.setIndexFetchMethod(FETCH_METHOD_BI_MAP.get(indexFetchMethod));
            return this;
        }

        @API(API.Status.EXPERIMENTAL)
        @CanIgnoreReturnValue
        @Nonnull
        public Builder addValueIndexOverScanNeeded(@Nonnull final String indexName) {
            protoBuilder.addValueIndexesOverScanNeeded(indexName);
            return this;
        }

        /**
         * Set whether the planner attempts to plan a complex filter using non-VALUE indexes before splitting it up.
         * @param planOtherAttemptWholeFilter whether to attempt planning the whole filter
         * @return this builder
         */
        @API(API.Status.EXPERIMENTAL)
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setPlanOtherAttemptWholeFilter(final boolean planOtherAttemptWholeFilter) {
            updateFlags(planOtherAttemptWholeFilter, PLAN_OTHER_ATTEMPT_FULL_FILTER_MASK);
            return this;
        }

        /**
         * Set the maximum number of replanning-attempts during IN-to-JOIN transformations in the planner. This
         * option only applies to {@link RecordQueryPlanner}.
         * @param maxNumReplansForInToJoin the number of replanning attempts; defaults to {@code 0} for no
         *        replanning attempts
         * @return this builder
         * @see #getMaxNumReplansForInToJoin()
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setMaxNumReplansForInToJoin(final int maxNumReplansForInToJoin) {
            protoBuilder.setMaxNumReplansForInToJoin(maxNumReplansForInToJoin);
            return this;
        }

        /**
         * Set the maximum number of replanning-attempts during IN-union transformations in the planner. This
         * option only applies to {@link RecordQueryPlanner}.
         * @param maxNumReplansForInUnion the number of replanning attempts; defaults to {@code -1} for no
         *        replanning attempts
         * @return this builder
         * @see #getMaxNumReplansForInUnion()
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setMaxNumReplansForInUnion(final int maxNumReplansForInUnion) {
            protoBuilder.setMaxNumReplansForInUnion(maxNumReplansForInUnion);
            return this;
        }

        /**
         * Set the maximum number of conjuncts whose combinations are enumerated when {@link PredicateToLogicalUnionRule} is
         * applied. This option only applies to {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
         * @param orToUnionMaxNumConjuncts the maximum number of conjuncts
         * @return this builder
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder setOrToUnionMaxNumConjuncts(final int orToUnionMaxNumConjuncts) {
            protoBuilder.setOrToUnionMaxNumConjuncts(orToUnionMaxNumConjuncts);
            return this;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public Builder setNormalizeNestedFields(boolean normalizeNestedFields) {
            updateFlags(normalizeNestedFields, NORMALIZE_NESTED_FIELDS_MASK);
            return this;
        }

        public RecordQueryPlannerConfiguration build() {
            if (protoBuilder.getFlags() != flags) {
                protoBuilder.setFlags(flags);
            }
            return new RecordQueryPlannerConfiguration(protoBuilder.build(), sortConfiguration);
        }
    }
}
