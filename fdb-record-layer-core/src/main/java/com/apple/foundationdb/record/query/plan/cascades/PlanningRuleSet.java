/*
 * PlanningRuleSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.rules.AdjustMatchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.AggregateDataAccessRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementRecursiveDfsJoinRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushDistinctBelowMapRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.WithPrimaryKeyDataAccessRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementDeleteRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementDistinctRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementDistinctUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementExplodeRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementInJoinRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementInUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementInsertRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementIntersectionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementNestedLoopJoinRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementRecursiveLevelUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementSimpleSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementStreamingAggregationRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementTableFunctionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementTempTableInsertRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementTempTableScanRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementTypeFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUniqueRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUnorderedUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUpdateRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.InComparisonToExplodeRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MatchIntermediateRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MatchLeafRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MergeFetchIntoCoveringIndexRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MergeProjectionAndFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.NormalizePredicatesRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PartitionBinarySelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PartitionSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PredicateToLogicalUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PullUpNullOnEmptyRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushDistinctBelowFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushDistinctThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushFilterThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushInJoinThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushMapThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushReferencedFieldsThroughDistinctRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushReferencedFieldsThroughFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushReferencedFieldsThroughSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushReferencedFieldsThroughUniqueRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughDeleteRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughDistinctRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughGroupByRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughInLikeSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughInsertRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughInsertTempTableRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughRecursiveUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughSelectExistentialRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughSortRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughUniqueRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughUpdateRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushSetOperationThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushTypeFilterBelowFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.RemoveProjectionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.RemoveSortRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.SplitSelectExtractIndependentQuantifiersRule;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class PlanningRuleSet extends CascadesRuleSet {
    private static final Set<ExplorationCascadesRule<? extends RelationalExpression>> EXPLORATION_RULES = ImmutableSet.of(
            new NormalizePredicatesRule(),
            new InComparisonToExplodeRule(),
            new SplitSelectExtractIndependentQuantifiersRule(),
            new PullUpNullOnEmptyRule(),
            new PartitionSelectRule(),
            new PartitionBinarySelectRule()
            );
    private static final Set<CascadesRule<? extends RelationalExpression>> MATCHING_RULES = ImmutableSet.of(
            new MatchLeafRule(),
            new MatchIntermediateRule()
    );
    private static final Set<CascadesRule<? extends RelationalExpression>> PREORDER_RULES = ImmutableSet.of(
            new PushReferencedFieldsThroughDistinctRule(),
            new PushReferencedFieldsThroughFilterRule(),
            new PushReferencedFieldsThroughSelectRule(),
            new PushReferencedFieldsThroughUniqueRule(),
            new PushRequestedOrderingThroughSortRule(),
            new PushRequestedOrderingThroughDistinctRule(),
            new PushRequestedOrderingThroughUnionRule(),
            new PushRequestedOrderingThroughRecursiveUnionRule(),
            new PushRequestedOrderingThroughInLikeSelectRule(),
            new PushRequestedOrderingThroughSelectRule(),
            new PushRequestedOrderingThroughSelectExistentialRule(),
            new PushRequestedOrderingThroughGroupByRule(),
            new PushRequestedOrderingThroughDeleteRule(),
            new PushRequestedOrderingThroughInsertRule(),
            new PushRequestedOrderingThroughInsertTempTableRule(),
            new PushRequestedOrderingThroughUpdateRule(),
            new PushRequestedOrderingThroughUniqueRule()
    );

    private static final Set<ImplementationCascadesRule<? extends RelationalExpression>> IMPLEMENTATION_RULES = ImmutableSet.of(
            new ImplementTempTableScanRule(),
            new ImplementTypeFilterRule(),
            new ImplementFilterRule(),
            new PushTypeFilterBelowFilterRule(),
            new ImplementIntersectionRule(),
            new ImplementDistinctUnionRule(),
            new ImplementUnorderedUnionRule(),
            new ImplementDistinctRule(),
            new ImplementUniqueRule(),
            new RemoveSortRule(),
            new PushDistinctBelowFilterRule(),
            new PushDistinctBelowMapRule(),
            new MergeFetchIntoCoveringIndexRule(),
            new PushInJoinThroughFetchRule<>(RecordQueryInValuesJoinPlan.class),
            new PushInJoinThroughFetchRule<>(RecordQueryInParameterJoinPlan.class),
            new PushMapThroughFetchRule(),
            new PushFilterThroughFetchRule(),
            new PushDistinctThroughFetchRule(),
            new PushSetOperationThroughFetchRule<>(RecordQueryIntersectionOnValuesPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryUnionOnValuesPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryUnorderedUnionPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryInUnionOnValuesPlan.class),
            new RemoveProjectionRule(),
            new MergeProjectionAndFetchRule(),
            new ImplementInJoinRule(),
            new ImplementInUnionRule(),
            new ImplementSimpleSelectRule(),
            new ImplementExplodeRule(),
            new ImplementNestedLoopJoinRule(),
            new ImplementStreamingAggregationRule(),
            new ImplementDeleteRule(),
            new ImplementInsertRule(),
            new ImplementTempTableInsertRule(),
            new ImplementUpdateRule(),
            new ImplementRecursiveLevelUnionRule(),
            new ImplementRecursiveDfsJoinRule(),
            new ImplementTableFunctionRule()
    );

    private static final Set<CascadesRule<? extends RelationalExpression>> ALL_EXPRESSION_RULES =
            ImmutableSet.<CascadesRule<? extends RelationalExpression>>builder()
                    .addAll(MATCHING_RULES)
                    .addAll(EXPLORATION_RULES)
                    .build();
    private static final Set<CascadesRule<? extends PartialMatch>> PARTIAL_MATCH_RULES = ImmutableSet.of(
            new AdjustMatchRule()
    );
    private static final Set<CascadesRule<? extends MatchPartition>> MATCH_PARTITION_RULES = ImmutableSet.of(
            new WithPrimaryKeyDataAccessRule(),
            new AggregateDataAccessRule(),
            new PredicateToLogicalUnionRule()
    );
    private static final Set<CascadesRule<? extends RelationalExpression>> ALL_RULES =
            ImmutableSet.<CascadesRule<? extends RelationalExpression>>builder()
                    .addAll(PREORDER_RULES)
                    .addAll(ALL_EXPRESSION_RULES)
                    .addAll(IMPLEMENTATION_RULES)
                    .build();

    public static final PlanningRuleSet DEFAULT = new PlanningRuleSet();

    PlanningRuleSet() {
        this(ALL_RULES);
    }

    @VisibleForTesting
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    PlanningRuleSet(@Nonnull Set<CascadesRule<? extends RelationalExpression>> rules) {
        super(rules);
    }

    @Nonnull
    @Override
    public Stream<CascadesRule<? extends PartialMatch>> getPartialMatchRules(@Nonnull final Predicate<CascadesRule<? extends PartialMatch>> rulePredicate) {
        return PARTIAL_MATCH_RULES.stream()
                .filter(rulePredicate);
    }

    @Nonnull
    @Override
    public Stream<CascadesRule<? extends MatchPartition>> getMatchPartitionRules(@Nonnull final Predicate<CascadesRule<? extends MatchPartition>> rulePredicate) {
        return MATCH_PARTITION_RULES.stream()
                .filter(rulePredicate);
    }

    public static PlanningRuleSet getDefault() {
        return DEFAULT;
    }
}
