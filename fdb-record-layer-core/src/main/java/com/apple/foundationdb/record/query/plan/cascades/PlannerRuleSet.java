/*
 * PlannerRuleSet.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.rules.AdjustMatchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.CombineFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.DataAccessRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.FullUnorderedExpressionToScanPlanRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementDistinctRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementDistinctUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementExplodeRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementInJoinRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementInUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementIndexScanRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementIntersectionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementSimpleSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementNestedLoopJoinRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementPhysicalScanRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementTypeFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUnorderedUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.InComparisonToExplodeRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MatchIntermediateRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MatchLeafRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MergeFetchIntoCoveringIndexRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.MergeProjectionAndFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.NormalizePredicatesRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.OrToLogicalUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushDistinctBelowFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushDistinctThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushFilterThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushInJoinThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughDistinctRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughInLikeSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughSortRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughUnionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushReferencedFieldsThroughDistinctRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushReferencedFieldsThroughFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushReferencedFieldsThroughSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushSetOperationThroughFetchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PushTypeFilterBelowFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.RemoveProjectionRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.RemoveRedundantTypeFilterRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.RemoveSortRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.SelectDataAccessRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.SplitSelectExtractIndependentQuantifiersRule;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class PlannerRuleSet {
    private static final List<PlannerRule<? extends RelationalExpression>> NORMALIZATION_RULES = ImmutableList.of(
            new NormalizePredicatesRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> REWRITE_RULES = ImmutableList.of(
            new CombineFilterRule(),
            new RemoveRedundantTypeFilterRule(),
            new OrToLogicalUnionRule(),
            new InComparisonToExplodeRule(),
            new SplitSelectExtractIndependentQuantifiersRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> MATCHING_RULES = ImmutableList.of(
            new MatchLeafRule(),
            new MatchIntermediateRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> PREORDER_RULES = ImmutableList.of(
            new PushReferencedFieldsThroughDistinctRule(),
            new PushReferencedFieldsThroughFilterRule(),
            new PushReferencedFieldsThroughSelectRule(),
            new PushRequestedOrderingThroughSortRule(),
            new PushRequestedOrderingThroughDistinctRule(),
            new PushRequestedOrderingThroughUnionRule(),
            new PushRequestedOrderingThroughInLikeSelectRule(),
            new PushRequestedOrderingThroughSelectRule()
    );

    private static final List<PlannerRule<? extends RelationalExpression>> IMPLEMENTATION_RULES = ImmutableList.of(
            new ImplementTypeFilterRule(),
            new ImplementFilterRule(),
            new PushTypeFilterBelowFilterRule(),
            new ImplementIndexScanRule(),
            new ImplementPhysicalScanRule(),
            new FullUnorderedExpressionToScanPlanRule(),
            new ImplementIntersectionRule(),
            new ImplementDistinctUnionRule(),
            new ImplementUnorderedUnionRule(),
            new ImplementDistinctRule(),
            new RemoveSortRule(),
            new PushDistinctBelowFilterRule(),
            new MergeFetchIntoCoveringIndexRule(),
            new PushInJoinThroughFetchRule<>(RecordQueryInValuesJoinPlan.class),
            new PushInJoinThroughFetchRule<>(RecordQueryInParameterJoinPlan.class),
            new PushFilterThroughFetchRule(),
            new PushDistinctThroughFetchRule(),
            new PushSetOperationThroughFetchRule<>(RecordQueryIntersectionOnKeyExpressionPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryUnionOnKeyExpressionPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryUnorderedUnionPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryInUnionOnKeyExpressionPlan.class),
            new RemoveProjectionRule(),
            new MergeProjectionAndFetchRule(),
            new ImplementInJoinRule(),
            new ImplementInUnionRule(),
            new ImplementSimpleSelectRule(),
            new ImplementExplodeRule(),
            new ImplementNestedLoopJoinRule()
    );

    private static final List<PlannerRule<? extends RelationalExpression>> EXPLORATION_RULES =
            ImmutableList.<PlannerRule<? extends RelationalExpression>>builder()
                    .addAll(NORMALIZATION_RULES)
                    .addAll(MATCHING_RULES)
                    .addAll(REWRITE_RULES)
                    .build();
    private static final List<PlannerRule<? extends PartialMatch>> PARTIAL_MATCH_RULES = ImmutableList.of(
            new AdjustMatchRule()
    );
    private static final List<PlannerRule<? extends MatchPartition>> MATCH_PARTITION_RULES = ImmutableList.of(
            new DataAccessRule(),
            new SelectDataAccessRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> ALL_EXPRESSION_RULES =
            ImmutableList.<PlannerRule<? extends RelationalExpression>>builder()
                    .addAll(PREORDER_RULES)
                    .addAll(EXPLORATION_RULES)
                    .addAll(IMPLEMENTATION_RULES)
                    .build();

    public static final PlannerRuleSet DEFAULT = new PlannerRuleSet(ALL_EXPRESSION_RULES);

    @Nonnull
    private final Multimap<Class<?>, PlannerRule<? extends RelationalExpression>> ruleIndex =
            MultimapBuilder.hashKeys().arrayListValues().build();
    @Nonnull
    private final List<PlannerRule<? extends RelationalExpression>> alwaysRules = new ArrayList<>();

    @VisibleForTesting
    PlannerRuleSet(@Nonnull List<PlannerRule<? extends RelationalExpression>> rules) {
        for (PlannerRule<? extends RelationalExpression> rule : rules) {
            Optional<Class<?>> root = rule.getRootOperator();
            if (root.isPresent()) {
                ruleIndex.put(root.get(), rule);
            } else {
                alwaysRules.add(rule);
            }
        }
    }

    @Nonnull
    public Stream<PlannerRule<? extends RelationalExpression>> getExpressionRules(@Nonnull RelationalExpression expression) {
        return getExpressionRules(expression, r -> true);
    }

    @Nonnull
    public Stream<PlannerRule<? extends RelationalExpression>> getExpressionRules(@Nonnull RelationalExpression expression,
                                                                                  @Nonnull final Predicate<PlannerRule<? extends RelationalExpression>> rulePredicate) {
        return Streams.concat(ruleIndex.get(expression.getClass()).stream(), alwaysRules.stream()).filter(rulePredicate);
    }

    @Nonnull
    public Stream<PlannerRule<? extends PartialMatch>> getPartialMatchRules() {
        return getPartialMatchRules(t -> true);
    }

    @Nonnull
    public Stream<PlannerRule<? extends PartialMatch>> getPartialMatchRules(@Nonnull final Predicate<PlannerRule<? extends PartialMatch>> rulePredicate) {
        return PARTIAL_MATCH_RULES.stream()
                .filter(rulePredicate);
    }

    @Nonnull
    public Stream<PlannerRule<? extends MatchPartition>> getMatchPartitionRules() {
        return getMatchPartitionRules(t -> true);
    }

    @Nonnull
    public Stream<PlannerRule<? extends MatchPartition>> getMatchPartitionRules(@Nonnull final Predicate<PlannerRule<? extends MatchPartition>> rulePredicate) {
        return MATCH_PARTITION_RULES.stream()
                .filter(rulePredicate);
    }

    @Nonnull
    public Stream<? extends PlannerRule<?>> getAllRules() {
        return Streams.concat(ALL_EXPRESSION_RULES.stream(), PARTIAL_MATCH_RULES.stream(), MATCH_PARTITION_RULES.stream());
    }
}
