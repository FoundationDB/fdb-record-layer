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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.rules.AdjustMatchRule;
import com.apple.foundationdb.record.query.plan.temp.rules.CombineFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.DataAccessRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FullUnorderedExpressionToScanPlanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementDistinctRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementDistinctUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementInJoinRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementInUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementIndexScanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementIntersectionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementMapRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementPhysicalScanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementTypeFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementUnorderedUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.InComparisonToExplodeRule;
import com.apple.foundationdb.record.query.plan.temp.rules.MatchIntermediateRule;
import com.apple.foundationdb.record.query.plan.temp.rules.MatchLeafRule;
import com.apple.foundationdb.record.query.plan.temp.rules.MergeFetchIntoCoveringIndexRule;
import com.apple.foundationdb.record.query.plan.temp.rules.MergeProjectionAndFetchRule;
import com.apple.foundationdb.record.query.plan.temp.rules.NormalizePredicatesRule;
import com.apple.foundationdb.record.query.plan.temp.rules.OrToLogicalUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushDistinctBelowFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushDistinctThroughFetchRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushFilterThroughFetchRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushInJoinThroughFetchRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushInterestingOrderingThroughDistinctRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushInterestingOrderingThroughInLikeSelectRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushInterestingOrderingThroughSortRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushInterestingOrderingThroughUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushReferencedFieldsThroughDistinctRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushReferencedFieldsThroughFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushReferencedFieldsThroughSelectRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushSetOperationThroughFetchRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushTypeFilterBelowFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.RemoveProjectionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.RemoveRedundantTypeFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.RemoveSortRule;
import com.apple.foundationdb.record.query.plan.temp.rules.SelectDataAccessRule;
import com.apple.foundationdb.record.query.plan.temp.rules.SplitSelectExtractIndependentQuantifiersRule;
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
            new PushSetOperationThroughFetchRule<>(RecordQueryIntersectionPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryUnionPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryUnorderedUnionPlan.class),
            new PushSetOperationThroughFetchRule<>(RecordQueryInUnionPlan.class),
            new RemoveProjectionRule(),
            new MergeProjectionAndFetchRule(),
            new PushReferencedFieldsThroughDistinctRule(),
            new PushReferencedFieldsThroughFilterRule(),
            new PushReferencedFieldsThroughSelectRule(),
            new PushInterestingOrderingThroughSortRule(),
            new PushInterestingOrderingThroughDistinctRule(),
            new PushInterestingOrderingThroughUnionRule(),
            new PushInterestingOrderingThroughInLikeSelectRule(),
            new ImplementInJoinRule(),
            new ImplementInUnionRule(),
            new ImplementMapRule()
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
