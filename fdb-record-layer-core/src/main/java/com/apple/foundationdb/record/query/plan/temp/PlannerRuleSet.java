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
import com.apple.foundationdb.record.query.plan.temp.rules.AdjustMatchRule;
import com.apple.foundationdb.record.query.plan.temp.rules.CombineFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.DataAccessRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FlattenNestedAndPredicateRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FullUnorderedExpressionToScanPlanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementDistinctRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementDistinctUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementIntersectionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementSortRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementTypeFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementUnorderedUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.LogicalToPhysicalIndexScanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.LogicalToPhysicalScanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.MatchIntermediateRule;
import com.apple.foundationdb.record.query.plan.temp.rules.MatchLeafRule;
import com.apple.foundationdb.record.query.plan.temp.rules.OrToUnorderedUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushDistinctFilterBelowFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushTypeFilterBelowFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.RemoveRedundantTypeFilterRule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class PlannerRuleSet {
    private static final List<PlannerRule<? extends RelationalExpression>> NORMALIZATION_RULES = ImmutableList.of(
            new FlattenNestedAndPredicateRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> REWRITE_RULES = ImmutableList.of(
            new CombineFilterRule(),
            new RemoveRedundantTypeFilterRule(),
            new OrToUnorderedUnionRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> MATCHING_RULES = ImmutableList.of(
            new MatchLeafRule(),
            new MatchIntermediateRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> IMPLEMENTATION_RULES = ImmutableList.of(
            new ImplementTypeFilterRule(),
            new ImplementFilterRule(),
            new PushTypeFilterBelowFilterRule(),
            new LogicalToPhysicalIndexScanRule(),
            new LogicalToPhysicalScanRule(),
            new FullUnorderedExpressionToScanPlanRule(),
            new ImplementIntersectionRule(),
            new ImplementDistinctUnionRule(),
            new ImplementUnorderedUnionRule(),
            new ImplementDistinctRule(),
            new ImplementSortRule(),
            new PushDistinctFilterBelowFilterRule()
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
            new DataAccessRule()
    );
    private static final List<PlannerRule<? extends RelationalExpression>> ALL_RULES =
            ImmutableList.<PlannerRule<? extends RelationalExpression>>builder()
                    .addAll(EXPLORATION_RULES)
                    .addAll(IMPLEMENTATION_RULES)
                    .build();

    public static final PlannerRuleSet ALL = new PlannerRuleSet(ALL_RULES);

    @Nonnull
    private final Multimap<Class<? extends Bindable>, PlannerRule<? extends RelationalExpression>> ruleIndex =
            MultimapBuilder.hashKeys().arrayListValues().build();
    @Nonnull
    private final List<PlannerRule<? extends RelationalExpression>> alwaysRules = new ArrayList<>();

    @VisibleForTesting
    PlannerRuleSet(@Nonnull List<PlannerRule<? extends RelationalExpression>> rules) {
        for (PlannerRule<? extends RelationalExpression> rule : rules) {
            Optional<Class<? extends Bindable>> root = rule.getRootOperator();
            if (root.isPresent()) {
                ruleIndex.put(root.get(), rule);
            } else {
                alwaysRules.add(rule);
            }
        }
    }

    @Nonnull
    public Stream<PlannerRule<? extends RelationalExpression>> getExpressionRules(@Nonnull RelationalExpression expression) {
        return Streams.concat(ruleIndex.get(expression.getClass()).stream(), alwaysRules.stream());
    }

    @Nonnull
    public Stream<PlannerRule<? extends PartialMatch>> getPartialMatchRules() {
        return PARTIAL_MATCH_RULES.stream();
    }

    @Nonnull
    public Stream<PlannerRule<? extends MatchPartition>> getMatchPartitionRules() {
        return MATCH_PARTITION_RULES.stream();
    }

}
