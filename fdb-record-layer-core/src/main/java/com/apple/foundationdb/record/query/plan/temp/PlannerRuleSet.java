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
import com.apple.foundationdb.record.query.plan.temp.rules.CombineFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FilterWithConjunctNestedToNestingContextRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FilterWithNestedToNestingContextRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FindPossibleIndexForAndComponentRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FlattenNestedAndComponentRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementDistinctRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FilterWithFieldWithComparisonRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementUnorderedUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.OrToUnorderedUnionRule;
import com.apple.foundationdb.record.query.plan.temp.rules.FullUnorderedExpressionToScanPlanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.LogicalToPhysicalScanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushConjunctFieldWithComparisonIntoExistingIndexScanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushFieldWithComparisonIntoExistingIndexScanRule;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementTypeFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.PushTypeFilterBelowFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.RemoveNestedContextRule;
import com.apple.foundationdb.record.query.plan.temp.rules.RemoveRedundantTypeFilterRule;
import com.apple.foundationdb.record.query.plan.temp.rules.SortToIndexRule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class PlannerRuleSet {
    private static final List<PlannerRule<? extends PlannerExpression>> NORMALIZATION_RULES = ImmutableList.of(
            new FlattenNestedAndComponentRule()
    );
    private static final List<PlannerRule<? extends PlannerExpression>> REWRITE_RULES = ImmutableList.of(
            new CombineFilterRule(),
            new SortToIndexRule(),
            new FilterWithFieldWithComparisonRule(),
            new PushFieldWithComparisonIntoExistingIndexScanRule(),
            new PushConjunctFieldWithComparisonIntoExistingIndexScanRule(),
            new RemoveRedundantTypeFilterRule(),
            new FindPossibleIndexForAndComponentRule(),
            new OrToUnorderedUnionRule(),
            new FilterWithNestedToNestingContextRule(),
            new FilterWithConjunctNestedToNestingContextRule(),
            new RemoveNestedContextRule()
    );
    private static final List<PlannerRule<? extends PlannerExpression>> IMPLEMENTATION_RULES = ImmutableList.of(
            new ImplementTypeFilterRule(),
            new ImplementFilterRule(),
            new PushTypeFilterBelowFilterRule(),
            new LogicalToPhysicalScanRule(),
            new FullUnorderedExpressionToScanPlanRule(),
            new ImplementUnorderedUnionRule(),
            new ImplementDistinctRule()
    );
    private static final List<PlannerRule<? extends PlannerExpression>> EXPLORATION_RULES =
            ImmutableList.<PlannerRule<? extends PlannerExpression>>builder()
                    .addAll(NORMALIZATION_RULES)
                    .addAll(REWRITE_RULES)
                    .build();
    private static final List<PlannerRule<? extends PlannerExpression>> ALL_RULES =
            ImmutableList.<PlannerRule<? extends PlannerExpression>>builder()
                    .addAll(EXPLORATION_RULES)
                    .addAll(IMPLEMENTATION_RULES)
                    .build();

    public static final PlannerRuleSet ALL = new PlannerRuleSet(ALL_RULES);

    @Nonnull
    private final Multimap<Class<? extends PlannerExpression>, PlannerRule<? extends PlannerExpression>> ruleIndex =
            MultimapBuilder.hashKeys().arrayListValues().build();
    @Nonnull
    private final List<PlannerRule<? extends PlannerExpression>> alwaysRules = new ArrayList<>();

    @VisibleForTesting
    PlannerRuleSet(@Nonnull List<PlannerRule<? extends PlannerExpression>> rules) {
        for (PlannerRule<? extends PlannerExpression> rule : rules) {
            Optional<Class<? extends PlannerExpression>> root = rule.getRootOperator();
            if (root.isPresent()) {
                ruleIndex.put(root.get(), rule);
            } else {
                alwaysRules.add(rule);
            }
        }
    }

    @Nonnull
    public Iterator<PlannerRule<? extends PlannerExpression>> getRulesMatching(@Nonnull PlannerExpression expression) {
        return Iterators.concat(ruleIndex.get(expression.getClass()).iterator(), alwaysRules.iterator());
    }
}
