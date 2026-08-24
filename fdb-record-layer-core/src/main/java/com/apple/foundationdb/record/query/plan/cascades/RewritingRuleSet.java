/*
 * RewritingRuleSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.ConditionalCascadesRule.ConditionalExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.rules.DecorrelateValuesRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.FinalizeExpressionsRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.PredicatePushDownRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.QueryPredicateSimplificationRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.RewriteOuterJoinRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.SelectMergeRule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class RewritingRuleSet extends CascadesRuleSet {
    // Note: The order of the rules does not affect the search space. Decorrelation comes first because it can be an
    // “enabler” for the other rule, and putting simplification first would increase overhead by adding unnecessary
    // simplification attempts.
    private static final ConditionalExplorationCascadesRule<SelectExpression> decorrelateThenSimplification =
            new ConditionalExplorationCascadesRule<>(
                    new DecorrelateValuesRule(),
                    new QueryPredicateSimplificationRule());

    private static final Set<ExplorationCascadesRule<? extends RelationalExpression>> EXPLORATION_RULES =
            ImmutableSet.of(
                    decorrelateThenSimplification,
                    new RewriteOuterJoinRule());

    private static final Set<AbstractCascadesRule<? extends RelationalExpression>> PREORDER_RULES =
            ImmutableSet.of();

    private static final ConditionalCascadesRule.ConditionalImplementationCascadesRule<SelectExpression>
            selectMergeThenPushDown =
            new ConditionalCascadesRule.ConditionalImplementationCascadesRule<>(
                    new SelectMergeRule(),
                    new PredicatePushDownRule());

    private static final Set<ImplementationCascadesRule<? extends RelationalExpression>> IMPLEMENTATION_RULES =
            ImmutableSet.of(
                    selectMergeThenPushDown,
                    new FinalizeExpressionsRule());

    @Nonnull
    private static final Set<CascadesRule<? extends RelationalExpression>> ALL_EXPRESSION_RULES =
            ImmutableSet.<CascadesRule<? extends RelationalExpression>>builder()
                    .addAll(PREORDER_RULES)
                    .addAll(EXPLORATION_RULES)
                    .addAll(IMPLEMENTATION_RULES)
                    .build();

    @Nonnull
    public static final Set<CascadesRule<? extends RelationalExpression>> OPTIONAL_RULES =
            Streams.<CascadesRule<? extends RelationalExpression>>concat(
                            PREORDER_RULES.stream(),
                            EXPLORATION_RULES.stream(),
                            IMPLEMENTATION_RULES.stream())
                    .flatMap(RewritingRuleSet::expandConditionalRules)
                    .filter(rule -> !(rule instanceof FinalizeExpressionsRule))
                    .collect(ImmutableSet.toImmutableSet());

    @Nonnull
    public static final RewritingRuleSet DEFAULT = new RewritingRuleSet();

    /**
     * Expands a rule into the rules that can be individually enabled or disabled through the planner configuration.
     * For a {@link ConditionalCascadesRule}, those are its inner rules, as the wrapping rule is never applied on its
     * own; for any other rule, it is the rule itself.
     */
    @Nonnull
    private static Stream<? extends CascadesRule<? extends RelationalExpression>> expandConditionalRules(
            @Nonnull final CascadesRule<? extends RelationalExpression> rule) {
        if (rule instanceof ConditionalCascadesRule<?, ?> conditionalRule) {
            return conditionalRule.getRules().stream();
        }
        return Stream.of(rule);
    }

    @VisibleForTesting
    RewritingRuleSet() {
        super(ALL_EXPRESSION_RULES);
    }

    @Nonnull
    public static RewritingRuleSet getDefault() {
        return DEFAULT;
    }
}
