/*
 * RewriteOuterJoinRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.OuterJoinExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.outerJoinExpression;

/**
 * An exploration rule that eliminates an {@link OuterJoinExpression} by rewriting it into two nested
 * {@link SelectExpression} boxes. This normalization happens early during planning so that all subsequent
 * rewrite and implementation rules do not need to explicitly deal with {@code OuterJoinExpression}.
 *
 * <p>The rewrite produces the following structure:
 * <pre>{@code
 *   Outer SelectExpression                          ← no predicates, combines both sides
 *   ├── ForEach quantifier → <preserved side>       ← normal quantifier
 *   └── ForEach(nullOnEmpty) quantifier →           ← emits NULL row when inner is empty
 *       Inner SelectExpression                      ← carries the ON predicates
 *       └── ForEach quantifier → <null-supplying side>
 * }</pre>
 *
 * <p>The {@code nullOnEmpty} flag on the quantifier connecting the inner {@code SelectExpression} to the outer one
 * provides the outer-join semantics: When the inner SELECT produces no rows (no match for the current preserved-side
 * row), the quantifier emits a single row with all columns set to {@code NULL}.
 *
 * <p>Only {@code LEFT} and {@code RIGHT} outer joins are supported. {@code FULL} outer joins are skipped.
 */
@API(API.Status.EXPERIMENTAL)
public class RewriteOuterJoinRule extends ExplorationCascadesRule<OuterJoinExpression> {

    @Nonnull
    private static final BindingMatcher<OuterJoinExpression> root = outerJoinExpression();

    public RewriteOuterJoinRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final OuterJoinExpression outerJoinExpression = bindings.get(root);

        if (outerJoinExpression.getJoinType() == OuterJoinExpression.JoinType.FULL) {
            return;
        }

        final Quantifier.ForEach preservedQun = outerJoinExpression.getPreservedQuantifier();
        final Quantifier.ForEach nullSupplyingQun = outerJoinExpression.getNullSupplyingQuantifier();

        // Build the inner `SelectExpression`. It carries the ON predicates and ranges over the null-supplying input.
        // The inner quantifier reuses the already-memoized reference from the original null-supplying quantifier and
        // preserves its alias so that correlations in the ON predicates continue to resolve correctly.
        final Quantifier.ForEach innerQun =
                Quantifier.forEach(nullSupplyingQun.getRangesOver(), nullSupplyingQun.getAlias());
        final SelectExpression innerSelect = new SelectExpression(
                RecordConstructorValue.ofUnnamed(ImmutableList.copyOf(innerQun.getFlowedValues())),
                ImmutableList.of(innerQun),
                ImmutableList.copyOf(outerJoinExpression.getJoinPredicates()));

        // Memoize the inner `SelectExpression` so it becomes part of the planner’s memo structure, then build a
        // null-on-empty quantifier over it. This quantifier emits a single NULL row when the inner SELECT is empty.
        final Reference innerRef = call.memoizeExploratoryExpression(innerSelect);
        final Quantifier.ForEach outerNullOnEmptyQun =
                Quantifier.forEachWithNullOnEmpty(innerRef, nullSupplyingQun.getAlias());

        // Build the outer `SelectExpression`. It combines the preserved side and the null-on-empty inner side.
        // The preserved side reuses its already-memoized reference.
        final Quantifier.ForEach outerPreservedQun =
                Quantifier.forEach(preservedQun.getRangesOver(), preservedQun.getAlias());

        // The outer result value must combine the flowed values from both quantifiers in the same order as the
        // original `OuterJoinExpression` (left then right).
        final ImmutableList.Builder<Value> resultValues = ImmutableList.builder();
        final Quantifier.ForEach outerLeftQun;
        final Quantifier.ForEach outerRightQun;
        if (outerJoinExpression.getJoinType() == OuterJoinExpression.JoinType.LEFT) {
            outerLeftQun = outerPreservedQun;
            outerRightQun = outerNullOnEmptyQun;
        } else {
            outerLeftQun = outerNullOnEmptyQun;
            outerRightQun = outerPreservedQun;
        }
        resultValues.addAll(outerLeftQun.getFlowedValues());
        resultValues.addAll(outerRightQun.getFlowedValues());

        final SelectExpression outerSelect = new SelectExpression(
                RecordConstructorValue.ofUnnamed(resultValues.build()),
                ImmutableList.of(outerLeftQun, outerRightQun),
                ImmutableList.of());

        call.yieldExploratoryExpression(outerSelect);
    }
}
