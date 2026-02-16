/*
 * PartitionBinarySelectRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.SetMatcher.exactlyInAnyOrder;

/**
 * A rule that splits a {@link SelectExpression} into a new upper and two lower {@link SelectExpression}
 * that can absorb predicates which may prove advantageous for the implementation rules. This rule represents
 * the special case of {@link PartitionSelectRule} for two {@link Quantifier}s. Unlike that rule, this one does not
 * reduce the number of quantifiers participating in the join, but rearranges all predicates in a way that they would
 * be evaluated in their leftmost possible place. As the rule enumerates {@code (q1, q2)} as well as {@code (q2, q1)}
 * even if {@code q2} and {@code q1} may be in their respective correlation dependencies predicates will
 * be moved towards {@code q1} and {@code q2} (as tolerated by the existing correlations). The newly created binary
 * {@link SelectExpression} does not have any predicates anymore.
 *
 * <p>
 * There are three cases that are worth considering here:
 * </p>
 *
 * <ul>
 *     <li>
 *         <em>One leg is correlated to the other.</em> In this case, the correlated-to leg must be planned
 *         as the outer of a nested loop join, and the correlated-from leg should be planned the inner.
 *     </li>
 *     <li>
 *         <em>There is not a correlation between the legs, but there are join predicates.</em> In this case,
 *         the partitioning process must push the join predicates to one side or the other. This effectively creates
 *         a correlation between the two rewritten legs, and thus forces whichever side absorbs the predicates to
 *         be the inner of a nested loop join.
 *     </li>
 *     <li>
 *         <em>The two legs are completely independent.</em> In this case, the partitioning does not actually
 *         force a join order (or, more generally, a join algorithm). Predicates are sent to the quantifier that
 *         they are correlated to.
 *     </li>
 * </ul>
 *
 * <p>
 * When this rule executes, it matches one quantifier as the {@code left} and one as the {@code right}. The
 * rule will push predicates as if the left is going to be planned as the outer (so all join predicates go
 * to the right). In the first case, this is only valid when the correlated-to side is the left; in the other
 * two cases, the rule will be executed twice, once for each assignment of quantifiers to the {@code left} and
 * {@code right}. In the third case, we will generally end up producing the same expression (unless there are
 * predicates uncorrelated to both sides).
 * </p>
 *
 * <p>
 * Current thinking on future extensions: At first glance, leaving the resulting {@link SelectExpression} without any
 * predicates, but potentially correlating one side to the other seems to only be beneficial for nested-loop joins.
 * However, the pre-transformation {@link SelectExpression} does contain all those predicates, which therefore can
 * still be utilized to e.g. implement a hash join.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PartitionBinarySelectRule extends ExplorationCascadesRule<SelectExpression> {
    private static final BindingMatcher<Quantifier> leftQuantifierMatcher = anyQuantifier();

    private static final BindingMatcher<Quantifier> rightQuantifierMatcher = anyQuantifier();

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(exactlyInAnyOrder(leftQuantifierMatcher, rightQuantifierMatcher));

    public PartitionBinarySelectRule() {
        super(root);
    }

    @SuppressWarnings("java:S135")
    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final var bindings = call.getBindings();

        final var selectExpression = bindings.get(root);

        //
        // We are going to try to pull as many predicates as we can towards the left side.
        //
        final var leftQuantifier = bindings.get(leftQuantifierMatcher);
        final var leftAlias = leftQuantifier.getAlias();
        final var rightQuantifier = bindings.get(rightQuantifierMatcher);
        final var rightAlias = rightQuantifier.getAlias();

        final var fullCorrelationOrder =
                selectExpression.getCorrelationOrder().getTransitiveClosure();

        final var leftDependencies = fullCorrelationOrder.get(leftAlias);
        final var leftDependsOnRight = leftDependencies.contains(rightAlias);
        if (leftDependsOnRight) {
            // If the left depends on the right, we cannot push any join
            // predicates to the right hand side. Terminate here, any rely
            // on the fact this rule will be executed with the opposite
            // assignment of quantifiers to left and right to ensure a proper
            // partitioning
            return;
        }

        final var leftPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        final var rightPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        for (final var predicate : selectExpression.getPredicates()) {
            final var correlatedTo = predicate.getCorrelatedTo();
            final var correlatedToRight = correlatedTo.contains(rightAlias);
            if (correlatedToRight) {
                // Predicate depends on the right. It must go to the right
                // Note: this could be problematic if the right depended on the left,
                // but that should be avoided by the earlier check
                rightPredicatesBuilder.add(predicate);
            } else {
                // Predicate either depends on the left or it is independent. Either way,
                // it should be sent to the left
                leftPredicatesBuilder.add(predicate);
            }
        }

        final var leftPredicates = leftPredicatesBuilder.build();
        final var rightPredicates = rightPredicatesBuilder.build();

        //
        // We only want to proceed with the partitioning if the partitioning itself is helpful:
        //
        if (leftPredicates.isEmpty() && rightPredicates.isEmpty()) {
            // not useful
            return;
        }

        final Quantifier newLeftQuantifier;
        if (leftPredicates.isEmpty()) {
            newLeftQuantifier = leftQuantifier;
        } else {
            final var leftExpansionBuilder = GraphExpansion.builder();
            leftExpansionBuilder.addQuantifier(leftQuantifier);
            leftExpansionBuilder.addAllPredicates(leftPredicates);

            final SelectExpression leftSelectExpression;
            if (leftQuantifier instanceof Quantifier.ForEach) {
                leftSelectExpression = leftExpansionBuilder.build().buildSimpleSelectOverQuantifier((Quantifier.ForEach)leftQuantifier);
            } else {
                leftExpansionBuilder.addResultValue(LiteralValue.ofScalar(1));
                leftSelectExpression = leftExpansionBuilder.build().buildSelect();
            }
            newLeftQuantifier = Quantifier.forEachBuilder()
                    .withAlias(leftQuantifier.getAlias())
                    .build(call.memoizeExploratoryExpression(leftSelectExpression));
        }

        final Quantifier newRightQuantifier;
        if (rightPredicates.isEmpty()) {
            newRightQuantifier = rightQuantifier;
        } else {
            final var rightExpansionBuilder = GraphExpansion.builder();
            rightExpansionBuilder.addQuantifier(rightQuantifier);
            rightExpansionBuilder.addAllPredicates(rightPredicates);

            final SelectExpression rightSelectExpression;
            if (rightQuantifier instanceof Quantifier.ForEach) {
                rightSelectExpression = rightExpansionBuilder.build().buildSimpleSelectOverQuantifier((Quantifier.ForEach)rightQuantifier);
            } else {
                rightExpansionBuilder.addResultValue(LiteralValue.ofScalar(1));
                rightSelectExpression = rightExpansionBuilder.build().buildSelect();
            }
            newRightQuantifier = Quantifier.forEachBuilder()
                    .withAlias(rightQuantifier.getAlias())
                    .build(call.memoizeExploratoryExpression(rightSelectExpression));
        }

        final var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(newLeftQuantifier);
        graphExpansionBuilder.addQuantifier(newRightQuantifier);

        final var newSelectExpression = graphExpansionBuilder.build().buildSelectWithResultValue(selectExpression.getResultValue());

        call.yieldExploratoryExpression(newSelectExpression);
    }
}
