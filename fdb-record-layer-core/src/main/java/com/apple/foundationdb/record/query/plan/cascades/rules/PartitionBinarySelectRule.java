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
 * <br>
 * Current thinking on future extensions: At first glance, leaving the resulting {@link SelectExpression} without any
 * predicates, but potentially correlating one side to the other seems to only be beneficial for nested-loop joins.
 * However, the pre-transformation {@link SelectExpression} does contain all those predicates, which therefore can
 * still be utilized to e.g. implement a hash join.
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

        final var leftPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        final var rightPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        for (final var predicate : selectExpression.getPredicates()) {
            final var correlatedTo = predicate.getCorrelatedTo();
            final var correlatedToLeft = correlatedTo.contains(leftAlias);
            final var correlatedToRight = correlatedTo.contains(rightAlias);

            if (correlatedToLeft && correlatedToRight) {
                // Predicate correlated to both. If the left depends on the right, we need to send it
                // to the left to avoid a circular dependency. Otherwise, we push it to the right
                if (leftDependsOnRight) {
                    leftPredicatesBuilder.add(predicate);
                } else {
                    rightPredicatesBuilder.add(predicate);
                }
            } else if (correlatedToRight) {
                // Predicate is correlated to just the right. Send it to the right
                rightPredicatesBuilder.add(predicate);
            } else {
                // Predicate is either correlated to just the left or it is correlated to neither.
                // Send it to the left, as this is the left most position it can be
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

        var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(leftQuantifier);
        graphExpansionBuilder.addAllPredicates(leftPredicates);

        final Quantifier newLeftQuantifier;
        if (!leftPredicates.isEmpty()) {
            final SelectExpression leftSelectExpression;
            if (leftQuantifier instanceof Quantifier.ForEach) {
                leftSelectExpression = graphExpansionBuilder.build().buildSimpleSelectOverQuantifier((Quantifier.ForEach)leftQuantifier);
            } else {
                graphExpansionBuilder.addResultValue(LiteralValue.ofScalar(1));
                leftSelectExpression = graphExpansionBuilder.build().buildSelect();
            }
            newLeftQuantifier = Quantifier.forEachBuilder()
                    .withAlias(leftQuantifier.getAlias())
                    .build(call.memoizeExploratoryExpression(leftSelectExpression));
        } else {
            newLeftQuantifier = leftQuantifier;
        }

        graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(rightQuantifier);
        graphExpansionBuilder.addAllPredicates(rightPredicates);

        final Quantifier newRightQuantifier;
        if (!rightPredicates.isEmpty()) {
            final SelectExpression rightSelectExpression;
            if (rightQuantifier instanceof Quantifier.ForEach) {
                rightSelectExpression = graphExpansionBuilder.build().buildSimpleSelectOverQuantifier((Quantifier.ForEach)rightQuantifier);
            } else {
                graphExpansionBuilder.addResultValue(LiteralValue.ofScalar(1));
                rightSelectExpression = graphExpansionBuilder.build().buildSelect();
            }
            newRightQuantifier = Quantifier.forEachBuilder()
                    .withAlias(rightQuantifier.getAlias())
                    .build(call.memoizeExploratoryExpression(rightSelectExpression));
        } else {
            newRightQuantifier = rightQuantifier;
        }

        graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(newLeftQuantifier);
        graphExpansionBuilder.addQuantifier(newRightQuantifier);

        final var newSelectExpression = graphExpansionBuilder.build().buildSelectWithResultValue(selectExpression.getResultValue());

        call.yieldExploratoryExpression(newSelectExpression);
    }
}
