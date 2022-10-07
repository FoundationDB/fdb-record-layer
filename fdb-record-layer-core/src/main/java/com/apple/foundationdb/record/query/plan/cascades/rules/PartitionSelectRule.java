/*
 * PartitionSelectRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher.combinations;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that splits a {@link SelectExpression} into two {@link SelectExpression}s which are then joined together.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PartitionSelectRule extends CascadesRule<SelectExpression> {
    private static final CollectionMatcher<Quantifier> combinationQuantifierMatcher = all(anyQuantifier());

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(combinations(combinationQuantifierMatcher, c -> 0, c -> c.size() / 2));

    public PartitionSelectRule() {
        super(root);
    }

    @SuppressWarnings("java:S135")
    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();

        final var selectExpression = bindings.get(root);
        if (selectExpression.getQuantifiers().size() < 3) {
            return;
        }

        final var leftAliases = bindings.get(combinationQuantifierMatcher)
                .stream()
                .map(Quantifier::getAlias)
                .collect(ImmutableSet.toImmutableSet());
        if (leftAliases.isEmpty()) {
            return;
        }

        final var rightAliasesBuilder = ImmutableSet.<CorrelationIdentifier>builder();
        for (final var quantifier : selectExpression.getQuantifiers()) {
            final var alias = quantifier.getAlias();
            if (!leftAliases.contains(alias)) {
                rightAliasesBuilder.add(alias);
            }
        }

        final var rightAliases = rightAliasesBuilder.build();
        if (rightAliases.isEmpty()) {
            return;
        }

        //
        // At this moment we have partitioned sets of aliases called left and right. We now try to use
        // (left, right) as (outer, inner) and if appropriate also the other way around using (right, left) as
        // (outer, inner).
        //
        final var isSymmetric = partitionSelect(call, selectExpression, leftAliases, rightAliases);
        if (!isSymmetric) {
            partitionSelect(call, selectExpression, rightAliases, leftAliases);
        }
    }

    private boolean partitionSelect(@Nonnull final CascadesRuleCall call,
                                    @Nonnull final SelectExpression selectExpression,
                                    @Nonnull final Set<CorrelationIdentifier> outerAliases,
                                    @Nonnull final Set<CorrelationIdentifier> innerAliases) {
        final var aliasToQuantifierMap = selectExpression.getAliasToQuantifierMap();
        final var fullCorrelationOrder =
                selectExpression.getCorrelationOrder().getTransitiveClosure();

        final var isAnyOuterDependingOnInner =
                outerAliases.stream()
                        .anyMatch(alias -> !Sets.intersection(innerAliases, fullCorrelationOrder.get(alias)).isEmpty());
        if (isAnyOuterDependingOnInner) {
            return false;
        }

        //
        // In order to avoid a costly calls to translateCorrelations(), we prefer deep-right dags.
        // Reject every constellation that would force us to rebase the outer side.
        //
        final var innerCorrelatedToOuter =
                innerAliases.stream()
                        .flatMap(innerAlias -> Sets.intersection(outerAliases, fullCorrelationOrder.get(innerAlias)).stream())
                        .collect(ImmutableSet.toImmutableSet());

        final var resultCorrelatedToOuter = Sets.intersection(outerAliases, selectExpression.getResultValue().getCorrelatedTo());

        final var outerCorrelationSourceAliases = Sets.union(innerCorrelatedToOuter, resultCorrelatedToOuter);
        if (outerCorrelationSourceAliases.size() > 1) {
            return false;
        }
        
        //
        // Classify predicates according to their correlations. Some predicates are dependent on other aliases
        // within the current select expression, those are not eligible yet. Subtract those out to reach
        // the set of newly eligible predicates that can be applied as part of joining outer and inner.
        //

        // predicates that are only correlated to outer
        final var outerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        // predicates that are only correlated to inner
        final var innerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        // predicates that are both correlated to outer and as well to inner
        final var joinPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        // predicates that are not correlated to inner nor outer (constant predicates)
        final var otherPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        for (final var predicate : selectExpression.getPredicates()) {
            final var correlatedTo = predicate.getCorrelatedTo();
            final var isCorrelatedToOuter = !Sets.intersection(outerAliases, correlatedTo).isEmpty();
            final var isCorrelatedToInner = !Sets.intersection(innerAliases, correlatedTo).isEmpty();

            if (isCorrelatedToInner) {
                if (isCorrelatedToOuter) {
                    joinPredicatesBuilder.add(predicate);
                } else {
                    innerPredicatesBuilder.add(predicate);
                }
            } else if (isCorrelatedToOuter) {
                outerPredicatesBuilder.add(predicate);
            } else {
                otherPredicatesBuilder.add(predicate);
            }
        }

        final var outerPredicates = outerPredicatesBuilder.build();
        final var innerPredicates = innerPredicatesBuilder.build();
        final var joinPredicates = joinPredicatesBuilder.build();
        final var otherPredicates = otherPredicatesBuilder.build();

        final var outerGraphExpansionBuilder = GraphExpansion.builder();
        outerGraphExpansionBuilder.addAllQuantifiers(outerAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
        outerGraphExpansionBuilder.addAllPredicates(outerPredicates);
        outerGraphExpansionBuilder.addAllPredicates(otherPredicates);
        final CorrelationIdentifier outerAlias;
        final SelectExpression outerSelectExpression;
        if (outerCorrelationSourceAliases.isEmpty()) {
            outerGraphExpansionBuilder.addResultValue(LiteralValue.ofScalar(1));
            outerAlias = Quantifier.uniqueID();
            outerSelectExpression = outerGraphExpansionBuilder.build().buildSelect();
        } else {
            outerAlias = Iterables.getOnlyElement(outerCorrelationSourceAliases);
            outerSelectExpression = outerGraphExpansionBuilder.build().buildSelectWithResultValue(Verify.verifyNotNull(aliasToQuantifierMap.get(outerAlias)).getFlowedObjectValue());
        }
        
        final var innerGraphExpansionBuilder = GraphExpansion.builder();
        innerGraphExpansionBuilder.addAllQuantifiers(innerAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
        innerGraphExpansionBuilder.addAllPredicates(innerPredicates);
        innerGraphExpansionBuilder.addAllPredicates(joinPredicates);
        final var innerSelectExpression = innerGraphExpansionBuilder.build().buildSelectWithResultValue(selectExpression.getResultValue());

        final var resultGraphExpansion = GraphExpansion.builder();
        resultGraphExpansion.addQuantifier(Quantifier.forEachBuilder().withAlias(outerAlias).build(call.ref(outerSelectExpression)));
        final var innerQuantifier = Quantifier.forEach(call.ref(innerSelectExpression));
        resultGraphExpansion.addQuantifier(innerQuantifier);
        final var partitionedSelectExpression = resultGraphExpansion.build().buildSelectWithResultValue(innerQuantifier.getFlowedObjectValue());
        call.yield(GroupExpressionRef.of(partitionedSelectExpression));

        return joinPredicates.isEmpty() && resultCorrelatedToOuter.isEmpty();
    }
}
