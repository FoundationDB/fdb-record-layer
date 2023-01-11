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

import java.util.Collection;

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
            selectExpression(combinations(combinationQuantifierMatcher, c -> 0, Collection::size));

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

        final var lowerAliases = bindings.get(combinationQuantifierMatcher)
                .stream()
                .map(Quantifier::getAlias)
                .collect(ImmutableSet.toImmutableSet());
        if (lowerAliases.size() < 2) {
            return;
        }

        final var upperAliasesBuilder = ImmutableSet.<CorrelationIdentifier>builder();
        for (final var quantifier : selectExpression.getQuantifiers()) {
            final var alias = quantifier.getAlias();
            if (!lowerAliases.contains(alias)) {
                upperAliasesBuilder.add(alias);
            }
        }

        final var upperAliases = upperAliasesBuilder.build();
        if (upperAliases.isEmpty()) {
            return;
        }

        final var aliasToQuantifierMap = selectExpression.getAliasToQuantifierMap();
        final var fullCorrelationOrder =
                selectExpression.getCorrelationOrder().getTransitiveClosure();

        //
        // Reject the partitioning if partitioning the select expression according to the lowers and uppers may
        // cause a dependency cycle.
        //

        // collect all upper aliases that depend on lower aliases
        final var uppersDependingOnLowersAliases =
                upperAliases.stream()
                        .filter(upperAlias -> !Sets.intersection(lowerAliases, fullCorrelationOrder.get(upperAlias)).isEmpty())
                        .collect(ImmutableSet.toImmutableSet());

        // check if any lower ones depend on those upper ones
        if (lowerAliases.stream()
                .anyMatch(lowerAlias -> !Sets.intersection(uppersDependingOnLowersAliases, fullCorrelationOrder.get(lowerAlias)).isEmpty())) {
            return;
        }

        //
        // In order to avoid a costly calls to translateCorrelations(), we prefer deep-right dags.
        // Reject a partitioning that would force us to rebase the outer side.
        //
        final var lowersCorrelatedToByUppersAliases =
                upperAliases.stream()
                        .flatMap(upperAlias -> Sets.intersection(lowerAliases, fullCorrelationOrder.get(upperAlias)).stream())
                        .collect(ImmutableSet.toImmutableSet());

        final var resultCorrelatedToOuter = Sets.intersection(lowerAliases, selectExpression.getResultValue().getCorrelatedTo());

        final var lowersCorrelatedToAliases = Sets.union(lowersCorrelatedToByUppersAliases, resultCorrelatedToOuter);
        if (lowersCorrelatedToAliases.size() > 1) {
            return;
        }

        final CorrelationIdentifier lowerAlias;
        if (lowersCorrelatedToAliases.isEmpty()) {
            lowerAlias = Quantifier.uniqueID();
        } else {
            lowerAlias = Iterables.getOnlyElement(lowersCorrelatedToAliases);
        }

        //
        // Classify predicates according to their correlations. Some predicates are dependent on other aliases
        // within the current select expression, those are not eligible yet. Subtract those out to reach
        // the set of newly eligible predicates that can be applied as part of joining outer and inner.
        //

        // predicates that are only correlated to lower aliases
        final var lowerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        // predicates that are only correlated to upper aliases
        final var upperPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        for (final var predicate : selectExpression.getPredicates()) {
            final var correlatedTo = predicate.getCorrelatedTo();
            final var correlatedToLowerAliases = Sets.intersection(lowerAliases, correlatedTo);
            final var correlatedToUpperAliases = Sets.intersection(upperAliases, correlatedTo);

            if (!correlatedToUpperAliases.isEmpty()) {
                if (!correlatedToLowerAliases.isEmpty()) {
                    if (Sets.intersection(correlatedToUpperAliases, uppersDependingOnLowersAliases).isEmpty()) {
                        // we can do it in lower
                        lowerPredicatesBuilder.add(predicate);
                    } else {
                        //
                        // This predicate is correlated to (an upper alias that depends on a lower alias) AND
                        // a lower alias. This means the predicate (if it can be applied anywhere at all) would need to
                        // go into the new upper select expression. However, due to our restriction on what the lower
                        // select expression can flow, we can only allow predicates whose correlatedTo set scoped to
                        // lower is just the one alias we do flow.
                        //
                        if (correlatedToLowerAliases.size() == 1 &&
                                Iterables.getOnlyElement(correlatedToLowerAliases).equals(lowerAlias)) {
                            upperPredicatesBuilder.add(predicate);
                        } else {
                            return;
                        }
                    }
                } else {
                    upperPredicatesBuilder.add(predicate);
                }
            } else {
                // correlated to lower or deeply correlated
                lowerPredicatesBuilder.add(predicate);
            }
        }

        final var lowerPredicates = lowerPredicatesBuilder.build();
        final var upperPredicates = upperPredicatesBuilder.build();

        final var lowerGraphExpansionBuilder = GraphExpansion.builder();
        lowerGraphExpansionBuilder.addAllQuantifiers(lowerAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
        lowerGraphExpansionBuilder.addAllPredicates(lowerPredicates);
        final SelectExpression lowerSelectExpression;
        if (lowersCorrelatedToAliases.isEmpty()) {
            lowerGraphExpansionBuilder.addResultValue(LiteralValue.ofScalar(1));
            lowerSelectExpression = lowerGraphExpansionBuilder.build().buildSelect();
        } else {
            lowerSelectExpression = lowerGraphExpansionBuilder.build().buildSelectWithResultValue(Verify.verifyNotNull(aliasToQuantifierMap.get(lowerAlias)).getFlowedObjectValue());
        }
        
        final var upperGraphExpansionBuilder = GraphExpansion.builder();
        upperGraphExpansionBuilder.addQuantifier(Quantifier.forEachBuilder().withAlias(lowerAlias).build(call.ref(lowerSelectExpression)));
        upperGraphExpansionBuilder.addAllQuantifiers(upperAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
        upperGraphExpansionBuilder.addAllPredicates(upperPredicates);
        upperGraphExpansionBuilder.build().buildSelectWithResultValue(selectExpression.getResultValue());

        final var upperSelectExpression = upperGraphExpansionBuilder.build().buildSelectWithResultValue(selectExpression.getResultValue());
        call.yield(GroupExpressionRef.of(upperSelectExpression));
    }
}
