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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher.combinations;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that splits a {@link SelectExpression} into two {@link SelectExpression}s .
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
        if (lowerAliases.isEmpty()) {
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

        final var independentQuantifiersPartitioning = selectExpression.getIndependentQuantifiersPartitioning();
        if (independentQuantifiersPartitioning.size() > 1) {
            final var plannerConfiguration = call.getContext().getPlannerConfiguration();
            if (plannerConfiguration.shouldDeferCrossProducts()) {
                //
                // If we are here it means that this select expression has at least one partitioning that falls along
                // with the partitioning of independent quantifiers. We allow only that partitioning which will become
                // a cross product. The partitions themselves will then have fewer independent quantifier partitions
                // (most likely a number that is close to one or one) which then won't trigger this if branch and will
                // use normal select partitioning.
                //
                if (!isCrossProduct(independentQuantifiersPartitioning, lowerAliases, upperAliases)) {
                    return;
                }
            }
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
        final var lowersCorrelatedToByUpperAliases =
                upperAliases.stream()
                        .flatMap(upperAlias -> Sets.intersection(lowerAliases, fullCorrelationOrder.get(upperAlias)).stream())
                        .collect(ImmutableSet.toImmutableSet());
        if (lowersCorrelatedToByUpperAliases.size() > 1) {
            return;
        }

        final CorrelationIdentifier lowerAliasCorrelatedToByUpperAliases;
        if (lowersCorrelatedToByUpperAliases.isEmpty()) {
            lowerAliasCorrelatedToByUpperAliases = Quantifier.uniqueID();
        } else {
            lowerAliasCorrelatedToByUpperAliases = Iterables.getOnlyElement(lowersCorrelatedToByUpperAliases);
        }

        final var lowersCorrelatedToByUppersBuilder = ImmutableList.<CorrelationIdentifier>builder();

        final var resultValue = selectExpression.getResultValue();
        final var resultCorrelatedToLowers = Sets.intersection(lowerAliases, resultValue.getCorrelatedTo());
        lowersCorrelatedToByUppersBuilder.addAll(resultCorrelatedToLowers);

        //
        // Classify predicates according to their correlations. Some predicates are dependent on other aliases
        // within the current select expression, those are not eligible yet. Subtract those out to reach
        // the set of newly eligible predicates that can be applied.
        //

        // predicates that are going to the lower select expression
        final var lowerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        // predicates that are going to the upper select expression
        final var upperPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        // predicates that only deeply correlated and in effect constants
        final var deeplyCorrelatedPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

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
                        // this may or may not be possible -- we'll see further down below
                        upperPredicatesBuilder.add(predicate);
                        lowersCorrelatedToByUppersBuilder.addAll(correlatedToLowerAliases);
                    }
                } else {
                    upperPredicatesBuilder.add(predicate);
                }
            } else {
                if (!correlatedToLowerAliases.isEmpty()) {
                    // correlated to lower or deeply correlated
                    lowerPredicatesBuilder.add(predicate);
                } else {
                    deeplyCorrelatedPredicatesBuilder.add(predicate);
                }
            }
        }

        final var lowerPredicates = lowerPredicatesBuilder.build();
        final var upperPredicates = upperPredicatesBuilder.build();
        final var deeplyCorrelatedPredicates = deeplyCorrelatedPredicatesBuilder.build();

        final var lowersCorrelatedToByUppers = lowersCorrelatedToByUppersBuilder.build();

        //
        // If an upper quantifier depends on lower quantifiers (and we can only support exactly one such
        // dependency), we need to make sure the predicates and the result value we are about to place can still be
        // placed when that quantifier dependency is honoured.
        //
        if (!lowersCorrelatedToByUpperAliases.isEmpty()) {
            if (lowersCorrelatedToByUppers.size() > 1 ) {
                return;
            }

            if (lowersCorrelatedToByUppers.size() == 1) {
                if (!Iterables.getOnlyElement(lowersCorrelatedToByUppers).equals(lowerAliasCorrelatedToByUpperAliases)) {
                    return;
                }
            }
        }

        //
        // We only want to proceed with the partitioning if the partitioning itself is helpful:
        // 1. The new upper select expression would have fewer quantifiers than the original select expression (that
        //    is always true except if we partition a binary select expression) OR
        // 2. lower predicates is not empty.
        //
        if (lowerAliases.size() == 1) { // this removes one and adds one
            if (lowerPredicates.isEmpty()) {
                // not useful
                return;
            }
        }

        final var lowerGraphExpansionBuilder = GraphExpansion.builder();
        lowerGraphExpansionBuilder.addAllQuantifiers(lowerAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
        lowerGraphExpansionBuilder.addAllPredicates(lowerPredicates);
        lowerGraphExpansionBuilder.addAllPredicates(deeplyCorrelatedPredicates);

        final SelectExpression upperSelectExpression;
        if (lowersCorrelatedToByUpperAliases.isEmpty() && lowersCorrelatedToByUppers.isEmpty()) {
            lowerGraphExpansionBuilder.addResultValue(LiteralValue.ofScalar(1));
            final var lowerSelectExpression = lowerGraphExpansionBuilder.build().buildSelect();

            final var upperGraphExpansionBuilder = GraphExpansion.builder();
            upperGraphExpansionBuilder.addQuantifier(Quantifier.forEachBuilder().withAlias(lowerAliasCorrelatedToByUpperAliases).build(call.memoizeExpression(lowerSelectExpression)));
            upperGraphExpansionBuilder.addAllQuantifiers(upperAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
            upperGraphExpansionBuilder.addAllPredicates(upperPredicates);
            upperSelectExpression = upperGraphExpansionBuilder.build().buildSelectWithResultValue(resultValue);
        } else if (!lowersCorrelatedToByUpperAliases.isEmpty() || lowersCorrelatedToByUppers.size() == 1) {
            final var lowerAlias = lowersCorrelatedToByUpperAliases.isEmpty() ? Iterables.getOnlyElement(lowersCorrelatedToByUppers) : lowerAliasCorrelatedToByUpperAliases;
            final var lowerSelectExpression = lowerGraphExpansionBuilder.build().buildSelectWithResultValue(Verify.verifyNotNull(aliasToQuantifierMap.get(lowerAlias)).getFlowedObjectValue());

            final var upperGraphExpansionBuilder = GraphExpansion.builder();
            upperGraphExpansionBuilder.addQuantifier(Quantifier.forEachBuilder().withAlias(lowerAlias).build(call.memoizeExpression(lowerSelectExpression)));
            upperGraphExpansionBuilder.addAllQuantifiers(upperAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
            upperGraphExpansionBuilder.addAllPredicates(upperPredicates);
            upperSelectExpression = upperGraphExpansionBuilder.build().buildSelectWithResultValue(resultValue);
        } else {
            final ImmutableList<Column<? extends Value>> lowerResultColumns =
                    lowersCorrelatedToByUppers.stream()
                            .map(lowerAlias -> Verify.verifyNotNull(aliasToQuantifierMap.get(lowerAlias)))
                            .map(quantifier -> (Value)QuantifiedObjectValue.of(quantifier))
                            .map(Column::unnamedOf)
                            .collect(ImmutableList.toImmutableList());
            final var joinedResultValue =
                    RecordConstructorValue.ofColumns(lowerResultColumns);

            final var lowerSelectExpression = lowerGraphExpansionBuilder.build().buildSelectWithResultValue(joinedResultValue);
            final var newUpperQuantifier = Quantifier.forEachBuilder().withAlias(lowerAliasCorrelatedToByUpperAliases).build(call.memoizeExpression(lowerSelectExpression));

            final var translationMapBuilder = TranslationMap.builder();
            for (int i = 0; i < lowerResultColumns.size(); i++) {
                final Column<? extends Value> lowerResultColumn = lowerResultColumns.get(i);
                final var lowerAlias = ((QuantifiedObjectValue)lowerResultColumn.getValue()).getAlias();
                final var index = i;
                translationMapBuilder.when(lowerAlias)
                        .then((sourceAlias, oldLeafValue) -> FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(newUpperQuantifier), index));
            }

            final var translationMap = translationMapBuilder.build();

            final var newUpperPredicates =
                    upperPredicates.stream()
                            .map(upperPredicate -> upperPredicate.replaceLeavesMaybe(leafPredicate -> leafPredicate.translateLeafPredicate(translationMap))
                                    .orElseThrow(() -> new RecordCoreException("unable to map leaf predicate")))
                            .collect(ImmutableList.toImmutableList());

            final var newResultValue =
                    resultValue.translateCorrelations(translationMap);

            final var upperGraphExpansionBuilder = GraphExpansion.builder();
            upperGraphExpansionBuilder.addQuantifier(newUpperQuantifier);
            upperGraphExpansionBuilder.addAllQuantifiers(upperAliases.stream().map(alias -> Verify.verifyNotNull(aliasToQuantifierMap.get(alias))).collect(ImmutableList.toImmutableList()));
            upperGraphExpansionBuilder.addAllPredicates(newUpperPredicates);
            upperSelectExpression = upperGraphExpansionBuilder.build().buildSelectWithResultValue(newResultValue);
        }
        
        call.yieldExpression(upperSelectExpression);
    }

    private boolean isCrossProduct(@Nonnull final Set<Set<CorrelationIdentifier>> independentQuantifiersPartitioning,
                                   @Nonnull final Set<CorrelationIdentifier> lowerAliases,
                                   @Nonnull final Set<CorrelationIdentifier> upperAliases) {
        // Check if any independent partitioning has members that are in both lower and upper, if so, this break
        // into lower and upper is NOT a cross-product.
        for (final var independentPartition : independentQuantifiersPartitioning) {
            boolean isInLower = false;
            boolean isInUpper = false;
            for (final var alias : independentPartition) {
                if (lowerAliases.contains(alias)) {
                    isInLower = true;
                } else if (upperAliases.contains(alias)) {
                    isInUpper = true;
                }
                if (isInLower && isInUpper) {
                    return false;
                }
            }
        }
        return true;
    }
}
