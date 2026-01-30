/*
 * CellMergeRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionPartition;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.References;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PredicateComplexityProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.RegularTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.argmin;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.expressionPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.filterExpressions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierWithoutDefaultOnEmptyOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.isExploratoryExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.withPredicatesExpression;

/**
 * Rule for merging related select boxes into a single, larger select box. This works by starting with
 * a single {@link SelectExpression}. It then loops over its children's exploratory members, and looks
 * for "mergeable" child expressions. A child expression is considered mergeable if (1) the quantifier
 * is a {@link com.apple.foundationdb.record.query.plan.cascades.Quantifier.ForEach} that does not
 * introduce a null-on-empty, (2) none of the other children are correlated to the original quantifier,
 * and (3) the child expression is a {@link SelectExpression} or a {@link LogicalFilterExpression}.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class SelectMergeRule extends ImplementationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<RelationalExpressionWithPredicates> childExpressionMatcher = withPredicatesExpression();

    @Nonnull
    private static final BindingMatcher<ExpressionPartition<RelationalExpression>> childPartitionsMatcher =
            argmin(ExpressionsPartitionMatchers.<RelationalExpressionWithPredicates>comparisonByPropertyList(
                    ExpressionCountProperty.selectCount(),
                    ExpressionCountProperty.tableFunctionCount(),
                    PredicateComplexityProperty.predicateComplexity()
            ), childExpressionMatcher);

    @Nonnull
    private static final BindingMatcher<Reference> childReferenceMatcher =
            expressionPartitions(rollUpPartitions(
                    any(filterExpressions(e -> e instanceof RelationalExpressionWithPredicates,
                            childPartitionsMatcher))));

    @Nonnull
    private static final CollectionMatcher<Quantifier.ForEach> quantifiersMatcher =
            some(forEachQuantifierWithoutDefaultOnEmptyOverRef(childReferenceMatcher));

    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression(quantifiersMatcher).where(isExploratoryExpression());

    public SelectMergeRule() {
        super(root);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var quantifiers = bindings.get(quantifiersMatcher);
        final var childSelectExpressions = bindings.getAll(childExpressionMatcher);
        final var selectExpression = bindings.get(root);

        // there are no child contenders to merge.
        if (quantifiers.isEmpty()) {
            return;
        }

        final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(selectExpression.getQuantifiers());
        final var correlationOrder = selectExpression.getCorrelationOrder();
        final var correlationPermutation = TopologicalSort.anyTopologicalOrderPermutation(correlationOrder);
        final var quantifierToChildSelectExpressionMap =
                Streams.zip(quantifiers.stream(),
                                childSelectExpressions.stream(),
                                (left, right) ->
                                        NonnullPair.of(left.getAlias(), right))
                        .collect(ImmutableMap.toImmutableMap(NonnullPair::getLeft, NonnullPair::getRight));

        final Set<CorrelationIdentifier> newQunAliases = new HashSet<>();
        final var aliasToNewQuantifiersMapBuilder = ImmutableMap.<CorrelationIdentifier, List<? extends Quantifier>>builder();
        final var newPredicates = ImmutableList.<QueryPredicate>builder();
        final var nextExpressionTranslationBuilder = TranslationMap.regularBuilder();
        for (final var alias: correlationPermutation.orElseThrow()) {
            final var oldQun = aliasToQuantifierMap.get(alias);
            final var childSelectExpression = quantifierToChildSelectExpressionMap.get(alias);
            if (childSelectExpression != null) {
                AliasMap.Builder childAliasMapBuilder = AliasMap.builder();
                for (Quantifier childQun : childSelectExpression.getQuantifiers()) {
                    if (!newQunAliases.add(childQun.getAlias())) {
                        //
                        // Check if the same quantifier alias appears in multiple children of
                        // childSelectExpression. If one does, we need to assign a new alias
                        // to it. Choose one here
                        //
                        CorrelationIdentifier newAlias = Quantifier.uniqueId();
                        childAliasMapBuilder.put(childQun.getAlias(), newAlias);
                        newQunAliases.add(newAlias);
                    }
                }
                final var childAliasMap = TranslationMap.rebaseWithAliasMap(childAliasMapBuilder.build());
                final var nextSelectExpressionTranslationMap = nextExpressionTranslationBuilder.build();
                TranslationMap resultantTranslationMap;
                if (childAliasMap.definesOnlyIdentities()) {
                    resultantTranslationMap = RegularTranslationMap.compose(ImmutableList.of(nextSelectExpressionTranslationMap));
                } else {
                    resultantTranslationMap = RegularTranslationMap.compose(ImmutableList.of(childAliasMap, nextSelectExpressionTranslationMap));
                }

                aliasToNewQuantifiersMapBuilder.put(alias, Quantifiers.rebaseGraphs(childSelectExpression.getQuantifiers(), (Memoizer) call, resultantTranslationMap, true));
                childSelectExpression.getPredicates().stream()
                        .map(predicate -> predicate.translateCorrelations(resultantTranslationMap, true))
                        .forEach(newPredicates::add);
                final var childSelectExpressionTranslatedResultValue = childSelectExpression.getResultValue().translateCorrelations(resultantTranslationMap);

                nextExpressionTranslationBuilder.when(alias)
                        .then((ignore1, ignore2) -> childSelectExpressionTranslatedResultValue);
            } else {
                // non-SelectExpression
                Verify.verify(newQunAliases.add(alias), "alias %s duplicated in pulled up children", alias);
                final var childReference = aliasToQuantifierMap.get(alias).getRangesOver();
                Reference newChildReference = call.memoizeFinalExpressionsFromOther(childReference, childReference.getFinalExpressions());
                if (!correlationOrder.getDependencyMap().get(alias).isEmpty()) {
                    // This might possibly prove wasteful as we first create and memoize a new ref of final expressions,
                    // and then rebase it for adjustments - which itself could memoize refs that changed.
                    newChildReference = Iterables.getOnlyElement(References.rebaseGraphs(ImmutableList.of(newChildReference), (Memoizer) call, nextExpressionTranslationBuilder.build(), true));
                }
                aliasToNewQuantifiersMapBuilder.put(alias, ImmutableList.of(oldQun.overNewReference(newChildReference)));
            }
        }

        //
        // Use the translation map to update the final result value as well as any pre-existing predicates
        //
        final var topLevelSelectTranslationMap = nextExpressionTranslationBuilder.build();
        selectExpression.getPredicates()
                .forEach(predicate -> newPredicates.add(predicate.translateCorrelations(topLevelSelectTranslationMap, true)));
        final var aliasToNewQuantifiersMap = aliasToNewQuantifiersMapBuilder.build();
        final SelectExpression newSelectExpression = new SelectExpression(
                selectExpression.getResultValue().translateCorrelations(topLevelSelectTranslationMap, true),
                selectExpression.getQuantifiers().stream().flatMap(q -> aliasToNewQuantifiersMap.get(q.getAlias()).stream()).collect(Collectors.toList()),
                newPredicates.build()
        );
        call.yieldFinalExpression(newSelectExpression);
    }
}
