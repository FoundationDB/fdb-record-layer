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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * Rule for merging related select boxes into a single, larger select box.
 */
public class SelectMergeRule extends ExplorationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression();

    public SelectMergeRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final SelectExpression select = call.get(root);

        // Identify the set of children of the current select that do not create correlations
        final Set<Quantifier> mergeableChildren = select.getQuantifiers().stream()
                .filter(qun -> qun instanceof Quantifier.ForEach)
                .map(qun -> (Quantifier.ForEach) qun)
                .filter(qun ->
                        // Cannot merge if child introduces a null on empty as that changes the overall cardinality
                        !qun.isNullOnEmpty()
                                // Require that at least one child expression is of a mergeable type
                                && qun.getRangesOver().getExploratoryExpressions().stream().anyMatch(expr -> expr instanceof SelectExpression || expr instanceof LogicalFilterExpression)
                                // If there are correlations pointing to that child from other children, then it can't be merged up as we'd have
                                // to potentially rewrite children to remove those references
                                && select.getQuantifiers().stream().filter(qun2 -> qun2 != qun).noneMatch(qun2 -> qun2.isCorrelatedTo(qun.getAlias()))
                )
                .collect(ImmutableSet.toImmutableSet());

        // Nothing to merge found. Exit early
        if (mergeableChildren.isEmpty()) {
            return;
        }

        // Collect up the mergeable children, grouping by which child they came from
        final List<List<NonnullPair<CorrelationIdentifier, RelationalExpression>>> mergeableIdExpressions = mergeableChildren.stream()
                .map(qun -> qun.getRangesOver().getExploratoryExpressions().stream()
                        .filter(expr -> expr instanceof SelectExpression || expr instanceof LogicalFilterExpression)
                        .map(expr -> NonnullPair.of(qun.getAlias(), expr))
                        .collect(Collectors.toList())
                )
                .collect(Collectors.toList());

        // For each element in the cross product of the available children, meld those children
        // into the upper select expression
        CrossProduct.crossProduct(mergeableIdExpressions).forEach(mergeableByIdList -> {
            // For each of the merged children, link the original quantifier alias to the expression
            final Map<CorrelationIdentifier, RelationalExpression> mergeableById = mergeableByIdList.stream()
                    .collect(Collectors.toMap(NonnullPair::getKey, NonnullPair::getValue));

            // Merge the quantifiers and predicates from each mergeable child expression
            final ImmutableList.Builder<Quantifier> newQuantifiers = ImmutableList.builder();
            final ImmutableList.Builder<QueryPredicate> newPredicates = ImmutableList.builder();
            final TranslationMap.Builder translationBuilder = TranslationMap.builder();
            for (Quantifier selectChild : select.getQuantifiers()) {
                @Nullable RelationalExpression childExpression = mergeableById.get(selectChild.getAlias());
                if (childExpression == null) {
                    //
                    // Not mergeable. Retain original quantifier
                    //
                    newQuantifiers.add(selectChild);
                } else if (childExpression instanceof SelectExpression || childExpression instanceof LogicalFilterExpression) {
                    //
                    // Mergeable. Add the child quantifiers in the old one's place and scoop up any predicates.
                    //
                    RelationalExpressionWithPredicates predicateExpression = (RelationalExpressionWithPredicates) childExpression;
                    newQuantifiers.addAll(predicateExpression.getQuantifiers());
                    newPredicates.addAll(predicateExpression.getPredicates());
                    translationBuilder.when(selectChild.getAlias())
                            .then((alias, leaf) -> predicateExpression.getResultValue());
                } else {
                    throw new RecordCoreException("unexpected mergeable expression type",
                            LogMessageKeys.VALUE, childExpression);
                }
            }

            //
            // Use the new translation map to update the final result value as well as any pre-existing predicates
            //
            TranslationMap translationMap = translationBuilder.build();
            select.getPredicates()
                    .forEach(predicate -> newPredicates.add(predicate.translateCorrelations(translationMap, true)));

            call.yieldExploratoryExpression(new SelectExpression(
                    select.getResultValue().translateCorrelations(translationMap, true),
                    newQuantifiers.build(),
                    newPredicates.build()
            ));
        });
    }
}
