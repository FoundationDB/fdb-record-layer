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

import com.apple.foundationdb.record.query.plan.cascades.ExpressionPartition;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.SelectCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.argmin;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.expressionPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.filterExpressions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
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
public class SelectMergeRule extends ImplementationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<RelationalExpressionWithPredicates> childExpressionMatcher = withPredicatesExpression();

    @Nonnull
    private static final BindingMatcher<ExpressionPartition<RelationalExpression>> childPartitionsMatcher =
            argmin(SelectCountProperty.selectCount(), childExpressionMatcher);

    @Nonnull
    private static final BindingMatcher<Reference> childReferenceMatcher =
            expressionPartitions(rollUpPartitions(
                    any(filterExpressions(e -> e instanceof RelationalExpressionWithPredicates,
                            childPartitionsMatcher))));

    @Nonnull
    private static final CollectionMatcher<Quantifier.ForEach> quantifiersMatcher =
            some(forEachQuantifierOverRef(childReferenceMatcher));

    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression(quantifiersMatcher);

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
        final var correlationOrder = selectExpression.getCorrelationOrder().dualOrder();
        final var dependencyMap = correlationOrder.getDependencyMap();

        final var mergeableChildren =
                Streams.zip(quantifiers.stream(),
                                childSelectExpressions.stream(),
                                (left, right) ->
                                        NonnullPair.of(left.getAlias(), right))
                        .filter(pair ->
                                // If there are correlations pointing to that child from other children, then it can't
                                // be merged up as we'd have to potentially rewrite children to remove those references
                                dependencyMap.get(pair.getLeft()).isEmpty())
                        .collect(ImmutableMap.toImmutableMap(NonnullPair::getLeft, NonnullPair::getRight));

        // Nothing to merge found. Exit early
        if (mergeableChildren.isEmpty()) {
            return;
        }


        // Collect up the mergeable children, grouping by which child they came from.

        // Merge the quantifiers and predicates from each mergeable child expression
        final var newQuantifiers = ImmutableList.<Quantifier>builder();
        final var newPredicates = ImmutableList.<QueryPredicate>builder();
        final var translationBuilder = TranslationMap.builder();
        for (final var quantifier : selectExpression.getQuantifiers()) {
            final var alias = quantifier.getAlias();
            final var childSelectExpression = mergeableChildren.get(alias);
            if (childSelectExpression != null) {
                //
                // Mergeable. Add the child quantifiers in the old one's place and scoop up any predicates.
                //
                newQuantifiers.addAll(childSelectExpression.getQuantifiers());
                newPredicates.addAll(childSelectExpression.getPredicates());
                translationBuilder.when(alias)
                        .then((ignored1, ignored2) ->
                                childSelectExpression.getResultValue());
            } else {
                //
                // Not mergeable. Retain original quantifier
                //
                newQuantifiers.add(quantifier);
            }
        }

        //
        // Use the new translation map to update the final result value as well as any pre-existing predicates
        //
        TranslationMap translationMap = translationBuilder.build();
        selectExpression.getPredicates()
                .forEach(predicate -> newPredicates.add(predicate.translateCorrelations(translationMap, true)));

        final SelectExpression newSelectExpression = new SelectExpression(
                selectExpression.getResultValue().translateCorrelations(translationMap, true),
                newQuantifiers.build(),
                newPredicates.build()
        );
        call.yieldFinalExpression(newSelectExpression);
    }
}
