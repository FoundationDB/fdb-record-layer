/*
 * SplitSelectExtractIndependentQuantifiersRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.combinatorics.PartialOrder;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.CollectionMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.explodeExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that splits a {@link SelectExpression} into two {@link SelectExpression}s if one or more quantifiers this
 * expression owns satisfy the following criteria:
 * <ul>
 *     <li>
 *         the quantifier is not correlated to any other quantifier in this {@link SelectExpression}
 *     </li>
 *     <li>
 *         the quantifier ranges over a sub expression of limited cardinality. That means that the maximum
 *         cardinality of the sub expression should have an explicit upper bound. In reality this means that
 *         the subexpression is an {@link ExplodeExpression} over a parameter as we do not properly derive a maximum
 *         cardinality property (yet).
 *     </li>
 * </ul>
 *
 * Normally, we don't want to interfere with join enumeration (TBD) by effectively partitioning the quantifiers (and
 * therefore fixing the join order between quantifiers across these partitions). The particular case this rule
 * addresses is very specific in a sense that splitting the {@link SelectExpression} is objectively not harmful.
 * In fact, most SQL engines support this or similar kinds of transformations for star joins where pre-joining the
 * dimensions may lead to a better plan where in a general case pre-joining independent sub-queries may not yield
 * any performance gain. The important part here is that we must prove that the sub-expressions we want to move to the
 * outer side are limited in their cardinalities as creating a non-constrained cross-product of sources of unknown
 * cardinalities is almost always a bad idea.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class SplitSelectExtractIndependentQuantifiersRule extends PlannerRule<SelectExpression> {
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final CollectionMatcher<Quantifier.ForEach> explodeQuantifiersMatcher = some(forEachQuantifier(explodeExpressionMatcher));

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(explodeQuantifiersMatcher);

    public SplitSelectExtractIndependentQuantifiersRule() {
        super(root);
    }

    @SuppressWarnings("java:S135")
    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var bindings = call.getBindings();

        final var selectExpression = bindings.get(root);
        final Collection<? extends Quantifier.ForEach> explodeQuantifiers = bindings.get(explodeQuantifiersMatcher);
        if (explodeQuantifiers.isEmpty()) {
            return;
        }

        final var explodeAliases =
                explodeQuantifiers
                        .stream()
                        .map(Quantifier::getAlias)
                        .collect(ImmutableSet.toImmutableSet());

        if (isSimpleSelect(selectExpression, explodeAliases)) {
            // for not inductively exploding the graph
            return;
        }

        //
        // We first need to filter out the quantifiers that are correlated to some other quantifier in the
        // select expression. We only want to split off the ones that are completely independent.
        //
        final var allAliasesInExpression =
                selectExpression.getQuantifiers()
                        .stream()
                        .map(Quantifier::getAlias)
                        .collect(ImmutableSet.toImmutableSet());

        final var aliasesPartialOrderBuilder = PartialOrder.<CorrelationIdentifier>builder();

        for (final var quantifier: selectExpression.getQuantifiers()) {
            final var alias = quantifier.getAlias();
            final var correlatedTo = quantifier.getCorrelatedTo();
            final var localCorrelatedTo = Sets.intersection(allAliasesInExpression, correlatedTo);

            aliasesPartialOrderBuilder.add(alias);
            localCorrelatedTo.forEach(dependentAlias -> aliasesPartialOrderBuilder.addDependency(alias, dependentAlias));
        }

        final var aliasesPartialOrder = aliasesPartialOrderBuilder.build();

        final var eligibleAliases = aliasesPartialOrder.eligibleSet().eligibleElements();
        final var partitionedQuantifiers =
                selectExpression.getQuantifiers()
                        .stream()
                        .collect(Collectors.partitioningBy(quantifier ->
                                        explodeAliases.contains(quantifier.getAlias()) && eligibleAliases.contains(quantifier.getAlias()),
                                ImmutableList.toImmutableList()));

        // we need a proper partitioning
        if (partitionedQuantifiers.get(false).isEmpty() || partitionedQuantifiers.get(true).isEmpty()) {
            return;
        }

        //
        // Create a new SelectExpression with just the non-eligible quantifiers.
        //
        final var lowerSelectExpression =
                new SelectExpression(selectExpression.getResultValue(), partitionedQuantifiers.get(false), selectExpression.getPredicates());
        final var lowerQuantifier = Quantifier.forEach(GroupExpressionRef.of(lowerSelectExpression));

        //
        // Create a new SelectExpression with just the eligible quantifiers and a new quantifier ranging over
        // the newly created lower SelectExpression.
        //
        final var upperSelectExpression =
                new SelectExpression(lowerQuantifier.getFlowedObjectValue(),
                        ImmutableList.<Quantifier>builder()
                                .addAll(partitionedQuantifiers.get(true))
                                .add(lowerQuantifier)
                                .build(),
                        ImmutableList.of());

        call.yield(GroupExpressionRef.of(upperSelectExpression));
    }

    private boolean isSimpleSelect(@Nonnull final SelectExpression selectExpression,
                                   @Nonnull final Set<CorrelationIdentifier> explodeAliases) {
        if (!selectExpression.getPredicates().isEmpty()) {
            return false;
        }

        return selectExpression
                .getResultValues()
                .stream()
                .flatMap(resultValue -> resultValue.getCorrelatedTo().stream())
                .noneMatch(explodeAliases::contains);
    }
}
