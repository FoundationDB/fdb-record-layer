/*
 * PredicatePushDownRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.exploratoryMembers;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.anyExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * TBD.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PredicatePushDownRule extends CascadesRule<SelectExpression> {
    @Nonnull
    private static final CollectionMatcher<RelationalExpression> belowExpressionsMatcher = all(anyExpression());
    @Nonnull
    private static final BindingMatcher<Reference> belowReferenceMatcher = exploratoryMembers(belowExpressionsMatcher);
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> forEachQuantifierMatcher =
            forEachQuantifierOverRef(belowReferenceMatcher);
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(forEachQuantifierMatcher);

    public PredicatePushDownRule() {
        super(root);
    }

    @SuppressWarnings("java:S135")
    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();

        final var selectExpression = bindings.get(root);

        //
        // This is the quantifier we are going to push predicates along.
        //
        final var pushQuantifier = bindings.get(forEachQuantifierMatcher);

        //
        // Identify all predicates in the select expression that are local to the quantifier we are pushing
        // down along, i.e. all predicates that are only correlated to the push quantifier or deeply correlated
        // to some other quantifier not owned by this select expression.
        //
        final var otherAliases =
                Quantifiers.aliases(() ->
                        selectExpression.getQuantifiers()
                                .stream()
                                .map(quantifier -> (Quantifier)quantifier)
                                .filter(quantifier -> !quantifier.getAlias().equals(pushQuantifier.getAlias()))
                                .iterator());

        final var partitionedPredicates = selectExpression
                .getPredicates()
                .stream()
                .collect(Collectors.partitioningBy(queryPredicate ->
                                queryPredicate.getCorrelatedTo().stream().noneMatch(otherAliases::contains),
                        LinkedIdentitySet.toLinkedIdentitySet()));

        final var pushablePredicates = partitionedPredicates.get(true);
        final var fixedPredicates = partitionedPredicates.get(false);

        //
        // We do not consider all possible subsets of pushable predicates. There really is no need to and would be
        // overkill. Either we are unable to push any predicate into or through an expression, or we can push all. The
        // shape of the predicate should not matter considering the expression we push into/through.
        //

        final var pushToVisitor = new PushToVisitor(pushablePredicates, pushQuantifier);
        final var newBelowExpressions = new LinkedIdentitySet<RelationalExpression>();

        //
        // Go through all expressions within the reference the push quantifier ranges over and find those that can
        // be pushed into/through.
        //
        final var belowExpressions = bindings.get(belowExpressionsMatcher);

        for (final var belowExpression : belowExpressions) {
            pushToVisitor.visit(belowExpression).ifPresent(newBelowExpressions::add);
        }

        final var newRangesOverReference =
                call.memoizeReference(Reference.of(newBelowExpressions));

        final var newPushQuantifier = Quantifier.forEachBuilder()
                .withAlias(pushQuantifier.getAlias())
                .build(newRangesOverReference);

        final var newOwnedQuantifiers = selectExpression.getQuantifiers()
                .stream()
                .map(quantifier -> quantifier.getAlias().equals(pushQuantifier.getAlias()) ? newPushQuantifier : quantifier)
                .collect(ImmutableList.toImmutableList());

        final var newSelectExpression = new SelectExpression(selectExpression.getResultValue(),
                newOwnedQuantifiers,
                ImmutableList.copyOf(fixedPredicates));

        call.yieldExpression(newSelectExpression);
    }

    private static class PushToVisitor implements RelationalExpressionVisitorWithDefaults<Optional<? extends RelationalExpression>> {
        @Nonnull
        private final Set<? extends QueryPredicate> originalPredicates;
        @Nonnull
        private final Quantifier.ForEach pushQuantifier;

        public PushToVisitor(@Nonnull final Set<? extends QueryPredicate> originalPredicates,
                             @Nonnull final Quantifier.ForEach pushQuantifier) {
            this.originalPredicates = originalPredicates;
            this.pushQuantifier = pushQuantifier;
        }

        @Nonnull
        private Set<? extends QueryPredicate> getOriginalPredicates() {
            return originalPredicates;
        }

        @Nonnull
        private Quantifier.ForEach getPushQuantifier() {
            return pushQuantifier;
        }

        @Nonnull
        @Override
        public Optional<SelectExpression> visitLogicalFilterExpression(@Nonnull final LogicalFilterExpression logicalFilterExpression) {
            final var inner = logicalFilterExpression.getInner();
            final var translationMap =
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(getPushQuantifier().getAlias(),
                            inner.getAlias()));

            final var predicatesBuilder = ImmutableList.<QueryPredicate>builder();
            predicatesBuilder.addAll(logicalFilterExpression.getPredicates());

            for (final var originalPredicate : getOriginalPredicates()) {
                predicatesBuilder.add(
                        originalPredicate.translateValues(value ->
                                value.translateCorrelations(translationMap)));
            }

            return Optional.of(
                    new SelectExpression(inner.getFlowedObjectValue(),
                            ImmutableList.of(inner),
                            predicatesBuilder.build()));
        }

        public Optional<? extends RelationalExpression> pushThrough(@Nonnull final RelationalExpression expression,
                                                                    @Nonnull final Quantifier.ForEach inner) {
            final var translationMap =
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(getPushQuantifier().getAlias(),
                            inner.getAlias()));

            final var predicatesBuilder = ImmutableList.<QueryPredicate>builder();

            for (final var originalPredicate : getOriginalPredicates()) {
                predicatesBuilder.add(
                        originalPredicate.translateValues(value ->
                                value.translateCorrelations(translationMap)));
            }

            return Optional.of(
                    new SelectExpression(inner.getFlowedObjectValue(),
                            ImmutableList.of(inner),
                            predicatesBuilder.build()));
        }

        @Nonnull
        @Override
        public Optional<SelectExpression> visitSelectExpression(@Nonnull final SelectExpression selectExpression) {
            final var translationMap = TranslationMap.builder()
                    .when(getPushQuantifier().getAlias())
                    .then(((sourceAlias, leafValue) -> selectExpression.getResultValue()))
                    .build();

            final var predicatesBuilder = ImmutableList.<QueryPredicate>builder();
            predicatesBuilder.addAll(selectExpression.getPredicates());
            for (final var originalPredicate : getOriginalPredicates()) {
                predicatesBuilder.add(
                        originalPredicate.translateValues(value ->
                                value.translateCorrelations(translationMap)
                                        .simplify(AliasMap.emptyMap(), ImmutableSet.of())));
            }

            return Optional.of(
                    new SelectExpression(selectExpression.getResultValue(),
                            selectExpression.getQuantifiers(),
                            predicatesBuilder.build()));
        }

        @Nonnull
        @Override
        public Optional<RelationalExpression> visitDefault(@Nonnull final RelationalExpression element) {
            return Optional.empty();
        }
    }
}
