/*
 * PredicatePushDownRule.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUniqueExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.members;
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
    private static final BindingMatcher<Reference> belowReferenceMatcher = members(belowExpressionsMatcher);
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
        if (pushablePredicates.isEmpty()) {
            //
            // None of the predicates can be pushed down. Don't generate a rewritten expression here
            //
            return;
        }
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
        // be pushed into/through. Any that cannot accept the predicate will be skipped over and excluded from the
        // rewritten expression.
        //
        final var belowExpressions = bindings.get(belowExpressionsMatcher);
        for (final var belowExpression : belowExpressions) {
            pushToVisitor.visit(belowExpression).ifPresent(newBelowExpressions::add);
        }

        if (newBelowExpressions.isEmpty()) {
            //
            // We were unable to push the predicates down into the child quantifier.
            // Return without yielding a new expression.
            //
            return;
        }

        final var newRangesOverReference =
                call.memoizeReference(Reference.from(newBelowExpressions));

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
        private List<QueryPredicate> updatedPredicates(@Nonnull TranslationMap translationMap, @Nonnull Collection<? extends QueryPredicate> preExistingPredicates) {
            var predicatesBuilder = ImmutableList.<QueryPredicate>builderWithExpectedSize(getOriginalPredicates().size() + preExistingPredicates.size())
                    .addAll(preExistingPredicates);
            for (QueryPredicate originalPredicate : getOriginalPredicates()) {
                predicatesBuilder.add(originalPredicate.translateValues(translationMap));
            }
            return predicatesBuilder.build();
        }

        @Nonnull
        private List<QueryPredicate> updatedPredicates(@Nonnull TranslationMap translationMap) {
            return updatedPredicates(translationMap, ImmutableList.of());
        }

        @Nonnull
        public Quantifier.ForEach pushOverChild(@Nonnull final Quantifier.ForEach child) {
            final var translationMap =
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(getPushQuantifier().getAlias(),
                            child.getAlias()));
            final var newPredicates = updatedPredicates(translationMap);
            return Quantifier.forEach(Reference.of(
                    new SelectExpression(
                            child.getFlowedObjectValue(),
                            ImmutableList.of(child),
                            newPredicates
                    )
            ));
        }

        @Nonnull
        public Optional<List<Quantifier>> pushOverChildren(@Nonnull final RelationalExpressionWithChildren expressionWithChildren) {
            var newChildrenBuilder = ImmutableList.<Quantifier>builderWithExpectedSize(expressionWithChildren.getRelationalChildCount());
            for (Quantifier childQuantifier : expressionWithChildren.getQuantifiers()) {
                if (!(childQuantifier instanceof Quantifier.ForEach)) {
                    return Optional.empty();
                }
                newChildrenBuilder.add(pushOverChild((Quantifier.ForEach) childQuantifier));
            }
            return Optional.of(newChildrenBuilder.build());
        }

        @Nonnull
        public Optional<Quantifier> pushOverChildSingleChild(@Nonnull final RelationalExpressionWithChildren expressionWithChildren) {
            var oldChildren = expressionWithChildren.getQuantifiers();
            if (oldChildren.size() != 1) {
                return Optional.empty();
            }
            Quantifier oldChild = Iterables.getOnlyElement(oldChildren);
            if (!(oldChild instanceof Quantifier.ForEach)) {
                return Optional.empty();
            }
            return Optional.of(pushOverChild((Quantifier.ForEach) oldChild));
        }

        @Nonnull
        @Override
        public Optional<SelectExpression> visitLogicalFilterExpression(@Nonnull final LogicalFilterExpression logicalFilterExpression) {
            //
            // Replace the logical filter expression with a SelectExpression. It should combine the original
            // predicates (now applied to expression's child quantifier) with the expressions original predicates.
            //
            final var inner = logicalFilterExpression.getInner();
            if (!(inner instanceof Quantifier.ForEach)) {
                return Optional.empty();
            }
            final var translationMap =
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(getPushQuantifier().getAlias(),
                            inner.getAlias()));
            final var newPredicates = updatedPredicates(translationMap, logicalFilterExpression.getPredicates());
            return Optional.of(
                    new SelectExpression(inner.getFlowedObjectValue(),
                            ImmutableList.of(inner),
                            newPredicates));
        }

        @Nonnull
        @Override
        public Optional<SelectExpression> visitSelectExpression(@Nonnull final SelectExpression selectExpression) {
            //
            // Push down the original predicates by translating them to apply to the select expression's inner
            // predicates, and then combine them with the select's original predicates
            //
            final var translationMap = TranslationMap.builder()
                    .when(getPushQuantifier().getAlias())
                    .then(((sourceAlias, leafValue) -> selectExpression.getResultValue()))
                    .build();
            final var newPredicates = updatedPredicates(translationMap, selectExpression.getPredicates());
            return Optional.of(
                    new SelectExpression(selectExpression.getResultValue(),
                            selectExpression.getQuantifiers(),
                            newPredicates));
        }

        @Nonnull
        @Override
        public Optional<GroupByExpression> visitGroupByExpression(@Nonnull final GroupByExpression groupByExpression) {
            final Quantifier inner = groupByExpression.getInnerQuantifier();
            if (!(inner instanceof Quantifier.ForEach)) {
                return Optional.empty();
            }

            final var translationMap = TranslationMap.builder()
                    .when(getPushQuantifier().getAlias())
                    .then((sourceAlias, leafValue) -> groupByExpression.getResultValue())
                    .build();
            final var newPredicates = updatedPredicates(translationMap);
            final var newSelectQun = Quantifier.forEach(Reference.of(new SelectExpression(inner.getFlowedObjectValue(), ImmutableList.of(inner), newPredicates)));

            final var resultTranslation = TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(inner.getAlias(), newSelectQun.getAlias()));

            return Optional.of(new GroupByExpression(
                    groupByExpression.getGroupingValue() == null ? null : groupByExpression.getGroupingValue().translateCorrelations(resultTranslation),
                    (AggregateValue) groupByExpression.getAggregateValue().translateCorrelations(resultTranslation),
                    (groupIgnore, aggregateIgnore) -> groupByExpression.getResultValue().translateCorrelations(resultTranslation),
                    newSelectQun
            ));
        }

        @Nonnull
        @Override
        public Optional<LogicalUnionExpression> visitLogicalUnionExpression(@Nonnull final LogicalUnionExpression unionExpression) {
            //
            // Push the original predicates through the union. For each leg of the union, translate the predicates
            // to apply to that child, and then create a new SelectExpression over the original child to hold
            // the predicates. Further rewriting of the resulting child will handle things like pushing the child
            // predicates down more or merging with any existing SelectExpressions
            //
            return pushOverChildren(unionExpression).map(LogicalUnionExpression::new);
        }

        @Nonnull
        @Override
        public Optional<LogicalSortExpression> visitLogicalSortExpression(@Nonnull final LogicalSortExpression sortExpression) {
            //
            // Note: there are Values in the sort expression's requested ordering. However, they are all defined on current (or constant)
            // aliases, neither of which need translating when we push the predicates down to a new select below the sort
            //
            return pushOverChildSingleChild(sortExpression).map(newChild -> new LogicalSortExpression(sortExpression.getOrdering(), newChild));
        }


        @Nonnull
        @Override
        public Optional<LogicalDistinctExpression> visitLogicalDistinctExpression(@Nonnull final LogicalDistinctExpression element) {
            return pushOverChildSingleChild(element).map(LogicalDistinctExpression::new);
        }

        @Nonnull
        @Override
        public Optional<LogicalUniqueExpression> visitLogicalUniqueExpression(@Nonnull final LogicalUniqueExpression element) {
            return pushOverChildSingleChild(element).map(LogicalUniqueExpression::new);
        }

        @Nonnull
        @Override
        public Optional<RelationalExpression> visitDefault(@Nonnull final RelationalExpression element) {
            //
            // By default, we cannot push things down. Return nothing
            //
            return Optional.empty();
        }
    }
}
