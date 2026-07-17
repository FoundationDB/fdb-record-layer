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
import com.apple.foundationdb.record.query.plan.cascades.AbstractCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionPartition;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PredicateComplexityProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.argmin;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.expressionPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ExpressionsPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.atLeastOne;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierWithoutDefaultOnEmptyOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.anyExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.isFinalExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * Planner rule for pushing predicates down in an expression tree. This rule is intended to run during the
 * {@link com.apple.foundationdb.record.query.plan.cascades.PlannerPhase#REWRITING REWRITING} phase, and it
 * is designed to ensure that predicates are as far down in the query tree as possible.
 * It works by identifying a {@link SelectExpression} and looking for predicates that only reference a single
 * one of that expression's child quantifiers. For example, an expression like:
 *
 * <pre>{@code
 *        +-----------------------+
 *        | SELECT q1.b AS b,     |
 *        |   q2.d AS d           |
 *        +-----------------------+
 *        | WHERE q1.a = 'x'      |
 *        |   AND q1.b = q2.c     |
 *        +-----------------------+
 *             / q1        \ q2
 *   +--------------+    +---------------+
 *   | SELECT a, b  |    | SELECT c, d   |
 *   +------------- +    +---------------+
 *     /   |   \             /   |   \
 *        ...                   ...
 * }</pre>
 * 
 * <p>
 * In the top most select box, there are two predicates, one on just the {@code q1} quantifier and another on the
 * both {@code q1} and {@code q2}. It will attempt to push down the {@code q1} predicate while leaving the other
 * (multi-quantifier) predicate as is. This will produce a new expression like:
 * </p>
 *
 * <pre>{@code
 *        +-----------------------+
 *        | SELECT q1.b AS b,     |
 *        |   q2.d AS d           |
 *        +-----------------------+
 *        | WHERE q1.b = q2.c     |
 *        +-----------------------+
 *             / q1        \ q2
 *   +--------------+    +---------------+
 *   | SELECT a, b  |    | SELECT c, d   |
 *   +------------- +    +---------------+
 *   | WHERE a = 'x'|        /   |   \
 *   +------------- +           ...
 *     /   |   \
 *        ...
 * }</pre>
 *
 * <p>
 * Retaining the join-predicate in the top-most box is important for join enumeration, as we may want to push
 * the predicate to either the left-hand side or the right-hand side when constructing the physical
 * plan.
 * </p>
 * 
 * <p>
 * The predicate may be pushed down in one or more different ways. If the child expression is a type that
 * can directly absorb predicates (like a {@link LogicalFilterExpression} another {@link SelectExpression}),
 * then the immediate child will be rewritten with the additional predicates added. If the child expression
 * does not admit predicates but allows for the predicate to be pushed through (like a {@link LogicalUnionExpression}
 * or a {@link LogicalSortExpression}), then a new select box will be introduced below the child expression.
 * For example, something like:
 * </p>
 *
 * <pre>{@code
 *    +-----------------------+
 *    | SELECT q1.b AS b,     |
 *    |   q1.c AS c           |
 *    +-----------------------+
 *    | WHERE q1.a = 'x'      |
 *    +-----------------------+
 *              | q1
 *    +-----------------------+
 *    | ORDER BY _.d ASC      |
 *    +-----------------------+
 *              | q2
 *    +-----------------------+
 *    | SELECT a, b, c, d     |
 *    +-----------------------+
 *              |
 *             ...
 * }</pre>
 *
 * <p>
 * Will be rewritten as:
 * </p>
 *
 * <pre>{@code
 *    +-----------------------+
 *    | SELECT q1.b AS b,     |
 *    |   q1.c AS c           |
 *    +-----------------------+
 *              | q1
 *    +-----------------------+
 *    | ORDER BY _.d ASC      |
 *    +-----------------------+
 *              | q3
 *    +-----------------------+
 *    | SELECT q2.*           |
 *    +-----------------------+
 *    | WHERE q2.a = 'x'      |
 *    +-----------------------+
 *              | q2
 *    +-----------------------+
 *    | SELECT a, b, c, d     |
 *    +-----------------------+
 *              |
 *             ...
 * }</pre>
 *
 * <p>
 * In this case, the predicate on {@code a} (which has been rewritten from referencing {@code q1} to referencing
 * {@code q2}) can be further pushed down into {@code q2}, but that is done by further invocations of this rule.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PredicatePushDownRule extends AbstractCascadesRule<SelectExpression> implements ImplementationCascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<RelationalExpression> childExpressionMatcher = anyExpression();

    @Nonnull
    private static final BindingMatcher<ExpressionPartition<RelationalExpression>> childPartitionsMatcher =
            argmin(ExpressionsPartitionMatchers.<RelationalExpression>comparisonByPropertyList(
                    ExpressionCountProperty.selectCount(),
                    ExpressionCountProperty.tableFunctionCount(),
                    PredicateComplexityProperty.predicateComplexity()
            ), childExpressionMatcher);

    @Nonnull
    private static final BindingMatcher<Reference> childReferenceMatcher =
            expressionPartitions(rollUpPartitions(only(childPartitionsMatcher)));

    @Nonnull
    private static final CollectionMatcher<Quantifier.ForEach> quantifiersMatcher =
            atLeastOne(forEachQuantifierWithoutDefaultOnEmptyOverRef(childReferenceMatcher));

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(quantifiersMatcher).where(isFinalExpression());

    public PredicatePushDownRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);

        if (!selectExpression.getResultValue().getResultType().isRecord()) {
            return;
        }

        final var quantifiers = bindings.get(quantifiersMatcher);
        final var childExpressions = bindings.getAll(childExpressionMatcher);
        final var childExpressionByAlias =
                Streams.zip(quantifiers.stream(),
                                childExpressions.stream(),
                                (quantifier, expression) -> NonnullPair.of(quantifier.getAlias(), expression))
                        .collect(ImmutableMap.toImmutableMap(NonnullPair::getLeft, NonnullPair::getRight));

        //
        // The predicates that remain on the top-level select. We start with all of the select's predicates and remove
        // those that we manage to push down into a child as we iterate over the matched quantifiers.
        //
        final Set<QueryPredicate> residualPredicates =
                selectExpression.getPredicates().stream().collect(LinkedIdentitySet.toLinkedIdentitySet());

        final Map<CorrelationIdentifier, Quantifier> newQuantifiersByAlias = new LinkedHashMap<>();

        //
        // First pass: go over all matched quantifiers and, for each, push the residual predicates that are local to it
        // into its (simplest) chosen child expression. Successful pushes are collected into a map from alias to the
        // rewritten quantifier. If nothing could be pushed at all, there is nothing to do.
        //
        for (final var childExpressionEntry : childExpressionByAlias.entrySet()) {
            final var pushAlias = childExpressionEntry.getKey();
            final var childExpression = childExpressionEntry.getValue();
            //
            // Identify all residual predicates that are local to the quantifier we are pushing down along, i.e. all
            // predicates that are only correlated to this quantifier or deeply correlated to some other quantifier not
            // owned by this select expression.
            //
            final var otherAliases =
                    Quantifiers.aliases(() ->
                            selectExpression.getQuantifiers()
                                    .stream()
                                    .map(q -> (Quantifier)q)
                                    .filter(other -> !other.getAlias().equals(pushAlias))
                                    .iterator());

            final var pushablePredicates =
                    residualPredicates.stream()
                            .filter(queryPredicate ->
                                    queryPredicate.getCorrelatedTo().stream().noneMatch(otherAliases::contains))
                            .collect(LinkedIdentitySet.toLinkedIdentitySet());
            if (pushablePredicates.isEmpty()) {
                continue;
            }

            //
            // We do not consider all possible subsets of pushable predicates. There really is no need to and would be
            // overkill. Either we are unable to push any predicate into or through an expression, or we can push all.
            // The shape of the predicate should not matter considering the expression we push into/through.
            //
            final var pushToVisitor = new PushToVisitor(call, pushablePredicates, pushAlias);
            final var newBelowExpressionOptional = pushToVisitor.visit(childExpression);
            if (newBelowExpressionOptional.isEmpty()) {
                continue;
            }

            final Reference newRangesOverReference = call.memoizeFinalExpression(newBelowExpressionOptional.get());
            newQuantifiersByAlias.put(pushAlias,
                    Quantifier.forEachBuilder()
                            .withAlias(pushAlias)
                            .build(newRangesOverReference));
            residualPredicates.removeAll(pushablePredicates);
        }

        if (newQuantifiersByAlias.isEmpty()) {
            return;
        }

        //
        // Second pass: for every quantifier that we did not push into, add it to the map on a fresh, disentangled
        // reference. Afterwards the map holds a (rewritten or carried-over) quantifier for every alias.
        //
        for (final var quantifier : selectExpression.getQuantifiers()) {
            if (!newQuantifiersByAlias.containsKey(quantifier.getAlias())) {
                final Reference newChildReference =
                        call.memoizeFinalExpressionsFromOther(quantifier.getRangesOver(),
                                quantifier.getRangesOver().getFinalExpressions());
                newQuantifiersByAlias.put(quantifier.getAlias(), quantifier.overNewReference(newChildReference));
            }
        }

        final var newSelectExpression = new SelectExpression(selectExpression.getResultValue(),
                selectExpression.getQuantifiers()
                        .stream()
                        .map(quantifier -> newQuantifiersByAlias.get(quantifier.getAlias()))
                        .collect(ImmutableList.toImmutableList()),
                ImmutableList.copyOf(residualPredicates));

        call.yieldFinalExpression(newSelectExpression);
    }

    private static class PushToVisitor implements RelationalExpressionVisitorWithDefaults<Optional<? extends RelationalExpression>> {
        @Nonnull
        private final FinalMemoizer memoizer;
        @Nonnull
        private final Set<? extends QueryPredicate> originalPredicates;
        @Nonnull
        private final CorrelationIdentifier pushAlias;

        public PushToVisitor(@Nonnull FinalMemoizer memoizer,
                             @Nonnull final Set<? extends QueryPredicate> originalPredicates,
                             @Nonnull final CorrelationIdentifier pushAlias) {
            this.memoizer = memoizer;
            this.originalPredicates = originalPredicates;
            this.pushAlias = pushAlias;
        }

        @Nonnull
        private Set<? extends QueryPredicate> getOriginalPredicates() {
            return originalPredicates;
        }

        @Nonnull
        private CorrelationIdentifier getPushAlias() {
            return pushAlias;
        }

        @Nonnull
        private List<QueryPredicate> updatedPredicates(@Nonnull TranslationMap translationMap, @Nonnull Collection<? extends QueryPredicate> preExistingPredicates) {
            var predicatesBuilder = ImmutableList.<QueryPredicate>builderWithExpectedSize(getOriginalPredicates().size() + preExistingPredicates.size())
                    .addAll(preExistingPredicates);
            for (QueryPredicate originalPredicate : getOriginalPredicates()) {
                predicatesBuilder.add(originalPredicate.translateCorrelations(translationMap, true));
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
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(getPushAlias(),
                            child.getAlias()));
            final var newPredicates = updatedPredicates(translationMap);
            final SelectExpression newSelect = new SelectExpression(child.getFlowedObjectValue(), ImmutableList.of(child), newPredicates);
            return Quantifier.forEach(memoizer.memoizeFinalExpression(newSelect));
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
                    TranslationMap.rebaseWithAliasMap(AliasMap.ofAliases(getPushAlias(),
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
            final var translationMap = TranslationMap.regularBuilder()
                    .when(getPushAlias())
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
            // We have to be a little careful here. In particular, we can push down any predicates on a
            // grouping column, but not any on the aggregate value. For now, just don't push anything down
            return Optional.empty();
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
