/*
 * DecorrelateValuesRule.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.ExploratoryMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RangeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.RegularTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher.empty;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierWithoutDefaultOnEmptyOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.exploratoryMember;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.isExploratoryExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithPredicate.typedMatcherWithPredicate;

/**
 * Rule to de-correlate any "values boxes" by pushing them into referencing expressions. In this case, a
 * "values box" is a special kind of {@link SelectExpression} that is over a {@code range(1)}
 * expression and which returns values that are uncorrelated to its child. These kinds of expressions
 * are generated during the in-lining of parameterized functions. Generally, an expression like:
 *
 * <pre>{@code
 * CREATE FUNCTION foo(x bigint, y string)
 *    AS SELECT c, d FROM T WHERE a = x AND b = y;
 * SELECT d FROM foo(42, 'hello') WHERE c IS NULL
 * }</pre>
 *
 * <p>
 * Will be expressed in the query graph as something approximating:
 * </p>
 *
 * <pre>{@code
 * SELECT f.d
 *   FROM (SELECT 42 AS x, 'hello' as y FROM range(1)) p,
 *        (SELECT c, d FROM T WHERE a = p.x AND b = p.y) f
 *   WHERE f.c IS NULL
 * }</pre>
 *
 * <p>
 * That is, the parameters are expressed on the left-hand side of the join, and the function body is now
 * in the right-hand side with correlations leading to its value box.
 * </p>
 *
 * <p>
 * This rule will then help facilitate value in-lining, in that it will detect cases where a select expression
 * has a child that is a values box and then re-write any correlated children. So, in the above expression,
 * it can push down the values box one level, giving us:
 * </p>
 *
 * <pre>{@code
 * SELECT f.d
 *   FROM (SELECT c, d
 *           FROM (SELECT 42 AS x, 'hello' as y FROM range(1)) p,
 *                T
 *           WHERE a = p.x AND b = p.y
 *   ) f
 *   WHERE f.c IS NULL
 * }</pre>
 *
 * <p>
 * The new child is also eligible for value decorrelation, leading to the expression:
 * </p>
 *
 * <pre>{@code
 * SELECT f.d
 *   FROM (SELECT c, d
 *           FROM T
 *           WHERE a = 42 AND b = 'hello'
 *   ) f
 *   WHERE f.c IS NULL
 * }</pre>
 *
 * <p>
 * At this point, this rule is done, but further straightforward selection merging can rewrite the expression
 * as:
 * </p>
 *
 * <pre>{@code
 * SELECT T.d
 *   FROM T
 *   WHERE a = 42 AND b = 'hello' AND c IS NULL
 * }</pre>
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class DecorrelateValuesRule extends ExplorationCascadesRule<SelectExpression> {
    // TODO: This could use filtered expression partitions, but we have to make modifications to the test infrastructure to ensure there are final children
    // We currently use a predicate over the expressions in the reference rather than a matcher here because we don't
    // want to create multiple matches if there happens to be a reference containing multiple range(1) values. Doing
    // so would result in the rule being run multiple times with the same input
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> rangeOneMatcher = forEachQuantifierWithoutDefaultOnEmptyOverRef(
            typedMatcherWithPredicate(Reference.class,
                    ref -> ref.getAllMemberExpressions().stream().anyMatch(expr -> expr instanceof TableFunctionExpression && CardinalitiesProperty.Cardinalities.exactlyOne().equals(CardinalitiesProperty.cardinalities().evaluate(expr))))
    );

    // Conditions for a "values box":
    //  1. Select expression over a single for each quantifier with no predicates.
    //      -> Validated by the first matcher's structure
    //  2. The child for each has a cardinality of exactly 1
    //      -> Validated by the downstream baseExpressionMatcher
    //  3. The select's result value is not correlated to its input
    //      -> Validated by the predicate in the matcher below
    //
    // Note: this currently matches each values box. That means that if we have multiple variations,
    // we'll end up pushing down the values box for each variation. We may want to have this
    // choose the "best" variation to avoid over-exploration
    @Nonnull
    private static final BindingMatcher<SelectExpression> valuesExpressionMatcher =
            selectExpression(empty(), only(rangeOneMatcher)).where(typedMatcherWithPredicate(SelectExpression.class,
                    expr -> {
                        // Values boxes' return value should not be correlated to its own child expression
                        final Quantifier childQun = Iterables.getOnlyElement(expr.getQuantifiers());
                        final Value resultValue = expr.getResultValue();
                        return !resultValue.isCorrelatedTo(childQun.getAlias());
                    }));

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> valuesQunMatcher = forEachQuantifierWithoutDefaultOnEmptyOverRef(exploratoryMember(valuesExpressionMatcher));

    // Match a select expression over the values boxes. Ideally, we'd also check each box's correlation sets to validate that
    // we don't match any that have references out to sibling quantifiers in the SelectExpression's root. However,
    // that's difficult to express with quantifiers, so in onMatch, we'll only select the subset without such correlations
    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression(some(valuesQunMatcher)).where(isExploratoryExpression());

    public DecorrelateValuesRule() {
        super(root);
    }

    private static <T> boolean emptyIntersection(@Nonnull Set<? extends T> set1, @Nonnull Set<? super T> set2) {
        return set1.stream().noneMatch(set2::contains);
    }

    private static boolean correlatedToNone(@Nonnull RelationalExpression expression, @Nonnull Set<CorrelationIdentifier> aliases) {
        return emptyIntersection(expression.getCorrelatedTo(), aliases);
    }

    private static boolean correlatedToNone(@Nonnull Quantifier qun, @Nonnull Set<CorrelationIdentifier> aliases) {
        return emptyIntersection(qun.getCorrelatedTo(), aliases);
    }

    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final List<? extends Quantifier.ForEach> valueQunCandidates = call.getBindings().getAll(valuesQunMatcher);
        if (valueQunCandidates.isEmpty()) {
            return;
        }
        final SelectExpression selectExpression = call.get(root);
        final Set<CorrelationIdentifier> childAliases = Quantifiers.aliases(selectExpression.getQuantifiers());
        final List<? extends SelectExpression> valuesExpressions = call.getBindings().getAll(valuesExpressionMatcher);

        final ImmutableMap.Builder<CorrelationIdentifier, SelectExpression> valuesByAlias = ImmutableMap.builderWithExpectedSize(valuesExpressions.size());
        final ImmutableMap.Builder<CorrelationIdentifier, Quantifier> qunsToPushDownBuilder = ImmutableMap.builderWithExpectedSize(valueQunCandidates.size());
        Streams.zip(valueQunCandidates.stream(), valuesExpressions.stream(), NonnullPair::of)
                //
                // Reject any values boxes here that have correlations to their siblings.
                // This is to prevent us from pushing down a values box that is sideways
                // correlated to another values box, which might lead to problems if they are
                // both pushed down in the same pass. Note that other expressions in the
                // quantifier may not have the same correlation set, so we may need to trim some
                //
                .filter(pair -> correlatedToNone(pair.getRight(), childAliases))
                .forEach(pair -> {
                    final Quantifier qun = pair.getLeft();
                    final SelectExpression valueExpression = pair.getRight();
                    final CorrelationIdentifier alias = qun.getAlias();
                    Verify.verify(qun.getRangesOver().containsExactly(valueExpression), "matched value box should be aligned with matched quantifier");
                    valuesByAlias.put(alias, valueExpression);
                    if (correlatedToNone(qun, childAliases)) {
                        // There are no expressions within the quantifier that have any siblings correlations.
                        // We can push down the entire quantifier
                        qunsToPushDownBuilder.put(alias, qun);
                    } else {
                        // There is at least one expression in the quantifier that is correlated to one of the
                        // siblings. Extract out only the variations that are not correlated to a sibling
                        Set<RelationalExpression> uncorrelatedExpressions = qun.getRangesOver().getExploratoryExpressions().stream()
                                .filter(expr -> correlatedToNone(expr, childAliases))
                                .collect(LinkedIdentitySet.toLinkedIdentitySet());
                        Verify.verify(uncorrelatedExpressions.contains(valueExpression), "target value expression should be in the uncorrelated subset of quantifier expressions");
                        Quantifier newQun = qun.overNewReference(call.memoizeExploratoryExpressions(uncorrelatedExpressions));
                        qunsToPushDownBuilder.put(alias, newQun);
                    }
                });

        final Map<CorrelationIdentifier, Quantifier> qunsToPushDownByAlias = qunsToPushDownBuilder.build();
        if (qunsToPushDownByAlias.isEmpty()) {
            // No actual values boxes here. Exit now
            return;
        }

        //
        // Create a translation map, and use it to translate the result value of this select as well as any predicates
        //
        final TranslationMap translationMap = createTranslationMapFromSelects(valuesByAlias.build());
        final Value newResultValue = selectExpression.getResultValue().translateCorrelations(translationMap, true);
        final List<QueryPredicate> newPredicates = selectExpression.getPredicates().stream()
                .map(predicate -> predicate.translateCorrelations(translationMap, true))
                .collect(ImmutableList.toImmutableList());

        //
        // Push the values box into each child for which it is relevant.
        //
        final PushValuesIntoVisitor visitor = new PushValuesIntoVisitor(qunsToPushDownByAlias, translationMap, call);
        ImmutableList.Builder<Quantifier> newQuantifiersBuilder = ImmutableList.builderWithExpectedSize(Math.max(1, selectExpression.getQuantifiers().size() - qunsToPushDownByAlias.size()));
        if (selectExpression.getQuantifiers().size() == qunsToPushDownByAlias.size()) {
            //
            // We're about to push down all the quantifiers. Introduce a range(1) box here to avoid creating a Select with no children
            //
            TableFunctionExpression rangeOneExpr = new TableFunctionExpression((StreamingValue) new RangeValue.RangeFn().encapsulate(ImmutableList.of(LiteralValue.ofScalar(1L))));
            Quantifier newRangeQun = Quantifier.forEach(call.memoizeExploratoryExpression(rangeOneExpr));
            newQuantifiersBuilder.add(newRangeQun);
        }
        for (Quantifier qun : selectExpression.getQuantifiers()) {
            if (qunsToPushDownByAlias.containsKey(qun.getAlias())) {
                //
                // This is one of the values boxes that we're pushing down. Omit this box entirely from the top-level select
                //
                continue;
            }

            boolean anyChanged = false;
            ImmutableList.Builder<RelationalExpression> newExpressionsBuilder = ImmutableList.builderWithExpectedSize(qun.getRangesOver().getExploratoryExpressions().size());
            for (RelationalExpression lowerExpression : qun.getRangesOver().getExploratoryExpressions()) {
                // Generate a new lower expression for each lower expression
                // We do this even if the lower expression is uncorrelated because if
                // some (but not all) of the expressions are rewritten, then if we don't
                // copy over the expression, it can end up in multiple (non-memoized) references
                final Set<CorrelationIdentifier> lowerCorrelatedTo = qunsToPushDownByAlias.keySet().stream()
                        .filter(lowerExpression::isCorrelatedTo)
                        .collect(ImmutableSet.toImmutableSet());
                if (lowerCorrelatedTo.isEmpty()) {
                    newExpressionsBuilder.add(lowerExpression);
                } else {
                    newExpressionsBuilder.add(visitor.visit(lowerExpression));
                    anyChanged = true;
                }
            }
            if (anyChanged) {
                // Some expression has changed. Create a new quantifier over the new expressions
                Reference newRef = call.memoizeExploratoryExpressions(newExpressionsBuilder.build());
                Quantifier newQun = qun.overNewReference(newRef);
                newQuantifiersBuilder.add(newQun);
            } else {
                // No expression was correlated. Put in the original quantifier
                newQuantifiersBuilder.add(qun);
            }
        }
        SelectExpression exprToYield = new SelectExpression(newResultValue, newQuantifiersBuilder.build(), newPredicates);
        call.yieldExploratoryExpression(exprToYield);
    }

    @Nonnull
    private TranslationMap createTranslationMapFromSelects(@Nonnull Map<CorrelationIdentifier, SelectExpression> valuesById) {
        RegularTranslationMap.Builder translationBuilder = TranslationMap.regularBuilder();
        valuesById.forEach((id, childSelect) ->
                translationBuilder
                        .when(id)
                        .then((source, leaf) -> childSelect.getResultValue()));
        return translationBuilder.build();
    }

    private static final class PushValuesIntoVisitor implements RelationalExpressionVisitorWithDefaults<RelationalExpression> {
        @Nonnull
        private final Map<CorrelationIdentifier, Quantifier> qunsToPushDown;
        @Nonnull
        private final TranslationMap translationMap;
        @Nonnull
        private final ExploratoryMemoizer memoizer;

        public PushValuesIntoVisitor(@Nonnull Map<CorrelationIdentifier, Quantifier> qunsToPushDown,
                                     @Nonnull TranslationMap translationMap,
                                     @Nonnull ExploratoryMemoizer memoizer) {
            this.qunsToPushDown = qunsToPushDown;
            this.translationMap = translationMap;
            this.memoizer = memoizer;
        }

        @Nonnull
        private SelectExpression selectWithQuantifiersPushed(@Nonnull Set<CorrelationIdentifier> correlatedTo,
                                                             @Nonnull Value resultValue,
                                                             @Nonnull Collection<? extends Quantifier> quantifierBase,
                                                             @Nonnull List<? extends QueryPredicate> predicates) {
            final ImmutableList.Builder<Quantifier> newQuantifiers = ImmutableList.builderWithExpectedSize(quantifierBase.size() + qunsToPushDown.size());
            for (Quantifier qun : qunsToPushDown.values()) {
                if (correlatedTo.contains(qun.getAlias())) {
                    newQuantifiers.add(qun);
                }
            }
            newQuantifiers.addAll(quantifierBase);
            return new SelectExpression(resultValue, newQuantifiers.build(), predicates);
        }

        @Nonnull
        @Override
        public SelectExpression visitSelectExpression(@Nonnull final SelectExpression select) {
            return selectWithQuantifiersPushed(
                    select.getCorrelatedTo(),
                    select.getResultValue(),
                    select.getQuantifiers(),
                    select.getPredicates()
            );
        }

        @Nonnull
        @Override
        public SelectExpression visitLogicalFilterExpression(@Nonnull final LogicalFilterExpression filter) {
            return selectWithQuantifiersPushed(
                    filter.getCorrelatedTo(),
                    filter.getResultValue(),
                    filter.getQuantifiers(),
                    filter.getPredicates()
            );
        }

        @Nonnull
        private Quantifier pushOnTopOfQuantifier(@Nonnull final Quantifier childQun) {
            //
            // First, check if there are any correlations that need to be pushed down. If not,
            // return the original quantifier
            //
            if (correlatedToNone(childQun, qunsToPushDown.keySet())) {
                return childQun;
            }

            //
            // Create a for-each quantifier over the same set of expressions (and with the same alias)
            // as the original child qun.
            //
            final Quantifier newChild = Quantifier.forEach(childQun.getRangesOver());
            SelectExpression newSelect = selectWithQuantifiersPushed(
                    newChild.getCorrelatedTo(),
                    newChild.getFlowedObjectValue(),
                    ImmutableList.of(newChild),
                    ImmutableList.of());
            //
            // Replace the reference with one over the new expression
            //
            final Reference ref = memoizer.memoizeExploratoryExpression(newSelect);
            return childQun.overNewReference(ref);
        }

        @Nonnull
        @Override
        public RelationalExpression visitDefault(@Nonnull final RelationalExpression expression) {
            //
            // By default, we rewrite all the child quantifiers so that the pushed down values
            // boxes are incorporated on top of their children. We only push the ones that are
            // relevant to each child (and leave any uncorrelated quantifiers alone)
            //
            final ImmutableList.Builder<Quantifier> newQuantifiers = ImmutableList.builderWithExpectedSize(expression.getQuantifiers().size());
            for (Quantifier qun : expression.getQuantifiers()) {
                Quantifier newQun = pushOnTopOfQuantifier(qun);
                newQuantifiers.add(newQun);
            }
            return expression.translateCorrelations(translationMap, true, newQuantifiers.build());
        }
    }
}
