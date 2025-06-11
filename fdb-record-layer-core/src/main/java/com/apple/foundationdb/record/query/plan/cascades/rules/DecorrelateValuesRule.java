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

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * Rule to de-correlate any "values boxes" by pushing them into referencing expressions. In this case, a
 * "values box" is a special kind of {@link SelectExpression} that is over a {@code range(1)}
 * expression and which returns values that are uncorrelated to its child. These kinds of expressions
 * are generated during the in-lining of parameterized functions. Generally, an expression like:
 *
 * <pre>{@code
 * CREATE FUNCTION foo(x bigint, y string)
 *    AS SELECT c, b FROM T WHERE a = x AND b = y;
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
public class DecorrelateValuesRule extends ExplorationCascadesRule<SelectExpression> {
    // TODO: This could use filtered expression partitions, but we have to make modifications to the test infrastructure to ensure there are final children
    @Nonnull
    private static final BindingMatcher<RelationalExpression> baseExpressionMatcher = TypedMatcherWithPredicate.typedMatcherWithPredicate(RelationalExpression.class,
            expr -> CardinalitiesProperty.Cardinalities.exactlyOne().equals(CardinalitiesProperty.cardinalities().evaluate(expr)));

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> childForEachMatcher = forEachQuantifier(baseExpressionMatcher);

    @Nonnull
    private static final BindingMatcher<SelectExpression> valuesExpressionMatcher = new TypedMatcher<>(SelectExpression.class) {
        @Nonnull
        @Override
        public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final SelectExpression in) {
            if (in.getQuantifiers().size() != 1 || !in.getPredicates().isEmpty()) {
                // Values boxes should be over only a single child and should have no predicates
                return Stream.of();
            }
            final Quantifier childQun = Iterables.getOnlyElement(in.getQuantifiers());
            final Value resultValue = in.getResultValue();
            if (resultValue.isCorrelatedTo(childQun.getAlias())) {
                // Values boxes should not be correlated to their own child expression
                return Stream.of();
            }
            // Note: we can only _really_ pick an expression here if we are un-correlated to our
            // siblings. However, that's hard to check at this point, so we'll have to check that later
            // in onMatch
            final PlannerBindings selectBoundBindings = PlannerBindings.from(this, in);
            return childForEachMatcher.bindMatches(plannerConfiguration, outerBindings, childQun)
                    .map(selectBoundBindings::mergedWith);
        }
    };

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> valuesQunMatcher = forEachQuantifier(valuesExpressionMatcher);

    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression(some(valuesQunMatcher));

    public DecorrelateValuesRule() {
        super(root);
    }

    private static <T> boolean emptyIntersection(@Nonnull Set<? extends T> set1, @Nonnull Set<? super T> set2) {
        return set1.stream().noneMatch(set2::contains);
    }

    private static boolean correlatedToNone(@Nonnull RelationalExpression expression, @Nonnull Set<CorrelationIdentifier> ids) {
        return emptyIntersection(expression.getCorrelatedTo(), ids);
    }

    private static boolean correlatedToNone(@Nonnull Quantifier qun, @Nonnull Set<CorrelationIdentifier> ids) {
        return emptyIntersection(qun.getCorrelatedTo(), ids);
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
        final Set<SelectExpression> valueExpressionSet = new LinkedIdentitySet<>(valuesExpressions);
        final Map<CorrelationIdentifier, SelectExpression> valuesByAlias = new HashMap<>();
        final ImmutableMap.Builder<CorrelationIdentifier, Quantifier> qunsToPushDownBuilder = ImmutableMap.builderWithExpectedSize(valueQunCandidates.size());
        for (Quantifier.ForEach valueQunCandidate : valueQunCandidates) {
            if (!correlatedToNone(valueQunCandidate, childAliases)) {
                //
                // Reject any quantifiers here that have correlations to their siblings
                //
                continue;
            }
            for (RelationalExpression expr : valueQunCandidate.getRangesOver().getAllMemberExpressions()) {
                //
                // Find the matched select expression(s) that correspond to this matched quantifier.
                // Pick the first one found that doesn't improperly correlate to one of the sibling quantifiers
                //
                if (expr instanceof SelectExpression
                        && valueExpressionSet.contains(expr)
                        && correlatedToNone(selectExpression, childAliases)) {
                    valuesByAlias.putIfAbsent(valueQunCandidate.getAlias(), (SelectExpression) expr);
                    qunsToPushDownBuilder.put(valueQunCandidate.getAlias(), valueQunCandidate);
                    break;
                }
            }
        }

        final Map<CorrelationIdentifier, Quantifier> qunsToPushDownByAlias = qunsToPushDownBuilder.build();
        if (qunsToPushDownByAlias.isEmpty()) {
            // No actual values boxes here. Exit now
            return;
        }
        if (qunsToPushDownByAlias.size() == selectExpression.getQuantifiers().size()) {
            // All the quantifiers are values boxes. We can't push them around as this would
            // leave no children at all. So we exit here.
            return;
        }

        //
        // Create a translation map, and use it to translate the result value of this select as well as any predicates
        //
        final TranslationMap translationMap = createTranslationMapFromSelects(valuesByAlias);
        final Value newResultValue = selectExpression.getResultValue().translateCorrelations(translationMap, true);
        final List<QueryPredicate> newPredicates = selectExpression.getPredicates().stream()
                .map(predicate -> predicate.translateCorrelations(translationMap, true))
                .collect(ImmutableList.toImmutableList());

        //
        // Push the values box into each child for which it is relevant.
        //
        final PushValuesIntoVisitor visitor = new PushValuesIntoVisitor(qunsToPushDownByAlias, translationMap, call);
        ImmutableList.Builder<Quantifier> newQuantifiersBuilder = ImmutableList.builderWithExpectedSize(selectExpression.getQuantifiers().size() - qunsToPushDownByAlias.size());
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
                if (correlatedToNone(lowerExpression, valuesByAlias.keySet())) {
                    newExpressionsBuilder.add(lowerExpression);
                } else {
                    anyChanged = true;
                    newExpressionsBuilder.add(visitor.visit(lowerExpression));
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
        call.yieldExploratoryExpression(new SelectExpression(newResultValue, newQuantifiersBuilder.build(), newPredicates));
    }

    @Nonnull
    private TranslationMap createTranslationMapFromSelects(@Nonnull Map<CorrelationIdentifier, SelectExpression> valuesById) {
        TranslationMap.Builder translationBuilder = TranslationMap.builder();
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
