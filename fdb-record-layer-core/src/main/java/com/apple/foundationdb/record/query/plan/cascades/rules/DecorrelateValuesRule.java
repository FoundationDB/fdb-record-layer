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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;

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

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> forEach = QuantifierMatchers.forEachQuantifier();

    @Nonnull
    private static final BindingMatcher<SelectExpression> root = RelationalExpressionMatchers.selectExpression(some(forEach));

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
        final SelectExpression selectExpression = call.get(root);
        final List<? extends Quantifier.ForEach> valueQunCandidates = call.getBindings().getAll(forEach);

        final Set<CorrelationIdentifier> selectChildQunIds = selectExpression.getQuantifiers().stream().map(Quantifier::getAlias).collect(ImmutableSet.toImmutableSet());
        ImmutableMap.Builder<CorrelationIdentifier, SelectExpression> valuesByIdBuilder = ImmutableMap.builderWithExpectedSize(valueQunCandidates.size());
        ImmutableMap.Builder<CorrelationIdentifier, Quantifier> qunsByIdBuilder = ImmutableMap.builderWithExpectedSize(valueQunCandidates.size());
        for (Quantifier.ForEach qun : valueQunCandidates) {
            @Nullable SelectExpression childSelect = findSelectForQuantifier(qun, selectChildQunIds);
            if (childSelect != null) {
                valuesByIdBuilder.put(qun.getAlias(), childSelect);
                qunsByIdBuilder.put(qun.getAlias(), qun);
            }
        }
        final Map<CorrelationIdentifier, SelectExpression> valuesById = valuesByIdBuilder.build();
        if (valuesById.isEmpty()) {
            // No actual values boxes here. Exit now
            return;
        }
        if (valuesById.size() == selectExpression.getQuantifiers().size()) {
            // All the quantifiers are values boxes. We can't push them around as this would
            // leave no children at all. So we exit here.
            return;
        }

        //
        // Create a translation map, and use it to translate the result value of this select as well as any predicates
        //
        final TranslationMap translationMap = createTranslationMapFromSelects(valuesById);
        final Value newResultValue = selectExpression.getResultValue().translateCorrelations(translationMap, true);
        final List<QueryPredicate> newPredicates = selectExpression.getPredicates().stream()
                .map(predicate -> predicate.translateCorrelations(translationMap, true))
                .collect(ImmutableList.toImmutableList());

        //
        // Push the values box into each child for which it is relevant.
        //
        final PushValuesIntoVisitor visitor = new PushValuesIntoVisitor(qunsByIdBuilder.build(), translationMap, call);
        ImmutableList.Builder<Quantifier> newQuantifiersBuilder = ImmutableList.builderWithExpectedSize(selectExpression.getQuantifiers().size() - valuesById.size());
        for (Quantifier qun : selectExpression.getQuantifiers()) {
            if (valuesById.containsKey(qun.getAlias())) {
                //
                // This is one of the values boxes that we're pushing down. Omit this box entirely from the top-level select
                //
                continue;
            }

            boolean anyChanged = false;
            ImmutableList.Builder<RelationalExpression> newExpressionsBuilder = ImmutableList.builderWithExpectedSize(qun.getRangesOver().getExploratoryExpressions().size());
            for (RelationalExpression lowerExpression : qun.getRangesOver().getExploratoryExpressions()) {
                if (correlatedToNone(lowerExpression, valuesById.keySet())) {
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

    @SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
    @Nullable
    private SelectExpression findSelectForQuantifier(@Nonnull Quantifier.ForEach qun,
                                                     @Nonnull Set<CorrelationIdentifier> selectChildQunIds) {
        // Make sure that the quantifier is over an acceptable value. For this to be the case, it should have a
        // select expression over a single quantifier. The returned SelectExpression's result value should not
        // contain references to any of the other children of the root select, nor should it be dependent on its input
        // quantifiers. In this way, it represents a pure "values box"
        for (RelationalExpression childExpr : qun.getRangesOver().getExploratoryExpressions()) {
            if (!(childExpr instanceof SelectExpression)) {
                continue;
            }
            final SelectExpression childSelect = (SelectExpression) childExpr;
            if (!childSelect.getPredicates().isEmpty()) {
                continue;
            }
            if (childSelect.getQuantifiers().size() != 1) {
                continue;
            }
            // Make sure that the select does not reference its underlying data stream, and that
            // it doesn't reference any of the sibling quantifiers of this select
            final Value childResultValue = childExpr.getResultValue();
            final Quantifier grandChildQuantifier = Iterables.getOnlyElement(childSelect.getQuantifiers());
            final Set<CorrelationIdentifier> childResultCorrelatedTo = childResultValue.getCorrelatedTo();
            if (childResultCorrelatedTo.contains(grandChildQuantifier.getAlias())
                    || !emptyIntersection(childResultCorrelatedTo, selectChildQunIds)) {
                continue;
            }

            // Validate that the quantifier is over a singleton range, like range(1)
            if (grandChildQuantifier.getRangesOver().getAllMemberExpressions()
                    .stream()
                    .filter(expr -> expr instanceof TableFunctionExpression)
                    .map(TableFunctionExpression.class::cast)
                    .noneMatch(tvf -> CardinalitiesProperty.Cardinalities.exactlyOne().equals(tvf.getValue().getCardinalities()))) {
                continue;
            }

            //
            // TODO: Check to make sure the values here can be moved around
            // There's an implicit assumption here that the value here can
            // moved and executed multiple times. That is to say, it's something
            // like a literal value or a field value, not a function like rand()
            // that return different results with each execution
            //
            // The select is a values box.
            return childSelect;
        }
        return null;
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
        private final Map<CorrelationIdentifier, Quantifier> valuesToPushDown;
        @Nonnull
        private final TranslationMap translationMap;
        @Nonnull
        private final ExploratoryMemoizer memoizer;

        public PushValuesIntoVisitor(@Nonnull Map<CorrelationIdentifier, Quantifier> valuesToPushDown,
                                     @Nonnull TranslationMap translationMap,
                                     @Nonnull ExploratoryMemoizer memoizer) {
            this.valuesToPushDown = valuesToPushDown;
            this.translationMap = translationMap;
            this.memoizer = memoizer;
        }

        @Nonnull
        private SelectExpression selectWithQuantifiersPushed(@Nonnull Set<CorrelationIdentifier> correlatedTo,
                                                             @Nonnull Value resultValue,
                                                             @Nonnull Collection<? extends Quantifier> quantifierBase,
                                                             @Nonnull List<? extends QueryPredicate> predicates) {
            final ImmutableList.Builder<Quantifier> newQuantifiers = ImmutableList.builderWithExpectedSize(quantifierBase.size() + valuesToPushDown.size());
            for (Quantifier qun : valuesToPushDown.values()) {
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
            if (correlatedToNone(childQun, valuesToPushDown.keySet())) {
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
