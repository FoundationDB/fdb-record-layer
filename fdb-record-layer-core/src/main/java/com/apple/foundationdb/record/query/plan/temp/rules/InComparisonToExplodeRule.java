/*
 * InComparisonToExplodeRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.ValueMatchers;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.anyComparisonOfType;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.selectExpression;

/**
 * TODO
 *
 * A simple rule that performs some basic Boolean normalization by flattening a nested {@link AndPredicate} into a single,
 * wider AND. This rule only attempts to remove a single {@code AndComponent}; it may be repeated if necessary.
 * For example, it would transform:
 * <code>
 *     Query.and(
 *         Query.and(Query.field("a").equals("foo"), Query.field("b").equals("bar")),
 *         Query.field(c").equals("baz"),
 *         Query.and(Query.field("d").equals("food"), Query.field("e").equals("bare"))
 * </code>
 * to
 * <code>
 *     Query.and(
 *         Query.field("a").equals("foo"),
 *         Query.field("b").equals("bar"),
 *         Query.field("c").equals("baz")),
 *         Query.and(Query.field("d").equals("food"), Query.field("e").equals("bare"))
 * </code>
 */
@API(API.Status.EXPERIMENTAL)
public class InComparisonToExplodeRule extends PlannerRule<SelectExpression> {
    private static final BindingMatcher<ValuePredicate> inPredicateMatcher =
            valuePredicate(ValueMatchers.anyValue(), anyComparisonOfType(Comparisons.Type.IN));
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier();

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(some(inPredicateMatcher), all(innerQuantifierMatcher));

    public InComparisonToExplodeRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final SelectExpression selectExpression = bindings.get(root);

        // we don't need iteration stability
        final List<? extends ValuePredicate> inPredicatesList = bindings.getAll(inPredicateMatcher);
        if (inPredicatesList.isEmpty()) {
            return;
        }

        final Set<QueryPredicate> inPredicates = Sets.newIdentityHashSet();
        inPredicates.addAll(inPredicatesList);

        final ImmutableList.Builder<Quantifier> transformedQuantifiers = ImmutableList.builder();

        final ImmutableList.Builder<QueryPredicate> transformedPredicates = ImmutableList.builder();
        for (final QueryPredicate predicate : selectExpression.getPredicates()) {
            if (inPredicates.contains(predicate)) {
                final ValuePredicate valuePredicate = (ValuePredicate)predicate;
                final Comparisons.Comparison comparison = valuePredicate.getComparison();
                Verify.verify(comparison.getType() == Comparisons.Type.IN);
                final ExplodeExpression explodeExpression;
                if (comparison instanceof Comparisons.ListComparison) {
                    explodeExpression = new ExplodeExpression(new LiteralValue<>(comparison.getComparand()));
                } else if (comparison instanceof Comparisons.ParameterComparison) {
                    explodeExpression = new ExplodeExpression(QuantifiedColumnValue.of(CorrelationIdentifier.of(((Comparisons.ParameterComparison)comparison).getParameter()), 0));
                } else {
                    throw new RecordCoreException("unknown in comparison");
                }

                final Quantifier.ForEach newQuantifier = Quantifier.forEach(GroupExpressionRef.of(explodeExpression));
                transformedPredicates.add(
                        new ValuePredicate(((ValuePredicate)predicate).getValue(),
                                new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, newQuantifier.getAlias().toString())));
                transformedQuantifiers.add(newQuantifier);
            } else {
                transformedPredicates.add(predicate);
            }
        }

        transformedQuantifiers.addAll(bindings.getAll(innerQuantifierMatcher));

        call.yield(call.ref(new SelectExpression(selectExpression.getResultValues(),
                transformedQuantifiers.build(),
                transformedPredicates.build())));
    }
}
