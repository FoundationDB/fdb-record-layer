/*
 * InComparisonToExplodeRule.java
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
import com.apple.foundationdb.record.Bindings;
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
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
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
 * A rule that traverses a predicate in a {@link SelectExpression} and attempts to extract IN-comparisons into
 * separate {@link ExplodeExpression}s, e.g.
 *
 * <pre>
 * {@code
 * SELECT ...
 * FROM T
 * WHERE t.a IN (1, 2, 3)
 * }
 * </pre>
 *
 * is transformed to
 *
 * <pre>
 * {@code
 * SELECT ...
 * FROM T, EXPLODE(LIST(1, 2, 3)) AS e
 * WHERE t.a = e
 * }
 * </pre>
 *
 *
 * The transformation cannot be done for a class of predicates that contain an IN within incompatible
 * constructs such as {@link com.apple.foundationdb.record.query.predicates.NotPredicate} as the transformed expression
 *
 * <pre>
 * {@code
 * Not(num_value_3_indexed EQUALS $__in_num_value_3_indexed__0) is not producing the correct result.
 * }
 * </pre>
 *
 * <em>not</em> is one of the cases that can cause this.
 *
 * In general for any possible record {@code  r} the following should hold:
 *
 * <pre>
 * {@code
 * for any v in IN-list: transformedPredicate(v) implies original IN-list predicate
 * }
 * </pre>
 *
 * or in other words
 *
 * <pre>
 * {@code
 * if ∃ v in IN-list such that transformedPredicate(v) is true then original IN-list predicate is true
 * }
 * </pre>
 *
 * In the <em>not</em> case:
 *
 * <pre>
 * {@code
 * there is no v in (1, 4, 2) such that not(num_value_3_indexed = v) implies not(num_value_3_indexed in (1, 4, 2))
 * }
 * </pre>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
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
                    // TODO this needs to resolve the proper type
                    explodeExpression = new ExplodeExpression(QuantifiedObjectValue.of(CorrelationIdentifier.of(((Comparisons.ParameterComparison)comparison).getParameter())));
                } else {
                    throw new RecordCoreException("unknown in comparison " + comparison.getClass().getSimpleName());
                }

                final Quantifier.ForEach newQuantifier = Quantifier.forEach(GroupExpressionRef.of(explodeExpression));
                transformedPredicates.add(
                        new ValuePredicate(((ValuePredicate)predicate).getValue(),
                                new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, Bindings.Internal.CORRELATION.bindingName(newQuantifier.getAlias().toString()), Bindings.Internal.CORRELATION)));
                transformedQuantifiers.add(newQuantifier);
            } else {
                transformedPredicates.add(predicate);
            }
        }

        transformedQuantifiers.addAll(bindings.getAll(innerQuantifierMatcher));

        call.yield(call.ref(new SelectExpression(selectExpression.getResultValue(),
                transformedQuantifiers.build(),
                transformedPredicates.build())));
    }
}
