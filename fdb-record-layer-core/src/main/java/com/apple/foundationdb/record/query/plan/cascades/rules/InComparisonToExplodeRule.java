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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyComparisonOfType;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

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
 * constructs such as {@link NotPredicate} as the
 * transformed expression.
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
public class InComparisonToExplodeRule extends CascadesRule<SelectExpression> {
    private static final BindingMatcher<ValuePredicate> inPredicateMatcher =
            valuePredicate(ValueMatchers.anyValue(), anyComparisonOfType(Comparisons.Type.IN));
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier();

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(some(inPredicateMatcher), all(innerQuantifierMatcher));

    public InComparisonToExplodeRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();

        final var selectExpression = bindings.get(root);

        final var inPredicatesList = bindings.getAll(inPredicateMatcher);
        if (inPredicatesList.isEmpty()) {
            return;
        }

        final var inPredicates = Sets.<QueryPredicate>newIdentityHashSet();
        inPredicates.addAll(inPredicatesList);

        final var transformedQuantifiers = ImmutableList.<Quantifier>builder();
        final var transformedPredicates = ImmutableList.<QueryPredicate>builder();
        for (final var predicate : selectExpression.getPredicates()) {
            if (inPredicates.contains(predicate)) {
                final var valuePredicate = (ValuePredicate)predicate;
                final var value = valuePredicate.getValue();

                final Type elementType = value.getResultType();
                final var comparison = valuePredicate.getComparison();
                Verify.verify(comparison.getType() == Comparisons.Type.IN);
                final ExplodeExpression explodeExpression;
                final Quantifier.ForEach newQuantifier;

                if (comparison instanceof Comparisons.ValueComparison) {
                    final var comparisonValue = (Comparisons.ValueComparison)comparison;
                    Verify.verify(comparisonValue.getComparandValue().getResultType().isArray());
                    Type arrayElementType = ((Type.Array) comparisonValue.getComparandValue().getResultType()).getElementType();
                    Verify.verify(arrayElementType != null);
                    explodeExpression = new ExplodeExpression(comparisonValue.getComparandValue());
                    newQuantifier = Quantifier.forEach(call.memoizeExpression(explodeExpression));
                    if (arrayElementType.isRecord()) {
                        transformedPredicates.addAll(createSimpleEqualities(value, newQuantifier));
                    } else {
                        transformedPredicates.add(new ValuePredicate(value,
                                new Comparisons.ValueComparison(Comparisons.Type.EQUALS, QuantifiedObjectValue.of(newQuantifier.getAlias(), elementType))));
                    }
                } else if (comparison instanceof Comparisons.ListComparison) {
                    final var listComparison = (Comparisons.ListComparison)comparison;
                    explodeExpression = new ExplodeExpression(LiteralValue.ofList((List<?>)listComparison.getComparand(null, null)));
                    newQuantifier = Quantifier.forEach(call.memoizeExpression(explodeExpression));
                    transformedPredicates.add(new ValuePredicate(value,
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, QuantifiedObjectValue.of(newQuantifier.getAlias(), elementType))));

                } else if (comparison instanceof Comparisons.ParameterComparison) {
                    explodeExpression = new ExplodeExpression(QuantifiedObjectValue.of(CorrelationIdentifier.of(((Comparisons.ParameterComparison)comparison).getParameter()), new Type.Array(elementType)));
                    newQuantifier = Quantifier.forEach(call.memoizeExpression(explodeExpression));
                    transformedPredicates.add(new ValuePredicate(value,
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, QuantifiedObjectValue.of(newQuantifier.getAlias(), elementType))));
                } else {
                    throw new RecordCoreException("unknown in comparison " + comparison.getClass().getSimpleName());
                }
                transformedQuantifiers.add(newQuantifier);
            } else {
                transformedPredicates.add(predicate);
            }
        }

        transformedQuantifiers.addAll(bindings.getAll(innerQuantifierMatcher));

        call.yieldExpression(new SelectExpression(selectExpression.getResultValue(),
                transformedQuantifiers.build(),
                transformedPredicates.build()));
    }

    /**
     * This method creates an equality predicate for all constituent parts of a tuple.
     */
    @Nonnull
    private static List<QueryPredicate> createSimpleEqualities(@Nonnull final Value value,
                                                               @Nonnull final Quantifier.ForEach newQuantifier) {
        List<Value> valueChildren = ImmutableList.copyOf(value.getChildren());
        List<Column<? extends FieldValue>> comparandValueChildren = newQuantifier.getFlowedColumns();
        Verify.verify(valueChildren.size() == comparandValueChildren.size());
        final var resultsBuilder = ImmutableList.<QueryPredicate>builder();
        for (int i = 0; i < valueChildren.size(); i++) {
            BooleanValue currentVal = (BooleanValue) new RelOpValue.EqualsFn().encapsulate(List.of(valueChildren.get(i), comparandValueChildren.get(i).getValue()));
            Optional<QueryPredicate> currentQueryPredicate = currentVal.toQueryPredicate(null, Quantifier.current());
            Verify.verify(currentQueryPredicate.isPresent());
            resultsBuilder.add(currentQueryPredicate.get());
        }
        return resultsBuilder.build();
    }
}
