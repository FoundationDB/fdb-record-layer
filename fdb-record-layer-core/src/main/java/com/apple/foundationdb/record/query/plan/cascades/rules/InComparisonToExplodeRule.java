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
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.checkerframework.checker.units.qual.A;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyComparisonOfType;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.explodeExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughInLikeSelectRule.findInnerQuantifier;

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
 * constructs such as {@link com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate} as the
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
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final CollectionMatcher<Quantifier.ForEach> explodeQuantifiersMatcher = some(forEachQuantifier(explodeExpressionMatcher));

    private static final BindingMatcher<ValuePredicate> inPredicateMatcher =
            valuePredicate(ValueMatchers.anyValue(), anyComparisonOfType(Comparisons.Type.IN));
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier();

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(some(inPredicateMatcher), all(innerQuantifierMatcher));

    private static final BindingMatcher<SelectExpression> root2 =
            selectExpression(explodeQuantifiersMatcher);

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

                if (comparison instanceof Comparisons.ValueComparison) {
                    final var comparisonValue = (Comparisons.ValueComparison)comparison;
                    Verify.verify(comparisonValue.getComparandValue().getResultType().isArray());
                    Type arrayElementType = ((Type.Array) comparisonValue.getComparandValue().getResultType()).getElementType();

                    if (arrayElementType.isRecord()) {
                        // explodeExpression should be promoted?
                        Value promotedComparandValue = promoteComparandValue(value, comparisonValue.getComparandValue());
                        System.out.println("promotedComparandValue elementType:" + ((Type.Array)promotedComparandValue.getResultType()).getElementType());
                        explodeExpression = new ExplodeExpression(promotedComparandValue);
                        final Quantifier.ForEach newQuantifier = Quantifier.forEach(call.memoizeExpression(explodeExpression));
                        QueryPredicate queryPredicate = findNewValuePredicate(value, newQuantifier);
                        transformedPredicates.add(queryPredicate);
                        transformedQuantifiers.add(newQuantifier);
                        System.out.println("new queryPredicate:" + queryPredicate);
                    } else {
                        System.out.println("value:" + value);
                        System.out.println("comparisonValue:" + comparisonValue + " comparandValue:" + comparisonValue.getComparandValue());
                        explodeExpression = new ExplodeExpression(comparisonValue.getComparandValue());
                        final Quantifier.ForEach newQuantifier = Quantifier.forEach(call.memoizeExpression(explodeExpression));
                        ValuePredicate newValuePredicate = new ValuePredicate(value,
                                new Comparisons.ValueComparison(Comparisons.Type.EQUALS, QuantifiedObjectValue.of(newQuantifier.getAlias(), elementType)));
                        transformedPredicates.add(newValuePredicate);
                        transformedQuantifiers.add(newQuantifier);
                    }
                } else if (comparison instanceof Comparisons.ListComparison) {
                    System.out.println("Comparison instance of ListComparison");
                    final var listComparison = (Comparisons.ListComparison)comparison;
                    explodeExpression = new ExplodeExpression(LiteralValue.ofList((List<?>)listComparison.getComparand(null, null)));
                    final Quantifier.ForEach newQuantifier = Quantifier.forEach(call.memoizeExpression(explodeExpression));
                    ValuePredicate newValuePredicate = new ValuePredicate(value,
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, QuantifiedObjectValue.of(newQuantifier.getAlias(), elementType)));
                    transformedPredicates.add(newValuePredicate);
                    transformedQuantifiers.add(newQuantifier);
                } else if (comparison instanceof Comparisons.ParameterComparison) {
                    System.out.println("Comparison instance of ParameterComparison");
                    explodeExpression = new ExplodeExpression(QuantifiedObjectValue.of(CorrelationIdentifier.of(((Comparisons.ParameterComparison)comparison).getParameter()), new Type.Array(elementType)));
                    final Quantifier.ForEach newQuantifier = Quantifier.forEach(call.memoizeExpression(explodeExpression));
                    ValuePredicate newValuePredicate = new ValuePredicate(value,
                            new Comparisons.ValueComparison(Comparisons.Type.EQUALS, QuantifiedObjectValue.of(newQuantifier.getAlias(), elementType)));
                    transformedPredicates.add(newValuePredicate);
                    transformedQuantifiers.add(newQuantifier);
                } else {
                    throw new RecordCoreException("unknown in comparison " + comparison.getClass().getSimpleName());
                }
            } else {
                transformedPredicates.add(predicate);
            }
        }

        transformedQuantifiers.addAll(bindings.getAll(innerQuantifierMatcher));

        call.yieldExpression(new SelectExpression(selectExpression.getResultValue(),
                transformedQuantifiers.build(),
                transformedPredicates.build()));
    }


    private AbstractArrayConstructorValue.LightArrayConstructorValue promoteComparandValue(Value value, Value comparandValue) {
        List<Value> valueChildren = toList(value.getChildren());
        System.out.println("comparandValue class:" + comparandValue.getClass());
        List<Value> promotedComparandValueChildren = new ArrayList<>();
        for (Value currentComparandValue: comparandValue.getChildren()) {
            System.out.println("currentComparandValue result type:" + currentComparandValue.getResultType());
            promotedComparandValueChildren.add(PromoteValue.inject(currentComparandValue, value.getResultType()));
        }
        return AbstractArrayConstructorValue.LightArrayConstructorValue.of(promotedComparandValueChildren);
    }


    /*
     * value:($T1.PK as _0, $T1.A as _1)
     * comparisonValue:IN array((@c13 as _0, @c15 as _1), (@c19 as _0, @c21 as _1)) comparandValue:array((@c13 as _0, @c15 as _1), (@c19 as _0, @c21 as _1))
     * return value_0 = newQuantifier_0 and value_1 = newQuantifier_1 and ...
     */
    private QueryPredicate findNewValuePredicate(Value value, Quantifier.ForEach newQuantifier) {
        for (Column<? extends Value> c: newQuantifier.getFlowedColumns()) {
            System.out.println("flowed column value:" + c.getValue() + " column field:" + c.getField());
        }
        List<Typed> conjuncts = new ArrayList<>();
        List<Value> valueChildren = toList(value.getChildren());
        List<Column<? extends FieldValue>> comparandValueChildren = newQuantifier.getFlowedColumns();

        for (int i = 0; i < valueChildren.size(); i++) {
            conjuncts.add(new RelOpValue.EqualsFn().encapsulate(List.of(valueChildren.get(i), comparandValueChildren.get(i).getValue())));
        }
        AndOrValue result = (AndOrValue) new AndOrValue.AndFn().encapsulate(conjuncts);
        System.out.println("new QueryPredicate:" + result.toQueryPredicate(null, Quantifier.current()).get());
        return result.toQueryPredicate(null, Quantifier.current()).get();
    }

    private List<Value> toList(Iterable<? extends Value> iterable) {
        List<Value> result = new ArrayList<>();
        iterable.forEach(result::add);
        return result;
    }
}
