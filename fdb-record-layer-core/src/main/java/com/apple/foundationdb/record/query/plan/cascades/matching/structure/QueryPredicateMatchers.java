/*
 * QueryPredicateMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithPredicate.typedMatcherWithPredicate;

/**
 * A <code>BindingMatcher</code> is an expression that can be matched against a
 * {@link RelationalExpression} tree, while binding certain expressions/references in the tree to expression matcher objects.
 * The bindings can be retrieved from the rule call once the binding is matched.
 *
 * <p>
 * Extreme care should be taken when implementing <code>ExpressionMatcher</code>, since it can be very delicate.
 * In particular, expression matchers may (or may not) be reused between successive rule calls and should be stateless.
 * Additionally, implementors of <code>ExpressionMatcher</code> must use the (default) reference equals.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class QueryPredicateMatchers {
    private QueryPredicateMatchers() {
        // do not instantiate
    }

    public static TypedMatcher<QueryPredicate> anyPredicate() {
        return ofType(QueryPredicate.class);
    }

    public static TypedMatcher<QueryPredicate> anyResidualPredicate() {
        return typedWithDownstream(QueryPredicate.class,
                Extractor.of(p -> !p.isIndexOnly(), p -> "residualPredicate(" + p + ")"),
                PrimitiveMatchers.equalsObject(true));
    }

    public static TypedMatcher<PredicateWithValue> predicateWithValue() {
        return ofType(PredicateWithValue.class);
    }

    public static <V extends Value> TypedMatcher<ValuePredicate> valuePredicate(@Nonnull final BindingMatcher<V> downstream) {
        return typedWithDownstream(ValuePredicate.class,
                Extractor.of(p -> Verify.verifyNotNull(p.getValue()), name -> "comparand(" + name + ")"),
                        downstream);
    }

    public static BindingMatcher<RangeConstraints> rangeConstraint(@Nonnull final BindingMatcher<? extends Collection<? extends Comparisons.Comparison>> comparisonBindingMatcher) {
        return typedWithDownstream(RangeConstraints.class,
                Extractor.of(RangeConstraints::getComparisons, name -> "comparisons(" + name + ")"),
                comparisonBindingMatcher);
    }

    public static <V extends Value, R extends RangeConstraints> TypedMatcher<PredicateWithValueAndRanges> predicateWithValueAndRanges(@Nonnull final BindingMatcher<V> downstreamValueMatcher,
                                                                                                                                      @Nonnull final BindingMatcher<Collection<R>> downstreamRangesMatcher) {
        return typedWithDownstream(PredicateWithValueAndRanges.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(PredicateWithValueAndRanges.class,
                        ImmutableList.of(
                                typedWithDownstream(PredicateWithValueAndRanges.class,
                                        Extractor.of(PredicateWithValueAndRanges::getValue, name -> "comparator(" + name + ")"),
                                        downstreamValueMatcher),
                                typedWithDownstream(PredicateWithValueAndRanges.class,
                                        Extractor.of(PredicateWithValueAndRanges::getRanges, name -> "ranges(" + name + ")"),
                                        downstreamRangesMatcher))));
    }

    public static <P extends QueryPredicate> TypedMatcher<P> ofType(@Nonnull final Class<P> bindableClass) {
        return typed(bindableClass);
    }

    public static <P extends QueryPredicate, C extends Collection<? extends QueryPredicate>> BindingMatcher<P> ofTypeWithChildren(@Nonnull final Class<P> bindableClass,
                                                                                                                                  @Nonnull final BindingMatcher<C> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(QueryPredicate::getChildren, name -> "children(" + name + ")"),
                downstream);
    }

    public static <C extends Collection<? extends QueryPredicate>> BindingMatcher<AndPredicate> andPredicate(@Nonnull final BindingMatcher<C> downstream) {
        return ofTypeWithChildren(AndPredicate.class, downstream);
    }

    public static TypedMatcher<Comparisons.Comparison> anyComparison() {
        return typed(Comparisons.Comparison.class);
    }

    public static TypedMatcher<Comparisons.Comparison> anyComparisonOfType(@Nonnull final Comparisons.Type type) {
        return typedWithDownstream(Comparisons.Comparison.class,
                Extractor.of(Comparisons.Comparison::getType, name -> "type(" + name + ")"),
                PrimitiveMatchers.equalsObject(type));
    }

    public static TypedMatcher<Comparisons.ValueComparison> anyValueComparison() {
        return typed(Comparisons.ValueComparison.class);
    }

    public static TypedMatcher<Comparisons.Comparison> anyUnaryComparison() {
        return typedMatcherWithPredicate(Comparisons.Comparison.class, comparison -> comparison.getType().isUnary());
    }

    public static <V extends Value> TypedMatcher<Comparisons.ValueComparison> anyValueComparison(@Nonnull final BindingMatcher<V> downstreamValue) {
        return typedWithDownstream(Comparisons.ValueComparison.class,
                Extractor.of(Comparisons.ValueComparison::getValue, name -> "operand(" + name + ")"),
                downstreamValue);
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<ValuePredicate> valuePredicate(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                  @Nonnull final Comparisons.Comparison comparison) {
        return valuePredicate(downstreamValue, PrimitiveMatchers.equalsObject(comparison));
    }

    @Nonnull
    public static <V extends Value, C extends Comparisons.Comparison> BindingMatcher<ValuePredicate> valuePredicate(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                                                    @Nonnull final BindingMatcher<C> downstreamComparison) {
        return typedWithDownstream(ValuePredicate.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(ValuePredicate.class,
                        ImmutableList.of(
                                typedWithDownstream(ValuePredicate.class,
                                        Extractor.of(ValuePredicate::getValue, name -> "value(" + name + ")"),
                                        downstreamValue),
                                typedWithDownstream(ValuePredicate.class,
                                        Extractor.of(ValuePredicate::getComparison, name -> "comparison(" + name + ")"),
                                        downstreamComparison))));
    }

    @Nonnull
    public static <P extends QueryPredicate> BindingMatcher<OrPredicate> orPredicate(@Nonnull final CollectionMatcher<P> downstream) {
        return typedWithDownstream(OrPredicate.class,
                Extractor.of(OrPredicate::getChildren, name -> "children(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static <P extends QueryPredicate> BindingMatcher<NotPredicate> notPredicate(@Nonnull final CollectionMatcher<P> downstream) {
        return typedWithDownstream(NotPredicate.class,
                Extractor.of(NotPredicate::getChildren, name -> "children(" + name + ")"),
                downstream);
    }
}
