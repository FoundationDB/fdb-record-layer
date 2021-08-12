/*
 * QueryPredicateMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.QueryComponentPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

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

    @Nonnull
    public static BindingMatcher<QueryComponentPredicate> queryComponentPredicate(@Nonnull BindingMatcher<? extends QueryComponent> downstream) {
        return typedWithDownstream(QueryComponentPredicate.class,
                Extractor.of(QueryComponentPredicate::getQueryComponent, name -> "queryComponent(" + name + ")"),
                downstream);
    }

    public static TypedMatcher<Comparisons.Comparison> anyComparison() {
        return typed(Comparisons.Comparison.class);
    }

    public static TypedMatcher<Comparisons.Comparison> anyComparisonOfType(@Nonnull final Comparisons.Type type) {
        return typedWithDownstream(Comparisons.Comparison.class,
                Extractor.of(Comparisons.Comparison::getType, name -> "type(" + name + ")"),
                PrimitiveMatchers.equalsObject(type));
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
}
