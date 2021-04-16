/*
 * RelationalExpressionMatchers.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;

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
public class RelationalExpressionMatchers {
    private RelationalExpressionMatchers() {
        // do not instantiate
    }

    public static <R extends RelationalExpression> TypedMatcher<R> ofType(@Nonnull final Class<R> bindableClass) {
        return new TypedMatcher<>(bindableClass);
    }

    public static <R extends RelationalExpression, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeOwning(@Nonnull final Class<R> bindableClass,
                                                                                                                              @Nonnull final BindingMatcher<C> downstream) {
        return TypedMatcherWithExtractAndDownstream.of(bindableClass,
                RelationalExpression::getQuantifiers,
                downstream);
    }

    public static <R extends RelationalExpressionWithPredicates, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicates(@Nonnull final Class<R> bindableClass,
                                                                                                                                                    @Nonnull final BindingMatcher<C> downstream) {
        return TypedMatcherWithExtractAndDownstream.of(bindableClass,
                RelationalExpressionWithPredicates::getPredicates,
                downstream);
    }

    public static <R extends RelationalExpressionWithPredicates, C1 extends Collection<? extends QueryPredicate>, C2 extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicatesAndOwning(@Nonnull final Class<R> bindableClass,
                                                                                                                                                                                                               @Nonnull final BindingMatcher<C1> downstreamPredicates,
                                                                                                                                                                                                               @Nonnull final BindingMatcher<C2> downstreamQuantifiers) {
        final ExtractingMatcher<RelationalExpressionWithPredicates> predicatesExtractingMatcher =
                ExtractingMatcher.of(RelationalExpressionWithPredicates::getPredicates, downstreamPredicates);

        final ExtractingMatcher<RelationalExpressionWithPredicates> quantifiersExtractingMatcher =
                ExtractingMatcher.of(RelationalExpressionWithPredicates::getQuantifiers, downstreamQuantifiers);

        return TypedMatcherWithExtractAndDownstream.of(bindableClass,
                t -> t,
                AllOfMatcher.matchingAllOf(RelationalExpressionWithPredicates.class,
                        ImmutableList.of(predicatesExtractingMatcher, quantifiersExtractingMatcher)));
    }

    public static BindingMatcher<RecordQueryPlan> anyPlan() {
        return RelationalExpressionMatchers.ofType(RecordQueryPlan.class);
    }
}
