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
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.SetMatcher.exactlyInAnyOrder;
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
public class RelationalExpressionMatchers {
    private RelationalExpressionMatchers() {
        // do not instantiate
    }

    public static <R extends RelationalExpression> TypedMatcher<R> ofType(@Nonnull final Class<R> bindableClass) {
        return new TypedMatcher<>(bindableClass);
    }

    public static <R extends RelationalExpression> BindingMatcher<R> ofType(@Nonnull final Class<R> bindableClass,
                                                                            @Nonnull final BindingMatcher<R> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.identity(),
                downstream);
    }

    public static <R extends RelationalExpression, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeOwning(@Nonnull final Class<R> bindableClass,
                                                                                                                              @Nonnull final BindingMatcher<C> downstream) {
        return typedWithDownstream(bindableClass,
                RelationalExpression::getQuantifiers,
                downstream);
    }

    public static <R extends RelationalExpressionWithPredicates, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicates(@Nonnull final Class<R> bindableClass,
                                                                                                                                                    @Nonnull final BindingMatcher<C> downstream) {
        return typedWithDownstream(bindableClass,
                RelationalExpressionWithPredicates::getPredicates,
                downstream);
    }

    public static <R extends RelationalExpressionWithPredicates, C1 extends Collection<? extends QueryPredicate>, C2 extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicatesAndOwning(@Nonnull final Class<R> bindableClass,
                                                                                                                                                                                                               @Nonnull final BindingMatcher<C1> downstreamPredicates,
                                                                                                                                                                                                               @Nonnull final BindingMatcher<C2> downstreamQuantifiers) {
        return typedWithDownstream(bindableClass,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(RelationalExpressionWithPredicates.class,
                        ImmutableList.of(typedWithDownstream(bindableClass, RelationalExpressionWithPredicates::getPredicates, downstreamPredicates),
                                typedWithDownstream(bindableClass, RelationalExpression::getQuantifiers, downstreamQuantifiers))));
    }

    public static BindingMatcher<RecordQueryPlan> anyPlan() {
        return ofType(RecordQueryPlan.class);
    }

    @Nonnull
    public static <R extends RecordQueryPlan> BindingMatcher<R> childrenPlans(@Nonnull final Class<R> bindableClass, @Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(bindableClass,
                RecordQueryPlan::getQueryPlanChildren,
                downstream);
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlan> descendantPlans(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                plan -> ImmutableList.copyOf(plan.collectDescendantPlans()),
                AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlan> descendantPlans(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                plan -> ImmutableList.copyOf(plan.collectDescendantPlans()),
                downstream);
    }


    @Nonnull
    public static BindingMatcher<RecordQueryPlan> selfOrDescendantPlans(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                plan -> ImmutableList.copyOf(Iterables.concat(plan.collectDescendantPlans(), ImmutableList.of(plan))),
                AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<RecordQueryPlan> selfOrDescendantPlans(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return typedWithDownstream(RecordQueryPlan.class,
                plan -> ImmutableList.copyOf(Iterables.concat(plan.collectDescendantPlans(), ImmutableList.of(plan))),
                downstream);
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static ListMatcher<? extends RecordQueryPlan> exactlyPlans(@Nonnull final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return exactly(Arrays.asList(downstreams));
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static SetMatcher<? extends RecordQueryPlan> exactlyPlansInAnyOrder(@Nonnull final BindingMatcher<? extends RecordQueryPlan>... downstreams) {
        return exactlyInAnyOrder(Arrays.asList(downstreams));
    }

    public static SetMatcher<? extends RecordQueryPlan> exactlyPlansInAnyOrder(@Nonnull final Collection<? extends BindingMatcher<? extends RecordQueryPlan>> downstreams) {
        return exactlyInAnyOrder(downstreams);
    }
}
