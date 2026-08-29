/*
 * OrderingValueComputationRuleSet.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.OrderingPartCreator;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.SortOrder;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.tuple.TupleOrdering.Direction;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A set of rules for simplifying {@link Value} trees used to expression ordering constraints.
 * @param <O> type variable for sort order
 * @param <P> type variable for ordering part
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class OrderingValueComputationRuleSet<O extends SortOrder, P extends OrderingPart<O>> extends ValueComputationRuleSet<OrderingPartCreator<O, P>, P> {

    private OrderingValueComputationRuleSet(final Set<? extends AbstractValueRule<NonnullPair<Value, P>, ValueComputationRuleCall<OrderingPartCreator<O, P>, P>, ? extends Value>> rules,
                                            final SetMultimap<? extends AbstractValueRule<NonnullPair<Value, P>, ValueComputationRuleCall<OrderingPartCreator<O, P>, P>, ? extends Value>, ? extends AbstractValueRule<NonnullPair<Value, P>, ValueComputationRuleCall<OrderingPartCreator<O, P>, P>, ? extends Value>> dependsOn) {
        super(rules, dependsOn);
    }

    protected static <O extends SortOrder, P extends OrderingPart<O>> ValueComputationRule<OrderingPartCreator<O, P>, P, ArithmeticValue> eliminateArithmeticValueWithConstantRule(final O sortOrder) {
        return ValueComputationRule.fromSimplificationRule(
                new EliminateArithmeticValueWithConstantRule(),
                (creator, value, ignored) -> Objects.requireNonNull(creator).create(value, sortOrder));
    }

    protected static <O extends SortOrder, P extends OrderingPart<O>> ValueComputationRule<OrderingPartCreator<O, P>, P, ToOrderedBytesValue> computeToOrderedBytesValueRule(final Function<Direction, O> directionToSortOrderFunction) {
        return new ComputeToOrderedBytesValueRule<>(directionToSortOrderFunction);
    }

    protected static <O extends SortOrder, P extends OrderingPart<O>> ValueComputationRule<OrderingPartCreator<O, P>, P, Value> defaultOrderingPartRule(final O sortOrder) {
        return new DefaultOrderingPartRule<>(sortOrder);
    }

    private static <O extends SortOrder, P extends OrderingPart<O>> OrderingValueComputationRuleSet<O, P> ruleSet(final O sortOrder,
                                                                                                                  final ValueComputationRule<OrderingPartCreator<O, P>, P, ArithmeticValue> eliminateArithmeticValueWithConstantRule,
                                                                                                                  final ValueComputationRule<OrderingPartCreator<O, P>, P, ToOrderedBytesValue> computeToOrderedBytesValueRule,
                                                                                                                  final ValueComputationRule<OrderingPartCreator<O, P>, P, Value> defaultOrderingPartRule) {
        final var transformedRules =
                ValueComputationRuleSet.<OrderingPartCreator<O, P>, P>fromSimplificationRules(DefaultValueSimplificationRuleSet.SIMPLIFICATION_RULES,
                        DefaultValueSimplificationRuleSet.SIMPLIFICATION_DEPENDS_ON,
                        (creator, value, ignored) -> Objects.requireNonNull(creator).create(value, sortOrder));

        final var localOrderingRules =
                ImmutableSet.<ValueComputationRule<OrderingPartCreator<O, P>, P, ? extends Value>>builder()
                        .add(eliminateArithmeticValueWithConstantRule)
                        .add(computeToOrderedBytesValueRule)
                        .add(defaultOrderingPartRule)
                        .build();

        final var orderingRules =
                ImmutableSet.<ValueComputationRule<OrderingPartCreator<O, P>, P, ? extends Value>>builder()
                        .addAll(transformedRules.getComputationRules())
                        .addAll(localOrderingRules)
                        .build();

        final var dependsOnBuilder =
                ImmutableSetMultimap.<ValueComputationRule<OrderingPartCreator<O, P>, P, ? extends Value>, ValueComputationRule<OrderingPartCreator<O, P>, P, ? extends Value>>builder();

        dependsOnBuilder.putAll(transformedRules.getComputationDependsOn());

        for (final var localOrderingRule : localOrderingRules) {
            for (final var orderingRule : transformedRules.getComputationRules()) {
                dependsOnBuilder.put(localOrderingRule, orderingRule);
            }
        }

        dependsOnBuilder.put(computeToOrderedBytesValueRule, eliminateArithmeticValueWithConstantRule);
        dependsOnBuilder.put(defaultOrderingPartRule, computeToOrderedBytesValueRule);

        return new OrderingValueComputationRuleSet<>(orderingRules, dependsOnBuilder.build());
    }

    private static final LoadingCache<OrderingPart.ProvidedSortOrder, OrderingValueComputationRuleSet<OrderingPart.ProvidedSortOrder, OrderingPart.ProvidedOrderingPart>> providedOrderingComputationRuleSetCache =
            CacheBuilder.newBuilder()
                    .maximumSize(OrderingPart.ProvidedSortOrder.values().length)
                    .build(new CacheLoader<>() {
                        @Override
                        public OrderingValueComputationRuleSet<OrderingPart.ProvidedSortOrder, OrderingPart.ProvidedOrderingPart> load(final OrderingPart.ProvidedSortOrder providedSortOrder) throws Exception {
                            return computeUsingProvidedOrderingParts(providedSortOrder);
                        }
                    });

    private static final LoadingCache<OrderingPart.MatchedSortOrder, OrderingValueComputationRuleSet<OrderingPart.MatchedSortOrder, OrderingPart.MatchedOrderingPart>> matchedOrderingComputationRuleSetCache =
            CacheBuilder.newBuilder()
                    .maximumSize(OrderingPart.ProvidedSortOrder.values().length)
                    .build(new CacheLoader<>() {
                        @Override
                        public OrderingValueComputationRuleSet<OrderingPart.MatchedSortOrder, OrderingPart.MatchedOrderingPart> load(final OrderingPart.MatchedSortOrder matchedSortOrder) {
                            return computeUsingMatchedOrderingParts(matchedSortOrder);
                        }
                    });

    private static final LoadingCache<OrderingPart.RequestedSortOrder, OrderingValueComputationRuleSet<OrderingPart.RequestedSortOrder, OrderingPart.RequestedOrderingPart>> requestedOrderingComputationRuleSetCache =
            CacheBuilder.newBuilder()
                    .maximumSize(OrderingPart.ProvidedSortOrder.values().length)
                    .build(new CacheLoader<>() {
                        @Override
                        public OrderingValueComputationRuleSet<OrderingPart.RequestedSortOrder, OrderingPart.RequestedOrderingPart> load(final OrderingPart.RequestedSortOrder requestedSortOrder) {
                            return computeUsingRequestedOrderingParts(requestedSortOrder);
                        }
                    });

    private static OrderingValueComputationRuleSet<OrderingPart.ProvidedSortOrder, OrderingPart.ProvidedOrderingPart> computeUsingProvidedOrderingParts(final OrderingPart.ProvidedSortOrder providedSortOrder) {
        return ruleSet(providedSortOrder,
                eliminateArithmeticValueWithConstantRule(providedSortOrder),
                computeToOrderedBytesValueRule(OrderingPart.ProvidedSortOrder::fromDirection),
                defaultOrderingPartRule(providedSortOrder));
    }

    public static OrderingValueComputationRuleSet<OrderingPart.ProvidedSortOrder, OrderingPart.ProvidedOrderingPart> usingProvidedOrderingParts() {
        return usingProvidedOrderingParts(OrderingPart.ProvidedSortOrder.ASCENDING);
    }

    public static OrderingValueComputationRuleSet<OrderingPart.ProvidedSortOrder, OrderingPart.ProvidedOrderingPart> usingProvidedOrderingParts(final OrderingPart.ProvidedSortOrder providedSortOrder) {
        return providedOrderingComputationRuleSetCache.getUnchecked(providedSortOrder);
    }

    public static OrderingValueComputationRuleSet<MatchedSortOrder, MatchedOrderingPart> computeUsingMatchedOrderingParts(final MatchedSortOrder matchedSortOrder) {
        return ruleSet(matchedSortOrder,
                eliminateArithmeticValueWithConstantRule(matchedSortOrder),
                computeToOrderedBytesValueRule(MatchedSortOrder::fromDirection),
                defaultOrderingPartRule(matchedSortOrder));
    }

    public static OrderingValueComputationRuleSet<OrderingPart.MatchedSortOrder, OrderingPart.MatchedOrderingPart> usingMatchedOrderingParts() {
        return usingMatchedOrderingParts(OrderingPart.MatchedSortOrder.ASCENDING);
    }

    public static OrderingValueComputationRuleSet<OrderingPart.MatchedSortOrder, OrderingPart.MatchedOrderingPart> usingMatchedOrderingParts(final OrderingPart.MatchedSortOrder matchedSortOrder) {
        return matchedOrderingComputationRuleSetCache.getUnchecked(matchedSortOrder);
    }

    public static OrderingValueComputationRuleSet<RequestedSortOrder, RequestedOrderingPart> computeUsingRequestedOrderingParts(final RequestedSortOrder requestedSortOrder) {
        return ruleSet(requestedSortOrder,
                eliminateArithmeticValueWithConstantRule(requestedSortOrder),
                computeToOrderedBytesValueRule(RequestedSortOrder::fromDirection),
                defaultOrderingPartRule(requestedSortOrder));
    }

    public static OrderingValueComputationRuleSet<OrderingPart.RequestedSortOrder, OrderingPart.RequestedOrderingPart> usingRequestedOrderingParts() {
        return usingRequestedOrderingParts(OrderingPart.RequestedSortOrder.ASCENDING);
    }

    public static OrderingValueComputationRuleSet<OrderingPart.RequestedSortOrder, OrderingPart.RequestedOrderingPart> usingRequestedOrderingParts(final OrderingPart.RequestedSortOrder requestedSortOrder) {
        return requestedOrderingComputationRuleSetCache.getUnchecked(requestedSortOrder);
    }
}
