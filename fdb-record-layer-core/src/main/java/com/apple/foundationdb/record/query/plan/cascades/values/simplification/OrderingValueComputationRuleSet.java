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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
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

    private OrderingValueComputationRuleSet(@Nonnull final Set<? extends AbstractValueRule<NonnullPair<Value, P>, ValueComputationRuleCall<OrderingPartCreator<O, P>, P>, ? extends Value>> rules,
                                            @Nonnull final SetMultimap<? extends AbstractValueRule<NonnullPair<Value, P>, ValueComputationRuleCall<OrderingPartCreator<O, P>, P>, ? extends Value>, ? extends AbstractValueRule<NonnullPair<Value, P>, ValueComputationRuleCall<OrderingPartCreator<O, P>, P>, ? extends Value>> dependsOn) {
        super(rules, dependsOn);
    }

    @Nonnull
    protected static <O extends SortOrder, P extends OrderingPart<O>> ValueComputationRule<OrderingPartCreator<O, P>, P, ArithmeticValue> eliminateArithmeticValueWithConstantRule(@Nonnull final O sortOrder) {
        return ValueComputationRule.fromSimplificationRule(
                new EliminateArithmeticValueWithConstantRule(),
                (creator, value, ignored) -> Objects.requireNonNull(creator).create(value, sortOrder));
    }

    @Nonnull
    protected static <O extends SortOrder, P extends OrderingPart<O>> ValueComputationRule<OrderingPartCreator<O, P>, P, ToOrderedBytesValue> computeToOrderedBytesValueRule(@Nonnull final Function<Direction, O> directionToSortOrderFunction) {
        return new ComputeToOrderedBytesValueRule<>(directionToSortOrderFunction);
    }

    @Nonnull
    protected static <O extends SortOrder, P extends OrderingPart<O>> ValueComputationRule<OrderingPartCreator<O, P>, P, Value> defaultOrderingPartRule(@Nonnull final O sortOrder) {
        return new DefaultOrderingPartRule<>(sortOrder);
    }

    private static <O extends SortOrder, P extends OrderingPart<O>> OrderingValueComputationRuleSet<O, P> ruleSet(@Nonnull final O sortOrder,
                                                                                                                  @Nonnull final ValueComputationRule<OrderingPartCreator<O, P>, P, ArithmeticValue> eliminateArithmeticValueWithConstantRule,
                                                                                                                  @Nonnull final ValueComputationRule<OrderingPartCreator<O, P>, P, ToOrderedBytesValue> computeToOrderedBytesValueRule,
                                                                                                                  @Nonnull final ValueComputationRule<OrderingPartCreator<O, P>, P, Value> defaultOrderingPartRule) {
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

    @Nonnull
    public static OrderingValueComputationRuleSet<OrderingPart.ProvidedSortOrder, OrderingPart.ProvidedOrderingPart> usingProvidedOrderingParts() {
        return ruleSet(OrderingPart.ProvidedSortOrder.ASCENDING,
                eliminateArithmeticValueWithConstantRule(OrderingPart.ProvidedSortOrder.ASCENDING),
                computeToOrderedBytesValueRule(OrderingPart.ProvidedSortOrder::fromDirection),
                defaultOrderingPartRule(OrderingPart.ProvidedSortOrder.ASCENDING));
    }

    @Nonnull
    public static OrderingValueComputationRuleSet<MatchedSortOrder, MatchedOrderingPart> usingMatchedOrderingParts() {
        return ruleSet(MatchedSortOrder.ASCENDING,
                eliminateArithmeticValueWithConstantRule(MatchedSortOrder.ASCENDING),
                computeToOrderedBytesValueRule(MatchedSortOrder::fromDirection),
                defaultOrderingPartRule(MatchedSortOrder.ASCENDING));
    }

    @Nonnull
    public static OrderingValueComputationRuleSet<RequestedSortOrder, RequestedOrderingPart> usingRequestedOrderingParts() {
        return ruleSet(RequestedSortOrder.ASCENDING,
                eliminateArithmeticValueWithConstantRule(RequestedSortOrder.ASCENDING),
                computeToOrderedBytesValueRule(RequestedSortOrder::fromDirection),
                defaultOrderingPartRule(RequestedSortOrder.ASCENDING));
    }
}
