/*
 * OrderingValueComputationPerPartRuleSet.java
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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for simplifying {@link Value} trees used to expression ordering constraints.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class OrderingValueComputationPerPartRuleSet extends ValueComputationRuleSet<Void, ProvidedOrderingPart> {
    @Nonnull
    protected static final ValueComputationRule<Void, ProvidedOrderingPart, ArithmeticValue> eliminateArithmeticValueWithConstantRule =
            ValueComputationRule.fromSimplificationRule(new EliminateArithmeticValueWithConstantRule(), value -> new ProvidedOrderingPart(value, OrderingPart.ProvidedSortOrder.ASCENDING));

    private static final Set<ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>> ORDERING_RULES;

    private static final SetMultimap<ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>, ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>> ORDERING_DEPENDS_ON;

    static {
        final var transformedRules =
                ValueComputationRuleSet.<Void, ProvidedOrderingPart>fromSimplificationRules(DefaultValueSimplificationRuleSet.SIMPLIFICATION_RULES,
                        DefaultValueSimplificationRuleSet.SIMPLIFICATION_DEPENDS_ON, v -> new ProvidedOrderingPart(v, OrderingPart.ProvidedSortOrder.ASCENDING));
        final Set<ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>> localOrderingRules =
                ImmutableSet.<ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>>builder()
                        .add(eliminateArithmeticValueWithConstantRule)
                        .build();

        ORDERING_RULES =
                ImmutableSet.<ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>>builder()
                        .addAll(transformedRules.getComputationRules())
                        .addAll(localOrderingRules)
                        .build();

        final var dependsOnBuilder =
                ImmutableSetMultimap.<ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>, ValueComputationRule<Void, ProvidedOrderingPart, ? extends Value>>builder();

        dependsOnBuilder.putAll(transformedRules.getComputationDependsOn());

        for (final var localOrderingRule : localOrderingRules) {
            for (final var orderingRule : transformedRules.getComputationRules()) {
                dependsOnBuilder.put(localOrderingRule, orderingRule);
            }
        }

        ORDERING_DEPENDS_ON = dependsOnBuilder.build();
    }

    private OrderingValueComputationPerPartRuleSet() {
        super(ORDERING_RULES, ORDERING_DEPENDS_ON);
    }

    public static OrderingValueComputationPerPartRuleSet ofOrderingValueComputationPerPartRules() {
        return new OrderingValueComputationPerPartRuleSet();
    }
}
