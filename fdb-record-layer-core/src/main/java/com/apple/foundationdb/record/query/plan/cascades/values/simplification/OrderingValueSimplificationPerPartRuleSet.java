/*
 * OrderingValueSimplificationPerPartRuleSet.java
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
public class OrderingValueSimplificationPerPartRuleSet extends AbstractValueRuleSet<Value, ValueSimplificationRuleCall> {
    @Nonnull
    protected static final ValueSimplificationRule<? extends Value> eliminateArithmeticValueWithConstantRule = new EliminateArithmeticValueWithConstantRule();

    private static final Set<ValueSimplificationRule<? extends Value>> ORDERING_SIMPLIFICATION_RULES =
            ImmutableSet.<ValueSimplificationRule<? extends Value>>builder()
                    .addAll(DefaultValueSimplificationRuleSet.SIMPLIFICATION_RULES)
                    .add(eliminateArithmeticValueWithConstantRule)
                    .build();

    private static final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> ORDERING_SIMPLIFICATION_DEPENDS_ON =
            ImmutableSetMultimap.<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>>builder()
                    .putAll(DefaultValueSimplificationRuleSet.SIMPLIFICATION_DEPENDS_ON)
                    .put(eliminateArithmeticValueWithConstantRule, DefaultValueSimplificationRuleSet.composeFieldValueOverRecordConstructorRule)
                    .build();

    private OrderingValueSimplificationPerPartRuleSet() {
        super(ORDERING_SIMPLIFICATION_RULES, ORDERING_SIMPLIFICATION_DEPENDS_ON);
    }

    public static OrderingValueSimplificationPerPartRuleSet ofOrderingSimplificationPerPartRules() {
        return new OrderingValueSimplificationPerPartRuleSet();
    }
}
