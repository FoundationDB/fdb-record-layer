/*
 * ValueComputationRuleSet.java
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
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 * @param <A> the type of argument that rules in this set consume
 * @param <R> the type of result that rules in this set (or subclasses thereof) produce
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public abstract class ValueComputationRuleSet<A, R> extends AbstractValueRuleSet<NonnullPair<Value, R>, ValueComputationRuleCall<A, R>> {
    public ValueComputationRuleSet(@Nonnull final Set<? extends AbstractValueRule<NonnullPair<Value, R>, ValueComputationRuleCall<A, R>, ? extends Value>> abstractValueSimplificationRules,
                                   @Nonnull final SetMultimap<? extends AbstractValueRule<NonnullPair<Value, R>, ValueComputationRuleCall<A, R>, ? extends Value>, ? extends AbstractValueRule<NonnullPair<Value, R>, ValueComputationRuleCall<A, R>, ? extends Value>> dependsOn) {
        super(abstractValueSimplificationRules, dependsOn);
    }

    @Nonnull
    static <A, R> TransformedRules<A, R> fromSimplificationRules(@Nonnull final Set<ValueSimplificationRule<? extends Value>> simplificationRules,
                                                                 @Nonnull final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> simplificationDependsOn,
                                                                 @Nonnull final ValueComputationRule.OnMatchComputationFunction<A, R> computationFunction) {
        final var simplificationToComputationRulesMap =
                new LinkedIdentityMap<ValueSimplificationRule<? extends Value>, ValueComputationRule<A, R, ? extends Value>>();
        for (final var simplificationRule : simplificationRules) {
            simplificationToComputationRulesMap.put(simplificationRule,
                    ValueComputationRule.fromSimplificationRule(simplificationRule, computationFunction));
        }

        final var computationDependsOnBuilder =
                ImmutableSetMultimap.<ValueComputationRule<A, R, ? extends Value>, ValueComputationRule<A, R, ? extends Value>>builder();

        for (final var entry : simplificationDependsOn.entries()) {
            computationDependsOnBuilder.put(Objects.requireNonNull(simplificationToComputationRulesMap.get(entry.getKey())),
                    Objects.requireNonNull(simplificationToComputationRulesMap.get(entry.getValue())));
        }

        return new TransformedRules<>(simplificationRules, simplificationDependsOn,
                ImmutableSet.copyOf(simplificationToComputationRulesMap.values()),
                computationDependsOnBuilder.build(),
                simplificationToComputationRulesMap);
    }

    /**
     * Holder class to easily transform simplification rules and their dependencies into computation rules and
     * their dependencies.
     * @param <A> argument type parameter
     * @param <R> result type parameter
     */
    public static class TransformedRules<A, R> {
        @Nonnull
        private final Set<ValueSimplificationRule<? extends Value>> simplificationRules;
        @Nonnull
        private final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> simplificationDependsOn;
        @Nonnull
        private final Set<ValueComputationRule<A, R, ? extends Value>> computationRules;
        @Nonnull
        private final SetMultimap<ValueComputationRule<A, R, ? extends Value>, ValueComputationRule<A, R, ? extends Value>> computationDependsOn;

        @Nonnull
        private final LinkedIdentityMap<ValueSimplificationRule<? extends Value>, ValueComputationRule<A, R, ? extends Value>> simplificationToComputationRulesMap;

        public TransformedRules(@Nonnull final Set<ValueSimplificationRule<? extends Value>> simplificationRules,
                                @Nonnull final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> simplificationDependsOn,
                                @Nonnull final Set<ValueComputationRule<A, R, ? extends Value>> computationRules,
                                @Nonnull final SetMultimap<ValueComputationRule<A, R, ? extends Value>, ValueComputationRule<A, R, ? extends Value>> computationDependsOn,
                                @Nonnull final LinkedIdentityMap<ValueSimplificationRule<? extends Value>, ValueComputationRule<A, R, ? extends Value>> simplificationToComputationRulesMap) {
            this.simplificationRules = simplificationRules;
            this.simplificationDependsOn = simplificationDependsOn;
            this.computationRules = computationRules;
            this.computationDependsOn = computationDependsOn;
            this.simplificationToComputationRulesMap = simplificationToComputationRulesMap;
        }

        @Nonnull
        public Set<ValueSimplificationRule<? extends Value>> getSimplificationRules() {
            return simplificationRules;
        }

        @Nonnull
        public SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> getSimplificationDependsOn() {
            return simplificationDependsOn;
        }

        @Nonnull
        public Set<ValueComputationRule<A, R, ? extends Value>> getComputationRules() {
            return computationRules;
        }

        @Nonnull
        public SetMultimap<ValueComputationRule<A, R, ? extends Value>, ValueComputationRule<A, R, ? extends Value>> getComputationDependsOn() {
            return computationDependsOn;
        }
    }
}
