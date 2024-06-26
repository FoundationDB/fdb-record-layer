/*
 * AliasMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * TODO.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public interface ValueEquivalence {
    @Nonnull
    Optional<QueryPlanConstraint> ALWAYS_EQUAL = Optional.of(QueryPlanConstraint.tautology());

    static Optional<QueryPlanConstraint> alwaysEqual() {
        return ALWAYS_EQUAL;
    }


    @Nonnull
    Optional<QueryPlanConstraint> equivalence(@Nonnull Value left,
                                              @Nonnull Value right);

    @Nonnull
    Optional<QueryPlanConstraint> equivalence(@Nonnull CorrelationIdentifier left,
                                              @Nonnull CorrelationIdentifier right);

    @Nonnull
    default <T extends UsesValueEquivalence<T>> Optional<QueryPlanConstraint> semanticEquals(@Nonnull final Set<T> left,
                                                                                             @Nonnull final Set<T> right) {
        if (left.size() != right.size()) {
            return Optional.empty();
        }

        QueryPlanConstraint constraint = QueryPlanConstraint.tautology();
        for (final T l : left) {
            for (final T r : right) {
                final var semanticEquals = l.semanticEquals(r, this);
                if (semanticEquals.isPresent()) {
                    constraint = constraint.compose(semanticEquals.get());
                    break;
                }
            }
        }

        return Optional.of(constraint);
    }

    default ValueEquivalence then(@Nonnull final ValueEquivalence thenEquivalence) {
        return new ThenEquivalence(this, thenEquivalence);
    }

    /**
     * Helper equivalence to compose to equivalences.
     */
    class ThenEquivalence implements ValueEquivalence {
        @Nonnull
        private final ValueEquivalence first;
        @Nonnull
        private final ValueEquivalence then;

        public ThenEquivalence(@Nonnull final ValueEquivalence first, @Nonnull final ValueEquivalence then) {
            this.first = first;
            this.then = then;
        }

        @Nonnull
        @Override
        public Optional<QueryPlanConstraint> equivalence(@Nonnull final Value left, @Nonnull final Value right) {
            final var firstEquivalence = first.equivalence(left, right);
            if (firstEquivalence.isPresent()) {
                return firstEquivalence;
            }
            return then.equivalence(left, right);
        }

        @Nonnull
        @Override
        public Optional<QueryPlanConstraint> equivalence(@Nonnull final CorrelationIdentifier left, @Nonnull final CorrelationIdentifier right) {
            final var firstEquivalence = first.equivalence(left, right);
            if (firstEquivalence.isPresent()) {
                return firstEquivalence;
            }
            return then.equivalence(left, right);
        }
    }

    @Nonnull
    static ValueMap.Builder valueMapBuilder() {
        return new ValueMap.Builder();
    }

    /**
     * Value equivalence based on a map of values. Note that we do not enforce {@code a R a} (reflexivity),
     * nor {@code a R b => b R a} (symmetry). While reflexivity is naturally enforced even when the corresponding
     * pairs are not in the equivalence map, symmetry needs to be added (and enforced) by the client of this class
     */
    class ValueMap implements ValueEquivalence {
        @Nonnull
        private final Map<Value, Value> valueEquivalenceMap;

        @Nonnull
        private final Map<Value, Supplier<QueryPlanConstraint>> valueConstraintSupplierMap;

        private ValueMap(@Nonnull final Map<Value, Value> valueEquivalenceMap,
                         @Nonnull final Map<Value, Supplier<QueryPlanConstraint>> valueConstraintSupplierMap) {
            this.valueEquivalenceMap = ImmutableMap.copyOf(valueEquivalenceMap);
            this.valueConstraintSupplierMap = ImmutableMap.copyOf(valueConstraintSupplierMap);
        }

        @Nonnull
        @Override
        public Optional<QueryPlanConstraint> equivalence(@Nonnull final Value left, @Nonnull final Value right) {
            final var rightFromMap = valueEquivalenceMap.get(left);
            if (rightFromMap == null || !rightFromMap.equals(right)) {
                return Optional.empty();
            }
            return Optional.of(Objects.requireNonNull(valueConstraintSupplierMap.get(left)).get());
        }

        @Nonnull
        @Override
        public Optional<QueryPlanConstraint> equivalence(@Nonnull final CorrelationIdentifier left, @Nonnull final CorrelationIdentifier right) {
            return Optional.empty();
        }

        /**
         * Builder.
         */
        public static class Builder {
            @Nonnull
            private final Map<Value, Value> valueEquivalenceMap;
            @Nonnull
            private final Map<Value, Supplier<QueryPlanConstraint>> valueConstraintSupplierMap;

            private Builder() {
                this(new LinkedHashMap<>(), new LinkedHashMap<>());
            }

            private Builder(@Nonnull final Map<Value, Value> valueEquivalenceMap,
                            @Nonnull final Map<Value, Supplier<QueryPlanConstraint>> valueConstraintSupplierMap) {
                this.valueEquivalenceMap = valueEquivalenceMap;
                this.valueConstraintSupplierMap = valueConstraintSupplierMap;
            }

            @Nonnull
            public Builder add(@Nonnull final Value left, @Nonnull final Value right,
                               @Nonnull final Supplier<QueryPlanConstraint> planConstraintSupplier) {
                valueEquivalenceMap.put(left, right);
                valueConstraintSupplierMap.put(left, planConstraintSupplier);
                return this;
            }

            @Nonnull
            public ValueMap build() {
                return new ValueMap(valueEquivalenceMap, valueConstraintSupplierMap);
            }
        }
    }

    @Nonnull
    static AliasMapBackedValueEquivalence fromAliasMap(@Nonnull final AliasMap aliasMap) {
        return new AliasMapBackedValueEquivalence(aliasMap);
    }

    /**
     * Equivalence that is being backed by an {@link AliasMap}.
     */
    class AliasMapBackedValueEquivalence implements ValueEquivalence {
        @Nonnull
        private final AliasMap aliasMap;

        public AliasMapBackedValueEquivalence(@Nonnull final AliasMap aliasMap) {
            this.aliasMap = aliasMap;
        }

        @Nonnull
        @Override
        public Optional<QueryPlanConstraint> equivalence(@Nonnull final Value left, @Nonnull final Value right) {
            //
            // If any of the participants is not a quantified value, left is not equal to right.
            //
            if (!(left instanceof QuantifiedValue) || !(right instanceof QuantifiedValue)) {
                return Optional.empty();
            }

            if (left.getClass() != right.getClass()) {
                return Optional.empty();
            }

            final var leftAlias = ((QuantifiedValue)left).getAlias();
            final var rightAlias = ((QuantifiedValue)right).getAlias();

            if (leftAlias.equals(rightAlias) || aliasMap.containsMapping(leftAlias, rightAlias)) {
                return alwaysEqual();
            }

            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<QueryPlanConstraint> equivalence(@Nonnull final CorrelationIdentifier left, @Nonnull final CorrelationIdentifier right) {
            return aliasMap.containsMapping(left, right) ? ValueEquivalence.alwaysEqual() : Optional.empty();
        }
    }
}
