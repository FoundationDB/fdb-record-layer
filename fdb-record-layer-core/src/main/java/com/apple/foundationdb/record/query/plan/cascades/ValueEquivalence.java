/*
 * ValueEquivalence.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint.alwaysTrue;
import static com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint.falseValue;
import static com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint.trueWithConstraint;

/**
 * An interface (together with a set of specific final classes) that captures axiomatic (defined) equivalence between
 * {@link Value}s. Implementors of this interface are passed in to calls to
 * {@link UsesValueEquivalence#semanticEquals(Object, ValueEquivalence)} and related methods to signal that there are
 * additional equalities that these methods need to consider. These equalities are axiomatic, i.e. they are assumed
 * to be true when e.g. semantic equality for e.g. two {@link Value}s is computed.
 * <br>
 * Example:
 * <pre>
 * {@code
 * semanticEquals(qov($i).a, qov($i).a) under no value equivalence --> true
 * semanticEquals(qov($i).a, qov($j).a) under no value equivalence --> false
 * semanticEquals(qov($i).a, qov($j).a) under alias equivalence ($i <-> $j) --> true
 * semanticEquals(qov($i).a.c, qov($j).b.c) under value equivalence (qov($i).a <-> qov($j).b) --> true
 * }
 * </pre>
 * <br>
 * Note that we do not enforce {@code a R a} (reflexivity) nor {@code a R b => b R a} (symmetry). While reflexivity is
 * naturally enforced in the absence of a defined equality, symmetry needs to be added (and enforced) by the client
 * of this class if necessary.
 */
public abstract class ValueEquivalence {
    @Nonnull
    private final Supplier<Optional<ValueEquivalence>> inverseOptionalSupplier;

    protected ValueEquivalence() {
        this.inverseOptionalSupplier = Suppliers.memoize(this::computeInverseMaybe);
    }

    @Nonnull
    public abstract BooleanWithConstraint isDefinedEqual(@Nonnull Value left,
                                                         @Nonnull Value right);

    @Nonnull
    public abstract BooleanWithConstraint isDefinedEqual(@Nonnull CorrelationIdentifier left,
                                                         @Nonnull CorrelationIdentifier right);

    /**
     * Method that returns the inverse of this value equivalence. Note that the inverse may not exist due to
     * ambiguous left sides that are equal to one right side. This does not happen currently, but it is possible
     * it may happen in the future, thus the inverse may or may not be defined.
     * @return the inverse if it exists, {@code Optional.empty()} otherwise.
     */
    @Nonnull
    public Optional<ValueEquivalence> inverseMaybe() {
        return inverseOptionalSupplier.get();
    }

    @Nonnull
    protected abstract Optional<ValueEquivalence> computeInverseMaybe();

    @Nonnull
    public <T extends UsesValueEquivalence<T>> BooleanWithConstraint semanticEquals(@Nonnull final Set<T> left,
                                                                                    @Nonnull final Set<T> right) {
        if (left.size() != right.size()) {
            return falseValue();
        }

        var constraint = alwaysTrue();
        for (final T l : left) {
            var found = false;
            for (final T r : right) {
                final var semanticEquals = l.semanticEquals(r, this);
                if (semanticEquals.isTrue()) {
                    found = true;
                    constraint = constraint.composeWithOther(semanticEquals);
                    break;
                }
            }
            if (!found) {
                return falseValue();
            }
        }

        return constraint;
    }

    @Nonnull
    public ValueEquivalence then(@Nonnull final ValueEquivalence thenEquivalence) {
        return new ThenEquivalence(this, thenEquivalence);
    }

    /**
     * Helper equivalence to compose to equivalences.
     */
    public static final class ThenEquivalence extends ValueEquivalence {
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
        public BooleanWithConstraint isDefinedEqual(@Nonnull final Value left, @Nonnull final Value right) {
            final var firstEquivalence = first.isDefinedEqual(left, right);
            if (firstEquivalence.isTrue()) {
                return firstEquivalence;
            }
            return then.isDefinedEqual(left, right);
        }

        @Nonnull
        @Override
        public BooleanWithConstraint isDefinedEqual(@Nonnull final CorrelationIdentifier left, @Nonnull final CorrelationIdentifier right) {
            final var firstEquivalence = first.isDefinedEqual(left, right);
            if (firstEquivalence.isTrue()) {
                return firstEquivalence;
            }
            return then.isDefinedEqual(left, right);
        }

        @Nonnull
        @Override
        protected Optional<ValueEquivalence> computeInverseMaybe() {
            return first.inverseMaybe()
                    .flatMap(inverseFirst -> {
                        final var inverseThenOptional = then.inverseMaybe();
                        if (inverseThenOptional.isEmpty()) {
                            return Optional.empty();
                        }
                        return Optional.of(new ThenEquivalence(inverseFirst, inverseThenOptional.get()));
                    });
        }
    }

    @Nonnull
    public static ValueMap.Builder valueMapBuilder() {
        return new ValueMap.Builder();
    }

    /**
     * Value equivalence based on a map of values.
     */
    public static final class ValueMap extends ValueEquivalence {
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
        @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE") // false-positive
        public BooleanWithConstraint isDefinedEqual(@Nonnull final Value left, @Nonnull final Value right) {
            final var rightFromMap = valueEquivalenceMap.get(left);
            if (rightFromMap == null || !rightFromMap.equals(right)) {
                return falseValue();
            }
            return trueWithConstraint(Objects.requireNonNull(Objects.requireNonNull(valueConstraintSupplierMap.get(left)).get()));
        }

        @Nonnull
        @Override
        public BooleanWithConstraint isDefinedEqual(@Nonnull final CorrelationIdentifier left, @Nonnull final CorrelationIdentifier right) {
            return falseValue();
        }

        @Nonnull
        @Override
        protected Optional<ValueEquivalence> computeInverseMaybe() {
            final var inverseValueEquivalenceMap =
                    new LinkedHashMap<Value, Value>();
            final var inverseValueConstraintSupplierMap =
                    new LinkedHashMap<Value, Supplier<QueryPlanConstraint>>();
            for (Map.Entry<Value, Value> entry : valueEquivalenceMap.entrySet()) {
                if (inverseValueEquivalenceMap.put(entry.getValue(), entry.getKey()) != null) {
                    return Optional.empty();
                }
                inverseValueConstraintSupplierMap.put(entry.getValue(), valueConstraintSupplierMap.get(entry.getKey()));
            }
            return Optional.of(new ValueMap(inverseValueEquivalenceMap, inverseValueConstraintSupplierMap));
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
                if (valueEquivalenceMap.put(left, right) != null) {
                    throw new RecordCoreException("duplicate mapping");
                }
                if (valueConstraintSupplierMap.put(left, planConstraintSupplier) != null) {
                    throw new RecordCoreException("duplicate constraint mapping");
                }
                return this;
            }

            @Nonnull
            public ValueMap build() {
                return new ValueMap(valueEquivalenceMap, valueConstraintSupplierMap);
            }
        }
    }

    @Nonnull
    public static ValueEquivalence fromAliasMap(@Nonnull final AliasMap aliasMap) {
        return new AliasMapBackedValueEquivalence(aliasMap);
    }

    /**
     * Equivalence that is being backed by an {@link AliasMap}.
     */
    public static final class AliasMapBackedValueEquivalence extends ValueEquivalence {
        @Nonnull
        private final AliasMap aliasMap;

        public AliasMapBackedValueEquivalence(@Nonnull final AliasMap aliasMap) {
            this.aliasMap = aliasMap;
        }

        @Nonnull
        @Override
        public BooleanWithConstraint isDefinedEqual(@Nonnull final Value left, @Nonnull final Value right) {
            //
            // If any of the participants is not a quantified value, left is not equal to right.
            //
            if (!(left instanceof QuantifiedValue) || !(right instanceof QuantifiedValue)) {
                return falseValue();
            }

            if (left.getClass() != right.getClass()) {
                return falseValue();
            }

            final var leftAlias = ((QuantifiedValue)left).getAlias();
            final var rightAlias = ((QuantifiedValue)right).getAlias();

            if (leftAlias.equals(rightAlias) || aliasMap.containsMapping(leftAlias, rightAlias)) {
                return alwaysTrue();
            }

            return falseValue();
        }

        @Nonnull
        @Override
        public BooleanWithConstraint isDefinedEqual(@Nonnull final CorrelationIdentifier left, @Nonnull final CorrelationIdentifier right) {
            return aliasMap.containsMapping(left, right) ? alwaysTrue() : falseValue();
        }

        @Nonnull
        @Override
        protected Optional<ValueEquivalence> computeInverseMaybe() {
            return Optional.of(new AliasMapBackedValueEquivalence(aliasMap.inverse()));
        }
    }

    @Nonnull
    public static ValueEquivalence constantEquivalenceWithEvaluationContext(@Nonnull final EvaluationContext evaluationContext) {
        return new ConstantValueEquivalence(evaluationContext);
    }

    /**
     * Value equivalence implementation that relates bound constant references between {@link ConstantObjectValue}s to
     * {@link LiteralValue}s.
     */
    public static class ConstantValueEquivalence extends ValueEquivalence {
        @Nonnull
        private final EvaluationContext evaluationContext;

        public ConstantValueEquivalence(@Nonnull final EvaluationContext evaluationContext) {
            this.evaluationContext = evaluationContext;
        }

        @Nonnull
        @Override
        public BooleanWithConstraint isDefinedEqual(@Nonnull final Value left, @Nonnull final Value right) {
            if (left instanceof ConstantObjectValue && right instanceof LiteralValue) {
                return isDefinedEqual((ConstantObjectValue)left, (LiteralValue<?>)right);
            } else if (right instanceof ConstantObjectValue && left instanceof LiteralValue) {
                // flip
                return isDefinedEqual((ConstantObjectValue)right, (LiteralValue<?>)left);
            }
            return falseValue();
        }

        @SpotBugsSuppressWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", justification = "compileTimeEval can return nullable")
        @Nonnull
        public BooleanWithConstraint isDefinedEqual(@Nonnull final ConstantObjectValue constantObjectValue,
                                                    @Nonnull final LiteralValue<?> literalValue) {
            final var constantObject = constantObjectValue.compileTimeEval(evaluationContext);
            final var literalObject = literalValue.getLiteralValue();
            if (constantObject == null && literalObject == null) {
                return trueWithConstraint(
                        QueryPlanConstraint.ofPredicate(new ValuePredicate(constantObjectValue,
                                new Comparisons.NullComparison(Comparisons.Type.IS_NULL))));
            }

            if (constantObject == null || literalObject == null) {
                return falseValue();
            }

            final boolean comparisonResult =
                    Objects.requireNonNull(Comparisons.evalComparison(Comparisons.Type.EQUALS, constantObject,
                            literalValue.getLiteralValue()));

            if (comparisonResult) {
                return trueWithConstraint(
                        QueryPlanConstraint.ofPredicate(new ValuePredicate(constantObjectValue,
                                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, literalObject))));
            }
            return falseValue();
        }

        @Nonnull
        @Override
        public BooleanWithConstraint isDefinedEqual(@Nonnull final CorrelationIdentifier left, @Nonnull final CorrelationIdentifier right) {
            return falseValue();
        }

        @Nonnull
        @Override
        protected Optional<ValueEquivalence> computeInverseMaybe() {
            // this equivalence is symmetrical
            return Optional.of(this);
        }
    }
}
