/*
 * ConstantFoldingTestUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantFoldingRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VariadicFunctionValue;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.fail;

public class ConstantFoldingTestUtils {

    private static int counter;

    public static final Type.Record lowerType = Type.Record.fromFields(false, List.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("a_non_null")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b_nullable"))
    ));

    public static final Type.Record upperType = Type.Record.fromFields(false, List.of(
            Type.Record.Field.of(lowerType.withNullability(false), Optional.of("a_non_null")),
            Type.Record.Field.of(lowerType.withNullability(true), Optional.of("b_nullable"))
    ));

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class ValueWrapper {

        private final Optional<EvaluationContext> evaluationContext;

        private final Value value;

        private ValueWrapper(final Value value, final Optional<EvaluationContext> evaluationContext) {
            this.evaluationContext = evaluationContext;
            this.value = value;
        }

        public Value value() {
            return value;
        }

        public Optional<EvaluationContext> getEvaluationContextMaybe() {
            return evaluationContext;
        }

        public EvaluationContext getEvaluationContext() {
            Verify.verify(getEvaluationContextMaybe().isPresent());
            return getEvaluationContextMaybe().get();
        }

        public EvaluationContext getEvaluationContextOrEmpty() {
            return evaluationContext.orElse(EvaluationContext.EMPTY);
        }

        public ValueWrapper withNewValue(Value value) {
            return new ValueWrapper(value, evaluationContext);
        }

        @Override
        public String toString() {
            if (value() instanceof ConstantObjectValue) {
                Verify.verify(evaluationContext.isPresent());
                return "(CoV → " + value().evalWithoutStore(evaluationContext.get()) + ")";
            }
            if (value instanceof LiteralValue<?>) {
                return "(ℓ " + ((LiteralValue<?>)value).getLiteralValue() + ")";
            }
            if (value instanceof NullValue) {
                return "∅";
            }
            return value.toString();
        }

        public EvaluationContext mergeEvaluationContext(final ValueWrapper that) {
            return mergeEvaluationContexts(this, that);
        }

        public static ValueWrapper of(final EvaluationContext evaluationContext, final Value value) {
            return ValueWrapper.of(Optional.of(evaluationContext), value);
        }

        public static ValueWrapper of(final Optional<EvaluationContext> evaluationContextMaybe, final Value value) {
            return new ValueWrapper(value, evaluationContextMaybe);
        }

        public static ValueWrapper of(final Value value) {
            return new ValueWrapper(value, Optional.empty());
        }

        public static EvaluationContext mergeEvaluationContexts(final ValueWrapper v1, final ValueWrapper v2) {
            return mergeEvaluationContexts(v1.evaluationContext, v2.evaluationContext);
        }

        public static EvaluationContext mergeEvaluationContexts(final Optional<EvaluationContext> v1,
                                                                final Optional<EvaluationContext>  v2) {
            if (v1.isEmpty() && v2.isEmpty()) {
                return EvaluationContext.empty();
            }
            if (v1.isEmpty()) {
                return v2.get();
            }
            if (v2.isEmpty()) {
                return v1.get();
            }
            final var thisBindingsChildBuilder = v1.get().getBindings().childBuilder();
            final var thatBindings = v2.get().getBindings();
            thatBindings.asMappingList().forEach(entry -> thisBindingsChildBuilder.set(entry.getKey(), entry.getValue()));
            return EvaluationContext.forBindings(thisBindingsChildBuilder.build());
        }

        public static EvaluationContext mergeEvaluationContexts(final ValueWrapper... vs) {
            return Arrays.stream(vs)
                    .map(ValueWrapper::getEvaluationContextMaybe)
                    .reduce(Optional.empty(),
                            (evaluationContext1, evaluationContext2) ->
                                    Optional.of(mergeEvaluationContexts(evaluationContext1,
                                            evaluationContext2)))
                    .orElse(EvaluationContext.EMPTY);
        }
    }

    public static ValueWrapper newCov(final Type type, @Nullable Object bindingValue) {
        final var correlationId = CorrelationIdentifier.uniqueId();
        final var constantId = String.valueOf(counter++);
        final var bindingKey = Bindings.Internal.CONSTANT.bindingName(correlationId.getId());
        final var bindingValueMap = new HashMap<String, Object>();
        bindingValueMap.put(constantId, bindingValue);
        return ValueWrapper.of(EvaluationContext.forBinding(bindingKey, bindingValueMap), ConstantObjectValue.of(correlationId, constantId, type));
    }

    public static ValueWrapper litNull() {
        return ValueWrapper.of(new NullValue(Type.nullType()));
    }

    public static ValueWrapper covNull() {
        return newCov(Type.nullType(), null);
    }

    public static ValueWrapper litFalse() {
        return ValueWrapper.of(LiteralValue.ofScalar(false));
    }

    public static ValueWrapper covFalse() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    }

    public static ValueWrapper litTrue() {
        return ValueWrapper.of(LiteralValue.ofScalar(true));
    }

    public static ValueWrapper covTrue() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    }

    public static ValueWrapper nonNullBoolean() {
        return qov(Type.primitiveType(Type.TypeCode.BOOLEAN, false));
    }

    public static ValueWrapper nullableBoolean() {
        return qov(Type.primitiveType(Type.TypeCode.BOOLEAN, true));
    }

    public static ValueWrapper litString(final String value) {
        return ValueWrapper.of(LiteralValue.ofScalar(value));
    }

    public static ValueWrapper litInt(int value) {
        return ValueWrapper.of(LiteralValue.ofScalar(value));
    }

    public static ValueWrapper notNullIntCov() {
        return newCov(Type.primitiveType(Type.TypeCode.INT, false), 42);
    }

    public static ValueWrapper throwingValue() {
        // this is for examining lazy evaluation of constant folding logic.
        // for example: NULL EQUALS <X | X IMMEDIATELY THROWS> should evaluate to NULL.
        return ValueWrapper.of(new ThrowsValue(Type.nullType()));
    }

    public static ValueWrapper coalesce(final ValueWrapper... valueWrappers) {
        final var evaluationContext = ValueWrapper.mergeEvaluationContexts(valueWrappers);
        final var values = Arrays.stream(valueWrappers).map(ValueWrapper::value).collect(ImmutableList.toImmutableList());
        final var value = (Value)new VariadicFunctionValue.CoalesceFn().encapsulate(CallSiteArguments.ofPositional(values));
        return new ValueWrapper(value, Optional.of(evaluationContext));
    }

    public static ValueWrapper promoteToBoolean(ValueWrapper valueWrapper) {
        final var promoteValue = new PromoteValue(valueWrapper.value(), Type.primitiveType(Type.TypeCode.BOOLEAN), null);
        return ValueWrapper.of(valueWrapper.getEvaluationContextMaybe(), promoteValue);
    }

    public static ValueWrapper qov(Type type) {
        final QuantifiedObjectValue qov = QuantifiedObjectValue.of(Quantifier.current(), type);
        return new ValueWrapper(qov, Optional.empty());
    }

    public static ValueWrapper fieldValue(ValueWrapper baseValue, String fieldName) {
        final FieldValue fieldValue = FieldValue.ofFieldNameAndFuseIfPossible(baseValue.value(), fieldName);
        return baseValue.withNewValue(fieldValue);
    }

    public static RangeConstraints buildSingletonRange(Comparisons.Type comparisonType) {
        return buildSingletonRange(comparisonType, null);
    }

    public static RangeConstraints buildSingletonRange(Comparisons.Type comparisonType, @Nullable final Value comparand) {
        Comparisons.Comparison comparison;
        switch (comparisonType) {
            case IS_NULL: // fallthrough
            case NOT_NULL:
                comparison = new Comparisons.NullComparison(comparisonType);
                break;
            default:
                comparison = new Comparisons.ValueComparison(comparisonType, Verify.verifyNotNull(comparand));
        }
        return buildMultiRange(Collections.singleton(comparison));
    }

    public static RangeConstraints buildMultiRange(Collection<Comparisons.Comparison> comparisons) {
        var constraintsBuilder = RangeConstraints.newBuilder();
        for (Comparisons.Comparison comparison : comparisons) {
            Assertions.assertThat(constraintsBuilder.addComparisonMaybe(comparison))
                    .as("should be able to add comparison: %s", comparison)
                    .isTrue();
        }
        return constraintsBuilder.build()
                .orElseGet(() -> fail("unable to construct range constraints over: " + comparisons));
    }

    public static QueryPredicate isNotNull(final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
    }

    public static QueryPredicate isNotNullAsRange(final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.NOT_NULL)));
    }

    public static QueryPredicate isNull(final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
    }

    public static QueryPredicate isNullAsRange(final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.IS_NULL)));
    }

    public static QueryPredicate areEqual(final Value value1, final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, value2));
    }

    public static QueryPredicate areEqualAsRange(final Value value1, final Value value2) {
        return PredicateWithValueAndRanges.ofRanges(value1, ImmutableSet.of(buildSingletonRange(Comparisons.Type.EQUALS, value2)));
    }

    public static QueryPredicate areNotNullAndEqualAsRange(final Value value1, final Value value2) {
        RangeConstraints multiRange = buildMultiRange(ImmutableSet.of(
                new Comparisons.NullComparison(Comparisons.Type.NOT_NULL),
                new Comparisons.ValueComparison(Comparisons.Type.EQUALS, value2)
        ));
        return PredicateWithValueAndRanges.ofRanges(value1, ImmutableSet.of(multiRange));
    }

    public static QueryPredicate areNotEqual(final Value value1, final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, value2));
    }

    public static QueryPredicate and(final QueryPredicate... preds) {
        return AndPredicate.and(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    public static QueryPredicate or(final QueryPredicate... preds) {
        return OrPredicate.or(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    public static QueryPredicate simplify(final QueryPredicate predicate) {
        return simplify(predicate, EvaluationContext.empty());
    }

    public static QueryPredicate simplify(final QueryPredicate predicate,
                                          final EvaluationContext evaluationContext) {
        final var result = Simplification.optimize(predicate,
                evaluationContext,
                AliasMap.emptyMap(),
                ImmutableSet.of(),
                ConstantFoldingRuleSet.ofSimplificationRules());
        return result.get();
    }
}
