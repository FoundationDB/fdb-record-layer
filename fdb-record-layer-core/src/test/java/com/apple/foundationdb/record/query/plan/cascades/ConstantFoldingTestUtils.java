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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.fail;

public class ConstantFoldingTestUtils {

    private static int counter;

    @Nonnull
    public static final Type.Record lowerType = Type.Record.fromFields(false, List.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("a_non_null")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b_nullable"))
    ));

    @Nonnull
    public static final Type.Record upperType = Type.Record.fromFields(false, List.of(
            Type.Record.Field.of(lowerType.withNullability(false), Optional.of("a_non_null")),
            Type.Record.Field.of(lowerType.withNullability(true), Optional.of("b_nullable"))
    ));

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class ValueWrapper {

        @Nonnull
        private final Optional<EvaluationContext> evaluationContext;

        @Nonnull
        private final Value value;


        private ValueWrapper(@Nonnull final Value value, @Nonnull final Optional<EvaluationContext> evaluationContext) {
            this.evaluationContext = evaluationContext;
            this.value = value;
        }

        @Nonnull
        public Value value() {
            return value;
        }

        @Nonnull
        public Optional<EvaluationContext> getEvaluationContextMaybe() {
            return evaluationContext;
        }

        @Nonnull
        public EvaluationContext getEvaluationContext() {
            Verify.verify(getEvaluationContextMaybe().isPresent());
            return getEvaluationContextMaybe().get();
        }

        @Nonnull
        public EvaluationContext getEvaluationContextOrEmpty() {
            return evaluationContext.orElse(EvaluationContext.EMPTY);
        }

        @Nonnull
        public ValueWrapper withNewValue(@Nonnull Value value) {
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

        @Nonnull
        public EvaluationContext mergeEvaluationContext(@Nonnull final ValueWrapper that) {
            return mergeEvaluationContexts(this, that);
        }

        @Nonnull
        public static ValueWrapper of(@Nonnull final EvaluationContext evaluationContext, @Nonnull final Value value) {
            return ValueWrapper.of(Optional.of(evaluationContext), value);
        }

        @Nonnull
        public static ValueWrapper of(@Nonnull final Optional<EvaluationContext> evaluationContextMaybe, @Nonnull final Value value) {
            return new ValueWrapper(value, evaluationContextMaybe);
        }

        @Nonnull
        public static ValueWrapper of(@Nonnull final Value value) {
            return new ValueWrapper(value, Optional.empty());
        }

        @Nonnull
        public static EvaluationContext mergeEvaluationContexts(@Nonnull final ValueWrapper v1, @Nonnull final ValueWrapper v2) {
            return mergeEvaluationContexts(v1.evaluationContext, v2.evaluationContext);
        }

        @Nonnull
        public static EvaluationContext mergeEvaluationContexts(@Nonnull final Optional<EvaluationContext> v1,
                                                                @Nonnull final Optional<EvaluationContext>  v2) {
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

        @Nonnull
        public static EvaluationContext mergeEvaluationContexts(@Nonnull final ValueWrapper... vs) {
            return Arrays.stream(vs)
                    .map(ValueWrapper::getEvaluationContextMaybe)
                    .reduce(Optional.empty(),
                            (evaluationContext1, evaluationContext2) ->
                                    Optional.of(mergeEvaluationContexts(evaluationContext1,
                                            evaluationContext2)))
                    .orElse(EvaluationContext.EMPTY);
        }
    }

    @Nonnull
    public static ValueWrapper newCov(@Nonnull final Type type, @Nullable Object bindingValue) {
        final var correlationId = CorrelationIdentifier.uniqueId();
        final var constantId = String.valueOf(counter++);
        final var bindingKey = Bindings.Internal.CONSTANT.bindingName(correlationId.getId());
        final var bindingValueMap = new HashMap<String, Object>();
        bindingValueMap.put(constantId, bindingValue);
        return ValueWrapper.of(EvaluationContext.forBinding(bindingKey, bindingValueMap), ConstantObjectValue.of(correlationId, constantId, type));
    }

    @Nonnull
    public static ValueWrapper litNull() {
        return ValueWrapper.of(new NullValue(Type.nullType()));
    }

    @Nonnull
    public static ValueWrapper covNull() {
        return newCov(Type.nullType(), null);
    }

    @Nonnull
    public static ValueWrapper litFalse() {
        return ValueWrapper.of(LiteralValue.ofScalar(false));
    }

    @Nonnull
    public static ValueWrapper covFalse() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    }

    @Nonnull
    public static ValueWrapper litTrue() {
        return ValueWrapper.of(LiteralValue.ofScalar(true));
    }

    @Nonnull
    public static ValueWrapper covTrue() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    }

    @Nonnull
    public static ValueWrapper nonNullBoolean() {
        return qov(Type.primitiveType(Type.TypeCode.BOOLEAN, false));
    }

    @Nonnull
    public static ValueWrapper nullableBoolean() {
        return qov(Type.primitiveType(Type.TypeCode.BOOLEAN, true));
    }

    @Nonnull
    public static ValueWrapper litString(@Nonnull final String value) {
        return ValueWrapper.of(LiteralValue.ofScalar(value));
    }

    @Nonnull
    public static ValueWrapper litInt(int value) {
        return ValueWrapper.of(LiteralValue.ofScalar(value));
    }

    @Nonnull
    public static ValueWrapper notNullIntCov() {
        return newCov(Type.primitiveType(Type.TypeCode.INT, false), 42);
    }

    @Nonnull
    public static ValueWrapper throwingValue() {
        // this is for examining lazy evaluation of constant folding logic.
        // for example: NULL EQUALS <X | X IMMEDIATELY THROWS> should evaluate to NULL.
        return ValueWrapper.of(new ThrowsValue(Type.nullType()));
    }

    @Nonnull
    public static ValueWrapper coalesce(@Nonnull final ValueWrapper... valueWrappers) {
        final var evaluationContext = ValueWrapper.mergeEvaluationContexts(valueWrappers);
        final var values = Arrays.stream(valueWrappers).map(ValueWrapper::value).collect(ImmutableList.toImmutableList());
        final var value = (Value)new VariadicFunctionValue.CoalesceFn().encapsulate(values);
        return new ValueWrapper(value, Optional.of(evaluationContext));
    }

    @Nonnull
    public static ValueWrapper promoteToBoolean(@Nonnull ValueWrapper valueWrapper) {
        final var promoteValue = new PromoteValue(valueWrapper.value(), Type.primitiveType(Type.TypeCode.BOOLEAN), null);
        return ValueWrapper.of(valueWrapper.getEvaluationContextMaybe(), promoteValue);
    }

    @Nonnull
    public static ValueWrapper qov(Type type) {
        final QuantifiedObjectValue qov = QuantifiedObjectValue.of(Quantifier.current(), type);
        return new ValueWrapper(qov, Optional.empty());
    }

    @Nonnull
    public static ValueWrapper fieldValue(@Nonnull ValueWrapper baseValue, @Nonnull String fieldName) {
        final FieldValue fieldValue = FieldValue.ofFieldNameAndFuseIfPossible(baseValue.value(), fieldName);
        return baseValue.withNewValue(fieldValue);
    }

    @Nonnull
    public static RangeConstraints buildSingletonRange(@Nonnull Comparisons.Type comparisonType) {
        return buildSingletonRange(comparisonType, null);
    }

    @Nonnull
    public static RangeConstraints buildSingletonRange(@Nonnull Comparisons.Type comparisonType, @Nullable final Value comparand) {
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

    @Nonnull
    public static RangeConstraints buildMultiRange(@Nonnull Collection<Comparisons.Comparison> comparisons) {
        var constraintsBuilder = RangeConstraints.newBuilder();
        for (Comparisons.Comparison comparison : comparisons) {
            Assertions.assertThat(constraintsBuilder.addComparisonMaybe(comparison))
                    .as("should be able to add comparison: %s", comparison)
                    .isTrue();
        }
        return constraintsBuilder.build()
                .orElseGet(() -> fail("unable to construct range constraints over: " + comparisons));
    }

    @Nonnull
    public static QueryPredicate isNotNull(@Nonnull final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
    }

    @Nonnull
    public static QueryPredicate isNotNullAsRange(@Nonnull final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.NOT_NULL)));
    }

    @Nonnull
    public static QueryPredicate isNull(@Nonnull final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
    }

    @Nonnull
    public static QueryPredicate isNullAsRange(@Nonnull final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.IS_NULL)));
    }

    @Nonnull
    public static QueryPredicate areEqual(@Nonnull final Value value1, @Nonnull final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, value2));
    }

    @Nonnull
    public static QueryPredicate areEqualAsRange(@Nonnull final Value value1, @Nonnull final Value value2) {
        return PredicateWithValueAndRanges.ofRanges(value1, ImmutableSet.of(buildSingletonRange(Comparisons.Type.EQUALS, value2)));
    }

    @Nonnull
    public static QueryPredicate areNotNullAndEqualAsRange(@Nonnull final Value value1, @Nonnull final Value value2) {
        RangeConstraints multiRange = buildMultiRange(ImmutableSet.of(
                new Comparisons.NullComparison(Comparisons.Type.NOT_NULL),
                new Comparisons.ValueComparison(Comparisons.Type.EQUALS, value2)
        ));
        return PredicateWithValueAndRanges.ofRanges(value1, ImmutableSet.of(multiRange));
    }

    @Nonnull
    public static QueryPredicate areNotEqual(@Nonnull final Value value1, @Nonnull final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, value2));
    }

    @Nonnull
    public static QueryPredicate and(@Nonnull final QueryPredicate... preds) {
        return AndPredicate.and(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static QueryPredicate or(@Nonnull final QueryPredicate... preds) {
        return OrPredicate.or(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static QueryPredicate simplify(@Nonnull final QueryPredicate predicate) {
        return simplify(predicate, EvaluationContext.empty());
    }

    @Nonnull
    public static QueryPredicate simplify(@Nonnull final QueryPredicate predicate,
                                          @Nonnull final EvaluationContext evaluationContext) {
        final var result = Simplification.optimize(predicate,
                evaluationContext,
                AliasMap.emptyMap(),
                ImmutableSet.of(),
                ConstantFoldingRuleSet.ofSimplificationRules());
        return result.get();
    }
}
