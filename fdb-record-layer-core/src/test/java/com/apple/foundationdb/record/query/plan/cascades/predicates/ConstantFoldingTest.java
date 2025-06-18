/*
 * ConstantFoldingTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantFoldingRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

public class ConstantFoldingTest {

    private static int counter;

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

        @Override
        public String toString() {
            if (value() instanceof ConstantObjectValue) {
                Verify.verify(evaluationContext.isPresent());
                return "(CoV → " + value().evalWithoutStore(evaluationContext.get()) + ")";
            }
            if (value instanceof LiteralValue<?>) {
                return "(ℓ " + ((LiteralValue<?>)value).getLiteralValue() + ")";
            }
            Verify.verify(value instanceof NullValue);
            return "∅";
        }

        @Nonnull
        public EvaluationContext mergeEvaluationContext(@Nonnull final ValueWrapper that) {
            if (evaluationContext.isEmpty() && that.evaluationContext.isEmpty()) {
                return EvaluationContext.empty();
            }
            if (evaluationContext.isEmpty()) {
                return that.evaluationContext.get();
            }
            if (that.evaluationContext.isEmpty()) {
                return evaluationContext.get();
            }
            final var thisBindingsChildBuilder = evaluationContext.get().getBindings().childBuilder();
            final var thatBindings = that.evaluationContext.get().getBindings();
            thatBindings.asMappingList().forEach(entry -> thisBindingsChildBuilder.set(entry.getKey(), entry.getValue()));
            return EvaluationContext.forBindings(thisBindingsChildBuilder.build());
        }

        @Nonnull
        public static ValueWrapper of(@Nonnull final EvaluationContext evaluationContext, @Nonnull final ConstantObjectValue constantObjectValue) {
            return new ValueWrapper(constantObjectValue, Optional.of(evaluationContext));
        }

        @Nonnull
        public static ValueWrapper of(@Nonnull final Value value) {
            return new ValueWrapper(value, Optional.empty());
        }
    }

    @Nonnull
    private static ValueWrapper newCov(@Nonnull final Type type, @Nullable Object bindingValue) {
        final var correlationId = CorrelationIdentifier.uniqueID();
        final var constantId = String.valueOf(counter++);
        final var bindingKey = Bindings.Internal.CONSTANT.bindingName(correlationId.getId());
        final var bindingValueMap = new HashMap<String, Object>();
        bindingValueMap.put(constantId, bindingValue);
        return ValueWrapper.of(EvaluationContext.forBinding(bindingKey, bindingValueMap), ConstantObjectValue.of(correlationId, constantId, type));
    }

    @Nonnull
    static ValueWrapper litNull() {
        return ValueWrapper.of(new NullValue(Type.nullType()));
    }

    @Nonnull
    static ValueWrapper covNull() {
        return newCov(Type.nullType(), null);
    }

    @Nonnull
    static ValueWrapper litFalse() {
        return ValueWrapper.of(LiteralValue.ofScalar(false));
    }

    @Nonnull
    static ValueWrapper covFalse() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    }

    @Nonnull
    static ValueWrapper litTrue() {
        return ValueWrapper.of(LiteralValue.ofScalar(true));
    }

    @Nonnull
    static ValueWrapper covTrue() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    }

    @Nonnull
    static ValueWrapper notNullIntCov() {
        return newCov(Type.primitiveType(Type.TypeCode.INT, false), 42);
    }

    @Nonnull
    static RangeConstraints buildSingletonRange(@Nonnull Comparisons.Type comparisonType) {
        return buildSingletonRange(comparisonType, null);
    }

    @Nonnull
    static RangeConstraints buildSingletonRange(@Nonnull Comparisons.Type comparisonType, @Nullable final Value comparand) {
        final var singletonRangeBuilder = RangeConstraints.newBuilder();
        switch (comparisonType) {
            case IS_NULL: // fallthrough
            case NOT_NULL:
                singletonRangeBuilder.addComparisonMaybe(new Comparisons.NullComparison(comparisonType));
                break;
            default:
                singletonRangeBuilder.addComparisonMaybe(new Comparisons.ValueComparison(comparisonType, Verify.verifyNotNull(comparand)));
        }
        final var singletonRange = singletonRangeBuilder.build();
        Assertions.assertThat(singletonRange).isPresent();
        return singletonRange.get();
    }

    @Nonnull
    static QueryPredicate isNotNull(@Nonnull final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
    }

    @Nonnull
    static QueryPredicate isNotNullAsRange(@Nonnull final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.NOT_NULL)));
    }

    @Nonnull
    static QueryPredicate isNull(@Nonnull final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
    }

    @Nonnull
    static QueryPredicate isNullAsRange(@Nonnull final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.IS_NULL)));

    }

    @Nonnull
    static QueryPredicate areEqual(@Nonnull final Value value1, @Nonnull final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, value2));
    }

    @Nonnull
    static QueryPredicate areEqualAsRange(@Nonnull final Value value1, @Nonnull final Value value2) {
        return PredicateWithValueAndRanges.ofRanges(value1, ImmutableSet.of(buildSingletonRange(Comparisons.Type.EQUALS, value2)));

    }

    @Nonnull
    static QueryPredicate areNotEqual(@Nonnull final Value value1, @Nonnull final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, value2));
    }

    @Nonnull
    static QueryPredicate and(@Nonnull final QueryPredicate... preds) {
        return AndPredicate.and(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    static QueryPredicate or(@Nonnull final QueryPredicate... preds) {
        return OrPredicate.or(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    static QueryPredicate simplify(@Nonnull final QueryPredicate predicate) {
        return simplify(predicate, EvaluationContext.empty());
    }

    @Nonnull
    static QueryPredicate simplify(@Nonnull final QueryPredicate predicate,
                                   @Nonnull final EvaluationContext evaluationContext) {
        final var result = Simplification.optimize(predicate,
                evaluationContext,
                AliasMap.emptyMap(),
                ImmutableSet.of(),
                ConstantFoldingRuleSet.ofSimplificationRules());
        return result.get();
    }

    ///
    /// EQUALS simplification tests
    ///

    @Nonnull
    public static Stream<Arguments> equalTestArguments() {
        return Stream.of(
                Arguments.arguments(litNull(), litNull(), ConstantPredicate.NULL),

                Arguments.arguments(litNull(), covTrue(), ConstantPredicate.NULL),
                Arguments.arguments(litNull(), covFalse(), ConstantPredicate.NULL),
                Arguments.arguments(litNull(), litTrue(), ConstantPredicate.NULL),
                Arguments.arguments(litNull(), litFalse(), ConstantPredicate.NULL),

                Arguments.arguments(covTrue(), litNull(), ConstantPredicate.NULL),
                Arguments.arguments(covFalse(), litNull(), ConstantPredicate.NULL),
                Arguments.arguments(litTrue(), litNull(), ConstantPredicate.NULL),
                Arguments.arguments(litFalse(), litNull(), ConstantPredicate.NULL),

                Arguments.arguments(litFalse(), covTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(litTrue(), covFalse(), ConstantPredicate.FALSE),
                Arguments.arguments(litTrue(), covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(litFalse(), covFalse(), ConstantPredicate.TRUE),

                Arguments.arguments(covFalse(), litTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(covTrue(), litFalse(), ConstantPredicate.FALSE),
                Arguments.arguments(covTrue(), litTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(covFalse(), litFalse(), ConstantPredicate.TRUE),

                Arguments.arguments(litFalse(), litTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(litTrue(), litFalse(), ConstantPredicate.FALSE),
                Arguments.arguments(litTrue(), litTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(litFalse(), litFalse(), ConstantPredicate.TRUE),

                Arguments.arguments(covFalse(), covTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(covTrue(), covFalse(), ConstantPredicate.FALSE),
                Arguments.arguments(covTrue(), covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(covFalse(), covFalse(), ConstantPredicate.TRUE));
    }

    @ParameterizedTest(name = "{0} = {1} ≡ {2}")
    @MethodSource("equalTestArguments")
    public void valuePredicateEquality(ValueWrapper value1, ValueWrapper value2, QueryPredicate queryPredicate) {
        final var evaluationContext = value1.mergeEvaluationContext(value2);
        final var result = simplify(areEqual(value1.value(), value2.value()), evaluationContext);
        Assertions.assertThat(result).isEqualTo(queryPredicate);
    }

    @ParameterizedTest(name = "{0} = [{1}, {1}] ≡ {2}")
    @MethodSource("equalTestArguments")
    public void predicateValueWithRangesEquality(ValueWrapper value1, ValueWrapper value2, QueryPredicate queryPredicate) {
        final var evaluationContext = value1.mergeEvaluationContext(value2);
        final var result = simplify(areEqualAsRange(value1.value(), value2.value()), evaluationContext);
        Assertions.assertThat(result).isEqualTo(queryPredicate);
    }

    ///
    /// NOT EQUALS simplification tests
    ///

    @Nonnull
    public static Stream<Arguments> notEqualsTestArguments() {
        return Stream.of(
                Arguments.arguments(litNull(), litNull(), ConstantPredicate.NULL),

                Arguments.arguments(litNull(), covTrue(), ConstantPredicate.NULL),
                Arguments.arguments(litNull(), covFalse(), ConstantPredicate.NULL),
                Arguments.arguments(litNull(), litTrue(), ConstantPredicate.NULL),
                Arguments.arguments(litNull(), litFalse(), ConstantPredicate.NULL),

                Arguments.arguments(covTrue(), litNull(), ConstantPredicate.NULL),
                Arguments.arguments(covFalse(), litNull(), ConstantPredicate.NULL),
                Arguments.arguments(litTrue(), litNull(), ConstantPredicate.NULL),
                Arguments.arguments(litFalse(), litNull(), ConstantPredicate.NULL),

                Arguments.arguments(litFalse(), covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(litTrue(), covFalse(), ConstantPredicate.TRUE),
                Arguments.arguments(litTrue(), covTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(litFalse(), covFalse(), ConstantPredicate.FALSE),

                Arguments.arguments(covFalse(), litTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(covTrue(), litFalse(), ConstantPredicate.TRUE),
                Arguments.arguments(covTrue(), litTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(covFalse(), litFalse(), ConstantPredicate.FALSE),

                Arguments.arguments(litFalse(), litTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(litTrue(), litFalse(), ConstantPredicate.TRUE),
                Arguments.arguments(litTrue(), litTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(litFalse(), litFalse(), ConstantPredicate.FALSE),

                Arguments.arguments(covFalse(), covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(covTrue(), covFalse(), ConstantPredicate.TRUE),
                Arguments.arguments(covTrue(), covTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(covFalse(), covFalse(), ConstantPredicate.FALSE));
    }

    @ParameterizedTest(name = "{0} ≠ {1} ≡ {2}")
    @MethodSource("notEqualsTestArguments")
    public void valuePredicateNotEqualsOfConstantObjectValues(ValueWrapper value1, ValueWrapper value2, QueryPredicate queryPredicate) {
        final var evaluationContext = value1.mergeEvaluationContext(value2);
        final var result = simplify(areNotEqual(value1.value(), value2.value()), evaluationContext);
        Assertions.assertThat(result).isEqualTo(queryPredicate);
    }

    // it is not possible to construct a RangeConstraint with NOT_EQUALS comparison since it can not be used as a
    // scan prefix. Therefore, it is not possible to test case (since we can't even construct it).

    ///
    /// IS NULL simplification tests
    ///

    @Nonnull
    public static Stream<Arguments> isNullTests() {
        return Stream.of(
                Arguments.arguments(litNull(), ConstantPredicate.TRUE),
                Arguments.arguments(covNull(), ConstantPredicate.TRUE),
                Arguments.arguments(litFalse(), ConstantPredicate.FALSE),
                Arguments.arguments(covFalse(), ConstantPredicate.FALSE),
                Arguments.arguments(litTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(covTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(notNullIntCov(), ConstantPredicate.FALSE));
    }

    @ParameterizedTest(name = "{0} is null ≡ {1}")
    @MethodSource("isNullTests")
    public void valuePredicateIsNull(@Nonnull ValueWrapper value, @Nonnull QueryPredicate queryPredicate) {
        final var evaluationContextMaybe = value.getEvaluationContextMaybe();
        if (evaluationContextMaybe.isPresent()) {
            Assertions.assertThat(simplify(isNull(value.value()), evaluationContextMaybe.get())).isEqualTo(queryPredicate);
        } else {
            Assertions.assertThat(simplify(isNull(value.value()))).isEqualTo(queryPredicate);
        }
    }

    @ParameterizedTest(name = "{0} is null ≡ {1}")
    @MethodSource("isNullTests")
    public void predicateValueWithRangesIsNull(@Nonnull ValueWrapper value, @Nonnull QueryPredicate queryPredicate) {
        final var evaluationContextMaybe = value.getEvaluationContextMaybe();
        if (evaluationContextMaybe.isPresent()) {
            Assertions.assertThat(simplify(isNullAsRange(value.value()), evaluationContextMaybe.get())).isEqualTo(queryPredicate);
        } else {
            Assertions.assertThat(simplify(isNullAsRange(value.value()))).isEqualTo(queryPredicate);
        }
    }

    ///
    /// IS NOT NULL simplification tests
    ///

    @Nonnull
    public static Stream<Arguments> isNotNullTests() {
        return Stream.of(
                Arguments.arguments(litNull(), ConstantPredicate.FALSE),
                Arguments.arguments(covNull(), ConstantPredicate.FALSE),
                Arguments.arguments(litFalse(), ConstantPredicate.TRUE),
                Arguments.arguments(covFalse(), ConstantPredicate.TRUE),
                Arguments.arguments(litTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(notNullIntCov(), ConstantPredicate.TRUE));
    }

    @ParameterizedTest(name = "{0} is null ≡ {1}")
    @MethodSource("isNotNullTests")
    public void valuePredicateIsNotNull(@Nonnull ValueWrapper value, @Nonnull QueryPredicate queryPredicate) {
        final var evaluationContextMaybe = value.getEvaluationContextMaybe();
        if (evaluationContextMaybe.isPresent()) {
            Assertions.assertThat(simplify(isNotNull(value.value()), evaluationContextMaybe.get())).isEqualTo(queryPredicate);
        } else {
            Assertions.assertThat(simplify(isNotNull(value.value()))).isEqualTo(queryPredicate);
        }
    }

    @ParameterizedTest(name = "{0} is null ≡ {1}")
    @MethodSource("isNotNullTests")
    public void predicateValueWithRangesIsNotNull(@Nonnull ValueWrapper value, @Nonnull QueryPredicate queryPredicate) {
        final var evaluationContextMaybe = value.getEvaluationContextMaybe();
        if (evaluationContextMaybe.isPresent()) {
            Assertions.assertThat(simplify(isNotNullAsRange(value.value()), evaluationContextMaybe.get())).isEqualTo(queryPredicate);
        } else {
            Assertions.assertThat(simplify(isNotNullAsRange(value.value()))).isEqualTo(queryPredicate);
        }
    }
}
