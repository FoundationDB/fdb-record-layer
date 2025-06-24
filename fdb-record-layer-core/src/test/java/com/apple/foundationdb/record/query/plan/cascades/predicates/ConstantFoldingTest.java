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

import com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.ValueWrapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.areEqual;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.areEqualAsRange;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.areNotEqual;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.coalesce;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.covFalse;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.covNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.covTrue;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNotNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNotNullAsRange;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNullAsRange;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litFalse;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litTrue;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.promoteToBoolean;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.throwingValue;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.notNullIntCov;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.simplify;

@SuppressWarnings("PMD.TooManyStaticImports")
public class ConstantFoldingTest {

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
                Arguments.arguments(covFalse(), covFalse(), ConstantPredicate.TRUE),

                Arguments.arguments(coalesce(litNull(), covFalse()), covTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(coalesce(covNull(), covTrue(), throwingValue()), covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(coalesce(covTrue(), covNull(), covFalse(), litFalse(), throwingValue()), covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(coalesce(covTrue(), litFalse()), covTrue(), ConstantPredicate.TRUE),

                Arguments.arguments(covTrue(), coalesce(litNull(), covFalse()), ConstantPredicate.FALSE),
                Arguments.arguments(covTrue(), coalesce(covNull(), covTrue(), throwingValue()), ConstantPredicate.TRUE),
                Arguments.arguments(covTrue(), coalesce(covTrue(), covNull(), covFalse(), litFalse(), throwingValue()), ConstantPredicate.TRUE),
                Arguments.arguments(covFalse(), coalesce(covFalse(), covNull()), ConstantPredicate.TRUE),

                Arguments.arguments(promoteToBoolean(litNull()), covTrue(), ConstantPredicate.NULL),
                Arguments.arguments(promoteToBoolean(litFalse()), covTrue(), ConstantPredicate.FALSE),
                Arguments.arguments(promoteToBoolean(covTrue()), covTrue(), ConstantPredicate.TRUE),
                Arguments.arguments(promoteToBoolean(covFalse()), covTrue(), ConstantPredicate.FALSE),

                Arguments.arguments(covTrue(), promoteToBoolean(litNull()), ConstantPredicate.NULL),
                Arguments.arguments(litFalse(), promoteToBoolean(litFalse()), ConstantPredicate.TRUE),
                Arguments.arguments(litTrue(), promoteToBoolean(covTrue()), ConstantPredicate.TRUE),
                Arguments.arguments(covTrue(), promoteToBoolean(covFalse()), ConstantPredicate.FALSE)
                );

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
                Arguments.arguments(litNull(), throwingValue(), ConstantPredicate.NULL),
                Arguments.arguments(covNull(), throwingValue(), ConstantPredicate.NULL),
                Arguments.arguments(throwingValue(), litNull(), ConstantPredicate.NULL),
                Arguments.arguments(throwingValue(), covNull(), ConstantPredicate.NULL),

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
