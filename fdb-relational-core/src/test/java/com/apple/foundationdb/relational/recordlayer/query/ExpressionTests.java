/*
 * ExpressionTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.test.BooleanSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tests for the plan generator {@link Expression} class.
 */
class ExpressionTests {
    private static final DataType.StructType REC_STRUCT_TYPE = DataType.StructType.from("rec", ImmutableList.of(
            DataType.StructType.Field.from("a", DataType.LongType.nullable(), 1),
            DataType.StructType.Field.from("b", DataType.StringType.notNullable(), 2)
    ), false);
    private static final Type.Record REC_TYPE = Type.Record.fromFields(false, ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a"), Optional.of(1)),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("b"), Optional.of(2))
    ));

    @Nonnull
    private static final Expression ANONYMOUS = new Expression(Optional.empty(), DataType.StringType.notNullable(), LiteralValue.ofScalar("hello"));
    @Nonnull
    private static final Expression FOO = new Expression(Optional.of(Identifier.of("foo")), DataType.LongType.notNullable(), LiteralValue.ofScalar(42L));
    @Nonnull
    private static final Expression BAR = new Expression(Optional.of(Identifier.of("bar")), REC_STRUCT_TYPE, QuantifiedObjectValue.of(Quantifier.current(), REC_TYPE));

    @Nonnull
    private static Expression createEphemeral(@Nonnull Expression expression) {
        final Expression ephemeral = expression.asEphemeral();
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(ephemeral)
                    .isInstanceOf(EphemeralExpression.class);
            softly.assertThat(ephemeral.getName())
                    .isEqualTo(expression.getName());
            softly.assertThat(ephemeral.getDataType())
                    .isSameAs(expression.getDataType());
            softly.assertThat(ephemeral.getUnderlying())
                    .isSameAs(expression.getUnderlying());
            softly.assertThat(ephemeral.asEphemeral())
                    .isSameAs(ephemeral);
        }
        return ephemeral;
    }

    @Nonnull
    static Stream<Expression> expressions() {
        return Stream.of(ANONYMOUS, FOO, BAR)
                .flatMap(expr -> expr.getName().isPresent() ? Stream.of(expr, createEphemeral(expr)) : Stream.of(expr));
    }

    @Nonnull
    static Stream<Arguments> withName() {
        final List<String> names = List.of("blah", "foo", "bar");
        return expressions().flatMap(expr ->
                names.stream().map(name -> Arguments.of(expr, Identifier.of(name))));
    }

    @ParameterizedTest
    @MethodSource
    void withName(@Nonnull Expression originalExpression, @Nonnull Identifier newName) {
        final Expression newExpression = originalExpression.withName(newName);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            if (originalExpression.getName().isPresent() && originalExpression.getName().get().equals(newName)) {
                softly.assertThat(newExpression)
                        .isSameAs(originalExpression)
                        .isEqualTo(originalExpression)
                        .hasSameHashCodeAs(originalExpression);
            } else {
                softly.assertThat(newExpression)
                        .isNotSameAs(originalExpression)
                        .isNotEqualTo(originalExpression)
                        .doesNotHaveSameHashCodeAs(originalExpression);
            }
            softly.assertThat(newExpression.getName())
                    .isPresent()
                    .get()
                    .isEqualTo(newName);
            softly.assertThat(newExpression.getDataType())
                    .isSameAs(originalExpression.getDataType());
            softly.assertThat(newExpression.getUnderlying())
                    .isSameAs(originalExpression.getUnderlying());

            softly.assertThat(newExpression.getClass())
                    .isEqualTo(originalExpression.getClass());
        }
    }

    @Nonnull
    static Stream<Arguments> withUnderlying() {
        return expressions().flatMap(expr ->
                Stream.of(Arguments.of(expr, expr.getUnderlying()), Arguments.of(expr, QuantifiedObjectValue.of(Quantifier.current(), expr.getUnderlying().getResultType()))));
    }

    @ParameterizedTest
    @MethodSource
    void withUnderlying(@Nonnull Expression originalExpression, @Nonnull Value newUnderlying) {
        final Expression newExpression = originalExpression.withUnderlying(newUnderlying);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            if (originalExpression.getUnderlying().semanticEquals(newUnderlying, AliasMap.emptyMap())) {
                softly.assertThat(newExpression)
                        .isSameAs(originalExpression)
                        .isEqualTo(originalExpression)
                        .hasSameHashCodeAs(originalExpression);
            } else {
                softly.assertThat(newExpression)
                        .isNotSameAs(originalExpression)
                        .isNotEqualTo(originalExpression)
                        .doesNotHaveSameHashCodeAs(originalExpression);
            }
            softly.assertThat(newExpression.getName())
                    .isEqualTo(originalExpression.getName());
            softly.assertThat(newExpression.getDataType())
                    .isSameAs(originalExpression.getDataType());
            softly.assertThat(newExpression.getUnderlying())
                    .isEqualTo(newUnderlying);

            softly.assertThat(newExpression.getClass())
                    .isEqualTo(originalExpression.getClass());
        }
    }

    @Nonnull
    private static Stream<Arguments> modifyQualifier() {
        return expressions().flatMap(expr ->
                Stream.of(
                        Arguments.of(expr, expr.withQualifier(ImmutableList.of("x", "y"))),
                        Arguments.of(expr, expr.replaceQualifier(ignore -> ImmutableList.of("x", "y"))),
                        Arguments.of(expr, expr.withQualifier(Optional.of(Identifier.of("y", ImmutableList.of("x")))))
                )
        );
    }

    @ParameterizedTest
    @MethodSource
    void modifyQualifier(final Expression expression, Expression qualified) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            if (expression.getName().isEmpty()) {
                softly.assertThat(qualified)
                        .isSameAs(expression);
            } else {
                softly.assertThat(qualified)
                        .isNotSameAs(expression);
                softly.assertThat(qualified.getName())
                        .get()
                        .isEqualTo(Identifier.of(expression.getName().get().getName(), ImmutableList.of("x", "y")));
            }

            // Underlying, type, value, and class should be the same
            softly.assertThat(qualified.getDataType())
                    .isSameAs(expression.getDataType());
            softly.assertThat(qualified.getUnderlying())
                    .isSameAs(expression.getUnderlying());
            softly.assertThat(qualified.getClass())
                    .isEqualTo(expression.getClass());
        }
    }

    @Nonnull
    private static Stream<Expression> clearQualifier() {
        return expressions().flatMap(expr ->
                Stream.of(
                        expr, expr.withQualifier(ImmutableList.of("a", "b"))
                )
        );
    }

    @ParameterizedTest
    @MethodSource
    void clearQualifier(@Nonnull Expression expression) {
        assertQualifierIsCleared(expression, expression.clearQualifier());
    }

    @ParameterizedTest
    @MethodSource("clearQualifier")
    void clearQualifierViaWithEmptyQualifier(@Nonnull Expression expression) {
        assertQualifierIsCleared(expression, expression.withQualifier(Optional.empty()));
    }

    private void assertQualifierIsCleared(@Nonnull Expression expression, @Nonnull Expression withClearedQualifier) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            if (expression.getName().isEmpty()) {
                softly.assertThat(withClearedQualifier)
                        .isSameAs(expression);
            } else {
                final Identifier originalName = expression.getName().get();
                if (originalName.getQualifier().isEmpty()) {
                    softly.assertThat(withClearedQualifier)
                            .isSameAs(expression);
                } else {
                    softly.assertThat(withClearedQualifier)
                            .isNotSameAs(expression);
                }
                softly.assertThat(withClearedQualifier.getName())
                        .get()
                        .matches(identifier -> identifier.getName().equals(originalName.getName()) && identifier.getQualifier().isEmpty());
            }

            // Underlying, type, value, and expression class should be preserved
            softly.assertThat(withClearedQualifier.getDataType())
                    .isSameAs(expression.getDataType());
            softly.assertThat(withClearedQualifier.getUnderlying())
                    .isSameAs(expression.getUnderlying());
            softly.assertThat(withClearedQualifier.getClass())
                    .isEqualTo(expression.getClass());
        }
    }

    @ParameterizedTest
    @BooleanSource
    void pullUp(boolean ephemeral) {
        final Value lower = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), REC_TYPE);
        Expression expression = Expression.fromUnderlying(FieldValue.ofFieldName(lower, "a")).withName(Identifier.of("blah"));
        if (ephemeral) {
            expression = expression.asEphemeral();
        }

        final Value newLower = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), REC_TYPE);
        final CorrelationIdentifier newId = Quantifier.uniqueId();
        final Expression newExpression = expression.pullUp(newLower, newId, ImmutableSet.of());

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(newExpression.getClass())
                    .isEqualTo(expression.getClass());
        }
    }
}
