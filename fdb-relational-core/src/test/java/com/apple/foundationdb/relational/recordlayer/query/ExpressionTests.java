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
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
    private static final Type.Record REC_TYPE = Type.Record.fromFields(false, ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a"), Optional.of(1)),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("b"), Optional.of(2))
    ));

    @Nonnull
    private static final Expression ANONYMOUS = new Expression(Optional.empty(), DataType.StringType.notNullable(), LiteralValue.ofScalar("hello"));
    @Nonnull
    private static final Expression FOO = new Expression(Optional.of(Identifier.of("foo")), DataType.LongType.notNullable(), LiteralValue.ofScalar(42L));

    @BeforeAll
    static void setUpDebugger() {
        // Set up the debugger to ensure the correlation identifiers are stable. This test works without
        // the debugger, but the test names contain UUIDs, which makes it hard to read
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    @AfterAll
    static void tearDownDebugger() {
        // Clear out the debugger
        Debugger.setDebugger(null);
    }

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
        // Construct the bar expression in this method as it references a quantifier, and we want the quantifier
        // unique ID to use the debugger, and so we want it to construct a new one with each invocation
        final Expression bar = Expression.fromUnderlying(QuantifiedObjectValue.of(Quantifier.uniqueId(), REC_TYPE)).withName(Identifier.of("bar"));
        return Stream.of(ANONYMOUS, FOO, bar)
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
                    .isEqualTo(originalExpression.getDataType());
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

    private static boolean dataTypesEqualIgnoringStructName(@Nonnull DataType type1, @Nonnull DataType type2) {
        if (type1 instanceof DataType.StructType && type2 instanceof DataType.StructType) {
            final DataType.StructType structType1 = (DataType.StructType) type1;
            final DataType.StructType structType2 = (DataType.StructType) type2;
            if (structType1.isNullable() != structType2.isNullable()) {
                return false;
            }
            if (structType1.getFields().size() != structType2.getFields().size()) {
                return false;
            }
            for (int i = 0; i < structType1.getFields().size(); i++) {
                final DataType.StructType.Field field1 = structType1.getFields().get(i);
                final DataType.StructType.Field field2 = structType1.getFields().get(i);
                if (!field1.getName().equals(field2.getName())
                        || field1.getIndex() != field2.getIndex()
                        || !dataTypesEqualIgnoringStructName(field1.getType(), field2.getType())) {
                    return false;
                }
            }
            return true;
        }
        return type1.equals(type2);
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

            // The withUnderlying method can reconstruct the data type from the value's Type, which means
            // that structs (which get assigned a random name) can be modified by it. For that reason,
            // use a special comparison that ignores struct names.
            softly.assertThat(newExpression.getDataType())
                    .matches(dt -> dataTypesEqualIgnoringStructName(dt, originalExpression.getDataType()));

            // The name, value, and expression class should all be retained
            softly.assertThat(newExpression.getName())
                    .isEqualTo(originalExpression.getName());
            softly.assertThat(newExpression.getUnderlying())
                    .isEqualTo(newUnderlying);
            softly.assertThat(newExpression.getClass())
                    .isEqualTo(originalExpression.getClass());
        }
    }

    @Nonnull
    static Stream<Arguments> modifyQualifier() {
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

    @Nonnull
    private static Stream<Expression> variantsForPullUp(@Nonnull Expression expression) {
        final Expression withName1 = expression.withName(Identifier.of("blah"));
        final Expression withName2 = expression.withName(Identifier.of("blah", ImmutableList.of("qualifier")));
        return Stream.of(expression, withName1, withName1.asEphemeral(), withName2, withName2.asEphemeral());
    }

    @Nonnull
    static Stream<Arguments> pullUp() {
        // Start with an expression on a field value, then pull it through an RCV
        final Value lower = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), REC_TYPE);
        final Expression expr1 = Expression.fromUnderlying(FieldValue.ofFieldName(lower, "a"));
        final Value toPullThrough1 = RecordConstructorValue.ofColumns(ImmutableList.of(Column.of(Optional.of("x"), lower)));
        final CorrelationIdentifier newId1 = CorrelationIdentifier.uniqueId();
        final Value pulledThrough1 = FieldValue.ofFieldNames(QuantifiedObjectValue.of(newId1, toPullThrough1.getResultType()), ImmutableList.of("x", "a"));
        final Stream<Arguments> args1 = variantsForPullUp(expr1).map(x -> Arguments.of(x, toPullThrough1, newId1, pulledThrough1));

        // Pull up a literal value. This will not actually result in any changes to the value
        final Expression expr2 = Expression.fromUnderlying(LiteralValue.ofScalar(false));
        final CorrelationIdentifier newId2 = CorrelationIdentifier.uniqueId();
        final Value toPullThrough2 = QuantifiedObjectValue.of(newId2, REC_TYPE);
        final Stream<Arguments> args2 = variantsForPullUp(expr2).map(x -> Arguments.of(x, toPullThrough2, newId2, expr2.getUnderlying()));

        return Stream.concat(args1, args2);
    }

    @ParameterizedTest
    @MethodSource
    void pullUp(final Expression expression, @Nonnull Value toPullThrough, @Nonnull CorrelationIdentifier newId, @Nonnull Value pulledThrough) {
        final Expression newExpression = expression.pullUp(toPullThrough, newId, ImmutableSet.of());

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            if (expression.getUnderlying().equals(pulledThrough)) {
                softly.assertThat(newExpression)
                        .isSameAs(expression);
            }

            // Underlying value should now be pulled through
            softly.assertThat(newExpression.getUnderlying())
                    .isEqualTo(pulledThrough);

            // Name, type, and expression class should be retained
            softly.assertThat(newExpression.getName())
                    .isEqualTo(expression.getName());
            softly.assertThat(newExpression.getDataType())
                    .isEqualTo(expression.getDataType());
            softly.assertThat(newExpression.getClass())
                    .isEqualTo(expression.getClass());
        }
    }

    @Nonnull
    static Stream<Arguments> namedArgumentExpressionCreateNew() {
        final Expression.NamedArgumentExpression namedArg = FOO.toNamedArgument();
        final Identifier newName = Identifier.of("newName");
        final Value newValue = LiteralValue.ofScalar("newValue");

        return Stream.of(
                Arguments.of(namedArg.withName(newName)),
                Arguments.of(namedArg.withUnderlying(newValue)),
                Arguments.of(namedArg.withQualifier(List.of("q1", "q2"))),
                Arguments.of(namedArg.clearQualifier()),
                Arguments.of(namedArg.asHidden())
        );
    }

    @ParameterizedTest
    @MethodSource
    void namedArgumentExpressionCreateNew(@Nonnull Expression result) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(result)
                    .isInstanceOf(Expression.NamedArgumentExpression.class)
                    .matches(Expression::isNamedArgument, "should be a named argument");
        }
    }

    @Nonnull
    static Stream<Arguments> toNamedArgument() {
        return Stream.of(
                Arguments.of(FOO, Identifier.of("id")),
                Arguments.of(FOO, Identifier.of("foo")),
                Arguments.of(Expression.ofUnnamed(LiteralValue.ofScalar(true)), Identifier.of("id"))
        );
    }

    @ParameterizedTest
    @MethodSource
    void toNamedArgument(@Nonnull Expression expression, @Nonnull Identifier argumentName) {
        final Expression.NamedArgumentExpression namedArg = expression.toNamedArgument(argumentName);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(namedArg)
                    .isInstanceOf(Expression.NamedArgumentExpression.class)
                    .matches(Expression::isNamedArgument, "should be a named argument");

            softly.assertThat(namedArg.getName())
                    .isPresent()
                    .get()
                    .isEqualTo(argumentName);

            softly.assertThat(namedArg.getArgumentName())
                    .isEqualTo(argumentName);

            softly.assertThat(namedArg.getDataType())
                    .isEqualTo(expression.getDataType());

            softly.assertThat(namedArg.getUnderlying())
                    .isSameAs(expression.getUnderlying());

            final Expression.NamedArgumentExpression sameNamedArg = namedArg.toNamedArgument(argumentName);
            softly.assertThat(sameNamedArg)
                    .isSameAs(namedArg);
        }
    }
}
