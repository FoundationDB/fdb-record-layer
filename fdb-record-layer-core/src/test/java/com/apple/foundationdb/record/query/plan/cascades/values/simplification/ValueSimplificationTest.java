/*
 * SpecificMatchingTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * Testcase class for specific matching.
 */
class ValueSimplificationTest {
    @Test
    void testSimpleFieldValueComposition1() {
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")));
        final var fieldValue = FieldValue.ofFieldName(RecordConstructorValue.ofColumns(columns), "a");

        final var simplifiedValue = defaultSimplify(fieldValue);
        System.out.println(simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition2() {
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(5)));
        final var fieldValue = FieldValue.ofFieldName(RecordConstructorValue.ofColumns(columns), "b");

        final var simplifiedValue = defaultSimplify(fieldValue);
        System.out.println(simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition3() {
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var innerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(innerRecordConstructor.getResultType(), Optional.of("x")),
                                innerRecordConstructor),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("y")),
                                LiteralValue.ofScalar(5)));
        final var outerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        final var fieldValue = FieldValue.ofFieldName(outerRecordConstructor, "y");

        final var simplifiedValue = defaultSimplify(fieldValue);
        System.out.println(simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition4() {
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var innerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(innerRecordConstructor.getResultType(), Optional.of("x")),
                                innerRecordConstructor),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("y")),
                                LiteralValue.ofScalar(5)));
        final var outerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        final var fieldValue = FieldValue.ofFieldName(outerRecordConstructor, "x");

        final var simplifiedValue = defaultSimplify(fieldValue);
        System.out.println(simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition5() {
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var innerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(innerRecordConstructor.getResultType(), Optional.of("x")),
                                innerRecordConstructor),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("y")),
                                LiteralValue.ofScalar(5)));
        final var outerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        final var fieldValue = FieldValue.ofFieldNames(outerRecordConstructor, ImmutableList.of("x", "a"));

        final var simplifiedValue = defaultSimplify(fieldValue);
        System.out.println(simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition6() {
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var innerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(innerRecordConstructor.getResultType(), Optional.of("x")),
                                innerRecordConstructor),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("y")),
                                LiteralValue.ofScalar(5)));
        final var outerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        var fieldValue = FieldValue.ofFieldName(outerRecordConstructor, "x");
        fieldValue = FieldValue.ofFieldName(fieldValue, "a");

        final var simplifiedValue = defaultSimplify(fieldValue);
        System.out.println(simplifiedValue);
    }

    @Test
    void testSimpleOrdinalFieldValueComposition1() {
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var recordConstructor = RecordConstructorValue.ofColumns(columns);

        final var ordinalFieldValue = FieldValue.ofOrdinalNumber(recordConstructor, 1);

        final var simplifiedValue = defaultSimplify(ordinalFieldValue);
        System.out.println(simplifiedValue);
    }
    
    @Test
    void testProjectionPushDown1() {
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("c")),
                                LiteralValue.ofScalar("World")));
        final var recordConstructor = RecordConstructorValue.ofColumns(columns);

        final var fieldValue1 = FieldValue.ofFieldName(recordConstructor, "a");
        final var fieldValue2 = FieldValue.ofFieldName(recordConstructor, "b");

        final var outerRecordConstructor = RecordConstructorValue.ofUnnamed(ImmutableList.of(fieldValue1, fieldValue2));

        final var simplifiedValue = defaultSimplify(outerRecordConstructor);
        System.out.println(simplifiedValue);
    }

    @Test
    void testOrderingSimplification() {
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                ObjectValue.of(CorrelationIdentifier.CURRENT, Type.primitiveType(Type.TypeCode.INT))),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("c")),
                                LiteralValue.ofScalar("World")));
        final var recordConstructor = RecordConstructorValue.ofColumns(columns);

        final var fieldValue2 = FieldValue.ofFieldName(recordConstructor, "b");

        final var arithmeticValue =
                new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_II,
                        fieldValue2, LiteralValue.ofScalar(3));

        // ('fieldValue' as a, object as b, 'World' as c).b + 3 -> object
        // meaning ORDER BY ('fieldValue' as a, object as b, 'World' as c).b + 3 <-> ORDER BY object
        final var simplifiedValue = simplifyOrderingWithConstantAliases(arithmeticValue, ImmutableSet.of());
        System.out.println(simplifiedValue);
    }

    @Nonnull
    private static Value defaultSimplify(@Nonnull final Value toBeSimplified) {
        return toBeSimplified.simplify(DefaultValueSimplificationRuleSet.ofSimplificationRules(), ImmutableSet.of());
    }

    @Nonnull
    private static Value simplifyWithConstantAliases(@Nonnull final Value toBeSimplified, @Nonnull Set<CorrelationIdentifier> constantAliases) {
        return toBeSimplified.simplify(DefaultValueSimplificationRuleSet.ofSimplificationRules(), constantAliases);
    }

    @Nonnull
    private static Value simplifyOrderingWithConstantAliases(@Nonnull final Value toBeSimplified, @Nonnull Set<CorrelationIdentifier> constantAliases) {
        return toBeSimplified.simplify(OrderingValueSimplificationRuleSet.ofOrderingSimplificationRules(), constantAliases);
    }
}
