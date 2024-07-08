/*
 * ValueSimplificationTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * TODO.
 */
class ValueSimplificationTest {
    private static final CorrelationIdentifier ALIAS = CorrelationIdentifier.of("_");

    @Test
    void testSimpleFieldValueComposition1() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // (_ as a)
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Optional.of("a"), someCurrentValue));
        // (_ as a).a
        final var fieldValue = FieldValue.ofFieldName(RecordConstructorValue.ofColumns(columns), "a");

        final var simplifiedValue = defaultSimplify(fieldValue);

        // (_ as a).a => _
        Assertions.assertEquals(someCurrentValue, simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition2() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // (_ as a, 5 as b)
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Optional.of("a"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(5)));
        // (_ as a, 5 as b).a
        final var fieldValue = FieldValue.ofFieldName(RecordConstructorValue.ofColumns(columns), "a");

        final var simplifiedValue = defaultSimplify(fieldValue);

        // (_ as a, 5 as b).a => _
        Assertions.assertEquals(someCurrentValue, simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition3() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // (_ as a, 10 as b)
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Optional.of("a"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var innerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        // ((_ as a, 10 as b) as x, 5 as y)
        columns =
                ImmutableList.of(
                        Column.of(Optional.of("x"), innerRecordConstructor),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("y")),
                                LiteralValue.ofScalar(5)));
        final var outerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        // ((_ as a, 10 as b) as x, 5 as y).x.a
        final var fieldValue = FieldValue.ofFieldNames(outerRecordConstructor, ImmutableList.of("x", "a"));

        final var simplifiedValue = defaultSimplify(fieldValue);

        // ((_ as a, 10 as b) as x, 5 as y).x.a => _
        Assertions.assertEquals(someCurrentValue, simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition4() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // (_ as a, 10 as b)
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Optional.of("a"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var innerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        // ((_ as a, 10 as b) as x, 5 as y)
        columns =
                ImmutableList.of(
                        Column.of(Optional.of("x"), innerRecordConstructor),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("y")),
                                LiteralValue.ofScalar(5)));
        final var outerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        // ((_ as a, 10 as b) as x, 5 as y).x
        final var fieldValue = FieldValue.ofFieldName(outerRecordConstructor, "x");

        final var simplifiedValue = defaultSimplify(fieldValue);

        // ((_ as a, 10 as b) as x, 5 as y).x => (_ as a, 10 as b)
        Assertions.assertEquals(innerRecordConstructor, simplifiedValue);
    }

    @Test
    void testSimpleFieldValueComposition5() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // (_ as a, 10 as b)
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Optional.of("a"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)));
        final var innerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        // ((_ as a, 10 as b) as x, 5 as y)
        columns =
                ImmutableList.of(
                        Column.of(Optional.of("x"), innerRecordConstructor),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("y")),
                                LiteralValue.ofScalar(5)));
        final var outerRecordConstructor = RecordConstructorValue.ofColumns(columns);

        // ((_ as a, 10 as b) as x, 5 as y).x
        var fieldValue = FieldValue.ofFieldName(outerRecordConstructor, "x");
        // (((_ as a, 10 as b) as x, 5 as y).x).a
        fieldValue = FieldValue.ofFieldName(fieldValue, "a");

        final var simplifiedValue = defaultSimplify(fieldValue);

        // (((_ as a, 10 as b) as x, 5 as y).x).a ==> _
        Assertions.assertEquals(someCurrentValue, simplifiedValue);
    }

    @Test
    void testSimpleOrdinalFieldValueComposition1() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // (_ as a, 5 as b)
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Optional.of("a"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(5)));
        // (_ as a, 5 as b)#0
        final var fieldValue = FieldValue.ofOrdinalNumber(RecordConstructorValue.ofColumns(columns), 0);

        final var simplifiedValue = defaultSimplify(fieldValue);

        // (_ as a, 5 as b)#0 => _
        Assertions.assertEquals(someCurrentValue, simplifiedValue);
    }
    
    @Test
    void testProjectionPushDown1() {
        // ('fieldValue' as a, 10 as b, 'World' as c)
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("b")),
                                LiteralValue.ofScalar(10)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("c")),
                                LiteralValue.ofScalar("World")));
        final var recordConstructor = RecordConstructorValue.ofColumns(columns);

        // ('fieldValue' as a, 10 as b, 'World' as c).a
        final var fieldValue1 = FieldValue.ofFieldName(recordConstructor, "a");

        // ('fieldValue' as a, 10 as b, 'World' as c).b
        final var fieldValue2 = FieldValue.ofFieldName(recordConstructor, "b");

        // (('fieldValue' as a, 10 as b, 'World' as c).a, ('fieldValue' as a, 10 as b, 'World' as c).b)
        final var outerRecordConstructor = RecordConstructorValue.ofUnnamed(ImmutableList.of(fieldValue1, fieldValue2));

        final var simplifiedValue = defaultSimplify(outerRecordConstructor);

        // (('fieldValue' as a, 10 as b, 'World' as c).a, ('fieldValue' as a, 10 as b, 'World' as c).b) => ('fieldValue, 10)
        Assertions.assertEquals(RecordConstructorValue.ofUnnamed(ImmutableList.of(
                LiteralValue.ofScalar("fieldValue"),
                LiteralValue.ofScalar(10))), simplifiedValue);
    }

    @Test
    void testOrderingSimplification1() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // ('fieldValue' as a, _ as b, 'World' as c)
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Optional.of("b"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("c")),
                                LiteralValue.ofScalar("World")));
        final var recordConstructor = RecordConstructorValue.ofColumns(columns);

        // ('fieldValue' as a, 10 as b, 'World' as c).b
        final var fieldValue2 = FieldValue.ofFieldName(recordConstructor, "b");

        // ('fieldValue' as a, 10 as b, 'World' as c).b + 3
        final var arithmeticValue =
                new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_II,
                        fieldValue2, LiteralValue.ofScalar(3));

        final var simplifiedValue = Iterables.getOnlyElement(simplifyOrderingValue(arithmeticValue));

        // ('fieldValue' as a, _ as b, 'World' as c).b + 3 => _
        // meaning ORDER BY ('fieldValue' as a, _ as b, 'World' as c).b + 3 <=> ORDER BY _
        Assertions.assertEquals(someCurrentValue, simplifiedValue);
    }

    @Test
    void testOrderingSimplification2() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // ('fieldValue' as a, _ as b, 'World' as c)
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Optional.of("b"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("c")),
                                LiteralValue.ofScalar("World")));
        final var recordConstructor = RecordConstructorValue.ofColumns(columns);

        // ('fieldValue' as a, _ as b, 'World' as c).b
        final var fieldValueB = FieldValue.ofFieldName(recordConstructor, "b");

        // ('fieldValue' as a, _ as b, 'World' as c).b + 3
        final var arithmeticValue =
                new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_II,
                        fieldValueB, LiteralValue.ofScalar(3));

        // ('fieldValue' as a, _ as b, 'World' as c).a
        final var fieldValueA = FieldValue.ofFieldName(recordConstructor, "a");
        // ('fieldValue' as a, _ as b, 'World' as c).c
        final var fieldValueC = FieldValue.ofFieldName(recordConstructor, "c");

        // ((('fieldValue' as a, _ as b, 'World' as c).b + 3, ('fieldValue' as a, _ as b, 'World' as c).a), ('fieldValue' as a, _ as b, 'World' as c).c)
        final var outerRecordConstructor =
                RecordConstructorValue.ofUnnamed(
                        ImmutableList.of(RecordConstructorValue.ofUnnamed(ImmutableList.of(arithmeticValue, fieldValueA)), fieldValueC));

        // record : = ('fieldValue' as a, _ as b, 'World' as c)
        // (record.b + 3, record.a), record.c) -> (record.b, record.a, record.c) -> (_, 'fieldValue', 'World')
        // meaning ORDER BY (record.b + 3, record.a), record.c) <-> ORDER BY (_, 'fieldValue', 'World')
        final var simplifiedValues = simplifyOrderingValue(outerRecordConstructor);


    }

    @Test
    void testOrderingSimplification3() {
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());
        final var recordConstructor1 =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xb")),
                        FieldValue.ofFieldName(someCurrentValue, "z")));

        final var recordConstructor2 =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xa")),
                        recordConstructor1,
                        FieldValue.ofFieldName(someCurrentValue, "z")));

        final var recordConstructor3 =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(
                        recordConstructor1,
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xc")),
                        FieldValue.ofFieldName(someCurrentValue, "z")));

        final var recordConstructor4 =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(recordConstructor1, recordConstructor2, recordConstructor3));

        final var simplifiedValues = simplifyOrderingValue(recordConstructor4);

        // ((_.x.xb as _0, _.z as _1) as _0, (_.x.xa as _0, (_.x.xb as _0, _.z as _1) as _1, _.z as _2) as _1, ((_.x.xb as _0, _.z as _1) as _0, _.x.xc as _1, _.z as _2) as _2)
        // ((_.x.xb, _.z), (_.x.xa, (_.x.xb, _.z), _.z), ((_.x.xb, _.z), _.x.xc, _.z))
        // (_.x.xb, _.z, _.x.xa, _.x.xb, _.z, _.z, _.x.xb, _.z, _.x.xc, _.z)

        final var expectedResult =
                ImmutableList.of(
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xb")),
                        FieldValue.ofFieldName(someCurrentValue, "z"),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xa")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xb")),
                        FieldValue.ofFieldName(someCurrentValue, "z"),
                        FieldValue.ofFieldName(someCurrentValue, "z"),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xb")),
                        FieldValue.ofFieldName(someCurrentValue, "z"),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xc")),
                        FieldValue.ofFieldName(someCurrentValue, "z"));

        Assertions.assertEquals(expectedResult, simplifiedValues);
    }

    @Test
    void testDeriveOrderingValues() {
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());
        final var type = someRecordType();

        final var orderingValues = Values.primitiveAccessorsForType(type, () -> someCurrentValue, ImmutableSet.of());

        final var expectedResult =
                ImmutableSet.of(
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "aa", "aaa")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "aa", "aab")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "aa", "aac")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "ab")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "ac")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xa")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xb")),
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xc")),
                        FieldValue.ofFieldName(someCurrentValue, "z"));

        Assertions.assertEquals(expectedResult, orderingValues);
    }

    @Test
    void testInvalidSimplification() {
        final var recordType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("pk")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("i")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("b"))));

        final var qov = QuantifiedObjectValue.of(Quantifier.current(), recordType);
        // _.i
        final var iField = FieldValue.ofFieldName(qov, "i");
        // _.b
        final var bField = FieldValue.ofFieldName(qov, "b");

        // (_.i, _.b)
        final var orderingKeyValues = ImmutableList.of(iField, bField);
        final var constantAliases = ImmutableSet.<CorrelationIdentifier>of();
        final var aliasMap = AliasMap.ofAliases(CorrelationIdentifier.of("q2"), Quantifier.current());

        // (q2) << note there is another record constructor around the record
        final var value = RecordConstructorValue.ofUnnamed(List.of(QuantifiedObjectValue.of(CorrelationIdentifier.of("q2"), recordType)));
        final var pulledUpValuesMap = value.pullUp(orderingKeyValues, aliasMap, constantAliases, Quantifier.current());
        Assertions.assertFalse(pulledUpValuesMap.isEmpty());

        final var qovPulledUp = QuantifiedObjectValue.of(Quantifier.current(), value.getResultType());
        // _._0
        final var zeroFieldPulledUp = FieldValue.ofOrdinalNumber(qovPulledUp, 0);

        // _._0.i
        final var iFieldPulledUp = FieldValue.ofFieldNameAndFuseIfPossible(zeroFieldPulledUp, "i");
        // _._0.b
        final var bFieldPulledUp = FieldValue.ofFieldNameAndFuseIfPossible(zeroFieldPulledUp, "b");

        Assertions.assertEquals(iFieldPulledUp, pulledUpValuesMap.get(iField));
        Assertions.assertEquals(bFieldPulledUp, pulledUpValuesMap.get(bField));
    }

    @Nonnull
    private static Type.Record someRecordType() {
        final var aaType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac"))));

        final var aType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aaType, Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac"))));

        final var xType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc"))));

        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aType, Optional.of("a")),
                Type.Record.Field.of(xType, Optional.of("x")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z"))));
    }

    @Nonnull
    private static Value defaultSimplify(@Nonnull final Value toBeSimplified) {
        return toBeSimplified.simplify(DefaultValueSimplificationRuleSet.ofSimplificationRules(), AliasMap.emptyMap(), ImmutableSet.of());
    }
    
    @Nonnull
    private static List<Value> simplifyOrderingValue(@Nonnull final Value toBeSimplified) {
        return toBeSimplified.simplifyOrderingValue(AliasMap.emptyMap(), ImmutableSet.of());
    }
}
