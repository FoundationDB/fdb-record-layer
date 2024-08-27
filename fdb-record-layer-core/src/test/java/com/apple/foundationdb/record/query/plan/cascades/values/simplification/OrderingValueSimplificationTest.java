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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.TupleOrdering;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Test cases that test logic around the simplification of {@link Value} trees.
 */
class OrderingValueSimplificationTest {
    private static final CorrelationIdentifier ALIAS = CorrelationIdentifier.of("_");

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

        // ('fieldValue' as a, 10 as b, 'World' as c).b.a.ab
        final var fieldValue2 = FieldValue.ofFieldNames(recordConstructor, ImmutableList.of("b", "a", "ab"));

        // ('fieldValue' as a, 10 as b, 'World' as c).b.a.ab + 3
        final var arithmeticValue =
                (Value)new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(fieldValue2, LiteralValue.ofScalar(3)));


        final var simplifiedValue = simplifyOrderingValue(arithmeticValue);

        // ('fieldValue' as a, _ as b, 'World' as c).b.a.ab + 3 => _
        // meaning ORDER BY ('fieldValue' as a, _ as b, 'World' as c).b.a.ab + 3 <=> ORDER BY _.a.ab
        Assertions.assertEquals(FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "ab")), simplifiedValue);
    }

    @Test
    void testOrderingSimplification2() {
        // _
        final var someCurrentValue = QuantifiedObjectValue.of(Quantifier.current(), someRecordType());

        // ('fieldValue' as a, _ as b, 'World' as c)
        ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValue")),
                        Column.of(Optional.of("b"), someCurrentValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("c")),
                                LiteralValue.ofScalar("World")));
        final var recordConstructor = RecordConstructorValue.ofColumns(columns);

        // ('fieldValue' as a, _ as b, 'World' as c).b.a.ab
        final var fieldValueB = FieldValue.ofFieldNames(recordConstructor, ImmutableList.of("b", "a", "ab"));

        // ('fieldValue' as a, _ as b, 'World' as c).b.a.ab + 3
        final var arithmeticValue =
                (Value)new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(fieldValueB, LiteralValue.ofScalar(3)));

        // ('fieldValue' as a, _ as b, 'World' as c).a
        final var fieldValueA = FieldValue.ofFieldName(recordConstructor, "a");
        // ('fieldValue' as a, _ as b, 'World' as c).c
        final var fieldValueC = FieldValue.ofFieldName(recordConstructor, "c");

        // ((('fieldValue' as a, _ as b, 'World' as c).b.a.ab + 3, ('fieldValue' as a, _ as b, 'World' as c).a),
        //   ('fieldValue' as a, _ as b, 'World' as c).c)
        final var outerRecordConstructor =
                RecordConstructorValue.ofUnnamed(
                        ImmutableList.of(RecordConstructorValue.ofUnnamed(ImmutableList.of(arithmeticValue, fieldValueA)), fieldValueC));

        // record : = ('fieldValue' as a, _ as b, 'World' as c)
        // (record.b.a.ab + 3, record.a), record.c) -> (record.b.a.ab, record.a, record.c) -> (_, 'fieldValue', 'World')
        // meaning ORDER BY (record.b.a.ab + 3, record.a), record.c) <-> ORDER BY (_.a.ab, 'fieldValue', 'World')
        final var requestedOrdering = RequestedOrdering.ofParts(
                ImmutableList.of(new OrderingPart.RequestedOrderingPart(outerRecordConstructor, OrderingPart.RequestedSortOrder.ANY)),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS,
                ImmutableSet.of());

        final var simplifiedValues =
                requestedOrdering.getOrderingParts()
                        .stream()
                        .map(OrderingPart::getValue)
                        .collect(ImmutableList.toImmutableList());

        final var expectedResult =
                ImmutableList.of(
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "ab")),
                        LiteralValue.ofScalar("fieldValue"),
                        LiteralValue.ofScalar("World"));

        Assertions.assertEquals(expectedResult, simplifiedValues);
    }

    @Test
    void testRequestedOrderingSimplification() {
        final var someCurrentValue = ObjectValue.of(Quantifier.current(), someRecordType());
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

        final var requestedOrdering = RequestedOrdering.ofParts(
                ImmutableList.of(new OrderingPart.RequestedOrderingPart(recordConstructor4, OrderingPart.RequestedSortOrder.ANY)),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS,
                ImmutableSet.of());

        final var simplifiedValues =
                requestedOrdering.getOrderingParts()
                        .stream()
                        .map(OrderingPart::getValue)
                        .collect(ImmutableList.toImmutableList());

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
    void testDeriveOrderingValues1() {
        final var someCurrentValue = ObjectValue.of(Quantifier.current(), someRecordType());

        final var _a_aa_aaa = FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "aa", "aaa"));

        final var orderingPart =
                _a_aa_aaa.deriveOrderingPart(AliasMap.emptyMap(), ImmutableSet.of(), OrderingPart.ProvidedOrderingPart::new,
                        OrderingValueComputationRuleSet.usingProvidedOrderingParts());

        final var expectedResult =
                FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "aa", "aaa"));

        Assertions.assertEquals(ProvidedSortOrder.ASCENDING, orderingPart.getSortOrder());
        Assertions.assertEquals(expectedResult, orderingPart.getValue());
    }

    @Test
    void testDeriveOrderingValues2() {
        final var someCurrentValue = ObjectValue.of(Quantifier.current(), someRecordType());
        final var type = someRecordType();

        final var _a_aa_aaa = FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "aa", "aaa"));
        final var tob = new ToOrderedBytesValue(_a_aa_aaa, TupleOrdering.Direction.DESC_NULLS_LAST);

        final var orderingPart =
                tob.deriveOrderingPart(AliasMap.emptyMap(), ImmutableSet.of(), OrderingPart.ProvidedOrderingPart::new,
                        OrderingValueComputationRuleSet.usingProvidedOrderingParts());

        final var expectedResult =
                FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "aa", "aaa"));

        Assertions.assertEquals(ProvidedSortOrder.DESCENDING, orderingPart.getSortOrder());
        Assertions.assertEquals(expectedResult, orderingPart.getValue());
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
    private static Value simplifyOrderingValue(@Nonnull final Value toBeSimplified) {
        return Simplification.simplify(toBeSimplified, AliasMap.emptyMap(), ImmutableSet.of(),
                RequestedOrderingValueSimplificationRuleSet.ofRequestedOrderSimplificationRules());
    }
}
