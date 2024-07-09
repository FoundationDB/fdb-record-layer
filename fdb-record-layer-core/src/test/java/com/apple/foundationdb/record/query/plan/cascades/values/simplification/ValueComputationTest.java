/*
 * ValueComputationTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Test cases to exercise the computation engine over {@link Value} trees.
 */
class ValueComputationTest {
    private static final CorrelationIdentifier ALIAS = CorrelationIdentifier.of("_");
    private static final CorrelationIdentifier UPPER_ALIAS = CorrelationIdentifier.of("!");

    @Test
    void testPullUpFieldValue1() {
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());
        // (_.x as _0, _.z as _1)
        final var pulledThroughValue =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x")),
                        FieldValue.ofFieldName(someCurrentValue, "z")));

        // (_.x.xa, _.z)
        final var _x_xa = FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xa"));
        final var _z = FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("z"));
        final var toBePulledUpValues = ImmutableList.<Value>of(_x_xa, _z);

        //
        // We should understand that
        //
        // SELECT outer.*
        // FROM (SELECT (_.x, _.z)
        //       FROM T _
        //       ORDER BY (_.x.xa, _.z)) outer
        //
        // is the same as
        //
        // SELECT outer.*
        // FROM (SELECT (_.x, _.z)
        //       FROM T _) outer
        // ORDER BY (outer._0.xa, outer._1)
        //
        // The following pulls (_.x.xa, _.z) through (_.x, _.z) to form (outer._0.xa, outer._1).
        //

        final var resultsMap = pulledThroughValue.pullUp(toBePulledUpValues, AliasMap.emptyMap(), ImmutableSet.of(), ALIAS);

        final var upperCurrentValue = QuantifiedObjectValue.of(ALIAS, pulledThroughValue.getResultType());

        final var expectedMap = ImmutableMap.of(
                _x_xa, FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("_0", "xa")),
                _z, FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("_1")));

        Assertions.assertEquals(expectedMap, resultsMap);
    }

    @Test
    void testPullUpFieldValueThroughStreamingAggregation() {
        // _
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());

        // [_.x, _.z]
        final var _x = FieldValue.ofFieldName(someCurrentValue, "x");
        final var _z = FieldValue.ofFieldName(someCurrentValue, "z");
        final var toBePulledUpValues = ImmutableList.<Value>of(_x, _z);

        // (_.x, _.z)
        final var groupingValue = RecordConstructorValue.ofUnnamed(toBePulledUpValues);

        final var completeResultValue =
                RecordConstructorValue.ofUnnamed(
                        ImmutableList.of(
                                FieldValue.ofOrdinalNumber(groupingValue, 0),
                                FieldValue.ofOrdinalNumber(groupingValue, 1),
                                FieldValue.ofFieldName(someCurrentValue, "a")));
        
        final var resultsMap = completeResultValue.pullUp(toBePulledUpValues, AliasMap.emptyMap(), ImmutableSet.of(), ALIAS);

        // _
        final var someNewCurrentValue = QuantifiedObjectValue.of(ALIAS, completeResultValue.getResultType());

        final var expectedResult = ImmutableMap.of(
                _x, FieldValue.ofOrdinalNumber(someNewCurrentValue, 0),
                _z, FieldValue.ofOrdinalNumber(someNewCurrentValue, 1));
        Assertions.assertEquals(expectedResult, resultsMap);
    }

    @Test
    void testPullUpValue1() {
        final var someCurrentValue = QuantifiedObjectValue.of(ALIAS, someRecordType());
        // _
        final var pulledThroughValue =
                QuantifiedObjectValue.of(ALIAS, someRecordType());

        // recordType(alias)
        final var record_type = new RecordTypeValue(QuantifiedObjectValue.of(ALIAS, new Type.AnyRecord(true)));
        final var _a_b = FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("a", "ab"));
        final var _x_b = FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xb"));
        // _.a.ab + _.x.xb
        final var _a_ab__plus__x_xb = (Value)new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(_a_b, _x_b));
        final var toBePulledUpValues = ImmutableList.of(record_type, _a_ab__plus__x_xb);

        final var resultsMap = pulledThroughValue.pullUp(toBePulledUpValues, AliasMap.emptyMap(), ImmutableSet.of(), UPPER_ALIAS);
        final var upperCurrentValue = QuantifiedObjectValue.of(UPPER_ALIAS, pulledThroughValue.getResultType());

        final var new_a_b = FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("a", "ab"));
        final var new_x_b = FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("x", "xb"));

        final var expectedMap = ImmutableMap.of(
                record_type, new RecordTypeValue(QuantifiedObjectValue.of(UPPER_ALIAS, new Type.AnyRecord(true))),
                _a_ab__plus__x_xb, (Value)new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(new_a_b, new_x_b)));
        Assertions.assertEquals(expectedMap, resultsMap);
    }

    @Test
    void testPullUpValue2() {
        final var qov = QuantifiedObjectValue.of(ALIAS, someRecordType());
        // _
        final var pulledThroughValue =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(FieldValue.ofFieldNames(qov, ImmutableList.of("a", "ac")), qov));

        // recordType(alias)
        final var record_type = new RecordTypeValue(QuantifiedObjectValue.of(ALIAS, new Type.AnyRecord(true)));
        final var _a_b = FieldValue.ofFieldNames(qov, ImmutableList.of("a", "ab"));
        final var _x_b = FieldValue.ofFieldNames(qov, ImmutableList.of("x", "xb"));
        // _.a.ab + _.x.xb
        final var _a_ab__plus__x_xb = (Value)new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(_a_b, _x_b));
        final var toBePulledUpValues = ImmutableList.of(record_type, _a_ab__plus__x_xb);

        final var resultsMap = pulledThroughValue.pullUp(toBePulledUpValues, AliasMap.emptyMap(), ImmutableSet.of(), UPPER_ALIAS);
        final var upperCurrentValue = QuantifiedObjectValue.of(UPPER_ALIAS, pulledThroughValue.getResultType());

        final var new_a_b = FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("_1", "a", "ab"));
        final var new_x_b = FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("_1", "x", "xb"));

        final var expectedMap = ImmutableMap.of(
                record_type, new RecordTypeValue(QuantifiedObjectValue.of(UPPER_ALIAS, new Type.AnyRecord(true))),
                _a_ab__plus__x_xb, (Value)new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(new_a_b, new_x_b)));
        Assertions.assertEquals(expectedMap, resultsMap);
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
}
