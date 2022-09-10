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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
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

    @Test
    void testPullUpFieldValue1() {
        final var someCurrentValue = ObjectValue.of(ALIAS, someRecordType());
        // (_.x as _0, _.z as _1)
        final var pulledThroughValue =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(
                        FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x")),
                        FieldValue.ofFieldName(someCurrentValue, "z")));

        // (_.x.xa, _.z)
        final var toBePulledUpValues = ImmutableList.<Value>of(
                FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("x", "xa")),
                FieldValue.ofFieldNames(someCurrentValue, ImmutableList.of("z")));

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

        final var resultsOptional = pulledThroughValue.pullUpMaybe(toBePulledUpValues, AliasMap.emptyMap(), ImmutableSet.of(), ALIAS);

        Assertions.assertTrue(resultsOptional.isPresent());

        final var upperCurrentValue = QuantifiedObjectValue.of(ALIAS, pulledThroughValue.getResultType());

        final var expectedMap = ImmutableList.<Value>of(
                FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("_0", "xa")),
                FieldValue.ofFieldNames(upperCurrentValue, ImmutableList.of("_1")));

        Assertions.assertEquals(expectedMap, resultsOptional.get());
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
        
        final var resultMapOptional = completeResultValue.pullUpMaybe(toBePulledUpValues, AliasMap.emptyMap(), ImmutableSet.of(), ALIAS);

        Assertions.assertTrue(resultMapOptional.isPresent());

        final var resultMap = resultMapOptional.get();

        // _
        final var someNewCurrentValue = QuantifiedObjectValue.of(ALIAS, completeResultValue.getResultType());

        final var expectedResult = ImmutableList.of(
                FieldValue.ofOrdinalNumber(someNewCurrentValue, 0),
                FieldValue.ofOrdinalNumber(someNewCurrentValue, 1));
        Assertions.assertEquals(expectedResult, resultMap);
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
