/*
 * PromoteValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PromoteValue}.
 */
class PromoteValueTest {
    private static final Type INT = Type.primitiveType(Type.TypeCode.INT);
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG);
    private static final Type FLOAT = Type.primitiveType(Type.TypeCode.FLOAT);
    private static final Type DOUBLE = Type.primitiveType(Type.TypeCode.DOUBLE);
    private static final Type STRING = Type.primitiveType(Type.TypeCode.STRING);
    private static final Type BOOLEAN = Type.primitiveType(Type.TypeCode.BOOLEAN);
    private static final Type UUID = Type.uuidType(true);
    private static final Type ENUM = Type.Enum.fromValues(true, ImmutableList.of(
            Type.Enum.EnumValue.from("A", 1), Type.Enum.EnumValue.from("B", 2)));

    private static final Type.Record RECORD_A_INT = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(INT, Optional.of("a"))));
    private static final Type.Record RECORD_A_LONG = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(LONG, Optional.of("a"))));
    private static final Type.Record RECORD_B_INT = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(LONG, Optional.of("b"))));
    private static final Type.Record NAMED_T1_A_INT = RECORD_A_INT.withName("T1");
    private static final Type.Record NAMED_T1_A_LONG = RECORD_A_LONG.withName("T1");
    private static final Type.Record NAMED_T2_A_INT = RECORD_A_INT.withName("T2");

    private static final Type.Array INT_ARRAY = new Type.Array(INT);
    private static final Type.Array LONG_ARRAY = new Type.Array(LONG);

    /**
     * Tests that a {@link PromoteValue.ArrayCoercionBiFunction} survives a round trip through its proto representation
     * when its elements trie refers to a record type that is defined by its array types.
     */
    @Test
    void testArrayCoercionBiFunctionSerialization() {
        final MessageHelpers.CoercionBiFunction nested = new PromoteValue.ArrayCoercionBiFunction(
                new Type.Array(false, ValueTestHelpers.namedRecordType()),
                new Type.Array(true, ValueTestHelpers.namedRecordType()), null);
        final PromoteValue.ArrayCoercionBiFunction function = new PromoteValue.ArrayCoercionBiFunction(
                new Type.Array(false, ValueTestHelpers.namedRecordType()),
                new Type.Array(true, ValueTestHelpers.namedRecordType()),
                new MessageHelpers.CoercionTrieNode(nested, null));

        final var proto = function.toProto(PlanSerializationContext.newForCurrentMode());
        final var deserialized = PromoteValue.ArrayCoercionBiFunction.fromProto(
                PlanSerializationContext.newForCurrentMode(), proto);

        assertThat(deserialized.toProto(PlanSerializationContext.newForCurrentMode())).isEqualTo(proto);
    }

    /**
     * Creates a nullable record type with unnamed fields of the given types.
     */
    @Nonnull
    private static Type.Record record(@Nonnull final Type... fieldTypes) {
        return Type.Record.fromFields(Arrays.stream(fieldTypes)
                .map(Type.Record.Field::unnamedOf)
                .toList());
    }

    @Nonnull
    static Stream<Arguments> isPromotableArguments() {
        return Stream.of(
                Arguments.argumentSet("int -> long", INT, LONG, true),
                Arguments.argumentSet("int -> float", INT, FLOAT, true),
                Arguments.argumentSet("int -> double", INT, DOUBLE, true),
                Arguments.argumentSet("long -> double", LONG, DOUBLE, true),
                Arguments.argumentSet("float -> double", FLOAT, DOUBLE, true),
                Arguments.argumentSet("string -> uuid", STRING, UUID, true),
                Arguments.argumentSet("string -> enum", STRING, ENUM, true),

                Arguments.argumentSet("long -> int", LONG, INT, false),
                Arguments.argumentSet("double -> float", DOUBLE, FLOAT, false),
                Arguments.argumentSet("int -> string", INT, STRING, false),
                Arguments.argumentSet("boolean -> int", BOOLEAN, INT, false),
                Arguments.argumentSet("int -> int", INT, INT, false),
                Arguments.argumentSet("long -> long", LONG, LONG, false),
                Arguments.argumentSet("enum -> string", ENUM, STRING, false),

                Arguments.argumentSet("null -> int", Type.nullType(), INT, true),
                Arguments.argumentSet("null -> uuid", Type.nullType(), UUID, true),
                Arguments.argumentSet("null -> array(int)", Type.nullType(), INT_ARRAY, true),
                Arguments.argumentSet("null -> record(int)", Type.nullType(), RECORD_A_INT, true),
                Arguments.argumentSet("none -> array(int)", Type.noneType(), INT_ARRAY, true),
                Arguments.argumentSet("null -> enum", Type.nullType(), ENUM, true),

                Arguments.argumentSet("array(int) -> array(long)", INT_ARRAY, LONG_ARRAY, true),
                Arguments.argumentSet("array(long) -> array(int)", LONG_ARRAY, INT_ARRAY, false),
                Arguments.argumentSet("array(int) -> array(string)", INT_ARRAY, new Type.Array(STRING), false),
                Arguments.argumentSet("array(array(int)) -> array(array(long))",
                        new Type.Array(INT_ARRAY), new Type.Array(LONG_ARRAY), true),
                Arguments.argumentSet("array(<erased>) -> array(int)", new Type.Array(null), INT_ARRAY, false),
                Arguments.argumentSet("array(int) -> array(<erased>)", INT_ARRAY, new Type.Array(null), false),
                Arguments.argumentSet("array(record(int, string)) -> array(record(long, string))",
                        new Type.Array(record(INT, STRING)), new Type.Array(record(LONG, STRING)), true),
                Arguments.argumentSet("array(int) -> int", new Type.Array(INT), INT, false),
                Arguments.argumentSet("int -> array(int)", INT, new Type.Array(INT), false),

                Arguments.argumentSet("record(int, string) -> record(int, string)", record(INT, STRING), record(INT, STRING), true),
                Arguments.argumentSet("record(int, string) -> record(long, string)", record(INT, STRING), record(LONG, STRING), true),
                Arguments.argumentSet("record(int, float) -> record(double, double)", record(INT, FLOAT), record(DOUBLE, DOUBLE), true),
                Arguments.argumentSet("record(long, string) -> record(int, string)", record(LONG, STRING), record(INT, STRING), false),
                Arguments.argumentSet("record(int, string) -> record(int, boolean)", record(INT, STRING), record(INT, BOOLEAN), false),
                Arguments.argumentSet("record(int) -> record(int, int)", record(INT), record(INT, INT), false),
                Arguments.argumentSet("record(int, int) -> record(int)", record(INT, INT), record(INT), false),
                Arguments.argumentSet("record(int as a) -> record(long as a)", RECORD_A_INT, RECORD_A_LONG, true),
                Arguments.argumentSet("record(int as a) -> record(int as b)", RECORD_A_INT, RECORD_B_INT, true),
                Arguments.argumentSet("record(int as a) as T1 -> record(int as b)", NAMED_T1_A_LONG, RECORD_B_INT, false),
                Arguments.argumentSet("record(int as a) -> record(long)", RECORD_A_INT, record(LONG), true),
                Arguments.argumentSet("record(int) -> record(int as a)", record(INT), RECORD_A_INT, true),
                Arguments.argumentSet("record(int as a) as T1 -> record(long as a) as T1", NAMED_T1_A_INT, NAMED_T1_A_LONG, true),
                Arguments.argumentSet("record(int as a) as T1 -> record(int as a) as T2", NAMED_T1_A_INT, NAMED_T2_A_INT, false),
                Arguments.argumentSet("record(int as a) as T1 -> record(long as a)", NAMED_T1_A_INT, RECORD_A_LONG, true),
                Arguments.argumentSet("record(long as a) -> record(int as a) as T1", RECORD_A_LONG, NAMED_T1_A_INT, false),
                Arguments.argumentSet("record(long as a) -> record(long as a) as T1", RECORD_A_LONG, NAMED_T1_A_LONG, true),
                Arguments.argumentSet("record(record(int), string) -> record(record(long), string)",
                        record(record(INT), STRING), record(record(LONG), STRING), true),
                Arguments.argumentSet("record(record(long), string) -> record(record(int), string)",
                        record(record(LONG), STRING), record(record(INT), STRING), false),
                Arguments.argumentSet("record(array(int)) -> record(array(long))", record(INT_ARRAY), record(LONG_ARRAY), true),
                Arguments.argumentSet("record(array(long)) -> record(array(int))", record(LONG_ARRAY), record(INT_ARRAY), false),
                Arguments.argumentSet("record(int) -> int", record(INT), INT, false),
                Arguments.argumentSet("int -> record(int)", INT, record(INT), false)
        );
    }

    @ParameterizedTest
    @MethodSource("isPromotableArguments")
    void testIsPromotableReturnsExpected(@Nonnull final Type inType, @Nonnull final Type promoteToType, final boolean expected) {
        assertThat(PromoteValue.isPromotable(inType, promoteToType)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> isPromotionNeededArguments() {
        return Stream.of(
                Arguments.argumentSet("int -> any", INT, Type.any(), false),
                Arguments.argumentSet("null -> any", Type.nullType(), Type.any(), false),
                Arguments.argumentSet("record(int) -> any", record(INT), Type.any(), false),
                Arguments.argumentSet("array(int) -> any", INT_ARRAY, Type.any(), false),
                Arguments.argumentSet("enum -> any", ENUM, Type.any(), false),

                Arguments.argumentSet("null -> int", Type.nullType(), INT, true),
                Arguments.argumentSet("null -> array(int)", Type.nullType(), INT_ARRAY, true),
                Arguments.argumentSet("null -> record(int)", Type.nullType(), record(INT), true),
                Arguments.argumentSet("null -> enum", Type.nullType(), ENUM, true),
                Arguments.argumentSet("none -> array(int)", Type.noneType(), new Type.Array(INT), true),

                Arguments.argumentSet("int -> int", INT, INT, false),
                Arguments.argumentSet("int -> long", INT, LONG, true),
                Arguments.argumentSet("long -> int", LONG, INT, true),
                Arguments.argumentSet("string -> uuid", STRING, UUID, true),
                Arguments.argumentSet("int? -> int", Type.primitiveType(Type.TypeCode.INT, false), INT, false),
                Arguments.argumentSet("int -> int?", INT, Type.primitiveType(Type.TypeCode.INT, false), false),

                Arguments.argumentSet("array(int) -> array(int)", new Type.Array(INT), new Type.Array(INT), false),
                Arguments.argumentSet("array(int) -> array(long)", new Type.Array(INT), new Type.Array(LONG), true),
                Arguments.argumentSet("array(array(int)) -> array(array(long))", new Type.Array(INT_ARRAY), new Type.Array(LONG_ARRAY), true),
                Arguments.argumentSet("array(array(int)) -> array(array(int))", new Type.Array(INT_ARRAY), new Type.Array(INT_ARRAY), false),
                Arguments.argumentSet("array(int) -> array(int)?", INT_ARRAY.withNullability(false), INT_ARRAY.withNullability(true), false),

                Arguments.argumentSet("record(int, string) -> record(int, string)", record(INT, STRING), record(INT, STRING), false),
                Arguments.argumentSet("record(int, string) -> record(long, string)", record(INT, STRING), record(LONG, STRING), true),
                Arguments.argumentSet("record(int as a) -> record(int as b)", RECORD_A_INT, RECORD_B_INT, true),
                Arguments.argumentSet("record(int as a @1) -> record(int as a @2)",
                        Type.Record.fromFields(ImmutableList.of(Type.Record.Field.of(INT,
                                Optional.of("a"), Optional.of(1)))),
                        Type.Record.fromFields(ImmutableList.of(Type.Record.Field.of(INT,
                                Optional.of("a"), Optional.of(2)))),
                        true),
                Arguments.argumentSet("record(record(int), string) -> record(record(long), string)",
                        record(record(INT), STRING), record(record(LONG), STRING), true),

                Arguments.argumentSet("vector(32, 3) -> vector(32, 3)", Type.Vector.of(true, 32, 3), Type.Vector.of(true, 32, 3), false)
        );
    }

    @ParameterizedTest
    @MethodSource("isPromotionNeededArguments")
    void testIsPromotionNeededReturnsExpected(@Nonnull final Type inType, @Nonnull final Type promoteToType, final boolean expected) {
        assertThat(PromoteValue.isPromotionNeeded(inType, promoteToType)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> isPromotionNeededIncompatibleArguments() {
        return Stream.of(
                Arguments.argumentSet("array(<erased>) -> array(int)", new Type.Array(null), new Type.Array(INT)),
                Arguments.argumentSet("record(int) -> record(int, int)", record(INT), record(INT, INT)),
                Arguments.argumentSet("vector(32, 3) -> vector(32, 3)?",
                        Type.Vector.of(false, 32, 12),
                        Type.Vector.of(true, 32, 3)),
                Arguments.argumentSet("int -> array(int)", INT, INT_ARRAY),
                Arguments.argumentSet("record(int as a) -> int", RECORD_A_INT, INT)
        );
    }

    @ParameterizedTest
    @MethodSource("isPromotionNeededIncompatibleArguments")
    void testIsPromotionNeededWithIncompatibleTypesThrows(@Nonnull final Type inType, @Nonnull final Type promoteToType) {
        assertThatThrownBy(() -> PromoteValue.isPromotionNeeded(inType, promoteToType))
                .isInstanceOf(SemanticException.class)
                .extracting(e -> ((SemanticException)e).getErrorCode())
                .isEqualTo(SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
    }

    @Test
    void testNestedPromoteValueUsesWith() {
        final var inputValue = LiteralValue.ofScalar(1);
        final var promoteValue = PromoteValue.inject(inputValue, Type.primitiveType(Type.TypeCode.LONG));

        assertThat(PromoteValue.inject(promoteValue, Type.primitiveType(Type.TypeCode.DOUBLE)))
                .isEqualTo(PromoteValue.inject(inputValue, Type.primitiveType(Type.TypeCode.DOUBLE)));
    }
}
