/*
 * CastValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests different aspects of functionality provided by {@link CastValue}.
 * <ul>
 *   <li>Valid cast operations between supported types.</li>
 *   <li>Invalid cast operations that should throw SemanticException.</li>
 *   <li>Numeric conversions with range checking.</li>
 *   <li>String to numeric conversions with validation.</li>
 *   <li>Boolean conversions.</li>
 *   <li>NULL value handling.</li>
 *   <li>Serialization and deserialization.</li>
 * </ul>
 */
class CastValueTest {

    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_42 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 42);
    private static final LiteralValue<Integer> INT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), null);

    private static final LiteralValue<Long> LONG_42 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 42L);
    private static final LiteralValue<Long> LONG_OVERFLOW = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), ((long)Integer.MAX_VALUE) + 1);
    private static final LiteralValue<Long> LONG_MAX = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), Long.MAX_VALUE);

    private static final LiteralValue<Float> FLOAT_1_5 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.5f);
    private static final LiteralValue<Float> FLOAT_42_7 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 42.7f);
    private static final LiteralValue<Float> FLOAT_NAN = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), Float.NaN);
    private static final LiteralValue<Float> FLOAT_POSITIVE_INFINITY = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), Float.POSITIVE_INFINITY);

    private static final LiteralValue<Double> DOUBLE_42_7 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 42.7);
    private static final LiteralValue<Double> DOUBLE_MAX = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), Double.MAX_VALUE);
    private static final LiteralValue<Double> DOUBLE_NAN = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), Double.NaN);
    private static final LiteralValue<Double> DOUBLE_NEGATIVE_INFINITY = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), Double.NEGATIVE_INFINITY);

    private static final LiteralValue<String> STRING_42 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "42");
    private static final LiteralValue<String> STRING_42_7 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "42.7");
    private static final LiteralValue<String> STRING_TRUE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "true");
    private static final LiteralValue<String> STRING_FALSE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "false");
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "1");
    private static final LiteralValue<String> STRING_0 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "0");
    private static final LiteralValue<String> STRING_INVALID = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "invalid");
    private static final LiteralValue<String> STRING_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), null);

    private static final LiteralValue<Boolean> BOOL_TRUE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    private static final LiteralValue<Boolean> BOOL_FALSE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), false);

    private static final LiteralValue<Void> NULL = new LiteralValue<>(Type.NULL, null);

    // Array test data
    private static final Type.Array INT_ARRAY_TYPE = new Type.Array(Type.primitiveType(Type.TypeCode.INT));
    private static final Type.Array STRING_ARRAY_TYPE = new Type.Array(Type.primitiveType(Type.TypeCode.STRING));
    private static final Type.Array LONG_ARRAY_TYPE = new Type.Array(Type.primitiveType(Type.TypeCode.LONG));
    private static final Type.Array DOUBLE_ARRAY_TYPE = new Type.Array(Type.primitiveType(Type.TypeCode.DOUBLE));
    private static final Type.Array BOOLEAN_ARRAY_TYPE = new Type.Array(Type.primitiveType(Type.TypeCode.BOOLEAN));

    private static final LiteralValue<java.util.List<Integer>> INT_ARRAY_123 =
            new LiteralValue<>(INT_ARRAY_TYPE, java.util.List.of(1, 2, 3));
    private static final LiteralValue<java.util.List<Integer>> INT_ARRAY_EMPTY =
            new LiteralValue<>(INT_ARRAY_TYPE, java.util.List.of());
    private static final LiteralValue<java.util.List<String>> STRING_ARRAY_ABC =
            new LiteralValue<>(STRING_ARRAY_TYPE, java.util.List.of("a", "b", "c"));
    private static final LiteralValue<java.util.List<Integer>> INT_ARRAY_WITH_NULL =
            new LiteralValue<>(INT_ARRAY_TYPE, java.util.Arrays.asList(1, null, 3));

    private static final TypeRepository.Builder typeRepositoryBuilder = TypeRepository.newBuilder().setName("foo").setPackage("a.b.c");

    @Test
    void testValidNumericCasts() {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // INT to LONG
        Value castIntToLong = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.LONG));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.LONG), castIntToLong.getResultType());
        Object result = castIntToLong.evalWithoutStore(evalContext);
        Assertions.assertEquals(42L, result);

        // INT to FLOAT
        Value castIntToFloat = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.FLOAT));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.FLOAT), castIntToFloat.getResultType());
        result = castIntToFloat.evalWithoutStore(evalContext);
        Assertions.assertEquals(42.0f, result);

        // INT to DOUBLE
        Value castIntToDouble = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.DOUBLE));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.DOUBLE), castIntToDouble.getResultType());
        result = castIntToDouble.evalWithoutStore(evalContext);
        Assertions.assertEquals(42.0, result);

        // LONG to INT (within range)
        Value castLongToInt = CastValue.inject(LONG_42, Type.primitiveType(Type.TypeCode.INT));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.INT), castLongToInt.getResultType());
        result = castLongToInt.evalWithoutStore(evalContext);
        Assertions.assertEquals(42, result);

        // FLOAT to INT (rounding)
        Value castFloatToInt = CastValue.inject(FLOAT_1_5, Type.primitiveType(Type.TypeCode.INT));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.INT), castFloatToInt.getResultType());
        result = castFloatToInt.evalWithoutStore(evalContext);
        Assertions.assertEquals(2, result); // Math.round(1.5f) = 2

        // DOUBLE to LONG (rounding)
        Value castDoubleToLong = CastValue.inject(DOUBLE_42_7, Type.primitiveType(Type.TypeCode.LONG));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.LONG), castDoubleToLong.getResultType());
        result = castDoubleToLong.evalWithoutStore(evalContext);
        Assertions.assertEquals(43L, result); // Math.round(42.7) = 43
    }

    @Test
    void testNumericToString() {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // INT to STRING
        Value castIntToString = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.STRING));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.STRING), castIntToString.getResultType());
        Object result = castIntToString.evalWithoutStore(evalContext);
        Assertions.assertEquals("42", result);

        // BOOLEAN to STRING
        Value castBoolToString = CastValue.inject(BOOL_TRUE, Type.primitiveType(Type.TypeCode.STRING));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.STRING), castBoolToString.getResultType());
        result = castBoolToString.evalWithoutStore(evalContext);
        Assertions.assertEquals("true", result);
    }

    @Test
    void testStringToNumeric() {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // STRING to INT
        Value castStringToInt = CastValue.inject(STRING_42, Type.primitiveType(Type.TypeCode.INT));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.INT), castStringToInt.getResultType());
        Object result = castStringToInt.evalWithoutStore(evalContext);
        Assertions.assertEquals(42, result);

        // STRING to FLOAT
        Value castStringToFloat = CastValue.inject(STRING_42_7, Type.primitiveType(Type.TypeCode.FLOAT));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.FLOAT), castStringToFloat.getResultType());
        result = castStringToFloat.evalWithoutStore(evalContext);
        Assertions.assertEquals(42.7f, result);
    }

    @Test
    void testBooleanConversions() {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // STRING to BOOLEAN
        Value castStringTrueToBool = CastValue.inject(STRING_TRUE, Type.primitiveType(Type.TypeCode.BOOLEAN));
        Object result = castStringTrueToBool.evalWithoutStore(evalContext);
        Assertions.assertEquals(Boolean.TRUE, result);

        Value castString1ToBool = CastValue.inject(STRING_1, Type.primitiveType(Type.TypeCode.BOOLEAN));
        result = castString1ToBool.evalWithoutStore(evalContext);
        Assertions.assertEquals(Boolean.TRUE, result);

        Value castStringFalseToBool = CastValue.inject(STRING_FALSE, Type.primitiveType(Type.TypeCode.BOOLEAN));
        result = castStringFalseToBool.evalWithoutStore(evalContext);
        Assertions.assertEquals(Boolean.FALSE, result);

        Value castString0ToBool = CastValue.inject(STRING_0, Type.primitiveType(Type.TypeCode.BOOLEAN));
        result = castString0ToBool.evalWithoutStore(evalContext);
        Assertions.assertEquals(Boolean.FALSE, result);

        // INT to BOOLEAN
        Value castIntToBool = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.BOOLEAN));
        result = castIntToBool.evalWithoutStore(evalContext);
        Assertions.assertEquals(Boolean.TRUE, result);

        Value castInt1ToBool = CastValue.inject(INT_1, Type.primitiveType(Type.TypeCode.BOOLEAN));
        result = castInt1ToBool.evalWithoutStore(evalContext);
        Assertions.assertEquals(Boolean.TRUE, result);

        // BOOLEAN to INT
        Value castBoolToInt = CastValue.inject(BOOL_TRUE, Type.primitiveType(Type.TypeCode.INT));
        result = castBoolToInt.evalWithoutStore(evalContext);
        Assertions.assertEquals(1, result);

        Value castBoolFalseToInt = CastValue.inject(BOOL_FALSE, Type.primitiveType(Type.TypeCode.INT));
        result = castBoolFalseToInt.evalWithoutStore(evalContext);
        Assertions.assertEquals(0, result);
    }

    @Nonnull
    static Stream<Type> testNullCasts() {
        return Stream.of(Type.TypeCode.values())
                .flatMap(t -> {
                    if (t == Type.TypeCode.UNKNOWN || t == Type.TypeCode.ANY || t == Type.TypeCode.RELATION || t == Type.TypeCode.NONE || t == Type.TypeCode.FUNCTION) {
                        // Types for which no conversion is defined. See testNullCastNegativeTest
                        return Stream.of();
                    } else if (t == Type.TypeCode.NULL) {
                        return Stream.of(Type.NULL);
                    } else if (t == Type.TypeCode.UUID) {
                        return Stream.of(Type.UUID_NULL_INSTANCE);
                    } else if (t == Type.TypeCode.VECTOR) {
                        return Stream.of(
                                Type.Vector.of(true, 16, 10),
                                Type.Vector.of(true, 32, 32),
                                Type.Vector.of(true, 64, 100));
                    } else if (t == Type.TypeCode.RECORD) {
                        return Stream.of(
                                Type.Record.fromFields(true, ImmutableList.of()),
                                Type.Record.fromFields(true, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("a"), Optional.empty()))));
                    } else if (t == Type.TypeCode.ENUM) {
                        return Stream.of(
                                Type.Enum.fromValues(true, ImmutableList.of()),
                                Type.Enum.fromValues(true, ImmutableList.of(Type.Enum.EnumValue.from("FOO", 1), Type.Enum.EnumValue.from("BAR", 2))));
                    } else if (t == Type.TypeCode.ARRAY) {
                        return Stream.of(
                                INT_ARRAY_TYPE, LONG_ARRAY_TYPE, STRING_ARRAY_TYPE, BOOLEAN_ARRAY_TYPE, DOUBLE_ARRAY_TYPE);
                    } else if (t.isPrimitive()) {
                        return Stream.of(Type.primitiveType(t, true));
                    }
                    return fail("Unknown type code: " + t);
                });
    }

    @ParameterizedTest
    @MethodSource
    void testNullCasts(@Nonnull Type targetType) {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // Convert a null (of NULL type) to the given target type. It should still return null when evaluated
        Value castNullToTarget = CastValue.inject(NULL, targetType);
        Assertions.assertEquals(targetType, castNullToTarget.getResultType());
        Object result = castNullToTarget.evalWithoutStore(evalContext);
        Assertions.assertNull(result);
    }

    @Nonnull
    static Stream<Type> testNullCastNegativeTest() {
        return Stream.of(
                Type.primitiveType(Type.TypeCode.UNKNOWN, true),
                Type.NONE,
                Type.any(),
                Type.FUNCTION,
                new Type.Relation(Type.primitiveType(Type.TypeCode.INT, true))
        );
    }

    @ParameterizedTest
    @MethodSource
    void testNullCastNegativeTest(Type targetType) {
        // Assert that these types cannot be converted from null. All types should be covered by either this test or the previous one
        final SemanticException err = Assertions.assertThrows(SemanticException.class, () -> CastValue.inject(NULL, targetType));
        assertThat(err.getMessage(), Matchers.containsString("Invalid cast operation No cast defined"));
    }

    @Test
    void testSameTypeCast() {
        // When types are the same, should return original value
        Value result = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.INT));
        Assertions.assertSame(INT_42, result);
    }

    @Test
    void testInvalidCasts() {
        // LONG out of INT range
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(LONG_OVERFLOW, Type.primitiveType(Type.TypeCode.INT));
            castValue.evalWithoutStore(evalContext);
        });

        // LONG_MAX to INT (overflow)
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(LONG_MAX, Type.primitiveType(Type.TypeCode.INT));
            castValue.evalWithoutStore(evalContext);
        });

        // DOUBLE_MAX to FLOAT (overflow)
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(DOUBLE_MAX, Type.primitiveType(Type.TypeCode.FLOAT));
            castValue.evalWithoutStore(evalContext);
        });

        // Float NaN to INT
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(FLOAT_NAN, Type.primitiveType(Type.TypeCode.INT));
            castValue.evalWithoutStore(evalContext);
        });

        // Float infinity to INT
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(FLOAT_POSITIVE_INFINITY, Type.primitiveType(Type.TypeCode.INT));
            castValue.evalWithoutStore(evalContext);
        });

        // Double NaN to LONG
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(DOUBLE_NAN, Type.primitiveType(Type.TypeCode.LONG));
            castValue.evalWithoutStore(evalContext);
        });

        // Double infinity to LONG
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(DOUBLE_NEGATIVE_INFINITY, Type.primitiveType(Type.TypeCode.LONG));
            castValue.evalWithoutStore(evalContext);
        });

        // Invalid string to INT
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(STRING_INVALID, Type.primitiveType(Type.TypeCode.INT));
            castValue.evalWithoutStore(evalContext);
        });

        // Invalid string to BOOLEAN
        Assertions.assertThrows(SemanticException.class, () -> {
            final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
            Value castValue = CastValue.inject(STRING_INVALID, Type.primitiveType(Type.TypeCode.BOOLEAN));
            castValue.evalWithoutStore(evalContext);
        });
    }

    @Test
    void testUnsupportedCast() {
        // Should throw SemanticException for unsupported cast
        Assertions.assertThrows(SemanticException.class, () -> {
            CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.BYTES));
        });
    }

    @Test
    void testIsCastSupported() {
        // Supported casts
        Assertions.assertTrue(CastValue.isCastSupported(
                Type.primitiveType(Type.TypeCode.INT),
                Type.primitiveType(Type.TypeCode.LONG)));

        Assertions.assertTrue(CastValue.isCastSupported(
                Type.primitiveType(Type.TypeCode.STRING),
                Type.primitiveType(Type.TypeCode.INT)));

        Assertions.assertTrue(CastValue.isCastSupported(
                Type.primitiveType(Type.TypeCode.NULL),
                Type.primitiveType(Type.TypeCode.STRING)));

        // Same type should be supported
        Assertions.assertTrue(CastValue.isCastSupported(
                Type.primitiveType(Type.TypeCode.INT),
                Type.primitiveType(Type.TypeCode.INT)));

        // Unsupported cast
        Assertions.assertFalse(CastValue.isCastSupported(
                Type.primitiveType(Type.TypeCode.INT),
                Type.primitiveType(Type.TypeCode.BYTES)));
    }

    @Test
    void testToString() {
        Value castValue = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.STRING));
        String expected = "CAST(" + INT_42 + " AS " + Type.primitiveType(Type.TypeCode.STRING) + ")";
        Assertions.assertEquals(expected, castValue.toString());
    }

    @Test
    void testSerialization() {
        Value originalCast = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.LONG));
        Value deserializedCast = verifySerialization(originalCast);

        Assertions.assertEquals(originalCast.getResultType(), deserializedCast.getResultType());
        Assertions.assertEquals(originalCast.toString(), deserializedCast.toString());

        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
        Assertions.assertEquals(
                originalCast.evalWithoutStore(evalContext),
                deserializedCast.evalWithoutStore(evalContext)
        );
    }

    @Test
    void testHashCodeAndEquals() {
        Value cast1 = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.LONG));
        Value cast2 = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.LONG));
        Value cast3 = CastValue.inject(INT_1, Type.primitiveType(Type.TypeCode.LONG));
        Value cast4 = CastValue.inject(INT_42, Type.primitiveType(Type.TypeCode.STRING));

        Assertions.assertEquals(cast1, cast2);
        Assertions.assertEquals(cast1.hashCode(), cast2.hashCode());

        Assertions.assertNotEquals(cast1, cast3);
        Assertions.assertNotEquals(cast1, cast4);
    }

    @Test
    void testArrayCasting() {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // INT array to STRING array: [1, 2, 3] -> ["1", "2", "3"]
        Value intToStringArray = CastValue.inject(INT_ARRAY_123, STRING_ARRAY_TYPE);
        Assertions.assertEquals(STRING_ARRAY_TYPE, intToStringArray.getResultType());
        Object result = intToStringArray.evalWithoutStore(evalContext);
        Assertions.assertTrue(result instanceof java.util.List);
        @SuppressWarnings("unchecked")
        java.util.List<String> stringList = (java.util.List<String>) result;
        Assertions.assertEquals(java.util.List.of("1", "2", "3"), stringList);

        // INT array to LONG array: [1, 2, 3] -> [1L, 2L, 3L]
        Value intToLongArray = CastValue.inject(INT_ARRAY_123, LONG_ARRAY_TYPE);
        Assertions.assertEquals(LONG_ARRAY_TYPE, intToLongArray.getResultType());
        result = intToLongArray.evalWithoutStore(evalContext);
        @SuppressWarnings("unchecked")
        java.util.List<Long> longList = (java.util.List<Long>) result;
        Assertions.assertEquals(java.util.List.of(1L, 2L, 3L), longList);

        // STRING array to INT array: ["42", "100", "7"] -> [42, 100, 7]
        final LiteralValue<java.util.List<String>> stringArrayNumbers =
                new LiteralValue<>(STRING_ARRAY_TYPE, java.util.List.of("42", "100", "7"));
        Value stringToIntArray = CastValue.inject(stringArrayNumbers, INT_ARRAY_TYPE);
        Assertions.assertEquals(INT_ARRAY_TYPE, stringToIntArray.getResultType());
        result = stringToIntArray.evalWithoutStore(evalContext);
        @SuppressWarnings("unchecked")
        java.util.List<Integer> intList = (java.util.List<Integer>) result;
        Assertions.assertEquals(java.util.List.of(42, 100, 7), intList);
    }

    @Test
    void testEmptyArrayCasting() {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // Empty INT array to STRING array: [] -> []
        Value emptyIntToStringArray = CastValue.inject(INT_ARRAY_EMPTY, STRING_ARRAY_TYPE);
        Assertions.assertEquals(STRING_ARRAY_TYPE, emptyIntToStringArray.getResultType());
        Object result = emptyIntToStringArray.evalWithoutStore(evalContext);
        Assertions.assertTrue(result instanceof java.util.List);
        @SuppressWarnings("unchecked")
        java.util.List<String> emptyStringList = (java.util.List<String>) result;
        Assertions.assertTrue(emptyStringList.isEmpty());

        // Empty array of one type to another type should work
        Value emptyIntToDoubleArray = CastValue.inject(INT_ARRAY_EMPTY, DOUBLE_ARRAY_TYPE);
        result = emptyIntToDoubleArray.evalWithoutStore(evalContext);
        @SuppressWarnings("unchecked")
        java.util.List<Double> emptyDoubleList = (java.util.List<Double>) result;
        Assertions.assertTrue(emptyDoubleList.isEmpty());
    }

    @Test
    void testArrayWithNullElementsCasting() {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // Array with nulls: [1, null, 3] -> ["1", null, "3"]
        Value intArrayWithNullToString = CastValue.inject(INT_ARRAY_WITH_NULL, STRING_ARRAY_TYPE);
        Assertions.assertEquals(STRING_ARRAY_TYPE, intArrayWithNullToString.getResultType());
        Object result = intArrayWithNullToString.evalWithoutStore(evalContext);
        @SuppressWarnings("unchecked")
        java.util.List<String> resultList = (java.util.List<String>) result;
        Assertions.assertEquals(3, resultList.size());
        Assertions.assertEquals("1", resultList.get(0));
        Assertions.assertNull(resultList.get(1));
        Assertions.assertEquals("3", resultList.get(2));
    }

    @Test
    void testSameTypeArrayCasting() {
        // When array types are the same, should return original value
        Value result = CastValue.inject(INT_ARRAY_123, INT_ARRAY_TYPE);
        Assertions.assertSame(INT_ARRAY_123, result);
    }

    @Test
    void testArrayCastSupported() {
        // Test isCastSupported for arrays
        Assertions.assertTrue(CastValue.isCastSupported(INT_ARRAY_TYPE, STRING_ARRAY_TYPE));
        Assertions.assertTrue(CastValue.isCastSupported(INT_ARRAY_TYPE, LONG_ARRAY_TYPE));
        Assertions.assertTrue(CastValue.isCastSupported(STRING_ARRAY_TYPE, INT_ARRAY_TYPE));

        // Same type should be supported
        Assertions.assertTrue(CastValue.isCastSupported(INT_ARRAY_TYPE, INT_ARRAY_TYPE));

        // Arrays with incompatible element types should not be supported
        // (Note: This depends on what element type casts are supported)
        final Type.Array bytesArrayType = new Type.Array(Type.primitiveType(Type.TypeCode.BYTES));
        Assertions.assertFalse(CastValue.isCastSupported(INT_ARRAY_TYPE, bytesArrayType));
    }

    @Test
    void testInvalidArrayCasting() {
        // Test invalid array element casting
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        // Create an array with invalid string numbers
        final LiteralValue<java.util.List<String>> invalidStringArray =
                new LiteralValue<>(STRING_ARRAY_TYPE, java.util.List.of("invalid", "not_a_number"));

        // This should throw an exception when trying to cast to INT array
        Assertions.assertThrows(SemanticException.class, () -> {
            Value invalidCast = CastValue.inject(invalidStringArray, INT_ARRAY_TYPE);
            invalidCast.evalWithoutStore(evalContext);
        });
    }

    @Test
    void testNestedArrayCasting() {
        // Test arrays of arrays (nested arrays)
        final Type.Array nestedIntArrayType = new Type.Array(INT_ARRAY_TYPE);
        final Type.Array nestedStringArrayType = new Type.Array(STRING_ARRAY_TYPE);

        final LiteralValue<java.util.List<java.util.List<Integer>>> nestedIntArray =
                new LiteralValue<>(nestedIntArrayType,
                    java.util.List.of(
                        java.util.List.of(1, 2),
                        java.util.List.of(3, 4)
                    ));

        // Nested array casting should be supported
        Assertions.assertTrue(CastValue.isCastSupported(nestedIntArrayType, nestedStringArrayType));

        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
        Value nestedCast = CastValue.inject(nestedIntArray, nestedStringArrayType);
        Object result = nestedCast.evalWithoutStore(evalContext);

        @SuppressWarnings("unchecked")
        java.util.List<java.util.List<String>> nestedResult = (java.util.List<java.util.List<String>>) result;
        Assertions.assertEquals(2, nestedResult.size());
        Assertions.assertEquals(java.util.List.of("1", "2"), nestedResult.get(0));
        Assertions.assertEquals(java.util.List.of("3", "4"), nestedResult.get(1));
    }

    static Stream<Arguments> castTestData() {
        return Stream.of(
                // Numeric to numeric casts
                Arguments.of(INT_42, Type.primitiveType(Type.TypeCode.LONG), 42L),
                Arguments.of(INT_42, Type.primitiveType(Type.TypeCode.FLOAT), 42.0f),
                Arguments.of(INT_42, Type.primitiveType(Type.TypeCode.DOUBLE), 42.0),
                Arguments.of(LONG_42, Type.primitiveType(Type.TypeCode.INT), 42),
                Arguments.of(LONG_42, Type.primitiveType(Type.TypeCode.FLOAT), 42.0f),
                Arguments.of(LONG_42, Type.primitiveType(Type.TypeCode.DOUBLE), 42.0),
                Arguments.of(FLOAT_42_7, Type.primitiveType(Type.TypeCode.DOUBLE), 42.70000076293945), // Float precision

                // Numeric to string casts
                Arguments.of(INT_42, Type.primitiveType(Type.TypeCode.STRING), "42"),
                Arguments.of(LONG_42, Type.primitiveType(Type.TypeCode.STRING), "42"),
                Arguments.of(FLOAT_42_7, Type.primitiveType(Type.TypeCode.STRING), "42.7"),
                Arguments.of(DOUBLE_42_7, Type.primitiveType(Type.TypeCode.STRING), "42.7"),
                Arguments.of(BOOL_TRUE, Type.primitiveType(Type.TypeCode.STRING), "true"),
                Arguments.of(BOOL_FALSE, Type.primitiveType(Type.TypeCode.STRING), "false"),

                // String to numeric casts
                Arguments.of(STRING_42, Type.primitiveType(Type.TypeCode.INT), 42),
                Arguments.of(STRING_42, Type.primitiveType(Type.TypeCode.LONG), 42L),
                Arguments.of(STRING_42_7, Type.primitiveType(Type.TypeCode.FLOAT), 42.7f),
                Arguments.of(STRING_42_7, Type.primitiveType(Type.TypeCode.DOUBLE), 42.7),

                // Boolean casts
                Arguments.of(STRING_TRUE, Type.primitiveType(Type.TypeCode.BOOLEAN), Boolean.TRUE),
                Arguments.of(STRING_FALSE, Type.primitiveType(Type.TypeCode.BOOLEAN), Boolean.FALSE),
                Arguments.of(STRING_1, Type.primitiveType(Type.TypeCode.BOOLEAN), Boolean.TRUE),
                Arguments.of(STRING_0, Type.primitiveType(Type.TypeCode.BOOLEAN), Boolean.FALSE),
                Arguments.of(INT_42, Type.primitiveType(Type.TypeCode.BOOLEAN), Boolean.TRUE),
                Arguments.of(BOOL_TRUE, Type.primitiveType(Type.TypeCode.INT), 1),
                Arguments.of(BOOL_FALSE, Type.primitiveType(Type.TypeCode.INT), 0),

                // NULL casts
                Arguments.of(NULL, Type.primitiveType(Type.TypeCode.INT), null),
                Arguments.of(NULL, Type.primitiveType(Type.TypeCode.LONG), null),
                Arguments.of(NULL, Type.primitiveType(Type.TypeCode.FLOAT), null),
                Arguments.of(NULL, Type.primitiveType(Type.TypeCode.DOUBLE), null),
                Arguments.of(NULL, Type.primitiveType(Type.TypeCode.BOOLEAN), null),
                Arguments.of(NULL, Type.primitiveType(Type.TypeCode.STRING), null),
                Arguments.of(INT_NULL, Type.primitiveType(Type.TypeCode.STRING), null),
                Arguments.of(STRING_NULL, Type.primitiveType(Type.TypeCode.INT), null)
        );
    }

    @ParameterizedTest
    @MethodSource("castTestData")
    void testParameterizedCasts(Value input, Type targetType, Object expectedResult) {
        final var evalContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());

        Value castValue = CastValue.inject(input, targetType);
        Assertions.assertEquals(targetType, castValue.getResultType());

        Object actualResult = castValue.evalWithoutStore(evalContext);
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Nonnull
    protected static Value verifySerialization(@Nonnull final Value value) {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        final PValue planProto = value.toValueProto(serializationContext);
        final byte[] serializedValue = planProto.toByteArray();
        final PValue parsedValueProto;
        try {
            parsedValueProto = PValue.parseFrom(serializedValue);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final Value deserializedValue =
                Value.fromValueProto(serializationContext, parsedValueProto);
        Assertions.assertEquals(value.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), deserializedValue.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        Assertions.assertEquals(value, deserializedValue);
        return deserializedValue;
    }
}
