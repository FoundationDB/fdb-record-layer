/*
 * StringFnValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tests for {@link StringFnValue} covering various aspects of string function functionality.
 * <ul>
 *   <li>LOWER and UPPER function evaluation</li>
 *   <li>NULL handling</li>
 *   <li>toString() method</li>
 *   <li>equals() and hashCode() consistency</li>
 *   <li>planHash() method</li>
 *   <li>Serialization and deserialization</li>
 * </ul>
 */
class StringFnValueTest {
    private static final LiteralValue<String> HELLO_UPPER = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "HELLO");
    private static final LiteralValue<String> HELLO_LOWER = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "hello");
    private static final LiteralValue<String> WORLD_UPPER = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "WORLD");
    private static final LiteralValue<String> MIXED_CASE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "HeLLo WoRLd");
    private static final LiteralValue<String> EMPTY_STRING = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "");
    private static final NullValue NULL_VALUE = new NullValue(Type.primitiveType(Type.TypeCode.STRING));

    /**
     * Tests basic LOWER function evaluation.
     */
    @Test
    void testLowerEvaluation() {
        final StringFnValue lowerValue = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final Object result = lowerValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals("hello", result);
    }

    /**
     * Tests basic UPPER function evaluation.
     */
    @Test
    void testUpperEvaluation() {
        final StringFnValue upperValue = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_LOWER);
        final Object result = upperValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals("HELLO", result);
    }

    /**
     * Tests LOWER with mixed case input.
     */
    @Test
    void testLowerMixedCase() {
        final StringFnValue lowerValue = new StringFnValue(StringFnValue.StringFn.LOWER, MIXED_CASE);
        final Object result = lowerValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals("hello world", result);
    }

    /**
     * Tests UPPER with mixed case input.
     */
    @Test
    void testUpperMixedCase() {
        final StringFnValue upperValue = new StringFnValue(StringFnValue.StringFn.UPPER, MIXED_CASE);
        final Object result = upperValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals("HELLO WORLD", result);
    }

    /**
     * Tests that LOWER with empty string returns empty string.
     */
    @Test
    void testLowerEmptyString() {
        final StringFnValue lowerValue = new StringFnValue(StringFnValue.StringFn.LOWER, EMPTY_STRING);
        final Object result = lowerValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals("", result);
    }

    /**
     * Tests that UPPER with empty string returns empty string.
     */
    @Test
    void testUpperEmptyString() {
        final StringFnValue upperValue = new StringFnValue(StringFnValue.StringFn.UPPER, EMPTY_STRING);
        final Object result = upperValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals("", result);
    }

    /**
     * Tests NULL handling - LOWER(NULL) should return NULL.
     */
    @Test
    void testLowerNull() {
        final StringFnValue lowerValue = new StringFnValue(StringFnValue.StringFn.LOWER, NULL_VALUE);
        final Object result = lowerValue.eval(null, EvaluationContext.empty());
        Assertions.assertNull(result);
    }

    /**
     * Tests NULL handling - UPPER(NULL) should return NULL.
     */
    @Test
    void testUpperNull() {
        final StringFnValue upperValue = new StringFnValue(StringFnValue.StringFn.UPPER, NULL_VALUE);
        final Object result = upperValue.eval(null, EvaluationContext.empty());
        Assertions.assertNull(result);
    }

    /**
     * Tests toString() method for LOWER function.
     */
    @Test
    void testToStringLower() {
        final StringFnValue lowerValue = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final String expected = "lower(" + HELLO_UPPER + ")";
        Assertions.assertEquals(expected, lowerValue.toString());
    }

    /**
     * Tests toString() method for UPPER function.
     */
    @Test
    void testToStringUpper() {
        final StringFnValue upperValue = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_LOWER);
        final String expected = "upper(" + HELLO_LOWER + ")";
        Assertions.assertEquals(expected, upperValue.toString());
    }

    /**
     * Tests equals() and hashCode() - same function and same argument should be equal.
     */
    @Test
    void testEqualsAndHashCodeSame() {
        final StringFnValue lower1 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue lower2 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);

        Assertions.assertEquals(lower1, lower2);
        Assertions.assertEquals(lower1.hashCode(), lower2.hashCode());
    }

    /**
     * Tests equals() - different functions should not be equal.
     */
    @Test
    void testNotEqualsDifferentFunction() {
        final StringFnValue lower = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue upper = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_UPPER);

        Assertions.assertNotEquals(lower, upper);
    }

    /**
     * Tests equals() - same function but different arguments should not be equal.
     */
    @Test
    void testNotEqualsDifferentArgument() {
        final StringFnValue lower1 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue lower2 = new StringFnValue(StringFnValue.StringFn.LOWER, WORLD_UPPER);

        Assertions.assertNotEquals(lower1, lower2);
    }

    /**
     * Tests semanticEquals() with empty AliasMap.
     */
    @Test
    void testSemanticEquals() {
        final StringFnValue lower1 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue lower2 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue upper = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_UPPER);

        Assertions.assertTrue(lower1.semanticEquals(lower2, AliasMap.emptyMap()));
        Assertions.assertFalse(lower1.semanticEquals(upper, AliasMap.emptyMap()));
    }

    /**
     * Tests semanticHashCode() consistency.
     */
    @Test
    void testSemanticHashCode() {
        final StringFnValue lower1 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue lower2 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);

        Assertions.assertEquals(lower1.semanticHashCode(), lower2.semanticHashCode());
    }

    /**
     * Tests planHash() for LOWER function.
     */
    @Test
    void testPlanHashLower() {
        final StringFnValue lower1 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue lower2 = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);

        Assertions.assertEquals(
                lower1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                lower2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    /**
     * Tests planHash() for UPPER function.
     */
    @Test
    void testPlanHashUpper() {
        final StringFnValue upper1 = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_LOWER);
        final StringFnValue upper2 = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_LOWER);

        Assertions.assertEquals(
                upper1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                upper2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    /**
     * Tests that different functions have different plan hashes.
     */
    @Test
    void testPlanHashDifferentFunctions() {
        final StringFnValue lower = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final StringFnValue upper = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_UPPER);

        Assertions.assertNotEquals(
                lower.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                upper.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    /**
     * Tests planHash() directly on StringFn enum for LOWER.
     */
    @Test
    void testStringFnPlanHashLower() {
        final int hash1 = StringFnValue.StringFn.LOWER.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
        final int hash2 = StringFnValue.StringFn.LOWER.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);

        Assertions.assertEquals(hash1, hash2, "Same enum value should produce same planHash");
    }

    /**
     * Tests planHash() directly on StringFn enum for UPPER.
     */
    @Test
    void testStringFnPlanHashUpper() {
        final int hash1 = StringFnValue.StringFn.UPPER.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
        final int hash2 = StringFnValue.StringFn.UPPER.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);

        Assertions.assertEquals(hash1, hash2, "Same enum value should produce same planHash");
    }

    /**
     * Tests that different StringFn enum values have different plan hashes.
     */
    @Test
    void testStringFnPlanHashDifferent() {
        final int lowerHash = StringFnValue.StringFn.LOWER.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
        final int upperHash = StringFnValue.StringFn.UPPER.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);

        Assertions.assertNotEquals(lowerHash, upperHash, "Different enum values should produce different planHashes");
    }

    /**
     * Tests serialization and deserialization for LOWER function.
     */
    @Test
    void testSerializationLower() {
        final StringFnValue originalLower = new StringFnValue(StringFnValue.StringFn.LOWER, HELLO_UPPER);
        final Value deserializedLower = verifySerialization(originalLower);

        Assertions.assertEquals(originalLower.getResultType(), deserializedLower.getResultType());
        Assertions.assertEquals(originalLower.toString(), deserializedLower.toString());
    }

    /**
     * Tests serialization and deserialization for UPPER function.
     */
    @Test
    void testSerializationUpper() {
        final StringFnValue originalUpper = new StringFnValue(StringFnValue.StringFn.UPPER, HELLO_LOWER);
        final Value deserializedUpper = verifySerialization(originalUpper);

        Assertions.assertEquals(originalUpper.getResultType(), deserializedUpper.getResultType());
        Assertions.assertEquals(originalUpper.toString(), deserializedUpper.toString());
    }

    /**
     * Tests that encapsulate() creates correct StringFnValue for LOWER.
     */
    @Test
    void testEncapsulateLower() {
        final List<Value> arguments = List.of(HELLO_UPPER);
        final Value result = StringFnValue.encapsulate(arguments, StringFnValue.StringFn.LOWER);

        Assertions.assertTrue(result instanceof StringFnValue);
        final StringFnValue stringFnValue = (StringFnValue) result;
        Assertions.assertEquals(StringFnValue.StringFn.LOWER, stringFnValue.getFunction());
    }

    /**
     * Tests that encapsulate() creates correct StringFnValue for UPPER.
     */
    @Test
    void testEncapsulateUpper() {
        final List<Value> arguments = List.of(HELLO_LOWER);
        final Value result = StringFnValue.encapsulate(arguments, StringFnValue.StringFn.UPPER);

        Assertions.assertTrue(result instanceof StringFnValue);
        final StringFnValue stringFnValue = (StringFnValue) result;
        Assertions.assertEquals(StringFnValue.StringFn.UPPER, stringFnValue.getFunction());
    }

    /**
     * Tests that encapsulate() with non-string type throws SemanticException.
     */
    @Test
    void testEncapsulateNonStringType() {
        final LiteralValue<Long> intValue = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 42L);
        final List<Value> arguments = List.of(intValue);

        Assertions.assertThrows(SemanticException.class, () ->
                StringFnValue.encapsulate(arguments, StringFnValue.StringFn.LOWER));
    }

    /**
     * Tests BuiltInFunction resolution for LOWER.
     */
    @Test
    void testBuiltInFunctionLower() {
        final var lowerFn = new StringFnValue.LowerFn();
        Assertions.assertEquals("lower", lowerFn.getFunctionName());
    }

    /**
     * Tests BuiltInFunction resolution for UPPER.
     */
    @Test
    void testBuiltInFunctionUpper() {
        final var upperFn = new StringFnValue.UpperFn();
        Assertions.assertEquals("upper", upperFn.getFunctionName());
    }

    /**
     * Parametrized test for multiple string inputs with LOWER.
     */
    @ParameterizedTest
    @MethodSource("lowerTestCases")
    void testLowerParametrized(String input, String expected) {
        final LiteralValue<String> inputValue = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), input);
        final StringFnValue lowerValue = new StringFnValue(StringFnValue.StringFn.LOWER, inputValue);
        final Object result = lowerValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals(expected, result);
    }

    /**
     * Parametrized test for multiple string inputs with UPPER.
     */
    @ParameterizedTest
    @MethodSource("upperTestCases")
    void testUpperParametrized(String input, String expected) {
        final LiteralValue<String> inputValue = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), input);
        final StringFnValue upperValue = new StringFnValue(StringFnValue.StringFn.UPPER, inputValue);
        final Object result = upperValue.eval(null, EvaluationContext.empty());
        Assertions.assertEquals(expected, result);
    }

    static Stream<Arguments> lowerTestCases() {
        return Stream.of(
                Arguments.of("HELLO", "hello"),
                Arguments.of("hello", "hello"),
                Arguments.of("HeLLo", "hello"),
                Arguments.of("ABC123", "abc123"),
                Arguments.of("Hello World!", "hello world!"),
                Arguments.of("", ""),
                Arguments.of("CAFÉ", "café")
        );
    }

    static Stream<Arguments> upperTestCases() {
        return Stream.of(
                Arguments.of("hello", "HELLO"),
                Arguments.of("HELLO", "HELLO"),
                Arguments.of("HeLLo", "HELLO"),
                Arguments.of("abc123", "ABC123"),
                Arguments.of("Hello World!", "HELLO WORLD!"),
                Arguments.of("", ""),
                Arguments.of("café", "CAFÉ")
        );
    }

    /**
     * Helper method to verify serialization, deserialization, planHash, and equals.
     * This is the key method that tests planHash and equals after serialization round-trip.
     */
    @Nonnull
    private static Value verifySerialization(@Nonnull final Value value) {
        PlanSerializationContext serializationContext = new PlanSerializationContext(
                DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        final PValue planProto = value.toValueProto(serializationContext);
        final byte[] serializedValue = planProto.toByteArray();
        final PValue parsedValueProto;
        try {
            parsedValueProto = PValue.parseFrom(serializedValue);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        serializationContext = new PlanSerializationContext(
                DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        final Value deserializedValue = Value.fromValueProto(serializationContext, parsedValueProto);

        // Test planHash equality after serialization
        Assertions.assertEquals(
                value.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                deserializedValue.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                "planHash should be equal after serialization/deserialization");

        // Test equals() after serialization
        Assertions.assertEquals(value, deserializedValue,
                "Value should be equal to itself after serialization/deserialization");

        return deserializedValue;
    }
}
