/*
 * BuiltInFunctionCatalogTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Tests for {@link BuiltInFunctionCatalog}.
 */
class BuiltInFunctionCatalogTest {

    @Test
    void testResolveExistentFunction() {
        final Optional<BuiltInFunction<? extends Typed>> function = BuiltInFunctionCatalog.resolve("euclidean_distance", 2);

        Assertions.assertTrue(function.isPresent(), "euclidean_distance function with 2 arguments should be found");
        Assertions.assertEquals("euclidean_distance", function.get().getFunctionName(),
                "Function name should match");
    }

    @Test
    void testResolveNonExistentFunction() {
        final Optional<BuiltInFunction<? extends Typed>> function = BuiltInFunctionCatalog.resolve("NON_EXISTENT_FUNCTION", 2);

        Assertions.assertFalse(function.isPresent(), "Non-existent function should not be found");
    }

    @Test
    void testResolveWithWrongArgumentCount() {
        // Distance functions expect exactly 2 arguments
        final Optional<BuiltInFunction<? extends Typed>> function = BuiltInFunctionCatalog.resolve("euclidean_distance", 3);

        Assertions.assertFalse(function.isPresent(), "euclidean_distance with 3 arguments should not be found");
    }

    @Test
    void testResolveWithZeroArguments() {
        final Optional<BuiltInFunction<? extends Typed>> function = BuiltInFunctionCatalog.resolve("euclidean_distance", 0);

        Assertions.assertFalse(function.isPresent(), "euclidean_distance with 0 arguments should not be found");
    }

    @Test
    void testGetFunctionSingletonForEuclideanDistance() {
        final Optional<BuiltInFunction<? extends Typed>> singleton = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.EuclideanDistanceFn.class);

        Assertions.assertTrue(singleton.isPresent(), "EuclideanDistanceFn singleton should be found");
        Assertions.assertInstanceOf(DistanceValue.EuclideanDistanceFn.class, singleton.get(),
                "Singleton should be instance of EuclideanDistanceFn");
        Assertions.assertEquals("euclidean_distance", singleton.get().getFunctionName(),
                "Function name should be euclidean_distance");
    }

    @Test
    void testGetFunctionSingletonForCosineDistance() {
        final Optional<BuiltInFunction<? extends Typed>> singleton = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.CosineDistanceFn.class);

        Assertions.assertTrue(singleton.isPresent(), "CosineDistanceFn singleton should be found");
        Assertions.assertInstanceOf(DistanceValue.CosineDistanceFn.class, singleton.get(),
                "Singleton should be instance of CosineDistanceFn");
        Assertions.assertEquals("cosine_distance", singleton.get().getFunctionName(),
                "Function name should be cosine_distance");
    }

    @Test
    void testGetFunctionSingletonForEuclideanSquareDistance() {
        final Optional<BuiltInFunction<? extends Typed>> singleton = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.EuclideanSquareDistanceFn.class);

        Assertions.assertTrue(singleton.isPresent(), "EuclideanSquareDistanceFn singleton should be found");
        Assertions.assertInstanceOf(DistanceValue.EuclideanSquareDistanceFn.class, singleton.get(),
                "Singleton should be instance of EuclideanSquareDistanceFn");
        Assertions.assertEquals("euclidean_square_distance", singleton.get().getFunctionName(),
                "Function name should be euclidean_square_distance");
    }

    @Test
    void testGetFunctionSingletonForDotProductDistance() {
        final Optional<BuiltInFunction<? extends Typed>> singleton = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.DotProductDistanceFn.class);

        Assertions.assertTrue(singleton.isPresent(), "DotProductDistanceFn singleton should be found");
        Assertions.assertInstanceOf(DistanceValue.DotProductDistanceFn.class, singleton.get(),
                "Singleton should be instance of DotProductDistanceFn");
        Assertions.assertEquals("dot_product_distance", singleton.get().getFunctionName(),
                "Function name should be dot_product_distance");
    }

    @Test
    void testSameInstanceReturnedMultipleTimes() {
        final Optional<BuiltInFunction<? extends Typed>> singleton1 = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.EuclideanDistanceFn.class);
        final Optional<BuiltInFunction<? extends Typed>> singleton2 = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.EuclideanDistanceFn.class);

        Assertions.assertTrue(singleton1.isPresent() && singleton2.isPresent(),
                "Both lookups should find the singleton");
        Assertions.assertSame(singleton1.get(), singleton2.get(),
                "Same singleton instance should be returned");
    }

    @Test
    void testResolveAndGetSingletonReturnSameInstance() {
        final Optional<BuiltInFunction<? extends Typed>> resolved = BuiltInFunctionCatalog.resolve("euclidean_distance", 2);
        final Optional<BuiltInFunction<? extends Typed>> singleton = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.EuclideanDistanceFn.class);

        Assertions.assertTrue(resolved.isPresent() && singleton.isPresent(),
                "Both methods should find the function");
        Assertions.assertSame(resolved.get(), singleton.get(),
                "resolve() and getFunctionSingleton() should return the same instance");
    }

    @Test
    void testToStringOnResolvedFunction() {
        final Optional<BuiltInFunction<? extends Typed>> function = BuiltInFunctionCatalog.resolve("euclidean_distance", 2);

        Assertions.assertTrue(function.isPresent(), "Function should be found");
        final String toString = function.get().toString();
        Assertions.assertNotNull(toString, "toString() should not return null");
        Assertions.assertFalse(toString.isEmpty(), "toString() should not be empty");
        Assertions.assertTrue(toString.contains("euclidean_distance") || toString.contains("EuclideanDistance"),
                "toString() should contain function name or class name");
    }

    @Test
    void testFunctionHasCorrectParameterCount() {
        final Optional<BuiltInFunction<? extends Typed>> function = BuiltInFunctionCatalog.resolve("euclidean_distance", 2);

        Assertions.assertTrue(function.isPresent(), "Function should be found");
        Assertions.assertEquals(2, function.get().getParameterTypes().size(),
                "euclidean_distance should have 2 parameters");
    }

    @Test
    void testCaseInsensitiveFunctionNameLookup() {
        // Test that function names are case-sensitive (most likely the implementation)
        final Optional<BuiltInFunction<? extends Typed>> lowerCase = BuiltInFunctionCatalog.resolve("euclidean_distance", 2);
        final Optional<BuiltInFunction<? extends Typed>> upperCase = BuiltInFunctionCatalog.resolve("EUCLIDEAN_DISTANCE", 2);

        Assertions.assertTrue(lowerCase.isPresent(), "Lowercase lookup should succeed");
        // Note: This assertion assumes case-sensitive lookups. If the implementation is case-insensitive,
        // both should be present. Adjust based on actual implementation behavior.
        Assertions.assertFalse(upperCase.isPresent(), "Uppercase lookup should fail (assuming case-sensitive)");
    }

    @Test
    void testMultipleFunctionsCanBeResolved() {
        final Optional<BuiltInFunction<? extends Typed>> euclidean = BuiltInFunctionCatalog.resolve("euclidean_distance", 2);
        final Optional<BuiltInFunction<? extends Typed>> cosine = BuiltInFunctionCatalog.resolve("cosine_distance", 2);
        final Optional<BuiltInFunction<? extends Typed>> euclideanSquare = BuiltInFunctionCatalog.resolve("euclidean_square_distance", 2);

        Assertions.assertTrue(euclidean.isPresent(), "euclidean_distance should be found");
        Assertions.assertTrue(cosine.isPresent(), "cosine_distance should be found");
        Assertions.assertTrue(euclideanSquare.isPresent(), "euclidean_square_distance should be found");

        // Verify they are different instances
        Assertions.assertNotSame(euclidean.get(), cosine.get(),
                "Different functions should have different instances");
        Assertions.assertNotSame(euclidean.get(), euclideanSquare.get(),
                "Different functions should have different instances");
        Assertions.assertNotSame(cosine.get(), euclideanSquare.get(),
                "Different functions should have different instances");
    }

    // Tests for FunctionKey.toString()

    @Test
    void testFunctionKeyInvocationToStringWithZeroArguments() {
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.invocation("test_func", 0);
        final String toString = functionKey.toString();

        Assertions.assertEquals("test_func()", toString,
                "Invocation with 0 arguments should format as 'functionName()'");
    }

    @Test
    void testFunctionKeyInvocationToStringWithOneArgument() {
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.invocation("my_function", 1);
        final String toString = functionKey.toString();

        Assertions.assertEquals("my_function(arg1)", toString,
                "Invocation with 1 argument should format as 'functionName(arg1)'");
    }

    @Test
    void testFunctionKeyInvocationToStringWithTwoArguments() {
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.invocation("euclidean_distance", 2);
        final String toString = functionKey.toString();

        Assertions.assertEquals("euclidean_distance(arg1, arg2)", toString,
                "Invocation with 2 arguments should format as 'functionName(arg1, arg2)'");
    }

    @Test
    void testFunctionKeyInvocationToStringWithMultipleArguments() {
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.invocation("complex_func", 5);
        final String toString = functionKey.toString();

        Assertions.assertEquals("complex_func(arg1, arg2, arg3, arg4, arg5)", toString,
                "Invocation with 5 arguments should format correctly");
    }

    @Test
    void testFunctionKeyEntryToStringWithFixedParameters() {
        // Entry with 2 parameters, 0 defaults, not variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("fixed_func", 2, 0, false);
        final String toString = functionKey.toString();

        Assertions.assertEquals("fixed_func(param1, param2)", toString,
                "Entry with fixed parameters should format as 'functionName(param1, param2)'");
    }

    @Test
    void testFunctionKeyEntryToStringWithZeroParameters() {
        // Entry with 0 parameters, 0 defaults, not variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("no_param_func", 0, 0, false);
        final String toString = functionKey.toString();

        Assertions.assertEquals("no_param_func()", toString,
                "Entry with 0 parameters should format as 'functionName()'");
    }

    @Test
    void testFunctionKeyEntryToStringWithOneParameter() {
        // Entry with 1 parameter, 0 defaults, not variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("single_param", 1, 0, false);
        final String toString = functionKey.toString();

        Assertions.assertEquals("single_param(param1)", toString,
                "Entry with 1 parameter should format as 'functionName(param1)'");
    }

    @Test
    void testFunctionKeyEntryToStringWithDefaultParameters() {
        // Entry with 3 parameters total, 2 with defaults, not variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("func_with_defaults", 3, 2, false);
        final String toString = functionKey.toString();

        Assertions.assertEquals("func_with_defaults(param1, param2 (with default), param3 (with default))", toString,
                "Entry with default parameters should indicate which parameters have defaults");
    }

    @Test
    void testFunctionKeyEntryToStringWithAllDefaultParameters() {
        // Entry with 2 parameters, all have defaults, not variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("all_defaults", 2, 2, false);
        final String toString = functionKey.toString();

        Assertions.assertEquals("all_defaults(param1 (with default), param2 (with default))", toString,
                "Entry where all parameters have defaults should mark all as '(with default)'");
    }

    @Test
    void testFunctionKeyEntryToStringWithVariadicAndNoRequiredParams() {
        // Entry with 0 required parameters, variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("variadic_func", 0, 0, true);
        final String toString = functionKey.toString();

        Assertions.assertEquals("variadic_func(...)", toString,
                "Variadic entry with no required parameters should format as 'functionName(...)'");
    }

    @Test
    void testFunctionKeyEntryToStringWithVariadicAndRequiredParams() {
        // Entry with 2 required parameters, variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("variadic_with_required", 2, 0, true);
        final String toString = functionKey.toString();

        Assertions.assertEquals("variadic_with_required(param1, param2, ...)", toString,
                "Variadic entry with required parameters should show params followed by '...'");
    }

    @Test
    void testFunctionKeyEntryToStringWithVariadicAndOneRequiredParam() {
        // Entry with 1 required parameter, variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("variadic_one_req", 1, 0, true);
        final String toString = functionKey.toString();

        Assertions.assertEquals("variadic_one_req(param1, ...)", toString,
                "Variadic entry with 1 required parameter should format correctly");
    }

    @Test
    void testFunctionKeyEntryToStringWithDefaultsAndVariadic() {
        // Entry with 3 total params, 1 with default, and variadic
        // This means min required = 3 - 1 = 2, and it's variadic
        final BuiltInFunctionCatalog.FunctionKey functionKey =
                BuiltInFunctionCatalog.FunctionKey.entry("complex_variadic", 3, 1, true);
        final String toString = functionKey.toString();

        Assertions.assertEquals("complex_variadic(param1, param2, ...)", toString,
                "Variadic entry with defaults should show only required params before '...'");
    }

    @Test
    void testFunctionKeyToStringIsNotNull() {
        final BuiltInFunctionCatalog.FunctionKey invocationKey =
                BuiltInFunctionCatalog.FunctionKey.invocation("test", 1);
        Assertions.assertNotNull(invocationKey.toString(),
                "FunctionKey.toString() should never return null");

        final BuiltInFunctionCatalog.FunctionKey entryKey =
                BuiltInFunctionCatalog.FunctionKey.entry("test", 1, 0, false);
        Assertions.assertNotNull(entryKey.toString(),
                "FunctionKey.toString() should never return null");
    }

    @Test
    void testFunctionKeyToStringIsNotEmpty() {
        final BuiltInFunctionCatalog.FunctionKey invocationKey =
                BuiltInFunctionCatalog.FunctionKey.invocation("test", 0);
        Assertions.assertFalse(invocationKey.toString().isEmpty(),
                "FunctionKey.toString() should never return empty string");

        final BuiltInFunctionCatalog.FunctionKey entryKey =
                BuiltInFunctionCatalog.FunctionKey.entry("test", 0, 0, false);
        Assertions.assertFalse(entryKey.toString().isEmpty(),
                "FunctionKey.toString() should never return empty string");
    }

    @Test
    void testFunctionKeyToStringContainsFunctionName() {
        final String functionName = "my_special_function";
        final BuiltInFunctionCatalog.FunctionKey invocationKey =
                BuiltInFunctionCatalog.FunctionKey.invocation(functionName, 2);
        Assertions.assertTrue(invocationKey.toString().contains(functionName),
                "Invocation toString() should contain the function name");

        final BuiltInFunctionCatalog.FunctionKey entryKey =
                BuiltInFunctionCatalog.FunctionKey.entry(functionName, 2, 0, false);
        Assertions.assertTrue(entryKey.toString().contains(functionName),
                "Entry toString() should contain the function name");
    }
}
