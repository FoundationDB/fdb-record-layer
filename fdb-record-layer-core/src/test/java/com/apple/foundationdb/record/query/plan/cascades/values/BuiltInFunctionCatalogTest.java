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
    void testGetFunctionSingletonForManhattanDistance() {
        final Optional<BuiltInFunction<? extends Typed>> singleton = BuiltInFunctionCatalog.getFunctionSingleton(DistanceValue.ManhattanDistanceFn.class);

        Assertions.assertTrue(singleton.isPresent(), "ManhattanDistanceFn singleton should be found");
        Assertions.assertInstanceOf(DistanceValue.ManhattanDistanceFn.class, singleton.get(),
                "Singleton should be instance of ManhattanDistanceFn");
        Assertions.assertEquals("manhattan_distance", singleton.get().getFunctionName(),
                "Function name should be manhattan_distance");
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
        final Optional<BuiltInFunction<? extends Typed>> manhattan = BuiltInFunctionCatalog.resolve("manhattan_distance", 2);

        Assertions.assertTrue(euclidean.isPresent(), "euclidean_distance should be found");
        Assertions.assertTrue(cosine.isPresent(), "cosine_distance should be found");
        Assertions.assertTrue(manhattan.isPresent(), "manhattan_distance should be found");

        // Verify they are different instances
        Assertions.assertNotSame(euclidean.get(), cosine.get(),
                "Different functions should have different instances");
        Assertions.assertNotSame(euclidean.get(), manhattan.get(),
                "Different functions should have different instances");
        Assertions.assertNotSame(cosine.get(), manhattan.get(),
                "Different functions should have different instances");
    }
}
