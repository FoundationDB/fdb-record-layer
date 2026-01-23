/*
 * DistanceValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.DistanceValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tests evaluation of {@link DistanceValue}.
 */
class DistanceValueTest {
    private static final LiteralValue<FloatRealVector> VECTOR_1_0_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {1.0f, 0.0f, 0.0f}));
    private static final LiteralValue<FloatRealVector> VECTOR_0_1_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {0.0f, 1.0f, 0.0f}));
    private static final LiteralValue<FloatRealVector> VECTOR_3_4_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {3.0f, 4.0f, 0.0f}));
    private static final LiteralValue<FloatRealVector> VECTOR_0_0_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {0.0f, 0.0f, 0.0f}));
    private static final LiteralValue<DoubleRealVector> VECTOR_DOUBLE_1_2_2 = new LiteralValue<>(Type.Vector.of(false, 64, 3), new DoubleRealVector(new double[] {1.0, 2.0, 2.0}));
    private static final LiteralValue<DoubleRealVector> VECTOR_DOUBLE_4_5_6 = new LiteralValue<>(Type.Vector.of(false, 64, 3), new DoubleRealVector(new double[] {4.0, 5.0, 6.0}));
    private static final LiteralValue<HalfRealVector> VECTOR_HALF_2_3_6 = new LiteralValue<>(Type.Vector.of(false, 16, 3), new HalfRealVector(new double[] {2.0, 3.0, 6.0}));
    private static final LiteralValue<HalfRealVector> VECTOR_HALF_5_6_9 = new LiteralValue<>(Type.Vector.of(false, 16, 3), new HalfRealVector(new double[] {5.0, 6.0, 9.0}));
    private static final LiteralValue<FloatRealVector> VECTOR_NULL = new LiteralValue<>(Type.Vector.of(false, 32, 3), null);

    private static final EvaluationContext evaluationContext = EvaluationContext.forBinding(Bindings.Internal.CORRELATION.bindingName("ident"), QueryResult.ofComputed(TestRecords7Proto.MyRecord1.newBuilder().setRecNo(4L).build()));

    @ParameterizedTest
    @MethodSource("vectorDistanceFunctionTests")
    void testVectorDistanceFunctions(LiteralValue<?> vector1, LiteralValue<?> vector2, BuiltInFunction<?> function, Double expectedDistance) {
        final List<Value> arguments = List.of(vector1, vector2);
        final DistanceValue value = (DistanceValue) function.encapsulate(arguments);
        final Object result = value.evalWithoutStore(evaluationContext);
        Assertions.assertNotNull(result, "Vector distance function should not return null for non-null vectors");
        Assertions.assertInstanceOf(Double.class, result, "Vector distance function should return a Double");
        Assertions.assertEquals(expectedDistance, (Double)result, 1e-6,
                String.format("Expected %s(%s, %s) to be %f", function.getFunctionName(), vector1, vector2, expectedDistance));
    }

    static Stream<Arguments> vectorDistanceFunctionTests() {
        return Stream.of(
                // Euclidean distance tests
                // Distance from (1,0,0) to (0,1,0) = sqrt(1^2 + 1^2) = sqrt(2) ≈ 1.414
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new DistanceValue.EuclideanDistanceFn(), Math.sqrt(2.0)),
                // Distance from (3,4,0) to (0,0,0) = sqrt(3^2 + 4^2) = 5.0
                Arguments.of(VECTOR_3_4_0, VECTOR_0_0_0, new DistanceValue.EuclideanDistanceFn(), 5.0),
                // Distance from same vector to itself = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new DistanceValue.EuclideanDistanceFn(), 0.0),
                // Distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new DistanceValue.EuclideanDistanceFn(),
                        new Metric.EuclideanMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Euclidean square distance tests
                // Squared distance from (1,0,0) to (0,1,0) = 1^2 + 1^2 = 2.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new DistanceValue.EuclideanSquareDistanceFn(), 2.0),
                // Squared distance from (3,4,0) to (0,0,0) = 3^2 + 4^2 = 25.0
                Arguments.of(VECTOR_3_4_0, VECTOR_0_0_0, new DistanceValue.EuclideanSquareDistanceFn(), 25.0),
                // Squared distance from same vector to itself = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new DistanceValue.EuclideanSquareDistanceFn(), 0.0),
                // Squared distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new DistanceValue.EuclideanSquareDistanceFn(),
                        new Metric.EuclideanSquareMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Manhattan distance tests
                // Manhattan distance from (1,0,0) to (0,1,0) = |1-0| + |0-1| + |0-0| = 2.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new DistanceValue.ManhattanDistanceFn(), 2.0),
                // Manhattan distance from (3,4,0) to (0,0,0) = |3| + |4| + |0| = 7.0
                Arguments.of(VECTOR_3_4_0, VECTOR_0_0_0, new DistanceValue.ManhattanDistanceFn(), 7.0),
                // Manhattan distance from same vector to itself = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new DistanceValue.ManhattanDistanceFn(), 0.0),
                // Manhattan distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new DistanceValue.ManhattanDistanceFn(),
                        new Metric.ManhattanMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Cosine distance tests
                // Cosine distance between orthogonal vectors (1,0,0) and (0,1,0) = 1.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new DistanceValue.CosineDistanceFn(), 1.0),
                // Cosine distance from same vector to itself = 0.0 (identical direction)
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new DistanceValue.CosineDistanceFn(), 0.0),
                // Cosine distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new DistanceValue.CosineDistanceFn(),
                        new Metric.CosineMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Dot product distance tests (negative dot product)
                // Dot product of (1,0,0) and (0,1,0) = 0, so distance = -0 = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new DistanceValue.DotProductDistanceFn(), 0.0),
                // Dot product of (1,0,0) and itself = 1, so distance = -1 = -1.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new DistanceValue.DotProductDistanceFn(), -1.0),
                // Dot product distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new DistanceValue.DotProductDistanceFn(),
                        new Metric.DotProductMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Half precision vector tests
                // Euclidean distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new DistanceValue.EuclideanDistanceFn(),
                        new Metric.EuclideanMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Euclidean square distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new DistanceValue.EuclideanSquareDistanceFn(),
                        new Metric.EuclideanSquareMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Manhattan distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new DistanceValue.ManhattanDistanceFn(),
                        new Metric.ManhattanMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Cosine distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new DistanceValue.CosineDistanceFn(),
                        new Metric.CosineMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Dot product distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new DistanceValue.DotProductDistanceFn(),
                        new Metric.DotProductMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData()))
        );
    }

    @ParameterizedTest
    @MethodSource("nullVectorTestCases")
    void testNullVectorThrowsException(LiteralValue<?> vector1, LiteralValue<?> vector2, BuiltInFunction<?> function) {
        final List<Value> arguments = List.of(vector1, vector2);
        final DistanceValue value = (DistanceValue) function.encapsulate(arguments);
        RecordCoreException exception = Assertions.assertThrows(RecordCoreException.class,
                () -> value.evalWithoutStore(evaluationContext),
                "Distance function should throw RecordCoreException for null vectors");
        Assertions.assertEquals("Vectors cannot be null", exception.getMessage(),
                "Exception should have the correct error message");
    }

    static Stream<Arguments> nullVectorTestCases() {
        return Stream.of(
                // Euclidean distance with null vectors
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new DistanceValue.EuclideanDistanceFn()),
                Arguments.of(VECTOR_NULL, VECTOR_1_0_0, new DistanceValue.EuclideanDistanceFn()),
                Arguments.of(VECTOR_1_0_0, VECTOR_NULL, new DistanceValue.EuclideanDistanceFn()),

                // Euclidean square distance with null vectors
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new DistanceValue.EuclideanSquareDistanceFn()),
                Arguments.of(VECTOR_NULL, VECTOR_1_0_0, new DistanceValue.EuclideanSquareDistanceFn()),
                Arguments.of(VECTOR_1_0_0, VECTOR_NULL, new DistanceValue.EuclideanSquareDistanceFn()),

                // Manhattan distance with null vectors
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new DistanceValue.ManhattanDistanceFn()),
                Arguments.of(VECTOR_NULL, VECTOR_1_0_0, new DistanceValue.ManhattanDistanceFn()),
                Arguments.of(VECTOR_1_0_0, VECTOR_NULL, new DistanceValue.ManhattanDistanceFn()),

                // Cosine distance with null vectors
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new DistanceValue.CosineDistanceFn()),
                Arguments.of(VECTOR_NULL, VECTOR_0_1_0, new DistanceValue.CosineDistanceFn()),
                Arguments.of(VECTOR_0_1_0, VECTOR_NULL, new DistanceValue.CosineDistanceFn()),

                // Dot product distance with null vectors
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new DistanceValue.DotProductDistanceFn()),
                Arguments.of(VECTOR_NULL, VECTOR_0_1_0, new DistanceValue.DotProductDistanceFn()),
                Arguments.of(VECTOR_0_1_0, VECTOR_NULL, new DistanceValue.DotProductDistanceFn())
        );
    }

    @Nonnull
    static Stream<BuiltInFunction<?>> vectorDistanceFunctions() {
        return Stream.of(
                new DistanceValue.EuclideanDistanceFn(),
                new DistanceValue.EuclideanSquareDistanceFn(),
                new DistanceValue.ManhattanDistanceFn(),
                new DistanceValue.CosineDistanceFn(),
                new DistanceValue.DotProductDistanceFn()
        );
    }

    @ParameterizedTest
    @MethodSource("vectorDistanceFunctions")
    void testVectorDistanceSemanticEquality(BuiltInFunction<?> distanceFunction) {
        final List<Value> arguments1 = List.of(VECTOR_1_0_0, VECTOR_0_1_0);
        final List<Value> arguments2 = List.of(VECTOR_1_0_0, VECTOR_0_1_0);
        final List<Value> arguments3 = List.of(VECTOR_1_0_0, VECTOR_3_4_0);

        final Value value1 = (Value) distanceFunction.encapsulate(arguments1);
        final Value value2 = (Value) distanceFunction.encapsulate(arguments2);
        final Value value3 = (Value) distanceFunction.encapsulate(arguments3);

        // Same arguments should be semantically equal
        Assertions.assertTrue(value1.semanticEquals(value2, AliasMap.emptyMap()),
                value1 + " and " + value2 + " should be semantically equal with same arguments");
        Assertions.assertEquals(value1.semanticHashCode(), value2.semanticHashCode(),
                value1 + " and " + value2 + " should have same hash code");

        // Different arguments should not be semantically equal
        Assertions.assertFalse(value1.semanticEquals(value3, AliasMap.emptyMap()),
                value1 + " and " + value3 + " should not be semantically equal with different arguments");
    }

    @ParameterizedTest
    @MethodSource("vectorDistanceFunctions")
    void testDistanceValueEquals(BuiltInFunction<?> distanceFunction) {
        final List<Value> arguments1 = List.of(VECTOR_1_0_0, VECTOR_0_1_0);
        final List<Value> arguments2 = List.of(VECTOR_1_0_0, VECTOR_0_1_0);
        final List<Value> arguments3 = List.of(VECTOR_1_0_0, VECTOR_3_4_0);

        final DistanceValue value1 = (DistanceValue) distanceFunction.encapsulate(arguments1);
        final DistanceValue value2 = (DistanceValue) distanceFunction.encapsulate(arguments2);
        final DistanceValue value3 = (DistanceValue) distanceFunction.encapsulate(arguments3);

        // Reflexive: value should equal itself
        Assertions.assertEquals(value1, value1,
                "DistanceValue should equal itself");

        // Symmetric: values with same arguments should be equal
        Assertions.assertEquals(value1, value2,
                "DistanceValue with same arguments should be equal");
        Assertions.assertEquals(value2, value1,
                "Equals should be symmetric");

        // Hash code consistency
        Assertions.assertEquals(value1.hashCode(), value2.hashCode(),
                "Equal DistanceValues should have same hash code");

        // Different arguments should not be equal
        Assertions.assertNotEquals(value1, value3,
                "DistanceValue with different arguments should not be equal");

        // Not equal to null
        Assertions.assertNotEquals(null, value1,
                "DistanceValue should not equal null");

        // Not equal to different type
        Assertions.assertNotEquals(value1, "not a DistanceValue",
                "DistanceValue should not equal object of different type");
    }

    @ParameterizedTest
    @MethodSource("vectorDistanceFunctions")
    void testDistanceValueEqualsDifferentDistanceFunctions(BuiltInFunction<?> distanceFunction) {
        // Create values using different distance functions with same arguments
        final List<Value> arguments = List.of(VECTOR_1_0_0, VECTOR_0_1_0);

        final DistanceValue euclideanValue = (DistanceValue) new DistanceValue.EuclideanDistanceFn().encapsulate(arguments);
        final DistanceValue cosineValue = (DistanceValue) new DistanceValue.CosineDistanceFn().encapsulate(arguments);
        final DistanceValue manhattanValue = (DistanceValue) new DistanceValue.ManhattanDistanceFn().encapsulate(arguments);

        // Different distance functions should produce non-equal values even with same arguments
        Assertions.assertNotEquals(euclideanValue, cosineValue,
                "Euclidean and Cosine distance values should not be equal");
        Assertions.assertNotEquals(euclideanValue, manhattanValue,
                "Euclidean and Manhattan distance values should not be equal");
        Assertions.assertNotEquals(cosineValue, manhattanValue,
                "Cosine and Manhattan distance values should not be equal");
    }
}
