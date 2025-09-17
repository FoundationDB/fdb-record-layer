/*
 * MetricTest.java
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

package com.apple.foundationdb.async.hnsw;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricTest {
    private final Metric.ManhattanMetric manhattanMetric = new Metric.ManhattanMetric();
    private final Metric.EuclideanMetric euclideanMetric = new Metric.EuclideanMetric();
    private final Metric.EuclideanSquareMetric euclideanSquareMetric = new Metric.EuclideanSquareMetric();
    private final Metric.CosineMetric cosineMetric = new Metric.CosineMetric();
    private Metric.DotProductMetric dotProductMetric;

    @BeforeEach

    public void setUp() {
        dotProductMetric = new Metric.DotProductMetric();
    }

    @Test
    public void manhattanMetricDistanceWithIdenticalVectorsShouldReturnZeroTest() {
        // Arrange
        Double[] vector1 = {1.0, 2.5, -3.0};
        Double[] vector2 = {1.0, 2.5, -3.0};
        double expectedDistance = 0.0;

        // Act
        double actualDistance = manhattanMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void manhattanMetricDistanceWithPositiveValueVectorsShouldReturnCorrectDistanceTest() {
        // Arrange
        Double[] vector1 = {1.0, 2.0, 3.0};
        Double[] vector2 = {4.0, 5.0, 6.0};
        double expectedDistance = 9.0; // |1-4| + |2-5| + |3-6| = 3 + 3 + 3

        // Act
        double actualDistance = manhattanMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void euclideanMetricDistanceWithIdenticalVectorsShouldReturnZeroTest() {
        // Arrange
        Double[] vector1 = {1.0, 2.5, -3.0};
        Double[] vector2 = {1.0, 2.5, -3.0};
        double expectedDistance = 0.0;

        // Act
        double actualDistance = euclideanMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void euclideanMetricDistanceWithDifferentPositiveVectorsShouldReturnCorrectDistanceTest() {
        // Arrange
        Double[] vector1 = {1.0, 2.0};
        Double[] vector2 = {4.0, 6.0};
        double expectedDistance = 5.0; // sqrt((1-4)^2 + (2-6)^2) = sqrt(9 + 16) = 5.0

        // Act
        double actualDistance = euclideanMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void euclideanSquareMetricDistanceWithIdenticalVectorsShouldReturnZeroTest() {
        // Arrange
        Double[] vector1 = {1.0, 2.5, -3.0};
        Double[] vector2 = {1.0, 2.5, -3.0};
        double expectedDistance = 0.0;

        // Act
        double actualDistance = euclideanSquareMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void euclideanSquareMetricDistanceWithDifferentPositiveVectorsShouldReturnCorrectDistanceTest() {
        // Arrange
        Double[] vector1 = {1.0, 2.0};
        Double[] vector2 = {4.0, 6.0};
        double expectedDistance = 25.0; // (1-4)^2 + (2-6)^2 = 9 + 16 = 25.0

        // Act
        double actualDistance = euclideanSquareMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void cosineMetricDistanceWithIdenticalVectorsReturnsZeroTest() {
        // Arrange
        Double[] vector1 = {5.0, 3.0, -2.0};
        Double[] vector2 = {5.0, 3.0, -2.0};
        double expectedDistance = 0.0;

        // Act
        double actualDistance = cosineMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void cosineMetricDistanceWithOrthogonalVectorsReturnsOneTest() {
        // Arrange
        Double[] vector1 = {1.0, 0.0};
        Double[] vector2 = {0.0, 1.0};
        double expectedDistance = 1.0;

        // Act
        double actualDistance = cosineMetric.distance(vector1, vector2);

        // Assert
        assertEquals(expectedDistance, actualDistance, 0.00001);
    }

    @Test
    public void dotProductMetricComparativeDistanceWithPositiveVectorsTest() {
        Double[] vector1 = {1.0, 2.0, 3.0};
        Double[] vector2 = {4.0, 5.0, 6.0};
        double expected = -32.0;

        double actual = dotProductMetric.comparativeDistance(vector1, vector2);

        assertEquals(expected, actual, 0.00001);
    }

    @Test
    public void dotProductMetricComparativeDistanceWithOrthogonalVectorsReturnsZeroTest() {
        Double[] vector1 = {1.0, 0.0};
        Double[] vector2 = {0.0, 1.0};
        double expected = -0.0;

        double actual = dotProductMetric.comparativeDistance(vector1, vector2);

        assertEquals(expected, actual, 0.00001);
    }
}