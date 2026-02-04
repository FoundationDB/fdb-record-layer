/*
 * Metric.java
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

package com.apple.foundationdb.linear;

import javax.annotation.Nonnull;

/**
 * Represents various distance calculation strategies (metrics) for vectors.
 * <p>
 * Each enum constant holds a specific metric implementation, providing a type-safe way to calculate the distance
 * between two points in a multidimensional space.
 *
 * @see MetricDefinition
 */
public enum Metric implements MetricDefinition {
    /**
     * Represents the Euclidean distance metric, implemented by {@link MetricDefinition.EuclideanMetric}.
     * <p>
     * This metric calculates the "ordinary" straight-line distance between two points
     * in Euclidean space. The distance is the square root of the sum of the
     * squared differences between the corresponding coordinates of the two points.
     * @see MetricDefinition.EuclideanMetric
     */
    EUCLIDEAN_METRIC(new MetricDefinition.EuclideanMetric()),

    /**
     * Represents the squared Euclidean distance metric, implemented by {@link MetricDefinition.EuclideanSquareMetric}.
     * <p>
     * This metric calculates the sum of the squared differences between the coordinates of two vectors, defined as
     * {@code sum((p_i - q_i)^2)}. It is computationally less expensive than the standard Euclidean distance because it
     * avoids the final square root operation.
     * <p>
     * This is often preferred in algorithms where comparing distances is more important than the actual distance value,
     * such as in clustering algorithms, as it preserves the relative ordering of distances.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Euclidean_distance#Squared_Euclidean_distance">Squared Euclidean
     * distance</a>
     * @see MetricDefinition.EuclideanSquareMetric
     */
    EUCLIDEAN_SQUARE_METRIC(new MetricDefinition.EuclideanSquareMetric()),

    /**
     * Represents the Cosine distance metric, implemented by {@link MetricDefinition.CosineMetric}.
     * <p>
     * This metric calculates a "distance" between two vectors {@code v1} and {@code v2} that ranges between
     * {@code 0.0d} and {@code 2.0d} that corresponds to {@code 1 - cos(v1, v2)}, meaning that if {@code v1 == v2},
     * the distance is {@code 0} while if {@code v1} is orthogonal to {@code v2} it is {@code 1}.
     * @see MetricDefinition.CosineMetric
     */
    COSINE_METRIC(new MetricDefinition.CosineMetric()),

    /**
     * Dot product similarity, implemented by {@link MetricDefinition.DotProductMetric}
     * <p>
     * This metric calculates the inverted dot product of two vectors. It is not a true metric as several properties of
     * true metrics do not hold, for instance this <i>metric</i> can be negative.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Dot_product">Dot Product</a>
     * @see MetricDefinition.DotProductMetric
     */
    DOT_PRODUCT_METRIC(new MetricDefinition.DotProductMetric());

    @Nonnull
    private final MetricDefinition metricDefinition;

    /**
     * Constructs a new Metric instance with the specified metric.
     * @param metricDefinition the metric to be associated with this Metric instance; must not be null.
     */
    Metric(@Nonnull final MetricDefinition metricDefinition) {
        this.metricDefinition = metricDefinition;
    }

    @Override
    public boolean satisfiesZeroSelfDistance() {
        return metricDefinition.satisfiesZeroSelfDistance();
    }

    @Override
    public boolean satisfiesPositivity() {
        return metricDefinition.satisfiesPositivity();
    }

    @Override
    public boolean satisfiesSymmetry() {
        return metricDefinition.satisfiesSymmetry();
    }

    @Override
    public boolean satisfiesTriangleInequality() {
        return metricDefinition.satisfiesTriangleInequality();
    }

    @Override
    public boolean satisfiesPreservedUnderTranslation() {
        return metricDefinition.satisfiesPreservedUnderTranslation();
    }


    @Override
    public double distance(@Nonnull final double[] vectorData1, @Nonnull final double[] vectorData2) {
        return metricDefinition.distance(vectorData1, vectorData2);
    }

    /**
     * Calculates a distance between two n-dimensional vectors.
     * <p>
     * The two vectors are represented as arrays of {@link  Double} and must be of the
     * same length (i.e., have the same number of dimensions).
     *
     * @param vector1 the first vector. Must not be null.
     * @param vector2 the second vector. Must not be null and must have the same
     * length as {@code vector1}.
     *
     * @return the calculated distance as a {@code double}.
     *
     * @throws IllegalArgumentException if the vectors have different lengths.
     * @throws NullPointerException if either {@code vector1} or {@code vector2} is null.
     */
    public double distance(@Nonnull RealVector vector1, @Nonnull RealVector vector2) {
        return distance(vector1.getData(), vector2.getData());
    }

    @Override
    public String toString() {
        return metricDefinition.toString();
    }
}
