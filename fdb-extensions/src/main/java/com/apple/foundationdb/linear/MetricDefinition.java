/*
 * MetricDefinition.java
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
 * Defines a metric for measuring the distance or similarity between n-dimensional vectors.
 * <p>
 * This interface provides a contract for various distance calculation algorithms, such as Euclidean, Manhattan,
 * and Cosine distance. Implementations of this interface can be used in algorithms that require a metric for
 * comparing data vectors, like clustering or nearest neighbor searches.
 */
public interface MetricDefinition {
    /**
     * Method to be implemented by the specific metric.
     * @return {@code true} iff for all {@link RealVector}s {@code x} holds that {@code distance(x, x) == 0}
     */
    default boolean satisfiesZeroSelfDistance() {
        return true;
    }

    /**
     * Method to be implemented by the specific metric.
     * @return {@code true} iff for all {@link RealVector}s {@code x, y} holds that {@code distance(x, y) >= 0}
     */
    default boolean satisfiesPositivity() {
        return true;
    }

    /**
     * Method to be implemented by the specific metric.
     * @return {@code true} iff for all {@link RealVector}s {@code x, y} holds that
     *         {@code distance(x, y) == distance(y, x)}
     */
    default boolean satisfiesSymmetry() {
        return true;
    }

    /**
     * Method to be implemented by the specific metric.
     * @return {@code true} iff for all {@link RealVector}s {@code x, y, z} holds that
     *         {@code distance(x, y) + distance(y, z) >= distance(x, z)}
     */
    default boolean satisfiesTriangleInequality() {
        return true;
    }

    /**
     * Convenience method that returns if all properties of a metric required to be a <i>true</i> metric are satisfied.
     * @return {@code true} iff this metric is a true metric.
     */
    default boolean isTrueMetric() {
        return satisfiesZeroSelfDistance() &&
                satisfiesPositivity() &&
                satisfiesSymmetry() &&
                satisfiesTriangleInequality();
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
    double distance(@Nonnull double[] vector1, @Nonnull double[] vector2);

    /**
     * A helper method to validate that vectors can be compared.
     * @param vector1 The first vector.
     * @param vector2 The second vector.
     */
    private static void validate(double[] vector1, double[] vector2) {
        if (vector1 == null || vector2 == null) {
            throw new IllegalArgumentException("Vectors cannot be null");
        }
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException(
                    "Vectors must have the same dimensionality. Got " + vector1.length + " and " + vector2.length
            );
        }
        if (vector1.length == 0) {
            throw new IllegalArgumentException("Vectors cannot be empty.");
        }
    }

    /**
     * Represents the Manhattan distance metric.
     * <p>
     * This metric calculates a distance overlaying the multidimensional space with a grid-like structure only allowing
     * orthogonal lines. In 2D this resembles the street structure in Manhattan where one would have to go {@code x}
     * blocks north/south and {@code y} blocks east/west leading to a total distance of {@code x + y}.
     */
    class ManhattanMetric implements MetricDefinition {
        @Override
        public double distance(@Nonnull final double[] vector1, @Nonnull final double[] vector2) {
            MetricDefinition.validate(vector1, vector2);
            double sumOfAbsDiffs = 0.0;
            for (int i = 0; i < vector1.length; i++) {
                sumOfAbsDiffs += Math.abs(vector1[i] - vector2[i]);
            }
            return sumOfAbsDiffs;
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    /**
     * Represents the Euclidean distance metric.
     * <p>
     * This metric calculates the "ordinary" straight-line distance between two points
     * in Euclidean space. The distance is the square root of the sum of the
     * squared differences between the corresponding coordinates of the two points.
     */
    class EuclideanMetric implements MetricDefinition {
        @Override
        public double distance(@Nonnull final double[] vector1, @Nonnull final double[] vector2) {
            MetricDefinition.validate(vector1, vector2);

            return Math.sqrt(EuclideanSquareMetric.distanceInternal(vector1, vector2));
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    /**
     * Represents the squared Euclidean distance metric.
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
     */
    class EuclideanSquareMetric implements MetricDefinition {
        @Override
        public boolean satisfiesTriangleInequality() {
            return false;
        }

        @Override
        public double distance(@Nonnull final double[] vector1, @Nonnull final double[] vector2) {
            MetricDefinition.validate(vector1, vector2);
            return distanceInternal(vector1, vector2);
        }

        private static double distanceInternal(@Nonnull final double[] vector1, @Nonnull final double[] vector2) {
            double sumOfSquares = 0.0d;
            for (int i = 0; i < vector1.length; i++) {
                double diff = vector1[i] - vector2[i];
                sumOfSquares += diff * diff;
            }
            return sumOfSquares;
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    /**
     * Represents the Cosine distance metric.
     * <p>
     * This metric calculates a "distance" between two vectors {@code v1} and {@code v2} that ranges between
     * {@code 0.0d} and {@code 2.0d} that corresponds to {@code 1 - cos(v1, v2)}, meaning that if {@code v1 == v2},
     * the distance is {@code 0} while if {@code v1} is orthogonal to {@code v2} it is {@code 1}.
     * @see MetricDefinition.CosineMetric
     */
    class CosineMetric implements MetricDefinition {
        @Override
        public boolean satisfiesTriangleInequality() {
            return false;
        }

        @Override
        public double distance(@Nonnull final double[] vector1, @Nonnull final double[] vector2) {
            MetricDefinition.validate(vector1, vector2);

            double normA = 0.0;
            double normB = 0.0;

            for (int i = 0; i < vector1.length; i++) {
                normA += vector1[i] * vector1[i];
                normB += vector2[i] * vector2[i];
            }

            // Handle the case of zero-vectors to avoid division by zero
            if (normA == 0.0 || normB == 0.0) {
                return Double.POSITIVE_INFINITY;
            }

            final double dotProduct = DotProductMetric.dotProduct(vector1, vector2);

            if (!Double.isFinite(normA) || !Double.isFinite(normB) || !Double.isFinite(dotProduct)) {
                return Double.NaN;
            }

            return 1.0d - DotProductMetric.dotProduct(vector1, vector2) / (Math.sqrt(normA) * Math.sqrt(normB));
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    /**
     * Dot product similarity.
     * <p>
     * This metric calculates the inverted dot product of two vectors. It is not a true metric as the dot product can
     * be positive at which point the distance is negative. In order to make callers aware of this fact, this distance
     * only allows {@link MetricDefinition#distance(double[], double[])} to be called.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Dot_product">Dot Product</a>
     * @see DotProductMetric
     */
    class DotProductMetric implements MetricDefinition {
        @Override
        public boolean satisfiesZeroSelfDistance() {
            return false;
        }

        @Override
        public boolean satisfiesPositivity() {
            return false;
        }

        @Override
        public boolean satisfiesTriangleInequality() {
            return false;
        }

        @Override
        public double distance(@Nonnull final double[] vector1, @Nonnull final double[] vector2) {
            return -dotProduct(vector1, vector2);
        }

        public static double dotProduct(@Nonnull final double[] vector1, @Nonnull final double[] vector2) {
            MetricDefinition.validate(vector1, vector2);

            double product = 0.0d;
            for (int i = 0; i < vector1.length; i++) {
                product += vector1[i] * vector2[i];
            }
            return product;
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }
}
