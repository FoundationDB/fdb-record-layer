/*
 * Metrics.java
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

import javax.annotation.Nonnull;

/**
 * Represents various distance calculation strategies (metrics) for vectors.
 * <p>
 * Each enum constant holds a specific metric implementation, providing a type-safe way to calculate the distance
 * between two points in a multidimensional space.
 *
 * @see Metric
 */
public enum Metrics {
    /**
     * Represents the Manhattan distance metric, implemented by {@link Metric.ManhattanMetric}.
     * <p>
     * This metric calculates a distance overlaying the multidimensional space with a grid-like structure only allowing
     * orthogonal lines. In 2D this resembles the street structure in Manhattan where one would have to go {@code x}
     * blocks north/south and {@code y} blocks east/west leading to a total distance of {@code x + y}.
     * @see Metric.ManhattanMetric
     */
    MANHATTAN_METRIC(new Metric.ManhattanMetric()),

    /**
     * Represents the Euclidean distance metric, implemented by {@link Metric.EuclideanMetric}.
     * <p>
     * This metric calculates the "ordinary" straight-line distance between two points
     * in Euclidean space. The distance is the square root of the sum of the
     * squared differences between the corresponding coordinates of the two points.
     * @see Metric.EuclideanMetric
     */
    EUCLIDEAN_METRIC(new Metric.EuclideanMetric()),

    /**
     * Represents the squared Euclidean distance metric, implemented by {@link Metric.EuclideanSquareMetric}.
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
     * @see Metric.EuclideanSquareMetric
     */
    EUCLIDEAN_SQUARE_METRIC(new Metric.EuclideanSquareMetric()),

    /**
     * Represents the Cosine distance metric, implemented by {@link Metric.CosineMetric}.
     * <p>
     * This metric calculates a "distance" between two vectors {@code v1} and {@code v2} that ranges between
     * {@code 0.0d} and {@code 2.0d} that corresponds to {@code 1 - cos(v1, v2)}, meaning that if {@code v1 == v2},
     * the distance is {@code 0} while if {@code v1} is orthogonal to {@code v2} it is {@code 1}.
     * @see Metric.CosineMetric
     */
    COSINE_METRIC(new Metric.CosineMetric()),

    /**
     * Dot product similarity, implemented by {@link Metric.DotProductMetric}
     * <p>
     * This metric calculates the inverted dot product of two vectors. It is not a true metric as the dot product can
     * be positive at which point the distance is negative. In order to make callers aware of this fact, this distance
     * only allows {@link Metric#comparativeDistance(double[], double[])} to be called.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Dot_product">Dot Product</a>
     * @see Metric.DotProductMetric
     */
    DOT_PRODUCT_METRIC(new Metric.DotProductMetric());

    @Nonnull
    private final Metric metric;

    /**
     * Constructs a new Metrics instance with the specified metric.
     * @param metric the metric to be associated with this Metrics instance; must not be null.
     */
    Metrics(@Nonnull final Metric metric) {
        this.metric = metric;
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
    public double distance(@Nonnull Vector vector1, @Nonnull Vector vector2) {
        return metric.distance(vector1, vector2);
    }

    /**
     * Calculates a comparative distance between two vectors. The comparative distance is used in contexts such as
     * ranking where the caller needs to "compare" two distances. In contrast to a true metric, the distances computed
     * by this method do not need to follow proper metric invariants: The distance can be negative; the distance
     * does not need to follow triangle inequality.
     * <p>
     * This method is an alias for {@link #distance(Vector, Vector)} under normal circumstances. It is not for e.g.
     * {@link Metric.DotProductMetric} where the distance is the negative dot product.
     *
     * @param vector1 the first vector, represented as an array of {@code double}.
     * @param vector2 the second vector, represented as an array of {@code double}.
     *
     * @return the distance between the two vectors.
     */
    public double comparativeDistance(@Nonnull Vector vector1, @Nonnull Vector vector2) {
        return metric.comparativeDistance(vector1, vector2);
    }
}
