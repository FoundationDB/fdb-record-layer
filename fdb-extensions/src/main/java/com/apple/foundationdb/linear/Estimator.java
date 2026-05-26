/*
 * Estimator.java
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
 * Computes a distance between two vectors. Implementations are typically tied to a specific
 * {@link Metric} but may add behavior on top of the raw metric — for example, applying a
 * quantization-aware fast path when one or both vectors are encoded.
 * <p>
 * Search and clustering algorithms drive vector comparisons through this interface so they can
 * stay agnostic of how distances are actually computed (raw metric, rabitq estimator, etc.).
 */
public interface Estimator {
    /**
     * Returns the underlying {@link Metric} this estimator computes.
     */
    @Nonnull
    Metric getMetric();

    /**
     * Convenience overload of {@link #isOptimized(RealVector, RealVector)} that unwraps the
     * underlying vectors from {@link Transformed} containers and forwards to the
     * {@code RealVector} variant.
     */
    default boolean isOptimized(@Nonnull final Transformed<? extends RealVector> vector1,
                                @Nonnull final Transformed<? extends RealVector> vector2) {
        return isOptimized(vector1.getUnderlyingVector(), vector2.getUnderlyingVector());
    }

    /**
     * Returns whether this estimator can compute the distance between {@code vector1} and
     * {@code vector2} on a metric-specific fast path that bypasses the raw metric. For example,
     * a quantization-aware estimator may report {@code true} when at least one of the vectors
     * is in the encoded form it knows how to consume directly.
     * <p>
     * Default {@link #ofMetric ofMetric}-built estimators have no fast path and always return
     * {@code false}. Callers can use this hint to skip work that is only worthwhile for the
     * generic path (e.g. avoiding decode of an encoded vector if both will go through the
     * estimator's fast path anyway).
     *
     * @param vector1 the first vector
     * @param vector2 the second vector
     * @return {@code true} iff this estimator has a metric-specific fast path for the given
     *         pair
     */
    boolean isOptimized(@Nonnull RealVector vector1,
                        @Nonnull RealVector vector2);

    /**
     * Convenience overload of {@link #distance(RealVector, RealVector)} that unwraps the
     * underlying vectors from {@link Transformed} containers and forwards to the
     * {@code RealVector} variant.
     */
    default double distance(@Nonnull final Transformed<? extends RealVector> vector1,
                            @Nonnull final Transformed<? extends RealVector> vector2) {
        return distance(vector1.getUnderlyingVector(), vector2.getUnderlyingVector());
    }

    /**
     * Calculates the distance between two vectors that have already been transformed into the
     * estimator's coordinate system.
     * <p>
     * Both arguments must already have been rotated/translated as expected by the estimator
     * before calling this method — implementations do <em>not</em> apply any preprocessing.
     * Implementations may reject distance values that are not finite (depending on the metric
     * and on whether the inputs are zero vectors, etc.) by throwing an
     * {@link IllegalArgumentException}.
     *
     * @param vector1 the first pre-rotated and translated vector
     * @param vector2 the second pre-rotated and translated vector
     * @return a non-negative {@code double} representing the distance between the two vectors
     */
    double distance(@Nonnull RealVector vector1,
                    @Nonnull RealVector vector2);

    /**
     * Returns a plain {@link Estimator} that delegates straight to {@code metric.distance(...)}.
     * The result has no fast path ({@link #isOptimized} is always {@code false}) and rejects
     * non-finite distances with an {@link IllegalArgumentException}.
     *
     * @param metric the metric to wrap
     * @return a non-null estimator that computes {@code metric}'s distance directly
     */
    @Nonnull
    static Estimator ofMetric(@Nonnull final Metric metric) {
        return new Estimator() {
            @Nonnull
            @Override
            public Metric getMetric() {
                return metric;
            }

            @Override
            public boolean isOptimized(@Nonnull final RealVector vector1, @Nonnull final RealVector vector2) {
                return false;
            }

            @Override
            public double distance(@Nonnull final RealVector vector1, @Nonnull final RealVector vector2) {
                final double distance = metric.distance(vector1, vector2);
                if (!Double.isFinite(distance)) {
                    throw new IllegalArgumentException("vector has an L2 norm of infinite, not a number, or 0");
                }
                return distance;
            }
        };
    }
}
