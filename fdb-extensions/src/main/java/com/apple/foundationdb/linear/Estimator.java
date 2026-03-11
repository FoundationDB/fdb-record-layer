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
 * Interface of an estimator used for calculating the distance between vectors.
 * <p>
 * Implementations of this interface are expected to provide a specific distance
 * metric calculation, often used in search or similarity contexts where one
 * vector (the query) is compared against many stored vectors.
 */
public interface Estimator {
    @Nonnull
    Metric getMetric();

    default boolean isOptimized(@Nonnull final Transformed<? extends RealVector> vector1,
                                @Nonnull final Transformed<? extends RealVector> vector2) {
        return isOptimized(vector1.getUnderlyingVector(), vector2.getUnderlyingVector());
    }

    default double distance(@Nonnull final Transformed<? extends RealVector> vector1,
                            @Nonnull final Transformed<? extends RealVector> vector2) {
        return distance(vector1.getUnderlyingVector(), vector2.getUnderlyingVector());
    }

    /**
     * Calculates the distance between a pre-rotated and translated query vector and a stored vector.
     * <p>
     * This method is designed to compute the distance metric between two vectors in a high-dimensional space. It is
     * crucial that both vectors have already been appropriately transformed (e.g., rotated and translated)
     * to align with the appropriate coordinate system of the {@code storedVector} before calling this method.
     * Note, that the particular metric in use may reject any distance that is not finite.
     *
     * @param vector1 the pre-rotated and translated vector, cannot be null.
     * @param vector2 the pre-rotated and translated vector to which the distance is calculated, cannot be null.
     * @return a non-negative {@code double} representing the distance between the two vectors.
     */
    double distance(@Nonnull RealVector vector1,
                    @Nonnull RealVector vector2);

    boolean isOptimized(@Nonnull final RealVector vector1,
                        @Nonnull final RealVector vector2);

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
