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
    default double distance(@Nonnull final Transformed<? extends RealVector> query,
                            @Nonnull final Transformed<? extends RealVector> storedVector) {
        return distance(query.getUnderlyingVector(), storedVector.getUnderlyingVector());
    }

    /**
     * Calculates the distance between a pre-rotated and translated query vector and a stored vector.
     * <p>
     * This method is designed to compute the distance metric between two vectors in a high-dimensional space. It is
     * crucial that the {@code query} vector has already been appropriately transformed (e.g., rotated and translated)
     * to align with the coordinate system of the {@code storedVector} before calling this method.
     *
     * @param query the pre-rotated and translated query vector, cannot be null.
     * @param storedVector the stored vector to which the distance is calculated, cannot be null.
     * @return a non-negative {@code double} representing the distance between the two vectors.
     */
    double distance(@Nonnull RealVector query,
                    @Nonnull RealVector storedVector);
}
