/*
 * Quantizer.java
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
 * Defines the contract for a quantizer, a component responsible for encoding data vectors into a different, ideally
 * a more compact, representation.
 * <p>
 * Quantizers are typically used in machine learning and information retrieval to transform raw data into a format that
 * is more suitable for processing, such as a compressed representation.
 */
public interface Quantizer {
    /**
     * Returns the {@code Estimator} instance associated with this object.
     * <p>
     * The estimator is responsible for performing the primary distance estimation or calculation logic. This method
     * provides access to that underlying component.
     *
     * @return the {@link Estimator} instance, which is guaranteed to be non-null.
     */
    @Nonnull
    Estimator estimator();

    @Nonnull
    default Transformed<RealVector> encode(@Nonnull final Transformed<RealVector> vector) {
        return new Transformed<>(encode(vector.getUnderlyingVector()));
    }

    /**
     * Encodes the given data vector into another vector representation.
     * <p>
     * This method transforms the raw input data into a different, quantized format, which is often a vector more
     * suitable for processing/storing the data. The specifics of the encoding depend on the implementation of the class.
     *
     * @param vector the input {@link RealVector} to be encoded. Must not be {@code null} and is assumed to have been
     *        preprocessed, such as by rotation and/or translation. The preprocessing has to align with the requirements
     *        of the specific quantizer.
     * @return the encoded vector representation of the input data, guaranteed to be non-null.
     */
    @Nonnull
    RealVector encode(@Nonnull RealVector vector);

    /**
     * Creates a no-op {@code Quantizer} that does not perform any data transformation.
     * <p>
     * The returned quantizer's {@link Quantizer#encode(RealVector)} method acts as an
     * identity function, returning the input vector without modification. The
     * {@link Quantizer#estimator()} is created directly from the distance function
     * of the provided {@link Metric}. This can be useful for baseline comparisons
     * or for algorithms that require a {@code Quantizer} but where no quantization
     * is desired.
     *
     * @param metric the {@link Metric} used to build the distance estimator for the quantizer.
     * @return a new {@link Quantizer} instance that performs no operation.
     */
    @Nonnull
    static Quantizer noOpQuantizer(@Nonnull final Metric metric) {
        return new Quantizer() {
            @Nonnull
            @Override
            public Estimator estimator() {
                return (vector1, vector2) -> {
                    final double distance = metric.distance(vector1, vector2);
                    if (!Double.isFinite(distance)) {
                        throw new IllegalArgumentException("vector has an L2 norm of infinite, not a number, or 0");
                    }
                    return distance;
                };
            }

            @Nonnull
            @Override
            public RealVector encode(@Nonnull final RealVector vector) {
                return vector;
            }
        };
    }
}
