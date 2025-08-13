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

package com.apple.foundationdb.async.hnsw;

import javax.annotation.Nonnull;

public interface Metric {
    double distance(Double[] vector1, Double[] vector2);

    default double comparativeDistance(Double[] vector1, Double[] vector2) {
        return distance(vector1, vector2);
    }

    /**
     * A helper method to validate that vectors can be compared.
     * @param vector1 The first vector.
     * @param vector2 The second vector.
     */
    private static void validate(Double[] vector1, Double[] vector2) {
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

    class ManhattanMetric implements Metric {
        @Override
        public double distance(final Double[] vector1, final Double[] vector2) {
            Metric.validate(vector1, vector2);

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

    class EuclideanMetric implements Metric {
        @Override
        public double distance(final Double[] vector1, final Double[] vector2) {
            Metric.validate(vector1, vector2);

            return Math.sqrt(EuclideanSquareMetric.distanceInternal(vector1, vector2));
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    class EuclideanSquareMetric implements Metric {
        @Override
        public double distance(final Double[] vector1, final Double[] vector2) {
            Metric.validate(vector1, vector2);
            return distanceInternal(vector1, vector2);
        }

        private static double distanceInternal(final Double[] vector1, final Double[] vector2) {
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

    class CosineMetric implements Metric {
        @Override
        public double distance(final Double[] vector1, final Double[] vector2) {
            Metric.validate(vector1, vector2);

            double dotProduct = 0.0;
            double normA = 0.0;
            double normB = 0.0;

            for (int i = 0; i < vector1.length; i++) {
                dotProduct += vector1[i] * vector2[i];
                normA += vector1[i] * vector1[i];
                normB += vector2[i] * vector2[i];
            }

            // Handle the case of zero-vectors to avoid division by zero
            if (normA == 0.0 || normB == 0.0) {
                return Double.POSITIVE_INFINITY;
            }

            return 1.0d - dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    class DotProductMetric implements Metric {
        @Override
        public double distance(final Double[] vector1, final Double[] vector2) {
            throw new UnsupportedOperationException("dot product metric is not a true metric and can only be used for ranking");
        }

        @Override
        public double comparativeDistance(final Double[] vector1, final Double[] vector2) {
            Metric.validate(vector1, vector2);

            double product = 0.0d;
            for (int i = 0; i < vector1.length; i++) {
                product += vector1[i] * vector2[i];
            }
            return -product;
        }

        @Override
        @Nonnull
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }
}
