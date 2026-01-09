/*
 * RaBitEstimator.java
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

package com.apple.foundationdb.rabitq;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;

import javax.annotation.Nonnull;

public class RaBitEstimator implements Estimator {
    @Nonnull
    private final Metric metric;
    private final int numExBits;

    public RaBitEstimator(@Nonnull final Metric metric,
                          final int numExBits) {
        this.metric = metric;
        this.numExBits = numExBits;
    }

    @Nonnull
    public Metric getMetric() {
        return metric;
    }

    public int getNumExBits() {
        return numExBits;
    }

    @Override
    public double distance(@Nonnull final RealVector query, @Nonnull final RealVector storedVector) {
        if (!(query instanceof EncodedRealVector) && storedVector instanceof EncodedRealVector) {
            // only use the estimator if the first (by convention) vector is not encoded, but the second is
            return distance(query, (EncodedRealVector)storedVector);
        }
        if (query instanceof EncodedRealVector && !(storedVector instanceof EncodedRealVector)) {
            return distance(storedVector, (EncodedRealVector)query);
        }
        // use the regular metric for all other cases
        return metric.distance(query, storedVector);
    }

    private double distance(@Nonnull final RealVector query, @Nonnull final EncodedRealVector encodedVector) {
        return estimateDistanceAndErrorBound(query, encodedVector).getDistance();
    }

    @Nonnull
    public Result estimateDistanceAndErrorBound(@Nonnull final RealVector query,
                                                @Nonnull final EncodedRealVector encodedVector) {
        if (metric == Metric.COSINE_METRIC) {
            return estimateCosineDistanceAndErrorBound(query, encodedVector);
        }

        final double cb = (1 << numExBits) - 0.5;
        final double gAdd = query.dot(query);
        final double gError = Math.sqrt(gAdd);

        final RealVector totalCode = new DoubleRealVector(encodedVector.getEncodedData());
        final RealVector xuc = totalCode.subtract(cb);
        final double dot = query.dot(xuc);

        switch (metric) {
            case DOT_PRODUCT_METRIC:
            case EUCLIDEAN_SQUARE_METRIC:
                return new Result(encodedVector.getAddEx() + gAdd + encodedVector.getRescaleEx() * dot,
                        encodedVector.getErrorEx() * gError);
            case EUCLIDEAN_METRIC:
                return new Result(Math.sqrt(encodedVector.getAddEx() + gAdd + encodedVector.getRescaleEx() * dot),
                        Math.sqrt(encodedVector.getErrorEx() * gError));
            default:
                throw new UnsupportedOperationException("metric not supported by quantizer");
        }
    }

    @Nonnull
    private Result estimateCosineDistanceAndErrorBound(@Nonnull final RealVector query,
                                                       @Nonnull final EncodedRealVector encodedVector) {
        // If query is zero, cosine is undefined; treat as maximal distance (1.0) in [0,1].
        final double qNormSqr = query.dot(query);
        if (!(qNormSqr > 0.0) || !Double.isFinite(qNormSqr)) {
            return new Result(1.0, 0.0);
        }

        // Normalize query
        final double qInv = 1.0 / Math.sqrt(qNormSqr);
        final double[] qHat = new double[query.getNumDimensions()];
        for (int i = 0; i < qHat.length; i++) {
            qHat[i] = query.getComponent(i) * qInv;
        }
        final RealVector qVec = new DoubleRealVector(qHat);

        // Now compute the RaBit estimate as if EUCLIDEAN_SQUARE_METRIC on normalized vectors
        final double cb = (1 << numExBits) - 0.5;

        // gAdd = ||q̂||^2 = 1, gError = ||q̂|| = 1
        final double gAdd = 1.0;
        final double gError = 1.0;

        final RealVector totalCode = new DoubleRealVector(encodedVector.getEncodedData());
        final RealVector xuc = totalCode.subtract(cb);

        final double dot = qVec.dot(xuc);

        final double euclSq = encodedVector.getAddEx() + gAdd + encodedVector.getRescaleEx() * dot;
        final double euclSqErr = encodedVector.getErrorEx() * gError;

        // cosine distance = 0.5 * ||q̂ - x̂||^2
        return new Result(0.5 * euclSq, 0.5 * euclSqErr);
    }

    public static class Result {
        private final double distance;
        private final double err;

        public Result(final double distance, final double err) {
            this.distance = distance;
            this.err = err;
        }

        public double getDistance() {
            return distance;
        }

        public double getErr() {
            return err;
        }

        @Override
        public String toString() {
            return "estimate[" + "distance=" + distance + ", err=" + err + "]";
        }
    }
}

