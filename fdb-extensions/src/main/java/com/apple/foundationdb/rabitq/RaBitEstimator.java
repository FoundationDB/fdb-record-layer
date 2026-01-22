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
        final double distance;
        if (!(query instanceof EncodedRealVector) && storedVector instanceof EncodedRealVector) {
            // only use the estimator if the first (by convention) vector is not encoded, but the second is
            distance = distance(query, (EncodedRealVector)storedVector);
        } else if (query instanceof EncodedRealVector && !(storedVector instanceof EncodedRealVector)) {
            distance = distance(storedVector, (EncodedRealVector)query);
        } else {
            // use the regular metric for all other cases
            distance = metric.distance(query, storedVector);
        }
        if (!Double.isFinite(distance)) {
            throw new IllegalArgumentException("distance is infinite or not a number");
        }
        return distance;
    }

    private double distance(@Nonnull final RealVector query, @Nonnull final EncodedRealVector encodedVector) {
        return estimateDistanceAndErrorBound(query, encodedVector).getDistance();
    }

    @Nonnull
    public Result estimateDistanceAndErrorBound(@Nonnull final RealVector query,
                                                @Nonnull final EncodedRealVector encodedVector) {
        if (metric == Metric.COSINE_METRIC) {
            //
            // In cosine metric there is a special case that conventionally if one vector is the zero vector, the
            // distance is NaN as that distance corresponds to all vectors that are orthogonal to each other.
            //
            final double qNormSqr = query.dot(query);
            if (!(qNormSqr > 0.0) || !Double.isFinite(qNormSqr)) {
                return new Result(Double.NaN, 0.0);
            }
        }

        final double cb = (1 << numExBits) - 0.5;
        final double gAdd = query.dot(query);
        final double gError = Math.sqrt(gAdd);

        final RealVector totalCode = new DoubleRealVector(encodedVector.getEncodedData());
        final RealVector xuc = totalCode.subtract(cb);
        final double dot = query.dot(xuc);

        final double euclideanSquare = encodedVector.getAddEx() + gAdd + encodedVector.getRescaleEx() * dot;
        final double euclideanSquareError = encodedVector.getErrorEx() * gError;

        switch (metric) {
            case COSINE_METRIC:
                return new Result(0.5 * euclideanSquare, 0.5 * euclideanSquareError);
            case DOT_PRODUCT_METRIC:
            case EUCLIDEAN_SQUARE_METRIC:
                return new Result(euclideanSquare, euclideanSquareError);
            case EUCLIDEAN_METRIC:
                return new Result(Math.sqrt(euclideanSquare), Math.sqrt(euclideanSquareError));
            default:
                throw new UnsupportedOperationException("metric not supported by quantizer");
        }
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

