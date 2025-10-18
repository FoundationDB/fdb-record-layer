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

package com.apple.foundationdb.async.rabitq;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;

import javax.annotation.Nonnull;

public class RaBitEstimator implements Estimator {
    @Nonnull
    private final Metric metric;
    @Nonnull
    private final RealVector centroid;
    private final int numExBits;

    public RaBitEstimator(@Nonnull final Metric metric,
                          @Nonnull final RealVector centroid,
                          final int numExBits) {
        this.metric = metric;
        this.centroid = centroid;
        this.numExBits = numExBits;
    }

    @Nonnull
    public Metric getMetric() {
        return metric;
    }

    public int getNumDimensions() {
        return centroid.getNumDimensions();
    }

    public int getNumExBits() {
        return numExBits;
    }

    @Override
    public double distance(@Nonnull final RealVector query,
                           @Nonnull final RealVector storedVector) {
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

    private double distance(@Nonnull final RealVector query, // pre-rotated query q
                            @Nonnull final EncodedRealVector encodedVector) {
        return estimateDistanceAndErrorBound(query, encodedVector).getDistance();
    }

    @Nonnull
    public Result estimateDistanceAndErrorBound(@Nonnull final RealVector query, // pre-rotated query q
                                                @Nonnull final EncodedRealVector encodedVector) {
        final double cb = (1 << numExBits) - 0.5;
        final RealVector qc = query;
        final double gAdd = qc.dot(qc);
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
            return "Estimate[" + "distance=" + distance + ", err=" + err + "]";
        }
    }
}

