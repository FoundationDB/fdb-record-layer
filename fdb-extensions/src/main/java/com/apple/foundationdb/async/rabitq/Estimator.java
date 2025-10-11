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

package com.apple.foundationdb.async.rabitq;

import com.apple.foundationdb.async.hnsw.DoubleVector;
import com.apple.foundationdb.async.hnsw.Vector;

import javax.annotation.Nonnull;

public class Estimator {
    @Nonnull
    private final Vector centroid;
    private final int numExBits;

    public Estimator(@Nonnull final Vector centroid,
                     final int numExBits) {
        this.centroid = centroid;
        this.numExBits = numExBits;
    }

    public int getNumDimensions() {
        return centroid.getNumDimensions();
    }

    /** Estimate metric(queryRot, encodedVector) using ex-bits-only factors. */
    public double estimateDistance(@Nonnull final Vector query, // pre-rotated query q
                                   @Nonnull final EncodedVector encodedVector) {
        final double cb = (1 << numExBits) - 0.5;
        final Vector qc = query.subtract(centroid);
        final double gAdd = qc.dot(qc);
        final Vector totalCode = new DoubleVector(encodedVector.getEncodedData());
        final Vector xuc = totalCode.subtract(cb);
        final double dot = query.dot(xuc);

        // Same formula for both metrics; just ensure fAddEx/fRescaleEx were computed for that metric.
        return encodedVector.getAddEx() + gAdd + encodedVector.getRescaleEx() * dot;
    }

    public Result estimateDistanceAndErrorBound(@Nonnull final Vector query, // pre-rotated query q
                                                @Nonnull final EncodedVector encodedVector) {
        final double cb = (1 << numExBits) - 0.5;
        final Vector qc = query.subtract(centroid);
        final double gAdd = qc.dot(qc);
        final double gError = Math.sqrt(gAdd);
        final Vector totalCode = new DoubleVector(encodedVector.getEncodedData());
        final Vector xuc = totalCode.subtract(cb);
        final double dot = query.dot(xuc);

        // Same formula for both metrics; just ensure fAddEx/fRescaleEx were computed for that metric.
        return new Result(encodedVector.getAddEx() + gAdd + encodedVector.getRescaleEx() * dot,
                encodedVector.getErrorEx() * gError);
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

