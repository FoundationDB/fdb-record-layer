/*
 * ClusterMetadata.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.guardiann;

import javax.annotation.Nonnull;
import java.util.Objects;

class RunningStandardDeviation {
    private static final RunningStandardDeviation IDENTITY = new RunningStandardDeviation(0L, 0.0d,
            0.0d);
    private final long numElements;

    private final double runningMean;
    private final double runningSumSquaredDeviations;

    public RunningStandardDeviation(final long numElements, final double runningMean,
                                    final double runningSumSquaredDeviations) {
        this.numElements = numElements;
        this.runningMean = runningMean;
        this.runningSumSquaredDeviations = runningSumSquaredDeviations;
    }

    public long getNumElements() {
        return numElements;
    }

    public double getRunningMean() {
        return runningMean;
    }

    public double getRunningSumSquaredDeviations() {
        return runningSumSquaredDeviations;
    }

    @Nonnull
    public RunningStandardDeviation add(final double newValue) {
        long newN = getNumElements() + 1;
        double delta = newValue - getRunningMean();
        double newMean = getRunningMean() + delta / newN;
        double delta2 = newValue - newMean;
        double newM2 = getRunningSumSquaredDeviations() + delta * delta2;

        return new RunningStandardDeviation(newN, newMean, newM2);
    }

    @Nonnull
    public RunningStandardDeviation remove(double x) {
        if (getNumElements() == 0) {
            throw new IllegalStateException("Cannot remove from an empty set");
        }

        if (getNumElements() == 1) {
            // removing the last value resets the state.
            return identity();
        }

        final long newN = getNumElements() - 1;
        final double newMean = (getNumElements() * getRunningMean() - x) / newN;
        final double delta = x - getRunningMean();
        final double delta2 = x - newMean;
        double newM2 = getRunningSumSquaredDeviations() - delta * delta2;

        // Guard against tiny negative values from floating-point roundoff.
        if (newM2 < 0.0 && newM2 > -1e-12) {
            newM2 = 0.0;
        }

        return new RunningStandardDeviation(newN, newMean, newM2);
    }

    @Nonnull
    public RunningStandardDeviation combine(@Nonnull final RunningStandardDeviation other) {
        if (other.getNumElements() == 0) {
            return this;
        }

        if (this.getNumElements() == 0) {
            return other;
        }

        final long combinedN = this.getNumElements() + other.getNumElements();
        final double delta = other.getRunningMean() - this.getRunningMean();
        final double combinedMean = this.getRunningMean() + delta * other.getNumElements() / combinedN;
        final double combinedM2 = this.getRunningSumSquaredDeviations() +
                other.getRunningSumSquaredDeviations() +
                delta * delta * this.getNumElements() * other.getNumElements() / combinedN;

        return new RunningStandardDeviation(combinedN, combinedMean, combinedM2);
    }

    @Nonnull
    public RunningStandardDeviation subtract(@Nonnull final RunningStandardDeviation other) {
        if (other.getNumElements() == 0) {
            return this;
        }
        if (other.getNumElements() > this.getNumElements()) {
            throw new IllegalArgumentException("cannot subtract a larger accumulator from a smaller one");
        }

        if (other.getNumElements() == this.getNumElements()) {
            // Only valid if they represent the same multiset; we trust the caller.
            return identity();
        }

        final long newN = this.getNumElements() - other.getNumElements();
        final double newMean = (this.getNumElements() * this.getRunningMean() - other.getNumElements() * other.getRunningMean()) / newN;
        final double delta = other.getRunningMean() - this.getRunningMean();
        double newM2 = this.getRunningSumSquaredDeviations() - other.getRunningSumSquaredDeviations() -
                delta * delta * this.getNumElements() * other.getNumElements() / newN;

        // Clamp tiny negative values caused by floating-point roundoff.
        if (newM2 < 0.0 && newM2 > -1e-12) {
            newM2 = 0.0;
        }
        if (newM2 < 0.0) {
            throw new IllegalStateException(
                    "Subtraction produced negative M2. " +
                            "This usually means the subtracted accumulator was not actually a subset.");
        }

        return new RunningStandardDeviation(newN, newMean, newM2);
    }

    public double mean() {
        return getNumElements() == 0 ? Double.NaN : runningMean;
    }

    public double populationVariance() {
        return getNumElements() == 0 ? Double.NaN : (getRunningSumSquaredDeviations() / getNumElements());
    }

    public double sampleVariance() {
        if (getNumElements() < 2) {
            throw new IllegalStateException("sample variance requires at least 2 values");
        }
        return getRunningSumSquaredDeviations() / (getNumElements() - 1);
    }

    public double populationStandardDeviation() {
        return Math.sqrt(populationVariance());
    }

    public double sampleStandardDeviation() {
        return Math.sqrt(sampleVariance());
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RunningStandardDeviation that = (RunningStandardDeviation)o;
        return getNumElements() == that.getNumElements() &&
                Double.compare(getRunningMean(), that.getRunningMean()) == 0 &&
                Double.compare(getRunningSumSquaredDeviations(), that.getRunningSumSquaredDeviations()) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNumElements(), getRunningMean(), getRunningSumSquaredDeviations());
    }

    @Override
    public String toString() {
        return "RunningStandardDeviation[" + getNumElements() + ", " + getRunningMean() + ", " +
                getRunningSumSquaredDeviations() + ']';
    }

    @Nonnull
    public static RunningStandardDeviation identity() {
        return IDENTITY;
    }

    @Nonnull
    public static RunningStandardDeviation of(final double newValue) {
        return identity().add(newValue);
    }
}
