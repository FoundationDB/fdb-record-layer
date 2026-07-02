/*
 * RunningStats.java
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

/**
 * Incrementally maintained statistics for a set of distance values, using Welford's online algorithm
 * for numerically stable computation of mean and variance. Used by cluster metadata to track the
 * distribution of vector-to-centroid distances without storing the individual values.
 *
 * <p>
 * Supports incremental {@link #add} and {@link #remove} of individual values, as well as parallel
 * {@link #combine} and {@link #subtract} of two accumulators. Also tracks the maximum value ever
 * observed ({@link #maxEver()}), which is maintained exactly on {@code add}/{@code combine} but
 * intentionally left as a stale upper bound after {@code remove}/{@code subtract}.
 * </p>
 *
 * @param numElements the number of values currently represented
 * @param runningMean the running mean (Welford's online mean)
 * @param runningSumSquaredDeviations the running sum of squared deviations from the mean (Welford's M2)
 * @param runningMaxEver the maximum value ever added (may be stale after removals;
 *        {@link Double#NEGATIVE_INFINITY} for empty accumulators)
 */
record RunningStats(long numElements, double runningMean, double runningSumSquaredDeviations,
                                double runningMaxEver) {
    private static final RunningStats IDENTITY = new RunningStats(0L, 0.0d,
            0.0d, Double.NEGATIVE_INFINITY);

    /**
     * Adds a new value to the accumulator, updating mean, M2, and max.
     *
     * @param newValue the value to add
     * @return a new accumulator reflecting the added value
     */
    @Nonnull
    public RunningStats add(final double newValue) {
        long newN = numElements() + 1;
        double delta = newValue - runningMean();
        double newMean = runningMean() + delta / newN;
        double delta2 = newValue - newMean;
        double newM2 = runningSumSquaredDeviations() + delta * delta2;

        return new RunningStats(newN, newMean, newM2, Math.max(runningMaxEver(), newValue));
    }

    /**
     * Removes a value from the accumulator using the inverse of Welford's update. The
     * {@code runningMaxEver} is intentionally not updated and remains a stale upper bound.
     *
     * @param x the value to remove (must have been previously added)
     * @return a new accumulator reflecting the removal
     * @throws IllegalStateException if the accumulator is empty
     */
    @Nonnull
    public RunningStats remove(double x) {
        if (numElements() == 0) {
            throw new IllegalStateException("Cannot remove from an empty set");
        }

        if (numElements() == 1) {
            // removing the last value resets the state.
            return identity();
        }

        final long newN = numElements() - 1;
        final double newMean = (numElements() * runningMean() - x) / newN;
        final double delta = x - runningMean();
        final double delta2 = x - newMean;
        double newM2 = runningSumSquaredDeviations() - delta * delta2;

        // Guard against tiny negative values from floating-point roundoff.
        if (newM2 < 0.0 && newM2 > -1e-12) {
            newM2 = 0.0;
        }

        // runningMaxEver is intentionally not updated — it remains an upper bound after removal
        return new RunningStats(newN, newMean, newM2, runningMaxEver());
    }

    /**
     * Combines two accumulators using the parallel combination formula. The resulting accumulator
     * represents the union of both value sets. The max is the maximum of both sides.
     *
     * @param other the accumulator to combine with
     * @return a new accumulator representing the combined statistics
     */
    @Nonnull
    public RunningStats combine(@Nonnull final RunningStats other) {
        if (other.numElements() == 0) {
            return this;
        }

        if (this.numElements() == 0) {
            return other;
        }

        final long combinedN = this.numElements() + other.numElements();
        final double delta = other.runningMean() - this.runningMean();
        final double combinedMean = this.runningMean() + delta * other.numElements() / combinedN;
        final double combinedM2 = this.runningSumSquaredDeviations() +
                other.runningSumSquaredDeviations() +
                delta * delta * this.numElements() * other.numElements() / combinedN;

        return new RunningStats(combinedN, combinedMean, combinedM2,
                Math.max(this.runningMaxEver(), other.runningMaxEver()));
    }

    /**
     * Subtracts another accumulator from this one, producing the statistics of the remaining values.
     * The {@code runningMaxEver} is intentionally not updated and remains a stale upper bound.
     *
     * @param other the accumulator to subtract (must represent a subset of this accumulator's values)
     * @return a new accumulator representing the remaining values
     * @throws IllegalArgumentException if the other accumulator has more elements
     * @throws IllegalStateException if the subtraction produces an invalid M2 (negative beyond roundoff)
     */
    @Nonnull
    public RunningStats subtract(@Nonnull final RunningStats other) {
        if (other.numElements() == 0) {
            return this;
        }
        if (other.numElements() > this.numElements()) {
            throw new IllegalArgumentException("cannot subtract a larger accumulator from a smaller one");
        }

        if (other.numElements() == this.numElements()) {
            // Only valid if they represent the same multiset; we trust the caller.
            return identity();
        }

        final long newN = this.numElements() - other.numElements();
        final double newMean = (this.numElements() * this.runningMean() - other.numElements() * other.runningMean()) / newN;
        final double delta = other.runningMean() - this.runningMean();
        double newM2 = this.runningSumSquaredDeviations() - other.runningSumSquaredDeviations() -
                delta * delta * this.numElements() * other.numElements() / newN;

        // Clamp tiny negative values caused by floating-point roundoff.
        if (newM2 < 0.0 && newM2 > -1e-12) {
            newM2 = 0.0;
        }
        if (newM2 < 0.0) {
            throw new IllegalStateException(
                    "Subtraction produced negative M2. " +
                            "This usually means the subtracted accumulator was not actually a subset.");
        }

        // runningMaxEver is intentionally not updated — it remains an upper bound after subtraction
        return new RunningStats(newN, newMean, newM2, runningMaxEver());
    }

    /** Returns the mean, or {@link Double#NaN} if the accumulator is empty. */
    public double mean() {
        return numElements() == 0 ? Double.NaN : runningMean;
    }

    /** Returns the population variance, or {@link Double#NaN} if the accumulator is empty. */
    public double populationVariance() {
        return numElements() == 0 ? Double.NaN : (runningSumSquaredDeviations() / numElements());
    }

    /**
     * Returns the sample variance (Bessel's correction).
     *
     * @throws IllegalStateException if fewer than 2 values have been added
     */
    public double sampleVariance() {
        if (numElements() < 2) {
            throw new IllegalStateException("sample variance requires at least 2 values");
        }
        return runningSumSquaredDeviations() / (numElements() - 1);
    }

    /** Returns the population standard deviation, or {@link Double#NaN} if the accumulator is empty. */
    public double populationStandardDeviation() {
        return Math.sqrt(populationVariance());
    }

    /**
     * Returns the sample standard deviation (Bessel's correction).
     *
     * @throws IllegalStateException if fewer than 2 values have been added
     */
    public double sampleStandardDeviation() {
        return Math.sqrt(sampleVariance());
    }

    /**
     * Returns the maximum value ever added to this accumulator. Returns {@link Double#NaN} if no values
     * have been added. After {@link #remove} or {@link #subtract}, this may overestimate the true
     * maximum of the remaining values.
     */
    public double maxEver() {
        return Double.isInfinite(runningMaxEver) && runningMaxEver < 0 ? Double.NaN : runningMaxEver;
    }

    @Nonnull
    @Override
    public String toString() {
        return "RunningStats[" + numElements() + ", " + runningMean() + ", " +
                runningSumSquaredDeviations() + ", " + runningMaxEver() + ']';
    }

    /** Returns the empty accumulator (zero elements). */
    @Nonnull
    public static RunningStats identity() {
        return IDENTITY;
    }

    /**
     * Creates an accumulator containing a single value.
     *
     * @param newValue the initial value
     * @return a new accumulator with one element
     */
    @Nonnull
    public static RunningStats of(final double newValue) {
        return identity().add(newValue);
    }
}
