/*
 * SimdBackend.java
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

package com.apple.foundationdb.linear.simd;

import com.apple.foundationdb.linear.Backend;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import javax.annotation.Nonnull;

/**
 * SIMD implementation of {@link Backend} using the {@code jdk.incubator.vector} API
 * ({@link DoubleVector} with {@link DoubleVector#SPECIES_PREFERRED}). Loaded reflectively by
 * {@code RealVectorPrimitives}; never imported directly from the parent package.
 * <p>
 * Each element-wise method walks the input in {@code SPECIES.length()}-sized chunks (vectorized
 * loop) and processes the remaining 0–{@code SPECIES.length()-1} elements with a scalar tail
 * loop. Reductions (dot, l2SquaredNorm, euclideanSquared) accumulate into a
 * {@link DoubleVector} via {@link DoubleVector#fma fused multiply-add} and reduce horizontally at
 * the end. The vectorized loop body is a no-op when the input is shorter than one vector lane,
 * so small vectors fall through to the scalar tail without any explicit threshold check.
 */
public final class SimdBackend implements Backend {
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    @Nonnull
    @Override
    public String name() {
        return "simd[" + SPECIES + "]";
    }

    @Override
    public void addInto(@Nonnull final double[] a, @Nonnull final double[] b, @Nonnull final double[] out) {
        final int len = a.length;
        final int bound = SPECIES.loopBound(len);
        int i = 0;
        for (; i < bound; i += SPECIES.length()) {
            final DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            final DoubleVector vb = DoubleVector.fromArray(SPECIES, b, i);
            va.add(vb).intoArray(out, i);
        }
        for (; i < len; i++) {
            out[i] = a[i] + b[i];
        }
    }

    @Override
    public void addInto(@Nonnull final double[] a, final double scalar, @Nonnull final double[] out) {
        final int len = a.length;
        final int bound = SPECIES.loopBound(len);
        int i = 0;
        for (; i < bound; i += SPECIES.length()) {
            DoubleVector.fromArray(SPECIES, a, i).add(scalar).intoArray(out, i);
        }
        for (; i < len; i++) {
            out[i] = a[i] + scalar;
        }
    }

    @Override
    public void subtractInto(@Nonnull final double[] a, @Nonnull final double[] b, @Nonnull final double[] out) {
        final int len = a.length;
        final int bound = SPECIES.loopBound(len);
        int i = 0;
        for (; i < bound; i += SPECIES.length()) {
            final DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            final DoubleVector vb = DoubleVector.fromArray(SPECIES, b, i);
            va.sub(vb).intoArray(out, i);
        }
        for (; i < len; i++) {
            out[i] = a[i] - b[i];
        }
    }

    @Override
    public void subtractInto(@Nonnull final double[] a, final double scalar, @Nonnull final double[] out) {
        final int len = a.length;
        final int bound = SPECIES.loopBound(len);
        int i = 0;
        for (; i < bound; i += SPECIES.length()) {
            DoubleVector.fromArray(SPECIES, a, i).sub(scalar).intoArray(out, i);
        }
        for (; i < len; i++) {
            out[i] = a[i] - scalar;
        }
    }

    @Override
    public void multiplyInto(@Nonnull final double[] a, final double scalar, @Nonnull final double[] out) {
        final int len = a.length;
        final int bound = SPECIES.loopBound(len);
        int i = 0;
        for (; i < bound; i += SPECIES.length()) {
            DoubleVector.fromArray(SPECIES, a, i).mul(scalar).intoArray(out, i);
        }
        for (; i < len; i++) {
            out[i] = a[i] * scalar;
        }
    }

    @Override
    public double dot(@Nonnull final double[] a, @Nonnull final double[] b) {
        final int len = a.length;
        final int laneCount = SPECIES.length();
        final int bound = SPECIES.loopBound(len);
        // Four independent FMA accumulators break the loop-carried dependency on a single
        // accumulator and let the CPU pipeline 4 FMAs per FMA-latency window instead of 1.
        DoubleVector acc0 = DoubleVector.zero(SPECIES);
        DoubleVector acc1 = DoubleVector.zero(SPECIES);
        DoubleVector acc2 = DoubleVector.zero(SPECIES);
        DoubleVector acc3 = DoubleVector.zero(SPECIES);

        final int unrolledBound = bound - (4 * laneCount - 1);
        int i = 0;
        for (; i < unrolledBound; i += 4 * laneCount) {
            acc0 = DoubleVector.fromArray(SPECIES, a, i)
                    .fma(DoubleVector.fromArray(SPECIES, b, i), acc0);
            acc1 = DoubleVector.fromArray(SPECIES, a, i + laneCount)
                    .fma(DoubleVector.fromArray(SPECIES, b, i + laneCount), acc1);
            acc2 = DoubleVector.fromArray(SPECIES, a, i + 2 * laneCount)
                    .fma(DoubleVector.fromArray(SPECIES, b, i + 2 * laneCount), acc2);
            acc3 = DoubleVector.fromArray(SPECIES, a, i + 3 * laneCount)
                    .fma(DoubleVector.fromArray(SPECIES, b, i + 3 * laneCount), acc3);
        }
        // Trailing full-width SIMD iterations (when len/laneCount mod 4 != 0).
        for (; i < bound; i += laneCount) {
            acc0 = DoubleVector.fromArray(SPECIES, a, i)
                    .fma(DoubleVector.fromArray(SPECIES, b, i), acc0);
        }
        double sum = acc0.add(acc1).add(acc2).add(acc3).reduceLanes(VectorOperators.ADD);
        // Scalar tail.
        for (; i < len; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    @Override
    public double l2SquaredNorm(@Nonnull final double[] a) {
        final int len = a.length;
        final int laneCount = SPECIES.length();
        final int bound = SPECIES.loopBound(len);
        DoubleVector acc0 = DoubleVector.zero(SPECIES);
        DoubleVector acc1 = DoubleVector.zero(SPECIES);
        DoubleVector acc2 = DoubleVector.zero(SPECIES);
        DoubleVector acc3 = DoubleVector.zero(SPECIES);

        final int unrolledBound = bound - (4 * laneCount - 1);
        int i = 0;
        for (; i < unrolledBound; i += 4 * laneCount) {
            final DoubleVector v0 = DoubleVector.fromArray(SPECIES, a, i);
            final DoubleVector v1 = DoubleVector.fromArray(SPECIES, a, i + laneCount);
            final DoubleVector v2 = DoubleVector.fromArray(SPECIES, a, i + 2 * laneCount);
            final DoubleVector v3 = DoubleVector.fromArray(SPECIES, a, i + 3 * laneCount);
            acc0 = v0.fma(v0, acc0);
            acc1 = v1.fma(v1, acc1);
            acc2 = v2.fma(v2, acc2);
            acc3 = v3.fma(v3, acc3);
        }
        for (; i < bound; i += laneCount) {
            final DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            acc0 = va.fma(va, acc0);
        }
        double sum = acc0.add(acc1).add(acc2).add(acc3).reduceLanes(VectorOperators.ADD);
        for (; i < len; i++) {
            sum += a[i] * a[i];
        }
        return sum;
    }

    @Override
    public double euclideanSquared(@Nonnull final double[] a, @Nonnull final double[] b) {
        final int len = a.length;
        final int laneCount = SPECIES.length();
        final int bound = SPECIES.loopBound(len);
        DoubleVector acc0 = DoubleVector.zero(SPECIES);
        DoubleVector acc1 = DoubleVector.zero(SPECIES);
        DoubleVector acc2 = DoubleVector.zero(SPECIES);
        DoubleVector acc3 = DoubleVector.zero(SPECIES);

        final int unrolledBound = bound - (4 * laneCount - 1);
        int i = 0;
        for (; i < unrolledBound; i += 4 * laneCount) {
            final DoubleVector d0 = DoubleVector.fromArray(SPECIES, a, i)
                    .sub(DoubleVector.fromArray(SPECIES, b, i));
            final DoubleVector d1 = DoubleVector.fromArray(SPECIES, a, i + laneCount)
                    .sub(DoubleVector.fromArray(SPECIES, b, i + laneCount));
            final DoubleVector d2 = DoubleVector.fromArray(SPECIES, a, i + 2 * laneCount)
                    .sub(DoubleVector.fromArray(SPECIES, b, i + 2 * laneCount));
            final DoubleVector d3 = DoubleVector.fromArray(SPECIES, a, i + 3 * laneCount)
                    .sub(DoubleVector.fromArray(SPECIES, b, i + 3 * laneCount));
            acc0 = d0.fma(d0, acc0);
            acc1 = d1.fma(d1, acc1);
            acc2 = d2.fma(d2, acc2);
            acc3 = d3.fma(d3, acc3);
        }
        for (; i < bound; i += laneCount) {
            final DoubleVector diff = DoubleVector.fromArray(SPECIES, a, i)
                    .sub(DoubleVector.fromArray(SPECIES, b, i));
            acc0 = diff.fma(diff, acc0);
        }
        double sum = acc0.add(acc1).add(acc2).add(acc3).reduceLanes(VectorOperators.ADD);
        for (; i < len; i++) {
            final double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }
}
