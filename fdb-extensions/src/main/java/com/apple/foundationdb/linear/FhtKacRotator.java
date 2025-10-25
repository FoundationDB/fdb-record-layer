/*
 * FhtKacRotator.java
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

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

/**
 * FhtKac-like random orthogonal rotator which implements {@link LinearOperator}.
 * <p>
 * An orthogonal rotator conceptually is an orthogonal matrix that is applied to some vector {@code x} yielding a
 * new vector {@code y} that is rotated in some way along its n dimensions. Important to notice here is that such a
 * rotation preserves distances/lengths as well as angles between vectors.
 * <p>
 * Practically, we do not want to materialize such a rotator in memory as for {@code n} dimensions that would amount
 * to {@code n^2} cells. In addition to that multiplying a matrix with a vector computationally is {@code O(n^2)} which
 * is prohibitively expensive for large {@code n}.
 * <p>
 * We also want to achieve some sort of randomness with these rotations. For small {@code n}, we can start by creating a
 * randomly generated matrix and decompose it using QR-decomposition into an orthogonal matrix {@code Q} and an upper
 * triangular matrix {@code R}. That matrix {@code Q} is indeed random (see matrix Haar randomness) and what we are
 * actually trying to find. For larger {@code R} this approach becomes impractical as {@code Q} needs to be represented
 * as a dense matrix which increases memory footprint and makes rotations slow (see above).
 * <p>
 * The main idea is to use several operators in conjunction:
 * <ul>
 *     <li>{@code K} which is orthogonal and applies several
 *          <a href="https://en.wikipedia.org/wiki/Givens_rotation">Givens rotation</a> at once.</li>
 *     <li>{@code D_n} which are Random Rademacher diagonal sign matrices, i.e. matrices that are conceptually all
 *         {@code 0} except for the diagonal which contains any combination {@code -1}, {@code 1}. These matrices
 *         are also orthogonal. In particular, it flips only the signs of the elements of some other matrix when applied
 *         to it.</li>
 *     <li>{@code H} a <a href="https://en.wikipedia.org/wiki/Hadamard_matrix">Hadamard matrix </a> of a suitable size.
 *     </li>
 * </ul>
 * <p>
 * All these linear operators are combined in a way that we eventually compute the result of
 * <pre>
 * {@code
 *     x' = D1 H D_2 H ... D_(R-1) H D_(R) H K x
 * }
 * </pre>
 * (for {@code R} rotations). None of these operators require a significant amount of memory (O(R * n) bits for signs).
 * They perform the complete rotation in {@code O(R * (n log n))}.
 */
@SuppressWarnings({"checkstyle:MethodName", "checkstyle:MemberName"})
public final class FhtKacRotator implements LinearOperator {
    private final int numDimensions;
    private final int rounds;
    private final BitSet[] signs; // signs[r] of i bits in {not set: -1, set: +1}
    private static final double INV_SQRT2 = 1.0 / Math.sqrt(2.0);

    public FhtKacRotator(final long seed, final int numDimensions, final int rounds) {
        if (numDimensions < 2) {
            throw new IllegalArgumentException("n must be >= 2");
        }
        if (rounds < 1) {
            throw new IllegalArgumentException("rounds must be >= 1");
        }
        this.numDimensions = numDimensions;
        this.rounds = rounds;

        // Pre-generate Rademacher signs for determinism/reuse.
        final Random rng = new Random(seed);
        this.signs = new BitSet[rounds];
        for (int r = 0; r < rounds; r++) {
            final BitSet s = new BitSet(numDimensions);
            for (int i = 0; i < numDimensions; i++) {
                s.set(i, rng.nextBoolean());
            }
            signs[r] = s;
        }
    }

    @Override
    public int getNumRowDimensions() {
        return numDimensions;
    }

    @Override
    public int getNumColumnDimensions() {
        return numDimensions;
    }

    @Override
    public boolean isTransposable() {
        return true;
    }

    @Nonnull
    @Override
    public RealVector apply(@Nonnull final RealVector x) {
        return new DoubleRealVector(operate(x.getData()));
    }

    @Nonnull
    private double[] operate(@Nonnull final double[] x) {
        if (x.length != numDimensions) {
            throw new IllegalArgumentException("dimensionality of x != n");
        }
        final double[] y = Arrays.copyOf(x, numDimensions);

        for (int r = 0; r < rounds; r++) {
            // 1) Rademacher signs
            final BitSet s = signs[r];
            for (int i = 0; i < numDimensions; i++) {
                y[i] *= s.get(i) ? 1 : -1;
            }

            // 2) FHT on largest 2^k block; alternate head/tail
            int m = largestPow2LE(numDimensions);
            int start = ((r & 1) == 0) ? 0 : (numDimensions - m); // head on even rounds, tail on odd
            fhtNormalized(y, start, m);

            // 3) π/4 Givens between halves (pair i with i+h)
            givensPiOver4(y);
        }
        return y;
    }

    @Nonnull
    @Override
    public RealVector applyTranspose(@Nonnull final RealVector x) {
        return new DoubleRealVector(operateTranspose(x.getData()));
    }

    @Nonnull
    public double[] operateTranspose(@Nonnull final double[] x) {
        if (x.length != numDimensions) {
            throw new IllegalArgumentException("dimensionality of x != n");
        }
        final double[] y = Arrays.copyOf(x, numDimensions);

        for (int r = rounds - 1; r >= 0; r--) {
            // Inverse of step 3: Givens transpose (angle -> -π/4)
            givensMinusPiOver4(y);

            // Inverse of step 2: FWHT is its own inverse (orthonormal)
            int m = largestPow2LE(numDimensions);
            int start = ((r & 1) == 0) ? 0 : (numDimensions - m);
            fhtNormalized(y, start, m);

            // Inverse of step 1: Rademacher signs (self-inverse)
            final BitSet s = signs[r];
            for (int i = 0; i < numDimensions; i++) {
                y[i] *= s.get(i) ? 1 : -1;
            }
        }
        return y;
    }

    /**
     *  Build dense P as double[n][n] (row-major). This method exists for testing purposes only.
     */
    @Nonnull
    @VisibleForTesting
    public RowMajorRealMatrix computeP() {
        final double[][] p = new double[numDimensions][numDimensions];
        final double[] e = new double[numDimensions];
        for (int j = 0; j < numDimensions; j++) {
            Arrays.fill(e, 0.0);
            e[j] = 1.0;
            double[] y = operate(e);     // column j of P
            for (int i = 0; i < numDimensions; i++) {
                p[i][j] = y[i];
            }
        }
        return new RowMajorRealMatrix(p);
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof FhtKacRotator)) {
            return false;
        }

        final FhtKacRotator rotator = (FhtKacRotator)o;
        return numDimensions == rotator.numDimensions && rounds == rotator.rounds &&
                Arrays.deepEquals(signs, rotator.signs);
    }

    @Override
    public int hashCode() {
        int result = numDimensions;
        result = 31 * result + rounds;
        result = 31 * result + Arrays.deepHashCode(signs);
        return result;
    }

    // ----- internals -----

    private static int largestPow2LE(int n) {
        // highest power of two <= n
        return 1 << (31 - Integer.numberOfLeadingZeros(n));
    }

    /**
     *  In-place normalized FHT on y[start ... start+m-1], where m is a power of two.
     */
    private static void fhtNormalized(double[] y, int start, int m) {
        // Cooley-Tukey style
        for (int len = 1; len < m; len <<= 1) {
            int step = len << 1;
            for (int i = start; i < start + m; i += step) {
                for (int j = 0; j < len; j++) {
                    int a = i + j;
                    int b = a + len;
                    double u = y[a];
                    double v = y[b];
                    y[a] = u + v;
                    y[b] = u - v;
                }
            }
        }
        double scale = 1.0 / Math.sqrt(m);
        for (int i = start; i < start + m; i++) {
            y[i] *= scale;
        }
    }

    /**
     *  Apply π/4 Givens rotation.
     *  <pre>
     *  {@code
     *  [u'; v'] = [  cos(π/4)  sin(π/4) ] [u]
     *             [ -sin(π/4)  cos(π/4) ] [v]
     *
     *  Since cos(π/4) = sin(π/4) = 1/sqrt(2) this can be rewritten as
     *
     *  [u'; v'] = 1/ sqrt(2) * [  1  1 ] [u]
     *                          [ -1  1 ] [v]
     *
     *  which allows for fast computation. Note that we rotate the incoming vector along many axes at once, the
     *  two-dimensional example is for illustrative purposes only.
     *  }
     *  </pre>
     */
    private static void givensPiOver4(double[] y) {
        int h = nHalfFloor(y.length);
        for (int i = 0; i < h; i++) {
            int j = i + h;
            if (j >= y.length) {
                break;
            }
            double u = y[i];
            double v = y[j];
            double up = (u + v) * INV_SQRT2;
            double vp = (-u + v) * INV_SQRT2; // -s*u + c*v with c=s
            y[i] = up;
            y[j] = vp;
        }
    }

    /**
     * Apply transpose (inverse) of the π/4 Givens.
     * @see #givensPiOver4(double[])
     */
    private static void givensMinusPiOver4(double[] y) {
        int h = nHalfFloor(y.length);
        for (int i = 0; i < h; i++) {
            int j = i + h;
            if (j >= y.length) {
                break;
            }
            double u = y[i];
            double v = y[j];
            double up = (u - v) * INV_SQRT2; // c*u - s*v
            double vp = (u + v) * INV_SQRT2; // s*u + c*v
            y[i] = up;
            y[j] = vp;
        }
    }

    private static int nHalfFloor(int n) {
        return n >>> 1;
    }
}
