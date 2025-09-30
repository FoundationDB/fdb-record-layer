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

package com.apple.foundationdb.async.rabitq;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/** FhtKac-like random orthogonal rotator.
 *  - R rounds (default 4)
 *  - Per round: random ±1 -> FWHT on largest 2^k block (head/tail alternation) -> π/4 Givens across halves
 *  Time per apply: O(R * (n log n)) with tiny constants; memory: O(R * n) bits for signs.
 */
@SuppressWarnings({"checkstyle:MethodName", "checkstyle:MemberName"})
public final class FhtKacRotator {
    private final int n;
    private final int rounds;
    private final byte[][] signs; // signs[r][i] in {-1, +1}
    private static final double INV_SQRT2 = 1.0 / Math.sqrt(2.0);

    public FhtKacRotator(int n) {
        this(n, 4);
    }

    public FhtKacRotator(int n, int rounds) {
        if (n < 2) {
            throw new IllegalArgumentException("n must be >= 2");
        }
        if (rounds < 1) {
            throw new IllegalArgumentException("rounds must be >= 1");
        }
        this.n = n;
        this.rounds = rounds;

        // Pre-generate Rademacher signs for determinism/reuse.
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        this.signs = new byte[rounds][n];
        for (int r = 0; r < rounds; r++) {
            for (int i = 0; i < n; i++) {
                signs[r][i] = rng.nextBoolean() ? (byte)1 : (byte)-1;
            }
        }
    }

    public int getN() {
        return n;
    }

    /** y = P x. (y may be x for in-place.) */
    public double[] apply(double[] x, double[] y) {
        if (x.length != n) {
            throw new IllegalArgumentException("x.length != n");
        }
        if (y == null) {
            y = Arrays.copyOf(x, n);
        } else if (y != x) {
            System.arraycopy(x, 0, y, 0, n);
        }

        for (int r = 0; r < rounds; r++) {
            // 1) Rademacher signs
            byte[] s = signs[r];
            for (int i = 0; i < n; i++) {
                y[i] = (s[i] == 1 ? y[i] : -y[i]);
            }

            // 2) FWHT on largest 2^k block; alternate head/tail
            int m = largestPow2LE(n);
            int start = ((r & 1) == 0) ? 0 : (n - m); // head on even rounds, tail on odd
            fwhtNormalized(y, start, m);

            // 3) π/4 Givens between halves (pair i with i+h)
            givensPiOver4(y);
        }
        return y;
    }

    /** y = P^T x (the inverse). */
    public double[] applyTranspose(double[] x, double[] y) {
        if (x.length != n) {
            throw new IllegalArgumentException("x.length != n");
        }
        if (y == null) {
            y = Arrays.copyOf(x, n);
        } else if (y != x) {
            System.arraycopy(x, 0, y, 0, n);
        }

        for (int r = rounds - 1; r >= 0; r--) {
            // Inverse of step 3: Givens transpose (angle -> -π/4)
            givensMinusPiOver4(y);

            // Inverse of step 2: FWHT is its own inverse (orthonormal)
            int m = largestPow2LE(n);
            int start = ((r & 1) == 0) ? 0 : (n - m);
            fwhtNormalized(y, start, m);

            // Inverse of step 1: Rademacher signs (self-inverse)
            byte[] s = signs[r];
            for (int i = 0; i < n; i++) {
                y[i] = (s[i] == 1 ? y[i] : -y[i]);
            }
        }
        return y;
    }

    @SuppressWarnings("SuspiciousNameCombination")
    public void applyInPlace(double[] x) {
        apply(x, x);
    }

    @SuppressWarnings("SuspiciousNameCombination")
    public void applyTransposeInPlace(double[] x) {
        applyTranspose(x, x);
    }

    /**
     *  Build dense P as double[n][n] (row-major).
     */
    public double[][] computeP() {
        final double[][] P = new double[n][n];
        final double[] e = new double[n];
        for (int j = 0; j < n; j++) {
            Arrays.fill(e, 0.0);
            e[j] = 1.0;
            double[] y = apply(e, null);     // column j of P
            for (int i = 0; i < n; i++) {
                P[i][j] = y[i];
            }
        }
        return P;
    }

    // ----- internals -----

    private static int largestPow2LE(int n) {
        // highest power of two <= n
        return 1 << (31 - Integer.numberOfLeadingZeros(n));
    }

    /** In-place normalized FWHT on y[start .. start+m-1], where m is a power of two. */
    private static void fwhtNormalized(double[] y, int start, int m) {
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

    /** Apply π/4 Givens: [u'; v'] = [ c  s; -s  c ] [u; v], with c=s=1/sqrt(2). */
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

    /** Apply transpose (inverse) of the π/4 Givens: [u'; v'] = [ c -s; s  c ] [u; v]. */
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

    static double norm2(double[] a) {
        double s = 0;
        for (double v : a) {
            s += v * v;
        }
        return Math.sqrt(s);
    }

    static double maxAbsDiff(double[] a, double[] b) {
        double m = 0;
        for (int i = 0; i < a.length; i++) {
            m = Math.max(m, Math.abs(a[i] - b[i]));
        }
        return m;
    }
}
