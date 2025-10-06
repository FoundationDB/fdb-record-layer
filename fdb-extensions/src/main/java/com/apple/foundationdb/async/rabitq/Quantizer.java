/*
 * Quantizer.java
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

import com.apple.foundationdb.async.hnsw.Metrics;

import java.util.Comparator;
import java.util.PriorityQueue;

public final class Quantizer {

    // Matches kTightStart[] from the C++ (index by ex_bits).
    // 0th entry unused; defined up to 8 extra bits in the source.
    private static final double[] TIGHT_START = {
            0.00, 0.15, 0.20, 0.52, 0.59, 0.71, 0.75, 0.77, 0.81
    };

    private static final double EPS = 1e-5;
    private static final int N_ENUM = 10;

    /** L2 norm. */
    private static double l2(double[] x) {
        double s = 0.0;
        for (double v : x) {
            s += v * v;
        }
        return Math.sqrt(s);
    }

    /** abs(normalize(x)). If ||x||==0, returns a zero array. */
    private static double[] absOfNormalized(double[] x) {
        double n = l2(x);
        double[] y = new double[x.length];
        if (n == 0.0 || !Double.isFinite(n)) {
            return y; // all zeros
        }
        double inv = 1.0 / n;
        for (int i = 0; i < x.length; i++) {
            y[i] = Math.abs(x[i] * inv);
        }
        return y;
    }

    private static double dot(double[] a, double[] b) {
        double s = 0.0;
        for (int i = 0; i < a.length; i++) {
            s += a[i] * b[i];
        }
        return s;
    }

    /**
     * Port of ex_bits_code_with_factor:
     * - params: data & centroid (rotated)
     * - forms residual internally
     * - computes shifted signed vector here (sign(r)*(k+0.5))
     * - applies C++ metric-dependent formulas exactly.
     */
    public static Result exBitsCodeWithFactor(double[] dataRot,
                                              double[] centroidRot,
                                              int exBits,
                                              Metrics metric) {
        final int dims = dataRot.length;

        // 2) Build residual again: r = data - centroid
        double[] residual = new double[dims];
        for (int i = 0; i < dims; i++) {
            residual[i] = dataRot[i] - centroidRot[i];
        }

        // 1) call ex_bits_code to get signedCode, t, ipnormInv
        QuantizeExResult base = exBitsCode(residual, exBits);
        int[] signedCode = base.code;
        double ipInv = base.ipnormInv;

        int[] totalCode = new int[dims];
        for (int i = 0; i < dims; i++) {
            int sgn = (residual[i] >= 0.0) ? +1 : 0;
            totalCode[i] = signedCode[i] + (sgn << exBits);
        }

        // 4) cb = -(2^b - 0.5), and xu_cb = signedShift + cb
        final double cb = -(((1 << exBits) - 0.5));
        double[] xu_cb = new double[dims];
        for (int i = 0; i < dims; i++) {
            xu_cb[i] = totalCode[i] + cb;
        }

        // 5) Precompute all needed values
        final double l2_norm = l2(residual);
        final double l2_sqr = l2_norm * l2_norm;
        final double ip_resi_xucb = dot(residual, xu_cb);
        final double ip_cent_xucb = dot(centroidRot, xu_cb);
        final double xuCbNormSqr = dot(xu_cb, xu_cb);

        final double ip_resi_xucb_safe =
                (ip_resi_xucb == 0.0) ? Double.POSITIVE_INFINITY : ip_resi_xucb;

        double tmp_error = l2_norm * EPS *
                Math.sqrt(((l2_sqr * xuCbNormSqr) / (ip_resi_xucb_safe * ip_resi_xucb_safe) - 1.0)
                        / (Math.max(1, dims - 1)));

        double fAddEx;
        double fRescaleEx;
        double fErrorEx;

        if (metric == Metrics.EUCLIDEAN_SQUARE_METRIC) {
            fAddEx = l2_sqr + 2.0 * l2_sqr * (ip_cent_xucb / ip_resi_xucb_safe);
            fRescaleEx = ipInv * (-2.0 * l2_norm);
            fErrorEx = 2.0 * tmp_error;
        } else if (metric == Metrics.DOT_PRODUCT_METRIC) {
            fAddEx = 1.0 - dot(residual, centroidRot) + l2_sqr * (ip_cent_xucb / ip_resi_xucb_safe);
            fRescaleEx = ipInv * (-1.0 * l2_norm);
            fErrorEx = tmp_error;
        } else {
            throw new IllegalArgumentException("Unsupported metric");
        }

        return new Result(totalCode, base.t, ipInv, fAddEx, fRescaleEx, fErrorEx);
    }

    /**
     * Builds per-dimension extra-bit levels using the best t found by bestRescaleFactor() and returns
     * ipnormInv.
     * @param residual Rotated residual vector r (same thing the C++ feeds here).
     *                 This method internally uses |r| normalized to unit L2.
     * @param exBits   # extra bits per dimension (e.g. 1..8)
     */
    public static QuantizeExResult exBitsCode(double[] residual, int exBits) {
        int dims = residual.length;

        // oAbs = |r| normalized (RaBitQ does this before quantizeEx)
        double[] oAbs = absOfNormalized(residual);

        final QuantizeExResult q = quantizeEx(oAbs, exBits);

        int[] k = q.code;

        // revert codes for negative dims
        int[] signed = new int[dims];
        int mask = (1 << exBits) - 1;
        for (int j = 0; j < dims; ++j) {
            if (residual[j] < 0) {
                int tmp = k[j];
                signed[j] = (~tmp) & mask;
            } else {
                signed[j] = k[j];
            }
        }

        return new QuantizeExResult(signed, q.t, q.ipnormInv);
    }

    /**
     * Method to quantize a vector.
     *
     * @param oAbs   absolute values of a L2-normalized residual vector (nonnegative; length = dim)
     * @param exBits number of extra bits per coordinate (e.g., 1..8)
     * @return       quantized levels (ex-bits), the chosen scale t, and ipnormInv
     * Notes:
     * - If the residual is the all-zero vector (or numerically so), this returns zero codes,
     *   t = 0, and ipnormInv = 1 (benign fallback, matching the C++ guard with isnormal()).
     * - Downstream code (ex_bits_code_with_factor) uses ipnormInv to compute f_rescale_ex, etc.
     */
    public static QuantizeExResult quantizeEx(double[] oAbs, int exBits) {
        final int dim = oAbs.length;
        final int maxLevel = (1 << exBits) - 1;

        // Choose t via the sweep.
        double t = bestRescaleFactor(oAbs, exBits);
        // ipnorm = sum_i ( (k_i + 0.5) * |r_i| )
        double ipnorm = 0.0;

        // Build per-coordinate integer levels: k_i = floor(t * |r_i|)
        int[] code = new int[dim];
        for (int i = 0; i < dim; i++) {
            int k = (int) Math.floor(t * oAbs[i] + EPS);
            if (k > maxLevel) {
                k = maxLevel;
            }
            code[i] = k;
            ipnorm += (k + 0.5) * oAbs[i];
        }

        // ipnormInv = 1 / ipnorm, with a benign fallback (matches std::isnormal guard).
        double ipnormInv;
        if (ipnorm > 0.0 && Double.isFinite(ipnorm)) {
            ipnormInv = 1.0 / ipnorm;
            if (!Double.isFinite(ipnormInv) || ipnormInv == 0.0) {
                ipnormInv = 1.0; // extremely defensive
            }
        } else {
            ipnormInv = 1.0; // fallback used in the C++ (`std::isnormal` guard pattern)
        }

        return new QuantizeExResult(code, t, ipnormInv);
    }

    /**
     *  Method to compute the best factor {@code t}.
     *  @param oAbs   absolute values of a (row-wise) normalized residual; length = dim; nonnegative
     *  @param exBits number of extra bits per coordinate (1..8 supported by the constants)
     *  @return t     the rescale factor that maximizes the objective
     */
    public static double bestRescaleFactor(double[] oAbs, int exBits) {
        final int dim = oAbs.length;
        if (dim == 0) {
            throw new IllegalArgumentException("don't support 0 dimensions");
        }
        if (exBits < 0 || exBits >= TIGHT_START.length) {
            throw new IllegalArgumentException("exBits out of supported range");
        }

        // max_o = max(oAbs)
        double maxO = 0.0d;
        for (double v : oAbs) {
            if (v > maxO) {
                maxO = v;
            }
        }
        if (maxO <= 0.0) {
            return 0.0; // all zeros: nothing to scale
        }

        // t_end and a "tight" t_start as in the C++ code
        final int maxLevel = (1 << exBits) - 1;
        final double tEnd = ((maxLevel) + N_ENUM) / maxO;
        final double tStart = tEnd * TIGHT_START[exBits];

        // cur_o_bar[i] = floor(tStart * oAbs[i]), but stored as int
        final int[] curOB = new int[dim];
        double sqrDen = dim * 0.25; // Σ (cur^2 + cur) starts from D/4
        double numer = 0.0;
        for (int i = 0; i < dim; i++) {
            int cur = (int) ((tStart * oAbs[i]) + EPS);
            curOB[i] = cur;
            sqrDen += (double) cur * cur + cur;
            numer  += (cur + 0.5) * oAbs[i];
        }

        // Min-heap keyed by next threshold t at which coord i increments:
        // t_i(k->k+1) = (curOB[i] + 1) / oAbs[i]

        PriorityQueue<Node> pq = new PriorityQueue<>(Comparator.comparingDouble(n -> n.t));
        for (int i = 0; i < dim; i++) {
            final double curOAbs = oAbs[i];
            if (curOAbs > 0.0) {
                double tNext = (curOB[i] + 1) / curOAbs;
                pq.add(new Node(tNext, i));
            }
        }

        double maxIp = 0.0;
        double bestT = 0.0;

        while (!pq.isEmpty()) {
            Node node = pq.poll();
            double curT = node.t;
            int i = node.idx;

            // increment cur_o_bar[i]
            curOB[i]++;
            int u = curOB[i];

            // update denominator and numerator:
            // sqrDen += 2*u; numer += oAbs[i]
            sqrDen += 2.0 * u;
            numer  += oAbs[i];

            // objective value
            double curIp = numer / Math.sqrt(sqrDen);
            if (curIp > maxIp) {
                maxIp = curIp;
                bestT = curT;
            }

            // schedule next threshold for this coordinate, unless we've hit max level
            if (u < maxLevel) {
                double oi = oAbs[i];
                double tNext = (u + 1) / oi;
                if (tNext < tEnd) {
                    pq.add(new Node(tNext, i));
                }
            }
        }

        return bestT;
    }

    @SuppressWarnings("checkstyle:MemberName")
    public static final class Result {
        public final int[] signedCode;   // sign ⊙ k
        public final double t;
        public final double ipnormInv;
        public final double fAddEx;
        public final double fRescaleEx;
        public final double fErrorEx;

        public Result(int[] signedCode, double t, double ipnormInv,
                      double fAddEx, double fRescaleEx, double fErrorEx) {
            this.signedCode = signedCode;
            this.t = t;
            this.ipnormInv = ipnormInv;
            this.fAddEx = fAddEx;
            this.fRescaleEx = fRescaleEx;
            this.fErrorEx = fErrorEx;
        }
    }

    @SuppressWarnings("checkstyle:MemberName")
    public static final class QuantizeExResult {
        public final int[] code;       // k_i = floor(t * oAbs[i]) in [0, 2^exBits - 1]
        public final double t;         // chosen global scale
        public final double ipnormInv; // 1 / sum_i ( (k_i + 0.5) * oAbs[i] )

        public QuantizeExResult(int[] code, double t, double ipnormInv) {
            this.code = code;
            this.t = t;
            this.ipnormInv = ipnormInv;
        }
    }

    @SuppressWarnings("checkstyle:MemberName")
    private static final class Node {
        private final double t;
        private final int idx;

        Node(double t, int idx) {
            this.t = t;
            this.idx = idx;
        }
    }
}
