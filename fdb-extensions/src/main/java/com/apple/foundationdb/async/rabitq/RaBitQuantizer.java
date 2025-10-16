/*
 * RaBitQuantizer.java
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
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.PriorityQueue;

public final class RaBitQuantizer implements Quantizer {
    // Matches kTightStart[] from the C++ (index by ex_bits).
    // 0th entry unused; defined up to 8 extra bits in the source.
    private static final double[] TIGHT_START = {
            0.00, 0.15, 0.20, 0.52, 0.59, 0.71, 0.75, 0.77, 0.81
    };

    @Nonnull
    private final RealVector centroid;
    final int numExBits;
    @Nonnull
    private final Metric metric;

    public RaBitQuantizer(@Nonnull final Metric metric,
                          @Nonnull final RealVector centroid,
                          final int numExBits) {
        this.centroid = centroid;
        this.numExBits = numExBits;
        this.metric = metric;
    }

    private static final double EPS = 1e-5;
    private static final double EPS0 = 1.9;
    private static final int N_ENUM = 10;

    public int getNumDimensions() {
        return centroid.getNumDimensions();
    }

    @Nonnull
    @Override
    public RaBitEstimator estimator() {
        return new RaBitEstimator(metric, centroid, numExBits);
    }

    @Nonnull
    @Override
    public EncodedRealVector encode(@Nonnull final RealVector data) {
        return encodeInternal(data).getEncodedVector();
    }

    /**
     * Port of ex_bits_code_with_factor:
     * - params: data & centroid (rotated)
     * - forms residual internally
     * - computes shifted signed vector here (sign(r)*(k+0.5))
     * - applies C++ metric-dependent formulas exactly.
     */
    @Nonnull
    Result encodeInternal(@Nonnull final RealVector data) {
        final int dims = data.getNumDimensions();

        // 2) Build residual again: r = data - centroid
        final RealVector residual = data; //.subtract(centroid);

        // 1) call ex_bits_code to get signedCode, t, ipNormInv
        QuantizeExResult base = exBitsCode(residual);
        int[] signedCode = base.code;
        double ipInv = base.ipNormInv;

        int[] totalCode = new int[dims];
        for (int i = 0; i < dims; i++) {
            int sgn = (residual.getComponent(i) >= 0.0) ? +1 : 0;
            totalCode[i] = signedCode[i] + (sgn << numExBits);
        }

        // 4) cb = -(2^b - 0.5), and xu_cb = signedShift + cb
        final double cb = -(((1 << numExBits) - 0.5));
        double[] xu_cb_data = new double[dims];
        for (int i = 0; i < dims; i++) {
            xu_cb_data[i] = totalCode[i] + cb;
        }
        final RealVector xu_cb = new DoubleRealVector(xu_cb_data);

        // 5) Precompute all needed values
        final double residual_l2_norm = residual.l2Norm();
        final double residual_l2_sqr = residual_l2_norm * residual_l2_norm;
        final double ip_resi_xucb = residual.dot(xu_cb);
        //final double ip_cent_xucb = centroid.dot(xu_cb);
        final double xuCbNorm = xu_cb.l2Norm();
        final double xuCbNormSqr = xuCbNorm * xuCbNorm;

        final double ip_resi_xucb_safe =
                (ip_resi_xucb == 0.0) ? Double.POSITIVE_INFINITY : ip_resi_xucb;

        double tmp_error = residual_l2_norm * EPS0 *
                Math.sqrt(((residual_l2_sqr * xuCbNormSqr) / (ip_resi_xucb_safe * ip_resi_xucb_safe) - 1.0)
                        / (Math.max(1, dims - 1)));

        double fAddEx;
        double fRescaleEx;
        double fErrorEx;

        if (metric == Metric.EUCLIDEAN_SQUARE_METRIC || metric == Metric.EUCLIDEAN_METRIC) {
            fAddEx = residual_l2_sqr; // + 2.0 * residual_l2_sqr * (ip_cent_xucb / ip_resi_xucb_safe);
            fRescaleEx = ipInv * (-2.0 * residual_l2_norm);
            fErrorEx = 2.0 * tmp_error;
        } else if (metric == Metric.DOT_PRODUCT_METRIC) {
            fAddEx = 1.0; //- residual.dot(centroid) + residual_l2_sqr * (ip_cent_xucb / ip_resi_xucb_safe);
            fRescaleEx = ipInv * (-1.0 * residual_l2_norm);
            fErrorEx = tmp_error;
        } else {
            throw new IllegalArgumentException("Unsupported metric");
        }

        return new Result(new EncodedRealVector(numExBits, totalCode, fAddEx, fRescaleEx, fErrorEx), base.t, ipInv);
    }

    /**
     * Builds per-dimension extra-bit levels using the best t found by bestRescaleFactor() and returns
     * ipNormInv.
     * @param residual Rotated residual vector r (same thing the C++ feeds here).
     *                 This method internally uses |r| normalized to unit L2.
     */
    private QuantizeExResult exBitsCode(@Nonnull final RealVector residual) {
        int dims = residual.getNumDimensions();

        // oAbs = |r| normalized (RaBitQ does this before quantizeEx)
        final RealVector oAbs = absOfNormalized(residual);

        final QuantizeExResult q = quantizeEx(oAbs);

        int[] k = q.code;
        // revert codes for negative dims
        int[] signed = new int[dims];
        int mask = (1 << numExBits) - 1;
        for (int j = 0; j < dims; ++j) {
            if (residual.getComponent(j) < 0) {
                int tmp = k[j];
                signed[j] = (~tmp) & mask;
            } else {
                signed[j] = k[j];
            }
        }

        return new QuantizeExResult(signed, q.t, q.ipNormInv);
    }

    /**
     * Method to quantize a vector.
     *
     * @param oAbs   absolute values of a L2-normalized residual vector (nonnegative; length = dim)
     * @return       quantized levels (ex-bits), the chosen scale t, and ipNormInv
     * Notes:
     * - If the residual is the all-zero vector (or numerically so), this returns zero codes,
     *   t = 0, and ipNormInv = 1 (benign fallback).
     * - Downstream code (ex_bits_code_with_factor) uses ipNormInv to compute f_rescale_ex, etc.
     */
    private QuantizeExResult quantizeEx(@Nonnull final RealVector oAbs) {
        final int dim = oAbs.getNumDimensions();
        final int maxLevel = (1 << numExBits) - 1;

        // Choose t via the sweep.
        double t = bestRescaleFactor(oAbs);
        // ipNorm = sum_i ( (k_i + 0.5) * |r_i| )
        double ipNorm = 0.0;

        // Build per-coordinate integer levels: k_i = floor(t * |r_i|)
        int[] code = new int[dim];
        for (int i = 0; i < dim; i++) {
            int k = (int) Math.floor(t * oAbs.getComponent(i) + EPS);
            if (k > maxLevel) {
                k = maxLevel;
            }
            code[i] = k;
            ipNorm += (k + 0.5) * oAbs.getComponent(i);
        }

        // ipNormInv = 1 / ipNorm, with a benign fallback.
        double ipNormInv;
        if (ipNorm > 0.0 && Double.isFinite(ipNorm)) {
            ipNormInv = 1.0 / ipNorm;
            if (!Double.isFinite(ipNormInv) || ipNormInv == 0.0) {
                ipNormInv = 1.0; // extremely defensive
            }
        } else {
            ipNormInv = 1.0; // fallback used in the C++ source
        }

        return new QuantizeExResult(code, t, ipNormInv);
    }

    /**
     *  Method to compute the best factor {@code t}.
     *  @param oAbs   absolute values of a (row-wise) normalized residual; length = dim; nonnegative
     *  @return t     the rescale factor that maximizes the objective
     */
    private double bestRescaleFactor(@Nonnull final RealVector oAbs) {
        if (numExBits < 0 || numExBits >= TIGHT_START.length) {
            throw new IllegalArgumentException("numExBits out of supported range");
        }

        // max_o = max(oAbs)
        double maxO = 0.0d;
        for (double v : oAbs.getData()) {
            if (v > maxO) {
                maxO = v;
            }
        }
        if (maxO <= 0.0) {
            return 0.0; // all zeros: nothing to scale
        }

        // t_end and a "tight" t_start as in the C++ code
        final int maxLevel = (1 << numExBits) - 1;
        final double tEnd = ((maxLevel) + N_ENUM) / maxO;
        final double tStart = tEnd * TIGHT_START[numExBits];

        // cur_o_bar[i] = floor(tStart * oAbs[i]), but stored as int
        final int[] curOB = new int[getNumDimensions()];
        double sqrDen = getNumDimensions() * 0.25; // Σ (cur^2 + cur) starts from D/4
        double numer = 0.0;
        for (int i = 0; i < getNumDimensions(); i++) {
            int cur = (int) ((tStart * oAbs.getComponent(i)) + EPS);
            curOB[i] = cur;
            sqrDen += (double) cur * cur + cur;
            numer  += (cur + 0.5) * oAbs.getComponent(i);
        }

        // Min-heap keyed by next threshold t at which coordinate "i" increments:
        // t_i(k->k+1) = (curOB[i] + 1) / oAbs[i]

        final PriorityQueue<Node> pq = new PriorityQueue<>(Comparator.comparingDouble(n -> n.t));
        for (int i = 0; i < getNumDimensions(); i++) {
            final double curOAbs = oAbs.getComponent(i);
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
            numer  += oAbs.getComponent(i);

            // objective value
            double curIp = numer / Math.sqrt(sqrDen);
            if (curIp > maxIp) {
                maxIp = curIp;
                bestT = curT;
            }

            // schedule next threshold for this coordinate, unless we've hit max level
            if (u < maxLevel) {
                double oi = oAbs.getComponent(i);
                double tNext = (u + 1) / oi;
                if (tNext < tEnd) {
                    pq.add(new Node(tNext, i));
                }
            }
        }

        return bestT;
    }

    private static RealVector absOfNormalized(@Nonnull final RealVector x) {
        double n = x.l2Norm();
        double[] y = new double[x.getNumDimensions()];
        if (n == 0.0 || !Double.isFinite(n)) {
            return new DoubleRealVector(y); // all zeros
        }
        double inv = 1.0 / n;
        for (int i = 0; i < x.getNumDimensions(); i++) {
            y[i] = Math.abs(x.getComponent(i) * inv);
        }
        return new DoubleRealVector(y);
    }

    @SuppressWarnings("checkstyle:MemberName")
    public static final class Result {
        public EncodedRealVector encodedVector;
        public final double t;
        public final double ipNormInv;

        public Result(@Nonnull final EncodedRealVector encodedVector, double t, double ipNormInv) {
            this.encodedVector = encodedVector;
            this.t = t;
            this.ipNormInv = ipNormInv;
        }

        public EncodedRealVector getEncodedVector() {
            return encodedVector;
        }

        public double getT() {
            return t;
        }

        public double getIpNormInv() {
            return ipNormInv;
        }
    }

    @SuppressWarnings("checkstyle:MemberName")
    private static final class QuantizeExResult {
        public final int[] code;       // k_i = floor(t * oAbs[i]) in [0, 2^exBits - 1]
        public final double t;         // chosen global scale
        public final double ipNormInv; // 1 / sum_i ( (k_i + 0.5) * oAbs[i] )

        public QuantizeExResult(int[] code, double t, double ipNormInv) {
            this.code = code;
            this.t = t;
            this.ipNormInv = ipNormInv;
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
