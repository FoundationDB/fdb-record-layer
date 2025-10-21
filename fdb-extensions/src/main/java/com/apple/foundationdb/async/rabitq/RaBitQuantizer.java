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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Implements the RaBit quantization scheme, a technique for compressing high-dimensional vectors into a compact
 * integer-based representation.
 * <p>
 * This class provides the logic to encode a {@link RealVector} into an {@link EncodedRealVector}.
 * The encoding process involves finding an optimal scaling factor, quantizing the vector's components,
 * and pre-calculating values that facilitate efficient distance estimation in the quantized space.
 * It is configured with a specific {@link Metric} and a number of "extra bits" ({@code numExBits})
 * which control the precision of the quantization.
 * <p>
 * Note that this implementation largely follows this <a href="https://arxiv.org/pdf/2409.09913">paper</a>
 * by Jianyang Gao et al. It also mirrors algorithmic similarity, terms, and variable/method naming-conventions of the
 * C++ implementation that can be found <a href="https://github.com/VectorDB-NTU/RaBitQ-Library">here</a>.
 *
 * @see Quantizer
 * @see RaBitEstimator
 * @see EncodedRealVector
 */
public final class RaBitQuantizer implements Quantizer {
    private static final double EPS = 1e-5;
    private static final double EPS0 = 1.9;
    private static final int N_ENUM = 10;

    // 0th entry unused; defined up to 8 extra bits in the source.
    private static final double[] TIGHT_START = {
            0.00, 0.15, 0.20, 0.52, 0.59, 0.71, 0.75, 0.77, 0.81
    };

    final int numExBits;
    @Nonnull
    private final Metric metric;

    /**
     * Constructs a new {@code RaBitQuantizer} instance.
     * <p>
     * This constructor initializes the quantizer with a specific metric and the number of
     * extra bits to be used in the quantization process.
     *
     * @param metric the {@link Metric} to be used for quantization; must not be null.
     * @param numExBits the number of extra bits for quantization.
     */
    public RaBitQuantizer(@Nonnull final Metric metric, final int numExBits) {
        Preconditions.checkArgument(numExBits > 0 && numExBits < TIGHT_START.length);

        this.numExBits = numExBits;
        this.metric = metric;
    }

    /**
     * Creates and returns a new {@link RaBitEstimator} instance.
     * <p>
     * This method acts as a factory, constructing the estimator based on the
     * {@code metric} and {@code numExBits} configuration of this object.
     * The {@code @Override} annotation indicates that this is an implementation
     * of a method from a superclass or interface.
     *
     * @return a new, non-null instance of {@link RaBitEstimator}
     */
    @Nonnull
    @Override
    public RaBitEstimator estimator() {
        return new RaBitEstimator(metric, numExBits);
    }

    /**
     * Encodes a given {@link RealVector} into its corresponding encoded representation.
     * <p>
     * This method overrides the parent's {@code encode} method. It delegates the
     * core encoding logic to an internal helper method and returns the final
     * {@link EncodedRealVector}.
     *
     * @param data the {@link RealVector} to be encoded; must not be null.
     *
     * @return the resulting {@link EncodedRealVector}, guaranteed to be non-null.
     */
    @Nonnull
    @Override
    public EncodedRealVector encode(@Nonnull final RealVector data) {
        return encodeInternal(data).getEncodedVector();
    }

    /**
     * Encodes a real-valued vector into a quantized representation.
     * <p>
     * This is an internal method that performs the core encoding logic. It first
     * generates a base code using {@link #exBitsCode(RealVector)}, then incorporates
     * sign information to create the final code. It precomputes various geometric
     * properties (norms, dot products) of the original vector and its quantized
     * counterpart to calculate metric-specific scaling and error factors. These
     * factors are used for efficient distance calculations with the encoded vector.
     *
     * @param data the real-valued vector to be encoded. Must not be null.
     * @return a {@code Result} object containing the {@link EncodedRealVector} and
     * other intermediate values from the encoding process. The result is never null.
     *
     * @throws IllegalArgumentException if the configured {@code metric} is not supported for encoding.
     */
    @Nonnull
    @VisibleForTesting
    Result encodeInternal(@Nonnull final RealVector data) {
        final int dims = data.getNumDimensions();

        QuantizeExResult base = exBitsCode(data);
        int[] signedCode = base.code;
        double ipInv = base.ipNormInv;

        int[] totalCode = new int[dims];
        for (int i = 0; i < dims; i++) {
            int sgn = (data.getComponent(i) >= 0.0) ? +1 : 0;
            totalCode[i] = signedCode[i] + (sgn << numExBits);
        }

        final double cb = -(((1 << numExBits) - 0.5));
        double[] xuCbData = new double[dims];
        for (int i = 0; i < dims; i++) {
            xuCbData[i] = totalCode[i] + cb;
        }
        final RealVector xuCb = new DoubleRealVector(xuCbData);

        // 5) Precompute all needed values
        final double residualL2Sqr = data.dot(data);
        final double residualL2Norm = Math.sqrt(residualL2Sqr);
        final double ipResidualXuCb = data.dot(xuCb);
        final double xuCbNorm = xuCb.l2Norm();
        final double xuCbNormSqr = xuCbNorm * xuCbNorm;

        final double ipResidualXuCbSafe =
                (ipResidualXuCb == 0.0) ? Double.POSITIVE_INFINITY : ipResidualXuCb;

        double tmpError = residualL2Norm * EPS0 *
                Math.sqrt(((residualL2Sqr * xuCbNormSqr) / (ipResidualXuCbSafe * ipResidualXuCbSafe) - 1.0)
                        / (Math.max(1, dims - 1)));

        double fAddEx;
        double fRescaleEx;
        double fErrorEx;

        if (metric == Metric.EUCLIDEAN_SQUARE_METRIC || metric == Metric.EUCLIDEAN_METRIC) {
            fAddEx = residualL2Sqr;
            fRescaleEx = ipInv * (-2.0 * residualL2Norm);
            fErrorEx = 2.0 * tmpError;
        } else if (metric == Metric.DOT_PRODUCT_METRIC) {
            fAddEx = 1.0;
            fRescaleEx = ipInv * (-1.0 * residualL2Norm);
            fErrorEx = tmpError;
        } else {
            throw new IllegalArgumentException("Unsupported metric");
        }

        return new Result(new EncodedRealVector(numExBits, totalCode, fAddEx, fRescaleEx, fErrorEx), base.t, ipInv);
    }

    /**
     * Builds per-dimension extra-bit code using the best {@code t} found by {@link #bestRescaleFactor(RealVector)} and
     * returns the code, {@code t}, and {@code ipNormInv}.
     * @param residual rotated residual vector r.
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
     * @param oAbs absolute values of a L2-normalized residual vector (nonnegative; length = dim)
     * @return quantized levels (ex-bits), the chosen scale t, and ipNormInv
     *         Notes: If the residual is the all-zero vector (or numerically so), this returns zero codes,
     *         {@code t = 0}, and {@code ipNormInv = 1} (benign fallback). Downstream code uses {@code ipNormInv} to
     *         compute {@code fRescaleEx}, etc.
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
     * Calculates the best rescaling factor {@code t} for a given vector of absolute values.
     * <p>
     * This method implements an efficient algorithm to find a scaling factor {@code t}
     * that maximizes an objective function related to the quantization of the input vector.
     * The objective function being maximized is effectively
     * {@code sum(u_i * o_i) / sqrt(sum(u_i^2 + u_i))}, where {@code u_i = floor(t * o_i)}
     * and {@code o_i} are the components of the input vector {@code oAbs}.
     * <p>
     * The algorithm performs a sweep over the scaling factor {@code t}. It uses a
     * min-priority queue to efficiently jump between critical values of {@code t} where
     * the floor of {@code t * o_i} changes for some coordinate {@code i}. The search is
     * bounded within a pre-calculated "tight" range {@code [tStart, tEnd]} to ensure
     * efficiency.
     *
     * @param oAbs The vector of absolute values for which to find the best rescale factor.
     * Components must be non-negative.
     *
     * @return The optimal scaling factor {@code t} that maximizes the objective function,
     * or 0.0 if the input vector is all zeros.
     */
    private double bestRescaleFactor(@Nonnull final RealVector oAbs) {
        final int numDimensions = oAbs.getNumDimensions();

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
        final int[] curOB = new int[numDimensions];
        double sqrDen = numDimensions * 0.25; // Σ (cur^2 + cur) starts from D/4
        double numer = 0.0;
        for (int i = 0; i < numDimensions; i++) {
            int cur = (int) ((tStart * oAbs.getComponent(i)) + EPS);
            curOB[i] = cur;
            sqrDen += (double) cur * cur + cur;
            numer  += (cur + 0.5) * oAbs.getComponent(i);
        }

        // Min-heap keyed by next threshold t at which coordinate "i" increments:
        // t_i(k->k+1) = (curOB[i] + 1) / oAbs[i]

        final PriorityQueue<Node> pq = new PriorityQueue<>(Comparator.comparingDouble(n -> n.t));
        for (int i = 0; i < numDimensions; i++) {
            final double curOAbs = oAbs.getComponent(i);
            if (curOAbs > 0.0) {
                double tNext = (curOB[i] + 1) / curOAbs;
                pq.add(new Node(tNext, i));
            }
        }

        double maxIp = 0.0;
        double bestT = 0.0;

        while (!pq.isEmpty()) {
            final Node node = pq.poll();
            final double curT = node.t;
            final int i = node.idx;

            // increment cur_o_bar[i]
            curOB[i]++;
            final int u = curOB[i];

            // update denominator and numerator:
            // sqrDen += 2*u; numer += oAbs[i]
            sqrDen += 2.0 * u;
            numer  += oAbs.getComponent(i);

            // objective value
            final double curIp = numer / Math.sqrt(sqrDen);
            if (curIp > maxIp) {
                maxIp = curIp;
                bestT = curT;
            }

            // schedule next threshold for this coordinate, unless we've hit max level
            if (u < maxLevel) {
                final double oi = oAbs.getComponent(i);
                final double tNext = (u + 1) / oi;
                if (tNext < tEnd) {
                    pq.add(new Node(tNext, i));
                }
            }
        }

        return bestT;
    }

    /**
     * Computes a new vector containing the element-wise absolute values of the L2-normalized input vector.
     * <p>
     * This operation is equivalent to first normalizing the vector {@code x} by its L2 norm,
     * and then taking the absolute value of each resulting component. If the L2 norm of {@code x}
     * is zero or not finite (e.g., {@link Double#POSITIVE_INFINITY}), a new zero vector of the
     * same dimension is returned.
     *
     * @param x the input vector to be normalized and processed. Must not be null.
     *
     * @return a new {@code RealVector} containing the absolute values of the components of the
     * normalized input vector.
     */
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
