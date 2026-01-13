/*
 * OrderQuality.java
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

package com.apple.foundationdb.async.hnsw;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"checkstyle:MemberName", "NewClassNamingConvention"})
public final class OrderQuality {

    private OrderQuality() {
        // no instantiations
    }

    /**
     * Scores how well unique-ID list s maps into unique-ID list u near a known start index x,
     * given these guarantees:
     *  - u has no duplicates
     *  - s has no duplicates
     *  - every element in s appears in u
     *
     * The mapping is deterministic: p[i] = position of s[i] in u.
     *
     * Metrics computed:
     *  - Locality vs known x: e[i] = p[i] - (x+i)
     *    * medianShift (robust global offset)
     *    * pctWithinWindow (raw) and pctWithinWindowAfterShift (shift-compensated)
     *    * p90AbsErr / p90AbsResidual, maxAbsErr / maxAbsResidual
     *  - Outliers: |e| > outlierThreshold
     *  - Order: inversion count of p (Kendall-style), normalized to orderScore in [0,1]
     *  - Contiguity: how compact mapped positions are in u
     *    * blockLen = max(p)-min(p)+1
     *    * extrasBetween = blockLen - m
     *    * contigScore = m / blockLen in (0,1]
     *  - Composite quality score in [0,1]
     */
    public static final class Result {
        public final int m;
        public final int n;
        public final int x;

        // Deterministic mapping positions
        public final int minPosInU;
        public final int maxPosInU;
        public final int blockLen;          // maxPosInU - minPosInU + 1
        public final int extrasBetween;     // blockLen - m (0 is ideal)
        public final double contigScore;    // m / blockLen in (0,1]

        // Locality relative to x: e[i] = p[i] - (x+i)
        public final int medianShift;       // median of e[i] (robust global shift)
        public final double pctWithinWindow;
        public final double pctWithinWindowAfterShift;
        public final int p90AbsErr;
        public final int p90AbsResidual;
        public final int maxAbsErr;
        public final int maxAbsResidual;

        // Outliers (based on raw error)
        public final double outlierRate;

        // Order
        public final long inversions;
        public final double orderScore;     // [0,1]

        // Component scores used in composite
        public final double localScore;     // [0,1]
        public final double outlierScore;   // [0,1]
        public final double quality;        // [0,1]

        private Result(
                int m, int n, int x,
                int minPosInU, int maxPosInU, int blockLen, int extrasBetween, double contigScore,
                int medianShift,
                double pctWithinWindow, double pctWithinWindowAfterShift,
                int p90AbsErr, int p90AbsResidual,
                int maxAbsErr, int maxAbsResidual,
                double outlierRate,
                long inversions, double orderScore,
                double localScore, double outlierScore,
                double quality
        ) {
            this.m = m;
            this.n = n;
            this.x = x;

            this.minPosInU = minPosInU;
            this.maxPosInU = maxPosInU;
            this.blockLen = blockLen;
            this.extrasBetween = extrasBetween;
            this.contigScore = contigScore;

            this.medianShift = medianShift;
            this.pctWithinWindow = pctWithinWindow;
            this.pctWithinWindowAfterShift = pctWithinWindowAfterShift;
            this.p90AbsErr = p90AbsErr;
            this.p90AbsResidual = p90AbsResidual;
            this.maxAbsErr = maxAbsErr;
            this.maxAbsResidual = maxAbsResidual;

            this.outlierRate = outlierRate;

            this.inversions = inversions;
            this.orderScore = orderScore;

            this.localScore = localScore;
            this.outlierScore = outlierScore;
            this.quality = quality;
        }

        public int getM() {
            return m;
        }

        public int getN() {
            return n;
        }

        public int getX() {
            return x;
        }

        public int getMinPosInU() {
            return minPosInU;
        }

        public int getMaxPosInU() {
            return maxPosInU;
        }

        public int getBlockLen() {
            return blockLen;
        }

        public int getExtrasBetween() {
            return extrasBetween;
        }

        public double getContigScore() {
            return contigScore;
        }

        public int getMedianShift() {
            return medianShift;
        }

        public double getPctWithinWindow() {
            return pctWithinWindow;
        }

        public double getPctWithinWindowAfterShift() {
            return pctWithinWindowAfterShift;
        }

        public int getP90AbsErr() {
            return p90AbsErr;
        }

        public int getP90AbsResidual() {
            return p90AbsResidual;
        }

        public int getMaxAbsErr() {
            return maxAbsErr;
        }

        public int getMaxAbsResidual() {
            return maxAbsResidual;
        }

        public double getOutlierRate() {
            return outlierRate;
        }

        public long getInversions() {
            return inversions;
        }

        public double getOrderScore() {
            return orderScore;
        }

        public double getLocalScore() {
            return localScore;
        }

        public double getOutlierScore() {
            return outlierScore;
        }

        public double getQuality() {
            return quality;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "m=" + m +
                    ", n=" + n +
                    ", x=" + x +
                    ", minPosInU=" + minPosInU +
                    ", maxPosInU=" + maxPosInU +
                    ", blockLen=" + blockLen +
                    ", extrasBetween=" + extrasBetween +
                    ", contigScore=" + contigScore +
                    ", medianShift=" + medianShift +
                    ", pctWithinWindow=" + pctWithinWindow +
                    ", pctWithinWindowAfterShift=" + pctWithinWindowAfterShift +
                    ", p90AbsErr=" + p90AbsErr +
                    ", p90AbsResidual=" + p90AbsResidual +
                    ", maxAbsErr=" + maxAbsErr +
                    ", maxAbsResidual=" + maxAbsResidual +
                    ", outlierRate=" + outlierRate +
                    ", inversions=" + inversions +
                    ", orderScore=" + orderScore +
                    ", localScore=" + localScore +
                    ", outlierScore=" + outlierScore +
                    ", quality=" + quality +
                    '}';
        }
    }

    /**
     * Score the result.
     *
     * @param okWindow absolute-error threshold for "close enough" (e.g., 100)
     * @param outlierThreshold absolute-error threshold for "mapped elsewhere" (must be > okWindow)
     * @param wLocal weight of locality component in composite
     * @param wOrder weight of order component in composite
     * @param wOutlier weight of outlier component in composite
     * @param wContig weight of contiguity component in composite
     */
    @Nonnull
    public static <T> Result score(@Nonnull final List<T> s, @Nonnull final List<T> u, final int x, final int okWindow,
                                   final int outlierThreshold, final double wLocal, final double wOrder,
                                   final double wOutlier, final double wContig) {
        validateArguments(s, u, x, okWindow, outlierThreshold, wLocal, wOrder, wOutlier, wContig);

        final int m = s.size();
        final int n = u.size();

        // Build pos map for u and validate uniqueness.
        final var posInU = posInU(n, u);

        // Build p and validate s uniqueness + presence in u.
        final int[] p = new int[m];
        final Set<T> seenS = Sets.newHashSet();
        int minPos = Integer.MAX_VALUE;
        int maxPos = Integer.MIN_VALUE;

        for (int i = 0; i < m; i++) {
            T id = s.get(i);
            if (!seenS.add(id)) {
                throw new IllegalArgumentException("s contains duplicate element: " + id);
            }
            final Integer pos = posInU.get(id);
            if (pos == null) {
                throw new IllegalArgumentException("s element not found in u: " + id);
            }
            int pi = pos;
            p[i] = pi;
            if (pi < minPos) {
                minPos = pi;
            }
            if (pi > maxPos) {
                maxPos = pi;
            }
        }

        // Contiguity metrics
        final int blockLen = maxPos - minPos + 1;
        final int extrasBetween = blockLen - m;
        final double contigScore = m / (double) blockLen; // in (0,1]

        // Errors relative to known x: e[i] = p[i] - (x+i)
        int[] e = new int[m];
        for (int i = 0; i < m; i++) {
            e[i] = p[i] - (x + i);
        }

        // Robust global shift estimate
        int medianShift = medianOfCopy(e);

        // Compute abs error distributions and rates.
        int within = 0;
        int withinAfter = 0;
        int outliers = 0;
        int maxAbsErr = 0;
        int maxAbsResidual = 0;
        int[] absErr = new int[m];
        int[] absResidual = new int[m];

        for (int i = 0; i < m; i++) {
            int ae = Math.abs(e[i]);
            int ar = Math.abs(e[i] - medianShift);

            absErr[i] = ae;
            absResidual[i] = ar;

            if (ae <= okWindow) {
                within++;
            }
            if (ar <= okWindow) {
                withinAfter++;
            }
            if (ae > outlierThreshold) {
                outliers++;
            }

            if (ae > maxAbsErr) {
                maxAbsErr = ae;
            }
            if (ar > maxAbsResidual) {
                maxAbsResidual = ar;
            }
        }

        final double pctWithinWindow = within / (double) m;
        final double pctWithinWindowAfterShift = withinAfter / (double) m;
        final double outlierRate = outliers / (double) m;

        final int p90AbsErr = percentile(absErr, 0.90);
        final int p90AbsResidual = percentile(absResidual, 0.90);

        // Order via inversion count
        final long inversions = countInversions(p);
        final long maxInv = (m <= 1) ? 0L : ((long) m * (m - 1)) / 2L;
        final double orderScore = clamp01((maxInv == 0L) ? 1.0 : 1.0 - inversions / (double) maxInv);


        // Component scores
        final double p90Component = clamp01(1.0 - (p90AbsResidual / (double) okWindow));
        final double localScore = clamp01(0.7 * pctWithinWindowAfterShift + 0.3 * p90Component);
        final double outlierScore = clamp01(1.0 - outlierRate);

        // Composite quality
        final double wSum = wLocal + wOrder + wOutlier + wContig;
        Verify.verify(wSum > 0);

        final double quality = clamp01((wLocal * localScore + wOrder * orderScore + wOutlier * outlierScore + wContig * contigScore) / wSum);

        return new Result(m, n, x,
                minPos, maxPos, blockLen, extrasBetween, contigScore,
                medianShift,
                pctWithinWindow, pctWithinWindowAfterShift,
                p90AbsErr, p90AbsResidual,
                maxAbsErr, maxAbsResidual,
                outlierRate,
                inversions, orderScore,
                localScore, outlierScore,
                quality
        );
    }

    private static <T> void validateArguments(final List<T> s, final List<T> u, final int x, final int okWindow, final int outlierThreshold, final double wLocal, final double wOrder, final double wOutlier, final double wContig) {
        Objects.requireNonNull(s, "s");
        Objects.requireNonNull(u, "u");
        Preconditions.checkArgument(!s.isEmpty());
        Preconditions.checkArgument(okWindow > 0);
        Preconditions.checkArgument(outlierThreshold > okWindow);
        Preconditions.checkArgument(wLocal >= 0 && wOrder >= 0 && wOutlier >= 0 && wContig >= 0);
        Preconditions.checkArgument(x >= 0 && x < u.size());
    }

    private static <T> @Nonnull Map<T, Integer> posInU(final int n, final List<T> u) {
        Map<T, Integer> posInU = Maps.newHashMapWithExpectedSize(n);
        for (int j = 0; j < u.size(); j++) {
            T id = u.get(j);
            Integer prev = posInU.put(id, j);
            if (prev != null) {
                throw new IllegalArgumentException("u contains duplicate element: " + id);
            }
        }
        return posInU;
    }

    /**
     * Convenience overload: you pass okWindow; outlierThreshold is derived as 5*okWindow.
     * Default weights (tune as needed): local=0.55, order=0.25, outlier=0.10, contig=0.10.
     */
    public static <T> Result score(List<T> s, List<T> u, int x, int okWindow) {
        int outlierThreshold = Math.max(okWindow + 1, 5 * okWindow);
        return score(s, u, x, okWindow, outlierThreshold, 0.55, 0.25, 0.10, 0.10);
    }

    // ---- helpers ----

    private static double clamp01(double v) {
        return v < 0 ? 0 : (v > 1 ? 1 : v);
    }

    private static int medianOfCopy(int[] a) {
        int[] c = Arrays.copyOf(a, a.length);
        Arrays.sort(c);
        return c[c.length / 2]; // lower median for even length
    }

    private static int percentile(int[] values, double q) {
        if (q <= 0.0) {
            return min(values);
        }
        if (q >= 1.0) {
            return max(values);
        }
        int[] c = Arrays.copyOf(values, values.length);
        Arrays.sort(c);
        int idx = (int) Math.ceil(q * c.length) - 1;
        if (idx < 0) {
            idx = 0;
        }
        if (idx >= c.length) {
            idx = c.length - 1;
        }
        return c[idx];
    }

    private static int min(int[] a) {
        int m = Integer.MAX_VALUE;
        for (int v : a) {
            m = Math.min(m, v);
        }
        return m;
    }

    private static int max(int[] a) {
        int m = Integer.MIN_VALUE;
        for (int v : a) {
            m = Math.max(m, v);
        }
        return m;
    }

    private static long countInversions(int[] p) {
        if (p.length <= 1) {
            return 0L;
        }
        int max = 0;
        for (int v : p) {
            if (v > max) {
                max = v;
            }
        }

        Fenwick bit = new Fenwick(max + 1);
        long inv = 0L;
        long seen = 0L;
        for (int v : p) {
            long leq = bit.sumInclusive(v); // count of seen <= v
            inv += (seen - leq);            // count of seen > v
            bit.add(v, 1);
            seen++;
        }
        return inv;
    }

    private static final class Fenwick {
        private final long[] t;

        Fenwick(int size) {
            this.t = new long[size + 1];
        }

        void add(int idx0, long delta) {
            for (int i = idx0 + 1; i < t.length; i += i & -i) {
                t[i] += delta;
            }
        }

        long sumInclusive(int idx0) {
            long res = 0L;
            for (int i = idx0 + 1; i > 0; i -= i & -i) {
                res += t[i];
            }
            return res;
        }
    }

    @Test
    void basicQualityTest() {
        /*
             * u:   X0 X1 A C B D E F G H
             * s:         A B C D E F G H
             * x = 2
             *
             * Only B and C are inverted, but all items are within ±1 of expected.
             */
        List<String> u = List.of(
                "X0", "X1",
                "A", "C", "B", "D", "E", "F", "G", "H"
        );
        List<String> s = List.of("A", "B", "C", "D", "E", "F", "G", "H");

        int x = 2;
        int okWindow = 3;

        OrderQuality.Result r =
                OrderQuality.score(s, u, x, okWindow);

        // Core sanity checks
        assertEquals(8, r.m);
        assertEquals(10, r.n);
        assertEquals(x, r.x);

        // All items are close after shift compensation
        assertEquals(1.0, r.pctWithinWindowAfterShift, 1e-9);

        // Exactly one inversion (B, C)
        assertEquals(1L, r.inversions);
        assertTrue(r.orderScore < 1.0);
        assertTrue(r.orderScore > 0.95);

        // Contiguity is perfect (span is still exactly length m)
        assertEquals(0, r.extrasBetween);
        assertEquals(1.0, r.contigScore, 1e-9);

        // No outliers
        assertEquals(0.0, r.outlierRate, 1e-9);

        // Overall quality should be high but not perfect (due to inversion)
        assertTrue(r.quality < 1.0);
        assertTrue(r.quality > 0.8);
    }
}
