/*
 * SplitMergeEvaluator.java
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

import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.util.Lens;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class SplitMergeEvaluator {
    private static final Logger log = LoggerFactory.getLogger(SplitMergeEvaluator.class);

    @Nonnull
    public static <V> UpgradeResult evaluateUpgrade(@Nonnull final List<V> currentVectors,
                                                    @Nonnull final Partition<?> current,
                                                    @Nonnull final List<V> candidateVectors,
                                                    @Nonnull final Partition<?> candidate,
                                                    @Nonnull final Lens<V, RealVector> vectorLens,
                                                    @Nonnull final Parameters parameters) {

        validate(currentVectors, current);
        validate(candidateVectors, candidate);

        final PartitionStats currentStats = evaluatePartition(currentVectors, vectorLens, current, parameters);
        final PartitionStats candidateStats = evaluatePartition(candidateVectors, vectorLens, candidate, parameters);

        // Candidate-specific hard rejects
        if (candidate.k() == 2) {
            if (candidateStats.getSmallestFrac() < parameters.getMinSmallestFracFor2()) {
                return reject(currentStats, candidateStats, "candidate 2-way split too imbalanced");
            }
        } else if (candidate.k() == 3) {
            if (candidateStats.getSmallestFrac() < parameters.getMinSmallestFracFor3()) {
                return reject(currentStats, candidateStats, "candidate 3-way has tiny child");
            }
            if (candidateStats.getLargestFrac() > parameters.getMaxLargestFracFor3()) {
                return reject(currentStats, candidateStats, "candidate 3-way largest child too large");
            }
        }

        if (Double.isNaN(candidateStats.getSeparation()) ||
                candidateStats.getSeparation() < parameters.getMinSeparation()) {
            return reject(currentStats, candidateStats, "candidate separation too low");
        }

        if (candidateStats.getLowMarginRate() > parameters.getMaxLowMarginRate()) {
            return reject(currentStats, candidateStats, "candidate low-margin rate too high");
        }

        final double relativeSseGain =
                (currentStats.getSse() - candidateStats.getSse()) / Math.max(currentStats.getSse(), 1e-12);

        if (relativeSseGain < parameters.getMinRelativeSseGain()) {
            return reject(currentStats, candidateStats, "relative SSE gain too small");
        }

        final double scoreGain;
        if (current.k() == 1 && candidate.k() == 2) {
            // For 1 -> 2, current separation/margins are undefined, so score only
            // from the candidate's absolute quality plus SSE gain.
            scoreGain =
                    parameters.getAlphaSseGain() * relativeSseGain +
                            parameters.getBetaSeparationGain() * candidateStats.getSeparation() -
                            parameters.getGammaImbalancePenalty() * candidateStats.getImbalance() -
                            parameters.getDeltaLowMarginPenalty() * candidateStats.getLowMarginRate();
        } else {
            // For 2 -> 3, compare candidate against current on routing-oriented metrics.
            double separationGain;
            if (Double.isNaN(currentStats.getSeparation())) {
                separationGain = 0.0;
            } else {
                separationGain = candidateStats.getSeparation() - currentStats.getSeparation();
            }

            double lowMarginPenalty =
                    Math.max(0.0, candidateStats.getLowMarginRate() - currentStats.getLowMarginRate());

            double imbalancePenalty =
                    Math.max(0.0, candidateStats.getImbalance() - currentStats.getImbalance());

            scoreGain =
                    parameters.getAlphaSseGain() * relativeSseGain +
                            parameters.getBetaSeparationGain() * separationGain -
                            parameters.getGammaImbalancePenalty() * imbalancePenalty -
                            parameters.getDeltaLowMarginPenalty() * lowMarginPenalty;
        }

        if (scoreGain < parameters.getMinScoreGain()) {
            return reject(currentStats, candidateStats, "overall gain too small");
        }

        return new UpgradeResult(Decision.ACCEPT_CANDIDATE, currentStats, candidateStats, relativeSseGain,
                scoreGain, "accept candidate partition");
    }

    private static UpgradeResult reject(@Nonnull final PartitionStats currentStats, @Nonnull final PartitionStats candidateStats,
                                        @Nonnull final String reason) {
        double relativeSseGain = (currentStats.getSse() - candidateStats.getSse()) / Math.max(currentStats.getSse(), 1e-12);
        return new UpgradeResult(Decision.KEEP_CURRENT, currentStats, candidateStats, relativeSseGain,
                Double.NEGATIVE_INFINITY, reason);
    }

    private static void validate(@Nonnull final List<?> vectors,
                                 @Nonnull final Partition<?> partition) {
        if (vectors.isEmpty()) {
            throw new IllegalArgumentException("points must not be empty");
        }
        if (partition.k() <= 0) {
            throw new IllegalArgumentException("partition must have at least one centroid");
        }
        if (partition.getAssignments().length != vectors.size()) {
            throw new IllegalArgumentException("assignment length mismatch");
        }
        for (int a : partition.getAssignments()) {
            if (a < 0 || a >= partition.k()) {
                throw new IllegalArgumentException("invalid assignment: " + a);
            }
        }
    }

    @Nonnull
    private static <V> PartitionStats evaluatePartition(@Nonnull final List<V> vectors,
                                                        @Nonnull final Lens<V, RealVector> vectorLens,
                                                        @Nonnull final Partition<?> partition,
                                                        @Nonnull final Parameters parameters) {
        final Estimator estimator = parameters.estimator;
        final int n = vectors.size();
        final int k = partition.k();

        Preconditions.checkArgument(n > 0, "points must not be empty");
        Preconditions.checkArgument(k > 0, "partition must have at least one centroid");
        Preconditions.checkArgument(partition.getAssignments().length == n,
                "assignment length mismatch");

        int[] childSizes = new int[k];
        @SuppressWarnings({"unchecked"})
        final List<Double>[] childRadii = (List<Double>[])new ArrayList<?>[k];
        for (int i = 0; i < k; i++) {
            childRadii[i] = new ArrayList<>();
        }

        final List<Double> margins = new ArrayList<>(n);
        final List<Double> assignedDistances = new ArrayList<>(n);

        double sse = 0.0;

        // First pass to support L2 threshold derivation.
        for (int i = 0; i < n; i++) {
            final int own = partition.getAssignment(i);
            Preconditions.checkArgument(own >= 0 && own < k,
                    "invalid assignment at index " + i + ": " + own);

            final RealVector v = vectorLens.getNonnull(vectors.get(i));
            final RealVector c = partition.getCentroid(own);
            assignedDistances.add(estimator.distance(v, c));
        }
        final double overallP95 = percentile(assignedDistances, 0.95d);
        final double lowMarginThreshold = computeLowMarginThreshold(parameters, overallP95);

        for (int i = 0; i < n; i++) {
            final RealVector v = vectorLens.getNonnull(vectors.get(i));
            final int own = partition.getAssignment(i);
            final RealVector ownC = partition.getCentroid(own);

            childSizes[own]++;

            sse += pointToCentroidDistanceForSse(estimator, v, ownC);

            double radiusD = estimator.distance(v, ownC);
            childRadii[own].add(radiusD);

            if (k >= 2) {
                double margin;

                switch (estimator.getMetric()) {
                    case EUCLIDEAN_METRIC: {
                        double ownD = estimator.distance(v, ownC);
                        double secondBest = Double.POSITIVE_INFINITY;
                        for (int j = 0; j < k; j++) {
                            if (j == own) {
                                continue;
                            }
                            secondBest = Math.min(secondBest, estimator.distance(v, partition.getCentroid(j)));
                        }
                        margin = secondBest - ownD;
                        break;
                    }
                    case COSINE_METRIC: {
                        double ownS = v.dot(ownC);
                        double secondBest = Double.NEGATIVE_INFINITY;
                        for (int j = 0; j < k; j++) {
                            if (j == own) {
                                continue;
                            }
                            secondBest = Math.max(secondBest, v.dot(partition.getCentroid(j)));
                        }
                        margin = ownS - secondBest;
                        break;
                    }

                    default:
                        throw new UnsupportedOperationException("metric currently unsupported.");
                }
                margins.add(margin);
            }
        }

        double target = (double) n / k;
        double imbalance = 0.0;
        int minSize = Integer.MAX_VALUE;
        int maxSize = Integer.MIN_VALUE;

        for (final int sz : childSizes) {
            final double d = (sz - target);
            imbalance += d * d;
            minSize = Math.min(minSize, sz);
            maxSize = Math.max(maxSize, sz);
        }
        imbalance /= ((double) n * n);

        final double largestFrac = (double) maxSize / n;
        final double smallestFrac = (double) minSize / n;

        double maxRadius95 = 0.0;
        for (int i = 0; i < k; i++) {
            if (!childRadii[i].isEmpty()) {
                maxRadius95 = Math.max(maxRadius95, percentile(childRadii[i], 0.95d));
            }
        }

        final double separation;
        final double medianMargin;
        final double p10Margin;
        final double lowMarginRate;

        if (k < 2) {
            // These concepts are undefined for a single centroid partition.
            separation = Double.NaN;
            medianMargin = Double.NaN;
            p10Margin = Double.NaN;
            lowMarginRate = 0.0;
        } else {
            double minCentroidDistance = Double.POSITIVE_INFINITY;
            for (int i = 0; i < k; i++) {
                for (int j = i + 1; j < k; j++) {
                    double d =
                            estimator.distance(partition.getCentroid(i),
                                    partition.getCentroid(j));
                    minCentroidDistance = Math.min(minCentroidDistance, d);
                }
            }

            separation = minCentroidDistance / Math.max(maxRadius95, 1e-12);

            medianMargin = percentile(margins, 0.5d);
            p10Margin = percentile(margins, 0.1d);

            int lowMarginCount = 0;
            for (double m : margins) {
                if (m < lowMarginThreshold) {
                    lowMarginCount++;
                }
            }
            lowMarginRate = (double) lowMarginCount / (double) n;
        }

        return new PartitionStats(k, sse, imbalance, separation, largestFrac, smallestFrac, maxRadius95, medianMargin,
                p10Margin, lowMarginRate);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private static double computeLowMarginThreshold(@Nonnull final Parameters parameters,
                                                    final double overallP95) {
        switch (parameters.estimator.getMetric()) {
            case COSINE_METRIC:
                return parameters.lowMarginThreshold > 0.0 ? parameters.lowMarginThreshold : 0.02;
            default:
                return parameters.lowMarginThreshold > 0.0 ? parameters.lowMarginThreshold : 0.05 * overallP95;
        }
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private static double pointToCentroidDistanceForSse(@Nonnull final Estimator estimator, @Nonnull final RealVector v,
                                                        @Nonnull final RealVector c) {
        switch (estimator.getMetric()) {
            case COSINE_METRIC:
                return 2.0 - 2.0 * v.dot(c);
            default:
                return v.subtract(c).l2SquaredNorm();
        }
    }

    private static double percentile(@Nonnull final List<Double> values, double p) {
        if (values.isEmpty()) {
            return Double.NaN;
        }
        final List<Double> copy = new ArrayList<>(values);
        copy.sort(Double::compare);
        if (copy.size() == 1) {
            return copy.get(0);
        }

        final double rank = p * (copy.size() - 1);
        final int lo = (int) Math.floor(rank);
        final int hi = (int) Math.ceil(rank);
        if (lo == hi) {
            return copy.get(lo);
        }

        double w = rank - lo;
        return copy.get(lo) * (1.0 - w) + copy.get(hi) * w;
    }

    public enum Decision {
        KEEP_CURRENT,
        ACCEPT_CANDIDATE
    }

    /**
     * A partition of the SAME point set passed to evaluateUpgrade(...).
     * assignment[i] tells which centroid owns points.get(i).
     * @param <V> type parameter of the vector type
     */
    public static final class Partition<V> {
        @Nonnull
        private final List<V> centroids;   // size k
        @Nonnull
        private final Lens<V, RealVector> vectorLens;
        @Nonnull
        private final int[] assignments;      // over the same vectors

        public Partition(@Nonnull final List<V> centroids,
                         @Nonnull final Lens<V, RealVector> vectorLens,
                         @Nonnull final int[] assignment) {
            this.centroids = centroids;
            this.vectorLens = vectorLens;
            this.assignments = assignment;
        }

        @Nonnull
        public RealVector getCentroid(final int index) {
            return vectorLens.getNonnull(centroids.get(index));
        }

        @Nonnull
        public int[] getAssignments() {
            return assignments;
        }

        public int getAssignment(final int index) {
            return assignments[index];
        }

        @SuppressWarnings("checkstyle:MethodName")
        public int k() {
            return centroids.size();
        }
    }

    @SuppressWarnings("checkstyle:MemberName")
    public static final class PartitionStats {
        private final int k;
        private final double sse;
        private final double imbalance;
        private final double separation;
        private final double largestFrac;
        private final double smallestFrac;
        private final double maxRadius95;
        private final double medianMargin;
        private final double p10Margin;
        private final double lowMarginRate;

        public PartitionStats(final int k,
                              final double sse,
                              final double imbalance,
                              final double separation,
                              final double largestFrac,
                              final double smallestFrac,
                              final double maxRadius95,
                              final double medianMargin,
                              final double p10Margin,
                              final double lowMarginRate) {
            this.k = k;
            this.sse = sse;
            this.imbalance = imbalance;
            this.separation = separation;
            this.largestFrac = largestFrac;
            this.smallestFrac = smallestFrac;
            this.maxRadius95 = maxRadius95;
            this.medianMargin = medianMargin;
            this.p10Margin = p10Margin;
            this.lowMarginRate = lowMarginRate;
        }

        public int getK() {
            return k;
        }

        public double getSse() {
            return sse;
        }

        public double getImbalance() {
            return imbalance;
        }

        public double getSeparation() {
            return separation;
        }

        public double getLargestFrac() {
            return largestFrac;
        }

        public double getSmallestFrac() {
            return smallestFrac;
        }

        public double getMaxRadius95() {
            return maxRadius95;
        }

        public double getMedianMargin() {
            return medianMargin;
        }

        public double getP10Margin() {
            return p10Margin;
        }

        public double getLowMarginRate() {
            return lowMarginRate;
        }

        public void log(@Nonnull final Logger logger, @Nonnull final String messagePrefix) {
            if (logger.isErrorEnabled()) {
                logger.error("{} k={}, sse={}, imbalance={}, separation={}, largestFrac={}, smallestFrac={}" +
                                ", maxRadius95={}, medianMargin={}, p10Margin={}, lowMarginRate={}",
                        messagePrefix, k, sse, imbalance, separation, largestFrac, smallestFrac,
                        maxRadius95, medianMargin, p10Margin, lowMarginRate);
            }
        }
    }

    public static final class Parameters {
        @Nonnull
        private final Estimator estimator;

        // Candidate hard rejects
        private final double minRelativeSseGain;
        private final double minSeparation;
        private final double maxLowMarginRate;

        // Candidate size sanity
        private final double minSmallestFracFor2; // when candidate k = 2
        private final double minSmallestFracFor3; // when candidate k = 3
        private final double maxLargestFracFor3;  // when candidate k = 3

        // Margin threshold
        // COSINE_NORMALIZED: if <= 0, default 0.02
        // L2: if <= 0, use 5% of overall p95 assigned distance
        private final double lowMarginThreshold;

        // Score weights for comparing current vs candidate
        private final double alphaSseGain;
        private final double betaSeparationGain;
        private final double gammaImbalancePenalty;
        private final double deltaLowMarginPenalty;

        // Candidate must beat current by at least this much
        private final double minScoreGain;

        public Parameters(@Nonnull final Estimator estimator) {
            this(estimator,
                    0.10d,
                    1.25d,
                    0.25d,
                    0.30d,
                    0.15d,
                    0.55d,
                    -1.0d,
                    1.0d,
                    0.5d,
                    1.0d,
                    0.75d,
                    0.05);
        }

        public Parameters(@Nonnull final Estimator estimator, final double minRelativeSseGain, final double minSeparation,
                          final double maxLowMarginRate, final double minSmallestFracFor2, final double minSmallestFracFor3,
                          final double maxLargestFracFor3, final double lowMarginThreshold, final double alphaSseGain,
                          final double betaSeparationGain, final double gammaImbalancePenalty,
                          final double deltaLowMarginPenalty, final double minScoreGain) {
            this.estimator = estimator;
            this.minRelativeSseGain = minRelativeSseGain;
            this.minSeparation = minSeparation;
            this.maxLowMarginRate = maxLowMarginRate;
            this.minSmallestFracFor2 = minSmallestFracFor2;
            this.minSmallestFracFor3 = minSmallestFracFor3;
            this.maxLargestFracFor3 = maxLargestFracFor3;
            this.lowMarginThreshold = lowMarginThreshold;
            this.alphaSseGain = alphaSseGain;
            this.betaSeparationGain = betaSeparationGain;
            this.gammaImbalancePenalty = gammaImbalancePenalty;
            this.deltaLowMarginPenalty = deltaLowMarginPenalty;
            this.minScoreGain = minScoreGain;
        }

        @Nonnull
        public Estimator getEstimator() {
            return estimator;
        }

        public double getMinRelativeSseGain() {
            return minRelativeSseGain;
        }

        public double getMinSeparation() {
            return minSeparation;
        }

        public double getMaxLowMarginRate() {
            return maxLowMarginRate;
        }

        public double getMinSmallestFracFor2() {
            return minSmallestFracFor2;
        }

        public double getMinSmallestFracFor3() {
            return minSmallestFracFor3;
        }

        public double getMaxLargestFracFor3() {
            return maxLargestFracFor3;
        }

        public double getLowMarginThreshold() {
            return lowMarginThreshold;
        }

        public double getAlphaSseGain() {
            return alphaSseGain;
        }

        public double getBetaSeparationGain() {
            return betaSeparationGain;
        }

        public double getGammaImbalancePenalty() {
            return gammaImbalancePenalty;
        }

        public double getDeltaLowMarginPenalty() {
            return deltaLowMarginPenalty;
        }

        public double getMinScoreGain() {
            return minScoreGain;
        }
    }

    public static final class UpgradeResult {
        @Nonnull
        private final Decision decision;
        @Nonnull
        private final PartitionStats currentStats;
        @Nonnull
        private final PartitionStats candidateStats;

        private final double relativeSseGain;
        private final double scoreGain;
        private final String reason;

        public UpgradeResult(@Nonnull final Decision decision,
                             @Nonnull final PartitionStats currentStats,
                             @Nonnull final PartitionStats candidateStats,
                             final double relativeSseGain,
                             final double scoreGain,
                             final String reason) {
            this.decision = decision;
            this.currentStats = currentStats;
            this.candidateStats = candidateStats;
            this.relativeSseGain = relativeSseGain;
            this.scoreGain = scoreGain;
            this.reason = reason;
        }

        @Nonnull
        public Decision getDecision() {
            return decision;
        }

        @Nonnull
        public PartitionStats getCurrentStats() {
            return currentStats;
        }

        @Nonnull
        public PartitionStats getCandidateStats() {
            return candidateStats;
        }

        public double getRelativeSseGain() {
            return relativeSseGain;
        }

        public double getScoreGain() {
            return scoreGain;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return "UpgradeResult{" +
                    "decision=" + decision +
                    ", currentStats=" + currentStats +
                    ", candidateStats=" + candidateStats +
                    ", relativeSseGain=" + relativeSseGain +
                    ", scoreGain=" + scoreGain +
                    ", reason='" + reason + '\'' +
                    '}';
        }

        public void log(@Nonnull final Logger logger) {
            currentStats.log(logger, "current stats");
            candidateStats.log(logger, "candidate stats");

            if (logger.isErrorEnabled()) {
                log.error("SPLIT evaluation result: decision={}, relativeSseGain={}, scoreGain={}, reason={}",
                        decision, relativeSseGain, scoreGain, reason);
            }
        }
    }
}

