/*
 * PartitionEvaluator.java
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

package com.apple.foundationdb.kmeans;

import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.util.Lens;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Evaluates whether a candidate partitioning should replace a current partitioning. Works
 * symmetrically across splits (more clusters in the candidate), merges (fewer clusters in the
 * candidate), and same-k re-partitionings; the {@code k == 1} case on either side is handled by
 * treating missing per-cluster statistics (separation, low-margin rate) as neutral zero
 * contributions to the composite score and by skipping separation/margin hard rejects when the
 * candidate has fewer than two clusters.
 */
public class PartitionEvaluator {
    private static final Logger logger = LoggerFactory.getLogger(PartitionEvaluator.class);

    @Nonnull
    public static <V> EvaluationResult evaluate(@Nonnull final List<V> currentVectors,
                                                @Nonnull final Partition<?> current,
                                                @Nonnull final List<V> candidateVectors,
                                                @Nonnull final Partition<?> candidate,
                                                @Nonnull final Lens<V, RealVector> vectorLens,
                                                @Nonnull final Parameters parameters) {

        validate(currentVectors, current);
        validate(candidateVectors, candidate);

        final PartitionStats currentStats = evaluatePartition(currentVectors, vectorLens, current, parameters);
        final PartitionStats candidateStats = evaluatePartition(candidateVectors, vectorLens, candidate, parameters);

        final double relativeSseGain =
                (currentStats.sse() - candidateStats.sse()) / Math.max(currentStats.sse(), 1e-12);

        //
        // Symmetric scoring: separation and lowMarginRate are NaN when k < 2; treating them as
        // 0 in the gain formula yields the same numbers as the original split-only code for
        // 1 → N transitions while extending naturally to N → 1 merges and same-k transitions.
        //
        final double separationGain = nanToZero(candidateStats.separation()) - nanToZero(currentStats.separation());
        final double lowMarginPenalty =
                Math.max(0.0d, nanToZero(candidateStats.lowMarginRate()) - nanToZero(currentStats.lowMarginRate()));
        final double imbalancePenalty = Math.max(0.0d, candidateStats.imbalance() - currentStats.imbalance());

        final double scoreGain =
                parameters.alphaSseGain() * relativeSseGain +
                        parameters.betaSeparationGain() * separationGain -
                        parameters.gammaImbalancePenalty() * imbalancePenalty -
                        parameters.deltaLowMarginPenalty() * lowMarginPenalty;

        final String transitionKind = "[" + current.k() + " → " + candidate.k() + "]";

        if (candidateStats.smallestFrac() < parameters.minSmallestFrac()) {
            return invalid(currentStats, candidateStats, relativeSseGain, scoreGain,
                    transitionKind + " smallest cluster too small");
        }
        if (candidateStats.largestFrac() > parameters.maxLargestFrac()) {
            return keepCurrent(currentStats, candidateStats, relativeSseGain, scoreGain,
                    transitionKind + " largest cluster too large");
        }

        //
        // Separation and low-margin are undefined for a single-centroid candidate; the caller can
        // still control whether such a candidate is acceptable through minRelativeSseGain,
        // minScoreGain and minSmallestFrac/maxLargestFrac.
        //
        if (candidate.k() >= 2) {
            if (Double.isNaN(candidateStats.separation()) ||
                    candidateStats.separation() < parameters.minSeparation()) {
                return keepCurrent(currentStats, candidateStats, relativeSseGain, scoreGain,
                        transitionKind + " candidate separation too low");
            }

            if (candidateStats.lowMarginRate() > parameters.maxLowMarginRate()) {
                return keepCurrent(currentStats, candidateStats, relativeSseGain, scoreGain,
                        transitionKind + " candidate low-margin rate too high");
            }
        }

        if (relativeSseGain < parameters.minRelativeSseGain()) {
            return keepCurrent(currentStats, candidateStats, relativeSseGain, scoreGain,
                    transitionKind + " relative SSE gain too small");
        }

        if (scoreGain < parameters.minScoreGain()) {
            return keepCurrent(currentStats, candidateStats, relativeSseGain, scoreGain,
                    transitionKind + " overall gain too small");
        }

        return accept(currentStats, candidateStats, relativeSseGain, scoreGain, "accept candidate partition");
    }

    private static double nanToZero(final double value) {
        return Double.isNaN(value) ? 0.0d : value;
    }

    @Nonnull
    private static EvaluationResult keepCurrent(@Nonnull final PartitionStats currentStats,
                                                @Nonnull final PartitionStats candidateStats,
                                                final double relativeSseGain, final double scoreGain,
                                                @Nonnull final String reason) {
        currentStats.log(logger, "current stats");
        candidateStats.log(logger, "candidate stats");
        logger.error("keep current candidate reason={}, relativeSseGain={}, scoreGain={}",
                reason, relativeSseGain, scoreGain);
        return new EvaluationResult(Decision.KEEP_CURRENT, currentStats, candidateStats,
                relativeSseGain, scoreGain, reason);
    }

    @Nonnull
    private static EvaluationResult invalid(@Nonnull final PartitionStats currentStats,
                                            @Nonnull final PartitionStats candidateStats,
                                            final double relativeSseGain, final double scoreGain,
                                            @Nonnull final String reason) {
        currentStats.log(logger, "current stats");
        candidateStats.log(logger, "candidate stats");
        logger.error("invalid candidate reason={}, relativeSseGain={}, scoreGain={}",
                reason, relativeSseGain, scoreGain);
        return new EvaluationResult(Decision.INVALID_CANDIDATE, currentStats, candidateStats,
                relativeSseGain, scoreGain, reason);
    }

    @Nonnull
    private static EvaluationResult accept(@Nonnull final PartitionStats currentStats,
                                           @Nonnull final PartitionStats candidateStats,
                                           final double relativeSseGain, final double scoreGain,
                                           @Nonnull final String reason) {
        currentStats.log(logger, "current stats");
        candidateStats.log(logger, "candidate stats");
        logger.error("accepted candidate, relativeSseGain={}, scoreGain={}", relativeSseGain, scoreGain);
        return new EvaluationResult(Decision.ACCEPT_CANDIDATE, currentStats, candidateStats,
                relativeSseGain, scoreGain, reason);
    }


    private static void validate(@Nonnull final List<?> vectors,
                                 @Nonnull final Partition<?> partition) {
        if (vectors.isEmpty()) {
            throw new IllegalArgumentException("points must not be empty");
        }
        if (partition.k() <= 0) {
            throw new IllegalArgumentException("partition must have at least one centroid");
        }
        if (partition.assignments().length != vectors.size()) {
            throw new IllegalArgumentException("assignment length mismatch");
        }
        for (int a : partition.assignments()) {
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
        final Estimator estimator = parameters.estimator();
        final int n = vectors.size();
        final int k = partition.k();

        Preconditions.checkArgument(n > 0, "points must not be empty");
        Preconditions.checkArgument(k > 0, "partition must have at least one centroid");
        Preconditions.checkArgument(partition.assignments().length == n,
                "assignment length mismatch");

        int[] childSizes = new int[k];
        @SuppressWarnings({"unchecked"}) final List<Double>[] childRadii = (List<Double>[])new ArrayList<?>[k];
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
            assignedDistances.add(geometricDistance(estimator, v, c));
        }
        final double overallP95 = percentile(assignedDistances, 0.95d);
        final double lowMarginThreshold = computeLowMarginThreshold(parameters, overallP95);

        for (int i = 0; i < n; i++) {
            final RealVector v = vectorLens.getNonnull(vectors.get(i));
            final int own = partition.getAssignment(i);
            final RealVector ownC = partition.getCentroid(own);

            childSizes[own]++;

            sse += distanceForSse(estimator, v, ownC);

            double radiusD = geometricDistance(estimator, v, ownC);
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
                        double ownS = v.clampedDot(ownC);
                        double secondBest = Double.NEGATIVE_INFINITY;
                        for (int j = 0; j < k; j++) {
                            if (j == own) {
                                continue;
                            }
                            secondBest = Math.max(secondBest, v.clampedDot(partition.getCentroid(j)));
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

        double target = (double)n / k;
        double imbalance = 0.0;
        int minSize = Integer.MAX_VALUE;
        int maxSize = Integer.MIN_VALUE;

        for (final int sz : childSizes) {
            final double d = (sz - target);
            imbalance += d * d;
            minSize = Math.min(minSize, sz);
            maxSize = Math.max(maxSize, sz);
        }
        imbalance /= ((double)n * n);

        final double largestFrac = (double)maxSize / n;
        final double smallestFrac = (double)minSize / n;

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
                    double d = geometricDistance(estimator, partition.getCentroid(i), partition.getCentroid(j));
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
            lowMarginRate = (double)lowMarginCount / (double)n;
        }

        return new PartitionStats(k, sse, imbalance, separation, largestFrac, smallestFrac, maxRadius95, medianMargin,
                p10Margin, lowMarginRate);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private static double computeLowMarginThreshold(@Nonnull final Parameters parameters,
                                                    final double overallP95) {
        return switch (parameters.estimator.getMetric()) {
            case COSINE_METRIC -> parameters.lowMarginThreshold > 0.0 ? parameters.lowMarginThreshold : 0.02;
            default -> parameters.lowMarginThreshold > 0.0 ? parameters.lowMarginThreshold : 0.05 * overallP95;
        };
    }

    private static double geometricDistance(@Nonnull final Estimator estimator, @Nonnull final RealVector a,
                                            @Nonnull final RealVector b) {
        return switch (estimator.getMetric()) {
            case COSINE_METRIC, EUCLIDEAN_METRIC -> estimator.distance(a, b);
            default -> throw new UnsupportedOperationException("metric is not supported");
        };
    }

    private static double distanceForSse(@Nonnull final Estimator estimator, @Nonnull final RealVector v,
                                         @Nonnull final RealVector c) {
        return switch (estimator.getMetric()) {
            case COSINE_METRIC -> 2.0d * estimator.distance(v, c);
            case EUCLIDEAN_METRIC -> v.subtract(c).l2SquaredNorm();
            default -> throw new UnsupportedOperationException("metric is not supported");
        };
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
        final int lo = (int)Math.floor(rank);
        final int hi = (int)Math.ceil(rank);
        if (lo == hi) {
            return copy.get(lo);
        }

        double w = rank - lo;
        return copy.get(lo) * (1.0 - w) + copy.get(hi) * w;
    }

    public enum Decision {
        KEEP_CURRENT,
        ACCEPT_CANDIDATE,
        INVALID_CANDIDATE
    }

    /**
     * A partition of a point set into {@code k} clusters, as passed to {@link #evaluate}. Each
     * vector is assigned to exactly one centroid via the {@code assignments} array, where
     * {@code assignments[i]} is the index into {@code centroids} that owns
     * {@code vectors.get(i)}.
     *
     * @param <V> the type of the centroid representation
     * @param centroids the cluster centroids (one per cluster, size determines {@code k})
     * @param vectorLens lens for extracting a {@link RealVector} from a centroid of type {@code V}
     * @param assignments per-vector cluster assignment; {@code assignments[i]} is the centroid index
     *        for the i-th vector in the corresponding vector list
     */
    public record Partition<V>(@Nonnull List<V> centroids,
                               @Nonnull Lens<V, RealVector> vectorLens,
                               @Nonnull int[] assignments) {

        @Nonnull
        public RealVector getCentroid(final int index) {
            return vectorLens.getNonnull(centroids.get(index));
        }

        public int getAssignment(final int index) {
            return assignments[index];
        }

        @SuppressWarnings("checkstyle:MethodName")
        public int k() {
            return centroids.size();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Partition<?> that = (Partition<?>)o;
            return Objects.equals(centroids, that.centroids) &&
                    Objects.equals(vectorLens, that.vectorLens) &&
                    Arrays.equals(assignments, that.assignments);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(centroids, vectorLens);
            result = 31 * result + Arrays.hashCode(assignments);
            return result;
        }
    }

    /**
     * Quality statistics computed for a partitioning (current or candidate). Used by the evaluator to
     * decide whether a candidate repartitioning improves upon the current layout.
     *
     * @param k number of clusters in this partitioning
     * @param sse total sum of squared distances from each vector to its assigned centroid
     * @param imbalance measure of how unevenly vectors are distributed across clusters (0 = perfectly balanced)
     * @param separation average inter-centroid distance normalized by cluster radii; higher values indicate
     *        better-separated clusters; {@link Double#NaN} when {@code k < 2}
     * @param largestFrac fraction of all vectors assigned to the largest cluster
     * @param smallestFrac fraction of all vectors assigned to the smallest cluster
     * @param maxRadius95 95th percentile of assigned distances across all clusters; used as a scale reference
     *        for margin thresholds
     * @param medianMargin median assignment margin across all vectors; the margin is the difference between
     *        a vector's distance to its second-nearest centroid and its nearest centroid;
     *        {@link Double#NaN} when {@code k < 2}
     * @param p10Margin 10th percentile of assignment margins; low values indicate many vectors near cluster
     *        boundaries; {@link Double#NaN} when {@code k < 2}
     * @param lowMarginRate fraction of vectors whose assignment margin falls below the configured threshold;
     *        {@code 0.0} when {@code k < 2}
     */
    @SuppressWarnings("checkstyle:MemberName")
    public record PartitionStats(int k, double sse, double imbalance, double separation, double largestFrac,
                                 double smallestFrac, double maxRadius95, double medianMargin, double p10Margin,
                                 double lowMarginRate) {
        public void log(@Nonnull final Logger logger, @Nonnull final String messagePrefix) {
            if (logger.isErrorEnabled()) {
                logger.error("{} k={}, sse={}, imbalance={}, separation={}, largestFrac={}, smallestFrac={}" +
                                ", maxRadius95={}, medianMargin={}, p10Margin={}, lowMarginRate={}",
                        messagePrefix, k, sse, imbalance, separation, largestFrac, smallestFrac,
                        maxRadius95, medianMargin, p10Margin, lowMarginRate);
            }
        }
    }

    /**
     * Tuning parameters that control when a candidate repartitioning is accepted or rejected, and
     * how the composite quality score is computed.
     * <p>
     * The {@code minSmallestFrac} and {@code maxLargestFrac} thresholds apply to the candidate
     * regardless of {@code k}; for a single-cluster candidate (k == 1) both
     * {@code smallestFrac} and {@code largestFrac} are trivially {@code 1.0}, so callers should
     * keep {@code minSmallestFrac <= 1.0} and {@code maxLargestFrac >= 1.0} if they want merges
     * to a single cluster to be admissible. Callers should pick {@code minSmallestFrac} and
     * {@code maxLargestFrac} based on their transition (e.g. tighter for an initial 1 → 2 split,
     * looser for 2 → 3 or for merges).
     *
     * @param estimator the distance estimator used for all distance computations
     * @param minRelativeSseGain minimum relative SSE (sum of squared errors) improvement required;
     *        candidates with less improvement are rejected. May be negative if the caller wants to
     *        accept some SSE increase (e.g. for merges).
     * @param minSeparation minimum inter-cluster separation required; not checked when the candidate
     *        has fewer than two clusters
     * @param maxLowMarginRate maximum fraction of vectors with low assignment margin; not checked
     *        when the candidate has fewer than two clusters
     * @param minSmallestFrac minimum fraction of vectors in the candidate's smallest cluster;
     *        candidates that violate this are reported as {@link Decision#INVALID_CANDIDATE}
     * @param maxLargestFrac maximum fraction of vectors in the candidate's largest cluster;
     *        candidates that violate this are reported as {@link Decision#KEEP_CURRENT}. Use
     *        {@code 1.0} to disable this check.
     * @param lowMarginThreshold distance threshold below which a vector's assignment margin is
     *        considered "low"; if non-positive, a metric-dependent default is used
     *        (0.02 for cosine, 5% of p95 for L2)
     * @param alphaSseGain weight for the SSE gain component in the composite score
     * @param betaSeparationGain weight for the separation gain component in the composite score
     * @param gammaImbalancePenalty weight for the imbalance penalty in the composite score
     * @param deltaLowMarginPenalty weight for the low-margin-rate penalty in the composite score
     * @param minScoreGain minimum composite score improvement the candidate must achieve over the
     *        current partitioning to be accepted
     */
    public record Parameters(@Nonnull Estimator estimator, double minRelativeSseGain, double minSeparation,
                             double maxLowMarginRate, double minSmallestFrac, double maxLargestFrac,
                             double lowMarginThreshold, double alphaSseGain, double betaSeparationGain,
                             double gammaImbalancePenalty, double deltaLowMarginPenalty, double minScoreGain) {
        public Parameters(@Nonnull final Estimator estimator) {
            this(estimator,
                    0.10d,
                    0.3d,
                    0.25d,
                    0.015d,
                    1.0d,
                    -1.0d,
                    1.0d,
                    0.5d,
                    1.0d,
                    0.75d,
                    0.05);
        }
    }

    /**
     * The outcome of evaluating a candidate partitioning against the current layout. Contains the
     * decision (accept, keep current, or invalid), the statistics for both partitionings, and the
     * computed quality metrics that led to the decision.
     *
     * @param decision the evaluator's decision for this candidate
     * @param currentStats quality statistics of the current (existing) partitioning
     * @param candidateStats quality statistics of the proposed candidate partitioning
     * @param relativeSseGain relative improvement in SSE: {@code (currentSSE - candidateSSE) / currentSSE}
     * @param scoreGain composite quality score difference between candidate and current
     * @param reason human-readable explanation of why this decision was made
     */
    public record EvaluationResult(@Nonnull Decision decision, @Nonnull PartitionStats currentStats,
                                   @Nonnull PartitionStats candidateStats, double relativeSseGain, double scoreGain,
                                   String reason) {
    }
}
