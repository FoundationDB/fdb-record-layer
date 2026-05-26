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

    /**
     * Evaluates a candidate partitioning against the current one and returns whether to accept,
     * keep, or reject it.
     * <p>
     * The decision is made by computing a panel of quality statistics for both partitionings (see
     * {@link PartitionStats}) and combining them into a composite score:
     * <pre>{@code
     * scoreGain = alphaSseGain * relativeSseGain
     *           + betaSeparationGain * separationGain
     *           - gammaImbalancePenalty * imbalancePenalty
     *           - deltaLowMarginPenalty * lowMarginPenalty
     * }</pre>
     * Hard rejects are applied first ({@code minSmallestFrac}, {@code maxLargestFrac}, candidate
     * separation/low-margin thresholds when {@code candidate.k() >= 2}, and the absolute
     * {@code minRelativeSseGain}/{@code minScoreGain} floors); only if all of those pass is the
     * candidate accepted.
     * <p>
     * Symmetric handling: separation and low-margin rate are undefined when {@code k < 2}; this
     * method treats them as {@code 0} in the score formula and skips the corresponding hard
     * rejects when {@code candidate.k() < 2}, so the same logic correctly handles splits, merges
     * (including merges to a single cluster), and same-{@code k} re-partitionings.
     *
     * @param currentVectors the vectors belonging to the current partitioning. Must be non-empty
     * @param current the current partitioning
     * @param candidateVectors the vectors belonging to the candidate partitioning. Often the same
     *                         list as {@code currentVectors} but may differ when the caller is
     *                         re-clustering a different point set
     * @param candidate the candidate partitioning to evaluate
     * @param vectorLens lens that extracts a {@link RealVector} from each {@code currentVectors}
     *                   /{@code candidateVectors} element
     * @param parameters tuning parameters that control thresholds and score weights
     * @param <V> caller's input vector representation
     * @return an {@link EvaluationResult} carrying the decision, both stats, and the metrics
     *         that led to the decision
     */
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

    /**
     * Returns {@code 0.0} if {@code value} is {@link Double#NaN}, otherwise returns the value
     * unchanged. Used by the score formula to absorb the NaN that {@link PartitionStats}
     * reports for separation / low-margin rate when {@code k < 2}.
     */
    private static double nanToZero(final double value) {
        return Double.isNaN(value) ? 0.0d : value;
    }

    /**
     * Builds a {@link Decision#KEEP_CURRENT} {@link EvaluationResult} and logs the decision and
     * both partitions' statistics.
     */
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

    /**
     * Builds a {@link Decision#INVALID_CANDIDATE} {@link EvaluationResult} and logs the decision
     * and both partitions' statistics. Used for candidates that violate a structural hard reject
     * (currently: smallest cluster fraction below {@code minSmallestFrac}).
     */
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

    /**
     * Builds a {@link Decision#ACCEPT_CANDIDATE} {@link EvaluationResult} and logs the decision
     * and both partitions' statistics.
     */
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


    /**
     * Validates the structural well-formedness of a partition against its associated point set.
     * Throws {@link IllegalArgumentException} if any of the following holds: {@code vectors} is
     * empty, {@code partition.k() <= 0}, the assignment array length differs from
     * {@code vectors.size()}, or any assignment entry is outside {@code [0, k)}.
     */
    private static void validate(@Nonnull final List<?> vectors,
                                 @Nonnull final Partition<?> partition) {
        if (vectors.isEmpty()) {
            throw new IllegalArgumentException("vectors must not be empty");
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

    /**
     * Computes the full panel of {@link PartitionStats} for a single partitioning. The work is
     * done in two passes: the first builds the distribution of assigned-distance values so the
     * {@code lowMarginThreshold} can be derived from its 95th percentile; the second accumulates
     * SSE, per-cluster radii and per-vector margins. Inter-centroid separation, median/p10
     * margin, and low-margin rate are computed only when {@code k >= 2} and reported as
     * {@link Double#NaN} (or {@code 0.0} for {@code lowMarginRate}) otherwise.
     *
     * @param vectors the vectors belonging to {@code partition}; must be non-empty
     * @param vectorLens lens that extracts a {@link RealVector} from each {@code vectors} element
     * @param partition the partitioning to evaluate
     * @param parameters supplies the distance estimator and the {@code lowMarginThreshold}
     *                   override (or, if non-positive, signals to derive it from the data)
     * @param <V> caller's input vector representation
     * @return the computed statistics
     */
    @Nonnull
    private static <V> PartitionStats evaluatePartition(@Nonnull final List<V> vectors,
                                                        @Nonnull final Lens<V, RealVector> vectorLens,
                                                        @Nonnull final Partition<?> partition,
                                                        @Nonnull final Parameters parameters) {
        final Estimator estimator = parameters.estimator();
        final int n = vectors.size();
        final int k = partition.k();

        Preconditions.checkArgument(n > 0, "vectors must not be empty");
        Preconditions.checkArgument(k > 0, "partition must have at least one centroid");
        Preconditions.checkArgument(partition.assignments().length == n,
                "assignment length mismatch");

        final List<Double> assigned = assignedDistances(vectors, vectorLens, partition, estimator);
        final double lowMarginThreshold =
                computeLowMarginThreshold(parameters, percentile(assigned, 0.95d));

        final SecondPassResult acc = accumulate(vectors, vectorLens, partition, estimator);
        final SizeStats sizes = summarizeSizes(acc.childSizes(), n);
        final double maxRadius95 = maxRadius95(acc.childRadii());
        final double separation = separation(partition, estimator, maxRadius95);
        final MarginStats margins = marginStats(acc.margins(), lowMarginThreshold, n, k);

        return new PartitionStats(k, acc.sse(), sizes.imbalance(), separation,
                sizes.largestFrac(), sizes.smallestFrac(), maxRadius95,
                margins.median(), margins.p10(), margins.lowRate());
    }

    /** Bundles the outputs of the second accumulation pass. */
    private record SecondPassResult(double sse, @Nonnull int[] childSizes,
                                    @Nonnull List<Double>[] childRadii,
                                    @Nonnull List<Double> margins) {
    }

    /** Cluster-size summary derived from a partition's per-cluster sizes. */
    private record SizeStats(double imbalance, double smallestFrac, double largestFrac) {
    }

    /** Margin-distribution summary; NaN/{@code 0.0} when {@code k < 2}. */
    private record MarginStats(double median, double p10, double lowRate) {
    }

    /**
     * First pass: collects the distance from each vector to its assigned centroid. The resulting
     * distribution drives the data-derived {@code lowMarginThreshold}.
     */
    @Nonnull
    private static <V> List<Double> assignedDistances(@Nonnull final List<V> vectors,
                                                      @Nonnull final Lens<V, RealVector> vectorLens,
                                                      @Nonnull final Partition<?> partition,
                                                      @Nonnull final Estimator estimator) {
        final int n = vectors.size();
        final int k = partition.k();
        final List<Double> distances = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final int own = partition.getAssignment(i);
            Preconditions.checkArgument(own >= 0 && own < k,
                    "invalid assignment at index " + i + ": " + own);
            final RealVector v = vectorLens.getNonnull(vectors.get(i));
            final RealVector c = partition.getCentroid(own);
            distances.add(geometricDistance(estimator, v, c));
        }
        return distances;
    }

    /**
     * Second pass: accumulates SSE, per-cluster sizes and radii, and per-vector margins. Margins
     * are only computed when {@code k >= 2}; otherwise the returned {@code margins} list is empty.
     */
    @Nonnull
    private static <V> SecondPassResult accumulate(@Nonnull final List<V> vectors,
                                                   @Nonnull final Lens<V, RealVector> vectorLens,
                                                   @Nonnull final Partition<?> partition,
                                                   @Nonnull final Estimator estimator) {
        final int n = vectors.size();
        final int k = partition.k();

        final int[] childSizes = new int[k];
        @SuppressWarnings("unchecked") final List<Double>[] childRadii = (List<Double>[]) new ArrayList<?>[k];
        for (int i = 0; i < k; i++) {
            childRadii[i] = new ArrayList<>();
        }
        final List<Double> margins = new ArrayList<>(k >= 2 ? n : 0);
        double sse = 0.0;

        for (int i = 0; i < n; i++) {
            final RealVector v = vectorLens.getNonnull(vectors.get(i));
            final int own = partition.getAssignment(i);
            final RealVector ownC = partition.getCentroid(own);

            childSizes[own]++;
            sse += distanceForSse(estimator, v, ownC);
            childRadii[own].add(geometricDistance(estimator, v, ownC));
            if (k >= 2) {
                margins.add(computeMargin(estimator, partition, v, own));
            }
        }
        return new SecondPassResult(sse, childSizes, childRadii, margins);
    }

    /**
     * Computes the assignment margin for a single vector under the estimator's metric: how much
     * better its assigned centroid is than the second-best alternative. For Euclidean this is
     * {@code distance(secondBest) - distance(own)}; for cosine it is
     * {@code clampedDot(own) - clampedDot(secondBest)}. Larger margins mean more confident
     * assignments. Requires {@code partition.k() >= 2}.
     *
     * @throws UnsupportedOperationException if the estimator's metric is neither
     *         {@code EUCLIDEAN_METRIC} nor {@code COSINE_METRIC}
     */
    private static double computeMargin(@Nonnull final Estimator estimator,
                                        @Nonnull final Partition<?> partition,
                                        @Nonnull final RealVector v,
                                        final int own) {
        final int k = partition.k();
        final RealVector ownC = partition.getCentroid(own);
        return switch (estimator.getMetric()) {
            case EUCLIDEAN_METRIC -> {
                final double ownD = estimator.distance(v, ownC);
                double secondBest = Double.POSITIVE_INFINITY;
                for (int j = 0; j < k; j++) {
                    if (j == own) {
                        continue;
                    }
                    secondBest = Math.min(secondBest, estimator.distance(v, partition.getCentroid(j)));
                }
                yield secondBest - ownD;
            }
            case COSINE_METRIC -> {
                final double ownS = v.clampedDot(ownC);
                double secondBest = Double.NEGATIVE_INFINITY;
                for (int j = 0; j < k; j++) {
                    if (j == own) {
                        continue;
                    }
                    secondBest = Math.max(secondBest, v.clampedDot(partition.getCentroid(j)));
                }
                yield ownS - secondBest;
            }
            default -> throw new UnsupportedOperationException("metric currently unsupported.");
        };
    }

    /**
     * Computes the imbalance (sum of squared deviations from the per-cluster target size,
     * normalized by {@code n^2}) and the smallest/largest per-cluster fractions.
     */
    @Nonnull
    private static SizeStats summarizeSizes(@Nonnull final int[] childSizes, final int n) {
        final int k = childSizes.length;
        final double target = (double) n / k;
        double sumSquaredDiff = 0.0;
        int minSize = Integer.MAX_VALUE;
        int maxSize = Integer.MIN_VALUE;
        for (final int sz : childSizes) {
            final double d = sz - target;
            sumSquaredDiff += d * d;
            minSize = Math.min(minSize, sz);
            maxSize = Math.max(maxSize, sz);
        }
        return new SizeStats(sumSquaredDiff / ((double) n * n),
                (double) minSize / n,
                (double) maxSize / n);
    }

    /**
     * Returns the maximum across clusters of the 95th-percentile assigned distance for that
     * cluster. Empty clusters do not contribute.
     */
    private static double maxRadius95(@Nonnull final List<Double>[] childRadii) {
        double max = 0.0;
        for (final List<Double> radii : childRadii) {
            if (!radii.isEmpty()) {
                max = Math.max(max, percentile(radii, 0.95d));
            }
        }
        return max;
    }

    /**
     * Returns the inter-centroid separation: the minimum pairwise centroid distance divided by
     * {@code maxRadius95} (floored at {@code 1e-12}). Returns {@link Double#NaN} when
     * {@code partition.k() < 2}.
     */
    private static double separation(@Nonnull final Partition<?> partition,
                                     @Nonnull final Estimator estimator,
                                     final double maxRadius95) {
        final int k = partition.k();
        if (k < 2) {
            return Double.NaN;
        }
        double minCentroidDistance = Double.POSITIVE_INFINITY;
        for (int i = 0; i < k; i++) {
            for (int j = i + 1; j < k; j++) {
                final double d = geometricDistance(estimator, partition.getCentroid(i), partition.getCentroid(j));
                minCentroidDistance = Math.min(minCentroidDistance, d);
            }
        }
        return minCentroidDistance / Math.max(maxRadius95, 1e-12);
    }

    /**
     * Summarizes the margin distribution: median, 10th percentile, and the fraction of vectors
     * below {@code lowMarginThreshold}. Returns NaN medians/p10 and {@code 0.0} low-rate when
     * {@code k < 2}, mirroring the {@link PartitionStats} convention.
     */
    @Nonnull
    private static MarginStats marginStats(@Nonnull final List<Double> margins,
                                           final double lowMarginThreshold,
                                           final int n,
                                           final int k) {
        if (k < 2) {
            return new MarginStats(Double.NaN, Double.NaN, 0.0);
        }
        final double median = percentile(margins, 0.5d);
        final double p10 = percentile(margins, 0.1d);
        int lowMarginCount = 0;
        for (final double m : margins) {
            if (m < lowMarginThreshold) {
                lowMarginCount++;
            }
        }
        return new MarginStats(median, p10, (double) lowMarginCount / (double) n);
    }

    /**
     * Resolves the threshold below which an assignment margin counts as "low" (used to compute
     * {@link PartitionStats#lowMarginRate()}).
     * <p>
     * If {@link Parameters#lowMarginThreshold()} is positive, that explicit value is used.
     * Otherwise the threshold is derived metric-dependently: a fixed {@code 0.02} for cosine,
     * or {@code 5%} of the 95th-percentile assigned distance for Euclidean (a
     * scale-relative cutoff suitable for arbitrary L2 magnitudes).
     */
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private static double computeLowMarginThreshold(@Nonnull final Parameters parameters,
                                                    final double overallP95) {
        return switch (parameters.estimator.getMetric()) {
            case COSINE_METRIC -> parameters.lowMarginThreshold > 0.0 ? parameters.lowMarginThreshold : 0.02;
            default -> parameters.lowMarginThreshold > 0.0 ? parameters.lowMarginThreshold : 0.05 * overallP95;
        };
    }

    /**
     * Returns the geometric distance between two vectors under the estimator's metric. For both
     * supported metrics this is just {@code estimator.distance(a, b)}; the indirection exists so
     * the SSE-specific transform in {@link #distanceForSse} stays separate.
     *
     * @throws UnsupportedOperationException if the estimator's metric is neither
     *         {@code EUCLIDEAN_METRIC} nor {@code COSINE_METRIC}
     */
    private static double geometricDistance(@Nonnull final Estimator estimator, @Nonnull final RealVector a,
                                            @Nonnull final RealVector b) {
        return switch (estimator.getMetric()) {
            case COSINE_METRIC, EUCLIDEAN_METRIC -> estimator.distance(a, b);
            default -> throw new UnsupportedOperationException("metric is not supported");
        };
    }

    /**
     * Returns the per-pair contribution to the partition's SSE — the metric-specific squared
     * distance, accumulated by {@link #evaluatePartition} into {@link PartitionStats#sse()}.
     * <ul>
     *   <li>For Euclidean: {@code ||v - c||^2}, computed directly without taking a square root
     *       and re-squaring.</li>
     *   <li>For cosine: {@code 2 * (1 - v·c) = 2 * cosine_distance(v, c)}, which is the squared
     *       chord length on the unit sphere when both vectors are unit-norm. The factor of 2
     *       keeps SSE comparable in scale to the Euclidean case.</li>
     * </ul>
     *
     * @throws UnsupportedOperationException if the estimator's metric is neither
     *         {@code EUCLIDEAN_METRIC} nor {@code COSINE_METRIC}
     */
    private static double distanceForSse(@Nonnull final Estimator estimator, @Nonnull final RealVector v,
                                         @Nonnull final RealVector c) {
        return switch (estimator.getMetric()) {
            case COSINE_METRIC -> 2.0d * estimator.distance(v, c);
            case EUCLIDEAN_METRIC -> v.subtract(c).l2SquaredNorm();
            default -> throw new UnsupportedOperationException("metric is not supported");
        };
    }

    /**
     * Returns the {@code p}-th percentile of {@code values} via linear interpolation between
     * adjacent ranks, matching numpy's default percentile behavior.
     *
     * @param values input values; not mutated (a sorted copy is taken internally)
     * @param p percentile in {@code [0, 1]}, e.g. {@code 0.95} for the 95th percentile
     * @return the percentile value, or {@link Double#NaN} if {@code values} is empty. If
     *         {@code values} has exactly one element, that element is returned regardless of
     *         {@code p}
     */
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

    /**
     * The verdict {@link #evaluate} returns about a candidate partitioning relative to the
     * current one.
     */
    public enum Decision {
        /** The candidate is admissible but not better than the current partition. */
        KEEP_CURRENT,
        /** The candidate improves on the current partition by enough to justify replacement. */
        ACCEPT_CANDIDATE,
        /** The candidate is structurally invalid (e.g. cluster too small relative to threshold). */
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

        /**
         * Returns the centroid at the given cluster index as a {@link RealVector}, dereferenced
         * through {@link #vectorLens}.
         *
         * @param index cluster index in {@code [0, k())}
         * @return the centroid as a {@link RealVector}; never {@code null}
         */
        @Nonnull
        public RealVector getCentroid(final int index) {
            return vectorLens.getNonnull(centroids.get(index));
        }

        /**
         * Returns the cluster index assigned to the {@code index}-th vector in the corresponding
         * point list.
         */
        public int getAssignment(final int index) {
            return assignments[index];
        }

        /** Returns the number of clusters in this partitioning, i.e. {@code centroids.size()}. */
        @SuppressWarnings("checkstyle:MethodName")
        public int k() {
            return centroids.size();
        }

        /**
         * Custom equality: the auto-generated record {@code equals} would compare
         * {@code assignments} by reference. This implementation compares the arrays element-wise
         * via {@link Arrays#equals(int[], int[])} and the other components via
         * {@link Objects#equals(Object, Object)}.
         */
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

        /** Custom hash code consistent with {@link #equals}. */
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
        /**
         * Logs every component of these stats at error level, prefixed with
         * {@code messagePrefix}. No-op when error logging is disabled on {@code logger}.
         */
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
        /**
         * Convenience constructor that picks a moderately permissive set of defaults: 10%
         * minimum relative SSE gain, separation floor of 0.3, max low-margin rate of 25%, smallest
         * cluster fraction of 1.5%, no upper bound on the largest cluster, metric-default
         * {@code lowMarginThreshold}, and the score weights
         * {@code (alpha=1.0, beta=0.5, gamma=1.0, delta=0.75)} with a {@code minScoreGain} of
         * {@code 0.05}. Tighten or loosen via the canonical constructor as needed.
         *
         * @param estimator the distance estimator used for all distance computations
         */
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
