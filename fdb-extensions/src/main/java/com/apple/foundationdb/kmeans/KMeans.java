/*
 * KMeans.java
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
import com.apple.foundationdb.linear.MutableDoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.util.Lens;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

/**
 * A restartable Lloyd-style k-means++ implementation intended for LOCAL cluster restructuring
 * (SPFresh-style). Self-contained and stateless: a single static {@link #fit fit} entry point
 * runs the whole algorithm and returns a {@link Result} describing the chosen partitioning.
 * <p>
 * The algorithm assumes the external metric is ordinary L2, but the k-means core optimizes the
 * standard squared-L2 objective internally:
 * <ul>
 *   <li>assignment uses squared distance,</li>
 *   <li>centroid update uses arithmetic mean,</li>
 *   <li>k-means++ initialization weights candidate centroids by squared distance to the nearest
 *       already-chosen centroid, and</li>
 *   <li>restart selection compares the per-restart geometric SSE.</li>
 * </ul>
 * <p>
 * Optional <i>soft size balancing</i> biases assignment by the running projected cluster size
 * (see {@link SizePenalty}); set {@code lambda == 0} or {@code sizePenalty == null} to disable.
 * <p>
 * After the main Lloyd loop exits — whether via convergence ({@code changed == 0}) or by hitting
 * {@code maxIterations} — {@link #fit} runs one final assignment pass against the latest
 * centroids using the same scoring as the loop. This guarantees that the returned
 * {@link Result#assignment()}, {@link Result#clusterSizes()}, and
 * {@link Result#distances()} are mutually consistent with {@link Result#clusterCentroids()},
 * regardless of how the loop terminated.
 */
public final class KMeans {
    private KMeans() {
        // nothing
    }

    /**
     * Returns a {@link SizePenalty} that penalizes only overflow above the target cluster size,
     * quadratically: {@code (max(0, projected - target))^2 / max(1, target)}. A reasonable default
     * for soft size balancing.
     *
     * @return a non-null overflow-quadratic penalty function
     */
    @Nonnull
    public static SizePenalty overflowQuadraticPenalty() {
        return (proj, target) -> {
            final int overflow = Math.max(0, proj - target);
            return (double) overflow * (double) overflow / Math.max(1, target);
        };
    }

    /**
     * Fits {@code k} clusters using a restartable Lloyd-style squared-L2 k-means with k-means++
     * initialization. Returns the best (lowest geometric SSE) result across all restarts.
     * <p>
     * Per restart, the algorithm runs at most {@code maxIterations} Lloyd iterations of
     * (assignment, centroid update). Empty clusters detected during the centroid update are
     * reseeded with the data point currently farthest from any centroid. After the main loop, a
     * final assignment pass against the produced centroids is run so the returned partitioning
     * is self-consistent.
     * <p>
     * Setting {@code shuffleEachIteration} to {@code true} reduces order-dependence when using
     * projected-size penalties (see {@link SizePenalty}) and usually improves balance quality
     * when {@code lambda > 0}; with {@code lambda == 0} it has no effect on results because the
     * assignment is order-independent.
     *
     * @param random the random source; consumed by k-means++ initialization, restart seeding,
     *               and per-iteration shuffling
     * @param estimator the distance estimator. Its metric must be either
     *                  {@code EUCLIDEAN_METRIC} or {@code COSINE_METRIC}; other metrics are
     *                  rejected with an {@link UnsupportedOperationException}
     * @param vectorLens lens that extracts a {@link RealVector} from each input element
     * @param centroidLens lens that wraps a {@link RealVector} as the caller's centroid type
     * @param vectors the input points to cluster; must contain at least {@code k} elements
     * @param k number of clusters; must be at least 2
     * @param maxIterations maximum Lloyd iterations per restart; must be at least 1
     * @param maxRestarts number of additional restarts beyond the first run; must be
     *                    non-negative. The total number of runs is {@code maxRestarts + 1};
     *                    the run with the lowest geometric SSE wins
     * @param lambda strength of size-balancing bias added to the assignment score; must be
     *               non-negative. {@code 0} disables balancing
     * @param sizePenalty penalty function applied to projected cluster sizes during
     *                    assignment; {@code null} disables balancing. Ignored when
     *                    {@code lambda == 0}
     * @param shuffleEachIteration whether to Fisher–Yates-shuffle the per-iteration
     *                             assignment order
     * @param <V> caller's input vector representation
     * @param <C> caller's centroid representation
     * @return the best partitioning found across all restarts, ranked by geometric SSE
     */
    public static <V, C> Result<C> fit(@Nonnull final SplittableRandom random,
                                       @Nonnull final Estimator estimator,
                                       @Nonnull final Lens<V, RealVector> vectorLens,
                                       @Nonnull final Lens<C, RealVector> centroidLens,
                                       @Nonnull final List<V> vectors,
                                       final int k,
                                       final int maxIterations,
                                       final int maxRestarts,
                                       final double lambda,
                                       @Nullable final SizePenalty sizePenalty,
                                       final boolean shuffleEachIteration) {

        Preconditions.checkArgument(k >= 2, "k must be >= 2");
        Preconditions.checkArgument(vectors.size() >= k, "vectors.size() must be >= k");
        Preconditions.checkArgument(maxIterations >= 1, "maxIterations must be >= 1");
        Preconditions.checkArgument(maxRestarts >= 0, "maxRestarts must be >= 0");
        Preconditions.checkArgument(lambda >= 0, "lambda must be >= 0");

        final MetricAdapter metricAdapter = fromEstimator(estimator);

        final int numDimensions = getVector(vectorLens, vectors, 0).getNumDimensions();
        final int n = vectors.size();
        final int targetSize = Math.max(1, n / k);

        // Pre-build an index array so we can shuffle without moving data.
        final int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }

        Result<C> best = null;

        //
        // Reusable swap buffer for the centroid update step. Allocated once and reused across
        // every iteration of every restart: each iteration zeros it, accumulates the new
        // centroids into it, finalizes, and then swaps it with `centroids`. Avoids allocating
        // k MutableDoubleRealVectors (each with their own double[d]) per iteration.
        //
        List<MutableDoubleRealVector> nextCentroids = new ArrayList<>(k);
        for (int c = 0; c < k; c++) {
            nextCentroids.add(MutableDoubleRealVector.zeroVector(numDimensions));
        }

        for (int r = 0; r <= maxRestarts; r++) {
            List<MutableDoubleRealVector> centroids = initKMeansPP(metricAdapter, random, vectorLens, vectors, k);
            final int[] assignment = new int[n];
            Arrays.fill(assignment, -1);
            final int[] clusterSizes = new int[k];

            for (int iteration = 0; iteration < maxIterations; iteration++) {
                if (shuffleEachIteration) {
                    shuffleInPlace(random, order);
                }

                //
                // Projected sizes updated online during assignment to implement size bias.
                // If balancing is disabled, this behaves like normal clusterSizes accumulation.
                //
                final int[] projected = new int[k];

                final int changed = assignmentStep(metricAdapter, vectorLens, vectors, centroids,
                        k, order, assignment, projected, targetSize, lambda, sizePenalty);

                // Use projected sizes as current sizes.
                System.arraycopy(projected, 0, clusterSizes, 0, k);

                if (changed == 0) {
                    break;
                }

                // Update step: arithmetic mean, correct for squared-L2 k-means. Zero the swap
                // buffer in place, accumulate into it, then swap roles with `centroids` so the
                // old centroid buffer becomes the next iteration's accumulator.
                for (int c = 0; c < k; c++) {
                    nextCentroids.get(c).zero();
                }

                for (int i = 0; i < n; i++) {
                    nextCentroids.get(assignment[i]).add(getVector(vectorLens, vectors, i));
                }

                for (int c = 0; c < k; c++) {
                    if (clusterSizes[c] == 0) {
                        // Reseed empty cluster with the "hardest" point under current centroids.
                        final int farthestVectorIndex = farthestVectorIndex(metricAdapter, vectorLens, vectors, centroids);
                        nextCentroids.get(c).withData(getVector(vectorLens, vectors, farthestVectorIndex).getData());
                        clusterSizes[c] = 1; // local guard only; next iteration recomputes true sizes
                    } else {
                        final MutableDoubleRealVector centroid = nextCentroids.get(c);
                        centroid.multiply(1.0d / clusterSizes[c]);
                        if (metricAdapter.isMeaninglessNorm(centroid)) {
                            final int farthestVectorIndex =
                                    farthestVectorIndex(metricAdapter, vectorLens, vectors, centroids);
                            centroid.withData(getVector(vectorLens, vectors, farthestVectorIndex).getData());
                        } else {
                            metricAdapter.renormalizeIfNecessary(centroid);
                        }
                    }
                }

                // Swap roles: the buffer we just filled becomes `centroids`; the previous
                // `centroids` becomes the swap target for the next iteration's accumulation.
                final List<MutableDoubleRealVector> tmp = centroids;
                centroids = nextCentroids;
                nextCentroids = tmp;
            }

            //
            // Final assignment pass. The main loop exits either via convergence
            // (changed == 0, in which case assignment is already consistent with centroids) or
            // via maxIterations (in which case centroids was just updated and assignment is one
            // step behind). Run one more assignment step against the final centroids using the
            // same scoring as the loop, so the returned result is self-consistent: when
            // lambda == 0 this gives geometric local optimality; when lambda > 0 the size bias
            // is preserved with the same order-dependent semantics as the loop's iterations.
            //
            if (shuffleEachIteration) {
                shuffleInPlace(random, order);
            }
            final int[] finalProjected = new int[k];
            assignmentStep(metricAdapter, vectorLens, vectors, centroids, k,
                    order, assignment, finalProjected, targetSize, lambda, sizePenalty);
            System.arraycopy(finalProjected, 0, clusterSizes, 0, k);

            // objective: sum of individual objective per-vector contributions, excluding any size penalty,
            // so restarts are compared on the geometric objective alone.
            final double[] distances = new double[n];
            double sumObjective = 0.0d;
            for (int i = 0; i < n; i++) {
                final double obj = metricAdapter.baseObjective(getVector(vectorLens, vectors, i),
                        centroids.get(assignment[i]));
                distances[i] = obj;
                sumObjective += obj;
            }

            final ImmutableList.Builder<C> centroidCopies = ImmutableList.builderWithExpectedSize(k);
            for (final MutableDoubleRealVector centroid : centroids) {
                centroidCopies.add(centroidLens.wrap(centroid.toImmutable()));
            }

            final Result<C> candidate =
                    new Result<>(centroidCopies.build(), clusterSizes.clone(), assignment.clone(),
                            distances.clone(), sumObjective);

            if (best == null || candidate.objective() < best.objective()) {
                best = candidate;
            }
        }

        return best;
    }

    /**
     * Computes the per-cluster assignment score for {@code vector} against centroid {@code c}:
     * the metric-adapter's base objective plus an optional size-balancing penalty derived from
     * the running projected size of cluster {@code c}.
     * <p>
     * When {@code lambda == 0} or {@code sizePenalty == null} this collapses to the unbiased
     * geometric base objective.
     *
     * @param metricAdapter adapter that computes the metric-specific base objective
     * @param vector the data vector being scored
     * @param c index of the candidate cluster
     * @param centroids current centroids
     * @param projectedSizes online projected sizes for the in-progress assignment pass; the
     *                       penalty is evaluated as if {@code vector} were assigned to
     *                       {@code c}, i.e. against {@code projectedSizes[c] + 1}
     * @param targetSize the target cluster size, {@code n / k}
     * @param lambda non-negative balancing strength
     * @param sizePenalty optional size penalty hook; if {@code null} no penalty is added
     * @return the score; lower is better
     */
    private static double score(@Nonnull final MetricAdapter metricAdapter,
                                @Nonnull final RealVector vector,
                                final int c,
                                @Nonnull final List<MutableDoubleRealVector> centroids,
                                final int[] projectedSizes,
                                final int targetSize,
                                final double lambda,
                                @Nullable final SizePenalty sizePenalty) {
        final double objective = metricAdapter.baseObjective(vector, centroids.get(c));

        if (lambda == 0.0d || sizePenalty == null) {
            return objective;
        }

        final int proj = projectedSizes[c] + 1;
        return objective + lambda * sizePenalty.penalty(proj, targetSize);
    }

    /**
     * One assignment-step pass over {@code vectors} in the order specified by {@code order}.
     * <p>
     * For each vector this picks the cluster minimizing
     * {@link #score(MetricAdapter, RealVector, int, List, int[], int, double, SizePenalty)} and
     * accumulates the resulting cluster index into {@code projected} as we go, so the
     * size-balance bias is applied online with the same order-dependent semantics as the main
     * loop. Used both inside the Lloyd loop and as the final reassignment pass after it.
     *
     * @param metricAdapter adapter that computes the metric-specific base objective
     * @param vectorLens lens that extracts a {@link RealVector} from each {@code vectors} element
     * @param vectors the data points being assigned
     * @param centroids the centroids to assign against
     * @param k number of clusters
     * @param order indices into {@code vectors} specifying the visitation order; typically a
     *              shuffled permutation when {@code lambda > 0} to reduce order bias
     * @param assignment per-vector cluster assignment, updated in place. Initial entries of
     *                   {@code -1} are treated as different from any valid cluster index, so
     *                   the very first pass always counts every vector as changed
     * @param projected running projected cluster sizes; must be all-zero on entry, and is
     *                  incremented as each vector is assigned. Used both to compute the
     *                  size-balance penalty for the next vector and as the final cluster sizes
     *                  for this pass
     * @param targetSize the target cluster size, {@code n / k}
     * @param lambda non-negative balancing strength
     * @param sizePenalty optional size penalty hook; {@code null} to disable balancing
     * @param <V> caller's input vector representation
     * @return the number of vectors whose new assignment differs from their prior value in
     *         {@code assignment}
     */
    private static <V> int assignmentStep(@Nonnull final MetricAdapter metricAdapter,
                                          @Nonnull final Lens<V, RealVector> vectorLens,
                                          @Nonnull final List<V> vectors,
                                          @Nonnull final List<MutableDoubleRealVector> centroids,
                                          final int k,
                                          @Nonnull final int[] order,
                                          @Nonnull final int[] assignment,
                                          @Nonnull final int[] projected,
                                          final int targetSize,
                                          final double lambda,
                                          @Nullable final SizePenalty sizePenalty) {
        int changed = 0;
        for (final int i : order) {
            final RealVector vector = getVector(vectorLens, vectors, i);

            int bestC = 0;
            double bestScore = score(metricAdapter, vector, 0, centroids, projected, targetSize,
                    lambda, sizePenalty);

            for (int c = 1; c < k; c++) {
                final double s = score(metricAdapter, vector, c, centroids, projected, targetSize,
                        lambda, sizePenalty);
                if (s < bestScore) {
                    bestScore = s;
                    bestC = c;
                }
            }

            if (assignment[i] != bestC) {
                assignment[i] = bestC;
                changed++;
            }

            projected[bestC]++; // commit this vector to the projected size
        }
        return changed;
    }

    /**
     * <a href="https://en.wikipedia.org/wiki/K-means%2B%2B">k-means++</a> initialization.
     * <p>
     * Vanilla random initialization (just pick {@code k} random points) often hands Lloyd's
     * algorithm a terrible starting point — two centroids placed near each other will fight
     * over the same cluster forever, locking in a bad local minimum. K-means++ instead biases
     * the initial centroids to be <em>well-spread</em>:
     * <ol>
     *   <li>Pick the first centroid uniformly at random.</li>
     *   <li>For every other point {@code i}, let {@code d²(i)} be the squared distance to the
     *       nearest already-chosen centroid.</li>
     *   <li>Pick the next centroid by sampling the data with probability proportional to
     *       {@code d²(i)}. Points poorly served by the current centroids are likely to be
     *       picked, but the randomization avoids deterministically choosing outliers.</li>
     *   <li>Repeat (2)–(3) until {@code k} centroids are chosen.</li>
     * </ol>
     * The squared-distance weighting is what gives k-means++ its theoretical guarantee that the
     * expected SSE is within {@code O(log k)} of the optimum, independent of the data.
     * <p>
     * Implementation note: a textbook k-means++ recomputes step (2) from scratch every round,
     * which is {@code O(k² · n)}. This implementation maintains {@code weights[i]} (the minimum
     * over all already-chosen centroids of {@code d²(i, centroid)}) across rounds and updates
     * it incrementally against the just-added centroid, bringing the total to {@code O(k · n)}.
     * <p>
     * Degenerate fallback: if the cumulative weight is zero (e.g. all remaining points coincide
     * with already-chosen centroids), the next centroid is picked uniformly at random.
     *
     * @param metricAdapter adapter that computes the metric-specific base objective used as the
     *                      sampling weight (squared distance for Euclidean, cosine distance for
     *                      cosine)
     * @param random the random source
     * @param vectorLens lens that extracts a {@link RealVector} from each {@code vectors} element
     * @param vectors the candidate points; must contain at least {@code k} elements
     * @param k number of centroids to pick
     * @param <V> caller's input vector representation
     * @return a list of {@code k} freshly allocated mutable centroids, copied from the chosen
     *         input vectors
     */
    @Nonnull
    private static <V> List<MutableDoubleRealVector> initKMeansPP(@Nonnull final MetricAdapter metricAdapter,
                                                                  @Nonnull final SplittableRandom random,
                                                                  @Nonnull final Lens<V, RealVector> vectorLens,
                                                                  @Nonnull final List<V> vectors,
                                                                  final int k) {
        final int n = vectors.size();

        // Step 1: pick the first centroid uniformly at random.
        final List<MutableDoubleRealVector> centroids = new ArrayList<>(k);
        MutableDoubleRealVector latestCentroid = getVector(vectorLens, vectors, random.nextInt(n)).toMutable();
        centroids.add(latestCentroid);

        //
        // weights[i] holds the minimum base-objective distance from vectors[i] to any
        // already-chosen centroid. Initialized against the first centroid here, then maintained
        // incrementally inside the loop. total = Σ weights[i] is the denominator of the
        // cumulative sampling distribution.
        //
        final double[] weights = new double[n];
        double total = 0.0d;
        for (int i = 0; i < n; i++) {
            final double d = metricAdapter.baseObjective(getVector(vectorLens, vectors, i), latestCentroid);
            weights[i] = d;
            total += d;
        }

        while (centroids.size() < k) {
            //
            // Step 3: weighted-sample the next centroid. Pick a uniform `pick` in [0, total),
            // walk weights[] cumulatively, and take the first index where the running sum
            // crosses `pick`. That picks index i with probability weights[i] / total =
            // d²(i) / Σ d²(j) — exactly what k-means++ specifies.
            //
            final int chosen;
            if (total == 0.0d) {
                // Degenerate fallback: every point coincides with an already-chosen centroid.
                chosen = random.nextInt(n);
            } else {
                final double pick = random.nextDouble() * total;
                double cumulativeSum = 0.0d;
                int idx = n - 1;
                for (int i = 0; i < n; i++) {
                    cumulativeSum += weights[i];
                    if (cumulativeSum >= pick) {
                        idx = i;
                        break;
                    }
                }
                chosen = idx;
            }

            latestCentroid = getVector(vectorLens, vectors, chosen).toMutable();
            centroids.add(latestCentroid);

            //
            // Step 2 (incremental): weights[i] should be the minimum over all chosen centroids
            // of d²(i, centroid). Since weights[i] already holds the min over the previous
            // centroids, the new centroid is the only one we haven't accounted for — so
            // weights[i] = min(weights[i], d²(i, latestCentroid)). Skip when we just added the
            // last centroid, since weights[] is consumed only by the next round's sampling and
            // there won't be one.
            //
            if (centroids.size() < k) {
                total = 0.0d;
                for (int i = 0; i < n; i++) {
                    final double d = metricAdapter.baseObjective(getVector(vectorLens, vectors, i), latestCentroid);
                    if (d < weights[i]) {
                        weights[i] = d;
                    }
                    total += weights[i];
                }
            }
        }

        return centroids;
    }

    /**
     * Returns the index of the data point whose minimum (metric-specific) base objective to any
     * centroid is maximal — i.e. the point currently <i>worst</i>-served by the centroid set.
     * Used by the centroid update step to reseed clusters that ended up empty (or that produced
     * a meaningless centroid, e.g. zero-norm in cosine mode).
     *
     * @param metricAdapter adapter that computes the metric-specific base objective
     * @param vectorLens lens that extracts a {@link RealVector} from each {@code vectors} element
     * @param vectors the candidate points
     * @param centroids the current centroids; must be non-empty
     * @param <V> caller's input vector representation
     * @return the index in {@code vectors} of the farthest point
     */
    @VisibleForTesting
    static <V> int farthestVectorIndex(@Nonnull final MetricAdapter metricAdapter,
                                       @Nonnull final Lens<V, RealVector> vectorLens,
                                       @Nonnull final List<V> vectors,
                                       @Nonnull final List<? extends RealVector> centroids) {
        //
        // Defensive snapshot: A MutableDoubleRealVector centroid would be mutated in place by any
        // baseObjective implementation that uses chained mutate-in-place ops (subtract/add/...);
        // Immutable centroid types are passed through.
        //
        final List<RealVector> frozenCentroids = new ArrayList<>(centroids.size());
        for (final RealVector centroid : centroids) {
            frozenCentroids.add(centroid.toImmutable());
        }

        double best = -1.0d;
        int bestIdx = 0;

        for (int i = 0; i < vectors.size(); i++) {
            final RealVector vector = getVector(vectorLens, vectors, i);

            double min = Double.MAX_VALUE;
            for (final RealVector centroid : frozenCentroids) {
                final double objective = metricAdapter.baseObjective(vector, centroid);
                if (objective < min) {
                    min = objective;
                }
            }

            if (min > best) {
                best = min;
                bestIdx = i;
            }
        }

        return bestIdx;
    }

    /**
     * Convenience accessor that extracts the {@link RealVector} for {@code vectors.get(index)}
     * via {@code vectorLens}, throwing on a null result.
     *
     * @param vectorLens lens that extracts a {@link RealVector} from a caller element
     * @param vectors the input list
     * @param index index into {@code vectors}
     * @param <V> caller's input vector representation
     * @return the extracted vector; never {@code null}
     */
    @Nonnull
    private static <V> RealVector getVector(@Nonnull final Lens<V, RealVector> vectorLens,
                                            @Nonnull final List<V> vectors,
                                            final int index) {
        return vectorLens.getNonnull(vectors.get(index));
    }

    /**
     * In-place Fisher–Yates shuffle of an int array using a {@link SplittableRandom} as the
     * source of randomness. Used to randomize the per-iteration assignment order so that, with
     * size-balance bias active, the first vectors processed don't always preferentially fill
     * one cluster.
     *
     * @param random random source
     * @param a array shuffled in place
     */
    private static void shuffleInPlace(@Nonnull final SplittableRandom random, @Nonnull final int[] a) {
        for (int i = a.length - 1; i > 0; i--) {
            final int j = random.nextInt(i + 1);
            final int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
    }

    /**
     * Returns the {@link MetricAdapter} that matches the metric exposed by {@code estimator}.
     *
     * @param estimator the distance estimator
     * @return a metric adapter for {@code estimator}'s metric
     * @throws UnsupportedOperationException if the estimator's metric is neither
     *         {@code EUCLIDEAN_METRIC} nor {@code COSINE_METRIC}
     */
    @Nonnull
    @VisibleForTesting
    static MetricAdapter fromEstimator(@Nonnull final Estimator estimator) {
        return switch (estimator.getMetric()) {
            case EUCLIDEAN_METRIC -> new EuclideanMetricAdapter(estimator);
            case COSINE_METRIC -> new CosineMetricAdapter(estimator);
            default -> throw new UnsupportedOperationException("metric is not supported");
        };
    }

    /**
     * Metric-specific knobs the k-means core needs in order to optimize the squared-L2
     * objective consistently across the supported metrics. There is one implementation per
     * supported metric; the right one is chosen by {@link #fromEstimator}.
     */
    @VisibleForTesting
    interface MetricAdapter {
        /**
         * Returns the per-pair contribution to the clustering objective. For Euclidean this is
         * the squared Euclidean distance; for cosine it is the cosine distance (i.e. the
         * estimator's distance directly, which is already in {@code [0, 2]}).
         *
         * @param vector the data vector
         * @param centroid the centroid being scored against
         * @return the per-pair contribution to the clustering objective
         */
        double baseObjective(@Nonnull RealVector vector, @Nonnull RealVector centroid);

        /**
         * Renormalizes a freshly averaged centroid in place if the metric requires it (e.g.
         * cosine, where centroids should live on the unit sphere). For Euclidean this is a
         * no-op.
         *
         * @param vector the centroid to potentially renormalize
         * @return {@code vector}, possibly modified in place; the same reference is returned
         *         for convenience in fluent expressions
         */
        @CanIgnoreReturnValue
        @Nonnull
        MutableDoubleRealVector renormalizeIfNecessary(@Nonnull MutableDoubleRealVector vector);

        /**
         * Returns whether the given vector has a "meaningless" norm under this metric — for
         * example, near-zero magnitude in cosine mode where direction is undefined. Used by
         * the centroid update to detect degenerate centroids that need reseeding.
         *
         * @param vector the vector to test
         * @return {@code true} if the vector's norm is meaningless under this metric
         */
        boolean isMeaninglessNorm(@Nonnull RealVector vector);
    }

    /**
     * {@link MetricAdapter} for the Euclidean metric. Base objective is squared Euclidean
     * distance.
     * <p>
     * For non-quantization-aware estimators (the common case: any
     * {@link Estimator#ofMetric}-built estimator) this is computed directly via
     * {@link RealVector#l2SquaredDistance} to skip the {@code sqrt} that
     * {@link Estimator#distance Estimator.distance(...)} performs only to be squared again.
     * <p>
     * When {@link Estimator#isOptimized} reports that the estimator has a metric-specific fast
     * path for the given pair (e.g. a {@code RaBitEstimator} acting on at least one encoded
     * vector), the call is routed through {@code estimator.distance(...)} so the
     * quantization-aware estimate is preserved — bypassing it via {@code getData()} would
     * produce a different (more accurate, but not what the caller wants) value.
     * <p>
     * No renormalization, no degenerate-norm handling — Euclidean centroids are always
     * well-defined.
     */
    private static class EuclideanMetricAdapter implements MetricAdapter {
        @Nonnull
        private final Estimator estimator;

        public EuclideanMetricAdapter(@Nonnull final Estimator estimator) {
            this.estimator = estimator;
        }

        @Override
        public double baseObjective(@Nonnull final RealVector vector,
                                    @Nonnull final RealVector centroid) {
            if (estimator.isOptimized(vector, centroid)) {
                final double d = estimator.distance(vector, centroid);
                return d * d;
            }
            return vector.l2SquaredDistance(centroid);
        }

        @Nonnull
        @Override
        public MutableDoubleRealVector renormalizeIfNecessary(@Nonnull final MutableDoubleRealVector vector) {
            return vector;
        }

        @Override
        public boolean isMeaninglessNorm(@Nonnull final RealVector vector) {
            return false;
        }
    }

    /**
     * {@link MetricAdapter} for the cosine metric. Base objective is the cosine distance
     * directly. Centroids are renormalized to the unit sphere after averaging, and a
     * near-zero-norm centroid is treated as meaningless and triggers a reseed.
     */
    private static class CosineMetricAdapter implements MetricAdapter {
        @Nonnull
        private final Estimator estimator;

        public CosineMetricAdapter(@Nonnull final Estimator estimator) {
            this.estimator = estimator;
        }

        @Override
        public double baseObjective(@Nonnull final RealVector vector,
                                    @Nonnull final RealVector centroid) {
            return estimator.distance(vector, centroid);
        }

        @Nonnull
        @Override
        public MutableDoubleRealVector renormalizeIfNecessary(@Nonnull final MutableDoubleRealVector vector) {
            return vector.normalize();
        }

        @Override
        public boolean isMeaninglessNorm(@Nonnull final RealVector vector) {
            return vector.isNearlyZeroNorm();
        }
    }

    /**
     * Outcome of a {@link #fit fit} call: the chosen centroids, the per-vector cluster
     * assignment, the per-cluster sizes, the per-vector objective contributions, and the total
     * objective. All components are mutually consistent — i.e. for every {@code i},
     * {@code distances[i] == baseObjective(vectors[i], clusterCentroids[assignment[i]])}, and
     * {@code clusterSizes[c] == count(j: assignment[j] == c)}.
     * <p>
     * The compact constructor defensively copies {@code clusterCentroids} into an immutable list.
     * The three primitive arrays are <em>not</em> copied — the caller transfers ownership to the
     * record, and accessors return the underlying array references directly.
     *
     * @param clusterCentroids the {@code k} cluster centroids in cluster-index order; defensively
     *                         copied into an immutable list at construction time
     * @param clusterSizes per-cluster size array of length {@code k}; entry {@code c} is the
     *                     number of input vectors assigned to cluster {@code c}. Ownership
     *                     transferred to the record
     * @param assignment per-vector cluster assignment; entry {@code i} is the index of the
     *                   cluster that {@code vectors.get(i)} was assigned to. Ownership
     *                   transferred to the record
     * @param distances per-vector contribution to the geometric objective: squared L2 distance to
     *                  the assigned centroid in Euclidean mode, cosine distance in cosine mode.
     *                  Ownership transferred to the record
     * @param objective sum of {@code distances} — the geometric SSE in Euclidean mode, or the sum
     *                  of cosine distances in cosine mode. The size-balance penalty is
     *                  intentionally excluded so restarts can be compared on the geometric
     *                  objective alone
     * @param <C> caller's centroid representation
     */
    public record Result<C>(@Nonnull List<C> clusterCentroids,
                            @Nonnull int[] clusterSizes,
                            @Nonnull int[] assignment,
                            @Nonnull double[] distances,
                            double objective) {
    }

    /**
     * Soft size-balancing penalty hook. {@link #fit fit} multiplies the value returned by this
     * function by {@code lambda} and adds it to the per-pair base objective during the
     * assignment step, biasing the algorithm away from oversized clusters.
     * <p>
     * Pass {@code lambda == 0} (or {@code sizePenalty == null}) to disable balancing.
     *
     * @see #overflowQuadraticPenalty()
     */
    @FunctionalInterface
    public interface SizePenalty {
        /**
         * Implementation of penalty function.
         * @param projectedSize the projected size of the candidate cluster <i>after</i>
         *                      hypothetically assigning the current vector to it
         * @param targetSize the target cluster size, {@code n / k}
         * @return a non-negative penalty; larger values discourage assignment to the cluster
         */
        double penalty(int projectedSize, int targetSize);
    }
}
