/*
 * BoundedKMeans.java
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
 * Bounded, restartable Lloyd-style k-means intended for LOCAL cluster restructuring (SPFresh-style).
 * <p>
 * This implementation assumes the external metric is ordinary L2, but the k-means
 * core optimizes the standard squared-L2 objective internally:
 * <p>
 *   - assignment uses squared distance
 *   - centroid update uses arithmetic mean
 *   - k-means++ uses squared distance weighting
 *   - restart selection uses SSE
 * <p>
 * Soft size balancing is optional.
 */
public final class BoundedKMeans {
    private BoundedKMeans() {
        // nothing
    }

    /** Reasonable default: penalize only overflow above target size (quadratic). */
    public static SizePenalty overflowQuadraticPenalty() {
        return (proj, target) -> {
            final int overflow = Math.max(0, proj - target);
            return (double) overflow * (double) overflow / Math.max(1, target);
        };
    }

    /**
     * Fits k clusters using a bounded, restartable Lloyd-style squared-L2 k-means.
     * Shuffling reduces order-dependence when using projected-size penalties and usually improves balance quality.
     *
     * @param lambda strength of size balancing; 0 disables
     * @param sizePenalty penalty function; null disables
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

        for (int r = 0; r <= maxRestarts; r++) {
            List<MutableDoubleRealVector> centroids = initKMeansPP(metricAdapter, random, vectorLens, vectors, k);
            final int[] assignment = new int[n];
            Arrays.fill(assignment, -1);
            final int[] clusterSizes = new int[k];

            for (int iteration = 0; iteration < maxIterations; iteration++) {
                if (shuffleEachIteration) {
                    shuffleInPlace(random, order);
                }

                int changed = 0;

                //
                // Projected sizes updated online during assignment to implement size bias.
                // If balancing is disabled, this behaves like normal clusterSizes accumulation.
                //
                final int[] projected = new int[k];

                // assignment step
                for (int t = 0; t < n; t++) {
                    final int i = order[t];
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

                // Use projected sizes as current sizes.
                System.arraycopy(projected, 0, clusterSizes, 0, k);

                if (changed == 0) {
                    break;
                }

                // Update step: arithmetic mean, correct for squared-L2 k-means.
                final List<MutableDoubleRealVector> newCentroids = new ArrayList<>(k);
                for (int c = 0; c < k; c++) {
                    newCentroids.add(MutableDoubleRealVector.zeroVector(numDimensions));
                }

                for (int i = 0; i < n; i++) {
                    newCentroids.get(assignment[i]).add(getVector(vectorLens, vectors, i));
                }

                for (int c = 0; c < k; c++) {
                    if (clusterSizes[c] == 0) {
                        // Reseed empty cluster with the "hardest" point under current centroids.
                        final int farthestVectorIndex = farthestVectorIndex(metricAdapter, vectorLens, vectors, centroids);
                        newCentroids.set(c, getVector(vectorLens, vectors, farthestVectorIndex).toMutable());
                        clusterSizes[c] = 1; // local guard only; next iteration recomputes true sizes
                    } else {
                        final MutableDoubleRealVector centroid = newCentroids.get(c);
                        centroid.multiply(1.0d / clusterSizes[c]);
                        if (metricAdapter.isMeaninglessNorm(centroid)) {
                            final int farthestVectorIndex =
                                    farthestVectorIndex(metricAdapter, vectorLens, vectors, centroids);
                            final MutableDoubleRealVector reseed =
                                    getVector(vectorLens, vectors, farthestVectorIndex).toMutable();
                            newCentroids.set(c, reseed);
                        } else {
                            metricAdapter.renormalizeIfNecessary(centroid);
                        }
                    }
                }

                centroids = newCentroids;
            }

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

            if (best == null || candidate.getObjective() < best.getObjective()) {
                best = candidate;
            }
        }

        return best;
    }

    /**
     * Squared-L2 assignment score plus optional size penalty.
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
     * k-means++ initialization: pick first centroid at random, then sample next centroids
     * with probability proportional to squared distance to nearest chosen centroid.
     * <p>
     * Assumes estimator.distance() returns ordinary L2 distance.
     */
    @Nonnull
    private static <V> List<MutableDoubleRealVector> initKMeansPP(@Nonnull final MetricAdapter metricAdapter,
                                                                  @Nonnull final SplittableRandom random,
                                                                  @Nonnull final Lens<V, RealVector> vectorLens,
                                                                  @Nonnull final List<V> vectors,
                                                                  final int k) {
        final int n = vectors.size();

        final List<MutableDoubleRealVector> centroids = new ArrayList<>(k);
        centroids.add(getVector(vectorLens, vectors, random.nextInt(n)).toMutable());

        final double[] weights = new double[n];

        while (centroids.size() < k) {
            double total = 0.0d;

            for (int i = 0; i < n; i++) {
                final RealVector vector = getVector(vectorLens, vectors, i);
                double minD = Double.MAX_VALUE;

                for (final MutableDoubleRealVector centroid : centroids) {
                    final double objective = metricAdapter.baseObjective(vector, centroid);
                    if (objective < minD) {
                        minD = objective;
                    }
                }

                weights[i] = minD;
                total += minD;
            }

            if (total == 0.0d) {
                centroids.add(getVector(vectorLens, vectors, random.nextInt(n)).toMutable());
                continue;
            }

            final double pick = random.nextDouble() * total;
            double cum = 0.0d;
            int chosen = n - 1;

            for (int i = 0; i < n; i++) {
                cum += weights[i];
                if (cum >= pick) {
                    chosen = i;
                    break;
                }
            }

            centroids.add(getVector(vectorLens, vectors, chosen).toMutable());
        }

        return centroids;
    }

    /**
     * Index of point whose minimum L2 distance to any centroid is maximal.
     * Used to reseed empty clusters.
     */
    private static <V> int farthestVectorIndex(@Nonnull final MetricAdapter metricAdapter,
                                               @Nonnull final Lens<V, RealVector> vectorLens,
                                               @Nonnull final List<V> vectors,
                                               @Nonnull final List<MutableDoubleRealVector> centroids) {
        double best = -1.0d;
        int bestIdx = 0;

        for (int i = 0; i < vectors.size(); i++) {
            final RealVector vector = getVector(vectorLens, vectors, i);

            double min = Double.MAX_VALUE;
            for (final MutableDoubleRealVector centroid : centroids) {
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

    @Nonnull
    private static <V> RealVector getVector(@Nonnull final Lens<V, RealVector> vectorLens,
                                            @Nonnull final List<V> vectors,
                                            final int index) {
        return vectorLens.getNonnull(vectors.get(index));
    }

    /** Fisher–Yates shuffle of an int[] using SplittableRandom. */
    private static void shuffleInPlace(@Nonnull final SplittableRandom random, @Nonnull final int[] a) {
        for (int i = a.length - 1; i > 0; i--) {
            final int j = random.nextInt(i + 1);
            final int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
    }

    @Nonnull
    private static MetricAdapter fromEstimator(@Nonnull final Estimator estimator) {
        switch (estimator.getMetric()) {
            case EUCLIDEAN_METRIC:
                return new EuclideanMetricAdapter(estimator);
            case COSINE_METRIC:
                return new CosineMetricAdapter(estimator);
            default:
                throw new UnsupportedOperationException("metric is not supported");
        }
    }

    private interface MetricAdapter {
        double baseObjective(@Nonnull final RealVector vector,
                             @Nonnull final RealVector centroid);

        @CanIgnoreReturnValue
        @Nonnull
        MutableDoubleRealVector renormalizeIfNecessary(@Nonnull final MutableDoubleRealVector vector);

        boolean isMeaninglessNorm(@Nonnull final RealVector vector);
    }

    private static class EuclideanMetricAdapter implements MetricAdapter {
        @Nonnull
        private final Estimator estimator;

        public EuclideanMetricAdapter(@Nonnull final Estimator estimator) {
            this.estimator = estimator;
        }

        @Override
        public double baseObjective(@Nonnull final RealVector vector,
                                    @Nonnull final RealVector centroid) {
            final double d = estimator.distance(vector, centroid);
            return d * d;
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

    public static final class Result<C> {
        @Nonnull
        private final List<C> clusterCentroids;
        @Nonnull
        private final int[] clusterSizes;
        @Nonnull
        private final int[] assignment;
        /**
         * Per-vector contribution to the clustering objective.
         *   - squared L2 distance in EUCLIDEAN mode
         *   - cosine distance in COSINE mode
         */
        @Nonnull
        private final double[] distances;
        /**
         * Sum of per vector objective contributions.
         */
        private final double objective;

        public Result(@Nonnull final List<C> clusterCentroids,
                      @Nonnull final int[] clusterSizes,
                      @Nonnull final int[] assignment,
                      @Nonnull final double[] distances,
                      final double objective) {
            this.clusterCentroids = ImmutableList.copyOf(clusterCentroids);
            this.assignment = assignment;
            this.clusterSizes = clusterSizes;
            this.distances = distances;
            this.objective = objective;
        }

        @Nonnull
        public List<C> getClusterCentroids() {
            return clusterCentroids;
        }

        @Nonnull
        public int[] getClusterSizes() {
            return clusterSizes;
        }

        @Nonnull
        public int[] getAssignment() {
            return assignment;
        }

        /**
         * Returns squared distances to assigned centroids.
         */
        @Nonnull
        public double[] getDistances() {
            return distances;
        }

        /**
         * Returns the sum of squared distances (SSE).
         */
        public double getObjective() {
            return objective;
        }
    }

    /**
     * Soft balancing penalty hook.
     * <p>
     * Returned value is multiplied by lambda and added to the squared-distance score during assignment.
     * Use lambda=0 or sizePenalty=null to disable.
     */
    @FunctionalInterface
    public interface SizePenalty {
        double penalty(int projectedSize, int targetSize);
    }
}
