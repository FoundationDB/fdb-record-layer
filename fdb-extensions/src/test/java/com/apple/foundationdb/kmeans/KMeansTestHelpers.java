/*
 * KMeansTestHelpers.java
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

import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.assertj.core.api.Assertions.within;

/**
 * Test helpers for the {@link com.apple.foundationdb.kmeans} package.
 */
final class KMeansTestHelpers {
    static final int SIFT_SMALL_SIZE = 10000;
    static final int SIFT_SMALL_DIMENSION = 128;

    private KMeansTestHelpers() {
    }

    /**
     * Loads the full SIFT-small base dataset (10,000 × 128-dim {@code float} vectors) as
     * {@link DoubleRealVector}s. The file is produced by the {@code extractSiftSmall} gradle task,
     * which {@code test} depends on.
     */
    @Nonnull
    static List<DoubleRealVector> loadSiftSmall() throws IOException {
        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");
        try (var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> it = new StoredVecsIterator.StoredFVecsIterator(fileChannel);
            final ImmutableList.Builder<DoubleRealVector> b = ImmutableList.builderWithExpectedSize(SIFT_SMALL_SIZE);
            while (it.hasNext()) {
                b.add(it.next());
            }
            final ImmutableList<DoubleRealVector> all = b.build();
            assertThat(all).hasSize(SIFT_SMALL_SIZE);
            return all;
        }
    }

    /**
     * Picks {@code n} distinct random elements from {@code items} using {@code random}.
     */
    @Nonnull
    static <T> List<T> pickRandomSubset(@Nonnull final Random random,
                                        @Nonnull final List<T> items,
                                        final int n) {
        Verify.verify(n <= items.size(), "cannot pick %s elements from a list of size %s", n, items.size());
        final List<T> remaining = Lists.newArrayList(items);
        final List<T> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            result.add(remaining.remove(random.nextInt(remaining.size())));
        }
        return result;
    }

    /**
     * Returns the L2-normalized form of every input vector. Useful for cosine-metric tests where
     * the algorithm and the asserted invariants assume unit-norm centroids.
     */
    @Nonnull
    static List<RealVector> normalizeAll(@Nonnull final List<? extends RealVector> vectors) {
        final ImmutableList.Builder<RealVector> b = ImmutableList.builderWithExpectedSize(vectors.size());
        for (final RealVector v : vectors) {
            b.add(v.normalize());
        }
        return b.build();
    }

    @Nonnull
    static RealVector gaussianND(@Nonnull final SplittableRandom random,
                                 @Nonnull final RealVector mean,
                                 final double sigma) {
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(random);
        final int d = mean.getNumDimensions();
        final double[] v = new double[d];
        for (int i = 0; i < d; i++) {
            final double z = sampler.nextGaussian();
            v[i] = mean.getComponent(i) + sigma * z;
        }
        return new DoubleRealVector(v);
    }

    @Nonnull
    static RealVector noisyUnitVector(@Nonnull final SplittableRandom random,
                                      @Nonnull final RealVector meanUnitVector,
                                      final double sigma) {
        return gaussianND(random, meanUnitVector, sigma).normalize();
    }

    /**
     * Internal "base objective" matching {@link KMeans}'s metric adapters: squared L2 for
     * Euclidean and cosine distance for cosine. {@link KMeans.Result#getDistances()} entries
     * and {@link KMeans.Result#getObjective()} are sums of this quantity.
     */
    static double baseObjective(@Nonnull final DistanceEstimator distanceEstimator,
                                @Nonnull final RealVector v,
                                @Nonnull final RealVector c) {
        switch (distanceEstimator.getMetric()) {
            case EUCLIDEAN_METRIC: {
                final double d = distanceEstimator.distance(v, c);
                return d * d;
            }
            case COSINE_METRIC:
                return distanceEstimator.distance(v, c);
            default:
                throw new UnsupportedOperationException("metric not supported in tests: " + distanceEstimator.getMetric());
        }
    }

    /**
     * Asserts the structural and (when {@code lambda == 0}) algorithmic invariants of a kmeans
     * result. The result must come from a {@code KMeans.fit(...)} call where both
     * {@code vectorLens} and {@code centroidLens} are
     * {@link com.apple.foundationdb.util.Lens#identity()}, so that centroids are themselves
     * {@link RealVector}s.
     * <p>
     * Invariants checked:
     * <ul>
     *   <li>{@code assignment.length == n}; every entry in {@code [0, k)}</li>
     *   <li>{@code centroids.size() == k}, {@code clusterSizes.length == k}</li>
     *   <li>{@code Σ clusterSizes == n} and {@code clusterSizes[c] == count(assignment == c)} for every c</li>
     *   <li>{@code distances.length == n} and {@code distances[i] ≈ baseObjective(vectors[i], centroids[assignment[i]])}</li>
     *   <li>{@code objective ≈ Σ distances}</li>
     *   <li>For cosine metric: every centroid is unit-norm or near-zero (reseed edge case)</li>
     *   <li>When {@code lambda == 0}: each {@code distances[i]} equals the minimum baseObjective across
     *       all centroids (local assignment optimality). {@link KMeans#fit} runs a final geometric
     *       reassignment pass, so this holds regardless of how the main loop exited.</li>
     * </ul>
     */
    static void assertKMeansInvariants(@Nonnull final KMeans.Result<RealVector> result,
                                       @Nonnull final List<? extends RealVector> vectors,
                                       final int k,
                                       @Nonnull final DistanceEstimator distanceEstimator,
                                       final double lambda) {
        final int n = vectors.size();
        final int[] assignment = result.assignment();
        final List<RealVector> centroids = result.clusterCentroids();
        final int[] clusterSizes = result.clusterSizes();
        final double[] distances = result.distances();
        final double objective = result.objective();

        // --- structural invariants ---
        assertThat(assignment).as("assignment length").hasSize(n);
        for (int i = 0; i < n; i++) {
            assertThat(assignment[i])
                    .as("assignment[%d] in [0, k)", i)
                    .isBetween(0, k - 1);
        }
        assertThat(centroids).as("centroid count").hasSize(k);
        assertThat(clusterSizes).as("clusterSizes length").hasSize(k);

        // recompute cluster sizes from assignment and compare
        final int[] expectedSizes = new int[k];
        for (int i = 0; i < n; i++) {
            expectedSizes[assignment[i]]++;
        }
        long sum = 0;
        for (int c = 0; c < k; c++) {
            assertThat(clusterSizes[c])
                    .as("clusterSizes[%d] matches count(assignment == %d)", c, c)
                    .isEqualTo(expectedSizes[c]);
            sum += clusterSizes[c];
        }
        assertThat(sum).as("Σ clusterSizes == n").isEqualTo(n);

        // --- distance and objective bookkeeping ---
        assertThat(distances).as("distances length").hasSize(n);
        // We check a relative tolerance per element since baseObjective values can span several
        // orders of magnitude on real data.
        double sumDistances = 0.0d;
        for (int i = 0; i < n; i++) {
            final RealVector centroid = centroids.get(assignment[i]);
            final double recomputed = baseObjective(distanceEstimator, vectors.get(i), centroid);
            assertThat(distances[i])
                    .as("distances[%d] matches baseObjective(vector, centroids[assignment])", i)
                    .isCloseTo(recomputed, withinRelative(recomputed, 1.0e-9d));
            sumDistances += distances[i];
        }
        assertThat(objective)
                .as("objective == Σ distances")
                .isCloseTo(sumDistances, withinRelative(sumDistances, 1.0e-9d));

        // --- cosine: centroids should be unit-norm (or near-zero, the reseed edge case) ---
        if (distanceEstimator.getMetric() == Metric.COSINE_METRIC) {
            for (int c = 0; c < k; c++) {
                final RealVector centroid = centroids.get(c);
                final double norm = centroid.l2Norm();
                if (norm > 1.0e-6d) {
                    assertThat(norm)
                            .as("cosine centroid[%d] is unit-norm", c)
                            .isCloseTo(1.0d, within(1.0e-6d));
                }
            }
        }

        // --- algorithmic invariants (only for unbiased assignment, lambda == 0) ---
        if (lambda == 0.0d) {
            for (int i = 0; i < n; i++) {
                double best = Double.POSITIVE_INFINITY;
                for (int c = 0; c < k; c++) {
                    best = Math.min(best, baseObjective(distanceEstimator, vectors.get(i), centroids.get(c)));
                }
                assertThat(distances[i])
                        .as("local assignment optimality at index %d", i)
                        .isCloseTo(best, withinRelative(best, 1.0e-9d));
            }
        }
    }

    /**
     * Builds an AssertJ {@code offset} for an isCloseTo check that uses an absolute floor and a
     * relative tolerance on top, so we don't fail on near-zero values where relative tolerance
     * becomes degenerate.
     */
    private static org.assertj.core.data.Offset<Double> withinRelative(final double reference,
                                                                       final double rel) {
        final double tol = Math.max(1.0e-9d, Math.abs(reference) * rel);
        return offset(tol);
    }
}
