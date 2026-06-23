/*
 * KMeansTest.java
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

import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.MutableDoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.util.Lens;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

class KMeansTest {
    private static final Logger logger = LoggerFactory.getLogger(KMeansTest.class);

    /** Number of vectors sampled from the SIFT-small base dataset for property tests. */
    private static final int SIFT_SAMPLE_SIZE = 2000;

    /** Cached SIFT-small base dataset. Loaded once per test class to amortize file I/O. */
    private static List<DoubleRealVector> siftSmallBase;

    @BeforeAll
    static void loadSiftSmall() throws IOException {
        siftSmallBase = KMeansTestHelpers.loadSiftSmall();
    }

    /**
     * Generates two Gaussian blobs in 3D, runs {@code k=2}, and checks the result.
     * <p>
     * Specifically:
     * <ul>
     *   <li>both clusters non-empty;</li>
     *   <li>centroids near the generating means (within a tolerance);</li>
     *   <li>objective significantly better than a "bad" baseline (single centroid at the global
     *       mean, duplicated).</li>
     * </ul>
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void twoSeparatedBlobsFindsTwoClusters(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);

        // Two well-separated means in 3D.
        final RealVector m0 = new DoubleRealVector(new double[] {-5.0, 0.0, 0.0});
        final RealVector m1 = new DoubleRealVector(new double[] {+5.0, 0.0, 0.0});

        final int nPer = 3000;
        final double sigma = 3.0d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(2 * nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(KMeansTestHelpers.gaussianND(rnd, m0, sigma));
        }
        for (int i = 0; i < nPer; i++) {
            vectors.add(KMeansTestHelpers.gaussianND(rnd, m1, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                2, 15, 3, 0.0,
                null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator, 0.0d);

        final RealVector c0 = res.clusterCentroids().get(0);
        final RealVector c1 = res.clusterCentroids().get(1);

        final double d00 = distanceEstimator.distance(m0, c0);
        final double d01 = distanceEstimator.distance(m0, c1);
        final double d10 = distanceEstimator.distance(m1, c0);
        final double d11 = distanceEstimator.distance(m1, c1);

        final double matchA = d00 + d11;
        final double matchB = d01 + d10;
        final double bestMatch = Math.min(matchA, matchB);

        assertThat(bestMatch).isLessThan(2.0d);

        final double baseline = baselineObjectiveSameCentroidTwice(vectors);
        assertThat(res.objective()).isLessThan(baseline * 0.65);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void oneBlobFindsTwoClusters(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);

        // One compact blob in 3D.
        final RealVector m0 = new DoubleRealVector(new double[] {-5.0, 0.0, 0.0});

        final int nPer = 3000;
        final double sigma = 0.5d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(KMeansTestHelpers.gaussianND(rnd, m0, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                2, 15, 3, 0.0,
                null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator, 0.0d);

        assertThat(res.clusterSizes()[0]).isGreaterThan(0);
        assertThat(res.clusterSizes()[1]).isGreaterThan(0);
    }

    /**
     * Exercises {@code k > 2} plus balancing: 3 blobs in 3D, ask for {@code k=3} and enable soft
     * size balancing.
     * <p>
     * Checks:
     * <ul>
     *   <li>all clusters non-empty;</li>
     *   <li>cluster sizes not wildly imbalanced.</li>
     * </ul>
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void threeBlobsWithSoftBalancingProducesReasonableSizes(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);

        final List<RealVector> means = ImmutableList.of(
                new DoubleRealVector(new double[] { -6.0d, -3.0d,  1.0d }),
                new DoubleRealVector(new double[] {  0.0d, +4.5d, -2.0d }),
                new DoubleRealVector(new double[] { +6.0d, -2.0d,  3.0d })
        );

        final int nPer = 2000;
        final double sigma = 3.0;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                vectors.add(KMeansTestHelpers.gaussianND(rnd, m, sigma));
            }
        }
        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final int k = 3;
        final double lambda = 0.08d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty());

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator, lambda);

        for (int c = 0; c < k; c++) {
            assertThat(res.clusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.clusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.clusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void threeBlobsWithSoftBalancingProducesReasonableSizesHighNumDimensions(final long seed) {
        final Random random = new Random(seed);
        final SplittableRandom splittableRandom = new SplittableRandom(seed);

        final int numDimensions = 512;

        final List<RealVector> means = ImmutableList.of(
                RealVectorTest.createRandomDoubleVector(random, numDimensions),
                RealVectorTest.createRandomDoubleVector(random, numDimensions),
                RealVectorTest.createRandomDoubleVector(random, numDimensions)
        );

        final int nPer = 2000;
        final double sigma = 3.0;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                vectors.add(KMeansTestHelpers.gaussianND(splittableRandom, m, sigma));
            }
        }
        Collections.shuffle(vectors, random);

        final int k = 3;
        final double lambda = 0.08d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                splittableRandom, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty());

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator, lambda);

        for (int c = 0; c < k; c++) {
            assertThat(res.clusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.clusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.clusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        logger.info("cluster sizes = {}", res.clusterSizes());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void twoSeparatedUnitNormalizedBlobsFindsTwoClustersCosine3D(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);

        final RealVector m0 = new DoubleRealVector(new double[] {-1.0, 0.0, 0.0}).normalize();
        final RealVector m1 = new DoubleRealVector(new double[] {+1.0, 0.0, 0.0}).normalize();

        final int nPer = 3000;
        final double sigma = 0.18d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(2 * nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(KMeansTestHelpers.noisyUnitVector(rnd, m0, sigma));
        }
        for (int i = 0; i < nPer; i++) {
            vectors.add(KMeansTestHelpers.noisyUnitVector(rnd, m1, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 15, 3, 0.0,
                null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator, 0.0d);

        final RealVector c0 = res.clusterCentroids().get(0).normalize();
        final RealVector c1 = res.clusterCentroids().get(1).normalize();

        final double d00 = distanceEstimator.distance(m0, c0);
        final double d01 = distanceEstimator.distance(m0, c1);
        final double d10 = distanceEstimator.distance(m1, c0);
        final double d11 = distanceEstimator.distance(m1, c1);

        final double matchA = d00 + d11;
        final double matchB = d01 + d10;
        final double bestMatch = Math.min(matchA, matchB);

        assertThat(bestMatch).isLessThan(0.20d);

        final double baseline = baselineObjectiveSameCentroidTwiceNormalized(vectors, distanceEstimator);
        assertThat(res.objective()).isLessThan(baseline * 0.65d);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void oneUnitNormalizedBlobFindsTwoClustersCosine3D(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);

        final RealVector m0 = new DoubleRealVector(new double[] {-1.0, 0.0, 0.0}).normalize();

        final int nPer = 3000;
        final double sigma = 0.08d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(KMeansTestHelpers.noisyUnitVector(rnd, m0, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 15, 3, 0.0,
                null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator, 0.0d);

        assertThat(res.clusterSizes()[0]).isGreaterThan(0);
        assertThat(res.clusterSizes()[1]).isGreaterThan(0);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void threeUnitNormalizedBlobsWithSoftBalancingProducesReasonableSizesCosine3D(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);

        final List<RealVector> means = ImmutableList.of(
                new DoubleRealVector(new double[] {1.0d, 1.0d, 0.0d}).normalize(),
                new DoubleRealVector(new double[] {0.0d, 1.0d, 1.0d}).normalize(),
                new DoubleRealVector(new double[] {1.0d, 0.0d, 1.0d}).normalize()
        );

        final int nPer = 2000;
        final double sigma = 0.4d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                vectors.add(KMeansTestHelpers.noisyUnitVector(rnd, m, sigma));
            }
        }
        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final int k = 3;
        final double lambda = 0.08d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty());

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator, lambda);

        for (int c = 0; c < k; c++) {
            assertThat(res.clusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.clusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.clusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void threeUnitNormalizedBlobsWithSoftBalancingProducesReasonableSizesCosineHighNumDimensions(final long seed) {
        final Random random = new Random(seed);
        final SplittableRandom splittableRandom = new SplittableRandom(seed);

        final int numDimensions = 512;

        final List<RealVector> means = ImmutableList.of(
                RealVectorTest.createRandomDoubleVector(random, numDimensions).normalize(),
                RealVectorTest.createRandomDoubleVector(random, numDimensions).normalize(),
                RealVectorTest.createRandomDoubleVector(random, numDimensions).normalize());

        final int nPer = 2000;
        final double sigma = 0.10d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                vectors.add(KMeansTestHelpers.noisyUnitVector(splittableRandom, m, sigma));
            }
        }
        Collections.shuffle(vectors, random);

        final int k = 3;
        final double lambda = 0.08d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                splittableRandom, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty());

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator, lambda);

        for (int c = 0; c < k; c++) {
            assertThat(res.clusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.clusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.clusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        logger.info("cosine cluster sizes = {}", res.clusterSizes());
    }

    /**
     * Property-based: sample {@value #SIFT_SAMPLE_SIZE} vectors from SIFT-small, run {@code fit}
     * across a grid of {@code k} and {@code lambda} values under the Euclidean metric, and
     * verify the structural and (where {@code lambda == 0}) algorithmic invariants of every
     * result.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void siftSmallEuclideanInvariants(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        for (final int k : new int[] {2, 5, 25, 100}) {
            for (final double lambda : new double[] {0.0d, 0.08d}) {
                final KMeans.Result<RealVector> res = KMeans.fit(
                        new SplittableRandom(seed),
                        distanceEstimator,
                        Lens.identity(), Lens.identity(),
                        sample,
                        k, /*maxIterations=*/30, /*maxRestarts=*/2, lambda,
                        lambda > 0.0d ? KMeans.overflowQuadraticPenalty() : null);

                logger.info("siftSmallEuclidean seed={} k={} lambda={} sse={} sizes={}",
                        seed, k, lambda, res.objective(), res.clusterSizes());

                KMeansTestHelpers.assertKMeansInvariants(res, sample, k, distanceEstimator, lambda);
            }
        }
    }

    /**
     * Cosine variant of {@link #siftSmallEuclideanInvariants}: vectors are L2-normalized first so
     * the cosine metric and the unit-norm centroid invariant make sense.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    @Tag(Tags.DualScalarSIMD)
    void siftSmallCosineInvariants(final long seed) {
        final List<RealVector> raw = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final List<RealVector> sample = KMeansTestHelpers.normalizeAll(raw);
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);

        for (final int k : new int[] {2, 5, 25, 100}) {
            for (final double lambda : new double[] {0.0d, 0.08d}) {
                final KMeans.Result<RealVector> res = KMeans.fit(
                        new SplittableRandom(seed),
                        distanceEstimator,
                        Lens.identity(), Lens.identity(),
                        sample,
                        k, /*maxIterations=*/30, /*maxRestarts=*/2, lambda,
                        lambda > 0.0d ? KMeans.overflowQuadraticPenalty() : null);

                logger.info("siftSmallCosine seed={} k={} lambda={} obj={} sizes={}",
                        seed, k, lambda, res.objective(), res.clusterSizes());

                KMeansTestHelpers.assertKMeansInvariants(res, sample, k, distanceEstimator, lambda);
            }
        }
    }

    /**
     * Two runs with the same seed on the same data and parameters must produce bit-identical
     * results: assignment, centroid coordinates, distances, and overall objective.
     * <p>
     * Tagged {@link Tags#RequiresScalar} so it runs only under {@code scalarFallbackTest} (which
     * sets {@code fdb.vector.simd=scalar}). SIMD reductions sum partial lanes in a different order
     * than scalar accumulation, which breaks the bit-exact equality on the distances and objective
     * comparisons below.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    @Tag(Tags.RequiresScalar)
    void siftSmallDeterminism(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        final KMeans.Result<RealVector> a = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, k, 20, 2, 0.0d, null);
        final KMeans.Result<RealVector> b = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, k, 20, 2, 0.0d, null);

        assertThat(a.assignment()).containsExactly(b.assignment());
        assertThat(a.clusterSizes()).containsExactly(b.clusterSizes());
        assertThat(a.distances()).containsExactly(b.distances());
        assertThat(a.objective()).isEqualTo(b.objective());

        for (int c = 0; c < k; c++) {
            final RealVector ca = a.clusterCentroids().get(c);
            final RealVector cb = b.clusterCentroids().get(c);
            for (int d = 0; d < ca.getNumDimensions(); d++) {
                assertThat(ca.getComponent(d)).isEqualTo(cb.getComponent(d));
            }
        }
    }

    /**
     * Extra restarts can only help: the best-of-R objective must be monotonically non-increasing
     * in {@code R}.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void siftSmallRestartsImproveObjective(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        final double obj0 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 0, 0.0d, null).objective();
        final double obj1 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 1, 0.0d, null).objective();
        final double obj3 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 3, 0.0d, null).objective();

        logger.info("restarts: 0->{}, 1->{}, 3->{}", obj0, obj1, obj3);
        assertThat(obj1).isLessThanOrEqualTo(obj0);
        assertThat(obj3).isLessThanOrEqualTo(obj1);
    }

    /**
     * More clusters cannot increase the squared-L2 objective: with sufficient iterations and
     * restarts, SSE should be non-increasing as {@code k} grows.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void siftSmallMoreClustersLowerSse(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        final double sse2 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 2, 50, 3, 0.0d, null).objective();
        final double sse10 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 10, 50, 3, 0.0d, null).objective();
        final double sse25 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 25, 50, 3, 0.0d, null).objective();

        logger.info("k-monotonic SSE: 2->{}, 10->{}, 25->{}", sse2, sse10, sse25);
        // Generous slack accounts for the local-optimum nature of Lloyd; in pathological cases a
        // higher-k run might not strictly beat a lower-k run, but it should be no worse than a
        // small relative slack.
        assertThat(sse10).isLessThanOrEqualTo(sse2 * 1.001d);
        assertThat(sse25).isLessThanOrEqualTo(sse10 * 1.001d);
    }

    /**
     * Soft size balancing should reduce cluster-size variance: with {@code lambda > 0} and the
     * default overflow penalty, the standard deviation of cluster sizes should — <i>averaged
     * across seeds</i> — be smaller than without balancing.
     * <p>
     * The metric is intentionally the standard deviation, not the maximum cluster size. The
     * overflow-quadratic penalty literally minimizes a sum of squared overflows, so what it
     * actually optimizes is closer to variance than to max — and max alone is a fragile proxy
     * that can go up while variance goes down (the penalty might be redistributing the small
     * clusters more than shaving the largest). Asserting on standard deviation tracks the
     * property the penalty is optimizing.
     * <p>
     * Aggregating across multiple seeds smooths out single-seed variance in the local-minimum
     * landscape both runs are converging into.
     * <p>
     * The {@code lambda} value used here ({@code 1000}) is large compared to the synthetic-blob
     * tests above ({@code lambda = 0.08}). That's because {@code lambda} is dimensional: the
     * penalty term is added to the per-vector base objective (squared L2 distance to the
     * centroid), and SIFT-small components run 0–127 so per-vector squared distances are in the
     * thousands. To make the penalty visible against that, {@code lambda} has to be in the
     * thousands too. The 3D-blob tests get away with tiny {@code lambda} because their
     * geometric scores are in the single digits.
     */
    @Test
    void siftSmallLambdaImprovesBalance() {
        final long[] seeds = {0x0fdbL, 0x5ca1eL, 0xC0FFEEL, 123456L, 78910L,
                0xDEADBEEFL, 0xBADCAFEL, 1123581321345589L};
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        double ratioSum = 0.0d;
        int balancedWins = 0;
        for (final long seed : seeds) {
            final List<RealVector> sample = ImmutableList.copyOf(
                    KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));

            final KMeans.Result<RealVector> unbalanced = KMeans.fit(
                    new SplittableRandom(seed), distanceEstimator,
                    Lens.identity(), Lens.identity(),
                    sample, k, 30, 2, 0.0d, null);
            final KMeans.Result<RealVector> balanced = KMeans.fit(
                    new SplittableRandom(seed), distanceEstimator,
                    Lens.identity(), Lens.identity(),
                    sample, k, 30, 2, 1000d,
                    KMeans.overflowQuadraticPenalty());

            final double unbalancedStddev = clusterSizeStddev(unbalanced.clusterSizes());
            final double balancedStddev = clusterSizeStddev(balanced.clusterSizes());
            final double ratio = balancedStddev / unbalancedStddev;
            ratioSum += ratio;
            if (balancedStddev <= unbalancedStddev) {
                balancedWins++;
            }

            logger.info("balance@seed={}: unbalanced stddev={}, balanced stddev={}, ratio={}",
                    Long.toHexString(seed), unbalancedStddev, balancedStddev, ratio);
        }

        final double meanRatio = ratioSum / seeds.length;
        logger.info("siftSmallLambdaImprovesBalance: meanRatio={} over {} seeds, balanced won {}/{}",
                meanRatio, seeds.length, balancedWins, seeds.length);

        // Averaged across seeds, balancing should reduce the standard deviation of cluster sizes
        // — that's the quantity the overflow-quadratic penalty literally targets.
        assertThat(meanRatio)
                .as("mean balancedStddev / unbalancedStddev across %d seeds (per-seed ratios in the log)",
                        seeds.length)
                .isLessThanOrEqualTo(1.0d);
    }

    /**
     * Standard deviation of an {@code int[]} treated as a sample, using the population formula
     * (divide by {@code n}, not {@code n-1}). Used by
     * {@link #siftSmallLambdaImprovesBalance()} to measure cluster-size dispersion.
     */
    private static double clusterSizeStddev(@Nonnull final int[] sizes) {
        final double mean = Arrays.stream(sizes).average().orElseThrow();
        double sumSquared = 0.0d;
        for (final int s : sizes) {
            final double delta = s - mean;
            sumSquared += delta * delta;
        }
        return Math.sqrt(sumSquared / sizes.length);
    }

    // ============================================================================================
    // unit tests for the internal reseed helper
    // ============================================================================================

    /**
     * {@link KMeans#farthestVectorIndex} returns the index of the data point whose minimum
     * (metric-specific) base objective to any centroid is maximal — i.e. the point currently
     * <i>worst</i>-served by the centroid set. The {@code fit} loop calls it to reseed empty or
     * degenerate clusters during the centroid-update step.
     * <p>
     * This test pins the contract on a hand-built configuration where the answer is unambiguous:
     * with two centroids on the x-axis at {@code ±5}, a point sitting far up the y-axis is
     * roughly equidistant from both centroids and much farther than any nearby x-axis point.
     */
    @Test
    void farthestVectorIndexPicksWorstServedPointEuclidean() {
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.MetricAdapter adapter = KMeans.fromEstimator(distanceEstimator);

        final List<RealVector> centroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d}));

        final List<RealVector> vectors = ImmutableList.of(
                // index 0: hugs the -5 centroid (sq-dist to nearest = 1)
                new DoubleRealVector(new double[] {-4.0d,  0.0d, 0.0d}),
                // index 1: hugs the +5 centroid (sq-dist to nearest = 1)
                new DoubleRealVector(new double[] { 4.0d,  0.0d, 0.0d}),
                // index 2: between centroids, near origin (sq-dist to nearest ≈ 26)
                new DoubleRealVector(new double[] { 0.0d,  5.0d, 0.0d}),
                // index 3: far up the y-axis (sq-dist to nearest ≈ 2525) <-- the worst served
                new DoubleRealVector(new double[] { 0.0d, 50.0d, 0.0d}));

        final int idx = KMeans.farthestVectorIndex(adapter, Lens.identity(), vectors, centroids);

        assertThat(idx).isEqualTo(3);
    }

    /**
     * On ties, the loop's strict {@code >} preserves the earliest worst-served point. We pin
     * that behavior here so a future refactor that flips to {@code >=} (which would silently
     * shift the reseed target) is caught.
     */
    @Test
    void farthestVectorIndexBreaksTiesByEarliestIndex() {
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.MetricAdapter adapter = KMeans.fromEstimator(distanceEstimator);

        final List<RealVector> centroids = ImmutableList.of(
                new DoubleRealVector(new double[] {0.0d, 0.0d, 0.0d}));

        // Three points on a sphere of radius 10 around the single centroid: all equally bad.
        final List<RealVector> vectors = ImmutableList.of(
                new DoubleRealVector(new double[] {10.0d,  0.0d,  0.0d}),
                new DoubleRealVector(new double[] { 0.0d, 10.0d,  0.0d}),
                new DoubleRealVector(new double[] { 0.0d,  0.0d, 10.0d}));

        final int idx = KMeans.farthestVectorIndex(adapter, Lens.identity(), vectors, centroids);

        assertThat(idx).isEqualTo(0);
    }

    /**
     * Computes a "bad" baseline objective for the squared-L2 case.
     * <p>
     * Computes the global mean, then pretends {@code k=2} but both centroids are identical at
     * the global mean. The objective is the sum of squared distances from each point to that
     * mean.
     */
    private static double baselineObjectiveSameCentroidTwice(@Nonnull final List<RealVector> pts) {
        final int d = pts.get(0).getNumDimensions();
        final MutableDoubleRealVector sum = MutableDoubleRealVector.zeroVector(d);
        for (final RealVector p : pts) {
            sum.addToThis(p);
        }
        final RealVector m = sum.multiplyThisBy(1.0d / pts.size()).toImmutable();

        double obj = 0.0;
        for (final RealVector p : pts) {
            final RealVector diff = p.subtract(m);
            for (int i = 0; i < d; i++) {
                final double x = diff.getComponent(i);
                obj += x * x;
            }
        }
        return obj;
    }

    /**
     * Computes a "bad" baseline objective for the cosine / normalized case.
     * <p>
     * Computes the global mean, normalizes it to a unit vector, then pretends both centroids are
     * that same vector. The objective is computed using the provided {@link DistanceEstimator}.
     */
    private static double baselineObjectiveSameCentroidTwiceNormalized(@Nonnull final List<RealVector> pts,
                                                                       @Nonnull final DistanceEstimator distanceEstimator) {
        final int d = pts.get(0).getNumDimensions();
        final MutableDoubleRealVector sum = MutableDoubleRealVector.zeroVector(d);
        for (final RealVector p : pts) {
            sum.addToThis(p);
        }

        final RealVector meanDirection = sum.normalizeThis().toImmutable();

        double obj = 0.0d;
        for (final RealVector p : pts) {
            obj += distanceEstimator.distance(p, meanDirection);
        }
        return obj;
    }

    @SuppressWarnings("unused")
    static void dumpVectors(@Nonnull final Path tempDir,
                            @Nonnull final String prefix,
                            @Nonnull final List<RealVector> vectors) throws IOException {
        final Path vectorsFile = tempDir.resolve(prefix + ".csv");

        try (final BufferedWriter vectorsWriter = Files.newBufferedWriter(vectorsFile)) {
            long id = 0;
            for (final RealVector vector : vectors) {
                final StringBuilder sb = new StringBuilder();
                sb.append(id);
                for (int i = 0; i < vector.getNumDimensions(); i++) {
                    sb.append(',').append(vector.getComponent(i));
                }
                vectorsWriter.write(sb.toString());
                vectorsWriter.newLine();
                id++;
            }
        }
    }
}
