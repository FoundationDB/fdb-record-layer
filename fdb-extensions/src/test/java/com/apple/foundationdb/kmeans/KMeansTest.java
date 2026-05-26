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

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.MutableDoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.util.Lens;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
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

    @TempDir
    Path tempDir;

    @BeforeAll
    static void loadSiftSmall() throws IOException {
        siftSmallBase = KMeansTestHelpers.loadSiftSmall();
    }

    /**
     * Generates two Gaussian blobs in 3D, runs k=2, and checks.
     * - both clusters non-empty
     * - centroids near the generating means (within a tolerance)
     * - objective significantly better than a "bad" baseline (single centroid at global mean, duplicated)
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void twoSeparatedBlobsFindsTwoClusters(final long seed) throws Exception {
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

        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                2, 15, 3, 0.0,
                null, true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, estimator, 0.0d);

        final RealVector c0 = res.clusterCentroids().get(0);
        final RealVector c1 = res.clusterCentroids().get(1);

        final double d00 = estimator.distance(m0, c0);
        final double d01 = estimator.distance(m0, c1);
        final double d10 = estimator.distance(m1, c0);
        final double d11 = estimator.distance(m1, c1);

        final double matchA = d00 + d11;
        final double matchB = d01 + d10;
        final double bestMatch = Math.min(matchA, matchB);

        assertThat(bestMatch).isLessThan(2.0d);

        final double baseline = baselineObjectiveSameCentroidTwice(vectors);
        assertThat(res.objective()).isLessThan(baseline * 0.65);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void oneBlobFindsTwoClusters(final long seed) throws Exception {
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

        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                2, 15, 3, 0.0,
                null, true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, estimator, 0.0d);

        assertThat(res.clusterSizes()[0]).isGreaterThan(0);
        assertThat(res.clusterSizes()[1]).isGreaterThan(0);
    }

    /**
     * Exercises k>2 plus balancing: 3 blobs in 3D, ask for k=3 and enable soft size balancing.
     * Checks:
     * - all clusters non-empty
     * - cluster sizes not wildly imbalanced
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void threeBlobsWithSoftBalancingProducesReasonableSizes(final long seed) throws Exception {
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
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty(),
                true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, estimator, lambda);

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
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                splittableRandom, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty(),
                true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, estimator, lambda);

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
    void twoSeparatedUnitNormalizedBlobsFindsTwoClustersCosine3D(final long seed) throws Exception {
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

        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 15, 3, 0.0,
                null, true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, estimator, 0.0d);

        final RealVector c0 = res.clusterCentroids().get(0).normalize();
        final RealVector c1 = res.clusterCentroids().get(1).normalize();

        final double d00 = estimator.distance(m0, c0);
        final double d01 = estimator.distance(m0, c1);
        final double d10 = estimator.distance(m1, c0);
        final double d11 = estimator.distance(m1, c1);

        final double matchA = d00 + d11;
        final double matchB = d01 + d10;
        final double bestMatch = Math.min(matchA, matchB);

        assertThat(bestMatch).isLessThan(0.20d);

        final double baseline = baselineObjectiveSameCentroidTwiceNormalized(vectors, estimator);
        assertThat(res.objective()).isLessThan(baseline * 0.65d);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void oneUnitNormalizedBlobFindsTwoClustersCosine3D(final long seed) throws Exception {
        final SplittableRandom rnd = new SplittableRandom(seed);

        final RealVector m0 = new DoubleRealVector(new double[] {-1.0, 0.0, 0.0}).normalize();

        final int nPer = 3000;
        final double sigma = 0.08d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(KMeansTestHelpers.noisyUnitVector(rnd, m0, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 15, 3, 0.0,
                null, true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, estimator, 0.0d);

        assertThat(res.clusterSizes()[0]).isGreaterThan(0);
        assertThat(res.clusterSizes()[1]).isGreaterThan(0);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void threeUnitNormalizedBlobsWithSoftBalancingProducesReasonableSizesCosine3D(final long seed) throws Exception {
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
        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty(),
                true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, estimator, lambda);

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
        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                splittableRandom, estimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 3, lambda,
                KMeans.overflowQuadraticPenalty(),
                true);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, estimator, lambda);

        for (int c = 0; c < k; c++) {
            assertThat(res.clusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.clusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.clusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        logger.info("cosine cluster sizes = {}", res.clusterSizes());
    }

    /**
     * Property-based: sample {@value SIFT_SAMPLE_SIZE} vectors from SIFT-small, run {@code fit}
     * across a grid of {@code k} and {@code lambda} values under the Euclidean metric, and
     * verify the structural and (where {@code lambda == 0}) algorithmic invariants of every
     * result.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void siftSmallEuclideanInvariants(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        for (final int k : new int[] {2, 5, 25, 100}) {
            for (final double lambda : new double[] {0.0d, 0.08d}) {
                final KMeans.Result<RealVector> res = KMeans.fit(
                        new SplittableRandom(seed),
                        estimator,
                        Lens.identity(), Lens.identity(),
                        sample,
                        k, /*maxIterations=*/30, /*maxRestarts=*/2, lambda,
                        lambda > 0.0d ? KMeans.overflowQuadraticPenalty() : null,
                        /*shuffleEachIteration=*/true);

                logger.info("siftSmallEuclidean seed={} k={} lambda={} sse={} sizes={}",
                        seed, k, lambda, res.objective(), res.clusterSizes());

                KMeansTestHelpers.assertKMeansInvariants(res, sample, k, estimator, lambda);
            }
        }
    }

    /**
     * Cosine variant of {@link #siftSmallEuclideanInvariants}: vectors are L2-normalized first so
     * the cosine metric and the unit-norm centroid invariant make sense.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void siftSmallCosineInvariants(final long seed) {
        final List<RealVector> raw = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final List<RealVector> sample = KMeansTestHelpers.normalizeAll(raw);
        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);

        for (final int k : new int[] {2, 5, 25, 100}) {
            for (final double lambda : new double[] {0.0d, 0.08d}) {
                final KMeans.Result<RealVector> res = KMeans.fit(
                        new SplittableRandom(seed),
                        estimator,
                        Lens.identity(), Lens.identity(),
                        sample,
                        k, /*maxIterations=*/30, /*maxRestarts=*/2, lambda,
                        lambda > 0.0d ? KMeans.overflowQuadraticPenalty() : null,
                        /*shuffleEachIteration=*/true);

                logger.info("siftSmallCosine seed={} k={} lambda={} obj={} sizes={}",
                        seed, k, lambda, res.objective(), res.clusterSizes());

                KMeansTestHelpers.assertKMeansInvariants(res, sample, k, estimator, lambda);
            }
        }
    }

    /**
     * Two runs with the same seed on the same data and parameters must produce bit-identical
     * results: assignment, centroid coordinates, distances, and overall objective.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void siftSmallDeterminism(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        final KMeans.Result<RealVector> a = KMeans.fit(
                new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(),
                sample, k, 20, 2, 0.0d, null, true);
        final KMeans.Result<RealVector> b = KMeans.fit(
                new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(),
                sample, k, 20, 2, 0.0d, null, true);

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
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        final double obj0 = KMeans.fit(new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 0, 0.0d, null, true).objective();
        final double obj1 = KMeans.fit(new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 1, 0.0d, null, true).objective();
        final double obj3 = KMeans.fit(new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 3, 0.0d, null, true).objective();

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
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        final double sse2 = KMeans.fit(new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(), sample, 2, 50, 3, 0.0d, null, true).objective();
        final double sse10 = KMeans.fit(new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(), sample, 10, 50, 3, 0.0d, null, true).objective();
        final double sse25 = KMeans.fit(new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(), sample, 25, 50, 3, 0.0d, null, true).objective();

        logger.info("k-monotonic SSE: 2->{}, 10->{}, 25->{}", sse2, sse10, sse25);
        // Generous slack accounts for the local-optimum nature of Lloyd; in pathological cases a
        // higher-k run might not strictly beat a lower-k run, but it should be no worse than a
        // small relative slack.
        assertThat(sse10).isLessThanOrEqualTo(sse2 * 1.001d);
        assertThat(sse25).isLessThanOrEqualTo(sse10 * 1.001d);
    }

    /**
     * Soft size balancing should reduce cluster-size variance: with {@code lambda > 0} and the
     * default overflow penalty, the largest-cluster fraction is no larger than without
     * balancing on the same sample/seed.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void siftSmallLambdaImprovesBalance(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        final KMeans.Result<RealVector> unbalanced = KMeans.fit(
                new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(),
                sample, k, 30, 2, 0.0d, null, true);
        final KMeans.Result<RealVector> balanced = KMeans.fit(
                new SplittableRandom(seed), estimator,
                Lens.identity(), Lens.identity(),
                sample, k, 30, 2, 0.08d,
                KMeans.overflowQuadraticPenalty(),
                true);

        final int unbalancedMax = Arrays.stream(unbalanced.clusterSizes()).max().orElseThrow();
        final int balancedMax = Arrays.stream(balanced.clusterSizes()).max().orElseThrow();

        logger.info("balance: unbalanced sizes={}, balanced sizes={}",
                unbalanced.clusterSizes(), balanced.clusterSizes());

        // Allow a small tolerance: tiny lambdas don't always strictly tighten balance.
        assertThat(balancedMax).isLessThanOrEqualTo((int)Math.round(unbalancedMax * 1.05d));
    }

    /**
     * Baseline: compute global mean, then pretend k=2 but both centroids are identical at global mean.
     * Objective = sum squared distances to that mean.
     */
    private static double baselineObjectiveSameCentroidTwice(@Nonnull final List<RealVector> pts) {
        final int d = pts.get(0).getNumDimensions();
        final MutableDoubleRealVector sum = MutableDoubleRealVector.zeroVector(d);
        for (final RealVector p : pts) {
            sum.add(p);
        }
        final RealVector m = sum.multiply(1.0d / pts.size()).toImmutable();

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
     * Baseline for cosine / normalized tests:
     * compute the global mean, normalize it to a unit vector, then pretend both centroids are that same vector.
     * Objective is computed using the provided estimator.
     */
    private static double baselineObjectiveSameCentroidTwiceNormalized(@Nonnull final List<RealVector> pts,
                                                                       @Nonnull final Estimator estimator) {
        final int d = pts.get(0).getNumDimensions();
        final MutableDoubleRealVector sum = MutableDoubleRealVector.zeroVector(d);
        for (final RealVector p : pts) {
            sum.add(p);
        }

        final RealVector meanDirection = sum.normalize().toImmutable();

        double obj = 0.0d;
        for (final RealVector p : pts) {
            obj += estimator.distance(p, meanDirection);
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
