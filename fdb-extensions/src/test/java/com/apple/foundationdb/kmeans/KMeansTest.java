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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
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
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.within;

@Execution(ExecutionMode.CONCURRENT)
class KMeansTest {
    private static final Logger logger = LoggerFactory.getLogger(KMeansTest.class);

    /** Number of vectors sampled from the SIFT-small base dataset for property tests. */
    private static final int SIFT_SAMPLE_SIZE = 2000;

    /**
     * Number of vectors sampled from SIFT-small for the run-selection tests: small enough that 3-means
     * on them has several distinct local optima for its runs to land in.
     */
    private static final int RUN_SELECTION_SAMPLE_SIZE = 300;

    /** Number of clusters the run-selection tests fit. */
    private static final int RUN_SELECTION_K = 3;

    /** Number of runs the run-selection tests make. */
    private static final int RUN_SELECTION_NUM_RUNS = 8;

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
                2, 15, 4, 0.0,
                null, null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator);

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
                2, 15, 4, 0.0,
                null, null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator);

        assertThat(res.clusterSizes()[0]).isGreaterThan(0);
        assertThat(res.clusterSizes()[1]).isGreaterThan(0);
    }

    /**
     * k=1 has a closed-form optimum: a single cluster holding every vector, whose centroid is the
     * arithmetic mean. This is the default 2→1 merge path. Verifies {@link KMeans#fit} returns that
     * (mean centroid, single full cluster, all-zero assignment) under the Euclidean metric.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void singleClusterReturnsMeanEuclidean(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final RealVector blobMean = new DoubleRealVector(new double[] {1.0d, -2.0d, 3.0d});
        final int n = 500;
        final List<RealVector> vectors = Lists.newArrayListWithCapacity(n);
        for (int i = 0; i < n; i++) {
            vectors.add(KMeansTestHelpers.gaussianND(rnd, blobMean, 2.0d));
        }

        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 1;
        final int maxIterations = 15;
        final int numRuns = 4;
        final double relativeLambda = 0.0d; // size-balancing strength; 0 disables the size penalty
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, maxIterations, numRuns, relativeLambda,
                null /* sizePenalty */, null /* sizePredicate */);

        assertThat(res.clusterCentroids()).hasSize(1);
        assertThat(res.clusterSizes()).containsExactly(n);
        assertThat(res.assignment()).containsOnly(0);

        // res's centroid and arithmeticMean(...) compute the mean of the same n vectors with the same summation, so
        // in practice they are bit-identical and this distance is 0. A tolerance only needs to cover the two sums
        // running in different (equally valid) orders. Recursive summation of n doubles has worst-case forward error
        // ~(n-1)*u*sum(|x_i|), where u = 2^-53 is the double unit roundoff, so two orders differ by ~2*u*sum(|x_i|)
        // per coordinate after dividing by n. Here sum(|x_i|) per coordinate is ~n * O(1) ~ 1.5e3, so that ceiling is
        // ~2 * 1.1e-16 * 1.5e3 ~ 3e-13 per coordinate, i.e. an L2 gap of a few 1e-13. 2E-10 is a few hundred times
        // looser than that ceiling -- deliberately loose to stay a simple constant -- yet still far below the O(1)
        // error a real bug (a wrong or unnormalized centroid) would produce.
        final RealVector expectedMean = arithmeticMean(vectors);
        assertThat(distanceEstimator.distance(res.clusterCentroids().get(0), expectedMean))
                .as("k=1 Euclidean centroid must equal the arithmetic mean")
                .isCloseTo(0.0d, within(2E-10));

        // Recompute the geometric objective independently from the returned centroid and assignment, then check
        // objective() AND the per-vector distances() against it. Asserting only objective() == sum(distances()) is
        // true by construction (objective is defined as that sum) and would not catch a systematic error shared by
        // both. For k=1 every vector is assigned to the single centroid, and the Euclidean base objective of a vector
        // is its squared L2 distance to that centroid.
        final RealVector centroid = res.clusterCentroids().get(0);
        double expectedObjective = 0.0d;
        for (final RealVector v : vectors) {
            final double d = distanceEstimator.distance(v, centroid);
            expectedObjective += d * d;
        }
        // A sum of n squared distances; summation error grows with n and with the magnitude (ulp) of the running sum.
        final double objectiveTolerance = 8.0 * n * Math.ulp(expectedObjective);
        assertThat(res.objective())
                .as("objective must equal the independently computed sum of squared distances")
                .isCloseTo(expectedObjective, within(objectiveTolerance));
        assertThat(Arrays.stream(res.distances()).sum())
                .as("sum of per-vector distances must equal the independently computed objective")
                .isCloseTo(expectedObjective, within(objectiveTolerance));
    }

    /**
     * k=1 under cosine: the single centroid is the <em>normalized</em> mean (the unit-sphere mean
     * direction), since the cosine metric adapter renormalizes averaged centroids.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void singleClusterReturnsNormalizedMeanCosine(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final RealVector blobMean = new DoubleRealVector(new double[] {1.0d, -2.0d, 3.0d});
        final int n = 500;
        final List<RealVector> raw = Lists.newArrayListWithCapacity(n);
        for (int i = 0; i < n; i++) {
            raw.add(KMeansTestHelpers.gaussianND(rnd, blobMean, 2.0d));
        }
        final List<RealVector> vectors = KMeansTestHelpers.normalizeAll(raw);

        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                1, 15, 4, 0.0d,
                null, null);

        assertThat(res.clusterCentroids()).hasSize(1);
        assertThat(res.clusterSizes()).containsExactly(n);
        assertThat(res.assignment()).containsOnly(0);

        final RealVector centroid = res.clusterCentroids().get(0);
        assertThat(centroid.l2Norm())
                .as("cosine k=1 centroid must lie on the unit sphere")
                .isCloseTo(1.0d, within(2E-10));
        final RealVector expected = arithmeticMean(vectors).normalize();
        assertThat(Metric.EUCLIDEAN_METRIC.distance(centroid, expected))
                .as("cosine k=1 centroid must equal the normalized mean")
                .isCloseTo(0.0d, within(2E-9));
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
        // Against the input's per-vector single-cluster SSE s of about 66, this is an absolute weight of about 0.08.
        final double relativeLambda = 0.0012d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 4, relativeLambda,
                KMeans.overflowQuadraticPenalty(), null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator);

        for (int c = 0; c < k; c++) {
            assertThat(res.clusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.clusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.clusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);
    }

    /**
     * High-dimensional twin of {@link #threeBlobsWithSoftBalancingProducesReasonableSizes}: the same checks on 3
     * blobs in 512 dimensions, with the same relative balancing strength, which carries over unchanged because it is
     * measured against the input's own per-vector single-cluster SSE {@code s}. (An absolute weight does not carry
     * over: {@code s} is about 4700 here, so the same absolute weight would be 70 times weaker than in 3D.)
     */
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
        final double relativeLambda = 0.0012d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                splittableRandom, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 4, relativeLambda,
                KMeans.overflowQuadraticPenalty(), null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator);

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
                vectors, 2, 15, 4, 0.0,
                null, null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator);

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
                vectors, 2, 15, 4, 0.0,
                null, null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, 2, distanceEstimator);

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
        // Against the input's per-vector single-cluster cosine distance s of about 0.31, this is an absolute weight
        // of about 0.08.
        final double relativeLambda = 0.25d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                rnd, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 4, relativeLambda,
                KMeans.overflowQuadraticPenalty(), null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator);

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
        // Against the input's per-vector single-cluster cosine distance s of about 0.77, this is an absolute weight
        // of about 0.08.
        final double relativeLambda = 0.1d;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);
        final KMeans.Result<RealVector> res = KMeans.fit(
                splittableRandom, distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 4, relativeLambda,
                KMeans.overflowQuadraticPenalty(), null);

        KMeansTestHelpers.assertKMeansInvariants(res, vectors, k, distanceEstimator);

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
     * across a grid of {@code k} and relative balancing strengths under the Euclidean metric, and
     * verify the structural and algorithmic invariants of every result.
     * <p>
     * The non-zero strength is the one of the cosine twin {@link #siftSmallCosineInvariants}, where it
     * amounts to an absolute weight of about 0.08. Being relative, it acts here just as well; the same
     * absolute weight would not: against squared distances of about {@code 1.5e5} per vector it is a
     * relative strength of {@code 5e-7}, which would leave the invariants under balancing untested.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void siftSmallEuclideanInvariants(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        for (final int k : new int[] {2, 5, 25, 100}) {
            for (final double relativeLambda : new double[] {0.0d, 0.25d}) {
                final KMeans.Result<RealVector> res = KMeans.fit(
                        new SplittableRandom(seed),
                        distanceEstimator,
                        Lens.identity(), Lens.identity(),
                        sample,
                        k, /*maxIterations=*/30, /*numRuns=*/3, relativeLambda,
                        relativeLambda > 0.0d ? KMeans.overflowQuadraticPenalty() : null, null);

                logger.info("siftSmallEuclidean seed={} k={} relativeLambda={} sse={} sizes={}",
                        seed, k, relativeLambda, res.objective(), res.clusterSizes());

                KMeansTestHelpers.assertKMeansInvariants(res, sample, k, distanceEstimator);
            }
        }
    }

    /**
     * Cosine variant of {@link #siftSmallEuclideanInvariants}: vectors are L2-normalized first so
     * the cosine metric and the unit-norm centroid invariant make sense. The sample's per-vector
     * single-cluster cosine distance is about 0.35, so the non-zero relative strength amounts to an
     * absolute weight of about 0.08.
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
            for (final double relativeLambda : new double[] {0.0d, 0.25d}) {
                final KMeans.Result<RealVector> res = KMeans.fit(
                        new SplittableRandom(seed),
                        distanceEstimator,
                        Lens.identity(), Lens.identity(),
                        sample,
                        k, /*maxIterations=*/30, /*numRuns=*/3, relativeLambda,
                        relativeLambda > 0.0d ? KMeans.overflowQuadraticPenalty() : null, null);

                logger.info("siftSmallCosine seed={} k={} relativeLambda={} obj={} sizes={}",
                        seed, k, relativeLambda, res.objective(), res.clusterSizes());

                KMeansTestHelpers.assertKMeansInvariants(res, sample, k, distanceEstimator);
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
                sample, k, 20, 3, 0.0d, null, null);
        final KMeans.Result<RealVector> b = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, k, 20, 3, 0.0d, null, null);

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
     * Extra runs can only help: the best-of-R objective must be monotonically non-increasing
     * in the number of runs {@code R}.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void siftSmallMoreRunsImproveObjective(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        final double obj1 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 1, 0.0d, null, null).objective();
        final double obj2 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 2, 0.0d, null, null).objective();
        final double obj4 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, k, 30, 4, 0.0d, null, null).objective();

        logger.info("runs: 1->{}, 2->{}, 4->{}", obj1, obj2, obj4);
        assertThat(obj2).isLessThanOrEqualTo(obj1);
        assertThat(obj4).isLessThanOrEqualTo(obj2);
    }

    /**
     * More clusters cannot increase the squared-L2 objective: with sufficient iterations and
     * runs, SSE should be non-increasing as {@code k} grows.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void siftSmallMoreClustersLowerSse(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        final double sse2 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 2, 50, 4, 0.0d, null, null).objective();
        final double sse10 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 10, 50, 4, 0.0d, null, null).objective();
        final double sse25 = KMeans.fit(new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 25, 50, 4, 0.0d, null, null).objective();

        logger.info("k-monotonic SSE: 2->{}, 10->{}, 25->{}", sse2, sse10, sse25);
        // Generous slack accounts for the local-optimum nature of Lloyd; in pathological cases a
        // higher-k run might not strictly beat a lower-k run, but it should be no worse than a
        // small relative slack.
        assertThat(sse10).isLessThanOrEqualTo(sse2 * 1.001d);
        assertThat(sse25).isLessThanOrEqualTo(sse10 * 1.001d);
    }

    /**
     * Soft size balancing should reduce cluster-size variance: with {@code relativeLambda > 0} and the
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
     * Aggregating across 32 inputs derived from the seed smooths out the variance of the local
     * minimum each run converges into.
     * <p>
     * The strength is relative to the sample's per-vector single-cluster SSE {@code s}, which is
     * about {@code 1.5e5} for SIFT-small: {@code relativeLambda = 0.007} amounts to an absolute
     * weight of about {@code 1000}. The cluster sizes compared are those of the returned,
     * nearest-centroid partitioning, so the test shows that the penalty moves the centroids.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void siftSmallLambdaImprovesBalance(final long seed) {
        final SplittableRandom inputSeeds = new SplittableRandom(seed);
        final int numInputs = 32;
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;

        double ratioSum = 0.0d;
        int balancedWins = 0;
        for (int input = 0; input < numInputs; input++) {
            final long inputSeed = inputSeeds.nextLong();
            final List<RealVector> sample = ImmutableList.copyOf(
                    KMeansTestHelpers.pickRandomSubset(new Random(inputSeed), siftSmallBase, SIFT_SAMPLE_SIZE));

            final KMeans.Result<RealVector> unbalanced = KMeans.fit(
                    new SplittableRandom(inputSeed), distanceEstimator,
                    Lens.identity(), Lens.identity(),
                    sample, k, 30, 3, 0.0d, null, null);
            final KMeans.Result<RealVector> balanced = KMeans.fit(
                    new SplittableRandom(inputSeed), distanceEstimator,
                    Lens.identity(), Lens.identity(),
                    sample, k, 30, 3, 0.007d,
                    KMeans.overflowQuadraticPenalty(), null);

            final double unbalancedStddev = clusterSizeStddev(unbalanced.clusterSizes());
            final double balancedStddev = clusterSizeStddev(balanced.clusterSizes());
            final double ratio = balancedStddev / unbalancedStddev;
            ratioSum += ratio;
            if (balancedStddev <= unbalancedStddev) {
                balancedWins++;
            }

            logger.info("balance@seed={}: unbalanced stddev={}, balanced stddev={}, ratio={}",
                    Long.toHexString(inputSeed), unbalancedStddev, balancedStddev, ratio);
        }

        final double meanRatio = ratioSum / numInputs;
        logger.info("siftSmallLambdaImprovesBalance: meanRatio={} over {} inputs, balanced won {}/{}",
                meanRatio, numInputs, balancedWins, numInputs);

        // Averaged across seeds, balancing should reduce the standard deviation of cluster sizes
        // — that's the quantity the overflow-quadratic penalty literally targets. Measured: a mean
        // ratio of about 0.81, with a per-input standard deviation of about 0.26.
        assertThat(meanRatio)
                .as("mean balancedStddev / unbalancedStddev across %d inputs (per-input ratios in the log)",
                        numInputs)
                .isLessThanOrEqualTo(1.0d);
    }

    /**
     * Check the scale invariance of the relative lambda parameter. This validates that we get the same
     * balancing effect on cluster sizes at different scaling factors. This requires a sample from the data
     * set where the tested lambda results in a noticeable change in cluster sizes. This is not guaranteed
     * from every random sample, so we re-roll whenever the tested sample fails to test anything.
     *
     * @see #runScaleInvarianceWhereBalancingFires(long) for more details
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    @Tag(Tags.DualScalarSIMD)
    void siftSmallRelativeLambdaIsScaleInvariant(final long seed) {
        final Random random = new Random(seed);
        final int trials = 100;
        for (int i = 0; i < trials; i++) {
            final long trialSeed = random.nextLong();
            try {
                if (runScaleInvarianceWhereBalancingFires(trialSeed)) {
                    return;
                }
            } catch (AssertionError e) {
                fail("trial " + i + " (seed=" + trialSeed + ") failed: " + e.getMessage(), e);
            }
        }
        fail("Could not find a sample on which balancing evens out the cluster sizes after " + trials + " trials");
    }

    /**
     * Checks the scale invariance of the lambda parameter for a single sample.
     * The balancing strength is relative, which makes it invariant under scaling the input: fitting
     * {@code Y = 1024 * X} with the same seed and the same {@code relativeLambda} yields exactly the
     * partitioning of {@code X}. Both terms of the assignment score scale alike: the squared-L2 base
     * objective by {@code 1024^2}, and so does the absolute weight {@code relativeLambda * s}, as
     * {@code s}, the per-vector SSE of the single-cluster fit, is a squared distance itself. That is
     * what makes one {@code relativeLambda} mean the same thing at every scale; an absolute weight
     * would press {@code 1024^2} times less on {@code Y} than on {@code X}.
     *
     * <p>
     * The factor is a power of two, so every sum, product and comparison {@code fit} computes on
     * {@code Y}, k-means++ sampling included, is exactly {@code 2^10} or {@code 2^20} times its
     * counterpart on {@code X}: the two runs take identical decisions, and their objectives differ by
     * exactly {@code 1024^2} (asserted up to a relative {@code 1e-12} all the same).
     * </p>
     *
     * <p>
     * With {@code k = 10}, the strength {@code 0.16} (the strongest rung of Guardiann's default balancing
     * ladder) visibly evens out the cluster sizes on most but not all samples. As this is a requirement
     * for the test to meaningfully assert on anything, if the chosen sample does not, then this
     * returns {@code false} and a new sample is re-rolled by the caller.
     * </p>
     *
     * @return whether balancing evened out the cluster sizes on this sample, so that the invariance
     *         was meaningfully checked
     */
    private boolean runScaleInvarianceWhereBalancingFires(final long seed) {
        final List<RealVector> x = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, SIFT_SAMPLE_SIZE));
        final double scale = 1024.0d;
        final List<RealVector> y = x.stream()
                .map(v -> v.multiply(scale))
                .collect(ImmutableList.toImmutableList());
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final int k = 10;
        final double relativeLambda = 0.16d;

        final KMeans.Result<RealVector> unbalancedX = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                x, k, 30, 3, 0.0d, null, null);
        final KMeans.Result<RealVector> balancedX = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                x, k, 30, 3, relativeLambda, KMeans.overflowQuadraticPenalty(), null);
        if (clusterSizeStddev(balancedX.clusterSizes()) >= clusterSizeStddev(unbalancedX.clusterSizes())) {
            return false;
        }

        final KMeans.Result<RealVector> balancedY = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                y, k, 30, 3, relativeLambda, KMeans.overflowQuadraticPenalty(), null);

        assertThat(balancedY.assignment()).containsExactly(balancedX.assignment());
        assertThat(balancedY.clusterSizes()).containsExactly(balancedX.clusterSizes());
        final double expectedObjectiveY = scale * scale * balancedX.objective();
        assertThat(balancedY.objective()).isCloseTo(expectedObjectiveY, within(1.0e-12d * expectedObjectiveY));
        return true;
    }

    /**
     * What the relative balancing strength does to a lopsided two-way split, at the strengths of
     * Guardiann's default balancing ladder ({@code 0}, {@code 0.016}, {@code 0.16}) and at
     * {@code 0.064} between its last two rungs. The input, see {@link #unequalBlobs}, is a dense blob
     * of 1600 vectors and a satellite blob of 400, far enough apart that unbalanced 2-means cuts close
     * to the gap,
     * leaving the smaller cluster with only about 29% of the vectors, and close enough that balancing
     * can move the cut into the dense blob at a small cost in SSE. The sizes are those of the returned
     * partitioning, which assigns every vector to its nearest centroid, so they show how far the
     * penalty moves the centroids. Averaged across 8 inputs derived from the seed:
     * <ul>
     *   <li>the smallest cluster's fraction never shrinks as the strength grows, and at {@code 0.16}
     *       it is clearly larger than without balancing, and</li>
     *   <li>the first balancing rung, {@code 0.016}, already gets most of the way there (the measured
     *       fractions are about 0.29, 0.38, 0.40 and 0.41) for about 0.2% of SSE.</li>
     * </ul>
     * Aggregating across inputs smooths out the variance of the local optimum each run converges
     * into, as in {@link #siftSmallLambdaImprovesBalance(long)}.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void unequalBlobsRelativeLambdaEvensOutSplit(final long seed) {
        final SplittableRandom inputSeeds = new SplittableRandom(seed);
        final int numInputs = 8;
        final double[] relativeLambdas = {0.0d, 0.016d, 0.064d, 0.16d};
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        final double[] meanSmallestFraction = new double[relativeLambdas.length];
        final double[] meanSse = new double[relativeLambdas.length];
        for (int input = 0; input < numInputs; input++) {
            final SplittableRandom rnd = new SplittableRandom(inputSeeds.nextLong());
            final List<RealVector> vectors = unequalBlobs(rnd);

            // Every strength fits from the same seed, so the strength is the only difference between the runs.
            final long fitSeed = rnd.nextLong();
            for (int i = 0; i < relativeLambdas.length; i++) {
                final KMeans.Result<RealVector> res = KMeans.fit(
                        new SplittableRandom(fitSeed), distanceEstimator,
                        Lens.identity(), Lens.identity(),
                        vectors, 2, 30, 3, relativeLambdas[i],
                        relativeLambdas[i] > 0.0d ? KMeans.overflowQuadraticPenalty() : null, null);
                meanSmallestFraction[i] += smallestClusterFraction(res.clusterSizes()) / numInputs;
                meanSse[i] += res.objective() / numInputs;
            }
        }
        logger.info("unequalBlobsRelativeLambdaEvensOutSplit: relativeLambdas={} meanSmallestFraction={} meanSse={}",
                relativeLambdas, meanSmallestFraction, meanSse);

        for (int i = 1; i < relativeLambdas.length; i++) {
            assertThat(meanSmallestFraction[i])
                    .as("mean smallest-cluster fraction at relativeLambda %s vs %s",
                            relativeLambdas[i], relativeLambdas[i - 1])
                    .isGreaterThanOrEqualTo(meanSmallestFraction[i - 1]);
        }
        // Measured: about 0.29 without balancing, 0.41 at 0.16.
        assertThat(meanSmallestFraction[3])
                .as("mean smallest-cluster fraction at relativeLambda 0.16 vs 0")
                .isGreaterThanOrEqualTo(meanSmallestFraction[0] + 0.05d);
        // Measured: about +0.2%.
        assertThat(meanSse[1])
                .as("mean SSE at relativeLambda 0.016 vs 0")
                .isLessThanOrEqualTo(1.01d * meanSse[0]);
    }

    /**
     * With balancing active, the returned partitioning is still the nearest-centroid partitioning of
     * the returned centroids: the penalty steers the centroids during the iterations, but every run
     * ends with an assignment pass by distance alone, and the sizes are counted from that pass. On the
     * lopsided input of {@link #unequalBlobs}, the strength {@code 0.16} visibly moves the centroids,
     * which the test checks first, so that the nearest-centroid check cannot pass because the penalty
     * never fired.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void fitWithBalancingReturnsNearestCentroidAssignment(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = unequalBlobs(rnd);
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final long fitSeed = rnd.nextLong();

        final KMeans.Result<RealVector> unbalanced = KMeans.fit(
                new SplittableRandom(fitSeed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 30, 3, 0.0d, null, null);
        final KMeans.Result<RealVector> balanced = KMeans.fit(
                new SplittableRandom(fitSeed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 30, 3, 0.16d, KMeans.overflowQuadraticPenalty(), null);

        assertThat(smallestClusterFraction(balanced.clusterSizes()))
                .as("balancing moves the centroids toward more even clusters")
                .isGreaterThan(smallestClusterFraction(unbalanced.clusterSizes()) + 0.05d);

        final int[] expectedSizes = new int[2];
        for (int i = 0; i < vectors.size(); i++) {
            final RealVector vector = vectors.get(i);
            final double ownDistance =
                    vector.l2SquaredDistance(balanced.clusterCentroids().get(balanced.assignment()[i]));
            final double otherDistance =
                    vector.l2SquaredDistance(balanced.clusterCentroids().get(1 - balanced.assignment()[i]));
            assertThat(ownDistance)
                    .as("vector %d is assigned to its nearest centroid", i)
                    .isLessThanOrEqualTo(otherDistance);
            expectedSizes[balanced.assignment()[i]]++;
        }
        assertThat(balanced.clusterSizes()).containsExactly(expectedSizes);
        KMeansTestHelpers.assertKMeansInvariants(balanced, vectors, 2, distanceEstimator);
    }

    /**
     * Without a size predicate, {@code fit} returns the run with the lowest objective. The runs of a
     * {@code numRuns}-run fit are reconstructed one by one with {@link #individualRuns}, and the
     * result must be the one among them with the lowest objective, its partitioning included.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void fitWithoutSizePredicateReturnsLowestObjectiveRun(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, RUN_SELECTION_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final List<KMeans.Result<RealVector>> runs = individualRuns(seed, sample);

        final KMeans.Result<RealVector> res = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, RUN_SELECTION_K, 30, RUN_SELECTION_NUM_RUNS, 0.0d, null, null);

        final KMeans.Result<RealVector> lowestObjectiveRun =
                runs.stream().min(Comparator.comparingDouble(KMeans.Result::objective)).orElseThrow();
        assertThat(res.objective()).isEqualTo(lowestObjectiveRun.objective());
        assertThat(res.assignment()).containsExactly(lowestObjectiveRun.assignment());
        assertThat(res.clusterSizes()).containsExactly(lowestObjectiveRun.clusterSizes());
    }

    /**
     * A run that passes the size predicate is preferred over every run that fails it, even one with a
     * lower objective. As we will sometimes fail to find a test case that exercises the scenario,
     * this runs {@link #runFitWhereSizePredicateRulesOutLowerObjectiveRun(long)} in a loop. Empirically,
     * we are able to find a passing run within about 5 trials (and often on the first trial).
     *
     * @see #runFitWhereSizePredicateRulesOutLowerObjectiveRun(long) for more details
     * @see #fitFallsBackToLowestObjectiveRunWhenNoRunPassesSizePredicate for when the size predicate rules
     *      out all runs
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void fitPrefersRunPassingSizePredicateOverLowerObjectiveRun(final long seed) {
        final Random random = new Random(seed);
        final int trials = 100;
        for (int i = 0; i < trials; i++) {
            final long trialSeed = random.nextLong();
            try {
                if (runFitWhereSizePredicateRulesOutLowerObjectiveRun(trialSeed)) {
                    return;
                }
            } catch (AssertionError e) {
                fail("trial " + i + " (seed=" + trialSeed + ") failed: " + e.getMessage(), e);
            }
        }
        fail("Could not find a passing run after " + trials + " runs");
    }

    /**
     * A run that passes the size predicate is preferred over every run that fails it, even one with a
     * lower objective. The predicate is a floor on the smallest cluster, set just above the smallest
     * cluster of the run with the lowest objective, which thus fails it by construction. 3-means on a
     * small SIFT-small sample has several local optima, and the test checks that at least one run is
     * more balanced than the lowest-objective one. If none of the runs on this sample are more balanced,
     * this returns {@code false} and is re-run by the caller. If there is a run passing the predicate,
     * then this asserts that {@code fit} returns the passing run with the lowest objective, even though
     * it costs more than the lowest-objective run.
     *
     * @return whether a run with this particular seed was able to provide a meaningful test
     * @see #fitFallsBackToLowestObjectiveRunWhenNoRunPassesSizePredicate for when the size predicate rules
     *      out all runs
     */
    private boolean runFitWhereSizePredicateRulesOutLowerObjectiveRun(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, RUN_SELECTION_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final List<KMeans.Result<RealVector>> runs = individualRuns(seed, sample);
        final KMeans.Result<RealVector> lowestObjectiveRun =
                runs.stream().min(Comparator.comparingDouble(KMeans.Result::objective)).orElseThrow();
        final int floor = Arrays.stream(lowestObjectiveRun.clusterSizes()).min().orElseThrow() + 1;
        final Predicate<int[]> sizePredicate = sizes -> Arrays.stream(sizes).min().orElseThrow() >= floor;
        final List<KMeans.Result<RealVector>> passingRuns = runs.stream()
                .filter(run -> sizePredicate.test(run.clusterSizes()))
                .collect(ImmutableList.toImmutableList());
        // For this test to be meaningful, at least one of the runs must have a better split than the one
        // with the lowest objective. The contrary case is handled in the test below:
        //  fitFallsBackToLowestObjectiveRunWhenNoRunPassesSizePredicate
        if (passingRuns.isEmpty()) {
            return false;
        }
        final KMeans.Result<RealVector> lowestObjectivePassingRun =
                passingRuns.stream().min(Comparator.comparingDouble(KMeans.Result::objective)).orElseThrow();

        final KMeans.Result<RealVector> res = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, RUN_SELECTION_K, 30, RUN_SELECTION_NUM_RUNS, 0.0d, null, sizePredicate);

        assertThat(sizePredicate.test(res.clusterSizes())).as("the returned run passes").isTrue();
        assertThat(res.objective())
                .isGreaterThan(lowestObjectiveRun.objective())
                .isEqualTo(lowestObjectivePassingRun.objective());
        assertThat(res.assignment()).containsExactly(lowestObjectivePassingRun.assignment());
        KMeansTestHelpers.assertKMeansInvariants(res, sample, RUN_SELECTION_K, distanceEstimator);
        return true;
    }

    /**
     * When no run passes the size predicate, {@code fit} falls back to the run with the lowest objective,
     * exactly the one it returns without a predicate, so the caller still gets a partitioning to judge.
     * Here no run can pass: the predicate demands a smallest cluster holding half the vectors, while
     * the smallest of 3 clusters holds at most a third.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void fitFallsBackToLowestObjectiveRunWhenNoRunPassesSizePredicate(final long seed) {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(seed), siftSmallBase, RUN_SELECTION_SAMPLE_SIZE));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final Predicate<int[]> unsatisfiable =
                sizes -> Arrays.stream(sizes).min().orElseThrow() >= Arrays.stream(sizes).sum() / 2;

        final KMeans.Result<RealVector> unfiltered = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, RUN_SELECTION_K, 30, RUN_SELECTION_NUM_RUNS, 0.0d, null, null);
        final KMeans.Result<RealVector> res = KMeans.fit(
                new SplittableRandom(seed), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, RUN_SELECTION_K, 30, RUN_SELECTION_NUM_RUNS, 0.0d, null, unsatisfiable);

        assertThat(unsatisfiable.test(res.clusterSizes())).as("the fallback fails the predicate").isFalse();
        assertThat(res.objective()).isEqualTo(unfiltered.objective());
        assertThat(res.assignment()).containsExactly(unfiltered.assignment());
        assertThat(res.clusterSizes()).containsExactly(unfiltered.clusterSizes());
    }

    /**
     * {@code numRuns} is the total number of runs: {@code fit} tests the size predicate once on the
     * final sizes of every run, so counting the tests counts the runs. One run is the minimum, and
     * {@code k == 1}, solved in closed form, makes no run at all.
     */
    @Test
    void fitMakesExactlyNumRunsRuns() {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(0x0fdbL), siftSmallBase, 500));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        for (final int numRuns : new int[] {1, 2, 4}) {
            final AtomicInteger numTests = new AtomicInteger();
            KMeans.fit(new SplittableRandom(0x0fdbL), distanceEstimator,
                    Lens.identity(), Lens.identity(),
                    sample, 5, 10, numRuns, 0.0d, null,
                    sizes -> numTests.incrementAndGet() > 0);
            assertThat(numTests).as("size predicate tests with numRuns=%d", numRuns).hasValue(numRuns);
        }

        final AtomicInteger numTestsForOneCluster = new AtomicInteger();
        KMeans.fit(new SplittableRandom(0x0fdbL), distanceEstimator,
                Lens.identity(), Lens.identity(),
                sample, 1, 10, 4, 0.0d, null,
                sizes -> numTestsForOneCluster.incrementAndGet() > 0);
        assertThat(numTestsForOneCluster).as("size predicate tests with k=1").hasValue(0);
    }

    /**
     * {@code fit} rejects fewer than one run and a balancing strength that is negative or not finite.
     */
    @Test
    void fitRejectsInvalidNumRunsAndRelativeLambda() {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(0x0fdbL), siftSmallBase, 100));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        assertThatThrownBy(() -> KMeans.fit(new SplittableRandom(0x0fdbL), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 2, 10, 0, 0.0d, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numRuns");
        for (final double relativeLambda : new double[] {-0.1d, Double.POSITIVE_INFINITY, Double.NaN}) {
            assertThatThrownBy(() -> KMeans.fit(new SplittableRandom(0x0fdbL), distanceEstimator,
                    Lens.identity(), Lens.identity(), sample, 2, 10, 1, relativeLambda,
                    KMeans.overflowQuadraticPenalty(), null))
                    .as("relativeLambda=%s", relativeLambda)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("relativeLambda");
        }
    }

    /**
     * Without a size penalty, a non-zero balancing strength has nothing to weigh: {@code fit} does
     * exactly what it does with a strength of zero, down to the random draws, since it neither
     * computes the scale of the strength nor shuffles the assignment order.
     */
    @Test
    void fitWithoutSizePenaltyIgnoresRelativeLambda() {
        final List<RealVector> sample = ImmutableList.copyOf(
                KMeansTestHelpers.pickRandomSubset(new Random(0x5ca1eL), siftSmallBase, 500));
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);

        final KMeans.Result<RealVector> unbalanced = KMeans.fit(new SplittableRandom(0x5ca1eL), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 5, 10, 3, 0.0d, null, null);
        final KMeans.Result<RealVector> withoutPenalty = KMeans.fit(new SplittableRandom(0x5ca1eL), distanceEstimator,
                Lens.identity(), Lens.identity(), sample, 5, 10, 3, 0.5d, null, null);

        assertThat(withoutPenalty.assignment()).containsExactly(unbalanced.assignment());
        assertThat(withoutPenalty.objective()).isEqualTo(unbalanced.objective());
    }

    /**
     * The runs that a fit of {@code vectors} into {@value #RUN_SELECTION_K} clusters with
     * {@value #RUN_SELECTION_NUM_RUNS} runs and without balancing makes from a random source seeded
     * with {@code seed}, each reconstructed as a single-run fit. {@code fit} draws all of its
     * randomness inside its runs, one run after another, so single-run fits drawing from one shared
     * random source in turn make exactly the same runs.
     */
    @Nonnull
    private static List<KMeans.Result<RealVector>> individualRuns(final long seed,
                                                                  @Nonnull final List<RealVector> vectors) {
        final SplittableRandom random = new SplittableRandom(seed);
        final DistanceEstimator distanceEstimator = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final ImmutableList.Builder<KMeans.Result<RealVector>> runs = ImmutableList.builder();
        for (int r = 0; r < RUN_SELECTION_NUM_RUNS; r++) {
            runs.add(KMeans.fit(random, distanceEstimator, Lens.identity(), Lens.identity(),
                    vectors, RUN_SELECTION_K, 30, 1, 0.0d, null, null));
        }
        return runs.build();
    }

    /**
     * A lopsided input for 2-means: a dense blob of 1600 vectors and a satellite blob of 400, both of
     * unit variance in 32 dimensions, 3 standard deviations apart along one axis, in shuffled order.
     */
    @Nonnull
    private static List<RealVector> unequalBlobs(@Nonnull final SplittableRandom rnd) {
        final int numDimensions = 32;
        final RealVector denseMean = DoubleRealVector.zeroVector(numDimensions);
        final double[] satelliteMeanData = new double[numDimensions];
        satelliteMeanData[0] = 3.0d;
        final RealVector satelliteMean = new DoubleRealVector(satelliteMeanData);

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(2000);
        for (int i = 0; i < 1600; i++) {
            vectors.add(KMeansTestHelpers.gaussianND(rnd, denseMean, 1.0d));
        }
        for (int i = 0; i < 400; i++) {
            vectors.add(KMeansTestHelpers.gaussianND(rnd, satelliteMean, 1.0d));
        }
        Collections.shuffle(vectors, new Random(rnd.nextLong()));
        return vectors;
    }

    /**
     * Standard deviation of an {@code int[]} treated as a sample, using the population formula
     * (divide by {@code n}, not {@code n-1}). Used to measure cluster-size dispersion.
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

    /**
     * The fraction of all vectors that the smallest cluster holds.
     */
    private static double smallestClusterFraction(@Nonnull final int[] sizes) {
        return (double) Arrays.stream(sizes).min().orElseThrow() / Arrays.stream(sizes).sum();
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
     * Returns the arithmetic mean of the given vectors as an immutable {@link RealVector}.
     */
    @Nonnull
    private static RealVector arithmeticMean(@Nonnull final List<RealVector> vectors) {
        final int d = vectors.get(0).getNumDimensions();
        final MutableDoubleRealVector sum = MutableDoubleRealVector.zeroVector(d);
        for (final RealVector v : vectors) {
            sum.addToThis(v);
        }
        return sum.multiplyThisBy(1.0d / vectors.size()).toImmutable();
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
