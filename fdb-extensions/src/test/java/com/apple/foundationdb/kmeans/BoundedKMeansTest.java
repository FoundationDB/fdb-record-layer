/*
 * BoundedKMeansTest.java
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
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.MutableDoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.util.Lens;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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

class BoundedKMeansTest {
    private static final Logger logger = LoggerFactory.getLogger(BoundedKMeansTest.class);

    @TempDir
    Path tempDir;

    /**
     * Generates two Gaussian blobs in 2D, runs k=2, and checks.
     * - both clusters non-empty
     * - centroids near the generating means (within a tolerance)
     * - objective significantly better than a "bad" baseline (single centroid at global mean, duplicated)
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void twoSeparatedBlobsFindsTwoClusters(final long seed) throws Exception {
        final SplittableRandom rnd = new SplittableRandom(seed);

        // Two well-separated means.
        final RealVector m0 = new DoubleRealVector(new double[] {-5.0, 0.0});
        final RealVector m1 = new DoubleRealVector(new double[] {+5.0, 0.0});

        int nPer = 3000;
        double sigma = 3.0d;

        List<RealVector> vectors = Lists.newArrayListWithCapacity(2 * nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(gaussian2D(rnd, m0, sigma));
        }
        for (int i = 0; i < nPer; i++) {
            vectors.add(gaussian2D(rnd, m1, sigma));
        }

        // Shuffle so ordering doesn't accidentally help/hurt (also exercises shuffle path).
        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(rnd, estimator,
                Lens.identity(), Lens.identity(), vectors, 2, 15, 3, 0.0,
                null, true);

        assertThat(res.getAssignment().length).isEqualTo(vectors.size());
        assertThat(res.getClusterCentroids().size()).isEqualTo(2);
        assertThat(res.getClusterSizes().length).isEqualTo(2);

        // Both clusters should be non-empty.
        assertThat(res.getClusterSizes()[0]).isGreaterThan(0);
        assertThat(res.getClusterSizes()[1]).isGreaterThan(0);

        // Centroids should be close to the generating means (order is arbitrary).
        final RealVector c0 = res.getClusterCentroids().get(0);
        final RealVector c1 = res.getClusterCentroids().get(1);

        double d00 = estimator.distance(m0, c0);
        double d01 = estimator.distance(m0, c1);
        double d10 = estimator.distance(m1, c0);
        double d11 = estimator.distance(m1, c1);

        // Best matching between found centroids and true means.
        double matchA = d00 + d11;
        double matchB = d01 + d10;
        double bestMatch = Math.min(matchA, matchB);

        // Tolerance: with 300 points per blob and sigma ~0.9, centroids should be pretty close.
        assertThat(bestMatch).isLessThan(2.0d);

        // Objective sanity: should be much better than a trivial baseline that uses same centroid twice.
        double baseline = baselineObjectiveSameCentroidTwice(vectors);
        assertThat(res.getObjective()).isLessThan(baseline * 0.65);

//        System.out.println(tempDir);
//        dumpVectors(tempDir, "vectors", vectors);
//        dumpVectors(tempDir, "centroids", res.getClusterCentroids());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void oneBlobFindsTwoClusters(final long seed) throws Exception {
        final SplittableRandom rnd = new SplittableRandom(seed);

        // Two well-separated means.
        final RealVector m0 = new DoubleRealVector(new double[] {-5.0, 0.0});

        int nPer = 3000;
        double sigma = 0.5d;

        List<RealVector> vectors = Lists.newArrayListWithCapacity(nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(gaussian2D(rnd, m0, sigma));
        }

        // Shuffle so ordering doesn't accidentally help/hurt (also exercises shuffle path).
        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(rnd, estimator,
                Lens.identity(), Lens.identity(), vectors, 2, 15, 3, 0.0,
                null, true);

        assertThat(res.getAssignment().length).isEqualTo(vectors.size());
        assertThat(res.getClusterCentroids().size()).isEqualTo(2);
        assertThat(res.getClusterSizes().length).isEqualTo(2);

        // Both clusters should be non-empty.
        assertThat(res.getClusterSizes()[0]).isGreaterThan(0);
        assertThat(res.getClusterSizes()[1]).isGreaterThan(0);

        // Objective sanity: should be much better than a trivial baseline that uses same centroid twice.
        double baseline = baselineObjectiveSameCentroidTwice(vectors);
        //assertThat(res.getObjective()).isLessThan(baseline * 0.65);

        System.out.println(tempDir);
        dumpVectors(tempDir, "vectors", vectors);
        dumpVectors(tempDir, "centroids", res.getClusterCentroids());
    }

    /**
     * Exercises k>2 plus balancing: 3 blobs, but ask for k=3 and enable soft size balancing.
     * Checks:
     * - all clusters non-empty
     * - cluster sizes not wildly imbalanced
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void threeBlobsWithSoftBalancingProducesReasonableSizes(final long seed) throws Exception {
        final SplittableRandom rnd = new SplittableRandom(seed);

        final List<RealVector> means = ImmutableList.of(
                new DoubleRealVector(new double[] { -6.0d, -3.0d }),
                new DoubleRealVector(new double[] { +0.0d, +4.5d }),
                new DoubleRealVector(new double[] { +6.0d, -2.0d })
        );

        final int nPer = 2000;
        final double sigma = 3.0;

        List<RealVector> vectors = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                vectors.add(gaussian2D(rnd, m, sigma));
            }
        }
        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final int k = 3;
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(rnd, estimator,
                Lens.identity(), Lens.identity(), vectors, k, 20, 3, 0.08,
                BoundedKMeans.overflowQuadraticPenalty(),
                true);

        assertThat(res.getClusterCentroids().size()).isEqualTo(k);
        assertThat(res.getClusterSizes().length).isEqualTo(k);

        for (int c = 0; c < k; c++) {
            assertThat(res.getClusterSizes()[c]).isGreaterThan(0);
        }

        // With equal generating sizes, results should be roughly balanced.
        // Allow some slack because k-means is not constrained and data is random.
        int min = Arrays.stream(res.getClusterSizes()).min().orElseThrow();
        int max = Arrays.stream(res.getClusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        System.out.println(tempDir);
        dumpVectors(tempDir, "vectors", vectors);
        dumpVectors(tempDir, "centroids", res.getClusterCentroids());
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

        List<RealVector> vectors = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                vectors.add(gaussianND(splittableRandom, m, sigma));
            }
        }
        Collections.shuffle(vectors, random);

        final int k = 3;
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(splittableRandom, estimator,
                Lens.identity(), Lens.identity(), vectors, k, 20, 3, 0.08,
                BoundedKMeans.overflowQuadraticPenalty(), true);

        assertThat(res.getClusterCentroids().size()).isEqualTo(k);
        assertThat(res.getClusterSizes().length).isEqualTo(k);

        for (int c = 0; c < k; c++) {
            assertThat(res.getClusterSizes()[c]).isGreaterThan(0);
        }

        // With equal generating sizes, results should be roughly balanced.
        // Allow some slack because k-means is not constrained and data is random.
        int min = Arrays.stream(res.getClusterSizes()).min().orElseThrow();
        int max = Arrays.stream(res.getClusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        logger.info("cluster sizes = {}", res.getClusterSizes());
    }

    @Nonnull
    static RealVector gaussianND(@Nonnull final SplittableRandom random,
                                 @Nonnull final RealVector mean,
                                 double sigma) {
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(random);
        final int d = mean.getNumDimensions();
        double[] v = new double[d];
        for (int i = 0; i < d; i++) {
            double z = sampler.nextGaussian();  // N(0,1)
            v[i] = mean.getComponent(i) + sigma * z;
        }

        return new DoubleRealVector(v);
    }

    @Nonnull
    private static RealVector gaussian2D(@Nonnull final SplittableRandom rnd,
                                         @Nonnull final RealVector m,
                                         double sigma) {
        // Box–Muller
        double u1 = Math.max(1e-12, rnd.nextDouble());
        double u2 = rnd.nextDouble();
        double r = Math.sqrt(-2.0 * Math.log(u1));
        double theta = 2.0 * Math.PI * u2;
        final RealVector z = new MutableDoubleRealVector(new double[] { r * Math.cos(theta), r * Math.sin(theta) });

        return m.add(z.multiply(sigma));
    }

    /**
     * Baseline: compute global mean, then pretend k=2 but both centroids are identical at global mean.
     * Objective = sum squared distances to that mean.
     */
    private static double baselineObjectiveSameCentroidTwice(@Nonnull final List<RealVector> pts) {
        final MutableDoubleRealVector sum = MutableDoubleRealVector.zeroVector(2);
        for (final RealVector p : pts) {
            sum.add(p);
        }
        final RealVector m = sum.multiply(1.0d / pts.size()).toImmutable();

        double obj = 0.0;
        for (final RealVector p : pts) {
            final RealVector d = p.subtract(m);
            obj += d.getComponent(0) * d.getComponent(0) +
                    d.getComponent(1) * d.getComponent(1);
        }
        return obj;
    }

    static void dumpVectors(@Nonnull final Path tempDir, @Nonnull final String prefix,
                            @Nonnull final List<RealVector> vectors) throws IOException {
        final Path vectorsFile = tempDir.resolve(prefix + ".csv");

        try (final BufferedWriter vectorsWriter = Files.newBufferedWriter(vectorsFile)) {
            long id = 0;
            for (final RealVector vector : vectors) {
                vectorsWriter.write(id + "," +
                        vector.getComponent(0) + "," +
                        vector.getComponent(1));
                vectorsWriter.newLine();
                id ++;
            }
        }
    }

}
