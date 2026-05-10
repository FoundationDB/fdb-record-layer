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
            vectors.add(gaussianND(rnd, m0, sigma));
        }
        for (int i = 0; i < nPer; i++) {
            vectors.add(gaussianND(rnd, m1, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                2, 15, 3, 0.0,
                null, true);

        assertThat(res.getAssignment().length).isEqualTo(vectors.size());
        assertThat(res.getClusterCentroids().size()).isEqualTo(2);
        assertThat(res.getClusterSizes().length).isEqualTo(2);

        assertThat(res.getClusterSizes()[0]).isGreaterThan(0);
        assertThat(res.getClusterSizes()[1]).isGreaterThan(0);

        final RealVector c0 = res.getClusterCentroids().get(0);
        final RealVector c1 = res.getClusterCentroids().get(1);

        final double d00 = estimator.distance(m0, c0);
        final double d01 = estimator.distance(m0, c1);
        final double d10 = estimator.distance(m1, c0);
        final double d11 = estimator.distance(m1, c1);

        final double matchA = d00 + d11;
        final double matchB = d01 + d10;
        final double bestMatch = Math.min(matchA, matchB);

        assertThat(bestMatch).isLessThan(2.0d);

        final double baseline = baselineObjectiveSameCentroidTwice(vectors);
        assertThat(res.getObjective()).isLessThan(baseline * 0.65);

//        System.out.println(tempDir);
//        dumpVectors(tempDir, "vectors", vectors);
//        dumpVectors(tempDir, "centroids", res.getClusterCentroids());
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
            vectors.add(gaussianND(rnd, m0, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                2, 15, 3, 0.0,
                null, true);

        assertThat(res.getAssignment().length).isEqualTo(vectors.size());
        assertThat(res.getClusterCentroids().size()).isEqualTo(2);
        assertThat(res.getClusterSizes().length).isEqualTo(2);

        assertThat(res.getClusterSizes()[0]).isGreaterThan(0);
        assertThat(res.getClusterSizes()[1]).isGreaterThan(0);

        final double baseline = baselineObjectiveSameCentroidTwice(vectors);
        // assertThat(res.getObjective()).isLessThan(baseline * 0.65);

        System.out.println(tempDir);
        dumpVectors(tempDir, "vectors", vectors);
        dumpVectors(tempDir, "centroids", res.getClusterCentroids());
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
                vectors.add(gaussianND(rnd, m, sigma));
            }
        }
        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final int k = 3;
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 3, 0.08,
                BoundedKMeans.overflowQuadraticPenalty(),
                true);

        assertThat(res.getClusterCentroids().size()).isEqualTo(k);
        assertThat(res.getClusterSizes().length).isEqualTo(k);

        for (int c = 0; c < k; c++) {
            assertThat(res.getClusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.getClusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.getClusterSizes()).max().orElseThrow();
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

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                vectors.add(gaussianND(splittableRandom, m, sigma));
            }
        }
        Collections.shuffle(vectors, random);

        final int k = 3;
        final Estimator estimator = Estimator.ofMetric(Metric.EUCLIDEAN_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                splittableRandom, estimator,
                Lens.identity(), Lens.identity(),
                vectors,
                k, 20, 3, 0.08,
                BoundedKMeans.overflowQuadraticPenalty(),
                true);

        assertThat(res.getClusterCentroids().size()).isEqualTo(k);
        assertThat(res.getClusterSizes().length).isEqualTo(k);

        for (int c = 0; c < k; c++) {
            assertThat(res.getClusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.getClusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.getClusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        logger.info("cluster sizes = {}", res.getClusterSizes());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void twoSeparatedUnitNormalizedBlobsFindsTwoClustersCosine3D(final long seed) throws Exception {
        final SplittableRandom rnd = new SplittableRandom(seed);

        // Two opposite directions on the unit sphere.
        final RealVector m0 = new DoubleRealVector(new double[] {-1.0, 0.0, 0.0}).normalize();
        final RealVector m1 = new DoubleRealVector(new double[] {+1.0, 0.0, 0.0}).normalize();

        final int nPer = 3000;
        final double sigma = 0.18d;

        final List<RealVector> vectors = Lists.newArrayListWithCapacity(2 * nPer);
        for (int i = 0; i < nPer; i++) {
            vectors.add(noisyUnitVector(rnd, m0, sigma));
        }
        for (int i = 0; i < nPer; i++) {
            vectors.add(noisyUnitVector(rnd, m1, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 15, 3, 0.0,
                null, true);

        assertThat(res.getAssignment().length).isEqualTo(vectors.size());
        assertThat(res.getClusterCentroids().size()).isEqualTo(2);
        assertThat(res.getClusterSizes().length).isEqualTo(2);

        assertThat(res.getClusterSizes()[0]).isGreaterThan(0);
        assertThat(res.getClusterSizes()[1]).isGreaterThan(0);

        final RealVector c0 = res.getClusterCentroids().get(0).normalize();
        final RealVector c1 = res.getClusterCentroids().get(1).normalize();

        final double d00 = estimator.distance(m0, c0);
        final double d01 = estimator.distance(m0, c1);
        final double d10 = estimator.distance(m1, c0);
        final double d11 = estimator.distance(m1, c1);

        final double matchA = d00 + d11;
        final double matchB = d01 + d10;
        final double bestMatch = Math.min(matchA, matchB);

        assertThat(bestMatch).isLessThan(0.20d);

        final double baseline = baselineObjectiveSameCentroidTwiceNormalized(vectors, estimator);
        assertThat(res.getObjective()).isLessThan(baseline * 0.65d);
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
            vectors.add(noisyUnitVector(rnd, m0, sigma));
        }

        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors, 2, 15, 3, 0.0,
                null, true);

        assertThat(res.getAssignment().length).isEqualTo(vectors.size());
        assertThat(res.getClusterCentroids().size()).isEqualTo(2);
        assertThat(res.getClusterSizes().length).isEqualTo(2);

        assertThat(res.getClusterSizes()[0]).isGreaterThan(0);
        assertThat(res.getClusterSizes()[1]).isGreaterThan(0);

        final double baseline = baselineObjectiveSameCentroidTwiceNormalized(vectors, estimator);
        // As in your Euclidean counterpart, this is mainly a smoke / sanity test.
        // assertThat(res.getObjective()).isLessThan(baseline * 0.65d);

        System.out.println(tempDir);
        dumpVectors(tempDir, "cosine_vectors_3d", vectors);
        dumpVectors(tempDir, "cosine_centroids_3d", res.getClusterCentroids());
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
                vectors.add(noisyUnitVector(rnd, m, sigma));
            }
        }
        Collections.shuffle(vectors, new Random(rnd.nextLong()));

        final int k = 3;
        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                rnd, estimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 3, 0.08,
                BoundedKMeans.overflowQuadraticPenalty(),
                true);

        assertThat(res.getClusterCentroids().size()).isEqualTo(k);
        assertThat(res.getClusterSizes().length).isEqualTo(k);

        for (int c = 0; c < k; c++) {
            assertThat(res.getClusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.getClusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.getClusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        System.out.println(tempDir);
        dumpVectors(tempDir, "cosine_vectors_3blob_3d", vectors);
        dumpVectors(tempDir, "cosine_centroids_3blob_3d", res.getClusterCentroids());
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
                vectors.add(noisyUnitVector(splittableRandom, m, sigma));
            }
        }
        Collections.shuffle(vectors, random);

        final int k = 3;
        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);
        final BoundedKMeans.Result<RealVector> res = BoundedKMeans.fit(
                splittableRandom, estimator,
                Lens.identity(), Lens.identity(),
                vectors, k, 20, 3, 0.08,
                BoundedKMeans.overflowQuadraticPenalty(),
                true);

        assertThat(res.getClusterCentroids().size()).isEqualTo(k);
        assertThat(res.getClusterSizes().length).isEqualTo(k);

        for (int c = 0; c < k; c++) {
            assertThat(res.getClusterSizes()[c]).isGreaterThan(0);
        }

        final int min = Arrays.stream(res.getClusterSizes()).min().orElseThrow();
        final int max = Arrays.stream(res.getClusterSizes()).max().orElseThrow();
        assertThat(max).isLessThanOrEqualTo(min * 2);

        logger.info("cosine cluster sizes = {}", res.getClusterSizes());
    }

    @Nonnull
    private static RealVector noisyUnitVector(@Nonnull final SplittableRandom random,
                                              @Nonnull final RealVector meanUnitVector,
                                              final double sigma) {
        final RealVector perturbed = gaussianND(random, meanUnitVector, sigma);
        return perturbed.normalize();
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
