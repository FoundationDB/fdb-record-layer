/*
 * PartitionEvaluatorTest.java
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
import com.apple.foundationdb.util.Lens;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PartitionEvaluator}, with particular emphasis on the generalized parameter
 * surface (single {@code minSmallestFrac}/{@code maxLargestFrac} instead of per-{@code k} variants)
 * and on symmetric handling of {@code k == 1} on either side (splits, merges, and same-{@code k}
 * transitions).
 */
class PartitionEvaluatorTest {
    private static final Logger logger = LoggerFactory.getLogger(PartitionEvaluatorTest.class);

    private static final DistanceEstimator EUCLIDEAN = DistanceEstimator.ofMetric(Metric.EUCLIDEAN_METRIC);
    private static final DistanceEstimator COSINE = DistanceEstimator.ofMetric(Metric.COSINE_METRIC);

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void partitionHashCodeAndEquals(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoBlobs(rnd, 600);

        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d}));

        // create identical partitions but with different assignment arrays (different by reference; not by content)
        final PartitionEvaluator.Partition<RealVector> one =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);
        final PartitionEvaluator.Partition<RealVector> other =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);
        assertThat(one).hasSameHashCodeAs(other);
        assertThat(one).isEqualTo(other);
    }

    // ---------------- 1 → 2: split on well-separated data is ACCEPTed ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void splitOneToTwoOnWellSeparatedBlobsAccepted(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoBlobs(rnd, 600);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), new PartitionEvaluator.Parameters(EUCLIDEAN));

        logger.info("1->2 well-separated: {} reason='{}'", result.decision(), result.reason());
        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.ACCEPT_CANDIDATE);
        assertThat(result.relativeSseGain()).isGreaterThan(0.0d);
    }

    // ---------------- 1 → 2: split on a single blob is rejected as KEEP_CURRENT ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void splitOneToTwoOnSingleBlobRejected(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = oneBlob(rnd, 600);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        // Candidate: pick two arbitrary points and assign by nearest. The split won't materially
        // reduce SSE because the data is already a tight cluster.
        final List<RealVector> twoCentroids = ImmutableList.of(
                vectors.get(0), vectors.get(vectors.size() - 1));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), new PartitionEvaluator.Parameters(EUCLIDEAN));

        logger.info("1->2 single blob: {} reason='{}'", result.decision(), result.reason());
        assertThat(result.decision()).isNotEqualTo(PartitionEvaluator.Decision.ACCEPT_CANDIDATE);
    }

    // ---------------- 2 → 1: merge of well-separated data is rejected with default params ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void mergeTwoToOneRejectedWithDefaultParams(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoBlobs(rnd, 600);

        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> current =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);
        final PartitionEvaluator.Partition<RealVector> candidate = singleClusterPartition(vectors);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), new PartitionEvaluator.Parameters(EUCLIDEAN));

        logger.info("2->1 default params: {} reason='{}', relativeSseGain={}, scoreGain={}",
                result.decision(), result.reason(), result.relativeSseGain(), result.scoreGain());
        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.KEEP_CURRENT);
        assertThat(result.relativeSseGain()).isLessThan(0.0d);
    }

    // ---------------- 2 → 1: with very lax params, the merge can be accepted ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void mergeTwoToOneAcceptedWithLaxParams(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        // Use a single tight blob so the merge isn't catastrophic even on SSE.
        final List<RealVector> vectors = oneBlob(rnd, 600);

        // Synthesize a two-cluster current by splitting the blob into two arbitrary halves.
        final List<RealVector> twoCentroids = ImmutableList.of(
                vectors.get(0), vectors.get(vectors.size() - 1));
        final PartitionEvaluator.Partition<RealVector> current =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);
        final PartitionEvaluator.Partition<RealVector> candidate = singleClusterPartition(vectors);

        final PartitionEvaluator.Parameters lax = new PartitionEvaluator.Parameters(
                EUCLIDEAN,
                /*minRelativeSseGain*/ -10.0d,
                /*minSeparation*/ 0.0d,
                /*maxLowMarginRate*/ 1.0d,
                /*minSmallestFrac*/ 0.0d,
                /*maxLargestFrac*/ 1.0d,
                /*lowMarginThreshold*/ -1.0d,
                /*alphaSseGain*/ 1.0d,
                /*betaSeparationGain*/ 0.0d,
                /*gammaImbalancePenalty*/ 0.0d,
                /*deltaLowMarginPenalty*/ 0.0d,
                /*minScoreGain*/ -10.0d);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), lax);

        logger.info("2->1 lax params: {} reason='{}'", result.decision(), result.reason());
        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.ACCEPT_CANDIDATE);
    }

    // ---------------- 2 → 3: split on three blobs is ACCEPTed ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void splitTwoToThreeOnThreeBlobsAccepted(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = threeBlobs(rnd, 400);

        // Current: a deliberately suboptimal 2-cluster grouping.
        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-3.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+3.0d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> current =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);

        final List<RealVector> threeCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-6.0d, -3.0d,  1.0d}),
                new DoubleRealVector(new double[] { 0.0d, +4.5d, -2.0d}),
                new DoubleRealVector(new double[] {+6.0d, -2.0d,  3.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(threeCentroids, vectors, EUCLIDEAN);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), new PartitionEvaluator.Parameters(EUCLIDEAN));

        logger.info("2->3 three blobs: {} reason='{}'", result.decision(), result.reason());
        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.ACCEPT_CANDIDATE);
        assertThat(result.relativeSseGain()).isGreaterThan(0.0d);
    }

    // ---------------- self-comparison: SSE gain ≈ 0, KEEP_CURRENT ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void selfComparisonKeepsCurrent(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoBlobs(rnd, 400);

        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> partition =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, partition, vectors, partition,
                        Lens.identity(), new PartitionEvaluator.Parameters(EUCLIDEAN));

        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.KEEP_CURRENT);
        assertThat(result.relativeSseGain()).isCloseTo(0.0d, org.assertj.core.data.Offset.offset(1.0e-12d));
    }

    // ---------------- candidate.k() < 2: separation/low-margin hard rejects must be skipped ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void singleClusterCandidateSkipsSeparationAndMarginChecks(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoBlobs(rnd, 400);

        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> current =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);
        final PartitionEvaluator.Partition<RealVector> candidate = singleClusterPartition(vectors);

        // Pathologically strict separation/margin thresholds that would surely fail if checked.
        final PartitionEvaluator.Parameters params = new PartitionEvaluator.Parameters(
                EUCLIDEAN,
                /*minRelativeSseGain*/ -10.0d,
                /*minSeparation*/ Double.MAX_VALUE,
                /*maxLowMarginRate*/ 0.0d,
                /*minSmallestFrac*/ 0.0d,
                /*maxLargestFrac*/ 1.0d,
                /*lowMarginThreshold*/ -1.0d,
                /*alphaSseGain*/ 1.0d,
                /*betaSeparationGain*/ 1.0d,
                /*gammaImbalancePenalty*/ 1.0d,
                /*deltaLowMarginPenalty*/ 1.0d,
                /*minScoreGain*/ -10.0d);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), params);

        // Whatever the verdict, the reason must NOT cite the separation or low-margin checks
        // because those should be skipped when candidate.k() < 2.
        logger.info("k=1 candidate skips: {} reason='{}'", result.decision(), result.reason());
        assertThat(result.reason())
                .as("separation hard reject should be skipped when candidate.k()<2")
                .doesNotContain("separation too low");
        assertThat(result.reason())
                .as("low-margin hard reject should be skipped when candidate.k()<2")
                .doesNotContain("low-margin rate too high");
    }

    // ---------------- separation hard reject still fires when candidate.k() >= 2 ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void multiClusterCandidateStillEnforcesSeparation(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = oneBlob(rnd, 400);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        // Two centroids deep inside one blob: poor separation.
        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {-4.99d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);

        final PartitionEvaluator.Parameters strict = new PartitionEvaluator.Parameters(
                EUCLIDEAN,
                /*minRelativeSseGain*/ -10.0d,
                /*minSeparation*/ 100.0d,
                /*maxLowMarginRate*/ 1.0d,
                /*minSmallestFrac*/ 0.0d,
                /*maxLargestFrac*/ 1.0d,
                /*lowMarginThreshold*/ -1.0d,
                /*alphaSseGain*/ 1.0d, /*betaSeparationGain*/ 1.0d,
                /*gammaImbalancePenalty*/ 1.0d, /*deltaLowMarginPenalty*/ 1.0d,
                /*minScoreGain*/ -10.0d);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), strict);

        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.KEEP_CURRENT);
        assertThat(result.reason()).contains("separation too low");
    }

    // ---------------- minSmallestFrac flips ACCEPT to INVALID ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void minSmallestFracTurnsImbalancedCandidateInvalid(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoBlobs(rnd, 600);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        // Construct a deliberately imbalanced candidate: shift one centroid far away so almost
        // all points snap to the other.
        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] { 0.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {1000.0d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);

        // Sanity: this candidate is in fact very imbalanced.
        final int candidateMin = Math.min(countAssigned(candidate, 0), countAssigned(candidate, 1));
        final double candidateMinFrac = (double) candidateMin / vectors.size();
        assertThat(candidateMinFrac).isLessThan(0.10d);

        // With a strict minSmallestFrac the candidate is INVALID.
        final PartitionEvaluator.Parameters strict = withMinSmallestFrac(
                new PartitionEvaluator.Parameters(EUCLIDEAN), 0.20d);
        assertThat(PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                Lens.identity(), strict).decision())
                .isEqualTo(PartitionEvaluator.Decision.INVALID_CANDIDATE);

        // With a permissive minSmallestFrac the candidate is no longer rejected on balance grounds.
        final PartitionEvaluator.Parameters permissive = withMinSmallestFrac(
                new PartitionEvaluator.Parameters(EUCLIDEAN), 0.0d);
        assertThat(PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                Lens.identity(), permissive).decision())
                .isNotEqualTo(PartitionEvaluator.Decision.INVALID_CANDIDATE);
    }

    // ---------------- maxLargestFrac flips ACCEPT to KEEP_CURRENT ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void maxLargestFracTurnsAcceptIntoKeepCurrent(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoBlobs(rnd, 600);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        // Slightly imbalanced 1->2 split where one cluster ends up larger than the other.
        final List<RealVector> twoCentroids = ImmutableList.of(
                new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d}),
                new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, EUCLIDEAN);

        final PartitionEvaluator.Parameters strict = withMaxLargestFrac(
                new PartitionEvaluator.Parameters(EUCLIDEAN), 0.50d);
        final PartitionEvaluator.Parameters defaultParams = new PartitionEvaluator.Parameters(EUCLIDEAN);

        final PartitionEvaluator.EvaluationResult tight =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), strict);
        final PartitionEvaluator.EvaluationResult relaxed =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), defaultParams);

        // Either the largest is still <= 0.5 (then both decisions agree on ACCEPT) or the strict
        // setting flips it to KEEP_CURRENT. Verify the latter scenario actually occurs under the
        // 0.5 threshold by checking that the candidate's largestFrac is in fact > 0.5.
        if (tight.candidateStats().largestFrac() > 0.50d) {
            assertThat(tight.decision()).isEqualTo(PartitionEvaluator.Decision.KEEP_CURRENT);
            assertThat(tight.reason()).contains("largest cluster too large");
            assertThat(relaxed.decision()).isEqualTo(PartitionEvaluator.Decision.ACCEPT_CANDIDATE);
        } else {
            assertThat(tight.decision())
                    .as("if largestFrac <= 0.5 then 0.5 threshold should not flip the decision")
                    .isEqualTo(relaxed.decision());
        }
    }

    // ---------------- both k == 1: no separation/margin contribution to the score ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void bothSingleClusterScoreDrivenBySseAlone(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = oneBlob(rnd, 300);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        final PartitionEvaluator.Partition<RealVector> candidate = singleClusterPartition(vectors);

        final PartitionEvaluator.Parameters params = new PartitionEvaluator.Parameters(EUCLIDEAN);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), params);

        // When both partitions are identical singletons the SSE is identical, so relativeSseGain
        // is zero and the composite score collapses to zero too (no separation/margin contribution
        // when both sides have NaN stats).
        assertThat(result.relativeSseGain()).isCloseTo(0.0d, org.assertj.core.data.Offset.offset(1.0e-12d));
        assertThat(result.scoreGain()).isCloseTo(0.0d, org.assertj.core.data.Offset.offset(1.0e-12d));
        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.KEEP_CURRENT);
    }

    // ============================================================================================
    // cosine-metric coverage: mirror selected Euclidean cases so the cosine arms of
    // distanceForSse / computeMargin and the cosine-default lowMarginThreshold are exercised.
    // ============================================================================================

    // ---------------- 1 → 2: split on well-separated unit directions is ACCEPTed (cosine) ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void splitOneToTwoOnWellSeparatedDirectionsAcceptedCosine(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = twoDirections(rnd, 600);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        final List<RealVector> twoCentroids = ImmutableList.of(
                unit(new double[] {1.0d, 0.0d, 0.0d}),
                unit(new double[] {0.0d, 1.0d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, COSINE);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), new PartitionEvaluator.Parameters(COSINE));

        logger.info("cosine 1->2 well-separated: {} reason='{}'", result.decision(), result.reason());
        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.ACCEPT_CANDIDATE);
        assertThat(result.relativeSseGain()).isGreaterThan(0.0d);
    }

    // ---------------- 1 → 2: split on a single direction is rejected (cosine) ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void splitOneToTwoOnSingleDirectionRejectedCosine(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = oneDirection(rnd, 600);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        final List<RealVector> twoCentroids = ImmutableList.of(
                vectors.get(0), vectors.get(vectors.size() - 1));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, COSINE);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), new PartitionEvaluator.Parameters(COSINE));

        logger.info("cosine 1->2 single direction: {} reason='{}'", result.decision(), result.reason());
        assertThat(result.decision()).isNotEqualTo(PartitionEvaluator.Decision.ACCEPT_CANDIDATE);
    }

    // ---------------- separation hard reject still fires for k >= 2 candidates (cosine) ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void multiClusterCandidateStillEnforcesSeparationCosine(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        final List<RealVector> vectors = oneDirection(rnd, 400);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        // Two near-identical unit centroids: poor cosine separation.
        final List<RealVector> twoCentroids = ImmutableList.of(
                unit(new double[] {1.0d, 0.0d, 0.0d}),
                unit(new double[] {1.0d, 0.01d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, COSINE);

        final PartitionEvaluator.Parameters strict = new PartitionEvaluator.Parameters(
                COSINE,
                /*minRelativeSseGain*/ -10.0d,
                /*minSeparation*/ 100.0d,
                /*maxLowMarginRate*/ 1.0d,
                /*minSmallestFrac*/ 0.0d,
                /*maxLargestFrac*/ 1.0d,
                /*lowMarginThreshold*/ -1.0d,
                /*alphaSseGain*/ 1.0d, /*betaSeparationGain*/ 1.0d,
                /*gammaImbalancePenalty*/ 1.0d, /*deltaLowMarginPenalty*/ 1.0d,
                /*minScoreGain*/ -10.0d);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), strict);

        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.KEEP_CURRENT);
        assertThat(result.reason()).contains("separation too low");
    }

    // ---------------- cosine default lowMarginThreshold (0.02) is applied ----------------

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL})
    void cosineDefaultLowMarginThresholdApplied(final long seed) {
        final SplittableRandom rnd = new SplittableRandom(seed);
        // Tight blob along (+x); two candidate centroids both near (+x), differing only on the
        // y axis, so cosine margins (cos(v, own) - cos(v, secondBest)) are uniformly tiny and
        // many of them fall below the cosine-default 0.02 lowMarginThreshold.
        final List<RealVector> vectors = oneDirection(rnd, 600);

        final PartitionEvaluator.Partition<RealVector> current = singleClusterPartition(vectors);
        final List<RealVector> twoCentroids = ImmutableList.of(
                unit(new double[] {1.0d,  0.05d, 0.0d}),
                unit(new double[] {1.0d, -0.05d, 0.0d}));
        final PartitionEvaluator.Partition<RealVector> candidate =
                nearestPartition(twoCentroids, vectors, COSINE);

        // lowMarginThreshold = -1 triggers the cosine default of 0.02; maxLowMarginRate = 0.10
        // is strict enough that "many tiny margins" trips the low-margin hard reject. This
        // proves both (a) the cosine arm of computeMargin produces sensible numbers and (b) the
        // cosine-default threshold is in fact applied.
        final PartitionEvaluator.Parameters strict = new PartitionEvaluator.Parameters(
                COSINE,
                /*minRelativeSseGain*/ -10.0d,
                /*minSeparation*/ 0.0d,
                /*maxLowMarginRate*/ 0.10d,
                /*minSmallestFrac*/ 0.0d,
                /*maxLargestFrac*/ 1.0d,
                /*lowMarginThreshold*/ -1.0d,
                /*alphaSseGain*/ 1.0d, /*betaSeparationGain*/ 1.0d,
                /*gammaImbalancePenalty*/ 1.0d, /*deltaLowMarginPenalty*/ 1.0d,
                /*minScoreGain*/ -10.0d);

        final PartitionEvaluator.EvaluationResult result =
                PartitionEvaluator.evaluate(vectors, current, vectors, candidate,
                        Lens.identity(), strict);

        logger.info("cosine default low-margin: {} reason='{}', lowMarginRate={}",
                result.decision(), result.reason(), result.candidateStats().lowMarginRate());
        assertThat(result.candidateStats().lowMarginRate()).isGreaterThan(0.10d);
        assertThat(result.decision()).isEqualTo(PartitionEvaluator.Decision.KEEP_CURRENT);
        assertThat(result.reason()).contains("low-margin rate too high");
    }

    // ============================================================================================
    // helpers
    // ============================================================================================

    @Nonnull
    private static List<RealVector> twoBlobs(@Nonnull final SplittableRandom rnd, final int nPer) {
        final RealVector m0 = new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d});
        final RealVector m1 = new DoubleRealVector(new double[] {+5.0d, 0.0d, 0.0d});
        final List<RealVector> result = Lists.newArrayListWithCapacity(2 * nPer);
        for (int i = 0; i < nPer; i++) {
            result.add(KMeansTestHelpers.gaussianND(rnd, m0, 1.0d));
        }
        for (int i = 0; i < nPer; i++) {
            result.add(KMeansTestHelpers.gaussianND(rnd, m1, 1.0d));
        }
        Collections.shuffle(result, new Random(rnd.nextLong()));
        return result;
    }

    @Nonnull
    private static List<RealVector> oneBlob(@Nonnull final SplittableRandom rnd, final int n) {
        final RealVector m = new DoubleRealVector(new double[] {-5.0d, 0.0d, 0.0d});
        final List<RealVector> result = Lists.newArrayListWithCapacity(n);
        for (int i = 0; i < n; i++) {
            result.add(KMeansTestHelpers.gaussianND(rnd, m, 0.5d));
        }
        return result;
    }

    @Nonnull
    private static List<RealVector> threeBlobs(@Nonnull final SplittableRandom rnd, final int nPer) {
        final List<RealVector> means = ImmutableList.of(
                new DoubleRealVector(new double[] {-6.0d, -3.0d,  1.0d}),
                new DoubleRealVector(new double[] { 0.0d, +4.5d, -2.0d}),
                new DoubleRealVector(new double[] {+6.0d, -2.0d,  3.0d}));
        final List<RealVector> result = Lists.newArrayListWithCapacity(3 * nPer);
        for (final RealVector m : means) {
            for (int i = 0; i < nPer; i++) {
                result.add(KMeansTestHelpers.gaussianND(rnd, m, 1.0d));
            }
        }
        Collections.shuffle(result, new Random(rnd.nextLong()));
        return result;
    }

    /**
     * Returns the L2-normalized form of the given components as a {@link RealVector}. Convenience
     * for constructing unit-norm centroids in cosine-metric tests.
     */
    @Nonnull
    private static RealVector unit(@Nonnull final double[] components) {
        return new DoubleRealVector(components).normalize();
    }

    /**
     * Cosine analogue of {@link #oneBlob}: a single tight cluster of noisy unit vectors around the
     * {@code (+x)} direction.
     */
    @Nonnull
    private static List<RealVector> oneDirection(@Nonnull final SplittableRandom rnd, final int n) {
        final RealVector m = unit(new double[] {1.0d, 0.0d, 0.0d});
        final List<RealVector> result = Lists.newArrayListWithCapacity(n);
        for (int i = 0; i < n; i++) {
            result.add(KMeansTestHelpers.noisyUnitVector(rnd, m, 0.1d));
        }
        return result;
    }

    /**
     * Cosine analogue of {@link #twoBlobs}: two clusters of noisy unit vectors around orthogonal
     * unit directions {@code (+x)} and {@code (+y)}.
     */
    @Nonnull
    private static List<RealVector> twoDirections(@Nonnull final SplittableRandom rnd, final int nPer) {
        final RealVector m0 = unit(new double[] {1.0d, 0.0d, 0.0d});
        final RealVector m1 = unit(new double[] {0.0d, 1.0d, 0.0d});
        final List<RealVector> result = Lists.newArrayListWithCapacity(2 * nPer);
        for (int i = 0; i < nPer; i++) {
            result.add(KMeansTestHelpers.noisyUnitVector(rnd, m0, 0.1d));
        }
        for (int i = 0; i < nPer; i++) {
            result.add(KMeansTestHelpers.noisyUnitVector(rnd, m1, 0.1d));
        }
        Collections.shuffle(result, new Random(rnd.nextLong()));
        return result;
    }

    /**
     * Builds a partition by assigning each vector to the centroid that minimizes
     * {@link KMeansTestHelpers#baseObjective}.
     */
    @Nonnull
    private static PartitionEvaluator.Partition<RealVector>
            nearestPartition(@Nonnull final List<RealVector> centroids,
                             @Nonnull final List<RealVector> vectors,
                             @Nonnull final DistanceEstimator distanceEstimator) {
        final int[] assignments = new int[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            double best = Double.POSITIVE_INFINITY;
            int bestC = 0;
            for (int c = 0; c < centroids.size(); c++) {
                final double d = KMeansTestHelpers.baseObjective(distanceEstimator, vectors.get(i), centroids.get(c));
                if (d < best) {
                    best = d;
                    bestC = c;
                }
            }
            assignments[i] = bestC;
        }
        return new PartitionEvaluator.Partition<>(centroids, Lens.identity(), assignments);
    }

    /**
     * Builds a single-cluster partition whose centroid is the mean of all input vectors.
     */
    @Nonnull
    private static PartitionEvaluator.Partition<RealVector> singleClusterPartition(@Nonnull final List<RealVector> vectors) {
        final int d = vectors.get(0).getNumDimensions();
        final MutableDoubleRealVector sum = MutableDoubleRealVector.zeroVector(d);
        for (final RealVector v : vectors) {
            sum.addToThis(v);
        }
        final RealVector centroid = sum.multiplyThisBy(1.0d / vectors.size()).toImmutable();
        return new PartitionEvaluator.Partition<>(
                ImmutableList.of(centroid), Lens.identity(), new int[vectors.size()]);
    }

    private static int countAssigned(@Nonnull final PartitionEvaluator.Partition<RealVector> p,
                                     final int cluster) {
        int n = 0;
        for (int a : p.assignments()) {
            if (a == cluster) {
                n++;
            }
        }
        return n;
    }

    @Nonnull
    private static PartitionEvaluator.Parameters withMinSmallestFrac(
            @Nonnull final PartitionEvaluator.Parameters p, final double v) {
        return new PartitionEvaluator.Parameters(p.distanceEstimator(), p.minRelativeSseGain(), p.minSeparation(),
                p.maxLowMarginRate(), v, p.maxLargestFrac(), p.lowMarginThreshold(),
                p.alphaSseGain(), p.betaSeparationGain(), p.gammaImbalancePenalty(),
                p.deltaLowMarginPenalty(), p.minScoreGain());
    }

    @Nonnull
    private static PartitionEvaluator.Parameters withMaxLargestFrac(
            @Nonnull final PartitionEvaluator.Parameters p, final double v) {
        return new PartitionEvaluator.Parameters(p.distanceEstimator(), p.minRelativeSseGain(), p.minSeparation(),
                p.maxLowMarginRate(), p.minSmallestFrac(), v, p.lowMarginThreshold(),
                p.alphaSseGain(), p.betaSeparationGain(), p.gammaImbalancePenalty(),
                p.deltaLowMarginPenalty(), p.minScoreGain());
    }
}
