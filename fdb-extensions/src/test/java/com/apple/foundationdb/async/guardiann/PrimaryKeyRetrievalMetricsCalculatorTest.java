/*
 * PrimaryKeyRetrievalMetricsCalculatorTest.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.async.common.CommonTestHelpers;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.PrimaryKeyVectorAndDistance;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link PrimaryKeyRetrievalMetricsCalculator}: recall, nDCG and Kendall similarity of hand-computed
 * examples, their edge cases, the static and dynamic computations of {@link RetrievalMetricsCalculator}, and the order
 * of {@link GuardiannTestHelpers#bruteForceTopK}.
 */
class PrimaryKeyRetrievalMetricsCalculatorTest {
    private static final PrimaryKeyRetrievalMetricsCalculator CALCULATOR = new PrimaryKeyRetrievalMetricsCalculator();
    private static final List<Tuple> TRUTH = keys(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    @Test
    void perfectResultsScoreOne() {
        final QueryRetrievalMetrics metrics = CALCULATOR.computeQuery(TRUTH, TRUTH, 10);

        assertThat(metrics).isEqualTo(new QueryRetrievalMetrics(1.0d, 1.0d, 1.0d));
    }

    @Test
    void weakestRecordFirst() {
        // All found, but the 10th nearest first: nDCG 24.510 / 29.966; 9 discordant pairs of K_max = 145.
        final QueryRetrievalMetrics metrics = CALCULATOR.computeQuery(TRUTH, keys(10, 1, 2, 3, 4, 5, 6, 7, 8, 9), 10);

        assertThat(metrics.recall()).isEqualTo(1.0d);
        assertThat(metrics.ndcg()).isCloseTo(0.8179d, within(1.0e-4));
        assertThat(metrics.kendallSimilarity()).isCloseTo(1.0d - 9.0d / 145.0d, within(1.0e-9));
    }

    @Test
    void sameRecordsDifferentPositions() {
        // 2 and 8 missed; 101 and 102 are not in the ground truth. A and B differ only in where 5 and the extras are.
        final QueryRetrievalMetrics resultA =
                CALCULATOR.computeQuery(TRUTH, keys(1, 5, 3, 4, 101, 6, 7, 102, 9, 10), 10);
        final QueryRetrievalMetrics resultB =
                CALCULATOR.computeQuery(TRUTH, keys(1, 101, 3, 4, 102, 6, 7, 5, 9, 10), 10);

        assertThat(resultA.recall()).isEqualTo(0.8d);
        assertThat(resultB.recall()).isEqualTo(0.8d);
        assertThat(resultA.ndcg()).isCloseTo(0.8278d, within(1.0e-4));
        assertThat(resultB.ndcg()).isCloseTo(0.7646d, within(1.0e-4));
        // K = 2 discordant + 9 missing-above + extra-above (6 for A, 12 for B) + 4 crossed + 0.5 * 2 unknown pairs.
        assertThat(resultA.kendallSimilarity()).isCloseTo(1.0d - 22.0d / 145.0d, within(1.0e-9));
        assertThat(resultB.kendallSimilarity()).isCloseTo(1.0d - 28.0d / 145.0d, within(1.0e-9));
    }

    @Test
    void repeatedResultTakesAPositionButGainsNothing() {
        final QueryRetrievalMetrics withRepeat =
                CALCULATOR.computeQuery(TRUTH, keys(1, 1, 2, 3, 4, 5, 6, 7, 8, 9), 10);
        final QueryRetrievalMetrics withExtra =
                CALCULATOR.computeQuery(TRUTH, keys(1, 2, 3, 4, 5, 6, 7, 8, 9, 101), 10);

        assertThat(withRepeat.recall()).isEqualTo(0.9d);
        assertThat(withRepeat.ndcg()).isLessThan(withExtra.ndcg());
        assertThat(withExtra.ndcg()).isCloseTo(0.9904d, within(1.0e-4));
    }

    @Test
    void disjointOrEmptyResultsScoreZero() {
        final QueryRetrievalMetrics disjoint = CALCULATOR.computeQuery(TRUTH, keys(101, 102, 103), 10);
        final QueryRetrievalMetrics empty = CALCULATOR.computeQuery(TRUTH, List.of(), 10);

        assertThat(disjoint).isEqualTo(new QueryRetrievalMetrics(0.0d, 0.0d, 0.0d));
        assertThat(empty).isEqualTo(new QueryRetrievalMetrics(0.0d, 0.0d, 0.0d));
    }

    @Test
    void onlyTheFirstKRecordsCount() {
        assertThat(CALCULATOR.computeQuery(TRUTH, keys(1, 2), 2))
                .isEqualTo(new QueryRetrievalMetrics(1.0d, 1.0d, 1.0d));
        assertThat(CALCULATOR.computeQuery(keys(1, 2), keys(1, 7, 8, 9), 4).recall()).isEqualTo(0.5d);
    }

    @Test
    void emptyGroundTruthIsUndefined() {
        assertThat(CALCULATOR.computeQuery(List.of(), keys(1), 1)).isSameAs(QueryRetrievalMetrics.UNDEFINED);
    }

    @Test
    void computeMeasuresEveryQueryWithGroundTruth() {
        final List<RealVector> queries = List.of(vector(1.0d), vector(2.0d), vector(3.0d));
        final List<Set<Integer>> groundTruth =
                List.of(ImmutableSet.of(1, 2), ImmutableSet.of(), ImmutableSet.of(3, 4));
        final AtomicInteger searches = new AtomicInteger();

        final RetrievalMetrics metrics = CALCULATOR.compute((query, k) -> {
            searches.incrementAndGet();
            return keys(1, 2);
        }, queries, groundTruth, 2);

        assertThat(searches).hasValue(2);
        assertThat(metrics.perQuery()).containsExactly(new QueryRetrievalMetrics(1.0d, 1.0d, 1.0d),
                QueryRetrievalMetrics.UNDEFINED, new QueryRetrievalMetrics(0.0d, 0.0d, 0.0d));
        assertThat(metrics.countedQueries()).isEqualTo(2);
        assertThat(metrics.meanRecall()).isEqualTo(0.5d);
        assertThat(metrics.meanNdcg()).isEqualTo(0.5d);
        assertThat(metrics.meanKendallSimilarity()).isEqualTo(0.5d);
    }

    @Test
    void computeRanksTheGroundTruthInItsIterationOrder() {
        // The same two records, ranked 2 before 1 by the ground truth: all found, but in the wrong order.
        final RetrievalMetrics metrics = CALCULATOR.compute((query, k) -> keys(1, 2), List.of(vector(1.0d)),
                List.of(ImmutableSet.of(2, 1)), 2);

        final QueryRetrievalMetrics query = metrics.perQuery().get(0);
        assertThat(query.recall()).isEqualTo(1.0d);
        assertThat(query.ndcg()).isLessThan(1.0d);
        assertThat(query.kendallSimilarity()).isLessThan(1.0d);
    }

    @Test
    void computeDynamicScoresExactSearchAsPerfect() {
        final Random random = new Random(0x5eedL);
        final List<PrimaryKeyAndVector> records = CommonTestHelpers.randomVectors(random, 8, 300);
        final Map<Tuple, RealVector> active = GuardiannTestHelpers.activeMapOf(records);
        final Metric metric = Metric.EUCLIDEAN_SQUARE_METRIC;
        final List<RealVector> queries = List.of(records.get(0).vector(), records.get(1).vector());

        final RetrievalMetrics metrics = CALCULATOR.computeDynamic((query, k) ->
                GuardiannTestHelpers.bruteForceTopK(metric, query, active, k).stream()
                        .map(PrimaryKeyVectorAndDistance::primaryKey)
                        .toList(), metric, queries, active, 20);

        assertThat(metrics.countedQueries()).isEqualTo(2);
        assertThat(metrics.meanRecall()).isEqualTo(1.0d);
        assertThat(metrics.meanNdcg()).isEqualTo(1.0d);
        assertThat(metrics.meanKendallSimilarity()).isEqualTo(1.0d);
    }

    @Test
    void bruteForceTopKIsTheStartOfOrderedByDistances() {
        final Random random = new Random(0xdecafL);
        final List<PrimaryKeyAndVector> records = new ArrayList<>(CommonTestHelpers.randomVectors(random, 8, 300));
        // A duplicate of the query's own record, so that the order has a tie to break.
        records.add(new PrimaryKeyAndVector(Tuple.from(1_000L), records.get(3).vector()));
        final RealVector query = records.get(3).vector();
        final Metric metric = Metric.EUCLIDEAN_SQUARE_METRIC;

        final List<PrimaryKeyVectorAndDistance> topK =
                GuardiannTestHelpers.bruteForceTopK(metric, query, GuardiannTestHelpers.activeMapOf(records), 25);

        final List<PrimaryKeyVectorAndDistance> expected =
                new ArrayList<>(CommonTestHelpers.orderedByDistances(metric, records, query)).subList(0, 25);
        assertThat(topK).isEqualTo(expected);
    }

    @Nonnull
    private static List<Tuple> keys(final int... indices) {
        return IntStream.of(indices).mapToObj(index -> Tuple.from((long) index)).toList();
    }

    @Nonnull
    private static RealVector vector(final double value) {
        return new DoubleRealVector(new double[] {value, value});
    }
}
