/*
 * SplitMergeTaskTest.java
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

import com.apple.foundationdb.kmeans.PartitionEvaluator;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for how {@link SplitMergeTask} ranks repartitioning candidates against each other.
 * <p>
 * This is what gives the evaluator's soft gates — the balance ceiling, separation, low-margin rate and the score
 * floors — any effect at all. Each of them reports {@link PartitionEvaluator.Decision#KEEP_CURRENT}, which leaves a
 * candidate usable but not preferred, and none of them touches {@code scoreGain}, which is computed before any gate
 * runs. So a candidate that trips a soft gate is distinguishable from one that clears them all only by its
 * {@code decision()}; ranking on {@code scoreGain} alone would make every soft gate inert.
 */
class SplitMergeTaskTest {

    @Test
    void acceptedCandidateBeatsKeepCurrentEvenWithLowerScore() {
        final PartitionEvaluator.EvaluationResult accepted =
                result(PartitionEvaluator.Decision.ACCEPT_CANDIDATE, 0.1d);
        final PartitionEvaluator.EvaluationResult keepCurrent =
                result(PartitionEvaluator.Decision.KEEP_CURRENT, 10.0d);

        assertThat(SplitMergeTask.isBetterCandidate(accepted, keepCurrent)).isTrue();
        assertThat(SplitMergeTask.isBetterCandidate(keepCurrent, accepted)).isFalse();
    }

    @Test
    void withinTheSameVerdictTheHigherScoreWins() {
        final PartitionEvaluator.EvaluationResult weaker =
                result(PartitionEvaluator.Decision.ACCEPT_CANDIDATE, 0.2d);
        final PartitionEvaluator.EvaluationResult stronger =
                result(PartitionEvaluator.Decision.ACCEPT_CANDIDATE, 0.5d);

        assertThat(SplitMergeTask.isBetterCandidate(stronger, weaker)).isTrue();
        assertThat(SplitMergeTask.isBetterCandidate(weaker, stronger)).isFalse();

        final PartitionEvaluator.EvaluationResult weakKeep =
                result(PartitionEvaluator.Decision.KEEP_CURRENT, 0.2d);
        final PartitionEvaluator.EvaluationResult strongKeep =
                result(PartitionEvaluator.Decision.KEEP_CURRENT, 0.5d);

        assertThat(SplitMergeTask.isBetterCandidate(strongKeep, weakKeep)).isTrue();
        assertThat(SplitMergeTask.isBetterCandidate(weakKeep, strongKeep)).isFalse();
    }

    @Test
    void anEqualScoreDoesNotDisplaceTheIncumbent() {
        final PartitionEvaluator.EvaluationResult first =
                result(PartitionEvaluator.Decision.ACCEPT_CANDIDATE, 0.42d);
        final PartitionEvaluator.EvaluationResult second =
                result(PartitionEvaluator.Decision.ACCEPT_CANDIDATE, 0.42d);

        assertThat(SplitMergeTask.isBetterCandidate(second, first)).isFalse();
    }

    /**
     * Builds an evaluation result carrying only the two fields the ranking consults. The statistics are placeholders:
     * the ranking never reads them, and pinning that is part of the point.
     */
    @Nonnull
    private static PartitionEvaluator.EvaluationResult result(@Nonnull final PartitionEvaluator.Decision decision,
                                                              final double scoreGain) {
        final PartitionEvaluator.PartitionStats stats =
                new PartitionEvaluator.PartitionStats(2, 1.0d, 0.0d, 1.0d, 0.5d, 0.5d, 1.0d, 1.0d, 1.0d, 0.0d);
        return new PartitionEvaluator.EvaluationResult(decision, stats, stats, 0.0d, scoreGain, "test");
    }
}
