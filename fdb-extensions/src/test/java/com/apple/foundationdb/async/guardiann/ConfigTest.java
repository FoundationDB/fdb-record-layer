/*
 * ConfigTest.java
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

import com.apple.foundationdb.linear.Metric;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConfigTest {
    private static final int NUM_DIMENSIONS = 128;

    @Test
    void testConfig() {
        final Config defaultConfig = Guardiann.defaultConfig(NUM_DIMENSIONS);

        Assertions.assertThat(Guardiann.newConfigBuilder().build(NUM_DIMENSIONS)).isEqualTo(defaultConfig);
        Assertions.assertThat(defaultConfig.toBuilder().build(NUM_DIMENSIONS)).isEqualTo(defaultConfig);

        final Metric metric = Metric.COSINE_METRIC;
        final int primaryClusterMin = Config.DEFAULT_PRIMARY_CLUSTER_MIN + 1;
        final int primaryClusterMax = Config.DEFAULT_PRIMARY_CLUSTER_MAX + 1;
        final int primaryClusterHardMax = Config.DEFAULT_PRIMARY_CLUSTER_HARD_MAX + 1;
        final int underreplicatedPrimaryClusterMax = Config.DEFAULT_UNDERREPLICATED_PRIMARY_CLUSTER_MAX + 1;
        final int replicatedClusterMaxWrites = Config.DEFAULT_REPLICATED_CLUSTER_MAX_WRITES + 1;
        final int replicatedClusterTarget = Config.DEFAULT_REPLICATED_CLUSTER_TARGET + 1;
        final double replicationPriorityMin = 0.123d;
        final double replicationDistanceRatioWeight = 0.234d;
        final double replicationZScoreWeight = 0.345d;
        final int replicationStatsMinSampleSize = Config.DEFAULT_REPLICATION_STATS_MIN_SAMPLE_SIZE + 1;
        final double sampleVectorStatsProbability = 0.000001d;
        final double maintainStatsProbability = 0.000002d;
        final int statsThreshold = Config.DEFAULT_STATS_THRESHOLD + 1;
        final boolean useRaBitQ = true;
        final int raBitQNumExBits = Config.DEFAULT_RABITQ_NUM_EX_BITS + 1;
        final boolean deterministicRandomness = true;
        final int sampleBatchSize = Config.DEFAULT_SAMPLE_BATCH_SIZE + 1;
        final int insertMaxCandidateClusters = Config.DEFAULT_INSERT_MAX_CANDIDATE_CLUSTERS + 1;
        final int deleteMaxCandidateClusters = Config.DEFAULT_DELETE_MAX_CANDIDATE_CLUSTERS + 1;
        final int deleteConcurrency = Config.DEFAULT_DELETE_CONCURRENCY + 1;
        final int splitNumNearestClusters = Config.DEFAULT_SPLIT_NUM_NEAREST_CLUSTERS + 1;
        final int mergeNumNearestClusters = Config.DEFAULT_MERGE_NUM_NEAREST_CLUSTERS + 1;
        final int kMeansMaxIterations = Config.DEFAULT_KMEANS_MAX_ITERATIONS + 1;
        final int kMeansMaxRestarts = Config.DEFAULT_KMEANS_MAX_RESTARTS + 1;
        final int reassignNumNeighboringClusters = Config.DEFAULT_REASSIGN_NUM_NEIGHBORING_CLUSTERS + 1;
        final int collapseMinDuplicates = Config.DEFAULT_COLLAPSE_MIN_DUPLICATES + 1;
        final int splitMergeConcurrency = Config.DEFAULT_SPLIT_MERGE_CONCURRENCY + 1;
        final int reassignConcurrency = Config.DEFAULT_REASSIGN_CONCURRENCY + 1;
        final int collapseConcurrency = Config.DEFAULT_COLLAPSE_CONCURRENCY + 1;
        final int bounceConcurrency = Config.DEFAULT_BOUNCE_CONCURRENCY + 1;
        final SearchConfig constructionSearchConfig = new SearchConfig.SearchConfigBuilder()
                .setCentroidEfRingSearch(SearchConfig.DEFAULT_CENTROID_EF_RING_SEARCH + 1)
                .build();
        final double mergeMaxEverFraction = 0.42d;
        final double minChildFraction = 0.07d;
        final double maxRelativeImbalance = 0.44d;
        final double splitImbalancePenalty = 2.5d;

        Assertions.assertThat(defaultConfig.metric()).isNotSameAs(metric);
        Assertions.assertThat(defaultConfig.primaryClusterMin()).isNotEqualTo(primaryClusterMin);
        Assertions.assertThat(defaultConfig.primaryClusterMax()).isNotEqualTo(primaryClusterMax);
        Assertions.assertThat(defaultConfig.primaryClusterHardMax()).isNotEqualTo(primaryClusterHardMax);
        Assertions.assertThat(defaultConfig.underreplicatedPrimaryClusterMax()).isNotEqualTo(underreplicatedPrimaryClusterMax);
        Assertions.assertThat(defaultConfig.replicatedClusterMaxWrites()).isNotEqualTo(replicatedClusterMaxWrites);
        Assertions.assertThat(defaultConfig.replicatedClusterTarget()).isNotEqualTo(replicatedClusterTarget);
        Assertions.assertThat(defaultConfig.replicationPriorityMin()).isNotEqualTo(replicationPriorityMin);
        Assertions.assertThat(defaultConfig.replicationDistanceRatioWeight()).isNotEqualTo(replicationDistanceRatioWeight);
        Assertions.assertThat(defaultConfig.replicationZScoreWeight()).isNotEqualTo(replicationZScoreWeight);
        Assertions.assertThat(defaultConfig.replicationStatsMinSampleSize()).isNotEqualTo(replicationStatsMinSampleSize);
        Assertions.assertThat(defaultConfig.sampleVectorStatsProbability()).isNotEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(defaultConfig.maintainStatsProbability()).isNotEqualTo(maintainStatsProbability);
        Assertions.assertThat(defaultConfig.statsThreshold()).isNotEqualTo(statsThreshold);
        Assertions.assertThat(defaultConfig.useRaBitQ()).isNotEqualTo(useRaBitQ);
        Assertions.assertThat(defaultConfig.raBitQNumExBits()).isNotEqualTo(raBitQNumExBits);
        Assertions.assertThat(defaultConfig.deterministicRandomness()).isNotEqualTo(deterministicRandomness);
        Assertions.assertThat(defaultConfig.sampleBatchSize()).isNotEqualTo(sampleBatchSize);
        Assertions.assertThat(defaultConfig.insertMaxCandidateClusters()).isNotEqualTo(insertMaxCandidateClusters);
        Assertions.assertThat(defaultConfig.deleteMaxCandidateClusters()).isNotEqualTo(deleteMaxCandidateClusters);
        Assertions.assertThat(defaultConfig.deleteConcurrency()).isNotEqualTo(deleteConcurrency);
        Assertions.assertThat(defaultConfig.splitNumNearestClusters()).isNotEqualTo(splitNumNearestClusters);
        Assertions.assertThat(defaultConfig.mergeNumNearestClusters()).isNotEqualTo(mergeNumNearestClusters);
        Assertions.assertThat(defaultConfig.kMeansMaxIterations()).isNotEqualTo(kMeansMaxIterations);
        Assertions.assertThat(defaultConfig.kMeansMaxRestarts()).isNotEqualTo(kMeansMaxRestarts);
        Assertions.assertThat(defaultConfig.reassignNumNeighboringClusters()).isNotEqualTo(reassignNumNeighboringClusters);
        Assertions.assertThat(defaultConfig.collapseMinDuplicates()).isNotEqualTo(collapseMinDuplicates);
        Assertions.assertThat(defaultConfig.splitMergeConcurrency()).isNotEqualTo(splitMergeConcurrency);
        Assertions.assertThat(defaultConfig.reassignConcurrency()).isNotEqualTo(reassignConcurrency);
        Assertions.assertThat(defaultConfig.collapseConcurrency()).isNotEqualTo(collapseConcurrency);
        Assertions.assertThat(defaultConfig.bounceConcurrency()).isNotEqualTo(bounceConcurrency);
        Assertions.assertThat(defaultConfig.constructionSearchConfig()).isNotEqualTo(constructionSearchConfig);
        Assertions.assertThat(defaultConfig.mergeMaxEverFraction()).isNotEqualTo(mergeMaxEverFraction);
        Assertions.assertThat(defaultConfig.minChildFraction()).isNotEqualTo(minChildFraction);
        Assertions.assertThat(defaultConfig.maxRelativeImbalance()).isNotEqualTo(maxRelativeImbalance);
        Assertions.assertThat(defaultConfig.splitImbalancePenalty()).isNotEqualTo(splitImbalancePenalty);

        final Config newConfig =
                defaultConfig.toBuilder()
                        .setMetric(metric)
                        .setPrimaryClusterMin(primaryClusterMin)
                        .setPrimaryClusterMax(primaryClusterMax)
                        .setPrimaryClusterHardMax(primaryClusterHardMax)
                        .setUnderreplicatedPrimaryClusterMax(underreplicatedPrimaryClusterMax)
                        .setReplicatedClusterMaxWrites(replicatedClusterMaxWrites)
                        .setReplicatedClusterTarget(replicatedClusterTarget)
                        .setReplicationPriorityMin(replicationPriorityMin)
                        .setReplicationDistanceRatioWeight(replicationDistanceRatioWeight)
                        .setReplicationZScoreWeight(replicationZScoreWeight)
                        .setReplicationStatsMinSampleSize(replicationStatsMinSampleSize)
                        .setSampleVectorStatsProbability(sampleVectorStatsProbability)
                        .setMaintainStatsProbability(maintainStatsProbability)
                        .setStatsThreshold(statsThreshold)
                        .setUseRaBitQ(useRaBitQ)
                        .setRaBitQNumExBits(raBitQNumExBits)
                        .setDeterministicRandomness(deterministicRandomness)
                        .setSampleBatchSize(sampleBatchSize)
                        .setInsertMaxCandidateClusters(insertMaxCandidateClusters)
                        .setDeleteMaxCandidateClusters(deleteMaxCandidateClusters)
                        .setDeleteConcurrency(deleteConcurrency)
                        .setSplitNumNearestClusters(splitNumNearestClusters)
                        .setMergeNumNearestClusters(mergeNumNearestClusters)
                        .setKMeansMaxIterations(kMeansMaxIterations)
                        .setKMeansMaxRestarts(kMeansMaxRestarts)
                        .setReassignNumNeighboringClusters(reassignNumNeighboringClusters)
                        .setCollapseMinDuplicates(collapseMinDuplicates)
                        .setSplitMergeConcurrency(splitMergeConcurrency)
                        .setReassignConcurrency(reassignConcurrency)
                        .setCollapseConcurrency(collapseConcurrency)
                        .setBounceConcurrency(bounceConcurrency)
                        .setConstructionSearchConfig(constructionSearchConfig)
                        .setMergeMaxEverFraction(mergeMaxEverFraction)
                        .setMinChildFraction(minChildFraction)
                        .setMaxRelativeImbalance(maxRelativeImbalance)
                        .setSplitImbalancePenalty(splitImbalancePenalty)
                        .build(NUM_DIMENSIONS);

        Assertions.assertThat(newConfig.metric()).isSameAs(metric);
        Assertions.assertThat(newConfig.primaryClusterMin()).isEqualTo(primaryClusterMin);
        Assertions.assertThat(newConfig.primaryClusterMax()).isEqualTo(primaryClusterMax);
        Assertions.assertThat(newConfig.primaryClusterHardMax()).isEqualTo(primaryClusterHardMax);
        Assertions.assertThat(newConfig.underreplicatedPrimaryClusterMax()).isEqualTo(underreplicatedPrimaryClusterMax);
        Assertions.assertThat(newConfig.replicatedClusterMaxWrites()).isEqualTo(replicatedClusterMaxWrites);
        Assertions.assertThat(newConfig.replicatedClusterTarget()).isEqualTo(replicatedClusterTarget);
        Assertions.assertThat(newConfig.replicationPriorityMin()).isEqualTo(replicationPriorityMin);
        Assertions.assertThat(newConfig.replicationDistanceRatioWeight()).isEqualTo(replicationDistanceRatioWeight);
        Assertions.assertThat(newConfig.replicationZScoreWeight()).isEqualTo(replicationZScoreWeight);
        Assertions.assertThat(newConfig.replicationStatsMinSampleSize()).isEqualTo(replicationStatsMinSampleSize);
        Assertions.assertThat(newConfig.sampleVectorStatsProbability()).isEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(newConfig.maintainStatsProbability()).isEqualTo(maintainStatsProbability);
        Assertions.assertThat(newConfig.statsThreshold()).isEqualTo(statsThreshold);
        Assertions.assertThat(newConfig.useRaBitQ()).isEqualTo(useRaBitQ);
        Assertions.assertThat(newConfig.raBitQNumExBits()).isEqualTo(raBitQNumExBits);
        Assertions.assertThat(newConfig.deterministicRandomness()).isEqualTo(deterministicRandomness);
        Assertions.assertThat(newConfig.sampleBatchSize()).isEqualTo(sampleBatchSize);
        Assertions.assertThat(newConfig.insertMaxCandidateClusters()).isEqualTo(insertMaxCandidateClusters);
        Assertions.assertThat(newConfig.deleteMaxCandidateClusters()).isEqualTo(deleteMaxCandidateClusters);
        Assertions.assertThat(newConfig.deleteConcurrency()).isEqualTo(deleteConcurrency);
        Assertions.assertThat(newConfig.splitNumNearestClusters()).isEqualTo(splitNumNearestClusters);
        Assertions.assertThat(newConfig.mergeNumNearestClusters()).isEqualTo(mergeNumNearestClusters);
        Assertions.assertThat(newConfig.kMeansMaxIterations()).isEqualTo(kMeansMaxIterations);
        Assertions.assertThat(newConfig.kMeansMaxRestarts()).isEqualTo(kMeansMaxRestarts);
        Assertions.assertThat(newConfig.reassignNumNeighboringClusters()).isEqualTo(reassignNumNeighboringClusters);
        Assertions.assertThat(newConfig.collapseMinDuplicates()).isEqualTo(collapseMinDuplicates);
        Assertions.assertThat(newConfig.splitMergeConcurrency()).isEqualTo(splitMergeConcurrency);
        Assertions.assertThat(newConfig.reassignConcurrency()).isEqualTo(reassignConcurrency);
        Assertions.assertThat(newConfig.collapseConcurrency()).isEqualTo(collapseConcurrency);
        Assertions.assertThat(newConfig.bounceConcurrency()).isEqualTo(bounceConcurrency);
        Assertions.assertThat(newConfig.constructionSearchConfig()).isEqualTo(constructionSearchConfig);
        Assertions.assertThat(newConfig.mergeMaxEverFraction()).isEqualTo(mergeMaxEverFraction);
        Assertions.assertThat(newConfig.minChildFraction()).isEqualTo(minChildFraction);
        Assertions.assertThat(newConfig.maxRelativeImbalance()).isEqualTo(maxRelativeImbalance);
        Assertions.assertThat(newConfig.splitImbalancePenalty()).isEqualTo(splitImbalancePenalty);

        // Round-tripping a config whose every component differs from the default is what actually pins toBuilder():
        // doing it on the default config above cannot detect a component that toBuilder() drops or transposes,
        // since the rebuilt builder would fall back to exactly the value that was lost.
        Assertions.assertThat(newConfig.toBuilder().build(NUM_DIMENSIONS)).isEqualTo(newConfig);
    }

    @Test
    void testEqualsHashCodeAndToString() {
        final Config config1 = Guardiann.newConfigBuilder().build(NUM_DIMENSIONS);
        final Config config2 = Guardiann.newConfigBuilder().build(NUM_DIMENSIONS);
        // collapseMinDuplicates must stay below primaryClusterMax (Config invariant), so lower it alongside the cap.
        final Config config3 = Guardiann.newConfigBuilder().setPrimaryClusterMax(4).setCollapseMinDuplicates(3)
                .build(NUM_DIMENSIONS);

        Assertions.assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        Assertions.assertThat(config1).isEqualTo(config2);
        Assertions.assertThat(config3).isNotEqualTo(config1);

        Assertions.assertThat(config1.toString()).isEqualTo(config2.toString());
        Assertions.assertThat(config1.toString()).isNotEqualTo(config3.toString());
    }

    @Test
    void testNumDimensions() {
        Assertions.assertThat(Guardiann.defaultConfig(NUM_DIMENSIONS).numDimensions()).isEqualTo(NUM_DIMENSIONS);
        Assertions.assertThat(Guardiann.defaultConfig(1).numDimensions()).isEqualTo(1);

        Assertions.assertThatThrownBy(() -> Guardiann.defaultConfig(0))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> Guardiann.defaultConfig(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPrimaryClusterHardMaxMustExceedMax() {
        // The hard cap must sit strictly above the split threshold; equal is not enough (it would back-pressure before
        // the normal split ever triggers). collapseMinDuplicates is kept below primaryClusterMax so that the collapse
        // invariant passes and the hard-cap invariant is the one exercised here.
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setPrimaryClusterMax(100).setCollapseMinDuplicates(50).setPrimaryClusterHardMax(100)
                        .build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setPrimaryClusterMax(100).setCollapseMinDuplicates(50).setPrimaryClusterHardMax(99)
                        .build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);

        Assertions.assertThat(Guardiann.newConfigBuilder()
                        .setPrimaryClusterMax(100).setCollapseMinDuplicates(50).setPrimaryClusterHardMax(101)
                        .build(NUM_DIMENSIONS)
                        .primaryClusterHardMax())
                .isEqualTo(101);
    }

    @Test
    void testMinChildFractionMustBeInLowerHalfOfUnitInterval() {
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMinChildFraction(-0.01d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMinChildFraction(0.5d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThat(Guardiann.newConfigBuilder()
                        .setMinChildFraction(0.0d).build(NUM_DIMENSIONS).minChildFraction())
                .isZero();
        Assertions.assertThat(Guardiann.newConfigBuilder()
                        .setMinChildFraction(0.499d).build(NUM_DIMENSIONS).minChildFraction())
                .isEqualTo(0.499d);
    }

    @Test
    void testMaxRelativeImbalanceMustBeInClosedUnitInterval() {
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMaxRelativeImbalance(-0.01d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMaxRelativeImbalance(1.01d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        // Both bounds are attainable: 0 admits only perfectly even partitionings, 1 disables the gate.
        Assertions.assertThat(Guardiann.newConfigBuilder()
                        .setMaxRelativeImbalance(0.0d).build(NUM_DIMENSIONS).maxRelativeImbalance())
                .isZero();
        Assertions.assertThat(Guardiann.newConfigBuilder()
                        .setMaxRelativeImbalance(1.0d).build(NUM_DIMENSIONS).maxRelativeImbalance())
                .isEqualTo(1.0d);
    }

    @Test
    void testSplitImbalancePenaltyMustBeNonNegative() {
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setSplitImbalancePenalty(-0.1d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        // Zero is legal and means imbalance does not influence the score at all.
        Assertions.assertThat(Guardiann.newConfigBuilder()
                        .setSplitImbalancePenalty(0.0d).build(NUM_DIMENSIONS).splitImbalancePenalty())
                .isZero();
    }

    @Test
    void testMergeMaxEverFractionMustBeStrictlyBetweenZeroAndOne() {
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMergeMaxEverFraction(0.0d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMergeMaxEverFraction(-0.1d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMergeMaxEverFraction(1.5d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> Guardiann.newConfigBuilder()
                        .setMergeMaxEverFraction(1.0d).build(NUM_DIMENSIONS))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThat(Guardiann.newConfigBuilder()
                        .setMergeMaxEverFraction(0.999d).build(NUM_DIMENSIONS).mergeMaxEverFraction())
                .isEqualTo(0.999d);
    }
}
