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
        final int maxNumConcurrentNodeFetches = Config.DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES + 1;
        final int maxNumConcurrentNeighborhoodFetches = Config.DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES + 1;
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
        final SearchConfig constructionSearchConfig = new SearchConfig.SearchConfigBuilder()
                .setCentroidEfRingSearch(SearchConfig.DEFAULT_CENTROID_EF_RING_SEARCH + 1)
                .build();

        Assertions.assertThat(defaultConfig.metric()).isNotSameAs(metric);
        Assertions.assertThat(defaultConfig.primaryClusterMin()).isNotEqualTo(primaryClusterMin);
        Assertions.assertThat(defaultConfig.primaryClusterMax()).isNotEqualTo(primaryClusterMax);
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
        Assertions.assertThat(defaultConfig.maxNumConcurrentNodeFetches()).isNotEqualTo(maxNumConcurrentNodeFetches);
        Assertions.assertThat(defaultConfig.maxNumConcurrentNeighborhoodFetches()).isNotEqualTo(maxNumConcurrentNeighborhoodFetches);
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
        Assertions.assertThat(defaultConfig.constructionSearchConfig()).isNotEqualTo(constructionSearchConfig);

        final Config newConfig =
                defaultConfig.toBuilder()
                        .setMetric(metric)
                        .setPrimaryClusterMin(primaryClusterMin)
                        .setPrimaryClusterMax(primaryClusterMax)
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
                        .setMaxNumConcurrentNodeFetches(maxNumConcurrentNodeFetches)
                        .setMaxNumConcurrentNeighborhoodFetches(maxNumConcurrentNeighborhoodFetches)
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
                        .setConstructionSearchConfig(constructionSearchConfig)
                        .build(NUM_DIMENSIONS);

        Assertions.assertThat(newConfig.metric()).isSameAs(metric);
        Assertions.assertThat(newConfig.primaryClusterMin()).isEqualTo(primaryClusterMin);
        Assertions.assertThat(newConfig.primaryClusterMax()).isEqualTo(primaryClusterMax);
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
        Assertions.assertThat(newConfig.maxNumConcurrentNodeFetches()).isEqualTo(maxNumConcurrentNodeFetches);
        Assertions.assertThat(newConfig.maxNumConcurrentNeighborhoodFetches()).isEqualTo(maxNumConcurrentNeighborhoodFetches);
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
        Assertions.assertThat(newConfig.constructionSearchConfig()).isEqualTo(constructionSearchConfig);
    }

    @Test
    void testEqualsHashCodeAndToString() {
        final Config config1 = Guardiann.newConfigBuilder().build(NUM_DIMENSIONS);
        final Config config2 = Guardiann.newConfigBuilder().build(NUM_DIMENSIONS);
        final Config config3 = Guardiann.newConfigBuilder().setPrimaryClusterMax(4).build(NUM_DIMENSIONS);

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
}
