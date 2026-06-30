/*
 * Config.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.common.VectorEncodingConfig;
import com.apple.foundationdb.linear.Metric;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * Configuration for the Guardiann vector structure.
 *
 * @param metric the metric in use for this Guardiann structure
 * @param numDimensions the number of dimensions of the vectors stored
 * @param primaryClusterMin minimum number of primary vectors in a cluster, underflow will result in a merge task to be
 *        enqueued
 * @param primaryClusterMax maximum number of primary vectors in a cluster, overflow will result in a split task to be
 *        enqueued
 * @param underreplicatedPrimaryClusterMax maximum number of under-replicated primary vectors in a cluster, overflow
 *        will result in a reassign task to be enqueued
 * @param replicatedClusterMaxWrites maximum number of writes of replicated vectors to a cluster
 * @param replicatedClusterTarget the number of replicated clusters we target whenever a split/merge or a reassign task
 *        is executed
 * @param replicationPriorityMin minimum threshold for the replication priority score
 * @param replicationDistanceRatioWeight weight of the border-proximity distance ratio term in the replication priority
 *        score
 * @param replicationZScoreWeight weight of the distance z-score term in the replication priority score
 * @param replicationStatsMinSampleSize minimum number of primary vectors in a cluster before its distance statistics
 *        (mean/standard deviation) are trusted for the replication priority z-score term
 * @param sampleVectorStatsProbability probability of sampling a vector write for statistics computation
 * @param maintainStatsProbability probability of maintaining statistics when inserting a vector
 * @param statsThreshold number of sampled vectors that triggers centroid computation
 * @param useRaBitQ indicator if we should use RaBitQ quantization
 * @param raBitQNumExBits number of extra bits per dimension for RaBitQ encoding
 * @param deterministicRandomness whether randomness should always be deterministic (for debugging/replay)
 * @param maxNumConcurrentNodeFetches maximum concurrent node fetches (passed through to HNSW config)
 * @param maxNumConcurrentNeighborhoodFetches maximum concurrent neighborhood fetches (passed through to HNSW config)
 * @param sampleBatchSize number of sampled vectors consumed per statistics-computation pass
 * @param searchConcurrency concurrency for parallel metadata fetches during search
 * @param insertMaxCandidateClusters maximum clusters evaluated as insertion targets
 * @param deleteMaxCandidateClusters maximum clusters probed when locating a vector's references during delete
 * @param deleteConcurrency concurrency for parallel operations during delete
 * @param splitNumNearestClusters number of nearest clusters fetched from HNSW for split candidate evaluation
 * @param mergeNumNearestClusters number of nearest clusters fetched from HNSW for merge candidate evaluation
 * @param kMeansMaxIterations maximum Lloyd's iterations per k-means restart
 * @param kMeansMaxRestarts maximum number of random restarts for bounded k-means during split/merge
 * @param reassignNumNeighboringClusters outer clusters considered as replication/migration targets during reassign
 * @param collapseMinDuplicates minimum identical vectors sharing a signature before collapse
 * @param splitMergeConcurrency concurrency for parallel operations during split/merge tasks
 * @param reassignConcurrency concurrency for parallel operations during reassign tasks
 */
@SuppressWarnings("checkstyle:MemberName")
public record Config(@Nonnull Metric metric,
                     int numDimensions,
                     int primaryClusterMin,
                     int primaryClusterMax,
                     int underreplicatedPrimaryClusterMax,
                     int replicatedClusterMaxWrites,
                     int replicatedClusterTarget,
                     double replicationPriorityMin,
                     double replicationDistanceRatioWeight,
                     double replicationZScoreWeight,
                     int replicationStatsMinSampleSize,
                     double sampleVectorStatsProbability,
                     double maintainStatsProbability,
                     int statsThreshold,
                     boolean useRaBitQ,
                     int raBitQNumExBits,
                     boolean deterministicRandomness,
                     int maxNumConcurrentNodeFetches,
                     int maxNumConcurrentNeighborhoodFetches,
                     int sampleBatchSize,
                     // search
                     int searchConcurrency,
                     // insert
                     int insertMaxCandidateClusters,
                     // delete
                     int deleteMaxCandidateClusters,
                     int deleteConcurrency,
                     // split/merge
                     int splitNumNearestClusters,
                     int mergeNumNearestClusters,
                     int kMeansMaxIterations,
                     int kMeansMaxRestarts,
                     // reassign
                     int reassignNumNeighboringClusters,
                     // collapse
                     int collapseMinDuplicates,
                     // per-task concurrency
                     int splitMergeConcurrency,
                     int reassignConcurrency) implements VectorEncodingConfig {

    @Nonnull public static final Metric DEFAULT_METRIC = Metric.EUCLIDEAN_METRIC;
    public static final int DEFAULT_PRIMARY_CLUSTER_MIN = 100;
    public static final int DEFAULT_PRIMARY_CLUSTER_MAX = 1000;
    public static final int DEFAULT_UNDERREPLICATED_PRIMARY_CLUSTER_MAX = 50;
    public static final int DEFAULT_REPLICATED_CLUSTER_MAX_WRITES = 3 * DEFAULT_PRIMARY_CLUSTER_MAX / 10;
    public static final int DEFAULT_REPLICATED_CLUSTER_TARGET = DEFAULT_PRIMARY_CLUSTER_MAX / 10;
    public static final double DEFAULT_REPLICATION_PRIORITY_MIN = 0.89d;
    public static final double DEFAULT_REPLICATION_DISTANCE_RATIO_WEIGHT = 1.0d;
    public static final double DEFAULT_REPLICATION_Z_SCORE_WEIGHT = 0.0d;
    public static final int DEFAULT_REPLICATION_STATS_MIN_SAMPLE_SIZE = 200;

    // stats
    public static final double DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY = 0.5d;
    public static final double DEFAULT_MAINTAIN_STATS_PROBABILITY = 0.05d;
    public static final int DEFAULT_STATS_THRESHOLD = 1000;
    // RaBitQ
    public static final boolean DEFAULT_USE_RABITQ = false;
    public static final int DEFAULT_RABITQ_NUM_EX_BITS = 4;
    // randomness
    public static final boolean DEFAULT_DETERMINISTIC_RANDOMNESS = false;
    // HNSW concurrency (passed through)
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES = 16;
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES = 10;
    // stats sampling
    public static final int DEFAULT_SAMPLE_BATCH_SIZE = 50;

    // search
    public static final int DEFAULT_SEARCH_CONCURRENCY = 10;
    // insert
    public static final int DEFAULT_INSERT_MAX_CANDIDATE_CLUSTERS = 10;
    // delete
    public static final int DEFAULT_DELETE_MAX_CANDIDATE_CLUSTERS = 10;
    public static final int DEFAULT_DELETE_CONCURRENCY = 10;
    // split/merge
    public static final int DEFAULT_SPLIT_NUM_NEAREST_CLUSTERS = 32;
    // covers the largest merge candidate (3-to-2 dissolves 3 core clusters) plus ~8 absorbing neighbors
    public static final int DEFAULT_MERGE_NUM_NEAREST_CLUSTERS = 11;
    public static final int DEFAULT_KMEANS_MAX_ITERATIONS = 8;
    public static final int DEFAULT_KMEANS_MAX_RESTARTS = 3;
    // reassign
    public static final int DEFAULT_REASSIGN_NUM_NEIGHBORING_CLUSTERS = 31;
    // collapse
    public static final int DEFAULT_COLLAPSE_MIN_DUPLICATES = 100;
    // per-task concurrency
    public static final int DEFAULT_SPLIT_MERGE_CONCURRENCY = 10;
    public static final int DEFAULT_REASSIGN_CONCURRENCY = 10;

    public Config {
        Preconditions.checkArgument(numDimensions >= 1, "numDimensions must be >= 1");
    }

    @Nonnull
    public ConfigBuilder toBuilder() {
        return new ConfigBuilder(metric(), primaryClusterMin(), primaryClusterMax(),
                underreplicatedPrimaryClusterMax(), replicatedClusterMaxWrites(), replicatedClusterTarget(),
                replicationPriorityMin(), replicationDistanceRatioWeight(), replicationZScoreWeight(),
                replicationStatsMinSampleSize(), sampleVectorStatsProbability(), maintainStatsProbability(),
                statsThreshold(), useRaBitQ(), raBitQNumExBits(), deterministicRandomness(),
                maxNumConcurrentNodeFetches(), maxNumConcurrentNeighborhoodFetches(),
                sampleBatchSize(), searchConcurrency(), insertMaxCandidateClusters(),
                deleteMaxCandidateClusters(), deleteConcurrency(),
                splitNumNearestClusters(), mergeNumNearestClusters(),
                kMeansMaxIterations(), kMeansMaxRestarts(),
                reassignNumNeighboringClusters(),
                collapseMinDuplicates(), splitMergeConcurrency(), reassignConcurrency());
    }

    @Override
    @Nonnull
    public String toString() {
        return "Config[metric=" + metric() + ", numDimensions=" + numDimensions() +
                ", primaryClusterMin=" + primaryClusterMin() + ", primaryClusterMax=" + primaryClusterMax() +
                ", underreplicatedPrimaryClusterMax=" + underreplicatedPrimaryClusterMax() +
                ", replicatedClusterMaxWrites=" + replicatedClusterMaxWrites() +
                ", replicatedClusterTarget=" + replicatedClusterTarget() +
                ", replicationPriorityMin=" + replicationPriorityMin() +
                ", replicationDistanceRatioWeight=" + replicationDistanceRatioWeight() +
                ", replicationZScoreWeight=" + replicationZScoreWeight() +
                ", replicationStatsMinSampleSize=" + replicationStatsMinSampleSize() +
                ", sampleVectorStatsProbability=" + sampleVectorStatsProbability() +
                ", maintainStatsProbability=" + maintainStatsProbability() + ", statsThreshold=" + statsThreshold() +
                ", useRaBitQ=" + useRaBitQ() + ", raBitQNumExBits=" + raBitQNumExBits() +
                ", deterministicRandomness=" + deterministicRandomness() +
                ", maxNumConcurrentNodeFetches=" + maxNumConcurrentNodeFetches() +
                ", maxNumConcurrentNeighborhoodFetches=" + maxNumConcurrentNeighborhoodFetches() +
                ", sampleBatchSize=" + sampleBatchSize() +
                ", searchConcurrency=" + searchConcurrency() +
                ", insertMaxCandidateClusters=" + insertMaxCandidateClusters() +
                ", deleteMaxCandidateClusters=" + deleteMaxCandidateClusters() +
                ", deleteConcurrency=" + deleteConcurrency() +
                ", splitNumNearestClusters=" + splitNumNearestClusters() +
                ", mergeNumNearestClusters=" + mergeNumNearestClusters() +
                ", kMeansMaxIterations=" + kMeansMaxIterations() +
                ", kMeansMaxRestarts=" + kMeansMaxRestarts() +
                ", reassignNumNeighboringClusters=" + reassignNumNeighboringClusters() +
                ", collapseMinDuplicates=" + collapseMinDuplicates() +
                ", splitMergeConcurrency=" + splitMergeConcurrency() +
                ", reassignConcurrency=" + reassignConcurrency() +
                "]";
    }

    /**
     * Builder for {@link Config}.
     *
     * @see Guardiann#newConfigBuilder
     */
    @CanIgnoreReturnValue
    @SuppressWarnings("checkstyle:MemberName")
    public static class ConfigBuilder {
        @Nonnull
        private Metric metric = DEFAULT_METRIC;
        private int primaryClusterMin = DEFAULT_PRIMARY_CLUSTER_MIN;
        private int primaryClusterMax = DEFAULT_PRIMARY_CLUSTER_MAX;
        private int underreplicatedPrimaryClusterMax = DEFAULT_UNDERREPLICATED_PRIMARY_CLUSTER_MAX;
        private int replicatedClusterMaxWrites = DEFAULT_REPLICATED_CLUSTER_MAX_WRITES;
        private int replicatedClusterTarget = DEFAULT_REPLICATED_CLUSTER_TARGET;
        private double replicationPriorityMin = DEFAULT_REPLICATION_PRIORITY_MIN;
        private double replicationDistanceRatioWeight = DEFAULT_REPLICATION_DISTANCE_RATIO_WEIGHT;
        private double replicationZScoreWeight = DEFAULT_REPLICATION_Z_SCORE_WEIGHT;
        private int replicationStatsMinSampleSize = DEFAULT_REPLICATION_STATS_MIN_SAMPLE_SIZE;

        private double sampleVectorStatsProbability = DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY;
        private double maintainStatsProbability = DEFAULT_MAINTAIN_STATS_PROBABILITY;
        private int statsThreshold = DEFAULT_STATS_THRESHOLD;

        private boolean useRaBitQ = DEFAULT_USE_RABITQ;
        private int raBitQNumExBits = DEFAULT_RABITQ_NUM_EX_BITS;

        private boolean deterministicRandomness = DEFAULT_DETERMINISTIC_RANDOMNESS;
        private int maxNumConcurrentNodeFetches = DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES;
        private int maxNumConcurrentNeighborhoodFetches = DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES;
        private int sampleBatchSize = DEFAULT_SAMPLE_BATCH_SIZE;

        // search
        private int searchConcurrency = DEFAULT_SEARCH_CONCURRENCY;
        // insert
        private int insertMaxCandidateClusters = DEFAULT_INSERT_MAX_CANDIDATE_CLUSTERS;
        // delete
        private int deleteMaxCandidateClusters = DEFAULT_DELETE_MAX_CANDIDATE_CLUSTERS;
        private int deleteConcurrency = DEFAULT_DELETE_CONCURRENCY;
        // split/merge
        private int splitNumNearestClusters = DEFAULT_SPLIT_NUM_NEAREST_CLUSTERS;
        private int mergeNumNearestClusters = DEFAULT_MERGE_NUM_NEAREST_CLUSTERS;
        private int kMeansMaxIterations = DEFAULT_KMEANS_MAX_ITERATIONS;
        private int kMeansMaxRestarts = DEFAULT_KMEANS_MAX_RESTARTS;
        // reassign
        private int reassignNumNeighboringClusters = DEFAULT_REASSIGN_NUM_NEIGHBORING_CLUSTERS;
        // collapse
        private int collapseMinDuplicates = DEFAULT_COLLAPSE_MIN_DUPLICATES;
        // per-task concurrency
        private int splitMergeConcurrency = DEFAULT_SPLIT_MERGE_CONCURRENCY;
        private int reassignConcurrency = DEFAULT_REASSIGN_CONCURRENCY;

        public ConfigBuilder() {
        }

        public ConfigBuilder(@Nonnull final Metric metric, final int primaryClusterMin, final int primaryClusterMax,
                             final int underreplicatedPrimaryClusterMax, final int replicatedClusterMaxWrites,
                             final int replicatedClusterTarget, final double replicationPriorityMin,
                             final double replicationDistanceRatioWeight, final double replicationZScoreWeight,
                             final int replicationStatsMinSampleSize,
                             final double sampleVectorStatsProbability, final double maintainStatsProbability,
                             final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits,
                             final boolean deterministicRandomness, final int maxNumConcurrentNodeFetches,
                             final int maxNumConcurrentNeighborhoodFetches,
                             final int sampleBatchSize,
                             final int searchConcurrency,
                             final int insertMaxCandidateClusters,
                             final int deleteMaxCandidateClusters, final int deleteConcurrency,
                             final int splitNumNearestClusters, final int mergeNumNearestClusters,
                             final int kMeansMaxIterations,
                             final int kMeansMaxRestarts,
                             final int reassignNumNeighboringClusters,
                             final int collapseMinDuplicates,
                             final int splitMergeConcurrency, final int reassignConcurrency) {
            this.metric = metric;
            this.primaryClusterMin = primaryClusterMin;
            this.primaryClusterMax = primaryClusterMax;
            this.underreplicatedPrimaryClusterMax = underreplicatedPrimaryClusterMax;
            this.replicatedClusterMaxWrites = replicatedClusterMaxWrites;
            this.replicatedClusterTarget = replicatedClusterTarget;
            this.replicationPriorityMin = replicationPriorityMin;
            this.replicationDistanceRatioWeight = replicationDistanceRatioWeight;
            this.replicationZScoreWeight = replicationZScoreWeight;
            this.replicationStatsMinSampleSize = replicationStatsMinSampleSize;
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            this.maintainStatsProbability = maintainStatsProbability;
            this.statsThreshold = statsThreshold;
            this.useRaBitQ = useRaBitQ;
            this.raBitQNumExBits = raBitQNumExBits;
            this.deterministicRandomness = deterministicRandomness;
            this.maxNumConcurrentNodeFetches = maxNumConcurrentNodeFetches;
            this.maxNumConcurrentNeighborhoodFetches = maxNumConcurrentNeighborhoodFetches;
            this.sampleBatchSize = sampleBatchSize;
            this.searchConcurrency = searchConcurrency;
            this.insertMaxCandidateClusters = insertMaxCandidateClusters;
            this.deleteMaxCandidateClusters = deleteMaxCandidateClusters;
            this.deleteConcurrency = deleteConcurrency;
            this.splitNumNearestClusters = splitNumNearestClusters;
            this.mergeNumNearestClusters = mergeNumNearestClusters;
            this.kMeansMaxIterations = kMeansMaxIterations;
            this.kMeansMaxRestarts = kMeansMaxRestarts;
            this.reassignNumNeighboringClusters = reassignNumNeighboringClusters;
            this.collapseMinDuplicates = collapseMinDuplicates;
            this.splitMergeConcurrency = splitMergeConcurrency;
            this.reassignConcurrency = reassignConcurrency;
        }

        @Nonnull
        public Metric getMetric() {
            return metric;
        }

        @Nonnull
        public ConfigBuilder setMetric(@Nonnull final Metric metric) {
            this.metric = metric;
            return this;
        }

        public int getPrimaryClusterMin() {
            return primaryClusterMin;
        }

        @Nonnull
        public ConfigBuilder setPrimaryClusterMin(final int primaryClusterMin) {
            this.primaryClusterMin = primaryClusterMin;
            return this;
        }

        public int getPrimaryClusterMax() {
            return primaryClusterMax;
        }

        @Nonnull
        public ConfigBuilder setPrimaryClusterMax(final int primaryClusterMax) {
            this.primaryClusterMax = primaryClusterMax;
            return this;
        }

        public int getUnderreplicatedPrimaryClusterMax() {
            return underreplicatedPrimaryClusterMax;
        }

        @Nonnull
        public ConfigBuilder setUnderreplicatedPrimaryClusterMax(final int underreplicatedPrimaryClusterMax) {
            this.underreplicatedPrimaryClusterMax = underreplicatedPrimaryClusterMax;
            return this;
        }

        public int getReplicatedClusterMaxWrites() {
            return replicatedClusterMaxWrites;
        }

        @Nonnull
        public ConfigBuilder setReplicatedClusterMaxWrites(final int replicatedClusterMaxWrites) {
            this.replicatedClusterMaxWrites = replicatedClusterMaxWrites;
            return this;
        }

        public int getReplicatedClusterTarget() {
            return replicatedClusterTarget;
        }

        @Nonnull
        public ConfigBuilder setReplicatedClusterTarget(final int replicatedClusterTarget) {
            this.replicatedClusterTarget = replicatedClusterTarget;
            return this;
        }

        public double getReplicationPriorityMin() {
            return replicationPriorityMin;
        }

        @Nonnull
        public ConfigBuilder setReplicationPriorityMin(final double replicationPriorityMin) {
            this.replicationPriorityMin = replicationPriorityMin;
            return this;
        }

        public double getReplicationDistanceRatioWeight() {
            return replicationDistanceRatioWeight;
        }

        @Nonnull
        public ConfigBuilder setReplicationDistanceRatioWeight(final double replicationDistanceRatioWeight) {
            this.replicationDistanceRatioWeight = replicationDistanceRatioWeight;
            return this;
        }

        public double getReplicationZScoreWeight() {
            return replicationZScoreWeight;
        }

        @Nonnull
        public ConfigBuilder setReplicationZScoreWeight(final double replicationZScoreWeight) {
            this.replicationZScoreWeight = replicationZScoreWeight;
            return this;
        }

        public int getReplicationStatsMinSampleSize() {
            return replicationStatsMinSampleSize;
        }

        @Nonnull
        public ConfigBuilder setReplicationStatsMinSampleSize(final int replicationStatsMinSampleSize) {
            this.replicationStatsMinSampleSize = replicationStatsMinSampleSize;
            return this;
        }

        public double getSampleVectorStatsProbability() {
            return sampleVectorStatsProbability;
        }

        @Nonnull
        public ConfigBuilder setSampleVectorStatsProbability(final double sampleVectorStatsProbability) {
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            return this;
        }

        public double getMaintainStatsProbability() {
            return maintainStatsProbability;
        }

        @Nonnull
        public ConfigBuilder setMaintainStatsProbability(final double maintainStatsProbability) {
            this.maintainStatsProbability = maintainStatsProbability;
            return this;
        }

        public int getStatsThreshold() {
            return statsThreshold;
        }

        @Nonnull
        public ConfigBuilder setStatsThreshold(final int statsThreshold) {
            this.statsThreshold = statsThreshold;
            return this;
        }

        public boolean isUseRaBitQ() {
            return useRaBitQ;
        }

        @Nonnull
        public ConfigBuilder setUseRaBitQ(final boolean useRaBitQ) {
            this.useRaBitQ = useRaBitQ;
            return this;
        }

        public int getRaBitQNumExBits() {
            return raBitQNumExBits;
        }

        @Nonnull
        public ConfigBuilder setRaBitQNumExBits(final int raBitQNumExBits) {
            this.raBitQNumExBits = raBitQNumExBits;
            return this;
        }

        public boolean isDeterministicRandomness() {
            return deterministicRandomness;
        }

        @Nonnull
        public ConfigBuilder setDeterministicRandomness(final boolean deterministicRandomness) {
            this.deterministicRandomness = deterministicRandomness;
            return this;
        }

        public int getMaxNumConcurrentNodeFetches() {
            return maxNumConcurrentNodeFetches;
        }

        public ConfigBuilder setMaxNumConcurrentNodeFetches(final int maxNumConcurrentNodeFetches) {
            this.maxNumConcurrentNodeFetches = maxNumConcurrentNodeFetches;
            return this;
        }

        public int getMaxNumConcurrentNeighborhoodFetches() {
            return maxNumConcurrentNeighborhoodFetches;
        }

        public ConfigBuilder setMaxNumConcurrentNeighborhoodFetches(final int maxNumConcurrentNeighborhoodFetches) {
            this.maxNumConcurrentNeighborhoodFetches = maxNumConcurrentNeighborhoodFetches;
            return this;
        }

        public int getSampleBatchSize() {
            return sampleBatchSize;
        }

        public ConfigBuilder setSampleBatchSize(final int sampleBatchSize) {
            this.sampleBatchSize = sampleBatchSize;
            return this;
        }

        public int getSearchConcurrency() {
            return searchConcurrency;
        }

        public ConfigBuilder setSearchConcurrency(final int searchConcurrency) {
            this.searchConcurrency = searchConcurrency;
            return this;
        }

        public int getInsertMaxCandidateClusters() {
            return insertMaxCandidateClusters;
        }

        public ConfigBuilder setInsertMaxCandidateClusters(final int insertMaxCandidateClusters) {
            this.insertMaxCandidateClusters = insertMaxCandidateClusters;
            return this;
        }

        public int getDeleteMaxCandidateClusters() {
            return deleteMaxCandidateClusters;
        }

        public ConfigBuilder setDeleteMaxCandidateClusters(final int deleteMaxCandidateClusters) {
            this.deleteMaxCandidateClusters = deleteMaxCandidateClusters;
            return this;
        }

        public int getDeleteConcurrency() {
            return deleteConcurrency;
        }

        public ConfigBuilder setDeleteConcurrency(final int deleteConcurrency) {
            this.deleteConcurrency = deleteConcurrency;
            return this;
        }

        public int getSplitNumNearestClusters() {
            return splitNumNearestClusters;
        }

        public ConfigBuilder setSplitNumNearestClusters(final int splitNumNearestClusters) {
            this.splitNumNearestClusters = splitNumNearestClusters;
            return this;
        }

        public int getMergeNumNearestClusters() {
            return mergeNumNearestClusters;
        }

        public ConfigBuilder setMergeNumNearestClusters(final int mergeNumNearestClusters) {
            this.mergeNumNearestClusters = mergeNumNearestClusters;
            return this;
        }

        public int getKMeansMaxIterations() {
            return kMeansMaxIterations;
        }

        public ConfigBuilder setKMeansMaxIterations(final int kMeansMaxIterations) {
            this.kMeansMaxIterations = kMeansMaxIterations;
            return this;
        }

        public int getKMeansMaxRestarts() {
            return kMeansMaxRestarts;
        }

        public ConfigBuilder setKMeansMaxRestarts(final int kMeansMaxRestarts) {
            this.kMeansMaxRestarts = kMeansMaxRestarts;
            return this;
        }

        public int getReassignNumNeighboringClusters() {
            return reassignNumNeighboringClusters;
        }

        public ConfigBuilder setReassignNumNeighboringClusters(final int reassignNumNeighboringClusters) {
            this.reassignNumNeighboringClusters = reassignNumNeighboringClusters;
            return this;
        }

        public int getCollapseMinDuplicates() {
            return collapseMinDuplicates;
        }

        public ConfigBuilder setCollapseMinDuplicates(final int collapseMinDuplicates) {
            this.collapseMinDuplicates = collapseMinDuplicates;
            return this;
        }

        public int getSplitMergeConcurrency() {
            return splitMergeConcurrency;
        }

        public ConfigBuilder setSplitMergeConcurrency(final int splitMergeConcurrency) {
            this.splitMergeConcurrency = splitMergeConcurrency;
            return this;
        }

        public int getReassignConcurrency() {
            return reassignConcurrency;
        }

        public ConfigBuilder setReassignConcurrency(final int reassignConcurrency) {
            this.reassignConcurrency = reassignConcurrency;
            return this;
        }

        public Config build(final int numDimensions) {
            return new Config(getMetric(), numDimensions, getPrimaryClusterMin(), getPrimaryClusterMax(),
                    getUnderreplicatedPrimaryClusterMax(), getReplicatedClusterMaxWrites(),
                    getReplicatedClusterTarget(), getReplicationPriorityMin(),
                    getReplicationDistanceRatioWeight(), getReplicationZScoreWeight(),
                    getReplicationStatsMinSampleSize(), getSampleVectorStatsProbability(),
                    getMaintainStatsProbability(), getStatsThreshold(), isUseRaBitQ(), getRaBitQNumExBits(),
                    isDeterministicRandomness(), getMaxNumConcurrentNodeFetches(),
                    getMaxNumConcurrentNeighborhoodFetches(),
                    getSampleBatchSize(), getSearchConcurrency(), getInsertMaxCandidateClusters(),
                    getDeleteMaxCandidateClusters(), getDeleteConcurrency(),
                    getSplitNumNearestClusters(), getMergeNumNearestClusters(),
                    getKMeansMaxIterations(), getKMeansMaxRestarts(),
                    getReassignNumNeighboringClusters(),
                    getCollapseMinDuplicates(), getSplitMergeConcurrency(), getReassignConcurrency());
        }
    }
}
