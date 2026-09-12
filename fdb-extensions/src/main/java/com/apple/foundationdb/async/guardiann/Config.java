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
 * @param primaryClusterHardMax hard cap on the number of primary vectors in a cluster, set above
 *        {@code primaryClusterMax}: on insert, reaching it back-pressures the caller (an exception is thrown) when
 *        deferred maintenance tasks are not run in the writing transaction, so the caller slows down while the
 *        background merge drains the split backlog rather than letting an un-drained cluster grow without bound
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
 * @param sampleBatchSize number of sampled vectors consumed per statistics-computation pass
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
 * @param collapseConcurrency concurrency for parallel operations during collapse tasks
 * @param bounceConcurrency concurrency for parallel operations during bounce tasks
 * @param constructionSearchConfig centroid-walk tuning ({@link SearchConfig}) for the non-search insert/delete/maintenance
 *        paths, which probe the centroid HNSW without a per-query {@code SearchConfig}; only its {@code centroidEf*}
 *        knobs are consulted there
 * @param mergeMaxEverFraction fraction of a cluster's max-ever primary count below which it becomes merge-eligible,
 *        floored by {@link #primaryClusterMin()}; see {@link ClusterMetadata#mergeThreshold(Config)}, which combines
 *        the two
 * @param minChildFraction floor on the smallest child's share of the population being repartitioned, below which a
 *        split or merge candidate is rejected outright. Guards against producing a cluster born small enough to be
 *        immediately merge-eligible, and is the <em>only</em> gate that rejects rather than merely disfavours, so
 *        raising it is what risks leaving a split with no admissible candidate. A plain fraction rather than one
 *        scaled by {@code k}, since it stands in for an absolute vector count; note that the population being
 *        repartitioned is itself larger for a wider split (which draws in a neighbouring cluster), so the same
 *        fraction is a somewhat stricter absolute floor there
 * @param maxRelativeImbalance ceiling on how uneven a candidate's cluster sizes may be, expressed as a fraction of
 *        the worst imbalance achievable for that number of clusters, so one value means the same thing for a 2-way
 *        and a 3-way partitioning. {@code 1} disables the check; a merge to a single cluster is perfectly balanced
 *        by definition and always passes
 * @param splitImbalancePenalty weight of the imbalance term when scoring a <em>split</em>, which biases the choice
 *        between otherwise comparable candidates toward the more balanced one. Merges keep the evaluator's default
 *        weight, since their shape is dictated by the clusters they are handed
 * @param clusterMetadataMaxPendingDeltas how many appended, not-yet-folded deltas a cluster's metadata value may
 *        accumulate before a writer compacts it instead of appending again; effectively "compact once every N
 *        metadata updates". Larger values mean fewer conflicting compactions but more work folding on every read
 */
@SuppressWarnings("checkstyle:MemberName")
public record Config(@Nonnull Metric metric,
                     int numDimensions,
                     int primaryClusterMin,
                     int primaryClusterMax,
                     int primaryClusterHardMax,
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
                     int sampleBatchSize,
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
                     int reassignConcurrency,
                     int collapseConcurrency,
                     int bounceConcurrency,
                     // construction (centroid-walk tuning for the non-search insert/delete/maintenance paths)
                     @Nonnull SearchConfig constructionSearchConfig,
                     // merge trigger (hysteresis relative to a cluster's max-ever primary count)
                     double mergeMaxEverFraction,
                     double minChildFraction,
                     double maxRelativeImbalance,
                     double splitImbalancePenalty,
                     // cluster-metadata delta log
                     int clusterMetadataMaxPendingDeltas) implements VectorEncodingConfig {

    @Nonnull public static final Metric DEFAULT_METRIC = Metric.EUCLIDEAN_METRIC;
    public static final int DEFAULT_PRIMARY_CLUSTER_MAX = 1000;
    // A tenth of primaryClusterMax. The ratio matters more than the value: set the floor much lower and clusters
    // whose lifetime peak is modest end up with a merge threshold pinned to the floor, so they hover just above it
    // and never consolidate — a population of small clusters that accumulate references to deleted vectors. Set it
    // much higher and it swallows the max-ever fraction, since that only binds once 0.2 * maxEver exceeds the floor.
    public static final int DEFAULT_PRIMARY_CLUSTER_MIN = DEFAULT_PRIMARY_CLUSTER_MAX / 10;
    public static final int DEFAULT_PRIMARY_CLUSTER_HARD_MAX = 2 * DEFAULT_PRIMARY_CLUSTER_MAX;
    // fraction of a cluster's max-ever primary count below which a merge is triggered (subject to the
    // primaryClusterMin floor); see ClusterMetadata#mergeThreshold
    public static final double DEFAULT_MERGE_MAX_EVER_FRACTION = 1.0d / 5.0d;
    // Floor on the smallest child of a repartitioning, as a fraction of the population being split. A child born
    // below primaryClusterMin is immediately merge-eligible, and a split fires no earlier than primaryClusterMax
    // vectors, so that ratio is the tightest case. Kept as an expression of the two so it tracks them. This is the
    // only gate that can reject a candidate outright; lopsidedness is steered softly by maxRelativeImbalance and
    // splitImbalancePenalty instead, neither of which can leave the evaluator with nothing to choose from.
    public static final double DEFAULT_MIN_CHILD_FRACTION =
            DEFAULT_PRIMARY_CLUSTER_MIN / (double)DEFAULT_PRIMARY_CLUSTER_MAX;
    // Ceiling on relative imbalance, chosen so a 2-way split may put at most ~80% of the population in one child,
    // keeping it clear of primaryClusterMax rather than pinned against it. Tightens automatically as k grows.
    public static final double DEFAULT_MAX_RELATIVE_IMBALANCE = 0.36d;
    // Weight of the imbalance term when scoring a split; above the evaluator's default of 1.0 so that, between
    // otherwise comparable candidates, the more balanced one wins.
    public static final double DEFAULT_SPLIT_IMBALANCE_PENALTY = 3.0d;
    // Deltas appended to a cluster's metadata value before a writer folds them back into the base value. See
    // ClusterMetadataDelta; the cap keeps base + MAX_PENDING x deltaSize far below FDB's value-size limit.
    public static final int DEFAULT_CLUSTER_METADATA_MAX_PENDING_DELTAS = 64;
    public static final int MAX_CLUSTER_METADATA_MAX_PENDING_DELTAS = 4096;
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
    // stats sampling
    public static final int DEFAULT_SAMPLE_BATCH_SIZE = 50;

    // insert
    public static final int DEFAULT_INSERT_MAX_CANDIDATE_CLUSTERS = 10;
    // delete
    public static final int DEFAULT_DELETE_MAX_CANDIDATE_CLUSTERS = 10;
    public static final int DEFAULT_DELETE_CONCURRENCY = 10;
    // split/merge
    public static final int DEFAULT_SPLIT_NUM_NEAREST_CLUSTERS = 32;
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
    public static final int DEFAULT_COLLAPSE_CONCURRENCY = 10;
    public static final int DEFAULT_BOUNCE_CONCURRENCY = 10;
    @Nonnull
    public static final SearchConfig DEFAULT_CONSTRUCTION_SEARCH_CONFIG = new SearchConfig.SearchConfigBuilder().build();

    public Config {
        Preconditions.checkArgument(numDimensions >= 1, "numDimensions must be >= 1");
        Preconditions.checkArgument(collapseMinDuplicates < primaryClusterMax,
                "collapseMinDuplicates must be < primaryClusterMax");
        Preconditions.checkArgument(primaryClusterHardMax > primaryClusterMax,
                "primaryClusterHardMax must be > primaryClusterMax");
        Preconditions.checkArgument(mergeMaxEverFraction > 0.0d && mergeMaxEverFraction < 1.0d,
                "mergeMaxEverFraction must be in (0, 1)");
        // Anything at or above 1/k makes every k-way candidate unsatisfiable; 0.5 is the loosest value that still
        // admits a two-way split, and the caller is responsible for staying below 1/k for the widest split it wants.
        Preconditions.checkArgument(minChildFraction >= 0.0d && minChildFraction < 0.5d,
                "minChildFraction must be in [0, 0.5)");
        Preconditions.checkArgument(maxRelativeImbalance >= 0.0d && maxRelativeImbalance <= 1.0d,
                "maxRelativeImbalance must be in [0, 1]");
        Preconditions.checkArgument(splitImbalancePenalty >= 0.0d,
                "splitImbalancePenalty must be >= 0");
        Preconditions.checkArgument(clusterMetadataMaxPendingDeltas >= 1
                        && clusterMetadataMaxPendingDeltas <= MAX_CLUSTER_METADATA_MAX_PENDING_DELTAS,
                "clusterMetadataMaxPendingDeltas must be in [1, %s]", MAX_CLUSTER_METADATA_MAX_PENDING_DELTAS);
    }

    @Nonnull
    public ConfigBuilder toBuilder() {
        return new ConfigBuilder(metric(), primaryClusterMin(), primaryClusterMax(), primaryClusterHardMax(),
                underreplicatedPrimaryClusterMax(), replicatedClusterMaxWrites(), replicatedClusterTarget(),
                replicationPriorityMin(), replicationDistanceRatioWeight(), replicationZScoreWeight(),
                replicationStatsMinSampleSize(), sampleVectorStatsProbability(), maintainStatsProbability(),
                statsThreshold(), useRaBitQ(), raBitQNumExBits(), deterministicRandomness(),
                sampleBatchSize(), insertMaxCandidateClusters(),
                deleteMaxCandidateClusters(), deleteConcurrency(),
                splitNumNearestClusters(), mergeNumNearestClusters(),
                kMeansMaxIterations(), kMeansMaxRestarts(),
                reassignNumNeighboringClusters(),
                collapseMinDuplicates(), splitMergeConcurrency(), reassignConcurrency(),
                collapseConcurrency(), bounceConcurrency(),
                constructionSearchConfig(), mergeMaxEverFraction(), minChildFraction(), maxRelativeImbalance(),
                splitImbalancePenalty(), clusterMetadataMaxPendingDeltas());
    }

    @Override
    @Nonnull
    public String toString() {
        return "Config[metric=" + metric() + ", numDimensions=" + numDimensions() +
                ", primaryClusterMin=" + primaryClusterMin() + ", primaryClusterMax=" + primaryClusterMax() +
                ", primaryClusterHardMax=" + primaryClusterHardMax() +
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
                ", sampleBatchSize=" + sampleBatchSize() +
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
                ", collapseConcurrency=" + collapseConcurrency() +
                ", bounceConcurrency=" + bounceConcurrency() +
                ", constructionSearchConfig=" + constructionSearchConfig() +
                ", mergeMaxEverFraction=" + mergeMaxEverFraction() +
                ", minChildFraction=" + minChildFraction() +
                ", maxRelativeImbalance=" + maxRelativeImbalance() +
                ", splitImbalancePenalty=" + splitImbalancePenalty() +
                ", clusterMetadataMaxPendingDeltas=" + clusterMetadataMaxPendingDeltas() +
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
        private int primaryClusterHardMax = DEFAULT_PRIMARY_CLUSTER_HARD_MAX;
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
        private int sampleBatchSize = DEFAULT_SAMPLE_BATCH_SIZE;

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
        private int collapseConcurrency = DEFAULT_COLLAPSE_CONCURRENCY;
        private int bounceConcurrency = DEFAULT_BOUNCE_CONCURRENCY;
        // construction (centroid-walk tuning for the non-search insert/delete/maintenance paths)
        @Nonnull
        private SearchConfig constructionSearchConfig = DEFAULT_CONSTRUCTION_SEARCH_CONFIG;
        // merge trigger
        private double mergeMaxEverFraction = DEFAULT_MERGE_MAX_EVER_FRACTION;
        // repartitioning balance gates
        private double minChildFraction = DEFAULT_MIN_CHILD_FRACTION;
        private double maxRelativeImbalance = DEFAULT_MAX_RELATIVE_IMBALANCE;
        private double splitImbalancePenalty = DEFAULT_SPLIT_IMBALANCE_PENALTY;
        // cluster-metadata delta log
        private int clusterMetadataMaxPendingDeltas = DEFAULT_CLUSTER_METADATA_MAX_PENDING_DELTAS;

        public ConfigBuilder() {
        }

        public ConfigBuilder(@Nonnull final Metric metric, final int primaryClusterMin, final int primaryClusterMax,
                             final int primaryClusterHardMax,
                             final int underreplicatedPrimaryClusterMax, final int replicatedClusterMaxWrites,
                             final int replicatedClusterTarget, final double replicationPriorityMin,
                             final double replicationDistanceRatioWeight, final double replicationZScoreWeight,
                             final int replicationStatsMinSampleSize,
                             final double sampleVectorStatsProbability, final double maintainStatsProbability,
                             final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits,
                             final boolean deterministicRandomness,
                             final int sampleBatchSize,
                             final int insertMaxCandidateClusters,
                             final int deleteMaxCandidateClusters, final int deleteConcurrency,
                             final int splitNumNearestClusters, final int mergeNumNearestClusters,
                             final int kMeansMaxIterations,
                             final int kMeansMaxRestarts,
                             final int reassignNumNeighboringClusters,
                             final int collapseMinDuplicates,
                             final int splitMergeConcurrency, final int reassignConcurrency,
                             final int collapseConcurrency,
                             final int bounceConcurrency,
                             @Nonnull final SearchConfig constructionSearchConfig,
                             final double mergeMaxEverFraction,
                             final double minChildFraction,
                             final double maxRelativeImbalance,
                             final double splitImbalancePenalty,
                             final int clusterMetadataMaxPendingDeltas) {
            this.metric = metric;
            this.primaryClusterMin = primaryClusterMin;
            this.primaryClusterMax = primaryClusterMax;
            this.primaryClusterHardMax = primaryClusterHardMax;
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
            this.sampleBatchSize = sampleBatchSize;
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
            this.collapseConcurrency = collapseConcurrency;
            this.bounceConcurrency = bounceConcurrency;
            this.constructionSearchConfig = constructionSearchConfig;
            this.mergeMaxEverFraction = mergeMaxEverFraction;
            this.minChildFraction = minChildFraction;
            this.maxRelativeImbalance = maxRelativeImbalance;
            this.splitImbalancePenalty = splitImbalancePenalty;
            this.clusterMetadataMaxPendingDeltas = clusterMetadataMaxPendingDeltas;
        }

        @Nonnull
        public Metric getMetric() {
            return metric;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setMetric(@Nonnull final Metric metric) {
            this.metric = metric;
            return this;
        }

        public int getPrimaryClusterMin() {
            return primaryClusterMin;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setPrimaryClusterMin(final int primaryClusterMin) {
            this.primaryClusterMin = primaryClusterMin;
            return this;
        }

        public double getMergeMaxEverFraction() {
            return mergeMaxEverFraction;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setMergeMaxEverFraction(final double mergeMaxEverFraction) {
            this.mergeMaxEverFraction = mergeMaxEverFraction;
            return this;
        }

        public double getMinChildFraction() {
            return minChildFraction;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setMinChildFraction(final double minChildFraction) {
            this.minChildFraction = minChildFraction;
            return this;
        }

        public double getMaxRelativeImbalance() {
            return maxRelativeImbalance;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setMaxRelativeImbalance(final double maxRelativeImbalance) {
            this.maxRelativeImbalance = maxRelativeImbalance;
            return this;
        }

        public double getSplitImbalancePenalty() {
            return splitImbalancePenalty;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setSplitImbalancePenalty(final double splitImbalancePenalty) {
            this.splitImbalancePenalty = splitImbalancePenalty;
            return this;
        }

        public int getClusterMetadataMaxPendingDeltas() {
            return clusterMetadataMaxPendingDeltas;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setClusterMetadataMaxPendingDeltas(final int clusterMetadataMaxPendingDeltas) {
            this.clusterMetadataMaxPendingDeltas = clusterMetadataMaxPendingDeltas;
            return this;
        }

        public int getPrimaryClusterMax() {
            return primaryClusterMax;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setPrimaryClusterMax(final int primaryClusterMax) {
            this.primaryClusterMax = primaryClusterMax;
            return this;
        }

        public int getPrimaryClusterHardMax() {
            return primaryClusterHardMax;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setPrimaryClusterHardMax(final int primaryClusterHardMax) {
            this.primaryClusterHardMax = primaryClusterHardMax;
            return this;
        }

        public int getUnderreplicatedPrimaryClusterMax() {
            return underreplicatedPrimaryClusterMax;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setUnderreplicatedPrimaryClusterMax(final int underreplicatedPrimaryClusterMax) {
            this.underreplicatedPrimaryClusterMax = underreplicatedPrimaryClusterMax;
            return this;
        }

        public int getReplicatedClusterMaxWrites() {
            return replicatedClusterMaxWrites;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReplicatedClusterMaxWrites(final int replicatedClusterMaxWrites) {
            this.replicatedClusterMaxWrites = replicatedClusterMaxWrites;
            return this;
        }

        public int getReplicatedClusterTarget() {
            return replicatedClusterTarget;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReplicatedClusterTarget(final int replicatedClusterTarget) {
            this.replicatedClusterTarget = replicatedClusterTarget;
            return this;
        }

        public double getReplicationPriorityMin() {
            return replicationPriorityMin;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReplicationPriorityMin(final double replicationPriorityMin) {
            this.replicationPriorityMin = replicationPriorityMin;
            return this;
        }

        public double getReplicationDistanceRatioWeight() {
            return replicationDistanceRatioWeight;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReplicationDistanceRatioWeight(final double replicationDistanceRatioWeight) {
            this.replicationDistanceRatioWeight = replicationDistanceRatioWeight;
            return this;
        }

        public double getReplicationZScoreWeight() {
            return replicationZScoreWeight;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReplicationZScoreWeight(final double replicationZScoreWeight) {
            this.replicationZScoreWeight = replicationZScoreWeight;
            return this;
        }

        public int getReplicationStatsMinSampleSize() {
            return replicationStatsMinSampleSize;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReplicationStatsMinSampleSize(final int replicationStatsMinSampleSize) {
            this.replicationStatsMinSampleSize = replicationStatsMinSampleSize;
            return this;
        }

        public double getSampleVectorStatsProbability() {
            return sampleVectorStatsProbability;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setSampleVectorStatsProbability(final double sampleVectorStatsProbability) {
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            return this;
        }

        public double getMaintainStatsProbability() {
            return maintainStatsProbability;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setMaintainStatsProbability(final double maintainStatsProbability) {
            this.maintainStatsProbability = maintainStatsProbability;
            return this;
        }

        public int getStatsThreshold() {
            return statsThreshold;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setStatsThreshold(final int statsThreshold) {
            this.statsThreshold = statsThreshold;
            return this;
        }

        public boolean isUseRaBitQ() {
            return useRaBitQ;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setUseRaBitQ(final boolean useRaBitQ) {
            this.useRaBitQ = useRaBitQ;
            return this;
        }

        public int getRaBitQNumExBits() {
            return raBitQNumExBits;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setRaBitQNumExBits(final int raBitQNumExBits) {
            this.raBitQNumExBits = raBitQNumExBits;
            return this;
        }

        public boolean isDeterministicRandomness() {
            return deterministicRandomness;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setDeterministicRandomness(final boolean deterministicRandomness) {
            this.deterministicRandomness = deterministicRandomness;
            return this;
        }

        public int getSampleBatchSize() {
            return sampleBatchSize;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setSampleBatchSize(final int sampleBatchSize) {
            this.sampleBatchSize = sampleBatchSize;
            return this;
        }

        public int getInsertMaxCandidateClusters() {
            return insertMaxCandidateClusters;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setInsertMaxCandidateClusters(final int insertMaxCandidateClusters) {
            this.insertMaxCandidateClusters = insertMaxCandidateClusters;
            return this;
        }

        public int getDeleteMaxCandidateClusters() {
            return deleteMaxCandidateClusters;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setDeleteMaxCandidateClusters(final int deleteMaxCandidateClusters) {
            this.deleteMaxCandidateClusters = deleteMaxCandidateClusters;
            return this;
        }

        public int getDeleteConcurrency() {
            return deleteConcurrency;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setDeleteConcurrency(final int deleteConcurrency) {
            this.deleteConcurrency = deleteConcurrency;
            return this;
        }

        public int getSplitNumNearestClusters() {
            return splitNumNearestClusters;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setSplitNumNearestClusters(final int splitNumNearestClusters) {
            this.splitNumNearestClusters = splitNumNearestClusters;
            return this;
        }

        public int getMergeNumNearestClusters() {
            return mergeNumNearestClusters;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setMergeNumNearestClusters(final int mergeNumNearestClusters) {
            this.mergeNumNearestClusters = mergeNumNearestClusters;
            return this;
        }

        public int getKMeansMaxIterations() {
            return kMeansMaxIterations;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setKMeansMaxIterations(final int kMeansMaxIterations) {
            this.kMeansMaxIterations = kMeansMaxIterations;
            return this;
        }

        public int getKMeansMaxRestarts() {
            return kMeansMaxRestarts;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setKMeansMaxRestarts(final int kMeansMaxRestarts) {
            this.kMeansMaxRestarts = kMeansMaxRestarts;
            return this;
        }

        public int getReassignNumNeighboringClusters() {
            return reassignNumNeighboringClusters;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReassignNumNeighboringClusters(final int reassignNumNeighboringClusters) {
            this.reassignNumNeighboringClusters = reassignNumNeighboringClusters;
            return this;
        }

        public int getCollapseMinDuplicates() {
            return collapseMinDuplicates;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setCollapseMinDuplicates(final int collapseMinDuplicates) {
            this.collapseMinDuplicates = collapseMinDuplicates;
            return this;
        }

        public int getSplitMergeConcurrency() {
            return splitMergeConcurrency;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setSplitMergeConcurrency(final int splitMergeConcurrency) {
            this.splitMergeConcurrency = splitMergeConcurrency;
            return this;
        }

        public int getReassignConcurrency() {
            return reassignConcurrency;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setReassignConcurrency(final int reassignConcurrency) {
            this.reassignConcurrency = reassignConcurrency;
            return this;
        }

        public int getCollapseConcurrency() {
            return collapseConcurrency;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setCollapseConcurrency(final int collapseConcurrency) {
            this.collapseConcurrency = collapseConcurrency;
            return this;
        }

        public int getBounceConcurrency() {
            return bounceConcurrency;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setBounceConcurrency(final int bounceConcurrency) {
            this.bounceConcurrency = bounceConcurrency;
            return this;
        }

        @Nonnull
        public SearchConfig getConstructionSearchConfig() {
            return constructionSearchConfig;
        }

        @CanIgnoreReturnValue
        @Nonnull
        public ConfigBuilder setConstructionSearchConfig(@Nonnull final SearchConfig constructionSearchConfig) {
            this.constructionSearchConfig = constructionSearchConfig;
            return this;
        }

        public Config build(final int numDimensions) {
            return new Config(getMetric(), numDimensions, getPrimaryClusterMin(), getPrimaryClusterMax(),
                    getPrimaryClusterHardMax(),
                    getUnderreplicatedPrimaryClusterMax(), getReplicatedClusterMaxWrites(),
                    getReplicatedClusterTarget(), getReplicationPriorityMin(),
                    getReplicationDistanceRatioWeight(), getReplicationZScoreWeight(),
                    getReplicationStatsMinSampleSize(), getSampleVectorStatsProbability(),
                    getMaintainStatsProbability(), getStatsThreshold(), isUseRaBitQ(), getRaBitQNumExBits(),
                    isDeterministicRandomness(),
                    getSampleBatchSize(), getInsertMaxCandidateClusters(),
                    getDeleteMaxCandidateClusters(), getDeleteConcurrency(),
                    getSplitNumNearestClusters(), getMergeNumNearestClusters(),
                    getKMeansMaxIterations(), getKMeansMaxRestarts(),
                    getReassignNumNeighboringClusters(),
                    getCollapseMinDuplicates(), getSplitMergeConcurrency(), getReassignConcurrency(),
                    getCollapseConcurrency(), getBounceConcurrency(),
                    getConstructionSearchConfig(), getMergeMaxEverFraction(), getMinChildFraction(),
                    getMaxRelativeImbalance(), getSplitImbalancePenalty(),
                    getClusterMetadataMaxPendingDeltas());
        }
    }
}
