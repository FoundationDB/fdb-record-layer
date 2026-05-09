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

import com.apple.foundationdb.async.common.BaseConfig;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.Metric;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * TODO.
 * @param metric the metric in use for this guardiann structure
 * @param numDimensions the number of dimensions of the vectors stored
 * @param primaryClusterMin minimum number of primary vectors in a cluster, underflow will result in a merge task to be
 *        enqueued
 * @param primaryClusterMax maximum number of primary vectors in a cluster, overflow will result in a split task to be
 *        enqueued
 * @param underreplicatedPrimaryClusterMax maximum number of under-replicated primary vectors in a cluster, overflow
 *        will result in a reassign task to be enqueued
 * @param replicatedClusterMaxWrites maximum number of writes of replicated vectors to a cluster. Since we might write
 *        duplicated replicated vectors, this threshold is not measured in actual the number of actual replicated
 *        vectors in a cluster but the number of writes of replicated clusters to that cluster.
 * @param replicatedClusterTarget the number of replicated clusters we target whenever a split/merge or a reassign task
 *        is executed. We keep the {@code replicatedClusterTarget} number of vectors with the highest
 *        {@code replication priorities} in that cluster.
 * @param replicationPriorityMin Minimum threshold for the replication priority. The replication priority is a
 *        score-like property that can be computed for each primary vector in a cluster in conjunction with a
 *        candidate neighboring cluster that we are trying to decide whether we should replicate this vector to or not.
 *        The higher the computed replication priority, the more necessary it becomes to keep a vector in the set of
 *        replicated vectors of the neighboring cluster In order to curb the amounts of writes caused by that
 *        replication push, we will not even attempt to write a vector into a neighboring cluster if the replication
 *        priority is less than {@code replicationPriorityMin}.
 * @param sampleVectorStatsProbability If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}
 *        but the metric is not {@link Metric#COSINE_METRIC}), this probability determines the chance that we sample
 *        a write of a new vector into {@link StorageAdapter#getSamplesSubspace()}.
 * @param maintainStatsProbability If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}
 *        but the metric is not {@link Metric#COSINE_METRIC}), this probability determines the chance that we maintain
 *        {@link StorageAdapter#getSamplesSubspace()} when inserting a vector.
 * @param statsThreshold If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true} but the metric
 *        is not {@link Metric#COSINE_METRIC}), this attribute represents the threshold (being a number of vectors) that
 *        when reached causes the stats maintenance logic to compute the actual statistics (currently the centroid of
 *        the vectors that have been inserted to far).
 * @param useRaBitQ Indicator if we should RaBitQ quantization. See {@link com.apple.foundationdb.rabitq.RaBitQuantizer}
 *        for more details.
 * @param raBitQNumExBits Number of bits per dimensions iff {@link #isUseRaBitQ()} is set to {@code true}, ignored
 *        otherwise. If RaBitQ encoding is used, a vector is stored using roughly
 *        {@code 25 + numDimensions * (numExBits + 1) / 8} bytes.
 * @param deterministicRandomness an indicator whether randomness should always be deterministic which is useful for
 *        debugging and to replay/recreate problematic situations. If {@code deterministicRandomness} is {@code true},
 *        cluster ids are assigned starting from {@code 0} monotonically increasing. Task ids are pseudo-randomly
 *        chosen.
 * @param maxNumConcurrentNodeFetches maximum number of concurrent node fetches during search and modification
 *        operations
 * @param maxNumConcurrentNeighborhoodFetches maximum number of concurrent neighborhood fetches during modification
 *        operations when the neighbors are pruned
 *
 *
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
                     double sampleVectorStatsProbability,
                     double maintainStatsProbability,
                     int statsThreshold,
                     boolean useRaBitQ,
                     int raBitQNumExBits,
                     boolean deterministicRandomness,
                     int maxNumConcurrentNodeFetches,
                     int maxNumConcurrentNeighborhoodFetches) implements BaseConfig {

    @Nonnull public static final Metric DEFAULT_METRIC = Metric.EUCLIDEAN_METRIC;
    public static final int DEFAULT_PRIMARY_CLUSTER_MIN = 100;
    public static final int DEFAULT_PRIMARY_CLUSTER_MAX = 1000;
    public static final int DEFAULT_UNDERREPLICATED_PRIMARY_CLUSTER_MAX = 50;
    public static final int DEFAULT_REPLICATED_CLUSTER_MAX_WRITES = 3 * DEFAULT_PRIMARY_CLUSTER_MAX / 10; // 30% of primary max
    public static final int DEFAULT_REPLICATED_CLUSTER_TARGET = DEFAULT_PRIMARY_CLUSTER_MAX / 10; // 10% of primary max
    public static final double DEFAULT_REPLICATION_PRIORITY_MIN = 0.89d;

    // stats
    public static final double DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY = 0.5d;
    public static final double DEFAULT_MAINTAIN_STATS_PROBABILITY = 0.05d;
    public static final int DEFAULT_STATS_THRESHOLD = 1000;
    // RaBitQ
    public static final boolean DEFAULT_USE_RABITQ = false;
    public static final int DEFAULT_RABITQ_NUM_EX_BITS = 4;
    // randomness
    public static final boolean DEFAULT_DETERMINISTIC_RANDOMNESS = false;
    // concurrency
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES = 16;
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES = 10;

    public Config {
        Preconditions.checkArgument(numDimensions >= 1, "numDimensions must be (1, MAX_INT]");
    }

    /**
     * The metric that is used to determine distances between vectors.
     */
    @Nonnull
    @Override
    public Metric getMetric() {
        return metric;
    }

    /**
     * The number of dimensions used. All vectors must have exactly this number of dimensions.
     */
    @Override
    public int getNumDimensions() {
        return numDimensions;
    }

    /**
     * Indicator if we should RaBitQ quantization. See {@link com.apple.foundationdb.rabitq.RaBitQuantizer} for more
     * details.
     */
    @Override
    public boolean isUseRaBitQ() {
        return useRaBitQ;
    }

    /**
     * Number of bits per dimensions iff {@link #isUseRaBitQ()} is set to {@code true}, ignored otherwise. If RaBitQ
     * encoding is used, a vector is stored using roughly {@code 25 + numDimensions * (numExBits + 1) / 8} bytes.
     */
    @Override
    public int getRaBitQNumExBits() {
        return raBitQNumExBits;
    }

    @Nonnull
    public ConfigBuilder toBuilder() {
        return new ConfigBuilder(getMetric(), primaryClusterMin(), primaryClusterMax(),
                underreplicatedPrimaryClusterMax(), replicatedClusterMaxWrites(), replicatedClusterTarget(),
                replicationPriorityMin(), sampleVectorStatsProbability(), maintainStatsProbability(),
                statsThreshold(), isUseRaBitQ(), getRaBitQNumExBits(), deterministicRandomness(),
                maxNumConcurrentNodeFetches(), maxNumConcurrentNeighborhoodFetches());
    }

    @Override
    @Nonnull
    public String toString() {
        return "Config[metric=" + getMetric() + ", numDimensions=" + getNumDimensions() +
                ", primaryClusterMin=" + primaryClusterMin() + ", clusterClusterMax=" + primaryClusterMax() +
                ", underreplicatedPrimaryClusterMax=" + underreplicatedPrimaryClusterMax() +
                ", replicatedClusterMax=" + replicatedClusterMaxWrites() +
                ", replicatedClusterTarget=" + replicatedClusterTarget() +
                ", replicationPriorityMin=" + replicationPriorityMin() +
                ", sampleVectorStatsProbability=" + sampleVectorStatsProbability() +
                ", mainStatsProbability=" + maintainStatsProbability() + ", statsThreshold=" + statsThreshold() +
                ", useRaBitQ=" + isUseRaBitQ() + ", raBitQNumExBits=" + getRaBitQNumExBits() +
                ", deterministicRandomness=" + deterministicRandomness() +
                ", maxNumConcurrentNodeFetches=" + maxNumConcurrentNodeFetches() +
                ", maxNumConcurrentNeighborhoodFetches=" + maxNumConcurrentNeighborhoodFetches() +
                "]";
    }

    /**
     * Builder for {@link Config}.
     *
     * @see HNSW#newConfigBuilder
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

        private double sampleVectorStatsProbability = DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY;
        private double maintainStatsProbability = DEFAULT_MAINTAIN_STATS_PROBABILITY;
        private int statsThreshold = DEFAULT_STATS_THRESHOLD;

        private boolean useRaBitQ = DEFAULT_USE_RABITQ;
        private int raBitQNumExBits = DEFAULT_RABITQ_NUM_EX_BITS;

        private boolean deterministicRandomness = DEFAULT_DETERMINISTIC_RANDOMNESS;
        private int maxNumConcurrentNodeFetches = DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES;
        private int maxNumConcurrentNeighborhoodFetches = DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES;

        public ConfigBuilder() {
        }

        public ConfigBuilder(@Nonnull final Metric metric, final int primaryClusterMin, final int primaryClusterMax,
                             final int underreplicatedPrimaryClusterMax, final int replicatedClusterMaxWrites,
                             final int replicatedClusterTarget, final double replicationPriorityMin,
                             final double sampleVectorStatsProbability, final double maintainStatsProbability,
                             final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits,
                             final boolean deterministicRandomness, final int maxNumConcurrentNodeFetches,
                             final int maxNumConcurrentNeighborhoodFetches) {
            this.metric = metric;
            this.primaryClusterMin = primaryClusterMin;
            this.underreplicatedPrimaryClusterMax = underreplicatedPrimaryClusterMax;
            this.primaryClusterMax = primaryClusterMax;
            this.replicatedClusterMaxWrites = replicatedClusterMaxWrites;
            this.replicatedClusterTarget = replicatedClusterTarget;
            this.replicationPriorityMin = replicationPriorityMin;
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            this.maintainStatsProbability = maintainStatsProbability;
            this.statsThreshold = statsThreshold;
            this.useRaBitQ = useRaBitQ;
            this.raBitQNumExBits = raBitQNumExBits;
            this.deterministicRandomness = deterministicRandomness;
            this.maxNumConcurrentNodeFetches = maxNumConcurrentNodeFetches;
            this.maxNumConcurrentNeighborhoodFetches = maxNumConcurrentNeighborhoodFetches;
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

        public Config build(final int numDimensions) {
            return new Config(getMetric(), numDimensions, getPrimaryClusterMin(), getPrimaryClusterMax(),
                    getUnderreplicatedPrimaryClusterMax(), getReplicatedClusterMaxWrites(),
                    getReplicatedClusterTarget(), getReplicationPriorityMin(), getSampleVectorStatsProbability(),
                    getMaintainStatsProbability(), getStatsThreshold(), isUseRaBitQ(), getRaBitQNumExBits(),
                    isDeterministicRandomness(), getMaxNumConcurrentNodeFetches(),
                    getMaxNumConcurrentNeighborhoodFetches());
        }
    }
}
