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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.async.common.VectorEncodingConfig;
import com.apple.foundationdb.linear.Metric;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * Configuration settings for an {@link HNSW}.
 *
 * @param metric the metric used to determine distances between vectors
 * @param numDimensions the number of dimensions; all vectors must have exactly this many dimensions
 * @param useInlining whether layers above 0 use inlined storage (vectors stored with neighbor links)
 * @param m the target connectivity for each node (named M in the HNSW paper)
 * @param mMax the maximum connectivity for nodes on layers above 0 (M_max in the paper)
 * @param mMax0 the maximum connectivity for nodes on layer 0 (M_max0 in the paper)
 * @param efConstruction the search queue size during insertion (higher = more accurate but slower)
 * @param efRepair the candidate set size during delete repair
 * @param extendCandidates whether to extend the candidate set with neighbors-of-neighbors during insert
 * @param keepPrunedConnections whether to re-add pruned candidates to pad underconnected nodes
 * @param sampleVectorStatsProbability probability of sampling a vector write for statistics computation
 * @param maintainStatsProbability probability of aggregating sampled vectors during insert
 * @param statsThreshold number of sampled vectors that triggers centroid computation
 * @param useRaBitQ whether to use RaBitQ quantization
 * @param raBitQNumExBits number of extra bits per dimension for RaBitQ encoding
 * @param maxNumConcurrentNodeFetches maximum concurrent node fetches during search and modification
 * @param maxNumConcurrentNeighborhoodFetches maximum concurrent neighborhood fetches during pruning
 * @param maxNumConcurrentDeleteFromLayer maximum concurrent delete operations per layer
 */
@SuppressWarnings("checkstyle:MemberName")
public record Config(@Nonnull Metric metric,
                     int numDimensions,
                     boolean useInlining,
                     int m,
                     int mMax,
                     int mMax0,
                     int efConstruction,
                     int efRepair,
                     boolean extendCandidates,
                     boolean keepPrunedConnections,
                     double sampleVectorStatsProbability,
                     double maintainStatsProbability,
                     int statsThreshold,
                     boolean useRaBitQ,
                     int raBitQNumExBits,
                     int maxNumConcurrentNodeFetches,
                     int maxNumConcurrentNeighborhoodFetches,
                     int maxNumConcurrentDeleteFromLayer) implements VectorEncodingConfig {

    @Nonnull public static final Metric DEFAULT_METRIC = Metric.EUCLIDEAN_METRIC;
    public static final boolean DEFAULT_USE_INLINING = false;
    public static final int DEFAULT_M = 16;
    public static final int DEFAULT_M_MAX_0 = 2 * DEFAULT_M;
    public static final int DEFAULT_M_MAX = DEFAULT_M;
    public static final int DEFAULT_EF_CONSTRUCTION = 200;
    public static final int DEFAULT_EF_REPAIR = 64;
    public static final boolean DEFAULT_EXTEND_CANDIDATES = false;
    public static final boolean DEFAULT_KEEP_PRUNED_CONNECTIONS = false;
    // stats
    public static final double DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY = 0.5d;
    public static final double DEFAULT_MAINTAIN_STATS_PROBABILITY = 0.05d;
    public static final int DEFAULT_STATS_THRESHOLD = 1000;
    // RaBitQ
    public static final boolean DEFAULT_USE_RABITQ = false;
    public static final int DEFAULT_RABITQ_NUM_EX_BITS = 4;
    // concurrency
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES = 16;
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES = 10;
    public static final int DEFAULT_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER = 2;

    public Config {
        Preconditions.checkArgument(numDimensions >= 1, "numDimensions must be (1, MAX_INT]");
        Preconditions.checkArgument(m >= 4 && m <= 200, "m must be [4, 200]");
        Preconditions.checkArgument(mMax >= 4 && mMax <= 200, "mMax must be [4, 200]");
        Preconditions.checkArgument(mMax0 >= 4 && mMax0 <= 300, "mMax0 must be [4, 300]");
        Preconditions.checkArgument(m <= mMax, "m must be less than or equal to mMax");
        Preconditions.checkArgument(mMax <= mMax0, "mMax must be less than or equal to mMax0");
        Preconditions.checkArgument(efConstruction >= 100 && efConstruction <= 400,
                "efConstruction must be [100, 400]");
        Preconditions.checkArgument(efRepair >= m && efRepair <= 400,
                "efRepair must be [m, 400]");
        Preconditions.checkArgument(!useRaBitQ ||
                (sampleVectorStatsProbability > 0.0d && sampleVectorStatsProbability <= 1.0d),
                "sampleVectorStatsProbability out of range");
        Preconditions.checkArgument(!useRaBitQ ||
                (maintainStatsProbability > 0.0d && maintainStatsProbability <= 1.0d),
                "maintainStatsProbability out of range");
        Preconditions.checkArgument(!useRaBitQ || statsThreshold > 10, "statThreshold out of range");
        Preconditions.checkArgument(!useRaBitQ || (raBitQNumExBits > 0 && raBitQNumExBits < 16),
                "raBitQNumExBits out of range");
        Preconditions.checkArgument(maxNumConcurrentNodeFetches > 0 && maxNumConcurrentNodeFetches <= 64,
                "maxNumConcurrentNodeFetches must be (0, 64]");
        Preconditions.checkArgument(maxNumConcurrentNeighborhoodFetches > 0 &&
                maxNumConcurrentNeighborhoodFetches <= 20,
                "maxNumConcurrentNeighborhoodFetches must be (0, 20]");
        Preconditions.checkArgument(maxNumConcurrentDeleteFromLayer > 0 &&
                        maxNumConcurrentDeleteFromLayer <= 10,
                "maxNumConcurrentDeleteFromLayer must be (0, 10]");
    }

    @Nonnull
    public ConfigBuilder toBuilder() {
        return new ConfigBuilder(metric(), useInlining(), m(), mMax(), mMax0(),
                efConstruction(), efRepair(), extendCandidates(), keepPrunedConnections(),
                sampleVectorStatsProbability(), maintainStatsProbability(), statsThreshold(),
                useRaBitQ(), raBitQNumExBits(), maxNumConcurrentNodeFetches(),
                maxNumConcurrentNeighborhoodFetches(), maxNumConcurrentDeleteFromLayer());
    }

    @Override
    @Nonnull
    public String toString() {
        return "Config[metric=" + metric() + ", numDimensions=" + numDimensions() +
                ", useInlining=" + useInlining() + ", m=" + m() + ", mMax=" + mMax() +
                ", mMax0=" + mMax0() + ", efConstruction=" + efConstruction() +
                ", efRepair=" + efRepair() + ", extendCandidates=" + extendCandidates() +
                ", keepPrunedConnections=" + keepPrunedConnections() +
                ", sampleVectorStatsProbability=" + sampleVectorStatsProbability() +
                ", maintainStatsProbability=" + maintainStatsProbability() +
                ", statsThreshold=" + statsThreshold() +
                ", useRaBitQ=" + useRaBitQ() + ", raBitQNumExBits=" + raBitQNumExBits() +
                ", maxNumConcurrentNodeFetches=" + maxNumConcurrentNodeFetches() +
                ", maxNumConcurrentNeighborhoodFetches=" + maxNumConcurrentNeighborhoodFetches() +
                ", maxNumConcurrentDeleteFromLayer=" + maxNumConcurrentDeleteFromLayer() +
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
        private boolean useInlining = DEFAULT_USE_INLINING;
        private int m = DEFAULT_M;
        private int mMax = DEFAULT_M_MAX;
        private int mMax0 = DEFAULT_M_MAX_0;
        private int efConstruction = DEFAULT_EF_CONSTRUCTION;
        private int efRepair = DEFAULT_EF_REPAIR;
        private boolean extendCandidates = DEFAULT_EXTEND_CANDIDATES;
        private boolean keepPrunedConnections = DEFAULT_KEEP_PRUNED_CONNECTIONS;

        private double sampleVectorStatsProbability = DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY;
        private double maintainStatsProbability = DEFAULT_MAINTAIN_STATS_PROBABILITY;
        private int statsThreshold = DEFAULT_STATS_THRESHOLD;

        private boolean useRaBitQ = DEFAULT_USE_RABITQ;
        private int raBitQNumExBits = DEFAULT_RABITQ_NUM_EX_BITS;

        private int maxNumConcurrentNodeFetches = DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES;
        private int maxNumConcurrentNeighborhoodFetches = DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES;
        private int maxNumConcurrentDeleteFromLayer = DEFAULT_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER;

        public ConfigBuilder() {
        }

        public ConfigBuilder(@Nonnull final Metric metric, final boolean useInlining, final int m, final int mMax,
                             final int mMax0, final int efConstruction, final int efRepair,
                             final boolean extendCandidates, final boolean keepPrunedConnections,
                             final double sampleVectorStatsProbability, final double maintainStatsProbability,
                             final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits,
                             final int maxNumConcurrentNodeFetches, final int maxNumConcurrentNeighborhoodFetches,
                             final int maxNumConcurrentDeleteFromLayer) {
            this.metric = metric;
            this.useInlining = useInlining;
            this.m = m;
            this.mMax = mMax;
            this.mMax0 = mMax0;
            this.efConstruction = efConstruction;
            this.efRepair = efRepair;
            this.extendCandidates = extendCandidates;
            this.keepPrunedConnections = keepPrunedConnections;
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            this.maintainStatsProbability = maintainStatsProbability;
            this.statsThreshold = statsThreshold;
            this.useRaBitQ = useRaBitQ;
            this.raBitQNumExBits = raBitQNumExBits;
            this.maxNumConcurrentNodeFetches = maxNumConcurrentNodeFetches;
            this.maxNumConcurrentNeighborhoodFetches = maxNumConcurrentNeighborhoodFetches;
            this.maxNumConcurrentDeleteFromLayer = maxNumConcurrentDeleteFromLayer;
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

        public boolean isUseInlining() {
            return useInlining;
        }

        @Nonnull
        public ConfigBuilder setUseInlining(final boolean useInlining) {
            this.useInlining = useInlining;
            return this;
        }

        public int getM() {
            return m;
        }

        @Nonnull
        public ConfigBuilder setM(final int m) {
            this.m = m;
            return this;
        }

        public int getMMax() {
            return mMax;
        }

        @Nonnull
        public ConfigBuilder setMMax(final int mMax) {
            this.mMax = mMax;
            return this;
        }

        public int getMMax0() {
            return mMax0;
        }

        @Nonnull
        public ConfigBuilder setMMax0(final int mMax0) {
            this.mMax0 = mMax0;
            return this;
        }

        public int getEfConstruction() {
            return efConstruction;
        }

        @Nonnull
        public ConfigBuilder setEfConstruction(final int efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        public int getEfRepair() {
            return efRepair;
        }

        @Nonnull
        public ConfigBuilder setEfRepair(final int efRepair) {
            this.efRepair = efRepair;
            return this;
        }

        public boolean isExtendCandidates() {
            return extendCandidates;
        }

        @Nonnull
        public ConfigBuilder setExtendCandidates(final boolean extendCandidates) {
            this.extendCandidates = extendCandidates;
            return this;
        }

        public boolean isKeepPrunedConnections() {
            return keepPrunedConnections;
        }

        @Nonnull
        public ConfigBuilder setKeepPrunedConnections(final boolean keepPrunedConnections) {
            this.keepPrunedConnections = keepPrunedConnections;
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

        public int getMaxNumConcurrentDeleteFromLayer() {
            return maxNumConcurrentDeleteFromLayer;
        }

        public ConfigBuilder setMaxNumConcurrentDeleteFromLayer(final int maxNumConcurrentDeleteFromLayer) {
            this.maxNumConcurrentDeleteFromLayer = maxNumConcurrentDeleteFromLayer;
            return this;
        }

        public Config build(final int numDimensions) {
            return new Config(getMetric(), numDimensions, isUseInlining(), getM(), getMMax(),
                    getMMax0(), getEfConstruction(), getEfRepair(), isExtendCandidates(), isKeepPrunedConnections(),
                    getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                    isUseRaBitQ(), getRaBitQNumExBits(), getMaxNumConcurrentNodeFetches(),
                    getMaxNumConcurrentNeighborhoodFetches(), getMaxNumConcurrentDeleteFromLayer());
        }
    }
}
