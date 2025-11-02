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

import com.apple.foundationdb.linear.Metric;
import com.google.common.base.Objects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * Configuration settings for a {@link HNSW}.
 */
@SuppressWarnings("checkstyle:MemberName")
public final class Config {
    public static final long DEFAULT_RANDOM_SEED = 0L;
    @Nonnull public static final Metric DEFAULT_METRIC = Metric.EUCLIDEAN_METRIC;
    public static final boolean DEFAULT_USE_INLINING = false;
    public static final int DEFAULT_M = 16;
    public static final int DEFAULT_M_MAX_0 = 2 * DEFAULT_M;
    public static final int DEFAULT_M_MAX = DEFAULT_M;
    public static final int DEFAULT_EF_CONSTRUCTION = 200;
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
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES = 16;

    /**
     * The random seed that is used to probabilistically determine the highest layer of an insert.
     */
    private final long randomSeed;

    /**
     * The metric that is used to determine distances between vectors.
     */
    @Nonnull
    private final Metric metric;

    /**
     * The number of dimensions used. All vectors must have exactly this number of dimensions.
     */
    private final int numDimensions;

    /**
     * Indicator if all layers except layer {@code 0} use inlining. If inlining is used, each node is persisted
     * as a key/value pair per neighbor which includes the vectors of the neighbors but not for itself. If inlining is
     * not used, each node is persisted as exactly one key/value pair per node which stores its own vector but
     * specifically excludes the vectors of the neighbors.
     */
    private final boolean useInlining;

    /**
     * This attribute (named {@code M} by the HNSW paper) is the connectivity value for all nodes stored on any layer.
     * While by no means enforced or even enforceable, we strive to create and maintain exactly {@code m} neighbors for
     * a node. Due to insert/delete operations it is possible that the actual number of neighbors a node references is
     * not exactly {@code m} at any given time.
     */
    private final int m;

    /**
     * This attribute (named {@code M_max} by the HNSW paper) is the maximum connectivity value for nodes stored on a
     * layer greater than {@code 0}. We will never create more that {@code mMax} neighbors for a node. That means that
     * we even prune the neighbors of a node if the actual number of neighbors would otherwise exceed {@code mMax}.
     */
    private final int mMax;

    /**
     * This attribute (named {@code M_max0} by the HNSW paper) is the maximum connectivity value for nodes stored on
     * layer {@code 0}. We will never create more that {@code mMax0} neighbors for a node that is stored on that layer.
     * That means that we even prune the neighbors of a node if the actual number of neighbors would otherwise exceed
     * {@code mMax0}.
     */
    private final int mMax0;

    /**
     * Maximum size of the search queues (on independent queue per layer) that are used during the insertion of a new
     * node. If {@code efConstruction} is set to {@code 1}, the search naturally follows a greedy approach
     * (monotonous descent), whereas a high number for {@code efConstruction} allows for a more nuanced search that can
     * tolerate (false) local minima.
     */
    private final int efConstruction;

    /**
     * Indicator to signal if, during the insertion of a node, the set of nearest neighbors of that node is to be
     * extended by the actual neighbors of those neighbors to form a set of candidates that the new node may be
     * connected to during the insert operation.
     */
    private final boolean extendCandidates;

    /**
     * Indicator to signal if, during the insertion of a node, candidates that have been discarded due to not satisfying
     * the select-neighbor heuristic may get added back in to pad the set of neighbors if the new node would otherwise
     * have too few neighbors (see {@link #m}).
     */
    private final boolean keepPrunedConnections;

    /**
     * If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}), this attribute represents the
     * probability of a vector being inserted to also be written into the
     * {@link StorageAdapter#SUBSPACE_PREFIX_SAMPLES} subspace. The vectors in that subspace are continuously aggregated
     * until a total {@link #statsThreshold} has been reached.
     */
    private final double sampleVectorStatsProbability;

    /**
     * If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}), this attribute represents the
     * probability of the {@link StorageAdapter#SUBSPACE_PREFIX_SAMPLES} subspace to be further aggregated (rolled-up)
     * when a new vector is inserted. The vectors in that subspace are continuously aggregated until a total
     * {@link #statsThreshold} has been reached.
     */
    private final double maintainStatsProbability;

    /**
     * If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}), this attribute represents the
     * threshold (being a number of vectors) that when reached causes the stats maintenance logic to compute the actual
     * statistics (currently the centroid of the vectors that have been inserted to far).
     */
    private final int statsThreshold;

    /**
     * Indicator if we should RaBitQ quantization. See {@link com.apple.foundationdb.rabitq.RaBitQuantizer} for more
     * details.
     */
    private final boolean useRaBitQ;

    /**
     * Number of bits per dimensions iff {@link #isUseRaBitQ()} is set to {@code true}, ignored otherwise. If RaBitQ
     * encoding is used, a vector is stored using roughly {@code 25 + numDimensions * (numExBits + 1) / 8} bytes.
     */
    private final int raBitQNumExBits;

    /**
     * Maximum number of concurrent node fetches during search and modification operations.
     */
    private final int maxNumConcurrentNodeFetches;

    /**
     * Maximum number of concurrent neighborhood fetches during modification operations when the neighbors are pruned.
     */
    private final int maxNumConcurrentNeighborhoodFetches;

    private Config(final long randomSeed, @Nonnull final Metric metric, final int numDimensions,
                   final boolean useInlining, final int m, final int mMax, final int mMax0,
                   final int efConstruction, final boolean extendCandidates, final boolean keepPrunedConnections,
                   final double sampleVectorStatsProbability, final double maintainStatsProbability,
                   final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits,
                   final int maxNumConcurrentNodeFetches, final int maxNumConcurrentNeighborhoodFetches) {
        this.randomSeed = randomSeed;
        this.metric = metric;
        this.numDimensions = numDimensions;
        this.useInlining = useInlining;
        this.m = m;
        this.mMax = mMax;
        this.mMax0 = mMax0;
        this.efConstruction = efConstruction;
        this.extendCandidates = extendCandidates;
        this.keepPrunedConnections = keepPrunedConnections;
        this.sampleVectorStatsProbability = sampleVectorStatsProbability;
        this.maintainStatsProbability = maintainStatsProbability;
        this.statsThreshold = statsThreshold;
        this.useRaBitQ = useRaBitQ;
        this.raBitQNumExBits = raBitQNumExBits;
        this.maxNumConcurrentNodeFetches = maxNumConcurrentNodeFetches;
        this.maxNumConcurrentNeighborhoodFetches = maxNumConcurrentNeighborhoodFetches;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    @Nonnull
    public Metric getMetric() {
        return metric;
    }

    public int getNumDimensions() {
        return numDimensions;
    }

    public boolean isUseInlining() {
        return useInlining;
    }

    public int getM() {
        return m;
    }

    public int getMMax() {
        return mMax;
    }

    public int getMMax0() {
        return mMax0;
    }

    public int getEfConstruction() {
        return efConstruction;
    }

    public boolean isExtendCandidates() {
        return extendCandidates;
    }

    public boolean isKeepPrunedConnections() {
        return keepPrunedConnections;
    }

    public double getSampleVectorStatsProbability() {
        return sampleVectorStatsProbability;
    }

    public double getMaintainStatsProbability() {
        return maintainStatsProbability;
    }

    public int getStatsThreshold() {
        return statsThreshold;
    }

    public boolean isUseRaBitQ() {
        return useRaBitQ;
    }

    public int getRaBitQNumExBits() {
        return raBitQNumExBits;
    }

    public int getMaxNumConcurrentNodeFetches() {
        return maxNumConcurrentNodeFetches;
    }

    public int getMaxNumConcurrentNeighborhoodFetches() {
        return maxNumConcurrentNeighborhoodFetches;
    }

    @Nonnull
    public ConfigBuilder toBuilder() {
        return new ConfigBuilder(getRandomSeed(), getMetric(), isUseInlining(), getM(), getMMax(), getMMax0(),
                getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections(),
                getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                isUseRaBitQ(), getRaBitQNumExBits(), getMaxNumConcurrentNodeFetches(),
                getMaxNumConcurrentNeighborhoodFetches());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Config)) {
            return false;
        }
        final Config config = (Config)o;
        return randomSeed == config.randomSeed && numDimensions == config.numDimensions &&
                useInlining == config.useInlining && m == config.m && mMax == config.mMax && mMax0 == config.mMax0 &&
                efConstruction == config.efConstruction && extendCandidates == config.extendCandidates &&
                keepPrunedConnections == config.keepPrunedConnections &&
                Double.compare(sampleVectorStatsProbability, config.sampleVectorStatsProbability) == 0 &&
                Double.compare(maintainStatsProbability, config.maintainStatsProbability) == 0 &&
                statsThreshold == config.statsThreshold && useRaBitQ == config.useRaBitQ &&
                raBitQNumExBits == config.raBitQNumExBits && metric == config.metric &&
                maxNumConcurrentNodeFetches == config.maxNumConcurrentNodeFetches &&
                maxNumConcurrentNeighborhoodFetches == config.maxNumConcurrentNeighborhoodFetches;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(randomSeed, metric, numDimensions, useInlining, m, mMax, mMax0, efConstruction,
                extendCandidates, keepPrunedConnections, sampleVectorStatsProbability, maintainStatsProbability,
                statsThreshold, useRaBitQ, raBitQNumExBits, maxNumConcurrentNodeFetches, maxNumConcurrentNeighborhoodFetches);
    }

    @Override
    @Nonnull
    public String toString() {
        return "Config[randomSeed=" + getRandomSeed() + ", metric=" + getMetric() +
                ", numDimensions=" + getNumDimensions() + ", isUseInlining=" + isUseInlining() + ", M=" + getM() +
                ", MMax=" + getMMax() + ", MMax0=" + getMMax0() + ", efConstruction=" + getEfConstruction() +
                ", isExtendCandidates=" + isExtendCandidates() +
                ", isKeepPrunedConnections=" + isKeepPrunedConnections() +
                ", sampleVectorStatsProbability=" + getSampleVectorStatsProbability() +
                ", mainStatsProbability=" + getMaintainStatsProbability() + ", statsThreshold=" + getStatsThreshold() +
                ", useRaBitQ=" + isUseRaBitQ() + ", raBitQNumExBits=" + getRaBitQNumExBits() +
                ", maxNumConcurrentNodeFetches=" + getMaxNumConcurrentNodeFetches() +
                ", maxNumConcurrentNeighborhoodFetches=" + getMaxNumConcurrentNeighborhoodFetches() +
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
        private long randomSeed = DEFAULT_RANDOM_SEED;
        @Nonnull
        private Metric metric = DEFAULT_METRIC;
        private boolean useInlining = DEFAULT_USE_INLINING;
        private int m = DEFAULT_M;
        private int mMax = DEFAULT_M_MAX;
        private int mMax0 = DEFAULT_M_MAX_0;
        private int efConstruction = DEFAULT_EF_CONSTRUCTION;
        private boolean extendCandidates = DEFAULT_EXTEND_CANDIDATES;
        private boolean keepPrunedConnections = DEFAULT_KEEP_PRUNED_CONNECTIONS;

        private double sampleVectorStatsProbability = DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY;
        private double maintainStatsProbability = DEFAULT_MAINTAIN_STATS_PROBABILITY;
        private int statsThreshold = DEFAULT_STATS_THRESHOLD;

        private boolean useRaBitQ = DEFAULT_USE_RABITQ;
        private int raBitQNumExBits = DEFAULT_RABITQ_NUM_EX_BITS;

        private int maxNumConcurrentNodeFetches = DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES;
        private int maxNumConcurrentNeighborhoodFetches = DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES;

        public ConfigBuilder() {
        }

        public ConfigBuilder(final long randomSeed, @Nonnull final Metric metric, final boolean useInlining,
                             final int m, final int mMax, final int mMax0, final int efConstruction,
                             final boolean extendCandidates, final boolean keepPrunedConnections,
                             final double sampleVectorStatsProbability, final double maintainStatsProbability,
                             final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits,
                             final int maxNumConcurrentNodeFetches, final int maxNumConcurrentNeighborhoodFetches) {
            this.randomSeed = randomSeed;
            this.metric = metric;
            this.useInlining = useInlining;
            this.m = m;
            this.mMax = mMax;
            this.mMax0 = mMax0;
            this.efConstruction = efConstruction;
            this.extendCandidates = extendCandidates;
            this.keepPrunedConnections = keepPrunedConnections;
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            this.maintainStatsProbability = maintainStatsProbability;
            this.statsThreshold = statsThreshold;
            this.useRaBitQ = useRaBitQ;
            this.raBitQNumExBits = raBitQNumExBits;
            this.maxNumConcurrentNodeFetches = maxNumConcurrentNodeFetches;
            this.maxNumConcurrentNeighborhoodFetches = maxNumConcurrentNeighborhoodFetches;
        }

        public long getRandomSeed() {
            return randomSeed;
        }

        @Nonnull
        public ConfigBuilder setRandomSeed(final long randomSeed) {
            this.randomSeed = randomSeed;
            return this;
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

        public Config build(final int numDimensions) {
            return new Config(getRandomSeed(), getMetric(), numDimensions, isUseInlining(), getM(), getMMax(),
                    getMMax0(), getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections(),
                    getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                    isUseRaBitQ(), getRaBitQNumExBits(), getMaxNumConcurrentNodeFetches(),
                    getMaxNumConcurrentNeighborhoodFetches());
        }
    }
}
