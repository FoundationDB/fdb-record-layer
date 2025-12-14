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
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Configuration settings for a {@link HNSW}.
 */
@SuppressWarnings("checkstyle:MemberName")
public final class Config {
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
    public static final int DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES = 16;
    public static final int DEFAULT_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER = 2;

    @Nonnull
    private final Metric metric;
    private final int numDimensions;
    private final boolean useInlining;
    private final int m;
    private final int mMax;
    private final int mMax0;
    private final int efConstruction;
    private final int efRepair;
    private final boolean extendCandidates;
    private final boolean keepPrunedConnections;
    private final double sampleVectorStatsProbability;
    private final double maintainStatsProbability;
    private final int statsThreshold;
    private final boolean useRaBitQ;
    private final int raBitQNumExBits;
    private final int maxNumConcurrentNodeFetches;
    private final int maxNumConcurrentNeighborhoodFetches;
    private final int maxNumConcurrentDeleteFromLayer;

    private Config(@Nonnull final Metric metric, final int numDimensions, final boolean useInlining, final int m,
                   final int mMax, final int mMax0, final int efConstruction, final int efRepair,
                   final boolean extendCandidates, final boolean keepPrunedConnections,
                   final double sampleVectorStatsProbability, final double maintainStatsProbability,
                   final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits,
                   final int maxNumConcurrentNodeFetches, final int maxNumConcurrentNeighborhoodFetches,
                   final int maxNumConcurrentDeleteFromLayer) {
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
                maxNumConcurrentNeighborhoodFetches <= 64,
                "maxNumConcurrentNeighborhoodFetches must be (0, 64]");
        Preconditions.checkArgument(maxNumConcurrentDeleteFromLayer > 0 &&
                        maxNumConcurrentDeleteFromLayer <= 64,
                "maxNumConcurrentDeleteFromLayer must be (0, 64]");

        this.metric = metric;
        this.numDimensions = numDimensions;
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

    /**
     * The metric that is used to determine distances between vectors.
     */
    @Nonnull
    public Metric getMetric() {
        return metric;
    }

    /**
     * The number of dimensions used. All vectors must have exactly this number of dimensions.
     */
    public int getNumDimensions() {
        return numDimensions;
    }

    /**
     * Indicator if all layers except layer {@code 0} use inlining. One entire layer is fully managed using either
     * the compact or the inlining layout. If inlining is used, each node is persisted as a key/value pair per neighbor
     * which includes the vectors of the neighbors but not the vector for itself. If inlining is not used, and therefore
     * the compact layout is used instead, each node is persisted as exactly one key/value pair per node which stores
     * its own vector but specifically excludes the vectors of the neighbors.
     * <p>
     * If a layer uses the compact storage layout, each vector is stored with the node and therefore is stored exactly
     * once. During a nearest neighbor search, a fetch of the neighborhood of a node incurs a fetch (get) for each of
     * the neighbors of that node.
     * <p>
     * If a layer uses the inlining storage layout, a vector of a node is stored with the neighboring information of an
     * adjacent node pointing to this node and is therefore is stored multiple times (once per neighbor). During a
     * nearest neighbor search, however, the neighboring vectors of a vector can all be fetched in one range scan.
     * <p>
     * Choosing which storage format is right for the use case depends on some factors:
     * <ul>
     *     <li>
     *         Inlining leaves uses more storage in the database as vectors are stored multiple times. However, since
     *         inlining is only supported for layers greater than {@code 0}, the overhead of storing more data should be
     *         acceptable in most use cases.
     *     </li>
     *     <li>
     *         Inlining should outperform the compact storage model during search. Tests have shown a latency
     *         improvement of searches of roughly 5% - 10%. Insert performance is slightly decreased due to higher
     *         number of bytes that need to be written. Note that this overhead can be mitigated by using
     *         {@link #isUseRaBitQ()} to drastically reduce the sizes of vectors on disk.
     *     </li>
     * </ul>
     */
    public boolean isUseInlining() {
        return useInlining;
    }

    /**
     * This attribute (named {@code M} by the HNSW paper) is the connectivity value for all nodes stored on any layer.
     * While by no means enforced or even enforceable, we strive to create and maintain exactly {@code m} neighbors for
     * a node. Due to insert/delete operations it is possible that the actual number of neighbors a node references is
     * not exactly {@code m} at any given time.
     */
    public int getM() {
        return m;
    }

    /**
     * This attribute (named {@code M_max} by the HNSW paper) is the maximum connectivity value for nodes stored on a
     * layer greater than {@code 0}. We will never create more that {@code mMax} neighbors for a node. That means that
     * we even prune the neighbors of a node if the actual number of neighbors would otherwise exceed {@code mMax}.
     * Note that this attribute must be greater than or equal to {@link #m}.
     */
    public int getMMax() {
        return mMax;
    }

    /**
     * This attribute (named {@code M_max0} by the HNSW paper) is the maximum connectivity value for nodes stored on
     * layer {@code 0}. We will never create more that {@code mMax0} neighbors for a node that is stored on that layer.
     * That means that we even prune the neighbors of a node if the actual number of neighbors would otherwise exceed
     * {@code mMax0}. Note that this attribute must be greater than or equal to {@link #mMax}.
     */
    public int getMMax0() {
        return mMax0;
    }

    /**
     * Maximum size of the search queues (one independent queue per layer) that are used during the insertion of a new
     * node. If {@code efConstruction} is set to a smaller number, the search naturally follows a more greedy approach
     * (monotonous descent), whereas a higher number for {@code efConstruction} allows for a more nuanced search that
     * can tolerate (false) local minima.
     */
    public int getEfConstruction() {
        return efConstruction;
    }

    /**
     * Maximum number of candidate nodes that are considered when a HNSW layer is locally repaired as part of a
     * delete operation. A smaller number causes the delete operation to create a smaller set of candidate nodes
     * which improves repair performance but not decreases repair quality, a higher number results in qualitatively
     * better repairs at the expense of slower performance.
     */
    public int getEfRepair() {
        return efRepair;
    }

    /**
     * Indicator to signal if, during the insertion of a node, the set of nearest neighbors of that node is to be
     * extended by the actual neighbors of those neighbors to form a set of candidates that the new node may be
     * connected to during the insert operation.
     */
    public boolean isExtendCandidates() {
        return extendCandidates;
    }

    /**
     * Indicator to signal if, during the insertion of a node, candidates that have been discarded due to not satisfying
     * the select-neighbor heuristic may get added back in to pad the set of neighbors if the new node would otherwise
     * have too few neighbors (see {@link #m}).
     */
    public boolean isKeepPrunedConnections() {
        return keepPrunedConnections;
    }

    /**
     * If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}), this attribute represents the
     * probability of a vector being inserted to also be written into the
     * {@link StorageAdapter#SUBSPACE_PREFIX_SAMPLES} subspace. The vectors in that subspace are continuously aggregated
     * until a total {@link #statsThreshold} has been reached.
     */
    public double getSampleVectorStatsProbability() {
        return sampleVectorStatsProbability;
    }

    /**
     * If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}), this attribute represents the
     * probability of the {@link StorageAdapter#SUBSPACE_PREFIX_SAMPLES} subspace to be further aggregated (rolled-up)
     * when a new vector is inserted. The vectors in that subspace are continuously aggregated until a total
     * {@link #statsThreshold} has been reached.
     */
    public double getMaintainStatsProbability() {
        return maintainStatsProbability;
    }

    /**
     * If sampling is necessary (currently iff {@link #isUseRaBitQ()} is {@code true}), this attribute represents the
     * threshold (being a number of vectors) that when reached causes the stats maintenance logic to compute the actual
     * statistics (currently the centroid of the vectors that have been inserted to far).
     */
    public int getStatsThreshold() {
        return statsThreshold;
    }

    /**
     * Indicator if we should RaBitQ quantization. See {@link com.apple.foundationdb.rabitq.RaBitQuantizer} for more
     * details.
     */
    public boolean isUseRaBitQ() {
        return useRaBitQ;
    }

    /**
     * Number of bits per dimensions iff {@link #isUseRaBitQ()} is set to {@code true}, ignored otherwise. If RaBitQ
     * encoding is used, a vector is stored using roughly {@code 25 + numDimensions * (numExBits + 1) / 8} bytes.
     */
    public int getRaBitQNumExBits() {
        return raBitQNumExBits;
    }

    /**
     * Maximum number of concurrent node fetches during search and modification operations.
     */
    public int getMaxNumConcurrentNodeFetches() {
        return maxNumConcurrentNodeFetches;
    }

    /**
     * Maximum number of concurrent neighborhood fetches during modification operations when the neighbors are pruned.
     */
    public int getMaxNumConcurrentNeighborhoodFetches() {
        return maxNumConcurrentNeighborhoodFetches;
    }

    /**
     * Maximum number of delete operations that can run concurrently during a delete operation.
     */
    public int getMaxNumConcurrentDeleteFromLayer() {
        return maxNumConcurrentDeleteFromLayer;
    }

    @Nonnull
    public ConfigBuilder toBuilder() {
        return new ConfigBuilder(getMetric(), isUseInlining(), getM(), getMMax(), getMMax0(),
                getEfConstruction(), getEfRepair(), isExtendCandidates(), isKeepPrunedConnections(),
                getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                isUseRaBitQ(), getRaBitQNumExBits(), getMaxNumConcurrentNodeFetches(),
                getMaxNumConcurrentNeighborhoodFetches(), getMaxNumConcurrentDeleteFromLayer());
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
        return numDimensions == config.numDimensions && useInlining == config.useInlining && m == config.m &&
                mMax == config.mMax && mMax0 == config.mMax0 && efConstruction == config.efConstruction &&
                efRepair == config.efRepair && extendCandidates == config.extendCandidates &&
                keepPrunedConnections == config.keepPrunedConnections &&
                Double.compare(sampleVectorStatsProbability, config.sampleVectorStatsProbability) == 0 &&
                Double.compare(maintainStatsProbability, config.maintainStatsProbability) == 0 &&
                statsThreshold == config.statsThreshold && useRaBitQ == config.useRaBitQ &&
                raBitQNumExBits == config.raBitQNumExBits && metric == config.metric &&
                maxNumConcurrentNodeFetches == config.maxNumConcurrentNodeFetches &&
                maxNumConcurrentNeighborhoodFetches == config.maxNumConcurrentNeighborhoodFetches &&
                maxNumConcurrentDeleteFromLayer == config.maxNumConcurrentDeleteFromLayer;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric, numDimensions, useInlining, m, mMax, mMax0, efConstruction, efRepair,
                extendCandidates, keepPrunedConnections, sampleVectorStatsProbability, maintainStatsProbability,
                statsThreshold, useRaBitQ, raBitQNumExBits, maxNumConcurrentNodeFetches,
                maxNumConcurrentNeighborhoodFetches, maxNumConcurrentDeleteFromLayer);
    }

    @Override
    @Nonnull
    public String toString() {
        return "Config[" + "metric=" + getMetric() + ", numDimensions=" + getNumDimensions() +
                ", isUseInlining=" + isUseInlining() + ", M=" + getM() + ", MMax=" + getMMax() +
                ", MMax0=" + getMMax0() + ", efConstruction=" + getEfConstruction() +
                ", efRepair=" + getEfRepair() + ", isExtendCandidates=" + isExtendCandidates() +
                ", isKeepPrunedConnections=" + isKeepPrunedConnections() +
                ", sampleVectorStatsProbability=" + getSampleVectorStatsProbability() +
                ", mainStatsProbability=" + getMaintainStatsProbability() + ", statsThreshold=" + getStatsThreshold() +
                ", useRaBitQ=" + isUseRaBitQ() + ", raBitQNumExBits=" + getRaBitQNumExBits() +
                ", maxNumConcurrentNodeFetches=" + getMaxNumConcurrentNodeFetches() +
                ", maxNumConcurrentNeighborhoodFetches=" + getMaxNumConcurrentNeighborhoodFetches() +
                ", maxNumConcurrentDeleteFromLayer=" + getMaxNumConcurrentDeleteFromLayer() +
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
