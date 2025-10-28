/*
 * HNSW.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.rabitq.RaBitQuantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;
import static com.apple.foundationdb.async.MoreAsyncUtil.forLoop;

/**
 * An implementation of the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 * <p>
 * HNSW constructs a multi-layer graph, where each layer is a subset of the one below it.
 * The top layers serve as fast entry points to navigate the graph, while the bottom layer
 * contains all the data points. This structure allows for logarithmic-time complexity
 * for search operations, making it suitable for large-scale, high-dimensional datasets.
 * <p>
 * This class provides methods for building the graph ({@link #insert(Transaction, Tuple, RealVector)})
 * and performing k-NN searches ({@link #kNearestNeighborsSearch(ReadTransaction, int, int, RealVector)}).
 * It is designed to be used with a transactional storage backend, managed via a {@link Subspace}.
 *
 * @see <a href="https://arxiv.org/abs/1603.09320">Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HNSW {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(HNSW.class);

    public static final int MAX_CONCURRENT_NODE_READS = 16;
    public static final int MAX_CONCURRENT_NEIGHBOR_FETCHES = 3;
    public static final int MAX_CONCURRENT_SEARCHES = 10;
    public static final long DEFAULT_RANDOM_SEED = 0L;
    @Nonnull public static final Metric DEFAULT_METRIC = Metric.EUCLIDEAN_METRIC;
    public static final boolean DEFAULT_USE_INLINING = false;
    public static final int DEFAULT_M = 16;
    public static final int DEFAULT_M_MAX = DEFAULT_M;
    public static final int DEFAULT_M_MAX_0 = 2 * DEFAULT_M;
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

    @Nonnull
    public static final ConfigBuilder DEFAULT_CONFIG_BUILDER = new ConfigBuilder();

    @Nonnull
    private final Random random;
    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final Config config;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    /**
     * Configuration settings for a {@link HNSW}.
     */
    @SuppressWarnings("checkstyle:MemberName")
    public static class Config {
        private final long randomSeed;
        @Nonnull
        private final Metric metric;
        private final int numDimensions;
        private final boolean useInlining;
        private final int m;
        private final int mMax;
        private final int mMax0;
        private final int efConstruction;
        private final boolean extendCandidates;
        private final boolean keepPrunedConnections;

        private final double sampleVectorStatsProbability;
        private final double maintainStatsProbability;
        private final int statsThreshold;

        private final boolean useRaBitQ;
        private final int raBitQNumExBits;

        protected Config(final long randomSeed, @Nonnull final Metric metric, final int numDimensions,
                         final boolean useInlining, final int m, final int mMax, final int mMax0,
                         final int efConstruction, final boolean extendCandidates, final boolean keepPrunedConnections,
                         final double sampleVectorStatsProbability, final double maintainStatsProbability,
                         final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits) {
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

        @Nonnull
        public ConfigBuilder toBuilder() {
            return new ConfigBuilder(getRandomSeed(), getMetric(), isUseInlining(), getM(), getMMax(), getMMax0(),
                    getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections(),
                    getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                    isUseRaBitQ(), getRaBitQNumExBits());
        }

        @Override
        public final boolean equals(final Object o) {
            if (!(o instanceof Config)) {
                return false;
            }

            final Config config = (Config)o;
            return randomSeed == config.randomSeed && numDimensions == config.numDimensions &&
                    useInlining == config.useInlining && m == config.m && mMax == config.mMax &&
                    mMax0 == config.mMax0 && efConstruction == config.efConstruction &&
                    extendCandidates == config.extendCandidates &&
                    keepPrunedConnections == config.keepPrunedConnections && useRaBitQ == config.useRaBitQ &&
                    sampleVectorStatsProbability == config.sampleVectorStatsProbability &&
                    maintainStatsProbability == config.maintainStatsProbability &&
                    statsThreshold == config.statsThreshold &&
                    raBitQNumExBits == config.raBitQNumExBits && metric == config.metric;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(randomSeed);
            result = 31 * result + metric.name().hashCode();
            result = 31 * result + numDimensions;
            result = 31 * result + Boolean.hashCode(useInlining);
            result = 31 * result + m;
            result = 31 * result + mMax;
            result = 31 * result + mMax0;
            result = 31 * result + efConstruction;
            result = 31 * result + Boolean.hashCode(extendCandidates);
            result = 31 * result + Boolean.hashCode(keepPrunedConnections);
            result = 31 * result + Double.hashCode(sampleVectorStatsProbability);
            result = 31 * result + Double.hashCode(maintainStatsProbability);
            result = 31 * result + statsThreshold;
            result = 31 * result + Boolean.hashCode(useRaBitQ);
            result = 31 * result + raBitQNumExBits;
            return result;
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
                    ", mainStatsProbability=" + getMaintainStatsProbability() +
                    ", statsThreshold=" + getStatsThreshold() +
                    ", useRaBitQ=" + isUseRaBitQ() +
                    ", raBitQNumExBits=" + getRaBitQNumExBits() + "]";
        }
    }

    /**
     * Builder for {@link Config}.
     *
     * @see #newConfigBuilder
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

        public ConfigBuilder() {
        }

        public ConfigBuilder(final long randomSeed, @Nonnull final Metric metric, final boolean useInlining,
                             final int m, final int mMax, final int mMax0, final int efConstruction,
                             final boolean extendCandidates, final boolean keepPrunedConnections,
                             final double sampleVectorStatsProbability, final double maintainStatsProbability,
                             final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits) {
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

        public Config build(final int numDimensions) {
            return new Config(getRandomSeed(), getMetric(), numDimensions, isUseInlining(), getM(), getMMax(),
                    getMMax0(), getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections(),
                    getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                    isUseRaBitQ(), getRaBitQNumExBits());
        }
    }

    /**
     * Start building a {@link Config}.
     * @return a new {@code Config} that can be altered and then built for use with a {@link HNSW}
     * @see ConfigBuilder#build
     */
    public static ConfigBuilder newConfigBuilder() {
        return new ConfigBuilder();
    }

    /**
     * Returns a default {@link Config}.
     * @param numDimensions number of dimensions
     * @return a new default {@code Config}.
     * @see ConfigBuilder#build
     */
    @Nonnull
    public static Config defaultConfig(int numDimensions) {
        return new ConfigBuilder().build(numDimensions);
    }

    /**
     * Creates a new {@code HNSW} instance using the default configuration, write listener, and read listener.
     * <p>
     * This constructor delegates to the main constructor, providing default values for configuration
     * and listeners, simplifying the instantiation process for common use cases.
     *
     * @param subspace the non-null {@link Subspace} to build the HNSW graph for.
     * @param executor the non-null {@link Executor} for concurrent operations, such as building the graph.
     * @param numDimensions the number of dimensions
     */
    public HNSW(@Nonnull final Subspace subspace, @Nonnull final Executor executor, final int numDimensions) {
        this(subspace, executor, DEFAULT_CONFIG_BUILDER.build(numDimensions), OnWriteListener.NOOP, OnReadListener.NOOP);
    }

    /**
     * Constructs a new HNSW graph instance.
     * <p>
     * This constructor initializes the HNSW graph with the necessary components for storage,
     * execution, configuration, and event handling. All parameters are mandatory and must not be null.
     *
     * @param subspace the {@link Subspace} where the graph data is stored.
     * @param executor the {@link Executor} service to use for concurrent operations.
     * @param config the {@link Config} object containing HNSW algorithm parameters.
     * @param onWriteListener a listener to be notified of write events on the graph.
     * @param onReadListener a listener to be notified of read events on the graph.
     *
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    public HNSW(@Nonnull final Subspace subspace,
                @Nonnull final Executor executor,
                @Nonnull final Config config,
                @Nonnull final OnWriteListener onWriteListener,
                @Nonnull final OnReadListener onReadListener) {
        this.random = new Random(config.getRandomSeed());
        this.subspace = subspace;
        this.executor = executor;
        this.config = config;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
    }


    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get this hnsw's configuration.
     * @return hnsw configuration
     */
    @Nonnull
    public Config getConfig() {
        return config;
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

    @Nonnull
    private AffineOperator storageTransform(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return AffineOperator.identity();
        }

        return new StorageTransform(accessInfo.getRotatorSeed(),
                getConfig().getNumDimensions(), Objects.requireNonNull(accessInfo.getCentroid()));
    }

    @Nonnull
    private Quantizer quantizer(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return Quantizer.noOpQuantizer(config.getMetric());
        }

        final Config config = getConfig();
        return config.isUseRaBitQ()
               ? new RaBitQuantizer(config.getMetric(), config.getRaBitQNumExBits())
               : Quantizer.noOpQuantizer(config.getMetric());
    }

    //
    // Read Path
    //

    /**
     * Performs a k-nearest neighbors (k-NN) search for a given query vector.
     * <p>
     * This method implements the search algorithm for an HNSW graph. The search begins at an entry point in the
     * highest layer and greedily traverses down through the layers. In each layer, it finds the node closest to the
     * {@code queryVector}. This node then serves as the entry point for the search in the layer below.
     * <p>
     * Once the search reaches the base layer (layer 0), it performs a more exhaustive search starting from the
     * determined entry point. It explores the graph, maintaining a dynamic list of the best candidates found so far.
     * The size of this candidate list is controlled by the {@code efSearch} parameter. Finally, the method selects
     * the top {@code k} nodes from the search results, sorted by their distance to the query vector.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param k the number of nearest neighbors to return
     * @param efSearch the size of the dynamic candidate list for the search. A larger value increases accuracy
     * at the cost of performance.
     * @param queryVector the vector to find the nearest neighbors for
     *
     * @return a {@link CompletableFuture} that will complete with a list of the {@code k} nearest neighbors,
     * sorted by distance in ascending order. The future completes with {@code null} if the index is empty.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    public CompletableFuture<? extends List<? extends NodeReferenceAndNode<? extends NodeReference>>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    final int efSearch,
                                    @Nonnull final RealVector queryVector) {
        return StorageAdapter.fetchAccessInfo(getConfig(), readTransaction, getSubspace(), getOnReadListener())
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(null); // not a single node in the index
                    }
                    final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();

                    final AffineOperator storageTransform = storageTransform(accessInfo);
                    final RealVector transformedQueryVector = storageTransform.applyInvert(queryVector);
                    final Quantizer quantizer = quantizer(accessInfo);
                    final Estimator estimator = quantizer.estimator();

                    final NodeReferenceWithDistance entryState =
                            new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                    entryNodeReference.getVector(),
                                    estimator.distance(transformedQueryVector, entryNodeReference.getVector()));

                    final int entryLayer = entryNodeReference.getLayer();
                    return forLoop(entryLayer, entryState,
                            layer -> layer >= 0,
                            layer -> layer - 1,
                            (layer, previousNodeReference) -> {
                                if (layer == 0) {
                                    // entry data points to a node in layer 0 directly
                                    return CompletableFuture.completedFuture(previousNodeReference);
                                }

                                final var storageAdapter = getStorageAdapterForLayer(layer);
                                return greedySearchLayer(storageAdapter, readTransaction, storageTransform, estimator,
                                        previousNodeReference, layer, transformedQueryVector);
                            }, executor)
                            .thenCompose(nodeReference -> {
                                if (nodeReference == null) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                final var storageAdapter = getStorageAdapterForLayer(0);

                                return searchLayer(storageAdapter, readTransaction, storageTransform, estimator,
                                        ImmutableList.of(nodeReference), 0, efSearch, Maps.newConcurrentMap(),
                                        transformedQueryVector)
                                        .thenApply(searchResult -> {
                                            // reverse the original queue
                                            final TreeMultimap<Double, NodeReferenceAndNode<? extends NodeReference>> sortedTopK =
                                                    TreeMultimap.create(Comparator.naturalOrder(),
                                                            Comparator.comparing(nodeReferenceAndNode ->
                                                                    nodeReferenceAndNode.getNode().getPrimaryKey()));

                                            for (final NodeReferenceAndNode<?> nodeReferenceAndNode : searchResult) {
                                                if (sortedTopK.size() < k || Objects.requireNonNull(sortedTopK.keySet().last()) >
                                                        nodeReferenceAndNode.getNodeReferenceWithDistance().getDistance()) {
                                                    sortedTopK.put(nodeReferenceAndNode.getNodeReferenceWithDistance().getDistance(),
                                                            nodeReferenceAndNode);
                                                }

                                                if (sortedTopK.size() > k) {
                                                    final Double lastKey = sortedTopK.keySet().last();
                                                    final NodeReferenceAndNode<?> lastNode = sortedTopK.get(lastKey).last();
                                                    sortedTopK.remove(lastKey, lastNode);
                                                }
                                            }

                                            return ImmutableList.copyOf(sortedTopK.values());
                                        });
                            });
                });
    }

    /**
     * Performs a greedy search on a single layer of the HNSW graph.
     * <p>
     * This method finds the node on the specified layer that is closest to the given query vector,
     * starting the search from a designated entry point. The search is "greedy" because it aims to find
     * only the single best neighbor.
     * <p>
     * The implementation strategy depends on the {@link NodeKind} of the provided {@link StorageAdapter}.
     * If the node kind is {@code INLINING}, it delegates to the specialized {@link #greedySearchInliningLayer} method.
     * Otherwise, it uses the more general {@link #searchLayer} method with a search size (ef) of 1.
     * The operation is asynchronous.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the {@link StorageAdapter} for accessing the graph data
     * @param readTransaction the {@link ReadTransaction} to use for the search
     * @param estimator a distance estimator
     * @param entryNeighbor the starting point for the search on this layer, which includes the node and its distance to
     * the query vector
     * @param layer the zero-based index of the layer to search within
     * @param queryVector the query vector for which to find the nearest neighbor
     *
     * @return a {@link CompletableFuture} that, upon completion, will contain the closest node found on the layer,
     * represented as a {@link NodeReferenceWithDistance}
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<NodeReferenceWithDistance>
            greedySearchLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                              @Nonnull final ReadTransaction readTransaction,
                              @Nonnull final AffineOperator storageTransform,
                              @Nonnull final Estimator estimator,
                              @Nonnull final NodeReferenceWithDistance entryNeighbor,
                              final int layer,
                              @Nonnull final RealVector queryVector) {
        if (storageAdapter.getNodeKind() == NodeKind.INLINING) {
            return greedySearchInliningLayer(storageAdapter.asInliningStorageAdapter(),
                    readTransaction, storageTransform, estimator, entryNeighbor, layer, queryVector);
        } else {
            return searchLayer(storageAdapter, readTransaction, storageTransform, estimator,
                    ImmutableList.of(entryNeighbor), layer, 1, Maps.newConcurrentMap(), queryVector)
                    .thenApply(searchResult ->
                            Iterables.getOnlyElement(searchResult).getNodeReferenceWithDistance());
        }
    }

    /**
     * Performs a greedy search for the nearest neighbor to a query vector within a single, non-zero layer of the HNSW
     * graph.
     * <p>
     * This search is performed on layers that use {@code InliningNode}s, where neighbor vectors are stored directly
     * within the node.
     * The search starts from a given {@code entryNeighbor} and iteratively moves to the closest neighbor in the current
     * node's
     * neighbor list, until no closer neighbor can be found.
     * <p>
     * The entire process is asynchronous, returning a {@link CompletableFuture} that will complete with the best node
     * found in this layer.
     *
     * @param storageAdapter the storage adapter to fetch nodes from the graph
     * @param readTransaction the transaction context for database reads
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     * storage space that is currently being used
     * @param estimator a distance estimator
     * @param entryNeighbor the entry point for the search in this layer, typically the result from a search in a higher
     * layer
     * @param layer the layer number to perform the search in. Must be greater than 0.
     * @param queryVector the vector for which to find the nearest neighbor
     *
     * @return a {@link CompletableFuture} that, upon completion, will hold the {@link NodeReferenceWithDistance} of the
     * nearest neighbor found in this layer's greedy search
     *
     * @throws IllegalStateException if a node that is expected to exist cannot be fetched from the
     * {@code storageAdapter} during the search
     */
    @Nonnull
    private CompletableFuture<NodeReferenceWithDistance>
            greedySearchInliningLayer(@Nonnull final StorageAdapter<NodeReferenceWithVector> storageAdapter,
                                      @Nonnull final ReadTransaction readTransaction,
                                      @Nonnull final AffineOperator storageTransform,
                                      @Nonnull final Estimator estimator,
                                      @Nonnull final NodeReferenceWithDistance entryNeighbor,
                                      final int layer,
                                      @Nonnull final RealVector queryVector) {
        Verify.verify(layer > 0);
        final AtomicReference<NodeReferenceWithDistance> currentNodeReferenceAtomic =
                new AtomicReference<>(entryNeighbor);

        return AsyncUtil.whileTrue(() -> onReadListener.onAsyncRead(
                        storageAdapter.fetchNode(readTransaction, storageTransform, layer,
                                currentNodeReferenceAtomic.get().getPrimaryKey()))
                .thenApply(node -> {
                    if (node == null) {
                        throw new IllegalStateException("unable to fetch node");
                    }
                    final InliningNode inliningNode = node.asInliningNode();
                    final List<NodeReferenceWithVector> neighbors = inliningNode.getNeighbors();

                    final NodeReferenceWithDistance currentNodeReference = currentNodeReferenceAtomic.get();
                    double minDistance = currentNodeReference.getDistance();

                    NodeReferenceWithVector nearestNeighbor = null;
                    for (final NodeReferenceWithVector neighbor : neighbors) {
                        final double distance = estimator.distance(queryVector, neighbor.getVector());
                        if (distance < minDistance) {
                            minDistance = distance;
                            nearestNeighbor = neighbor;
                        }
                    }

                    if (nearestNeighbor == null) {
                        return false;
                    }

                    currentNodeReferenceAtomic.set(
                            new NodeReferenceWithDistance(nearestNeighbor.getPrimaryKey(), nearestNeighbor.getVector(),
                                    minDistance));
                    return true;
                }), executor).thenApply(ignored -> currentNodeReferenceAtomic.get());
    }

    /**
     * Searches a single layer of the graph to find the nearest neighbors to a query vector.
     * <p>
     * This method implements the greedy search algorithm used in HNSW (Hierarchical Navigable Small World)
     * graphs for a specific layer. It begins with a set of entry points and iteratively explores the graph,
     * always moving towards nodes that are closer to the {@code queryVector}.
     * <p>
     * It maintains a priority queue of candidates to visit and a result set of the nearest neighbors found so far.
     * The size of the dynamic candidate list is controlled by the {@code efSearch} parameter, which balances
     * search quality and performance. The entire process is asynchronous, leveraging
     * {@link java.util.concurrent.CompletableFuture}
     * to handle I/O operations (fetching nodes) without blocking.
     *
     * @param <N> The type of the node reference, extending {@link NodeReference}.
     * @param storageAdapter The storage adapter for accessing node data from the underlying storage.
     * @param readTransaction The transaction context for all database read operations.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param estimator the estimator to use
     * @param entryNeighbors A collection of starting nodes for the search in this layer, with their distances
     * to the query vector already calculated.
     * @param layer The zero-based index of the layer to search.
     * @param efSearch The size of the dynamic candidate list. A larger value increases recall at the
     * cost of performance.
     * @param nodeCache A cache of nodes that have already been fetched from storage to avoid redundant I/O.
     * @param queryVector The vector for which to find the nearest neighbors.
     *
     * @return A {@link java.util.concurrent.CompletableFuture} that, upon completion, will contain a list of the
     * best candidate nodes found in this layer, paired with their full node data.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>>
            searchLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                        @Nonnull final ReadTransaction readTransaction,
                        @Nonnull final AffineOperator storageTransform,
                        @Nonnull final Estimator estimator,
                        @Nonnull final Collection<NodeReferenceWithDistance> entryNeighbors,
                        final int layer,
                        final int efSearch,
                        @Nonnull final Map<Tuple, Node<N>> nodeCache,
                        @Nonnull final RealVector queryVector) {
        final Set<Tuple> visited = Sets.newConcurrentHashSet(NodeReference.primaryKeys(entryNeighbors));
        final Queue<NodeReferenceWithDistance> candidates =
                new PriorityBlockingQueue<>(config.getM(),
                        Comparator.comparing(NodeReferenceWithDistance::getDistance));
        candidates.addAll(entryNeighbors);
        final Queue<NodeReferenceWithDistance> nearestNeighbors =
                new PriorityBlockingQueue<>(config.getM(),
                        Comparator.comparing(NodeReferenceWithDistance::getDistance).reversed());
        nearestNeighbors.addAll(entryNeighbors);

        return AsyncUtil.whileTrue(() -> {
            if (candidates.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }

            final NodeReferenceWithDistance candidate = candidates.poll();
            final NodeReferenceWithDistance furthestNeighbor = Objects.requireNonNull(nearestNeighbors.peek());

            if (candidate.getDistance() > furthestNeighbor.getDistance()) {
                return AsyncUtil.READY_FALSE;
            }

            return fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, layer, candidate, nodeCache)
                    .thenApply(candidateNode ->
                            Iterables.filter(candidateNode.getNeighbors(),
                                    neighbor -> !visited.contains(Objects.requireNonNull(neighbor).getPrimaryKey())))
                    .thenCompose(neighborReferences -> fetchNeighborhood(storageAdapter, readTransaction,
                            storageTransform, layer, neighborReferences, nodeCache))
                    .thenApply(neighborReferences -> {
                        for (final NodeReferenceWithVector current : neighborReferences) {
                            visited.add(current.getPrimaryKey());
                            final double furthestDistance =
                                    Objects.requireNonNull(nearestNeighbors.peek()).getDistance();

                            final double currentDistance = estimator.distance(queryVector, current.getVector());
                            if (currentDistance < furthestDistance || nearestNeighbors.size() < efSearch) {
                                final NodeReferenceWithDistance currentWithDistance =
                                        new NodeReferenceWithDistance(current.getPrimaryKey(), current.getVector(),
                                                currentDistance);
                                candidates.add(currentWithDistance);
                                nearestNeighbors.add(currentWithDistance);
                                if (nearestNeighbors.size() > efSearch) {
                                    nearestNeighbors.poll();
                                }
                            }
                        }
                        return true;
                    });
        }).thenCompose(ignored ->
                fetchSomeNodesIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
                        nearestNeighbors, nodeCache))
                .thenApply(searchResult -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("searched layer={} for efSearch={} with result=={}", layer, efSearch,
                                searchResult.stream()
                                        .map(nodeReferenceAndNode ->
                                                "(primaryKey=" +
                                                        nodeReferenceAndNode.getNodeReferenceWithDistance().getPrimaryKey() +
                                                        ",distance=" +
                                                        nodeReferenceAndNode.getNodeReferenceWithDistance().getDistance() + ")")
                                        .collect(Collectors.joining(",")));
                    }
                    return searchResult;
                });
    }

    /**
     * Asynchronously fetches a node if it is not already present in the cache.
     * <p>
     * This method first attempts to retrieve the node from the provided {@code nodeCache} using the
     * primary key of the {@code nodeReference}. If the node is not found in the cache, it is
     * fetched from the underlying storage using the {@code storageAdapter}. Once fetched, the node
     * is added to the {@code nodeCache} before the future is completed.
     * <p>
     * This is a convenience method that delegates to
     * {@link #fetchNodeIfNecessaryAndApply(StorageAdapter, ReadTransaction, AffineOperator, int, NodeReference, Function, BiFunction)}.
     *
     * @param <N> the type of the node reference, which must extend {@link NodeReference}
     * @param storageAdapter the storage adapter used to fetch the node from persistent storage
     * @param readTransaction the transaction to use for reading from storage
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer the layer index where the node is located
     * @param nodeReference the reference to the node to fetch
     * @param nodeCache the cache to check for the node and to which the node will be added if fetched
     *
     * @return a {@link CompletableFuture} that will be completed with the fetched or cached {@link Node}
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<Node<N>>
            fetchNodeIfNotCached(@Nonnull final StorageAdapter<N> storageAdapter,
                                 @Nonnull final ReadTransaction readTransaction,
                                 @Nonnull final AffineOperator storageTransform,
                                 final int layer,
                                 @Nonnull final NodeReference nodeReference,
                                 @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchNodeIfNecessaryAndApply(storageAdapter, readTransaction, storageTransform, layer, nodeReference,
                nR -> nodeCache.get(nR.getPrimaryKey()),
                (nR, node) -> {
                    nodeCache.put(nR.getPrimaryKey(), node);
                    return node;
                });
    }

    /**
     * Conditionally fetches a node from storage and applies a function to it.
     * <p>
     * This method first attempts to generate a result by applying the {@code fetchBypassFunction}.
     * If this function returns a non-null value, that value is returned immediately in a
     * completed {@link CompletableFuture}, and no storage access occurs. This provides an
     * optimization path, for example, if the required data is already available in a cache.
     * <p>
     * If the bypass function returns {@code null}, the method proceeds to asynchronously fetch the
     * node from the given {@code StorageAdapter}. Once the node is retrieved, the
     * {@code biMapFunction} is applied to the original {@code nodeReference} and the fetched
     * {@code Node} to produce the final result.
     *
     * @param <R> The type of the input node reference.
     * @param <N> The type of the node reference used by the storage adapter.
     * @param <U> The type of the result.
     * @param storageAdapter The storage adapter used to fetch the node if necessary.
     * @param readTransaction The read transaction context for the storage operation.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer The layer index from which to fetch the node.
     * @param nodeReference The reference to the node that may need to be fetched.
     * @param fetchBypassFunction A function that provides a potential shortcut. If it returns a
     * non-null value, the node fetch is bypassed.
     * @param biMapFunction A function to be applied after a successful node fetch, combining the
     * original reference and the fetched node to produce the final result.
     *
     * @return A {@link CompletableFuture} that will complete with the result from either the
     * {@code fetchBypassFunction} or the {@code biMapFunction}.
     */
    @Nonnull
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<U>
            fetchNodeIfNecessaryAndApply(@Nonnull final StorageAdapter<N> storageAdapter,
                                         @Nonnull final ReadTransaction readTransaction,
                                         @Nonnull final AffineOperator storageTransform,
                                         final int layer,
                                         @Nonnull final R nodeReference,
                                         @Nonnull final Function<R, U> fetchBypassFunction,
                                         @Nonnull final BiFunction<R, Node<N>, U> biMapFunction) {
        final U bypass = fetchBypassFunction.apply(nodeReference);
        if (bypass != null) {
            return CompletableFuture.completedFuture(bypass);
        }

        return onReadListener.onAsyncRead(
                        storageAdapter.fetchNode(readTransaction, storageTransform, layer,
                                nodeReference.getPrimaryKey()))
                .thenApply(node -> biMapFunction.apply(nodeReference, node));
    }

    /**
     * Asynchronously fetches neighborhood nodes and returns them as {@link NodeReferenceWithVector} instances,
     * which include the node's vector.
     * <p>
     * This method efficiently retrieves node data by first checking an in-memory {@code nodeCache}. If a node is not
     * in the cache, it is fetched from the {@link StorageAdapter}. Fetched nodes are then added to the cache to
     * optimize subsequent lookups. It also handles cases where the input {@code neighborReferences} may already
     * contain {@link NodeReferenceWithVector} instances, avoiding redundant work.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter to fetch nodes from if they are not in the cache
     * @param readTransaction the transaction context for database read operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer the graph layer from which to fetch the nodes
     * @param neighborReferences an iterable of references to the neighbor nodes to be fetched
     * @param nodeCache a map serving as an in-memory cache for nodes. This map will be populated with any
     * nodes fetched from storage.
     *
     * @return a {@link CompletableFuture} that, upon completion, will contain a list of
     * {@link NodeReferenceWithVector} objects for the specified neighbors
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithVector>>
            fetchNeighborhood(@Nonnull final StorageAdapter<N> storageAdapter,
                              @Nonnull final ReadTransaction readTransaction,
                              @Nonnull final AffineOperator storageTransform,
                              final int layer,
                              @Nonnull final Iterable<? extends NodeReference> neighborReferences,
                              @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchSomeNodesAndApply(storageAdapter, readTransaction, storageTransform, layer, neighborReferences,
                neighborReference -> {
                    if (neighborReference instanceof NodeReferenceWithVector) {
                        return (NodeReferenceWithVector)neighborReference;
                    }
                    final Node<N> neighborNode = nodeCache.get(neighborReference.getPrimaryKey());
                    if (neighborNode == null) {
                        return null;
                    }
                    return new NodeReferenceWithVector(neighborReference.getPrimaryKey(),
                            neighborNode.asCompactNode().getVector());
                },
                (neighborReference, neighborNode) -> {
                    nodeCache.put(neighborReference.getPrimaryKey(), neighborNode);
                    return new NodeReferenceWithVector(neighborReference.getPrimaryKey(),
                            neighborNode.asCompactNode().getVector());
                });
    }

    /**
     * Fetches a collection of nodes, attempting to retrieve them from a cache first before
     * accessing the underlying storage.
     * <p>
     * This method iterates through the provided {@code nodeReferences}. For each reference, it
     * first checks the {@code nodeCache}. If the corresponding {@link Node} is found, it is
     * used directly. If not, the node is fetched from the {@link StorageAdapter}. Any nodes
     * fetched from storage are then added to the {@code nodeCache} to optimize subsequent lookups.
     * The entire operation is performed asynchronously.
     *
     * @param <N> The type of the node reference, which must extend {@link NodeReference}.
     * @param storageAdapter The storage adapter used to fetch nodes from storage if they are not in the cache.
     * @param readTransaction The transaction context for the read operation.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer The layer from which to fetch the nodes.
     * @param nodeReferences An {@link Iterable} of {@link NodeReferenceWithDistance} objects identifying the nodes to
     * be fetched.
     * @param nodeCache A map used as a cache. It is checked for existing nodes and updated with any newly fetched
     * nodes.
     *
     * @return A {@link CompletableFuture} which will complete with a {@link List} of
     * {@link NodeReferenceAndNode} objects, pairing each requested reference with its corresponding node.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>>
            fetchSomeNodesIfNotCached(@Nonnull final StorageAdapter<N> storageAdapter,
                                      @Nonnull final ReadTransaction readTransaction,
                                      @Nonnull final AffineOperator storageTransform,
                                      final int layer,
                                      @Nonnull final Iterable<NodeReferenceWithDistance> nodeReferences,
                                      @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchSomeNodesAndApply(storageAdapter, readTransaction, storageTransform, layer, nodeReferences,
                nodeReference -> {
                    final Node<N> node = nodeCache.get(nodeReference.getPrimaryKey());
                    if (node == null) {
                        return null;
                    }
                    return new NodeReferenceAndNode<>(nodeReference, node);
                },
                (nodeReferenceWithDistance, node) -> {
                    nodeCache.put(nodeReferenceWithDistance.getPrimaryKey(), node);
                    return new NodeReferenceAndNode<>(nodeReferenceWithDistance, node);
                });
    }

    /**
     * Asynchronously fetches a collection of nodes from storage and applies a function to each.
     * <p>
     * For each {@link NodeReference} in the provided iterable, this method concurrently fetches the corresponding
     * {@code Node} using the given {@link StorageAdapter}. The logic delegates to
     * {@code fetchNodeIfNecessaryAndApply}, which determines whether a full node fetch is required.
     * If a node is fetched from storage, the {@code biMapFunction} is applied. If the fetch is bypassed
     * (e.g., because the reference itself contains sufficient information), the {@code fetchBypassFunction} is used
     * instead.
     *
     * @param <R> The type of the node references to be processed, extending {@link NodeReference}.
     * @param <N> The type of the key references within the nodes, extending {@link NodeReference}.
     * @param <U> The type of the result after applying one of the mapping functions.
     * @param storageAdapter The {@link StorageAdapter} used to fetch nodes from the underlying storage.
     * @param readTransaction The {@link ReadTransaction} context for the read operations.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer The layer index from which the nodes are being fetched.
     * @param nodeReferences An {@link Iterable} of {@link NodeReference}s for the nodes to be fetched and processed.
     * @param fetchBypassFunction The function to apply to a node reference when the actual node fetch is bypassed,
     * mapping the reference directly to a result of type {@code U}.
     * @param biMapFunction The function to apply when a node is successfully fetched, mapping the original
     * reference and the fetched {@link Node} to a result of type {@code U}.
     *
     * @return A {@link CompletableFuture} that, upon completion, will hold a {@link java.util.List} of results
     * of type {@code U}, corresponding to each processed node reference.
     */
    @Nonnull
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<List<U>>
            fetchSomeNodesAndApply(@Nonnull final StorageAdapter<N> storageAdapter,
                                   @Nonnull final ReadTransaction readTransaction,
                                   @Nonnull final AffineOperator storageTransform,
                                   final int layer,
                                   @Nonnull final Iterable<R> nodeReferences,
                                   @Nonnull final Function<R, U> fetchBypassFunction,
                                   @Nonnull final BiFunction<R, Node<N>, U> biMapFunction) {
        return forEach(nodeReferences,
                currentNeighborReference -> fetchNodeIfNecessaryAndApply(storageAdapter, readTransaction,
                        storageTransform, layer, currentNeighborReference, fetchBypassFunction, biMapFunction),
                MAX_CONCURRENT_NODE_READS,
                getExecutor());
    }

    /**
     * Asynchronously inserts a node reference and its corresponding vector into the index.
     * <p>
     * This is a convenience method that extracts the primary key and vector from the
     * provided {@link NodeReferenceWithVector} and delegates to the
     * {@link #insert(Transaction, Tuple, RealVector)} method.
     *
     * @param transaction the transaction context for the operation. Must not be {@code null}.
     * @param nodeReferenceWithVector a container object holding the primary key of the node
     * and its vector representation. Must not be {@code null}.
     *
     * @return a {@link CompletableFuture} that will complete when the insertion operation is finished.
     */
    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction,
                                          @Nonnull final NodeReferenceWithVector nodeReferenceWithVector) {
        return insert(transaction, nodeReferenceWithVector.getPrimaryKey(), nodeReferenceWithVector.getVector());
    }

    /**
     * Inserts a new vector with its associated primary key into the HNSW graph.
     * <p>
     * The method first determines a random layer for the new node, called the {@code insertionLayer}.
     * It then traverses the graph from the entry point downwards, greedily searching for the nearest
     * neighbors to the {@code newVector} at each layer. This search identifies the optimal
     * connection points for the new node.
     * <p>
     * Once the nearest neighbors are found, the new node is linked into the graph structure at all
     * layers up to its {@code insertionLayer}. Special handling is included for inserting the
     * first-ever node into the graph or when a new node's layer is higher than any existing node,
     * which updates the graph's entry point. All operations are performed asynchronously.
     *
     * @param transaction the {@link Transaction} context for all database operations
     * @param newPrimaryKey the unique {@link Tuple} primary key for the new node being inserted
     * @param newVector the {@link RealVector} data to be inserted into the graph
     *
     * @return a {@link CompletableFuture} that completes when the insertion operation is finished
     */
    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction, @Nonnull final Tuple newPrimaryKey,
                                          @Nonnull final RealVector newVector) {
        final int insertionLayer = insertionLayer();
        if (logger.isTraceEnabled()) {
            logger.trace("new node with key={} selected to be inserted into layer={}", newPrimaryKey, insertionLayer);
        }

        return StorageAdapter.fetchAccessInfo(getConfig(), transaction, getSubspace(), getOnReadListener())
                .thenCompose(accessInfo -> {
                    final AccessInfo currentAccessInfo;
                    final AffineOperator storageTransform = storageTransform(accessInfo);
                    final RealVector transformedNewVector = storageTransform.applyInvert(newVector);
                    final Quantizer quantizer = quantizer(accessInfo);
                    final Estimator estimator = quantizer.estimator();

                    if (accessInfo == null) {
                        // this is the first node
                        writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector,
                                insertionLayer, -1);
                        currentAccessInfo = new AccessInfo(
                                new EntryNodeReference(newPrimaryKey, transformedNewVector, insertionLayer),
                                -1L, null);
                        StorageAdapter.writeAccessInfo(transaction, getSubspace(), currentAccessInfo,
                                getOnWriteListener());
                        if (logger.isTraceEnabled()) {
                            logger.trace("written initial entry node reference with key={} on layer={}",
                                    newPrimaryKey, insertionLayer);
                        }
                    } else {
                        final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
                        final int lMax = entryNodeReference.getLayer();
                        if (insertionLayer > lMax) {
                            writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector,
                                    insertionLayer, lMax);
                            currentAccessInfo = accessInfo.withNewEntryNodeReference(
                                    new EntryNodeReference(newPrimaryKey, transformedNewVector,
                                            insertionLayer));
                            StorageAdapter.writeAccessInfo(transaction, getSubspace(), currentAccessInfo,
                                    getOnWriteListener());
                            if (logger.isTraceEnabled()) {
                                logger.trace("written higher entry node reference with key={} on layer={}",
                                        newPrimaryKey, insertionLayer);
                            }
                        } else {
                            currentAccessInfo = accessInfo;
                        }
                    }

                    if (accessInfo == null) {
                        return AsyncUtil.DONE;
                    }

                    final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
                    final int lMax = entryNodeReference.getLayer();
                    if (logger.isTraceEnabled()) {
                        logger.trace("entry node read with key {} at layer {}", entryNodeReference.getPrimaryKey(), lMax);
                    }

                    final NodeReferenceWithDistance initialNodeReference =
                            new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                    entryNodeReference.getVector(),
                                    estimator.distance(transformedNewVector, entryNodeReference.getVector()));
                    return forLoop(lMax, initialNodeReference,
                            layer -> layer > insertionLayer,
                            layer -> layer - 1,
                            (layer, previousNodeReference) -> {
                                final StorageAdapter<? extends NodeReference> storageAdapter = getStorageAdapterForLayer(layer);
                                return greedySearchLayer(storageAdapter, transaction, storageTransform,
                                        estimator, previousNodeReference, layer, transformedNewVector);
                            }, executor)
                            .thenCompose(nodeReference ->
                                    insertIntoLayers(transaction, storageTransform, quantizer, newPrimaryKey,
                                            transformedNewVector, nodeReference, lMax, insertionLayer))
                            .thenCompose(ignored -> {
                                if (getConfig().isUseRaBitQ() && !accessInfo.canUseRaBitQ()) {
                                    if (shouldSampleVector()) {
                                        StorageAdapter.appendSampledVector(transaction, getSubspace(),
                                                1, transformedNewVector, onWriteListener);
                                    }
                                    if (shouldMaintainStats()) {
                                        return StorageAdapter.consumeSampledVectors(transaction, getSubspace(),
                                                        50, onReadListener)
                                                .thenApply(sampledVectors -> {
                                                    RealVector partialVector = null;
                                                    int partialCount = 0;
                                                    for (final AggregatedVector sampledVector : sampledVectors) {
                                                        partialVector = partialVector == null
                                                                        ? sampledVector.getPartialVector()
                                                                        : partialVector.add(sampledVector.getPartialVector());
                                                        partialCount += sampledVector.getPartialCount();
                                                    }
                                                    if (partialCount > 0) {
                                                        StorageAdapter.appendSampledVector(transaction, getSubspace(),
                                                                partialCount, partialVector, onWriteListener);
                                                        if (logger.isTraceEnabled()) {
                                                            logger.trace("updated stats with numVectors={}, partialCount={}, partialVector={}",
                                                                    sampledVectors.size(), partialCount, partialVector);
                                                        }

                                                        if (partialCount >= getConfig().getStatsThreshold()) {
                                                            final long rotatorSeed = random.nextLong();
                                                            final FhtKacRotator rotator = new FhtKacRotator(rotatorSeed, getConfig().getNumDimensions(), 10);

                                                            final RealVector centroid =
                                                                    partialVector.multiply(1.0d / partialCount);
                                                            final RealVector transformedCentroid = rotator.applyInvert(centroid);

                                                            final var transformedEntryNodeVector =
                                                                    rotator.applyInvert(currentAccessInfo.getEntryNodeReference()
                                                                            .getVector()).subtract(transformedCentroid);

                                                            final AccessInfo newAccessInfo =
                                                                    new AccessInfo(currentAccessInfo.getEntryNodeReference().withVector(transformedEntryNodeVector),
                                                                            rotatorSeed, transformedCentroid);
                                                            StorageAdapter.writeAccessInfo(transaction, getSubspace(), newAccessInfo, onWriteListener);
                                                            StorageAdapter.removeAllSampledVectors(transaction, getSubspace());
                                                            if (logger.isTraceEnabled()) {
                                                                logger.trace("established rotatorSeed={}, centroid with count={}, centroid={}",
                                                                        rotatorSeed, partialCount, transformedCentroid);
                                                            }
                                                        }
                                                    }
                                                    return null;
                                                });
                                    }
                                }
                                return AsyncUtil.DONE;
                            });
                }).thenCompose(ignored -> AsyncUtil.DONE);
    }

    /**
     * Inserts a batch of nodes into the HNSW graph asynchronously.
     *
     * <p>This method orchestrates the batch insertion of nodes into the HNSW graph structure.
     * For each node in the input {@code batch}, it first assigns a random layer based on the configured
     * probability distribution. The batch is then sorted in descending order of these assigned layers to
     * ensure higher-layer nodes are processed first, which can optimize subsequent insertions by providing
     * better entry points.</p>
     *
     * <p>The insertion logic proceeds in two main asynchronous stages:
     * <ol>
     *   <li><b>Search Phase:</b> For each node to be inserted, the method concurrently performs a greedy search
     *   from the graph's main entry point down to the node's target layer. This identifies the nearest neighbors
     *   at each level, which will serve as entry points for the insertion phase.</li>
     *   <li><b>Insertion Phase:</b> The method then iterates through the nodes and inserts each one into the graph
     *   from its target layer downwards, connecting it to its nearest neighbors. If a node's assigned layer is
     *   higher than the current maximum layer of the graph, it becomes the new main entry point.</li>
     * </ol>
     * All underlying storage operations are performed within the context of the provided {@link Transaction}.</p>
     *
     * @param transaction the transaction to use for all storage operations; must not be {@code null}
     * @param batch a {@code List} of {@link NodeReferenceWithVector} objects to insert; must not be {@code null}
     *
     * @return a {@link CompletableFuture} that completes with {@code null} when the entire batch has been inserted
     */
    @Nonnull
    public CompletableFuture<Void> insertBatch(@Nonnull final Transaction transaction,
                                               @Nonnull List<NodeReferenceWithVector> batch) {
        // determine the layer each item should be inserted at
        final List<NodeReferenceWithLayer> batchWithLayers = Lists.newArrayListWithCapacity(batch.size());
        for (final NodeReferenceWithVector current : batch) {
            batchWithLayers.add(
                    new NodeReferenceWithLayer(current.getPrimaryKey(), current.getVector(), insertionLayer()));
        }
        // sort the layers in reverse order
        batchWithLayers.sort(Comparator.comparing(NodeReferenceWithLayer::getLayer).reversed());

        return StorageAdapter.fetchAccessInfo(getConfig(), transaction, getSubspace(), getOnReadListener())
                .thenCompose(accessInfo -> {
                    final int lMax =
                            accessInfo == null ? -1 : accessInfo.getEntryNodeReference().getLayer();

                    final AffineOperator storageTransform = storageTransform(accessInfo);
                    final Quantizer quantizer = quantizer(accessInfo);
                    final Estimator estimator = quantizer.estimator();

                    return forEach(batchWithLayers,
                            item -> {
                                if (lMax == -1) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();

                                final RealVector itemVector = item.getVector();
                                final RealVector transformedItemVector = storageTransform.applyInvert(itemVector);

                                final int itemL = item.getLayer();

                                final NodeReferenceWithDistance initialNodeReference =
                                        new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                                entryNodeReference.getVector(),
                                                estimator.distance(transformedItemVector, entryNodeReference.getVector()));

                                return forLoop(lMax, initialNodeReference,
                                        layer -> layer > itemL,
                                        layer -> layer - 1,
                                        (layer, previousNodeReference) -> {
                                            final StorageAdapter<? extends NodeReference> storageAdapter = getStorageAdapterForLayer(layer);
                                            return greedySearchLayer(storageAdapter, transaction, storageTransform,
                                                    estimator, previousNodeReference, layer, transformedItemVector);
                                        }, executor);
                            }, MAX_CONCURRENT_SEARCHES, getExecutor())
                            .thenCompose(searchEntryReferences ->
                                    forLoop(0, accessInfo == null ? null : accessInfo.getEntryNodeReference(),
                                            index -> index < batchWithLayers.size(),
                                            index -> index + 1,
                                            (index, currentEntryNodeReference) -> {
                                                final NodeReferenceWithLayer item = batchWithLayers.get(index);
                                                final Tuple itemPrimaryKey = item.getPrimaryKey();
                                                final RealVector itemVector = item.getVector();
                                                final int itemL = item.getLayer();

                                                final EntryNodeReference newEntryNodeReference;
                                                final int currentLMax;

                                                if (accessInfo == null) {
                                                    // this is the first node
                                                    writeLonelyNodes(quantizer, transaction, itemPrimaryKey, itemVector, itemL, -1);
                                                    newEntryNodeReference =
                                                            new EntryNodeReference(itemPrimaryKey, itemVector, itemL);
                                                    StorageAdapter.writeAccessInfo(transaction, getSubspace(),
                                                            new AccessInfo(newEntryNodeReference, -1L, null), getOnWriteListener());
                                                    if (logger.isTraceEnabled()) {
                                                        logger.trace("written initial entry node reference for batch with key={} on layer={}", itemPrimaryKey, itemL);
                                                    }

                                                    return CompletableFuture.completedFuture(newEntryNodeReference);
                                                } else {
                                                    currentLMax = currentEntryNodeReference.getLayer();
                                                    if (itemL > currentLMax) {
                                                        writeLonelyNodes(quantizer, transaction, itemPrimaryKey, itemVector, itemL, lMax);
                                                        newEntryNodeReference =
                                                                new EntryNodeReference(itemPrimaryKey, itemVector, itemL);
                                                        StorageAdapter.writeAccessInfo(transaction, getSubspace(),
                                                                accessInfo.withNewEntryNodeReference(newEntryNodeReference),
                                                                getOnWriteListener());
                                                        if (logger.isTraceEnabled()) {
                                                            logger.trace("written higher entry node reference for batch with key={} on layer={}", itemPrimaryKey, itemL);
                                                        }
                                                    } else {
                                                        // entry node stays the same
                                                        newEntryNodeReference = accessInfo.getEntryNodeReference();
                                                    }
                                                }

                                                if (logger.isTraceEnabled()) {
                                                    logger.trace("entry node read for batch with key {} at layer {}",
                                                            currentEntryNodeReference.getPrimaryKey(), currentLMax);
                                                }

                                                final var currentSearchEntry =
                                                        searchEntryReferences.get(index);

                                                return insertIntoLayers(transaction, storageTransform, quantizer,
                                                        itemPrimaryKey, itemVector, currentSearchEntry, lMax, itemL)
                                                        .thenApply(ignored -> newEntryNodeReference);
                                            }, getExecutor()));
                }).thenCompose(ignored -> AsyncUtil.DONE);
    }

    /**
     * Inserts a new vector into the HNSW graph across multiple layers, starting from a given entry point.
     * <p>
     * This method implements the second phase of the HNSW insertion algorithm. It begins at a starting layer, which is
     * the minimum of the graph's maximum layer ({@code lMax}) and the new node's randomly assigned
     * {@code insertionLayer}. It then iterates downwards to layer 0. In each layer, it invokes
     * {@link #insertIntoLayer(StorageAdapter, Transaction, AffineOperator, Quantizer, List, int, Tuple, RealVector)} to
     * perform the search and connect the new node. The set of nearest neighbors found at layer {@code L} serves as the
     * entry points for the search at layer {@code L-1}.
     * </p>
     *
     * @param transaction the transaction to use for database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     * storage space that is currently being used
     * @param quantizer the quantizer to be used for this insert
     * @param newPrimaryKey the primary key of the new node being inserted
     * @param newVector the vector data of the new node
     * @param nodeReference the initial entry point for the search, typically the nearest neighbor found in the highest
     * layer
     * @param lMax the maximum layer number in the HNSW graph
     * @param insertionLayer the randomly determined layer for the new node. The node will be inserted into all layers
     * from this layer down to 0.
     *
     * @return a {@link CompletableFuture} that completes when the new node has been successfully inserted into all
     * its designated layers
     */
    @Nonnull
    private CompletableFuture<Void> insertIntoLayers(@Nonnull final Transaction transaction,
                                                     @Nonnull final AffineOperator storageTransform,
                                                     @Nonnull final Quantizer quantizer,
                                                     @Nonnull final Tuple newPrimaryKey,
                                                     @Nonnull final RealVector newVector,
                                                     @Nonnull final NodeReferenceWithDistance nodeReference,
                                                     final int lMax,
                                                     final int insertionLayer) {
        if (logger.isTraceEnabled()) {
            logger.trace("nearest entry point at lMax={} is at key={}", lMax, nodeReference.getPrimaryKey());
        }
        return MoreAsyncUtil.<List<NodeReferenceWithDistance>>forLoop(Math.min(lMax, insertionLayer), ImmutableList.of(nodeReference),
                layer -> layer >= 0,
                layer -> layer - 1,
                (layer, previousNodeReferences) -> {
                    final StorageAdapter<? extends NodeReference> storageAdapter = getStorageAdapterForLayer(layer);
                    return insertIntoLayer(storageAdapter, transaction, storageTransform, quantizer,
                            previousNodeReferences, layer, newPrimaryKey, newVector);
                }, executor).thenCompose(ignored -> AsyncUtil.DONE);
    }

    /**
     * Inserts a new node into a specified layer of the HNSW graph.
     * <p>
     * This method orchestrates the complete insertion process for a single layer. It begins by performing a search
     * within the given layer, starting from the provided {@code nearestNeighbors} as entry points, to find a set of
     * candidate neighbors for the new node. From this candidate set, it selects the best connections based on the
     * graph's parameters (M).
     * </p>
     * <p>
     * After selecting the neighbors, it creates the new node and links it to them. It then reciprocally updates
     * the selected neighbors to link back to the new node. If adding this new link causes a neighbor to exceed its
     * maximum allowed connections, its connections are pruned. All changes, including the new node and the updated
     * neighbors, are persisted to storage within the given transaction.
     * </p>
     * <p>
     * The operation is asynchronous and returns a {@link CompletableFuture}. The future completes with the list of
     * nodes found during the initial search phase, which are then used as the entry points for insertion into the
     * next lower layer.
     * </p>
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter for reading from and writing to the graph
     * @param transaction the transaction context for the database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param quantizer the quantizer for this insert
     * @param nearestNeighbors the list of nearest neighbors from the layer above, used as entry points for the search
     * in this layer
     * @param layer the layer number to insert the new node into
     * @param newPrimaryKey the primary key of the new node to be inserted
     * @param newVector the vector associated with the new node
     *
     * @return a {@code CompletableFuture} that completes with a list of the nearest neighbors found during the
     * initial search phase. This list serves as the entry point for insertion into the next lower layer
     * (i.e., {@code layer - 1}).
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithDistance>>
            insertIntoLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final Transaction transaction,
                            @Nonnull final AffineOperator storageTransform,
                            @Nonnull final Quantizer quantizer,
                            @Nonnull final List<NodeReferenceWithDistance> nearestNeighbors,
                            final int layer,
                            @Nonnull final Tuple newPrimaryKey,
                            @Nonnull final RealVector newVector) {
        if (logger.isTraceEnabled()) {
            logger.trace("begin insert key={} at layer={}", newPrimaryKey, layer);
        }
        final Map<Tuple, Node<N>> nodeCache = Maps.newConcurrentMap();
        final Estimator estimator = quantizer.estimator();

        return searchLayer(storageAdapter, transaction, storageTransform, estimator,
                nearestNeighbors, layer, config.getEfConstruction(), nodeCache, newVector)
                .thenCompose(searchResult -> {
                    final List<NodeReferenceWithDistance> references = NodeReferenceAndNode.getReferences(searchResult);

                    return selectNeighbors(storageAdapter, transaction, storageTransform, estimator, searchResult,
                            layer, getConfig().getM(), getConfig().isExtendCandidates(), nodeCache, newVector)
                            .thenCompose(selectedNeighbors -> {
                                final NodeFactory<N> nodeFactory = storageAdapter.getNodeFactory();

                                // TODO Quantize the neighbors. (if in inlining mode and the neighbors were fetched as
                                //      regular vectors)

                                final Node<N> newNode =
                                        nodeFactory.create(newPrimaryKey, newVector,
                                                NodeReferenceAndNode.getReferences(selectedNeighbors));

                                final NeighborsChangeSet<N> newNodeChangeSet =
                                        new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                                                newNode.getNeighbors());

                                storageAdapter.writeNode(transaction, quantizer, newNode, layer, newNodeChangeSet);

                                // create change sets for each selected neighbor and insert new node into them
                                final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap =
                                        Maps.newLinkedHashMap();
                                for (final NodeReferenceAndNode<N> selectedNeighbor : selectedNeighbors) {
                                    final NeighborsChangeSet<N> baseSet =
                                            new BaseNeighborsChangeSet<>(selectedNeighbor.getNode().getNeighbors());
                                    final NeighborsChangeSet<N> insertSet =
                                            new InsertNeighborsChangeSet<>(baseSet, ImmutableList.of(newNode.getSelfReference(newVector)));
                                    neighborChangeSetMap.put(selectedNeighbor.getNode().getPrimaryKey(),
                                            insertSet);
                                }

                                final int currentMMax = layer == 0 ? getConfig().getMMax0() : getConfig().getMMax();
                                return forEach(selectedNeighbors,
                                                selectedNeighbor -> {
                                                    final Node<N> selectedNeighborNode = selectedNeighbor.getNode();
                                                    final NeighborsChangeSet<N> changeSet =
                                                            Objects.requireNonNull(neighborChangeSetMap.get(selectedNeighborNode.getPrimaryKey()));
                                                    return pruneNeighborsIfNecessary(storageAdapter, transaction,
                                                            storageTransform, estimator, selectedNeighbor, layer,
                                                            currentMMax, changeSet, nodeCache)
                                                            .thenApply(nodeReferencesAndNodes -> {
                                                                if (nodeReferencesAndNodes == null) {
                                                                    return changeSet;
                                                                }
                                                                return resolveChangeSetFromNewNeighbors(changeSet, nodeReferencesAndNodes);
                                                            });
                                                }, MAX_CONCURRENT_NEIGHBOR_FETCHES, getExecutor())
                                        .thenApply(changeSets -> {
                                            for (int i = 0; i < selectedNeighbors.size(); i++) {
                                                final NodeReferenceAndNode<N> selectedNeighbor = selectedNeighbors.get(i);
                                                final NeighborsChangeSet<N> changeSet = changeSets.get(i);
                                                storageAdapter.writeNode(transaction, quantizer,
                                                        selectedNeighbor.getNode(), layer, changeSet);
                                            }
                                            return ImmutableList.copyOf(references);
                                        });
                            });
                }).thenApply(nodeReferencesWithDistances -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("end insert key={} at layer={}", newPrimaryKey, layer);
                    }
                    return nodeReferencesWithDistances;
                });
    }

    /**
     * Calculates the delta between a current set of neighbors and a new set, producing a
     * {@link NeighborsChangeSet} that represents the required insertions and deletions.
     * <p>
     * This method compares the neighbors present in the initial {@code beforeChangeSet} with
     * the provided {@code afterNeighbors}. It identifies which neighbors from the "before" state
     * are missing in the "after" state (to be deleted) and which new neighbors are present in the
     * "after" state but not in the "before" state (to be inserted). It then constructs a new
     * {@code NeighborsChangeSet} by wrapping the original one with {@link DeleteNeighborsChangeSet}
     * and {@link InsertNeighborsChangeSet} as needed.
     *
     * @param <N> the type of the node reference, which must extend {@link NodeReference}
     * @param beforeChangeSet the change set representing the state of neighbors before the update.
     * This is used as the base for calculating changes. Must not be null.
     * @param afterNeighbors an iterable collection of the desired neighbors after the update.
     * Must not be null.
     *
     * @return a new {@code NeighborsChangeSet} that includes the necessary deletion and insertion
     * operations to transform the neighbors from the "before" state to the "after" state.
     */
    private <N extends NodeReference> NeighborsChangeSet<N>
            resolveChangeSetFromNewNeighbors(@Nonnull final NeighborsChangeSet<N> beforeChangeSet,
                                             @Nonnull final Iterable<NodeReferenceAndNode<N>> afterNeighbors) {
        final Map<Tuple, N> beforeNeighborsMap = Maps.newLinkedHashMap();
        for (final N n : beforeChangeSet.merge()) {
            beforeNeighborsMap.put(n.getPrimaryKey(), n);
        }

        final Map<Tuple, N> afterNeighborsMap = Maps.newLinkedHashMap();
        for (final NodeReferenceAndNode<N> nodeReferenceAndNode : afterNeighbors) {
            final NodeReferenceWithDistance nodeReferenceWithDistance = nodeReferenceAndNode.getNodeReferenceWithDistance();

            afterNeighborsMap.put(nodeReferenceWithDistance.getPrimaryKey(),
                    nodeReferenceAndNode.getNode().getSelfReference(nodeReferenceWithDistance.getVector()));
        }

        final ImmutableList.Builder<Tuple> toBeDeletedBuilder = ImmutableList.builder();
        for (final Map.Entry<Tuple, N> beforeNeighborEntry : beforeNeighborsMap.entrySet()) {
            if (!afterNeighborsMap.containsKey(beforeNeighborEntry.getKey())) {
                toBeDeletedBuilder.add(beforeNeighborEntry.getValue().getPrimaryKey());
            }
        }
        final List<Tuple> toBeDeleted = toBeDeletedBuilder.build();

        final ImmutableList.Builder<N> toBeInsertedBuilder = ImmutableList.builder();
        for (final Map.Entry<Tuple, N> afterNeighborEntry : afterNeighborsMap.entrySet()) {
            if (!beforeNeighborsMap.containsKey(afterNeighborEntry.getKey())) {
                toBeInsertedBuilder.add(afterNeighborEntry.getValue());
            }
        }
        final List<N> toBeInserted = toBeInsertedBuilder.build();

        NeighborsChangeSet<N> changeSet = beforeChangeSet;

        if (!toBeDeleted.isEmpty()) {
            changeSet = new DeleteNeighborsChangeSet<>(changeSet, toBeDeleted);
        }
        if (!toBeInserted.isEmpty()) {
            changeSet = new InsertNeighborsChangeSet<>(changeSet, toBeInserted);
        }
        return changeSet;
    }

    /**
     * Prunes the neighborhood of a given node if its number of connections exceeds the maximum allowed ({@code mMax}).
     * <p>
     * This is a maintenance operation for the HNSW graph. When new nodes are added, an existing node's neighborhood
     * might temporarily grow beyond its limit. This method identifies such cases and trims the neighborhood back down
     * to the {@code mMax} best connections, based on the configured distance metric. If the neighborhood size is
     * already within the limit, this method does nothing.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter to fetch nodes from the database
     * @param transaction the transaction context for database operations
     * @param estimator an estimator to estimate distances
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param selectedNeighbor the node whose neighborhood is being considered for pruning
     * @param layer the graph layer on which the operation is performed
     * @param mMax the maximum number of neighbors a node is allowed to have on this layer
     * @param neighborChangeSet a set of pending changes to the neighborhood that must be included in the pruning
     * calculation
     * @param nodeCache a cache of nodes to avoid redundant database fetches
     *
     * @return a {@link CompletableFuture} which completes with a list of the newly selected neighbors for the pruned node.
     * If no pruning was necessary, it completes with {@code null}.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>>
            pruneNeighborsIfNecessary(@Nonnull final StorageAdapter<N> storageAdapter,
                                      @Nonnull final Transaction transaction,
                                      @Nonnull final AffineOperator storageTransform,
                                      @Nonnull final Estimator estimator,
                                      @Nonnull final NodeReferenceAndNode<N> selectedNeighbor,
                                      int layer,
                                      int mMax,
                                      @Nonnull final NeighborsChangeSet<N> neighborChangeSet,
                                      @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        final Node<N> selectedNeighborNode = selectedNeighbor.getNode();
        if (selectedNeighborNode.getNeighbors().size() < mMax) {
            return CompletableFuture.completedFuture(null);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("pruning neighborhood of key={} which has numNeighbors={} out of mMax={}",
                        selectedNeighborNode.getPrimaryKey(), selectedNeighborNode.getNeighbors().size(), mMax);
            }
            return fetchNeighborhood(storageAdapter, transaction, storageTransform, layer, neighborChangeSet.merge(), nodeCache)
                    .thenCompose(nodeReferenceWithVectors -> {
                        final ImmutableList.Builder<NodeReferenceWithDistance> nodeReferencesWithDistancesBuilder =
                                ImmutableList.builder();
                        for (final NodeReferenceWithVector nodeReferenceWithVector : nodeReferenceWithVectors) {
                            final var vector = nodeReferenceWithVector.getVector();
                            final double distance =
                                    estimator.distance(vector,
                                            selectedNeighbor.getNodeReferenceWithDistance().getVector());
                            nodeReferencesWithDistancesBuilder.add(
                                    new NodeReferenceWithDistance(nodeReferenceWithVector.getPrimaryKey(),
                                            vector, distance));
                        }
                        return fetchSomeNodesIfNotCached(storageAdapter, transaction, storageTransform, layer,
                                nodeReferencesWithDistancesBuilder.build(), nodeCache);
                    })
                    .thenCompose(nodeReferencesAndNodes ->
                            selectNeighbors(storageAdapter, transaction, storageTransform, estimator,
                                    nodeReferencesAndNodes, layer,
                                    mMax, false, nodeCache,
                                    selectedNeighbor.getNodeReferenceWithDistance().getVector()));
        }
    }

    /**
     * Selects the {@code m} best neighbors for a new node from a set of candidates using the HNSW selection heuristic.
     * <p>
     * This method implements the core logic for neighbor selection within a layer of the HNSW graph. It starts with an
     * initial set of candidates ({@code nearestNeighbors}), which can be optionally extended by fetching their own
     * neighbors.
     * It then iteratively refines this set using a greedy best-first search.
     * <p>
     * The selection heuristic ensures diversity among neighbors. A candidate is added to the result set only if it is
     * closer to the query {@code vector} than to any node already in the result set. This prevents selecting neighbors
     * that are clustered together. If the {@code keepPrunedConnections} configuration is enabled, candidates that are
     * pruned by this heuristic are kept and may be added at the end if the result set is not yet full.
     * <p>
     * The process is asynchronous and returns a {@link CompletableFuture} that will eventually contain the list of
     * selected neighbors with their full node data.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter to fetch nodes and their neighbors
     * @param readTransaction the transaction for performing database reads
     * @param estimator the estimator in use
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param nearestNeighbors the initial pool of candidate neighbors, typically from a search in a higher layer
     * @param layer the layer in the HNSW graph where the selection is being performed
     * @param m the maximum number of neighbors to select
     * @param isExtendCandidates a flag indicating whether to extend the initial candidate pool by fetching the
     * neighbors of the {@code nearestNeighbors}
     * @param nodeCache a cache of nodes to avoid redundant storage lookups
     * @param vector the query vector for which neighbors are being selected
     *
     * @return a {@link CompletableFuture} which will complete with a list of the selected neighbors,
     * each represented as a {@link NodeReferenceAndNode}
     */
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>>
            selectNeighbors(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final ReadTransaction readTransaction,
                            @Nonnull final AffineOperator storageTransform,
                            @Nonnull final Estimator estimator,
                            @Nonnull final Iterable<NodeReferenceAndNode<N>> nearestNeighbors,
                            final int layer,
                            final int m,
                            final boolean isExtendCandidates,
                            @Nonnull final Map<Tuple, Node<N>> nodeCache,
                            @Nonnull final RealVector vector) {
        return extendCandidatesIfNecessary(storageAdapter, readTransaction, storageTransform, estimator,
                nearestNeighbors, layer, isExtendCandidates, nodeCache, vector)
                .thenApply(extendedCandidates -> {
                    final List<NodeReferenceWithDistance> selected = Lists.newArrayListWithExpectedSize(m);
                    final Queue<NodeReferenceWithDistance> candidates =
                            new PriorityBlockingQueue<>(config.getM(),
                                    Comparator.comparing(NodeReferenceWithDistance::getDistance));
                    candidates.addAll(extendedCandidates);
                    final Queue<NodeReferenceWithDistance> discardedCandidates =
                            getConfig().isKeepPrunedConnections()
                            ? new PriorityBlockingQueue<>(config.getM(),
                                    Comparator.comparing(NodeReferenceWithDistance::getDistance))
                            : null;

                    while (!candidates.isEmpty() && selected.size() < m) {
                        final NodeReferenceWithDistance nearestCandidate = candidates.poll();
                        boolean shouldSelect = true;
                        for (final NodeReferenceWithDistance alreadySelected : selected) {
                            if (estimator.distance(nearestCandidate.getVector(),
                                    alreadySelected.getVector()) < nearestCandidate.getDistance()) {
                                shouldSelect = false;
                                break;
                            }
                        }
                        if (shouldSelect) {
                            selected.add(nearestCandidate);
                        } else if (discardedCandidates != null) {
                            discardedCandidates.add(nearestCandidate);
                        }
                    }

                    if (discardedCandidates != null) { // isKeepPrunedConnections is set to true
                        while (!discardedCandidates.isEmpty() && selected.size() < m) {
                            selected.add(discardedCandidates.poll());
                        }
                    }

                    return ImmutableList.copyOf(selected);
                }).thenCompose(selectedNeighbors ->
                        fetchSomeNodesIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
                                selectedNeighbors, nodeCache))
                .thenApply(selectedNeighbors -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("selected neighbors={}",
                                selectedNeighbors.stream()
                                        .map(selectedNeighbor ->
                                                "(primaryKey=" + selectedNeighbor.getNodeReferenceWithDistance().getPrimaryKey() +
                                                        ",distance=" + selectedNeighbor.getNodeReferenceWithDistance().getDistance() + ")")
                                        .collect(Collectors.joining(",")));
                    }
                    return selectedNeighbors;
                });
    }

    /**
     * Conditionally extends a set of candidate nodes by fetching and evaluating their neighbors.
     * <p>
     * If {@code isExtendCandidates} is {@code true}, this method gathers the neighbors of the provided
     * {@code candidates}, fetches their full node data, and calculates their distance to the given
     * {@code vector}. The resulting list will contain both the original candidates and their newly
     * evaluated neighbors.
     * <p>
     * If {@code isExtendCandidates} is {@code false}, the method simply returns a list containing
     * only the original candidates. This operation is asynchronous and returns a {@link CompletableFuture}.
     *
     * @param <N> the type of the {@link NodeReference}
     * @param storageAdapter the {@link StorageAdapter} used to access node data from storage
     * @param readTransaction the active {@link ReadTransaction} for database access
     * @param estimator the estimator
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param candidates an {@link Iterable} of initial candidate nodes, which have already been evaluated
     * @param layer the graph layer from which to fetch nodes
     * @param isExtendCandidates a boolean flag; if {@code true}, the candidate set is extended with neighbors
     * @param nodeCache a cache mapping primary keys to {@link Node} objects to avoid redundant fetches
     * @param vector the query vector used to calculate distances for any new neighbor nodes
     *
     * @return a {@link CompletableFuture} which will complete with a list of {@link NodeReferenceWithDistance},
     * containing the original candidates and potentially their neighbors
     */
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithDistance>>
            extendCandidatesIfNecessary(@Nonnull final StorageAdapter<N> storageAdapter,
                                        @Nonnull final ReadTransaction readTransaction,
                                        @Nonnull final AffineOperator storageTransform,
                                        @Nonnull final Estimator estimator,
                                        @Nonnull final Iterable<NodeReferenceAndNode<N>> candidates,
                                        int layer,
                                        boolean isExtendCandidates,
                                        @Nonnull final Map<Tuple, Node<N>> nodeCache,
                                        @Nonnull final RealVector vector) {
        if (isExtendCandidates) {
            final Set<Tuple> candidatesSeen = Sets.newConcurrentHashSet();
            for (final NodeReferenceAndNode<N> candidate : candidates) {
                candidatesSeen.add(candidate.getNode().getPrimaryKey());
            }

            final ImmutableList.Builder<N> neighborsOfCandidatesBuilder = ImmutableList.builder();
            for (final NodeReferenceAndNode<N> candidate : candidates) {
                for (final N neighbor : candidate.getNode().getNeighbors()) {
                    final Tuple neighborPrimaryKey = neighbor.getPrimaryKey();
                    if (!candidatesSeen.contains(neighborPrimaryKey)) {
                        candidatesSeen.add(neighborPrimaryKey);
                        neighborsOfCandidatesBuilder.add(neighbor);
                    }
                }
            }

            final Iterable<N> neighborsOfCandidates = neighborsOfCandidatesBuilder.build();

            return fetchNeighborhood(storageAdapter, readTransaction, storageTransform, layer,
                    neighborsOfCandidates, nodeCache)
                    .thenApply(withVectors -> {
                        final ImmutableList.Builder<NodeReferenceWithDistance> extendedCandidatesBuilder =
                                ImmutableList.builder();
                        for (final NodeReferenceAndNode<N> candidate : candidates) {
                            extendedCandidatesBuilder.add(candidate.getNodeReferenceWithDistance());
                        }

                        for (final NodeReferenceWithVector withVector : withVectors) {
                            final double distance = estimator.distance(vector, withVector.getVector());
                            extendedCandidatesBuilder.add(new NodeReferenceWithDistance(withVector.getPrimaryKey(),
                                    withVector.getVector(), distance));
                        }
                        return extendedCandidatesBuilder.build();
                    });
        } else {
            final ImmutableList.Builder<NodeReferenceWithDistance> resultBuilder = ImmutableList.builder();
            for (final NodeReferenceAndNode<N> candidate : candidates) {
                resultBuilder.add(candidate.getNodeReferenceWithDistance());
            }

            return CompletableFuture.completedFuture(resultBuilder.build());
        }
    }

    /**
     * Writes lonely nodes for a given key across a specified range of layers.
     * <p>
     * A "lonely node" is a node in the layered structure that does not have a right
     * sibling. This method iterates downwards from the {@code highestLayerInclusive}
     * to the {@code lowestLayerExclusive}. For each layer in this range, it
     * retrieves the appropriate {@link StorageAdapter} and calls
     * {@link #writeLonelyNodeOnLayer} to persist the node's information.
     *
     * @param quantizer the quantizer
     * @param transaction the transaction to use for writing to the database
     * @param primaryKey the primary key of the record for which lonely nodes are being written
     * @param vector the search path vector that was followed to find this key
     * @param highestLayerInclusive the highest layer (inclusive) to begin writing lonely nodes on
     * @param lowestLayerExclusive the lowest layer (exclusive) at which to stop writing lonely nodes
     */
    private void writeLonelyNodes(@Nonnull final Quantizer quantizer,
                                  @Nonnull final Transaction transaction,
                                  @Nonnull final Tuple primaryKey,
                                  @Nonnull final RealVector vector,
                                  final int highestLayerInclusive,
                                  final int lowestLayerExclusive) {
        for (int layer = highestLayerInclusive; layer > lowestLayerExclusive; layer --) {
            final StorageAdapter<?> storageAdapter = getStorageAdapterForLayer(layer);
            writeLonelyNodeOnLayer(quantizer, storageAdapter, transaction, layer, primaryKey, vector);
        }
    }

    /**
     * Writes a new, isolated ('lonely') node to a specified layer within the graph.
     * <p>
     * This method uses the provided {@link StorageAdapter} to create a new node with the
     * given primary key and vector but with an empty set of neighbors. The write
     * operation is performed as part of the given {@link Transaction}. This is typically
     * used to insert the very first node into an empty graph layer.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param quantizer the quantizer
     * @param storageAdapter the {@link StorageAdapter} used to access the data store and create nodes; must not be null
     * @param transaction the {@link Transaction} context for the write operation; must not be null
     * @param layer the layer index where the new node will be written
     * @param primaryKey the primary key for the new node; must not be null
     * @param vector the vector data for the new node; must not be null
     */
    private <N extends NodeReference> void writeLonelyNodeOnLayer(@Nonnull final Quantizer quantizer,
                                                                  @Nonnull final StorageAdapter<N> storageAdapter,
                                                                  @Nonnull final Transaction transaction,
                                                                  final int layer,
                                                                  @Nonnull final Tuple primaryKey,
                                                                  @Nonnull final RealVector vector) {
        storageAdapter.writeNode(transaction, quantizer,
                storageAdapter.getNodeFactory()
                        .create(primaryKey, vector, ImmutableList.of()), layer,
                new BaseNeighborsChangeSet<>(ImmutableList.of()));
        if (logger.isTraceEnabled()) {
            logger.trace("written lonely node at key={} on layer={}", primaryKey, layer);
        }
    }

    /**
     * Scans all nodes within a given layer of the database.
     * <p>
     * The scan is performed transactionally in batches to avoid loading the entire layer
     * into memory at once. Each discovered node is passed to the provided {@link Consumer}
     * for processing. The operation continues fetching batches until all nodes in the
     * specified layer have been processed.
     *
     * @param db the non-null {@link Database} instance to run the scan against.
     * @param layer the specific layer index to scan.
     * @param batchSize the number of nodes to retrieve and process in each batch.
     * @param nodeConsumer the non-null {@link Consumer} that will accept each {@link Node}
     * found in the layer.
     */
    public void scanLayer(@Nonnull final Database db,
                          final int layer,
                          final int batchSize,
                          @Nonnull final Consumer<Node<? extends NodeReference>> nodeConsumer) {
        final StorageAdapter<? extends NodeReference> storageAdapter = getStorageAdapterForLayer(layer);
        final AtomicReference<Tuple> lastPrimaryKeyAtomic = new AtomicReference<>();
        Tuple newPrimaryKey;
        do {
            final Tuple lastPrimaryKey = lastPrimaryKeyAtomic.get();
            lastPrimaryKeyAtomic.set(null);
            newPrimaryKey = db.run(tr -> {
                Streams.stream(storageAdapter.scanLayer(tr, layer, lastPrimaryKey, batchSize))
                        .forEach(node -> {
                            nodeConsumer.accept(Objects.requireNonNull(node));
                            lastPrimaryKeyAtomic.set(node.getPrimaryKey());
                        });
                return lastPrimaryKeyAtomic.get();
            }, executor);
        } while (newPrimaryKey != null);
    }

    /**
     * Gets the appropriate storage adapter for a given layer.
     * <p>
     * This method selects a {@link StorageAdapter} implementation based on the layer number.
     * The logic is intended to use an {@code InliningStorageAdapter} for layers greater
     * than 0 and a {@code CompactStorageAdapter} for layer 0. However, the switch to
     * the inlining adapter is currently disabled with a hardcoded {@code false},
     * so this method will always return a {@code CompactStorageAdapter}.
     *
     * @param layer the layer number for which to get the storage adapter; currently unused
     *
     * @return a non-null {@link StorageAdapter} instance, which will always be a
     * {@link CompactStorageAdapter} in the current implementation
     */
    @Nonnull
    private StorageAdapter<? extends NodeReference> getStorageAdapterForLayer(final int layer) {
        return config.isUseInlining() && layer > 0
               ? new InliningStorageAdapter(getConfig(), InliningNode.factory(), getSubspace(), getOnWriteListener(),
                getOnReadListener())
               : new CompactStorageAdapter(getConfig(), CompactNode.factory(), getSubspace(), getOnWriteListener(),
                getOnReadListener());
    }

    /**
     * Calculates a random layer for a new element to be inserted.
     * <p>
     * The layer is selected according to a logarithmic distribution, which ensures that
     * the probability of choosing a higher layer decreases exponentially. This is
     * achieved by applying the inverse transform sampling method. The specific formula
     * is {@code floor(-ln(u) * lambda)}, where {@code u} is a uniform random
     * number and {@code lambda} is a normalization factor derived from a system
     * configuration parameter {@code M}.
     *
     * @return a non-negative integer representing the randomly selected layer.
     */
    private int insertionLayer() {
        double lambda = 1.0 / Math.log(getConfig().getM());
        double u = 1.0 - random.nextDouble();  // Avoid log(0)
        return (int) Math.floor(-Math.log(u) * lambda);
    }

    private boolean shouldSampleVector() {
        return random.nextDouble() < getConfig().getSampleVectorStatsProbability();
    }

    private boolean shouldMaintainStats() {
        return random.nextDouble() < getConfig().getMaintainStatsProbability();
    }

    private static class NodeReferenceWithLayer extends NodeReferenceWithVector {
        private final int layer;

        public NodeReferenceWithLayer(@Nonnull final Tuple primaryKey, @Nonnull final RealVector vector,
                                      final int layer) {
            super(primaryKey, vector);
            this.layer = layer;
        }

        public int getLayer() {
            return layer;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof NodeReferenceWithLayer)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            return layer == ((NodeReferenceWithLayer)o).layer;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), layer);
        }
    }
}
