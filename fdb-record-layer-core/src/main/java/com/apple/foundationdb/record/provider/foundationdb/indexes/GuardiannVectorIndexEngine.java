/*
 * GuardiannVectorIndexEngine.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.TimedAsyncIterable;
import com.apple.foundationdb.async.guardiann.Config;
import com.apple.foundationdb.async.guardiann.Config.ConfigBuilder;
import com.apple.foundationdb.async.guardiann.Guardiann;
import com.apple.foundationdb.async.guardiann.OnReadListener;
import com.apple.foundationdb.async.guardiann.OnWriteListener;
import com.apple.foundationdb.async.guardiann.SearchConfig;
import com.apple.foundationdb.async.guardiann.TaskKind;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.allowChange;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.applyBoolean;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.applyDouble;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.applyInteger;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.disallowChange;

/**
 * The {@link VectorIndexEngine} backed by a {@link Guardiann} clustered vector structure. Like the HNSW engine it holds
 * a parsed {@link Config} and constructs a fresh {@code Guardiann} bound to the partition subspace for each operation;
 * Guardiann runs its deferred maintenance (split/merge/reassign/collapse) piggy-backed on inserts and deletes, so
 * per-call construction is the intended usage.
 * <p>
 * Two things differ from HNSW. First, a Guardiann {@code delete} needs the vector (not just the primary key) to locate
 * the vector's cluster references, which is why the {@link VectorIndexEngine} delete contract always carries the vector.
 * Second, a Guardiann search requires a per-query {@link SearchConfig}, which is built from the per-query scan options
 * ({@link VectorIndexScanOptions}'s {@code GUARDIANN_*} knobs), each knob falling back to its {@link SearchConfig}
 * default when the option is absent; the engine also honors the shared return-vectors scan option.
 */
final class GuardiannVectorIndexEngine implements VectorIndexEngine {
    @Nonnull
    private final Config config;

    GuardiannVectorIndexEngine(@Nonnull final Config config) {
        this.config = config;
    }

    @Nonnull
    @Override
    public CompletableFuture<List<? extends ResultEntry>> search(@Nonnull final ReadTransaction readTransaction,
                                                                 @Nonnull final Subspace subspace,
                                                                 @Nonnull final Executor executor,
                                                                 @Nonnull final FDBStoreTimer timer,
                                                                 @Nonnull final VectorIndexScanBounds scanBounds) {
        final Guardiann guardiann =
                new Guardiann(subspace, executor, config, OnWriteListener.NOOP, new OnRead(timer));
        return guardiann.kNearestNeighborsSearch(readTransaction, scanBounds.getAdjustedLimit(),
                searchConfig(scanBounds), VectorIndexOptionsHelper.returnVectors(scanBounds, config.useRaBitQ()),
                Objects.requireNonNull(scanBounds.getQueryVector()));
    }

    /**
     * Builds the per-query {@link SearchConfig} from the scan options, leaving each knob at its {@code SearchConfig}
     * default when the corresponding option is absent — so a search with no scan options set behaves exactly as it did
     * with the all-defaults config.
     *
     * @param scanBounds the per-query scan bounds carrying the scan options
     * @return the search config for this query
     */
    @Nonnull
    private static SearchConfig searchConfig(@Nonnull final VectorIndexScanBounds scanBounds) {
        final VectorIndexScanOptions scanOptions = scanBounds.getVectorIndexScanOptions();
        final SearchConfig.SearchConfigBuilder builder = new SearchConfig.SearchConfigBuilder();
        applyScanDouble(scanOptions, VectorIndexScanOptions.GUARDIANN_CANDIDATE_POOL_FACTOR,
                builder::setCandidatePoolFactor);
        applyScanInteger(scanOptions, VectorIndexScanOptions.GUARDIANN_SEARCH_MAX_CLUSTERS,
                builder::setSearchMaxClusters);
        applyScanInteger(scanOptions, VectorIndexScanOptions.GUARDIANN_SEARCH_MIN_CLUSTERS_BEFORE_PRUNING,
                builder::setSearchMinClustersBeforePruning);
        applyScanDouble(scanOptions, VectorIndexScanOptions.GUARDIANN_SEARCH_DISTANCE_RATIO_CUTOFF,
                builder::setSearchDistanceRatioCutoff);
        applyScanInteger(scanOptions, VectorIndexScanOptions.GUARDIANN_CENTROID_EF_RING_SEARCH,
                builder::setCentroidEfRingSearch);
        applyScanInteger(scanOptions, VectorIndexScanOptions.GUARDIANN_CENTROID_EF_OUTWARD_SEARCH,
                builder::setCentroidEfOutwardSearch);
        applyScanInteger(scanOptions, VectorIndexScanOptions.GUARDIANN_SEARCH_CONCURRENCY,
                builder::setSearchConcurrency);
        return builder.build();
    }

    private static void applyScanInteger(@Nonnull final VectorIndexScanOptions scanOptions,
                                         @Nonnull final VectorOptionKey<Integer> key,
                                         @Nonnull final IntConsumer setter) {
        final Integer value = scanOptions.getOption(key);
        if (value != null) {
            setter.accept(value);
        }
    }

    private static void applyScanDouble(@Nonnull final VectorIndexScanOptions scanOptions,
                                        @Nonnull final VectorOptionKey<Double> key,
                                        @Nonnull final DoubleConsumer setter) {
        final Double value = scanOptions.getOption(key);
        if (value != null) {
            setter.accept(value);
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction,
                                          @Nonnull final Subspace subspace,
                                          @Nonnull final Executor executor,
                                          @Nonnull final FDBStoreTimer timer,
                                          @Nonnull final Tuple primaryKey,
                                          @Nonnull final RealVector vector) {
        // Insert reads (to find candidate clusters) and writes (references and deferred-task bookkeeping), so wire both
        // listeners.
        final Guardiann guardiann =
                new Guardiann(subspace, executor, config, new OnWrite(timer), new OnRead(timer));
        return guardiann.insert(transaction, primaryKey, vector, null);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> delete(@Nonnull final Transaction transaction,
                                          @Nonnull final Subspace subspace,
                                          @Nonnull final Executor executor,
                                          @Nonnull final FDBStoreTimer timer,
                                          @Nonnull final Tuple primaryKey,
                                          @Nonnull final RealVector vector) {
        // Guardiann needs the vector to locate the cluster references to remove; it reads while probing candidate
        // clusters and writes as it removes references, so both listeners are wired.
        final Guardiann guardiann =
                new Guardiann(subspace, executor, config, new OnWrite(timer), new OnRead(timer));
        return guardiann.delete(transaction, primaryKey, vector);
    }

    /**
     * Builds the Guardiann engine for an index, parsing its {@link Config} from the index options.
     *
     * @param index the index definition
     * @return the Guardiann engine
     */
    @Nonnull
    static GuardiannVectorIndexEngine fromIndex(@Nonnull final Index index) {
        return new GuardiannVectorIndexEngine(parseConfig(index));
    }

    /**
     * Parses a Guardiann {@link Config} from an index's options via the {@link VectorIndexOptionKeys} catalog: each key
     * resolves its value under the current name or a legacy alias and parses it to the right type. The nested
     * construction-time search config is deliberately not exposed and stays at its default. Unspecified options fall
     * back to the {@link Config} defaults.
     *
     * @param index the index definition
     * @return the parsed Guardiann config
     */
    @Nonnull
    static Config parseConfig(@Nonnull final Index index) {
        final ConfigBuilder builder = Guardiann.newConfigBuilder();

        builder.setMetric(VectorIndexOptionsHelper.getMetric(index));
        final int numDimensions = VectorIndexOptionsHelper.getNumDimensions(index);

        // Shared concepts.
        applyDouble(VectorIndexOptionKeys.SAMPLE_VECTOR_STATS_PROBABILITY, index,
                builder::setSampleVectorStatsProbability);
        applyDouble(VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY, index, builder::setMaintainStatsProbability);
        applyInteger(VectorIndexOptionKeys.STATS_THRESHOLD, index, builder::setStatsThreshold);
        applyBoolean(VectorIndexOptionKeys.USE_RABITQ, index, builder::setUseRaBitQ);
        applyInteger(VectorIndexOptionKeys.RABITQ_NUM_EX_BITS, index, builder::setRaBitQNumExBits);
        applyInteger(VectorIndexOptionKeys.MAX_NUM_CONCURRENT_NODE_FETCHES, index,
                builder::setMaxNumConcurrentNodeFetches);
        applyInteger(VectorIndexOptionKeys.MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES, index,
                builder::setMaxNumConcurrentNeighborhoodFetches);

        // Guardiann-only knobs.
        applyInteger(VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MIN, index, builder::setPrimaryClusterMin);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MAX, index, builder::setPrimaryClusterMax);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_UNDERREPLICATED_PRIMARY_CLUSTER_MAX, index,
                builder::setUnderreplicatedPrimaryClusterMax);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_REPLICATED_CLUSTER_MAX_WRITES, index,
                builder::setReplicatedClusterMaxWrites);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_REPLICATED_CLUSTER_TARGET, index,
                builder::setReplicatedClusterTarget);
        applyDouble(VectorIndexOptionKeys.GUARDIANN_REPLICATION_PRIORITY_MIN, index, builder::setReplicationPriorityMin);
        applyDouble(VectorIndexOptionKeys.GUARDIANN_REPLICATION_DISTANCE_RATIO_WEIGHT, index,
                builder::setReplicationDistanceRatioWeight);
        applyDouble(VectorIndexOptionKeys.GUARDIANN_REPLICATION_Z_SCORE_WEIGHT, index,
                builder::setReplicationZScoreWeight);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_REPLICATION_STATS_MIN_SAMPLE_SIZE, index,
                builder::setReplicationStatsMinSampleSize);
        applyBoolean(VectorIndexOptionKeys.GUARDIANN_DETERMINISTIC_RANDOMNESS, index,
                builder::setDeterministicRandomness);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_SAMPLE_BATCH_SIZE, index, builder::setSampleBatchSize);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_INSERT_MAX_CANDIDATE_CLUSTERS, index,
                builder::setInsertMaxCandidateClusters);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_DELETE_MAX_CANDIDATE_CLUSTERS, index,
                builder::setDeleteMaxCandidateClusters);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_DELETE_CONCURRENCY, index, builder::setDeleteConcurrency);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_SPLIT_NUM_NEAREST_CLUSTERS, index,
                builder::setSplitNumNearestClusters);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_MERGE_NUM_NEAREST_CLUSTERS, index,
                builder::setMergeNumNearestClusters);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_KMEANS_MAX_ITERATIONS, index, builder::setKMeansMaxIterations);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_KMEANS_MAX_RESTARTS, index, builder::setKMeansMaxRestarts);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_REASSIGN_NUM_NEIGHBORING_CLUSTERS, index,
                builder::setReassignNumNeighboringClusters);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_COLLAPSE_MIN_DUPLICATES, index, builder::setCollapseMinDuplicates);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_SPLIT_MERGE_CONCURRENCY, index, builder::setSplitMergeConcurrency);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_REASSIGN_CONCURRENCY, index, builder::setReassignConcurrency);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_COLLAPSE_CONCURRENCY, index, builder::setCollapseConcurrency);
        applyInteger(VectorIndexOptionKeys.GUARDIANN_BOUNCE_CONCURRENCY, index, builder::setBounceConcurrency);

        return builder.build(numDimensions);
    }

    /**
     * Validates which Guardiann options may change on an existing index. Following the same rule as HNSW, only the
     * statistics knobs and the concurrency knobs are mutable runtime tuning; every other option — the encoding (metric,
     * dimensions, RaBitQ), the cluster shape and replication thresholds, the per-task candidate counts, the k-means
     * tuning, and the determinism flag — is immutable, as changing it would reinterpret or restructure data already on
     * disk. Comparison is on <em>effective</em> config values (parsed and defaulted), so re-specifying an immutable
     * option at its default value is not treated as a change. Handled options are removed from {@code changedOptions}.
     * ({@code guardiannSampleBatchSize} is a stats knob with no HNSW counterpart, since HNSW has no batched-sampling
     * concept.)
     *
     * @param oldIndex the pre-change index
     * @param newIndex the post-change index
     * @param changedOptions the mutable set of changed option names; handled options are removed
     */
    static void validateChangedOptions(@Nonnull final Index oldIndex, @Nonnull final Index newIndex,
                                       @Nonnull final Set<String> changedOptions) {
        final Config oldConfig = parseConfig(oldIndex);
        final Config newConfig = parseConfig(newIndex);
        final String name = newIndex.getName();

        // Immutable: encoding shared with HNSW.
        disallowChange(changedOptions, VectorIndexOptionKeys.METRIC, oldConfig.metric(), newConfig.metric(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.NUM_DIMENSIONS, oldConfig.numDimensions(),
                newConfig.numDimensions(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.USE_RABITQ, oldConfig.useRaBitQ(),
                newConfig.useRaBitQ(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.RABITQ_NUM_EX_BITS, oldConfig.raBitQNumExBits(),
                newConfig.raBitQNumExBits(), name);

        // Immutable: Guardiann cluster shape, replication scoring, candidate counts, k-means, determinism.
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MIN,
                oldConfig.primaryClusterMin(), newConfig.primaryClusterMin(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MAX,
                oldConfig.primaryClusterMax(), newConfig.primaryClusterMax(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_UNDERREPLICATED_PRIMARY_CLUSTER_MAX,
                oldConfig.underreplicatedPrimaryClusterMax(), newConfig.underreplicatedPrimaryClusterMax(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_REPLICATED_CLUSTER_MAX_WRITES,
                oldConfig.replicatedClusterMaxWrites(), newConfig.replicatedClusterMaxWrites(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_REPLICATED_CLUSTER_TARGET,
                oldConfig.replicatedClusterTarget(), newConfig.replicatedClusterTarget(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_REPLICATION_PRIORITY_MIN,
                oldConfig.replicationPriorityMin(), newConfig.replicationPriorityMin(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_REPLICATION_DISTANCE_RATIO_WEIGHT,
                oldConfig.replicationDistanceRatioWeight(), newConfig.replicationDistanceRatioWeight(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_REPLICATION_Z_SCORE_WEIGHT,
                oldConfig.replicationZScoreWeight(), newConfig.replicationZScoreWeight(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_REPLICATION_STATS_MIN_SAMPLE_SIZE,
                oldConfig.replicationStatsMinSampleSize(), newConfig.replicationStatsMinSampleSize(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_DETERMINISTIC_RANDOMNESS,
                oldConfig.deterministicRandomness(), newConfig.deterministicRandomness(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_INSERT_MAX_CANDIDATE_CLUSTERS,
                oldConfig.insertMaxCandidateClusters(), newConfig.insertMaxCandidateClusters(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_DELETE_MAX_CANDIDATE_CLUSTERS,
                oldConfig.deleteMaxCandidateClusters(), newConfig.deleteMaxCandidateClusters(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_SPLIT_NUM_NEAREST_CLUSTERS,
                oldConfig.splitNumNearestClusters(), newConfig.splitNumNearestClusters(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_MERGE_NUM_NEAREST_CLUSTERS,
                oldConfig.mergeNumNearestClusters(), newConfig.mergeNumNearestClusters(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_KMEANS_MAX_ITERATIONS,
                oldConfig.kMeansMaxIterations(), newConfig.kMeansMaxIterations(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_KMEANS_MAX_RESTARTS,
                oldConfig.kMeansMaxRestarts(), newConfig.kMeansMaxRestarts(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_REASSIGN_NUM_NEIGHBORING_CLUSTERS,
                oldConfig.reassignNumNeighboringClusters(), newConfig.reassignNumNeighboringClusters(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.GUARDIANN_COLLAPSE_MIN_DUPLICATES,
                oldConfig.collapseMinDuplicates(), newConfig.collapseMinDuplicates(), name);

        // Mutable: stats and concurrency knobs.
        for (final VectorOptionKey<?> key : MUTABLE_OPTIONS) {
            allowChange(changedOptions, key);
        }
    }

    // Only stats and concurrency knobs may change on an existing index; see validateChangedOptions. Each key already
    // covers its current and legacy names.
    private static final List<VectorOptionKey<?>> MUTABLE_OPTIONS = ImmutableList.of(
            // stats knobs
            VectorIndexOptionKeys.SAMPLE_VECTOR_STATS_PROBABILITY,
            VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY,
            VectorIndexOptionKeys.STATS_THRESHOLD,
            VectorIndexOptionKeys.GUARDIANN_SAMPLE_BATCH_SIZE,
            // concurrency knobs
            VectorIndexOptionKeys.MAX_NUM_CONCURRENT_NODE_FETCHES,
            VectorIndexOptionKeys.MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES,
            VectorIndexOptionKeys.GUARDIANN_DELETE_CONCURRENCY,
            VectorIndexOptionKeys.GUARDIANN_SPLIT_MERGE_CONCURRENCY,
            VectorIndexOptionKeys.GUARDIANN_REASSIGN_CONCURRENCY,
            VectorIndexOptionKeys.GUARDIANN_COLLAPSE_CONCURRENCY,
            VectorIndexOptionKeys.GUARDIANN_BOUNCE_CONCURRENCY);

    /**
     * Read listener that attributes Guardiann reads to the store timer. Guardiann has no notion of graph layers, so
     * reads are counted against the generic index-load counters plus a Guardiann-specific vector-read counter.
     */
    static final class OnRead implements OnReadListener {
        @Nonnull
        private final FDBStoreTimer timer;

        OnRead(@Nonnull final FDBStoreTimer timer) {
            this.timer = timer;
        }

        @Override
        public <T> CompletableFuture<T> onAsyncRead(@Nonnull final CompletableFuture<T> future) {
            return timer.instrument(VectorIndexHelper.Events.VECTOR_SCAN, future);
        }

        @Nonnull
        @Override
        public AsyncIterable<KeyValue> onAsyncReadRange(@Nonnull final AsyncIterable<KeyValue> iterable) {
            return TimedAsyncIterable.wrap(iterable,
                    elapsed -> timer.record(VectorIndexHelper.Events.VECTOR_SCAN, elapsed));
        }

        @Override
        public void onKeyValueRead(@Nonnull final byte[] key, @Nullable final byte[] value) {
            VectorIndexInstrumentation.recordKeyValueRead(timer, key, value);
        }

        @Override
        public void onVectorRead(@Nonnull final UUID clusterId, @Nonnull final Tuple primaryKey,
                                 @Nonnull final UUID vectorUuid, @Nonnull final Transformed<RealVector> vector) {
            timer.increment(FDBStoreTimer.Counts.VECTOR_VECTOR_READS);
        }
    }

    /**
     * Write listener that attributes Guardiann writes and deferred-task activity to the store timer.
     */
    static final class OnWrite implements OnWriteListener {
        @Nonnull
        private final FDBStoreTimer timer;

        OnWrite(@Nonnull final FDBStoreTimer timer) {
            this.timer = timer;
        }

        @Override
        public void onKeyValueWritten(@Nonnull final byte[] key, @Nonnull final byte[] value) {
            VectorIndexInstrumentation.recordKeyValueWritten(timer, key, value);
        }

        @Override
        public void onTaskEnqueued(@Nonnull final TaskKind kind, @Nonnull final UUID taskId,
                                   @Nonnull final Set<UUID> targetClusterIds) {
            timer.increment(FDBStoreTimer.Counts.VECTOR_TASK_ENQUEUED);
        }

        @Override
        public void onTaskExecuted(@Nonnull final TaskKind taskKind, @Nonnull final UUID taskId,
                                   @Nonnull final Set<UUID> targetClusterIds) {
            timer.increment(FDBStoreTimer.Counts.VECTOR_TASK_EXECUTED);
        }
    }
}
