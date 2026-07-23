/*
 * VectorIndexOptionKeys.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * The catalog of index-time vector option keys — the single place where each option's (canonical, legacy) name pairing
 * and value type is declared. Each {@link VectorOptionKey} references the {@link IndexOptions} string constant(s) that
 * remain the public source of the wire names: shared concepts pair the engine-neutral {@code VECTOR_*} canonical name
 * with the legacy {@code HNSW_*} alias, while engine-specific options declare a single {@code HNSW_*} or
 * {@code GUARDIANN_*} name. Readers ({@code parseConfig}) and change validators in the engines derive everything from
 * these keys, so the pairing is never hand-spelled at a call site.
 * <p>
 * Query-time (per-query) options are catalogued separately, on
 * {@link com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions}, but share the same
 * {@link VectorOptionKey} primitive.
 */
@API(API.Status.EXPERIMENTAL)
public final class VectorIndexOptionKeys {
    //
    // Shared concepts: engine-neutral canonical name + legacy hnsw* alias.
    //
    public static final VectorOptionKey<Metric> METRIC =
            VectorOptionKey.ofMetric(IndexOptions.VECTOR_METRIC, IndexOptions.HNSW_METRIC);
    public static final VectorOptionKey<Integer> NUM_DIMENSIONS =
            VectorOptionKey.ofInteger(IndexOptions.VECTOR_NUM_DIMENSIONS, IndexOptions.HNSW_NUM_DIMENSIONS);
    public static final VectorOptionKey<Double> SAMPLE_VECTOR_STATS_PROBABILITY =
            VectorOptionKey.ofDouble(IndexOptions.VECTOR_SAMPLE_VECTOR_STATS_PROBABILITY,
                    IndexOptions.HNSW_SAMPLE_VECTOR_STATS_PROBABILITY);
    public static final VectorOptionKey<Double> MAINTAIN_STATS_PROBABILITY =
            VectorOptionKey.ofDouble(IndexOptions.VECTOR_MAINTAIN_STATS_PROBABILITY,
                    IndexOptions.HNSW_MAINTAIN_STATS_PROBABILITY);
    public static final VectorOptionKey<Integer> STATS_THRESHOLD =
            VectorOptionKey.ofInteger(IndexOptions.VECTOR_STATS_THRESHOLD, IndexOptions.HNSW_STATS_THRESHOLD);
    public static final VectorOptionKey<Boolean> USE_RABITQ =
            VectorOptionKey.ofBoolean(IndexOptions.VECTOR_USE_RABITQ, IndexOptions.HNSW_USE_RABITQ);
    public static final VectorOptionKey<Integer> RABITQ_NUM_EX_BITS =
            VectorOptionKey.ofInteger(IndexOptions.VECTOR_RABITQ_NUM_EX_BITS, IndexOptions.HNSW_RABITQ_NUM_EX_BITS);

    //
    // HNSW-only options.
    //
    public static final VectorOptionKey<Boolean> HNSW_USE_INLINING =
            VectorOptionKey.ofBoolean(IndexOptions.HNSW_USE_INLINING);
    public static final VectorOptionKey<Integer> HNSW_M =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_M);
    public static final VectorOptionKey<Integer> HNSW_M_MAX =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_M_MAX);
    public static final VectorOptionKey<Integer> HNSW_M_MAX_0 =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_M_MAX_0);
    public static final VectorOptionKey<Integer> HNSW_EF_CONSTRUCTION =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_EF_CONSTRUCTION);
    public static final VectorOptionKey<Integer> HNSW_EF_REPAIR =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_EF_REPAIR);
    public static final VectorOptionKey<Boolean> HNSW_EXTEND_CANDIDATES =
            VectorOptionKey.ofBoolean(IndexOptions.HNSW_EXTEND_CANDIDATES);
    public static final VectorOptionKey<Boolean> HNSW_KEEP_PRUNED_CONNECTIONS =
            VectorOptionKey.ofBoolean(IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS);
    public static final VectorOptionKey<Integer> HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER);
    public static final VectorOptionKey<Integer> HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES);
    public static final VectorOptionKey<Integer> HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES =
            VectorOptionKey.ofInteger(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES);

    //
    // Guardiann-only options.
    //
    public static final VectorOptionKey<Integer> GUARDIANN_PRIMARY_CLUSTER_MIN =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN);
    public static final VectorOptionKey<Integer> GUARDIANN_PRIMARY_CLUSTER_MAX =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX);
    public static final VectorOptionKey<Integer> GUARDIANN_UNDERREPLICATED_PRIMARY_CLUSTER_MAX =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_UNDERREPLICATED_PRIMARY_CLUSTER_MAX);
    public static final VectorOptionKey<Integer> GUARDIANN_REPLICATED_CLUSTER_MAX_WRITES =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_REPLICATED_CLUSTER_MAX_WRITES);
    public static final VectorOptionKey<Integer> GUARDIANN_REPLICATED_CLUSTER_TARGET =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_REPLICATED_CLUSTER_TARGET);
    public static final VectorOptionKey<Double> GUARDIANN_REPLICATION_PRIORITY_MIN =
            VectorOptionKey.ofDouble(IndexOptions.GUARDIANN_REPLICATION_PRIORITY_MIN);
    public static final VectorOptionKey<Double> GUARDIANN_REPLICATION_DISTANCE_RATIO_WEIGHT =
            VectorOptionKey.ofDouble(IndexOptions.GUARDIANN_REPLICATION_DISTANCE_RATIO_WEIGHT);
    public static final VectorOptionKey<Double> GUARDIANN_REPLICATION_Z_SCORE_WEIGHT =
            VectorOptionKey.ofDouble(IndexOptions.GUARDIANN_REPLICATION_Z_SCORE_WEIGHT);
    public static final VectorOptionKey<Integer> GUARDIANN_REPLICATION_STATS_MIN_SAMPLE_SIZE =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_REPLICATION_STATS_MIN_SAMPLE_SIZE);
    public static final VectorOptionKey<Boolean> GUARDIANN_DETERMINISTIC_RANDOMNESS =
            VectorOptionKey.ofBoolean(IndexOptions.GUARDIANN_DETERMINISTIC_RANDOMNESS);
    public static final VectorOptionKey<Integer> GUARDIANN_SAMPLE_BATCH_SIZE =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_SAMPLE_BATCH_SIZE);
    public static final VectorOptionKey<Integer> GUARDIANN_INSERT_MAX_CANDIDATE_CLUSTERS =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_INSERT_MAX_CANDIDATE_CLUSTERS);
    public static final VectorOptionKey<Integer> GUARDIANN_DELETE_MAX_CANDIDATE_CLUSTERS =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_DELETE_MAX_CANDIDATE_CLUSTERS);
    public static final VectorOptionKey<Integer> GUARDIANN_DELETE_CONCURRENCY =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_DELETE_CONCURRENCY);
    public static final VectorOptionKey<Integer> GUARDIANN_SPLIT_NUM_NEAREST_CLUSTERS =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_SPLIT_NUM_NEAREST_CLUSTERS);
    public static final VectorOptionKey<Integer> GUARDIANN_MERGE_NUM_NEAREST_CLUSTERS =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_MERGE_NUM_NEAREST_CLUSTERS);
    public static final VectorOptionKey<Integer> GUARDIANN_KMEANS_MAX_ITERATIONS =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_K_MEANS_MAX_ITERATIONS);
    public static final VectorOptionKey<Integer> GUARDIANN_KMEANS_MAX_RESTARTS =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_K_MEANS_MAX_RESTARTS);
    public static final VectorOptionKey<Integer> GUARDIANN_REASSIGN_NUM_NEIGHBORING_CLUSTERS =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_REASSIGN_NUM_NEIGHBORING_CLUSTERS);
    public static final VectorOptionKey<Integer> GUARDIANN_COLLAPSE_MIN_DUPLICATES =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_COLLAPSE_MIN_DUPLICATES);
    public static final VectorOptionKey<Integer> GUARDIANN_SPLIT_MERGE_CONCURRENCY =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_SPLIT_MERGE_CONCURRENCY);
    public static final VectorOptionKey<Integer> GUARDIANN_REASSIGN_CONCURRENCY =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_REASSIGN_CONCURRENCY);
    public static final VectorOptionKey<Integer> GUARDIANN_COLLAPSE_CONCURRENCY =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_COLLAPSE_CONCURRENCY);
    public static final VectorOptionKey<Integer> GUARDIANN_BOUNCE_CONCURRENCY =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_BOUNCE_CONCURRENCY);
    public static final VectorOptionKey<Integer> GUARDIANN_CONSTRUCTION_CENTROID_EF_RING_SEARCH =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_CONSTRUCTION_CENTROID_EF_RING_SEARCH);
    public static final VectorOptionKey<Integer> GUARDIANN_CONSTRUCTION_CENTROID_EF_OUTWARD_SEARCH =
            VectorOptionKey.ofInteger(IndexOptions.GUARDIANN_CONSTRUCTION_CENTROID_EF_OUTWARD_SEARCH);

    /**
     * Every index-time key declared above. Used to validate an index's options against all keys at once (e.g. to reject
     * specifying one option under more than one of its names). Kept in sync with the field declarations by
     * {@code VectorIndexOptionKeysTest.allContainsEveryDeclaredKey}.
     */
    static final List<VectorOptionKey<?>> ALL = ImmutableList.of(
            METRIC, NUM_DIMENSIONS, SAMPLE_VECTOR_STATS_PROBABILITY, MAINTAIN_STATS_PROBABILITY, STATS_THRESHOLD,
            USE_RABITQ, RABITQ_NUM_EX_BITS,
            HNSW_USE_INLINING, HNSW_M, HNSW_M_MAX, HNSW_M_MAX_0, HNSW_EF_CONSTRUCTION, HNSW_EF_REPAIR,
            HNSW_EXTEND_CANDIDATES, HNSW_KEEP_PRUNED_CONNECTIONS, HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER,
            HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES, HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES,
            GUARDIANN_PRIMARY_CLUSTER_MIN, GUARDIANN_PRIMARY_CLUSTER_MAX, GUARDIANN_UNDERREPLICATED_PRIMARY_CLUSTER_MAX,
            GUARDIANN_REPLICATED_CLUSTER_MAX_WRITES, GUARDIANN_REPLICATED_CLUSTER_TARGET,
            GUARDIANN_REPLICATION_PRIORITY_MIN, GUARDIANN_REPLICATION_DISTANCE_RATIO_WEIGHT,
            GUARDIANN_REPLICATION_Z_SCORE_WEIGHT, GUARDIANN_REPLICATION_STATS_MIN_SAMPLE_SIZE,
            GUARDIANN_DETERMINISTIC_RANDOMNESS, GUARDIANN_SAMPLE_BATCH_SIZE, GUARDIANN_INSERT_MAX_CANDIDATE_CLUSTERS,
            GUARDIANN_DELETE_MAX_CANDIDATE_CLUSTERS, GUARDIANN_DELETE_CONCURRENCY, GUARDIANN_SPLIT_NUM_NEAREST_CLUSTERS,
            GUARDIANN_MERGE_NUM_NEAREST_CLUSTERS, GUARDIANN_KMEANS_MAX_ITERATIONS, GUARDIANN_KMEANS_MAX_RESTARTS,
            GUARDIANN_REASSIGN_NUM_NEIGHBORING_CLUSTERS, GUARDIANN_COLLAPSE_MIN_DUPLICATES,
            GUARDIANN_SPLIT_MERGE_CONCURRENCY, GUARDIANN_REASSIGN_CONCURRENCY, GUARDIANN_COLLAPSE_CONCURRENCY,
            GUARDIANN_BOUNCE_CONCURRENCY, GUARDIANN_CONSTRUCTION_CENTROID_EF_RING_SEARCH,
            GUARDIANN_CONSTRUCTION_CENTROID_EF_OUTWARD_SEARCH);

    private VectorIndexOptionKeys() {
    }
}
