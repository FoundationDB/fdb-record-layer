/*
 * Guardiann.java
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A transactional approximate nearest neighbor (ANN) vector structure built on top of FoundationDB.
 * Guardiann organizes vectors into clusters, each represented by a centroid stored in an HNSW graph
 * for fast centroid lookup, and maintains per-cluster metadata (vector counts, running statistics,
 * replication state) to support incremental maintenance.
 *
 * <h2>Architecture</h2>
 * <p>
 * Vectors are partitioned into clusters. Each cluster stores a set of <em>primary</em> vector
 * references (the authoritative copies) and <em>replicated</em> references (copies from neighboring
 * clusters kept for improved search recall). Cluster centroids are indexed in an HNSW graph so that
 * the nearest clusters to a query vector can be found efficiently.
 * </p>
 *
 * <h2>Operations</h2>
 * <ul>
 *   <li><b>Insert</b> ({@link #insert}) — finds the nearest cluster(s) for a new vector, writes
 *       primary and replicated references, and triggers deferred maintenance tasks (split/merge/reassign)
 *       if cluster size invariants are violated.</li>
 *   <li><b>Search</b> ({@link #kNearestNeighborsSearch}) — probes the nearest cluster centroids,
 *       scans their vector references, and returns the top-k closest vectors. Supports distance-ratio
 *       pruning and centroid-ball pruning via {@code maxEver} bounds.</li>
 *   <li><b>Delete</b> ({@link #delete}) — locates and removes vector references from clusters,
 *       updates metadata, and handles collapsed (deduplicated) vectors.</li>
 * </ul>
 *
 * <h2>Maintenance</h2>
 * <p>
 * Structural maintenance is performed lazily via deferred tasks that are executed piggy-backed on
 * insert and delete operations:
 * </p>
 * <ul>
 *   <li><b>Split/Merge</b> — when a cluster grows beyond {@link Config#primaryClusterMax()} or shrinks
 *       below {@link Config#primaryClusterMin()}, a {@link SplitMergeTask} repartitions the affected
 *       clusters using bounded k-means and updates the HNSW centroid index.</li>
 *   <li><b>Reassign</b> — a {@link ReassignTask} recomputes vector assignments and replication for a
 *       cluster, fixing underreplicated vectors and cleaning up excess replicas.</li>
 *   <li><b>Collapse</b> — a {@link CollapseTask} detects large groups of identical vectors and replaces
 *       them with a single collapsed representative to reduce storage.</li>
 *   <li><b>Bounce</b> — a {@link BounceTask} coordinates execution order between dependent tasks,
 *       ensuring prerequisite tasks complete before follow-up tasks are created.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <p>
 * This class is the primary entry point for interacting with the Guardiann vector structure. It
 * delegates to specialized operation classes ({@link Insert}, {@link Search}, {@link Delete}) which
 * implement the respective algorithms. All operations are asynchronous and designed for use within
 * FoundationDB transactions.
 * </p>
 *
 * @see Config
 * @see Search
 * @see Insert
 * @see Delete
 */
@API(API.Status.EXPERIMENTAL)
public class Guardiann {
    @Nonnull
    private final Locator locator;

    /**
     * Start building a {@link Config}.
     * @return a new {@code Config} that can be altered and then built for use with a {@link Guardiann}
     * @see Config.ConfigBuilder#build
     */
    public static Config.ConfigBuilder newConfigBuilder() {
        return new Config.ConfigBuilder();
    }

    /**
     * Returns a default {@link Config}.
     * @param numDimensions number of dimensions
     * @return a new default {@code Config}.
     * @see Config.ConfigBuilder#build
     */
    @Nonnull
    public static Config defaultConfig(int numDimensions) {
        return new Config.ConfigBuilder().build(numDimensions);
    }

    /**
     * Constructs a new Guardiann vector structure instance.
     * <p>
     * Initializes the structure with the necessary components for storage, execution, configuration,
     * and event handling. All parameters are mandatory and must not be null.
     *
     * @param subspace the {@link Subspace} where the vector structure data is stored
     * @param executor the {@link Executor} service to use for concurrent operations
     * @param config the {@link Config} object containing algorithm and storage parameters
     * @param onWriteListener a listener to be notified of write events
     * @param onReadListener a listener to be notified of read events
     */
    public Guardiann(@Nonnull final Subspace subspace,
                     @Nonnull final Executor executor,
                     @Nonnull final Config config,
                     @Nonnull final OnWriteListener onWriteListener,
                     @Nonnull final OnReadListener onReadListener) {
        this.locator = new Locator(subspace, executor, config, onWriteListener, onReadListener);
    }

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    public Subspace getSubspace() {
        return getLocator().getSubspace();
    }

    /**
     * Get the executor used by this object.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get this object's configuration.
     * @return the configuration
     */
    @Nonnull
    public Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    public OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    private Search search() {
        return getLocator().search();
    }

    @Nonnull
    private Insert insert() {
        return getLocator().insert();
    }

    @Nonnull
    private Delete delete() {
        return getLocator().delete();
    }

    /**
     * Performs a search for the k-nearest neighbors for a given query vector.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param k the number of nearest neighbors to return
     * @param searchConfig the performance/recall tuning knobs for this search (see {@link SearchConfig})
     * @param includeVectors indicator if the caller would like the search to also include vectors in the result set
     * @param queryVector the vector to find the nearest neighbors of
     *
     * @return a {@link CompletableFuture} that will complete with a list of the {@code k} nearest neighbors,
     *         sorted by distance in ascending order.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name normally used in literature
    @Nonnull
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    @Nonnull final SearchConfig searchConfig,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        return search().kNearestNeighborsSearch(readTransaction, k, searchConfig, includeVectors, queryVector);
    }

    /**
     * Performs a distance-ordered k-nearest-neighbor search — the streaming, paginatable sibling of
     * {@link #kNearestNeighborsSearch}. Probes up to {@code searchMaxClusters} clusters and returns up to
     * {@code k} results in ascending distance order, starting strictly after the
     * {@code (minimumRadius, minimumPrimaryKey)} cursor (and skipping cluster centroids within
     * {@code minimumRadiusCluster}). Pass {@code minimumRadiusCluster = 0.0},
     * {@code minimumRadius = Double.NEGATIVE_INFINITY} and {@code minimumPrimaryKey = null} to search from the
     * beginning; pass the {@code (distance, primaryKey)} of a page's last result to resume after it.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param k the maximum number of nearest neighbors to return
     * @param searchConfig the performance/recall tuning knobs for this search (see {@link SearchConfig}); the
     *        distance-ratio pruning knobs do not apply to this streaming variant
     * @param minimumRadiusCluster exclusive lower bound on cluster-centroid distance (cluster-level cursor)
     * @param minimumRadius exclusive lower bound on result distance (result-level cursor)
     * @param minimumPrimaryKey tie-breaker applied at exactly {@code minimumRadius}; a result is kept only when
     *        its primary key is strictly greater (may be {@code null} to disable the tie-break)
     * @param includeVectors indicator if the caller would like the search to also include vectors in the result set
     * @param queryVector the vector to find the nearest neighbors for
     *
     * @return a {@link CompletableFuture} completing with up to {@code k} results in ascending distance order
     */
    @Nonnull
    CompletableFuture<List<? extends ResultEntry>>
            searchOrderedByDistance(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    @Nonnull final SearchConfig searchConfig,
                                    final double minimumRadiusCluster,
                                    final double minimumRadius,
                                    @Nullable final Tuple minimumPrimaryKey,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        return search().searchOrderedByDistanceResults(readTransaction, k, searchConfig,
                queryVector, minimumRadiusCluster, minimumRadius, minimumPrimaryKey, includeVectors);
    }

    /**
     * Inserts a new vector with its associated primary key into the vector structure.
     * <p>
     * Finds the nearest cluster(s) for the new vector by querying the HNSW centroid index, then writes a
     * primary vector reference to the nearest cluster and replicated references to neighboring clusters
     * based on replication priority scoring. Updates cluster metadata and enqueues deferred maintenance
     * tasks (split, merge, reassign) if cluster size invariants are violated.
     *
     * @param transaction the {@link Transaction} context for all database operations
     * @param newPrimaryKey the unique {@link Tuple} primary key for the new vector being inserted
     * @param newVector the {@link RealVector} data to be inserted
     * @param additionalValues additional values to be associated with the new vector/record, or {@code null}
     * @return a {@link CompletableFuture} that completes when the insertion operation is finished
     */
    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction, @Nonnull final Tuple newPrimaryKey,
                                          @Nonnull final RealVector newVector, @Nullable final Tuple additionalValues) {
        return insert().insert(transaction, newPrimaryKey, newVector, additionalValues);
    }

    /**
     * Deletes a vector identified by its primary key from the Guardiann index.
     * <p>
     * Removes the vector's references from all clusters it belongs to (primary and replicated),
     * updates cluster metadata, and enqueues maintenance tasks if needed. The caller must provide
     * the vector data to locate which clusters contain the vector.
     *
     * @param transaction the {@link Transaction} context for all database operations
     * @param primaryKey the unique {@link Tuple} primary key of the vector to delete
     * @param vector the {@link RealVector} data of the vector being deleted
     * @return a {@link CompletableFuture} that completes when the deletion is finished
     */
    @Nonnull
    public CompletableFuture<Void> delete(@Nonnull final Transaction transaction, @Nonnull final Tuple primaryKey,
                                          @Nonnull final RealVector vector) {
        return delete().delete(transaction, primaryKey, vector);
    }
}
