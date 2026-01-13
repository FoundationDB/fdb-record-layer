/*
 * HNSW.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

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
 * and performing k-NN searches ({@link #kNearestNeighborsSearch(ReadTransaction, int, int, boolean, RealVector)}).
 * It is designed to be used with a transactional storage backend, managed via a {@link Subspace}.
 * <p>
 * This class functions as the entry point for any interactions with the HNSW data structure. It delegates to
 * the respective operations classes which implement the actual algorithms to maintain and to search the structure.
 * @see <a href="https://arxiv.org/abs/1603.09320">Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HNSW {
    @Nonnull
    private final Locator locator;

    /**
     * Start building a {@link Config}.
     * @return a new {@code Config} that can be altered and then built for use with a {@link HNSW}
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
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get this hnsw's configuration.
     * @return hnsw configuration
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
     * @param efSearch the size of the dynamic candidate list for the search. A larger value increases accuracy
     *        at the cost of performance.
     * @param includeVectors indicator if the caller would like the search to also include vectors in the result set
     * @param queryVector the vector to find the nearest neighbors for
     *
     * @return a {@link CompletableFuture} that will complete with a list of the {@code k} nearest neighbors,
     *         sorted by distance in ascending order.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    final int efSearch,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        return search().kNearestNeighborsSearch(readTransaction, k, efSearch, includeVectors, queryVector);
    }

    /**
     * Performs a search for the k-nearest neighbors of a ring around a given query vector at a given radius.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param k the number of nearest neighbors to return
     * @param efSearch the size of the dynamic candidate list for the search. A larger value increases accuracy
     *        at the cost of performance.
     * @param includeVectors indicator if the caller would like the search to also include vectors in the result set
     * @param queryVector the vector to find the nearest neighbors for
     *
     * @return a {@link CompletableFuture} that will complete with a list of the {@code k} nearest neighbors,
     *         sorted by distance in ascending order.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsRingSearch(@Nonnull final ReadTransaction readTransaction,
                                        final int k,
                                        final int efSearch,
                                        final boolean includeVectors,
                                        @Nonnull final RealVector queryVector,
                                        final double radius) {
        return search().kNearestNeighborsRingSearch(readTransaction, k, efSearch, includeVectors, queryVector,
                radius);
    }

    /**
     * Inserts a new vector with its associated primary key into the HNSW graph.
     * <p>
     * The method first determines a layer for the new node, called the {@code top layer}.
     * It then traverses the graph from the entry point downwards, greedily searching for the nearest
     * neighbors to the {@code newVector} at each layer. This search identifies the optimal
     * connection points for the new node.
     * <p>
     * Once the nearest neighbors are found, the new node is linked into the graph structure at all
     * layers up to its {@code top layer}. Special handling is included for inserting the
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
        return insert().insert(transaction, newPrimaryKey, newVector);
    }

    /**
     * Deletes a record using its associated primary key from the HNSW graph.
     * <p>
     * This method implements a multi-layer deletion algorithm that maintains the structural integrity of the HNSW
     * graph. The deletion process consists of several key phases:
     * <ul>
     *     <li><b>Layer Determination:</b> First determines the top layer for the node using the same deterministic
     *         algorithm used during insertion, ensuring consistent layer assignment across operations.
     *     </li>
     *     <li><b>Existence Verification:</b> Checks whether the node actually exists in the graph before attempting
     *          deletion. If the node doesn't exist, the operation completes immediately without error.
     *     </li>
     *     <li><b>Multi-Layer Deletion:</b> Removes the node from all layers spanning from layer 0 (base layer
     *         containing all nodes) up to and including the node's top layer. The deletion is performed in parallel
     *         across all layers for optimal performance.
     *     </li>
     *     <li><b>Graph Repair:</b> For each layer where the node is deleted, the algorithm repairs the local graph
     *         structure by identifying the deleted node's neighbors and reconnecting them appropriately. This process:
     *         <ul>
     *             <li>Finds candidate replacement connections among the neighbors of neighbors</li>
     *             <li>Selects optimal new connections using the HNSW distance heuristics</li>
     *             <li>Updates neighbor lists to maintain graph connectivity and search performance</li>
     *             <li>Applies connection limits (M, MMax) and prunes excess connections if necessary</li>
     *         </ul>
     *     </li>
     *     <li><b>Entry Point Management:</b> If the deleted node was serving as the graph's entry point (the starting
     *         node for search operations), the method automatically selects a new entry point from the remaining nodes
     *         at the highest available layer. If no nodes remain after deletion, the access information is cleared,
     *         effectively resetting the graph to an empty state.
     *     </li>
     * </ul>
     * All operations are performed transactionally and asynchronously, ensuring consistency and enabling
     * non-blocking execution in concurrent environments.
     *
     * @param transaction the {@link Transaction} context for all database operations, ensuring atomicity
     *        and consistency of the deletion and repair operations
     * @param primaryKey the unique {@link Tuple} primary key identifying the node to be deleted from the graph
     *
     * @return a {@link CompletableFuture} that completes when the deletion operation is fully finished,
     *         including all graph repairs and entry point updates. The future completes with {@code null}
     *         on successful deletion.
     */
    @Nonnull
    public CompletableFuture<Void> delete(@Nonnull final Transaction transaction, @Nonnull final Tuple primaryKey) {
        return delete().delete(transaction, primaryKey);
    }

    /**
     * Returns an async iterator that returns results ordered by their distance from a given center vector.
     * <p>
     * This method initiates an outward traversal from the {@code centerVector}, effectively performing a k-NN
     * (k-Nearest Neighbor) or beam search. The results are returned as an {@link AsyncIterator}, with items
     * yielded in increasing order of their distance from the center. The search can be started or resumed from a
     * specific point defined by {@code minimumRadius} and {@code minimumPrimaryKey}, allowing for pagination.
     *
     * @param readTransaction the transaction to use for reading data
     * @param efRingSearch the exploration factor for the initial ring search phase of the HNSW algorithm
     * @param efOutwardSearch the exploration factor for the main outward search phase, determining the size of the
     * candidate queue
     * @param includeVectors a boolean flag indicating whether the full vectors should be reconstructed and included in
     *        the results. If {@code false}, the vector in each {@link ResultEntry} will be {@code null}.
     * @param centerVector the vector to search around. Results will be ordered by their distance to this vector
     * @param minimumRadius the minimum distance from the {@code centerVector}. Only results with a distance greater
     *        than will be returned.
     * @param minimumPrimaryKey the primary key of the last item from a previous scan, used for pagination. If provided
     *        along with {@code minimumRadius}, the scan will resume after the item with this key at that radius. Can be
     *        {@code null} to start from the beginning.
     * @return an {@link AsyncIterator} of {@link ResultEntry} objects, ordered by increasing distance from the
     *         {@code centerVector}
     */
    @Nonnull
    public AsyncIterator<ResultEntry>
            orderByDistance(@Nonnull final ReadTransaction readTransaction,
                            final int efRingSearch,
                            final int efOutwardSearch,
                                                      final boolean includeVectors,
                            @Nonnull final RealVector centerVector,
                            final double minimumRadius,
                            @Nullable final Tuple minimumPrimaryKey) {
        return search().orderByDistance(readTransaction, efRingSearch, efOutwardSearch, includeVectors, centerVector,
                minimumRadius, minimumPrimaryKey);
    }

    /**
     * Scans all nodes within a given layer of the database.
     * <p>
     * The scan is performed transactionally in batches to avoid loading the entire layer into memory at once. Each
     * discovered node is passed to the provided {@link Consumer} for processing. The operation continues fetching
     * batches until all nodes in the specified layer have been processed.
     *
     * @param db the non-null {@link Database} instance to run the scan against.
     * @param layer the specific layer index to scan.
     * @param batchSize the number of nodes to retrieve and process in each batch.
     * @param nodeConsumer the non-null {@link Consumer} that will accept each {@link AbstractNode}
     * found in the layer.
     */
    @VisibleForTesting
    static void scanLayer(@Nonnull final Config config,
                          @Nonnull final Subspace subspace,
                          @Nonnull final Database db,
                          final int layer,
                          final int batchSize,
                          @Nonnull final Consumer<AbstractNode<? extends NodeReference>> nodeConsumer) {
        Primitives.scanLayer(config, subspace, db, layer, batchSize, nodeConsumer);
    }

    /**
     * Gets the appropriate storage adapter for a given layer.
     * <p>
     * This method selects a {@link StorageAdapter} implementation based on the layer number. The logic is intended to
     * use an {@code InliningStorageAdapter} for layers greater than {@code 0} and a {@code CompactStorageAdapter} for
     * layer 0. Note that we will only use inlining at all if the config indicates we should use inlining.
     *
     * @param config the config to use
     * @param subspace the subspace of the HNSW object itself
     * @param onWriteListener a listener that the new {@link StorageAdapter} will call back for any write events
     * @param onReadListener a listener that the new {@link StorageAdapter} will call back for any read events
     * @param layer the layer number for which to get the storage adapter
     * @return a non-null {@link StorageAdapter} instance
     */
    @Nonnull
    @VisibleForTesting
    static StorageAdapter<? extends NodeReference>
            storageAdapterForLayer(@Nonnull final Config config,
                                   @Nonnull final Subspace subspace,
                                   @Nonnull final OnWriteListener onWriteListener,
                                   @Nonnull final OnReadListener onReadListener,
                                   final int layer) {
        return Primitives.storageAdapterForLayer(config, subspace, onWriteListener, onReadListener, layer);
    }
}
