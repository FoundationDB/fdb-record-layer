/*
 * Delete.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;

/**
 * An implementation of the delete/repair operations of the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 * <p>
 * HNSW constructs a multi-layer graph, where each layer is a subset of the one below it. The top layers serve as fast
 * entry points to navigate the graph, while the bottom layer contains all the data points. This structure allows for
 * logarithmic-time complexity for search operations, making it suitable for large-scale, high-dimensional datasets.
 * <p>
 * The entry point for any interactions with the HNSW data structure is implemented in {@link HNSW}. Do not instantiate
 * this class directly.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class Delete {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Delete.class);

    @Nonnull
    private final Locator locator;

    /**
     * This constructor initializes a new delete operations object with the necessary components for storage,
     * execution, configuration, and event handling. All parameters are mandatory and must not be null.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Delete(@Nonnull final Locator locator) {
        this.locator = locator;
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
    private Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration of this hnsw.
     * @return hnsw configuration
     */
    @Nonnull
    private Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    private OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    private OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    private Primitives primitives() {
        return getLocator().primitives();
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
        final Primitives primitives = primitives();
        final SplittableRandom random = Primitives.random(primaryKey);
        final int topLayer = primitives.topLayer(primaryKey);
        if (logger.isTraceEnabled()) {
            logger.trace("node with key={} to be deleted form layer={}", primaryKey, topLayer);
        }

        return StorageAdapter.fetchAccessInfo(getConfig(), transaction, getSubspace(), getOnReadListener())
                .thenCombine(primitives.exists(transaction, primaryKey),
                        (accessInfo, nodeExists) -> {
                            if (!nodeExists) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("record does not exists in HNSW with key={} on layer={}",
                                            primaryKey, topLayer);
                                }
                            }
                            return new Primitives.AccessInfoAndNodeExistence(accessInfo, nodeExists);
                        })
                .thenCompose(accessInfoAndNodeExistence -> {
                    if (!accessInfoAndNodeExistence.isNodeExists()) {
                        return AsyncUtil.DONE;
                    }

                    final AccessInfo accessInfo = accessInfoAndNodeExistence.getAccessInfo();
                    final EntryNodeReference entryNodeReference =
                            accessInfo == null ? null : accessInfo.getEntryNodeReference();
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Quantizer quantizer = primitives.quantizer(accessInfo);

                    return deleteFromLayers(transaction, storageTransform, quantizer, random, primaryKey, topLayer)
                            .thenCompose(potentialEntryNodeReferences -> {
                                if (entryNodeReference != null && primaryKey.equals(entryNodeReference.getPrimaryKey())) {
                                    // find (and store) a new entry reference
                                    for (int i = potentialEntryNodeReferences.size() - 1; i >= 0; i --) {
                                        final EntryNodeReference potentialEntyNodeReference =
                                                potentialEntryNodeReferences.get(i);
                                        if (potentialEntyNodeReference != null) {
                                            StorageAdapter.writeAccessInfo(transaction, getSubspace(),
                                                    accessInfo.withNewEntryNodeReference(potentialEntyNodeReference),
                                                    getOnWriteListener());
                                            // early out
                                            return AsyncUtil.DONE;
                                        }
                                    }

                                    // there is no data in the structure, delete access info to start new
                                    StorageAdapter.deleteAccessInfo(transaction, getSubspace(), getOnWriteListener());
                                }
                                return AsyncUtil.DONE;
                            });
                });
    }

    /**
     * Deletes a node from the HNSW graph across multiple layers, using a primary key and a given top layer.
     *
     * @param transaction the transaction to use for database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     * storage space that is currently being used
     * @param quantizer the quantizer to be used for this insert
     * @param primaryKey the primary key of the new node being inserted
     * @param topLayer the top layer for the node.
     *
     * @return a {@link CompletableFuture} that completes when the new node has been successfully inserted into all
     *         its designated layers and contains an existing neighboring entry node reference on that layer.
     */
    @Nonnull
    private CompletableFuture<List<EntryNodeReference>> deleteFromLayers(@Nonnull final Transaction transaction,
                                                                         @Nonnull final StorageTransform storageTransform,
                                                                         @Nonnull final Quantizer quantizer,
                                                                         @Nonnull final SplittableRandom random,
                                                                         @Nonnull final Tuple primaryKey,
                                                                         final int topLayer) {
        // delete the node from all layers in parallel (inside layer in [0, topLayer])
        return forEach(() -> IntStream.rangeClosed(0, topLayer).iterator(),
                layer ->
                        deleteFromLayer(primitives().storageAdapterForLayer(layer), transaction, storageTransform,
                                quantizer, random.split(), layer, primaryKey),
                getConfig().getMaxNumConcurrentDeleteFromLayer(),
                getExecutor());
    }

    /**
     * Deletes a node from a specified layer of the HNSW graph. This method orchestrates the complete deletion process
     * for a single layer.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter for reading from and writing to the graph
     * @param transaction the transaction context for the database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param quantizer the quantizer for this insert
     * @param layer the layer number to insert the new node into
     * @param toBeDeletedPrimaryKey the primary key of the new node to be inserted
     *
     * @return a {@code CompletableFuture} that completes with a {@code null}
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<EntryNodeReference>
            deleteFromLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final Transaction transaction,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final Quantizer quantizer,
                            @Nonnull final SplittableRandom random,
                            final int layer,
                            @Nonnull final Tuple toBeDeletedPrimaryKey) {
        if (logger.isTraceEnabled()) {
            logger.trace("begin delete key={} at layer={}", toBeDeletedPrimaryKey, layer);
        }
        final Primitives primitives = primitives();
        final Estimator estimator = quantizer.estimator();
        final Map<Tuple, AbstractNode<N>> nodeCache = Maps.newConcurrentMap();
        final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> candidateChangeSetMap =
                Maps.newConcurrentMap();

        return storageAdapter.fetchNode(transaction, storageTransform, layer, toBeDeletedPrimaryKey)
                .thenCompose(toBeDeletedNode -> {
                    final NodeReferenceAndNode<NodeReference, N> toBeDeletedNodeReferenceAndNode =
                            new NodeReferenceAndNode<>(new NodeReference(toBeDeletedPrimaryKey), toBeDeletedNode);

                    return findDeletionRepairCandidates(storageAdapter, transaction, storageTransform, random, layer,
                            toBeDeletedNodeReferenceAndNode, nodeCache)
                            .thenCompose(candidates -> {
                                initializeCandidateChangeSetMap(toBeDeletedPrimaryKey, toBeDeletedNode, candidates,
                                        candidateChangeSetMap);
                                // resolve the actually existing direct neighbors
                                final ImmutableList<N> primaryNeighbors =
                                        primitives.primaryNeighbors(toBeDeletedNode, candidateChangeSetMap);

                                //
                                // Repair each primary neighbor in parallel, there should not be much actual I/O,
                                // except in edge cases, but we should still parallelize it.
                                //
                                return forEach(primaryNeighbors,
                                        neighborReference ->
                                                repairNeighbor(storageAdapter, transaction,
                                                        storageTransform, estimator, layer, neighborReference,
                                                        candidates, candidateChangeSetMap, nodeCache),
                                        getConfig().getMaxNumConcurrentNeighborhoodFetches(), getExecutor())
                                        .thenApply(ignored -> {
                                            final ImmutableMap.Builder<Tuple, NodeReferenceWithVector> candidateReferencesMapBuilder =
                                                    ImmutableMap.builder();
                                            for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
                                                final var candidatePrimaryKey = candidate.getNodeReference().getPrimaryKey();
                                                if (candidateChangeSetMap.containsKey(candidatePrimaryKey)) {
                                                    candidateReferencesMapBuilder.put(candidatePrimaryKey, candidate.getNodeReference());
                                                }
                                            }
                                            return candidateReferencesMapBuilder.build();
                                        });
                            })
                            .thenCompose(candidateReferencesMap -> {
                                final int currentMMax =
                                        layer == 0 ? getConfig().getMMax0() : getConfig().getMMax();

                                //
                                // If we previously went beyond the mMax/mMax0, we need to prune the neighbors.
                                // Pruning is independent among different nodes -- we can therefore prune in
                                // parallel.
                                //
                                return forEach(candidateChangeSetMap.entrySet(), // for each modified neighbor set
                                        changeSetEntry -> {
                                            final NodeReferenceWithVector candidateReference =
                                                    Objects.requireNonNull(candidateReferencesMap.get(changeSetEntry.getKey()));
                                            final NeighborsChangeSet<N> candidateChangeSet = changeSetEntry.getValue();
                                            return primitives.pruneNeighborsIfNecessary(storageAdapter, transaction,
                                                    storageTransform, estimator, layer, candidateReference,
                                                    currentMMax, candidateChangeSet, nodeCache)
                                                    .thenApply(nodeReferencesAndNodes -> {
                                                        if (nodeReferencesAndNodes == null) {
                                                            return candidateChangeSet;
                                                        }

                                                        final var prunedCandidateChangeSet =
                                                                primitives.resolveChangeSetFromNewNeighbors(candidateChangeSet,
                                                                        nodeReferencesAndNodes);
                                                        candidateChangeSetMap.put(changeSetEntry.getKey(), prunedCandidateChangeSet);
                                                        return prunedCandidateChangeSet;
                                                    });
                                        },
                                        getConfig().getMaxNumConcurrentNeighborhoodFetches(), getExecutor())
                                        .thenApply(ignored -> candidateReferencesMap);
                            })
                            .thenApply(candidateReferencesMap -> {
                                //
                                // Finally delete the node we set out to delete and persist the change sets for all
                                // repaired nodes.
                                //
                                storageAdapter.deleteNode(transaction, layer, toBeDeletedPrimaryKey);

                                for (final Map.Entry<Tuple, NeighborsChangeSet<N>> changeSetEntry : candidateChangeSetMap.entrySet()) {
                                    final NeighborsChangeSet<N> changeSet = changeSetEntry.getValue();
                                    if (changeSet.hasChanges()) {
                                        final AbstractNode<N> candidateNode =
                                                primitives.nodeFromCache(changeSetEntry.getKey(), nodeCache);
                                        storageAdapter.writeNode(transaction, quantizer,
                                                layer, candidateNode, changeSet);
                                    }
                                }

                                //
                                // Return the first item in the candidates reference map as a potential new
                                // entry node reference in order to avoid a costly search for a new global entry point.
                                // This reference is guaranteed to exist.
                                //
                                final Tuple firstPrimaryKey =
                                        Iterables.getFirst(candidateReferencesMap.keySet(), null);
                                return firstPrimaryKey == null
                                       ? null
                                       : new EntryNodeReference(firstPrimaryKey,
                                        Objects.requireNonNull(candidateReferencesMap.get(firstPrimaryKey)).getVector(),
                                        layer);
                            });
                }).thenApply(result -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("end delete key={} at layer={}", toBeDeletedPrimaryKey, layer);
                    }
                    return result;
                });
    }

    private <N extends NodeReference> void initializeCandidateChangeSetMap(@Nonnull final Tuple toBeDeletedPrimaryKey,
                                                                           @Nonnull final AbstractNode<N> toBeDeletedNode,
                                                                           @Nonnull final List<NodeReferenceAndNode<NodeReferenceWithVector, N>> candidates,
                                                                           @Nonnull final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap) {
        for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
            final AbstractNode<N> candidateNode = candidate.getNode();
            boolean foundToBeDeleted = false;
            for (final N neighborOfCandidate : candidateNode.getNeighbors()) {
                if (neighborOfCandidate.getPrimaryKey().equals(toBeDeletedPrimaryKey)) {
                    //
                    // Make sure a neighbor pointing to the node being deleted is deleted as well.
                    //
                    candidateChangeSetMap.put(candidateNode.getPrimaryKey(),
                            new DeleteNeighborsChangeSet<>(
                                    new BaseNeighborsChangeSet<>(candidateNode.getNeighbors()),
                                    ImmutableList.of(toBeDeletedPrimaryKey)));
                    foundToBeDeleted = true;
                    break;
                }
            }
            if (!foundToBeDeleted) {
                // if there is no reference back to the node being deleted, just create the base set
                candidateChangeSetMap.put(candidateNode.getPrimaryKey(),
                        new BaseNeighborsChangeSet<>(candidateNode.getNeighbors()));
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("number of neighbors to repair={}", toBeDeletedNode.getNeighbors().size());
        }
    }

    /**
     * Find candidates starting from the node to be deleted. To this end we find all the existing first degree (primary)
     * and second-degree (secondary) neighbors. As that set is too big to consider for the repair we rely on sampling
     * to eventually compile a list of roughly {@code efRepair} number of candidates.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param storageTransform the storage transform
     * @param random a {@link SplittableRandom} used for sampling the candidate set
     * @param layer the layer
     * @param toBeDeletedNodeReferenceAndNode the node that is about to be deleted
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithVector, N>>>
             findDeletionRepairCandidates(final @Nonnull StorageAdapter<N> storageAdapter,
                                          final @Nonnull Transaction transaction,
                                          final @Nonnull StorageTransform storageTransform,
                                          final @Nonnull SplittableRandom random,
                                          final int layer,
                                          final NodeReferenceAndNode<NodeReference, N> toBeDeletedNodeReferenceAndNode,
                                          final Map<Tuple, AbstractNode<N>> nodeCache) {
        final Primitives primitives = primitives();
        return primitives.neighbors(storageAdapter, transaction, storageTransform, random,
                ImmutableList.of(toBeDeletedNodeReferenceAndNode),
                ((r, initialNodeKeys, size, nodeReference) ->
                         shouldUsePrimaryCandidateForRepair(nodeReference,
                                 toBeDeletedNodeReferenceAndNode.getNodeReference().getPrimaryKey())), layer, nodeCache)
                .thenCompose(candidates ->
                        primitives.neighbors(storageAdapter, transaction, storageTransform, random,
                                candidates,
                                ((r, initialNodeKeys, size, nodeReference) ->
                                         shouldUseSecondaryCandidateForRepair(r, initialNodeKeys, size, nodeReference,
                                                 toBeDeletedNodeReferenceAndNode.getNodeReference().getPrimaryKey())),
                                layer, nodeCache))
                .thenApply(candidates -> {
                    if (logger.isTraceEnabled()) {
                        final ImmutableList.Builder<String> candidateStringsBuilder = ImmutableList.builder();
                        for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
                            candidateStringsBuilder.add(candidate.getNode().getPrimaryKey().toString());
                        }
                        logger.trace("found at layer={} num={} candidates={}", layer, candidates.size(),
                                String.join(",", candidateStringsBuilder.build()));
                    }
                    return candidates;
                });
    }

    /**
     * Repair a neighbor node of the node that is being deleted using a set of candidates. All candidates contain only
     * the vector (in addition to identifying information like the primary key). The logic in
     * computes distances between the neighbor vector and each candidate vector which is required by
     * {@link #repairInsForNeighborNode}.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param storageTransform the storage transform
     * @param estimator an estimator for distances
     * @param layer the layer
     * @param neighborReference the reference for which this method repairs incoming references
     * @param candidates the set of candidates
     * @param neighborChangeSetMap the change set map which records all changes to all nodes that are being repaired
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    private <N extends NodeReference> @Nonnull CompletableFuture<Void>
            repairNeighbor(@Nonnull final StorageAdapter<N> storageAdapter,
                           @Nonnull final Transaction transaction,
                           @Nonnull final StorageTransform storageTransform,
                           @Nonnull final Estimator estimator,
                           final int layer,
                           @Nonnull final N neighborReference,
                           @Nonnull final Collection<NodeReferenceAndNode<NodeReferenceWithVector, N>> candidates,
                           @Nonnull final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap,
                           @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {

        return primitives().fetchNodeIfNotCached(storageAdapter, transaction,
                storageTransform, layer, neighborReference, nodeCache)
                .thenCompose(neighborNode -> {
                    final ImmutableList.Builder<NodeReferenceWithDistance> candidatesReferencesBuilder =
                            ImmutableList.builder();
                    final Transformed<RealVector> neighborVector =
                            storageAdapter.getVector(neighborReference, neighborNode);
                    // transform the NodeReferencesWithVectors into NodeReferencesWithDistance
                    for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
                        // do not add the candidate if that candidate is in fact the neighbor itself
                        if (!candidate.getNodeReference().getPrimaryKey().equals(neighborReference.getPrimaryKey())) {
                            final Transformed<RealVector> candidateVector =
                                    candidate.getNodeReference().getVector();
                            final double distance =
                                    estimator.distance(candidateVector, neighborVector);
                            candidatesReferencesBuilder.add(new NodeReferenceWithDistance(
                                    candidate.getNode().getPrimaryKey(), candidateVector, distance));
                        }
                    }
                    return repairInsForNeighborNode(storageAdapter, transaction, storageTransform, estimator,
                            layer, neighborReference, candidatesReferencesBuilder.build(),
                            neighborChangeSetMap, nodeCache);
                });
    }

    /**
     * Repairs the ins of a neighbor node of the node that is being deleted using a set of candidates. Each such
     * neighbor is part of a set that is referred to as {@code p_out} in literature. In this method we only repair
     * incoming references to this node. As this method is called once per direct neighbor and all direct neighbors are
     * in the candidate set, outgoing references from this node to other nodes (in {@code p_out}) are repaired when this
     * method is called for the respective neighbors.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param storageTransform the storage transform
     * @param estimator an estimator for distances
     * @param layer the layer
     * @param neighborReference the reference for which this method repairs incoming references
     * @param candidates the set of candidates
     * @param neighborChangeSetMap the change set map which records all changes to all nodes that are being repaired
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    private <N extends NodeReference> CompletableFuture<Void>
            repairInsForNeighborNode(@Nonnull final StorageAdapter<N> storageAdapter,
                                     @Nonnull final Transaction transaction,
                                     @Nonnull final StorageTransform storageTransform,
                                     @Nonnull final Estimator estimator,
                                     final int layer,
                                     @Nonnull final N neighborReference,
                                     @Nonnull final Iterable<NodeReferenceWithDistance> candidates,
                                     @Nonnull final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap,
                                     final Map<Tuple, AbstractNode<N>> nodeCache) {
        return primitives().selectCandidates(storageAdapter, transaction, storageTransform, estimator, candidates,
                layer, getConfig().getM(), nodeCache)
                .thenApply(selectedCandidates -> {
                    if (logger.isTraceEnabled()) {
                        final ImmutableList.Builder<String> candidateStringsBuilder = ImmutableList.builder();
                        for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> candidate : selectedCandidates) {
                            candidateStringsBuilder.add(candidate.getNode().getPrimaryKey().toString());
                        }
                        logger.trace("selected for neighbor={}, candidates={}",
                                neighborReference.getPrimaryKey(),
                                String.join(",", candidateStringsBuilder.build()));
                    }
                    return selectedCandidates;
                })
                .thenCompose(selectedCandidates -> {
                    // create change sets for each selected neighbor and insert new node into them
                    for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> selectedCandidate : selectedCandidates) {
                        neighborChangeSetMap.compute(selectedCandidate.getNode().getPrimaryKey(),
                                (ignored, oldChangeSet) -> {
                                    Objects.requireNonNull(oldChangeSet);
                                    // insert a reference to the neighbor
                                    return new InsertNeighborsChangeSet<>(oldChangeSet, ImmutableList.of(neighborReference));
                                });
                    }
                    return AsyncUtil.DONE;
                });
    }

    /**
     * Predicate to determine if a potential candidate is to be used as a candidate for repairing the HNSW.
     * The predicate rejects the candidate reference if it is referring to the node that is being deleted, otherwise the
     * predicate accepts the candidate reference.
     * @param candidateReference a potential candidate that is either accepted or rejected
     * @param toBeDeletedPrimaryKey the {@link Tuple} representing the node that is being deleted
     * @return {@code true} iff {@code candidateReference} is accepted as an actual candidate for repair.
     */
    private boolean shouldUsePrimaryCandidateForRepair(@Nonnull final NodeReference candidateReference,
                                                       @Nonnull final Tuple toBeDeletedPrimaryKey) {
        final Tuple candidatePrimaryKey = candidateReference.getPrimaryKey();

        //
        // If the node reference is the record we are trying to delete we must reject it here as it is not a suitable
        // candidate.
        //
        return !candidatePrimaryKey.equals(toBeDeletedPrimaryKey);
    }

    /**
     * Predicate to determine if a potential candidate is to be used ad a candidate for repairing the HNSW.
     * <ol>
     *    <li> The predicate rejects the candidate reference if it is referring to the node that is being deleted. </li>
     *    <li> The predicate always accepts a direct neighbor of the node that is about to be deleted.</li>
     *    <li> Sample the remaining potential candidates such that eventually the repair algorithm can use
     *         roughly {@code efRepair} actual candidates.</li>
     * </ol>
     * @param random the PRNG to be used (splittable)
     * @param initialNodeKeys a set of {@link Tuple}s that hold the primary neighbors of the node being deleted.
     * @param numberOfCandidates the number of potential candidates the repair algorithm compiled
     * @param candidateReference a potential candidate that is either accepted or rejected
     * @param toBeDeletedPrimaryKey the {@link Tuple} representing the node that is being deleted
     * @return {@code true} iff {@code candidateReference} is accepted as an actual candidate for repair.
     */
    private boolean shouldUseSecondaryCandidateForRepair(@Nullable final SplittableRandom random,
                                                         @Nonnull final Set<Tuple> initialNodeKeys,
                                                         final int numberOfCandidates,
                                                         @Nonnull final NodeReference candidateReference,
                                                         @Nonnull final Tuple toBeDeletedPrimaryKey) {
        final Tuple candidatePrimaryKey = candidateReference.getPrimaryKey();

        //
        // If the node reference is the record we are trying to delete we must reject it here as it is not a suitable
        // candidate.
        //
        if (candidatePrimaryKey.equals(toBeDeletedPrimaryKey)) {
            return false;
        }

        //
        // If the node reference is among the initial nodes we must accept it as they are very likely the best
        // candidates.
        //
        if (initialNodeKeys.contains(candidatePrimaryKey)) {
            return true;
        }

        //
        // Sample all the rest -- For the sampling rate, subtract the size of initialNodeKeys so that we get roughly
        // efRepair nodes.
        //
        final double sampleRate = (double)(getConfig().getEfRepair() - initialNodeKeys.size()) / numberOfCandidates;
        if (sampleRate >= 1) {
            return true;
        }
        return Objects.requireNonNull(random).nextDouble() < sampleRate;
    }
}
