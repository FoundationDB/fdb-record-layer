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
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
        final SplittableRandom random = RandomHelpers.random(primaryKey);
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
        return RandomHelpers.forEach(random, () -> IntStream.rangeClosed(0, topLayer).iterator(),
                (layer, nestedRandom) ->
                        deleteFromLayer(primitives().storageAdapterForLayer(layer), transaction, storageTransform,
                                quantizer, nestedRandom, layer, primaryKey),
                getConfig().maxNumConcurrentDeleteFromLayer(),
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
        final DistanceEstimator distanceEstimator = quantizer.estimator();
        final Map<Tuple, AbstractNode<N>> nodeCache = Maps.newConcurrentMap();
        //
        // Per direct neighbor of the node being deleted, the nearest candidate the selection picked for it. That is
        // the target of that neighbor's replacement edge, recorded here so that addReplacementOutEdges does not have
        // to recompute distances that the repair already computed.
        //
        final Map<Tuple /* primaryKey */, NodeReferenceWithDistance> nearestSelectedCandidates =
                Maps.newConcurrentMap();

        return storageAdapter.fetchNode(transaction, storageTransform, layer, toBeDeletedPrimaryKey)
                .thenCompose(toBeDeletedNode -> {
                    final NodeReferenceAndNode<NodeReference, N> toBeDeletedNodeReferenceAndNode =
                            new NodeReferenceAndNode<>(new NodeReference(toBeDeletedPrimaryKey), toBeDeletedNode);

                    return findDeletionRepairCandidates(storageAdapter, transaction, storageTransform, random, layer,
                            toBeDeletedNodeReferenceAndNode, nodeCache)
                            .thenCompose(candidates -> {
                                final RepairContext<N> repairContext =
                                        buildRepairContext(toBeDeletedPrimaryKey, toBeDeletedNode,
                                                candidates);
                                final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap =
                                        repairContext.candidateChangeSets();
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
                                                        storageTransform, distanceEstimator, layer, neighborReference,
                                                        candidates, candidateChangeSetMap, nearestSelectedCandidates,
                                                        nodeCache),
                                        getConfig().maxNumConcurrentNeighborhoodFetches(), getExecutor())
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
                                        })
                                        .thenCompose(candidateReferencesMap -> {
                                            final int currentMMax =
                                                    layer == 0 ? getConfig().mMax0() : getConfig().mMax();

                                            //
                                            // If we previously went beyond the mMax/mMax0, we need to prune the
                                            // neighbors. Pruning is independent among different nodes -- we can
                                            // therefore prune in parallel.
                                            //
                                            return forEach(candidateChangeSetMap.entrySet(), // each modified set
                                                    changeSetEntry -> {
                                                        final NodeReferenceWithVector candidateReference =
                                                                Objects.requireNonNull(candidateReferencesMap.get(changeSetEntry.getKey()));
                                                        final NeighborsChangeSet<N> candidateChangeSet = changeSetEntry.getValue();
                                                        return primitives.pruneNeighborsIfNecessary(storageAdapter,
                                                                transaction, storageTransform, distanceEstimator, layer,
                                                                candidateReference, currentMMax, candidateChangeSet,
                                                                nodeCache)
                                                                .thenApply(nodeReferencesAndNodes -> {
                                                                    if (nodeReferencesAndNodes == null) {
                                                                        return candidateChangeSet;
                                                                    }

                                                                    final var prunedCandidateChangeSet =
                                                                            primitives.resolveChangeSetFromNewNeighbors(candidateChangeSet,
                                                                                    nodeReferencesAndNodes);
                                                                    candidateChangeSetMap.put(changeSetEntry.getKey(),
                                                                            prunedCandidateChangeSet);
                                                                    return prunedCandidateChangeSet;
                                                                });
                                                    },
                                                    getConfig().maxNumConcurrentNeighborhoodFetches(), getExecutor())
                                                    .thenApply(ignored -> candidateReferencesMap);
                                        })
                                        .thenApply(candidateReferencesMap -> {
                                            //
                                            // Every repair and the pruning have completed, so each change set now
                                            // reflects the neighbor list as it will be persisted. This is therefore the
                                            // point at which a node's remaining out-degree can be decided, and it runs
                                            // on a single thread.
                                            //
                                            addReplacementOutEdges(layer, distanceEstimator, repairContext,
                                                    nearestSelectedCandidates, candidateReferencesMap, nodeCache);

                                            //
                                            // Finally delete the node we set out to delete and persist the change sets
                                            // for all repaired nodes.
                                            //
                                            storageAdapter.deleteNode(transaction, layer, toBeDeletedPrimaryKey);

                                            for (final Map.Entry<Tuple, NeighborsChangeSet<N>> changeSetEntry
                                                    : candidateChangeSetMap.entrySet()) {
                                                final NeighborsChangeSet<N> changeSet = changeSetEntry.getValue();
                                                if (changeSet.hasChanges()) {
                                                    final AbstractNode<N> candidateNode =
                                                            primitives.nodeFromCache(changeSetEntry.getKey(), nodeCache);
                                                    storageAdapter.writeNode(transaction, quantizer, layer,
                                                            candidateNode, changeSet);
                                                }
                                            }

                                            //
                                            // Return the first item in the candidates reference map as a potential new
                                            // entry node reference in order to avoid a costly search for a new global
                                            // entry point. This reference is guaranteed to exist.
                                            //
                                            final Tuple firstPrimaryKey =
                                                    Iterables.getFirst(candidateReferencesMap.keySet(), null);
                                            return firstPrimaryKey == null
                                                   ? null
                                                   : new EntryNodeReference(firstPrimaryKey,
                                                    Objects.requireNonNull(candidateReferencesMap.get(firstPrimaryKey)).getVector(),
                                                    layer);
                                        });
                            });
                }).thenApply(result -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("end delete key={} at layer={}", toBeDeletedPrimaryKey, layer);
                    }
                    return result;
                });
    }

    /**
     * Establishes what one layer's repair needs to know about its candidates: a pending neighbor list for each of
     * them, already carrying the removal of the reference to the node being deleted, and which of them that removal
     * applied to.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param toBeDeletedPrimaryKey the primary key of the node being deleted
     * @param toBeDeletedNode the node being deleted
     * @param candidates the repair candidates of this layer
     * @return the change sets and the candidates that lost their reference to the node being deleted
     */
    @Nonnull
    private <N extends NodeReference> RepairContext<N>
            buildRepairContext(@Nonnull final Tuple toBeDeletedPrimaryKey,
                                            @Nonnull final AbstractNode<N> toBeDeletedNode,
                                            @Nonnull final List<NodeReferenceAndNode<NodeReferenceWithVector, N>> candidates) {
        final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> candidateChangeSetMap = Maps.newConcurrentMap();
        final Set<Tuple /* primaryKey */> candidatesThatLostTheirEdge = Sets.newLinkedHashSet();
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
                    candidatesThatLostTheirEdge.add(candidateNode.getPrimaryKey());
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
        return new RepairContext<>(candidateChangeSetMap, candidatesThatLostTheirEdge);
    }

    /**
     * What {@link #buildRepairContext} establishes about the candidates of one layer's repair, so that
     * the two parts of it travel together rather than as separate arguments.
     * <p>
     * Both components are mutable and are filled out further as the repair proceeds: the change sets accumulate the
     * edges each step grants, and the repair reads them back once every step has completed. This record is therefore a
     * grouping of state belonging to a single {@code deleteFromLayer} call, not a value.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param candidateChangeSets the pending neighbor list of every candidate, keyed by primary key, holding the
     *        removal of the reference to the node being deleted and every change made after that
     * @param candidatesThatLostTheirEdge the candidates that held a reference to the node being deleted, so each of
     *        them ends this delete with one outgoing edge fewer than it started with unless something replaces it
     */
    private record RepairContext<N extends NodeReference>(
            @Nonnull Map<Tuple, NeighborsChangeSet<N>> candidateChangeSets,
            @Nonnull Set<Tuple> candidatesThatLostTheirEdge) {
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
     * @param distanceEstimator an estimator for distances
     * @param layer the layer
     * @param neighborReference the reference for which this method repairs incoming references
     * @param candidates the set of candidates
     * @param neighborChangeSetMap the change set map which records all changes to all nodes that are being repaired
     * @param nearestSelectedCandidates collects, per repaired neighbor, the nearest candidate the selection picked
     *        for it, for {@link #addReplacementOutEdges} to use as a replacement target
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    private <N extends NodeReference> @Nonnull CompletableFuture<Void>
            repairNeighbor(@Nonnull final StorageAdapter<N> storageAdapter,
                           @Nonnull final Transaction transaction,
                           @Nonnull final StorageTransform storageTransform,
                           @Nonnull final DistanceEstimator distanceEstimator,
                           final int layer,
                           @Nonnull final N neighborReference,
                           @Nonnull final Collection<NodeReferenceAndNode<NodeReferenceWithVector, N>> candidates,
                           @Nonnull final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap,
                           @Nonnull final Map<Tuple /* primaryKey */, NodeReferenceWithDistance> nearestSelectedCandidates,
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
                                    distanceEstimator.distance(candidateVector, neighborVector);
                            candidatesReferencesBuilder.add(new NodeReferenceWithDistance(
                                    candidate.getNode().getPrimaryKey(), candidateVector, distance));
                        }
                    }
                    return repairInsForNeighborNode(storageAdapter, transaction, storageTransform, distanceEstimator,
                            layer, neighborReference, candidatesReferencesBuilder.build(), neighborChangeSetMap,
                            nearestSelectedCandidates, nodeCache);
                });
    }

    /**
     * Repairs the ins of a neighbor node of the node that is being deleted using a set of candidates. Each such
     * neighbor is part of a set that is referred to as {@code p_out} in literature. In this method we only repair
     * incoming references to this node. As this method is called once per direct neighbor and all direct neighbors are
     * in the candidate set, outgoing references from this node to other nodes (in {@code p_out}) are repaired when this
     * method is called for the respective neighbors.
     * <p>
     * This method does not give the neighbor any outgoing edge. The edge it held to the node being deleted was removed
     * in {@link #buildRepairContext}, and {@link #addReplacementOutEdges} decides once, after every
     * repair has completed, whether to replace it.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param storageTransform the storage transform
     * @param distanceEstimator an estimator for distances
     * @param layer the layer
     * @param neighborReference the reference for which this method repairs incoming references
     * @param candidates the set of candidates
     * @param neighborChangeSetMap the change set map which records all changes to all nodes that are being repaired
     * @param nearestSelectedCandidates collects, per repaired neighbor, the nearest candidate the selection picked
     *        for it, for {@link #addReplacementOutEdges} to use as a replacement target
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    private <N extends NodeReference> CompletableFuture<Void>
            repairInsForNeighborNode(@Nonnull final StorageAdapter<N> storageAdapter,
                                     @Nonnull final Transaction transaction,
                                     @Nonnull final StorageTransform storageTransform,
                                     @Nonnull final DistanceEstimator distanceEstimator,
                                     final int layer,
                                     @Nonnull final N neighborReference,
                                     @Nonnull final Iterable<NodeReferenceWithDistance> candidates,
                                     @Nonnull final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap,
                                     @Nonnull final Map<Tuple /* primaryKey */, NodeReferenceWithDistance> nearestSelectedCandidates,
                                     final Map<Tuple, AbstractNode<N>> nodeCache) {
        return primitives().selectCandidates(storageAdapter, transaction, storageTransform, distanceEstimator, candidates,
                layer, getConfig().m(), nodeCache)
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

                    //
                    // Record the nearest of the selected candidates. If this neighbor ends the delete short of
                    // outgoing edges, that candidate is the target of its replacement edge: it is the closest node
                    // the selection considered good enough to connect to this neighbor, so an edge to it is the
                    // shortest one available and therefore the one the trimming rule is least likely to remove.
                    //
                    NodeReferenceWithDistance nearestSelectedCandidate = null;
                    for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> selectedCandidate : selectedCandidates) {
                        final NodeReferenceWithDistance selectedReference = selectedCandidate.getNodeReference();
                        if (nearestSelectedCandidate == null
                                || selectedReference.getDistance() < nearestSelectedCandidate.getDistance()) {
                            nearestSelectedCandidate = selectedReference;
                        }
                    }
                    if (nearestSelectedCandidate != null) {
                        nearestSelectedCandidates.put(neighborReference.getPrimaryKey(), nearestSelectedCandidate);
                    }
                    return AsyncUtil.DONE;
                });
    }

    /**
     * Grants one replacement outgoing edge to each node that lost its reference to the node being deleted and is left
     * short of outgoing edges.
     * <p>
     * A single delete can cost a node at most one outgoing edge, namely the one reference it held to the node being
     * deleted, because a neighbor list holds at most one reference per primary key.
     * {@link #repairInsForNeighborNode} only ever grants <em>incoming</em> edges, so without this step that one edge
     * is never replaced, and a node that participates in many deletes runs out of outgoing edges. Whether that node is
     * a direct neighbor of the deleted node or only a second degree candidate, the loss is the same.
     * <p>
     * This runs after every repair and the pruning have completed, which has three consequences. The out-degree read
     * here already accounts for the edges the repair granted, so a node the repair happened to compensate is not
     * compensated twice. The decision is made on one thread, so it does not depend on the order in which the repairs
     * completed. And a granted edge raises the out-degree to at most
     * {@link Config#replacementEdgeMaxOutDegree()}, which is below {@code mMax}, so it can never take a node past the
     * degree cap and no further pruning is required.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param layer the layer
     * @param distanceEstimator an estimator for distances
     * @param repairContext the change sets and the candidates whose reference to the node being deleted was removed;
     *        the change sets are updated in place for every node granted an edge
     * @param nearestSelectedCandidates the nearest candidate the selection picked per repaired direct neighbor
     * @param candidateReferencesMap the surviving candidates with their vectors, keyed by primary key
     * @param nodeCache the node cache, which holds a node for every candidate
     */
    private <N extends NodeReference> void
            addReplacementOutEdges(final int layer,
                                   @Nonnull final DistanceEstimator distanceEstimator,
                                   @Nonnull final RepairContext<N> repairContext,
                                   @Nonnull final Map<Tuple, NodeReferenceWithDistance> nearestSelectedCandidates,
                                   @Nonnull final Map<Tuple, NodeReferenceWithVector> candidateReferencesMap,
                                   @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final int maxOutDegree = getConfig().replacementEdgeMaxOutDegree();
        if (maxOutDegree <= 0) {
            return;
        }
        final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap = repairContext.candidateChangeSets();
        for (final Tuple primaryKey : repairContext.candidatesThatLostTheirEdge()) {
            final NeighborsChangeSet<N> changeSet = candidateChangeSetMap.get(primaryKey);
            final NodeReferenceWithVector reference = candidateReferencesMap.get(primaryKey);
            if (changeSet == null || reference == null) {
                // the node did not survive as a candidate, so there is nothing to write a replacement edge into
                continue;
            }

            if (changeSet.size() >= maxOutDegree) {
                continue;
            }

            //
            // Prefer the candidate the selection already picked for this node, which the repair recorded. That only
            // exists for direct neighbors of the deleted node, and it is unusable if the node points at it already,
            // in which case the nearest candidate it does not point at is computed here.
            //
            NodeReferenceWithDistance target = nearestSelectedCandidates.get(primaryKey);
            if (target == null || changeSet.containsNeighbor(target.getPrimaryKey())) {
                target = nearestUnreferencedCandidate(reference, changeSet, candidateReferencesMap,
                        distanceEstimator);
            }
            if (target == null) {
                continue;
            }

            final Tuple targetPrimaryKey = target.getPrimaryKey();
            final N targetReference =
                    primitives().nodeFromCache(targetPrimaryKey, nodeCache).getSelfReference(target.getVector());
            candidateChangeSetMap.put(primaryKey,
                    new InsertNeighborsChangeSet<>(changeSet, ImmutableList.of(targetReference)));
            if (logger.isTraceEnabled()) {
                logger.trace("replaced outgoing edge of key={} with an edge to key={} on layer={}", primaryKey,
                        targetPrimaryKey, layer);
            }
        }
    }

    /**
     * Returns the candidate closest to {@code reference} that it does not already point at, or {@code null} if there is
     * none. Used for the nodes {@link #repairInsForNeighborNode} never ran for, i.e. those that pointed at the node
     * being deleted without being pointed at by it.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param reference the node a replacement edge is being chosen for
     * @param changeSet that node's pending neighbor list, consulted for the nodes it already points at
     * @param candidateReferencesMap the surviving candidates with their vectors, keyed by primary key
     * @param distanceEstimator an estimator for distances
     * @return the closest candidate not already pointed at, or {@code null}
     */
    @Nullable
    private static <N extends NodeReference> NodeReferenceWithDistance
            nearestUnreferencedCandidate(@Nonnull final NodeReferenceWithVector reference,
                                         @Nonnull final NeighborsChangeSet<N> changeSet,
                                         @Nonnull final Map<Tuple, NodeReferenceWithVector> candidateReferencesMap,
                                         @Nonnull final DistanceEstimator distanceEstimator) {
        NodeReferenceWithDistance nearest = null;
        for (final NodeReferenceWithVector candidate : candidateReferencesMap.values()) {
            final Tuple candidatePrimaryKey = candidate.getPrimaryKey();
            if (candidatePrimaryKey.equals(reference.getPrimaryKey())
                    || changeSet.containsNeighbor(candidatePrimaryKey)) {
                continue;
            }
            final double distance = distanceEstimator.distance(candidate.getVector(), reference.getVector());
            if (nearest == null || distance < nearest.getDistance()) {
                nearest = new NodeReferenceWithDistance(candidatePrimaryKey, candidate.getVector(), distance);
            }
        }
        return nearest;
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
        final double sampleRate = (double)(getConfig().efRepair() - initialNodeKeys.size()) / numberOfCandidates;
        if (sampleRate >= 1) {
            return true;
        }
        return Objects.requireNonNull(random).nextDouble() < sampleRate;
    }
}
