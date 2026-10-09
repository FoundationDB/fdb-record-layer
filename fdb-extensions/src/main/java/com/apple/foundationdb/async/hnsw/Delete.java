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
import com.apple.foundationdb.async.AsyncIterable;
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

                    //
                    // Only a delete of the entry node has to find a replacement for it, so only such a delete has the
                    // layers choose one, which may read extra nodes. Every other delete skips that.
                    //
                    final boolean isDeletingEntryNode =
                            entryNodeReference != null && primaryKey.equals(entryNodeReference.getPrimaryKey());

                    return deleteFromLayers(transaction, storageTransform, quantizer, random, primaryKey, topLayer,
                            isDeletingEntryNode)
                            .thenCompose(potentialEntryNodeReferences -> {
                                if (isDeletingEntryNode) {
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
     * @param needNewEntryNode whether the node being deleted is the entry node, so that each layer has to offer a
     *        replacement for it; if not, every layer offers {@code null}
     *
     * @return a {@link CompletableFuture} of what each layer offers as a replacement entry node, one element per layer
     *         in layer order, each {@code null} if that layer offers none or {@code needNewEntryNode} is {@code false}
     */
    @Nonnull
    private CompletableFuture<List<EntryNodeReference>> deleteFromLayers(@Nonnull final Transaction transaction,
                                                                         @Nonnull final StorageTransform storageTransform,
                                                                         @Nonnull final Quantizer quantizer,
                                                                         @Nonnull final SplittableRandom random,
                                                                         @Nonnull final Tuple primaryKey,
                                                                         final int topLayer,
                                                                         final boolean needNewEntryNode) {
        // delete the node from all layers in parallel (inside layer in [0, topLayer])
        return RandomHelpers.forEach(random, () -> IntStream.rangeClosed(0, topLayer).iterator(),
                (layer, nestedRandom) ->
                        deleteFromLayer(primitives().storageAdapterForLayer(layer), transaction, storageTransform,
                                quantizer, nestedRandom, layer, primaryKey, needNewEntryNode),
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
     * @param needNewEntryNode whether the node being deleted is the entry node, so that this layer has to offer a
     *        replacement for it; if not, this layer offers {@code null}
     *
     * @return a {@code CompletableFuture} of the node this layer offers as a replacement entry node, or of {@code null}
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<EntryNodeReference>
            deleteFromLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final Transaction transaction,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final Quantizer quantizer,
                            @Nonnull final SplittableRandom random,
                            final int layer,
                            @Nonnull final Tuple toBeDeletedPrimaryKey,
                            final boolean needNewEntryNode) {
        if (logger.isTraceEnabled()) {
            logger.trace("begin delete key={} at layer={}", toBeDeletedPrimaryKey, layer);
        }
        final Primitives primitives = primitives();
        final DistanceEstimator distanceEstimator = quantizer.estimator();
        final Map<Tuple, AbstractNode<N>> nodeCache = Maps.newConcurrentMap();
        //
        // Per direct neighbor of the node being deleted, the nearest candidate that the neighbor did not point at before
        // this delete. That is the target of the neighbor's replacement edge, should addReplacementOutEdges grant it
        // one. The repair records it while it computes the distances from the neighbor to every candidate, so that
        // addReplacementOutEdges does not have to compute them again.
        //
        final Map<Tuple /* primaryKey */, NodeReferenceWithDistance> replacementTargets = Maps.newConcurrentMap();
        //
        // References that the candidate search read storage for and found no node under. They name nodes an earlier
        // delete removed without removing the references to them, because the node holding the reference was outside
        // that delete's candidate set. Gathering them costs nothing: the reads happen either way.
        //
        final Set<Tuple /* primaryKey */> provenAbsentPrimaryKeys = Sets.newConcurrentHashSet();

        return storageAdapter.fetchNode(transaction, storageTransform, layer, toBeDeletedPrimaryKey)
                .thenCompose(toBeDeletedNode -> {
                    final NodeReferenceAndNode<NodeReference, N> toBeDeletedNodeReferenceAndNode =
                            new NodeReferenceAndNode<>(new NodeReference(toBeDeletedPrimaryKey), toBeDeletedNode);

                    return findDeletionRepairCandidates(storageAdapter, transaction, storageTransform, random, layer,
                            toBeDeletedNodeReferenceAndNode, nodeCache, provenAbsentPrimaryKeys)
                            .thenCompose(candidates -> {
                                final RepairContext<N> repairContext =
                                        buildRepairContext(toBeDeletedPrimaryKey, toBeDeletedNode, candidates,
                                                provenAbsentPrimaryKeys, layer);
                                final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap =
                                        repairContext.candidateChangeSets();
                                getOnWriteListener().onNeighborReferencesReaped(layer,
                                        repairContext.numReapedReferences());
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
                                                        candidates, candidateChangeSetMap, replacementTargets,
                                                        nodeCache),
                                        getConfig().maxNumConcurrentNeighborhoodFetches(), getExecutor())
                                        .thenApply(ignored -> {
                                            //
                                            // Every repair has completed and the pruning has not run yet. A node that
                                            // lost its reference to the node being deleted has more neighbors than
                                            // right after that loss exactly if a repair granted it an edge to a node it
                                            // did not point at yet. That edge replaces the lost one, so the node needs
                                            // no replacement edge. This is decided here because the pruning, which runs
                                            // next, can remove that edge again, after which the count no longer shows
                                            // it.
                                            //
                                            repairContext.candidatesThatLostTheirEdge().entrySet()
                                                    .removeIf(lostEdgeEntry -> Objects.requireNonNull(
                                                            candidateChangeSetMap.get(lostEdgeEntry.getKey())).size()
                                                            > lostEdgeEntry.getValue());

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
                                            //
                                            // If we previously went beyond the mMax/mMax0, we need to prune the
                                            // neighbors. Pruning is independent among different nodes -- we can
                                            // therefore prune in parallel.
                                            //
                                            final int mMax = primitives.getMMaxForLayer(layer);
                                            return forEach(candidateChangeSetMap.entrySet(), // each modified set
                                                    changeSetEntry -> {
                                                        final NodeReferenceWithVector candidateReference =
                                                                Objects.requireNonNull(candidateReferencesMap.get(changeSetEntry.getKey()));
                                                        final NeighborsChangeSet<N> candidateChangeSet = changeSetEntry.getValue();
                                                        return primitives.pruneNeighborsIfNecessary(storageAdapter,
                                                                transaction, storageTransform, distanceEstimator, layer,
                                                                candidateReference, mMax,
                                                                candidateChangeSet, nodeCache)
                                                                .thenApply(nodeReferencesAndNodes -> {
                                                                    if (nodeReferencesAndNodes == null) {
                                                                        return candidateChangeSet;
                                                                    }

                                                                    final NeighborsChangeSet<N> prunedCandidateChangeSet =
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
                                        .thenCompose(candidateReferencesMap -> {
                                            //
                                            // Every repair and the pruning have completed, so each change set now
                                            // reflects the neighbor list as it will be persisted. This is therefore the
                                            // point at which a node's remaining out-degree can be decided.
                                            //
                                            addReplacementOutEdges(layer, distanceEstimator, repairContext,
                                                    replacementTargets, candidateReferencesMap, nodeCache);

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

                                            if (!needNewEntryNode) {
                                                //
                                                // If the caller does not need a new entry node, we can just return
                                                // a future producing null here.
                                                //
                                                return CompletableFuture.completedFuture(null);
                                            }

                                            //
                                            // Hand the promotion a node on this layer that can actually be traversed
                                            // through. The neighbors of the node being deleted are preferred, since
                                            // their vectors are already in hand, but one of them is only useful if it
                                            // still has an outgoing edge; otherwise promoting it would leave every
                                            // search returning it alone. A null here therefore means this layer holds
                                            // no node at all, which is what makes the decision further up -- to
                                            // discard the access info -- correct rather than merely inferred.
                                            //
                                            return chooseEntryNodeCandidate(storageAdapter, transaction, layer,
                                                    candidateReferencesMap, candidateChangeSetMap);
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
     * Chooses the node this layer offers the promotion as a replacement entry node, or {@code null} if the layer holds
     * no node at all. It is only called for a delete of the entry node, the only delete whose offers are used.
     * <p>
     * A node is only useful as an entry node if it has an outgoing edge, since a search starting at a node with none
     * returns that node and nothing else. The neighbors of the node being deleted are considered first because their
     * vectors are already in hand and their pending neighbor lists are already known, so that check costs nothing.
     * Only when none of them can be traversed through does this read further nodes of the layer, bounded by
     * {@link Config#replacementEntryNodeScanLimit()}.
     * <p>
     * Returning {@code null} only when the layer is empty is what lets the caller treat "no layer offered anything" as
     * "the structure is empty" rather than inferring it.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param layer the layer
     * @param candidateReferencesMap the candidates with their vectors, keyed by primary key
     * @param candidateChangeSetMap the change sets as they will be persisted, for the out-degree check
     * @return a future of the node to offer the promotion, or of {@code null} if this layer holds no node
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<EntryNodeReference>
            chooseEntryNodeCandidate(@Nonnull final StorageAdapter<N> storageAdapter,
                                     @Nonnull final Transaction transaction,
                                     final int layer,
                                     @Nonnull final Map<Tuple, NodeReferenceWithVector> candidateReferencesMap,
                                     @Nonnull final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap) {
        NodeReferenceWithVector traversableCandidate = null;
        NodeReferenceWithVector anyCandidate = null;
        for (final Map.Entry<Tuple, NodeReferenceWithVector> entry : candidateReferencesMap.entrySet()) {
            if (anyCandidate == null) {
                anyCandidate = entry.getValue();
            }
            final NeighborsChangeSet<N> changeSet = candidateChangeSetMap.get(entry.getKey());
            if (changeSet != null && changeSet.size() > 0) {
                traversableCandidate = entry.getValue();
                break;
            }
        }
        if (traversableCandidate != null) {
            return CompletableFuture.completedFuture(entryNodeReferenceOrNull(traversableCandidate, layer));
        }

        //
        // No neighbor of the deleted node can be traversed through on this layer, so read a bounded number of its
        // other nodes looking for one that can. An inlining layer is not read: its scan counts edge records rather
        // than nodes, and its nodes do not carry their own vector. Layer 0 is always compact and holds every node, so
        // the promotion still finds a node by walking down to it.
        //
        final int scanLimit = getConfig().replacementEntryNodeScanLimit();
        if (scanLimit <= 0 || storageAdapter.isInliningStorageAdapter()) {
            return CompletableFuture.completedFuture(entryNodeReferenceOrNull(anyCandidate, layer));
        }

        final NodeReferenceWithVector anyCandidateFinal = anyCandidate;
        final AsyncIterable<AbstractNode<N>> layerNodes =
                (AsyncIterable<AbstractNode<N>>)storageAdapter.scanLayer(transaction, layer, null, scanLimit);
        return AsyncUtil.collect(layerNodes, getExecutor())
                .thenApply(nodes -> {
                    for (final AbstractNode<N> node : nodes) {
                        if (!node.getNeighbors().isEmpty()) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("offering scanned key={} as a replacement entry node on layer={}",
                                        node.getPrimaryKey(), layer);
                            }
                            return new EntryNodeReference(node.getPrimaryKey(), node.asCompactNode().getVector(),
                                    layer);
                        }
                    }
                    if (anyCandidateFinal != null) {
                        return entryNodeReferenceOrNull(anyCandidateFinal, layer);
                    }
                    //
                    // Nothing read on this layer can be traversed through. Offering one of them anyway is still better
                    // than offering nothing, which the caller would read as the structure being empty.
                    //
                    return nodes.isEmpty()
                           ? null
                           : new EntryNodeReference(nodes.get(0).getPrimaryKey(),
                                   nodes.get(0).asCompactNode().getVector(), layer);
                });
    }

    @Nullable
    private static EntryNodeReference entryNodeReferenceOrNull(@Nullable final NodeReferenceWithVector reference,
                                                               final int layer) {
        return reference == null
               ? null
               : new EntryNodeReference(reference.getPrimaryKey(), reference.getVector(), layer);
    }

    /**
     * Establishes what one layer's repair needs to know about its candidates: a pending neighbor list for each of
     * them, already carrying the removal of the reference to the node being deleted and of any reference naming a node
     * an earlier delete removed; which of them held a reference to the node being deleted, each with the number of
     * neighbors it is left with after both removals; and how many references naming an already deleted node were
     * removed.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param toBeDeletedPrimaryKey the primary key of the node being deleted
     * @param toBeDeletedNode the node being deleted
     * @param candidates the repair candidates of this layer
     * @param provenAbsentPrimaryKeys the references the candidate search read storage for and found to name no node
     * @param layer the layer, for logging only
     * @return the change sets, the candidates that lost their reference to the node being deleted, each with the
     *         number of neighbors it is left with, and the number of references naming an already deleted node that
     *         were removed
     */
    @Nonnull
    private <N extends NodeReference> RepairContext<N>
            buildRepairContext(@Nonnull final Tuple toBeDeletedPrimaryKey,
                               @Nonnull final AbstractNode<N> toBeDeletedNode,
                               @Nonnull final List<NodeReferenceAndNode<NodeReferenceWithVector, N>> candidates,
                               @Nonnull final Set<Tuple> provenAbsentPrimaryKeys,
                               final int layer) {
        final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> candidateChangeSetMap = Maps.newConcurrentMap();
        final Map<Tuple /* primaryKey */, Integer /* numNeighbors */> candidatesThatLostTheirEdge = Maps.newHashMap();
        int numReapedReferences = 0;
        for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
            final AbstractNode<N> candidateNode = candidate.getNode();
            //
            // Collect the references this candidate must lose: the one to the node being deleted, plus any that name a
            // node an earlier delete removed without removing the references to it. The latter are only reaped here
            // because the candidate search already read storage for them and found nothing, so they are known to be
            // dead rather than merely unaccounted for. A candidate whose own out-neighbors were never read contributes
            // nothing, which is correct -- not knowing whether a node exists is not the same as knowing it does not.
            //
            final ImmutableList.Builder<Tuple> toBeDeletedBuilder = ImmutableList.builder();
            boolean heldReferenceToDeleted = false;
            int numReapedForCandidate = 0;
            for (final N neighborOfCandidate : candidateNode.getNeighbors()) {
                final Tuple neighborPrimaryKey = neighborOfCandidate.getPrimaryKey();
                if (neighborPrimaryKey.equals(toBeDeletedPrimaryKey)) {
                    heldReferenceToDeleted = true;
                    toBeDeletedBuilder.add(neighborPrimaryKey);
                } else if (provenAbsentPrimaryKeys.contains(neighborPrimaryKey)) {
                    toBeDeletedBuilder.add(neighborPrimaryKey);
                    numReapedForCandidate ++;
                }
            }

            final ImmutableList<Tuple> toBeDeleted = toBeDeletedBuilder.build();
            final NeighborsChangeSet<N> baseChangeSet = new BaseNeighborsChangeSet<>(candidateNode.getNeighbors());
            if (toBeDeleted.isEmpty()) {
                // nothing to remove from this candidate, so leave it a base change set, which is never written
                candidateChangeSetMap.put(candidateNode.getPrimaryKey(), baseChangeSet);
            } else {
                final NeighborsChangeSet<N> changeSet = new DeleteNeighborsChangeSet<>(baseChangeSet, toBeDeleted);
                candidateChangeSetMap.put(candidateNode.getPrimaryKey(), changeSet);
                if (heldReferenceToDeleted) {
                    //
                    // Counted after the reaped references are removed as well, so that only an edge a repair grants
                    // afterwards can raise the count.
                    //
                    candidatesThatLostTheirEdge.put(candidateNode.getPrimaryKey(), changeSet.size());
                }
                numReapedReferences += numReapedForCandidate;
                if (numReapedForCandidate > 0 && logger.isTraceEnabled()) {
                    logger.trace("reaped numReferences={} naming deleted nodes from key={} on layer={}",
                            numReapedForCandidate, candidateNode.getPrimaryKey(), layer);
                }
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("number of neighbors to repair={}", toBeDeletedNode.getNeighbors().size());
        }
        return new RepairContext<>(candidateChangeSetMap, candidatesThatLostTheirEdge, numReapedReferences);
    }

    /**
     * What {@link #buildRepairContext} establishes about the candidates of one layer's repair, held in one record so
     * that it is passed as one argument rather than as separate ones.
     * <p>
     * The two collections are mutable and change as the repair proceeds: the change sets accumulate the edges each
     * step grants, and once every repair has completed, the candidates a repair granted an edge are removed from
     * {@code candidatesThatLostTheirEdge}. This record is therefore a grouping of state belonging to a single
     * {@code deleteFromLayer} call, not a value.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param candidateChangeSets the pending neighbor list of every candidate, keyed by primary key, holding the
     *        removal of the reference to the node being deleted and every change made after that
     * @param candidatesThatLostTheirEdge the candidates that held a reference to the node being deleted, each with the
     *        number of neighbors it has right after this delete removed that reference and any reference naming an
     *        already deleted node. A candidate whose number of neighbors has grown once every repair has completed
     *        received an edge from a repair, which replaces the lost one, and is removed at that point. Every
     *        candidate left has lost one usable outgoing edge in this delete, unless {@link #addReplacementOutEdges}
     *        replaces it; the reaped references named nodes that no longer exist
     * @param numReapedReferences how many references naming a node an earlier delete removed were removed here, which
     *        may be zero
     */
    private record RepairContext<N extends NodeReference>(
            @Nonnull Map<Tuple, NeighborsChangeSet<N>> candidateChangeSets,
            @Nonnull Map<Tuple, Integer> candidatesThatLostTheirEdge,
            int numReapedReferences) {
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
     * @param provenAbsentPrimaryKeys collects the primary keys of references that were read from storage and found to
     *        name no node, i.e. references left behind by an earlier delete
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
                                          final Map<Tuple, AbstractNode<N>> nodeCache,
                                          final Set<Tuple> provenAbsentPrimaryKeys) {
        final Primitives primitives = primitives();
        return primitives.neighbors(storageAdapter, transaction, storageTransform, random,
                ImmutableList.of(toBeDeletedNodeReferenceAndNode),
                ((r, initialNodeKeys, size, nodeReference) ->
                         shouldUsePrimaryCandidateForRepair(nodeReference,
                                 toBeDeletedNodeReferenceAndNode.getNodeReference().getPrimaryKey())), layer, nodeCache,
                provenAbsentPrimaryKeys::add)
                .thenCompose(candidates ->
                        primitives.neighbors(storageAdapter, transaction, storageTransform, random,
                                candidates,
                                ((r, initialNodeKeys, size, nodeReference) ->
                                         shouldUseSecondaryCandidateForRepair(r, initialNodeKeys, size, nodeReference,
                                                 toBeDeletedNodeReferenceAndNode.getNodeReference().getPrimaryKey())),
                                layer, nodeCache, provenAbsentPrimaryKeys::add))
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
     * @param replacementTargets collects, per repaired neighbor, the nearest candidate that neighbor did not point at
     *        before this delete, for {@link #addReplacementOutEdges} to use as the target of a replacement edge
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
                           @Nonnull final Map<Tuple /* primaryKey */, NodeReferenceWithDistance> replacementTargets,
                           @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {

        return primitives().fetchNodeIfNotCached(storageAdapter, transaction,
                storageTransform, layer, neighborReference, nodeCache)
                .thenCompose(neighborNode -> {
                    final ImmutableList.Builder<NodeReferenceWithDistance> candidatesReferencesBuilder =
                            ImmutableList.builder();
                    final Transformed<RealVector> neighborVector =
                            storageAdapter.getVector(neighborReference, neighborNode);
                    final Set<Tuple> outNeighborPrimaryKeys = Sets.newHashSet();
                    for (final N outNeighbor : neighborNode.getNeighbors()) {
                        outNeighborPrimaryKeys.add(outNeighbor.getPrimaryKey());
                    }
                    NodeReferenceWithDistance replacementTarget = null;
                    // transform the NodeReferencesWithVectors into NodeReferencesWithDistance
                    for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
                        // do not add the candidate if that candidate is in fact the neighbor itself
                        if (!candidate.getNodeReference().getPrimaryKey().equals(neighborReference.getPrimaryKey())) {
                            final Transformed<RealVector> candidateVector =
                                    candidate.getNodeReference().getVector();
                            final double distance =
                                    distanceEstimator.distance(candidateVector, neighborVector);
                            final NodeReferenceWithDistance candidateReference = new NodeReferenceWithDistance(
                                    candidate.getNode().getPrimaryKey(), candidateVector, distance);
                            candidatesReferencesBuilder.add(candidateReference);
                            if (!outNeighborPrimaryKeys.contains(candidateReference.getPrimaryKey())
                                    && (replacementTarget == null || distance < replacementTarget.getDistance())) {
                                replacementTarget = candidateReference;
                            }
                        }
                    }

                    //
                    // The target is the nearest candidate the neighbor did not point at before this delete.
                    // addReplacementOutEdges decides, once every repair has completed, whether the neighbor gets an
                    // edge to it.
                    //
                    if (replacementTarget != null) {
                        replacementTargets.put(neighborReference.getPrimaryKey(), replacementTarget);
                    }
                    return repairInsForNeighborNode(storageAdapter, transaction, storageTransform, distanceEstimator,
                            layer, neighborReference, candidatesReferencesBuilder.build(), neighborChangeSetMap,
                            nodeCache);
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
                    return AsyncUtil.DONE;
                });
    }

    /**
     * Grants one replacement outgoing edge to each node that lost its reference to the node being deleted, received no
     * edge from a repair in its place, and is left short of outgoing edges.
     * <p>
     * {@link #buildRepairContext} removes exactly one reference to an existing node from such a node, the one to the
     * node being deleted, because a neighbor list holds at most one reference per primary key; any other reference it
     * removes names a node that no longer exists. {@link #repairInsForNeighborNode} adds edges only towards the nodes
     * the deleted node pointed at, from the candidates it selects for each of them, so the node regains an outgoing
     * edge only if one of those repairs selects it for a node it did not point at yet. Otherwise nothing replaces the
     * lost edge, and a node that participates in many deletes can run out of outgoing edges. Whether that node is a
     * direct neighbor of the deleted node or only a second degree candidate, the loss is the same.
     * <p>
     * The candidates a repair granted such an edge were removed from the repair context before the pruning, so every
     * node considered here received none. This runs after every repair and the pruning have completed, which has two
     * consequences. The decision does not depend on the order in which the repairs completed. And a node that is granted
     * an edge ends the delete with at most as many neighbors as its stored neighbor list held before the delete, fewer
     * if references were reaped. Every write path keeps that list within the degree cap of the layer, so a granted
     * edge can never take a node past the cap and no further pruning is required, whatever the value of
     * {@link Config#replacementEdgeMaxOutDegree()}.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param layer the layer
     * @param distanceEstimator an estimator for distances
     * @param repairContext the change sets, and the candidates whose reference to the node being deleted was removed
     *        and that no repair granted an edge since; the change sets are updated in place for every node granted an
     *        edge
     * @param replacementTargets per direct neighbor of the node being deleted, the nearest candidate it did not point
     *        at before this delete
     * @param candidateReferencesMap the candidates with their vectors, keyed by primary key
     * @param nodeCache the node cache, which holds a node for every candidate
     */
    private <N extends NodeReference> void
            addReplacementOutEdges(final int layer,
                                   @Nonnull final DistanceEstimator distanceEstimator,
                                   @Nonnull final RepairContext<N> repairContext,
                                   @Nonnull final Map<Tuple, NodeReferenceWithDistance> replacementTargets,
                                   @Nonnull final Map<Tuple, NodeReferenceWithVector> candidateReferencesMap,
                                   @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final int maxOutDegree = getConfig().replacementEdgeMaxOutDegree();
        if (maxOutDegree <= 0) {
            return;
        }
        final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap = repairContext.candidateChangeSets();
        for (final Tuple primaryKey : repairContext.candidatesThatLostTheirEdge().keySet()) {
            final NeighborsChangeSet<N> changeSet = Objects.requireNonNull(candidateChangeSetMap.get(primaryKey));
            if (changeSet.size() >= maxOutDegree) {
                continue;
            }

            //
            // A direct neighbor of the deleted node has its target recorded by its repair: the nearest candidate it did
            // not point at before this delete. No repair granted this node an edge and the pruning only removes edges,
            // so it does not point at that target now either. A node that pointed at the deleted node without being
            // pointed at by it was never repaired, so its target is computed here.
            //
            NodeReferenceWithDistance target = replacementTargets.get(primaryKey);
            if (target == null) {
                target = nearestUnreferencedCandidate(Objects.requireNonNull(candidateReferencesMap.get(primaryKey)),
                        changeSet, candidateReferencesMap, distanceEstimator);
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
     * none. Used for the nodes without a recorded replacement target: those {@link #repairNeighbor} never ran for, i.e.
     * those that pointed at the node being deleted without being pointed at by it. A direct neighbor without a recorded
     * target already pointed at every candidate, so this returns {@code null} for it.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param reference the node a replacement edge is being chosen for
     * @param changeSet that node's pending neighbor list, consulted for the nodes it already points at
     * @param candidateReferencesMap the candidates with their vectors, keyed by primary key
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
