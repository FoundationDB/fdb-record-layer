/*
 * Inserter.java
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
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.ToDoubleFunction;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;
import static com.apple.foundationdb.async.MoreAsyncUtil.forLoop;

/**
 * An implementation of insert path of the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 * <p>
 * HNSW constructs a multi-layer graph, where each layer is a subset of the one below it.
 * The top layers serve as fast entry points to navigate the graph, while the bottom layer
 * contains all the data points. This structure allows for logarithmic-time complexity
 * for search operations, making it suitable for large-scale, high-dimensional datasets.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class Inserter {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Inserter.class);

    @Nonnull
    private final Locator locator;

    /**
     * Constructs a new HNSW graph instance.
     * <p>
     * This constructor initializes the HNSW graph with the necessary components for storage,
     * execution, configuration, and event handling.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Inserter(@Nonnull final Locator locator) {
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
    public Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration of this hnsw.
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
    private Primitives primitives() {
        return getLocator().primitives();
    }

    @Nonnull
    private Searcher searcher() {
        return getLocator().searcher();
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
        final Primitives primitives = primitives();
        final SplittableRandom random = Primitives.random(newPrimaryKey);
        final int insertionLayer = primitives.topLayer(newPrimaryKey);
        if (logger.isTraceEnabled()) {
            logger.trace("new node with key={} selected to be inserted into layer={}", newPrimaryKey, insertionLayer);
        }

        return StorageAdapter.fetchAccessInfo(getConfig(), transaction, getSubspace(), getOnReadListener())
                .thenCombine(primitives.exists(transaction, newPrimaryKey),
                        (accessInfo, nodeAlreadyExists) -> {
                            if (nodeAlreadyExists) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("new record already exists in HNSW with key={} on layer={}",
                                            newPrimaryKey, insertionLayer);
                                }
                            }
                            return new Primitives.AccessInfoAndNodeExistence(accessInfo, nodeAlreadyExists);
                        })
                .thenCompose(accessInfoAndNodeExistence -> {
                    if (accessInfoAndNodeExistence.isNodeExists()) {
                        return AsyncUtil.DONE;
                    }

                    final AccessInfo accessInfo = accessInfoAndNodeExistence.getAccessInfo();
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedNewVector = storageTransform.transform(newVector);
                    final Quantizer quantizer = primitives.quantizer(accessInfo);
                    final Estimator estimator = quantizer.estimator();

                    final AccessInfo currentAccessInfo;
                    if (accessInfo == null) {
                        // this is the first node
                        primitives.writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector,
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
                        return AsyncUtil.DONE;
                    } else {
                        final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
                        final int lMax = entryNodeReference.getLayer();
                        if (insertionLayer > lMax) {
                            primitives.writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector,
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
                    
                    final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
                    final int lMax = entryNodeReference.getLayer();
                    if (logger.isTraceEnabled()) {
                        logger.trace("entry node read with key {} at layer {}", entryNodeReference.getPrimaryKey(), lMax);
                    }

                    final ToDoubleFunction<Transformed<RealVector>> objectiveFunction =
                            Searcher.distanceToTargetVector(estimator, transformedNewVector);
                    final NodeReferenceWithDistance initialNodeReference =
                            new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                    entryNodeReference.getVector(),
                                    objectiveFunction.applyAsDouble(entryNodeReference.getVector()));
                    final Searcher searcher = searcher();

                    return forLoop(lMax, initialNodeReference,
                            layer -> layer > insertionLayer,
                            layer -> layer - 1,
                            (layer, previousNodeReference) -> {
                                final StorageAdapter<? extends NodeReference> storageAdapter =
                                        primitives.storageAdapterForLayer(layer);
                                return searcher.greedySearchLayer(storageAdapter, transaction, storageTransform,
                                        previousNodeReference, layer, objectiveFunction);
                            }, getExecutor())
                            .thenCompose(nodeReference ->
                                    insertIntoLayers(transaction, storageTransform, quantizer, newPrimaryKey,
                                            transformedNewVector, nodeReference, lMax, insertionLayer))
                            .thenCompose(ignored ->
                                    addToStatsIfNecessary(random, transaction, currentAccessInfo, transformedNewVector));
                }).thenCompose(ignored -> AsyncUtil.DONE);
    }

    /**
     * Method to keep stats if necessary. Stats need to be kept and maintained when the client would like to use
     * e.g. RaBitQ as RaBitQ needs a stable somewhat correct centroid in order to function properly.
     * <p>
     * Specifically for RaBitQ, we add vectors to a set of sampled vectors in a designated subspace of the HNSW
     * structure. The parameter {@link Config#getSampleVectorStatsProbability()} governs when we do sample. Another
     * parameter, {@link Config#getMaintainStatsProbability()}, determines how many times we add-up/replace (consume)
     * vectors from this sampled-vector space and aggregate them in the typical running count/running sum scheme
     * in order to finally compute the centroid if {@link Config#getStatsThreshold()} number of vectors have been
     * sampled and aggregated. That centroid is then used to update the access info.
     *
     * @param random a random to use
     * @param transaction the transaction
     * @param currentAccessInfo this current access info that was fetched as part of an insert
     * @param transformedNewVector the new vector (in the transformed coordinate system) that may be added
     * @return a future that returns {@code null} when completed
     */
    @Nonnull
    private CompletableFuture<Void> addToStatsIfNecessary(@Nonnull final SplittableRandom random,
                                                          @Nonnull final Transaction transaction,
                                                          @Nonnull final AccessInfo currentAccessInfo,
                                                          @Nonnull final Transformed<RealVector> transformedNewVector) {
        if (getConfig().isUseRaBitQ() && !currentAccessInfo.canUseRaBitQ()) {
            if (shouldSampleVector(random)) {
                StorageAdapter.appendSampledVector(transaction, getSubspace(),
                        1, transformedNewVector, getOnWriteListener());
            }
            if (shouldMaintainStats(random)) {
                return StorageAdapter.consumeSampledVectors(transaction, getSubspace(),
                                50, getOnReadListener())
                        .thenApply(sampledVectors -> {
                            final AggregatedVector aggregatedSampledVector =
                                    aggregateVectors(sampledVectors);

                            if (aggregatedSampledVector != null) {
                                final int partialCount = aggregatedSampledVector.getPartialCount();
                                final Transformed<RealVector> partialVector = aggregatedSampledVector.getPartialVector();
                                StorageAdapter.appendSampledVector(transaction, getSubspace(),
                                        partialCount, partialVector, getOnWriteListener());
                                if (logger.isTraceEnabled()) {
                                    logger.trace("updated stats with numVectors={}, partialCount={}, partialVector={}",
                                            sampledVectors.size(), partialCount, partialVector);
                                }

                                if (partialCount >= getConfig().getStatsThreshold()) {
                                    final long rotatorSeed = random.nextLong();
                                    final FhtKacRotator rotator =
                                            new FhtKacRotator(rotatorSeed, getConfig().getNumDimensions(), 10);

                                    final Transformed<RealVector> centroid =
                                            partialVector.multiply(-1.0d / partialCount);
                                    final RealVector rotatedCentroid =
                                            rotator.apply(centroid.getUnderlyingVector());
                                    final StorageTransform storageTransform =
                                            new StorageTransform(rotator, rotatedCentroid);

                                    //
                                    // The entry node reference is expressed in a transformation that has so-far been
                                    // the identity-transformation. We now need to get the underlying identical vector
                                    // and, for the first time, transform that vector into the new rotated and
                                    // translated coordinate system. In this way we guarantee, that the entry node is
                                    // always expressed in the internal system, while data vectors may be a mix of
                                    // vectors.
                                    //
                                    final Transformed<RealVector> transformedEntryNodeVector =
                                            storageTransform.transform(currentAccessInfo.getEntryNodeReference()
                                                    .getVector().getUnderlyingVector());

                                    final AccessInfo newAccessInfo =
                                            new AccessInfo(currentAccessInfo.getEntryNodeReference().withVector(transformedEntryNodeVector),
                                                    rotatorSeed, rotatedCentroid);
                                    StorageAdapter.writeAccessInfo(transaction, getSubspace(), newAccessInfo, getOnWriteListener());
                                    StorageAdapter.deleteAllSampledVectors(transaction, getSubspace(), getOnWriteListener());
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("established rotatorSeed={}, centroid with count={}, centroid={}",
                                                rotatorSeed, partialCount, rotatedCentroid);
                                    }
                                }
                            }
                            return null;
                        });
            }
        }
        return AsyncUtil.DONE;
    }

    @Nullable
    private AggregatedVector aggregateVectors(@Nonnull final Iterable<AggregatedVector> vectors) {
        Transformed<RealVector> partialVector = null;
        int partialCount = 0;
        for (final AggregatedVector vector : vectors) {
            partialVector = partialVector == null
                            ? vector.getPartialVector() : partialVector.add(vector.getPartialVector());
            partialCount += vector.getPartialCount();
        }
        return partialCount == 0 ? null : new AggregatedVector(partialCount, partialVector);
    }

    /**
     * Inserts a new vector into the HNSW graph across multiple layers, starting from a given entry point.
     * <p>
     * This method implements the second phase of the HNSW insertion algorithm. It begins at a starting layer, which is
     * the minimum of the graph's maximum layer ({@code lMax}) and the new node's randomly assigned
     * {@code layer}. It then iterates downwards to layer 0. In each layer, it invokes
     * {@link #insertIntoLayer(StorageAdapter, Transaction, StorageTransform, Quantizer, List, int, Tuple, Transformed)}
     * to perform the search and connect the new node. The set of nearest neighbors found at layer {@code L} serves as
     * the entry points for the search at layer {@code L-1}.
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
                                                     @Nonnull final StorageTransform storageTransform,
                                                     @Nonnull final Quantizer quantizer,
                                                     @Nonnull final Tuple newPrimaryKey,
                                                     @Nonnull final Transformed<RealVector> newVector,
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
                    final StorageAdapter<? extends NodeReference> storageAdapter =
                            primitives().storageAdapterForLayer(layer);
                    return insertIntoLayer(storageAdapter, transaction, storageTransform, quantizer,
                            previousNodeReferences, layer, newPrimaryKey, newVector)
                            .thenApply(NodeReferenceAndNode::references);
                }, getExecutor()).thenCompose(ignored -> AsyncUtil.DONE);
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
     *         initial search phase. This list serves as the entry point for insertion into the next lower layer
     *         (i.e., {@code layer - 1}).
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
            insertIntoLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final Transaction transaction,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final Quantizer quantizer,
                            @Nonnull final List<NodeReferenceWithDistance> nearestNeighbors,
                            final int layer,
                            @Nonnull final Tuple newPrimaryKey,
                            @Nonnull final Transformed<RealVector> newVector) {
        if (logger.isTraceEnabled()) {
            logger.trace("begin insert key={} at layer={}", newPrimaryKey, layer);
        }
        final Primitives primitives = primitives();
        final Map<Tuple, AbstractNode<N>> nodeCache = Maps.newConcurrentMap();
        final Estimator estimator = quantizer.estimator();

        return searcher().beamSearchLayer(storageAdapter, transaction, storageTransform,
                nearestNeighbors, layer, getConfig().getEfConstruction(),
                Searcher.distanceToTargetVector(estimator, newVector), nodeCache)
                .thenCompose(searchResult ->
                        primitives.extendCandidatesIfNecessary(storageAdapter, transaction, storageTransform, estimator,
                                searchResult, layer, getConfig().isExtendCandidates(), nodeCache, newVector)
                                .thenCompose(extendedCandidates ->
                                        primitives.selectCandidates(storageAdapter, transaction, storageTransform, estimator,
                                                extendedCandidates, layer, getConfig().getM(), nodeCache))
                                .thenCompose(selectedNeighbors -> {
                                    final NodeFactory<N> nodeFactory = storageAdapter.getNodeFactory();

                                    final AbstractNode<N> newNode =
                                            nodeFactory.create(newPrimaryKey, newVector,
                                                    NodeReferenceAndNode.references(selectedNeighbors));

                                    final NeighborsChangeSet<N> newNodeChangeSet =
                                            new InsertNeighborsChangeSet<>(
                                                    new BaseNeighborsChangeSet<>(ImmutableList.of()),
                                                    newNode.getNeighbors());

                                    storageAdapter.writeNode(transaction, quantizer, layer, newNode,
                                            newNodeChangeSet);

                                    // create change sets for each selected neighbor and insert new node into them
                                    final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap =
                                            Maps.newLinkedHashMap();
                                    for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> selectedNeighbor : selectedNeighbors) {
                                        final NeighborsChangeSet<N> baseSet =
                                                new BaseNeighborsChangeSet<>(
                                                        selectedNeighbor.getNode().getNeighbors());
                                        final NeighborsChangeSet<N> insertSet =
                                                new InsertNeighborsChangeSet<>(baseSet,
                                                        ImmutableList.of(newNode.getSelfReference(newVector)));
                                        neighborChangeSetMap.put(selectedNeighbor.getNode().getPrimaryKey(),
                                                insertSet);
                                    }

                                    final int currentMMax =
                                            layer == 0 ? getConfig().getMMax0() : getConfig().getMMax();

                                    return forEach(selectedNeighbors,
                                            selectedNeighbor -> {
                                                final NodeReferenceWithDistance selectedNeighborReference =
                                                        selectedNeighbor.getNodeReference();
                                                final AbstractNode<N> selectedNeighborNode = selectedNeighbor.getNode();
                                                final NeighborsChangeSet<N> changeSet =
                                                        Objects.requireNonNull(neighborChangeSetMap.get(selectedNeighborNode.getPrimaryKey()));
                                                return primitives.pruneNeighborsIfNecessary(storageAdapter, transaction,
                                                        storageTransform, estimator, layer, selectedNeighborReference,
                                                        currentMMax, changeSet, nodeCache)
                                                        .thenApply(nodeReferencesAndNodes -> {
                                                            if (nodeReferencesAndNodes == null) {
                                                                return changeSet;
                                                            }
                                                            return primitives.resolveChangeSetFromNewNeighbors(changeSet, nodeReferencesAndNodes);
                                                        });
                                            }, getConfig().getMaxNumConcurrentNeighborhoodFetches(), getExecutor())
                                            .thenApply(changeSets -> {
                                                for (int i = 0; i < selectedNeighbors.size(); i++) {
                                                    final NodeReferenceAndNode<NodeReferenceWithDistance, N> selectedNeighbor =
                                                            selectedNeighbors.get(i);
                                                    final NeighborsChangeSet<N> changeSet = changeSets.get(i);
                                                    if (changeSet.hasChanges()) {
                                                        storageAdapter.writeNode(transaction, quantizer,
                                                                layer, selectedNeighbor.getNode(), changeSet);
                                                    }
                                                }
                                                return ImmutableList.copyOf(searchResult);
                                            });
                                }))
                .thenApply(nodeReferencesWithDistances -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("end insert key={} at layer={}", newPrimaryKey, layer);
                    }
                    return nodeReferencesWithDistances;
                });
    }

    private boolean shouldSampleVector(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getSampleVectorStatsProbability();
    }

    private boolean shouldMaintainStats(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getMaintainStatsProbability();
    }
}
