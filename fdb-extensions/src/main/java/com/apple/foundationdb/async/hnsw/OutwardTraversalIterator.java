/*
 * OutwardTraversalIterator.java
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jspecify.annotations.Nullable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Async iterator to iterate outwards starting from a given {@code (minimumRadius, minimumPrimaryKey)} (exclusive)
 * where {@code minimumRadius} is measured as the distance of a vector to the given {@code centerVector}.
 */
class OutwardTraversalIterator implements AsyncIterator<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> {
    private static final Logger logger = LoggerFactory.getLogger(OutwardTraversalIterator.class);

    private final Locator locator;
    private final StorageAdapter<NodeReference> storageAdapter;
    private final ReadTransaction readTransaction;
    private final CompletableFuture<Search.SearchResult> zoomInResultFuture;
    private final RealVector centerVector;
    private final double minimumRadius;
    @Nullable
    private final Tuple minimumPrimaryKey;
    private final int efOutwardSearch;

    private final boolean shouldQuickStart;

    /**
     * State of the iteration. All structures within the state are mutable (and in fact updates frequently)
     * as part of {@link  #computeNextRecord()}.
     */
    @Nullable
    private OutwardTraversalState traversalState;

    @Nullable
    private CompletableFuture<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> nextFuture;

    public OutwardTraversalIterator(final Locator locator,
                                    final StorageAdapter<NodeReference> storageAdapter,
                                    final ReadTransaction readTransaction,
                                    final CompletableFuture<Search.SearchResult> zoomInResultFuture,
                                    final RealVector centerVector,
                                    final double minimumRadius,
                                    @Nullable final Tuple minimumPrimaryKey,
                                    final int efOutwardSearch,
                                    final boolean shouldQuickStart) {
        this.locator = locator;
        this.storageAdapter = storageAdapter;
        this.readTransaction = readTransaction;
        this.zoomInResultFuture = zoomInResultFuture;
        this.centerVector = centerVector;
        this.minimumRadius = minimumRadius;
        this.minimumPrimaryKey = minimumPrimaryKey;
        this.efOutwardSearch = efOutwardSearch;
        this.shouldQuickStart = shouldQuickStart;

        this.traversalState = null;
        this.nextFuture = null;
    }

    public OutwardTraversalState getTraversalState() {
        return Objects.requireNonNull(traversalState);
    }

    private Config getConfig() {
        return locator.getConfig();
    }

    private Primitives primitives() {
        return locator.primitives();
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            if (traversalState == null) {
                nextFuture =
                        zoomInResultFuture
                                .thenAccept(zoomInResult ->
                                        this.traversalState = initialTravelState(zoomInResult))
                                .thenCompose(ignored -> computeNextRecord());
            } else {
                nextFuture = computeNextRecord();
            }
        }
        return nextFuture.thenApply(Objects::nonNull);
    }

    private OutwardTraversalState initialTravelState(final Search.SearchResult zoomInResult) {
        final StorageTransform storageTransform = zoomInResult.getStorageTransform();
        final Transformed<RealVector> transformedCenterVector = storageTransform.transform(centerVector);
        final PriorityQueue<NodeReferenceWithDistance> candidates =
                // This initial capacity is somewhat arbitrary as m is not necessarily
                // a limit, but it gives us a number that is better than the default.
                new PriorityQueue<>(getConfig().m(), NodeReferenceWithDistance.comparator());
        final SpatialRestrictions visited = new SpatialRestrictions(1, minimumRadius, minimumPrimaryKey);

        final PriorityQueue<NodeReferenceWithDistance> out =
                new PriorityQueue<>(efOutwardSearch + 1, // prevent reallocation further down
                        NodeReferenceWithDistance.comparator());
        final PriorityQueue<NodeReferenceWithDistance> quickStart =
                new PriorityQueue<>(efOutwardSearch + 1, // prevent reallocation further down
                        NodeReferenceWithDistance.comparator());

        final DistanceEstimator distanceEstimator = primitives().quantizer(zoomInResult.getAccessInfo()).estimator();

        // rekey the distances to distance around the center
        for (final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference> referenceAndNode : zoomInResult.getNearestReferenceAndNodes()) {
            final Transformed<RealVector> vector = referenceAndNode.getNodeReference().getVector();
            final double distance = distanceEstimator.distance(transformedCenterVector, vector);
            final Tuple primaryKey = referenceAndNode.getNode().getPrimaryKey();

            final NodeReferenceWithDistance nodeReferenceWithDistance =
                    new NodeReferenceWithDistance(primaryKey, vector, distance);
            visited.add(nodeReferenceWithDistance);
            candidates.add(nodeReferenceWithDistance);
            if (shouldQuickStart) {
                quickStart.add(nodeReferenceWithDistance);
            }
        }
        return new OutwardTraversalState(storageTransform, distanceEstimator,
                transformedCenterVector, candidates, visited, out, quickStart, zoomInResult.getNodeCache());
    }

    private CompletableFuture<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> computeNextRecord() {
        final Primitives primitives = primitives();
        final OutwardTraversalState localTraversalState = getTraversalState();
        final StorageTransform storageTransform = localTraversalState.getStorageTransform();
        final DistanceEstimator distanceEstimator = localTraversalState.getEstimator();
        final Transformed<RealVector> transformedCenterVector = localTraversalState.getTransformedCenterVector();
        final Queue<NodeReferenceWithDistance> candidates = localTraversalState.getCandidates();
        final SpatialRestrictions spatialRestrictions = localTraversalState.getSpatialRestrictions();
        final Queue<NodeReferenceWithDistance> out = localTraversalState.getOut();
        final Queue<NodeReferenceWithDistance> quickStart = localTraversalState.getQuickStart();
        final Set<Tuple> quickStartPrimaryKeys = localTraversalState.getQuickStartPrimaryKeys();
        final Map<Tuple, AbstractNode<NodeReference>> nodeCache = localTraversalState.getNodeCache();

        return AsyncUtil.whileTrue(() -> {
            while (!quickStart.isEmpty()) {
                final NodeReferenceWithDistance currentQuickStart = quickStart.peek();
                if (spatialRestrictions.isGreaterThanMinimum(currentQuickStart)) {
                    return AsyncUtil.READY_FALSE;
                }
                quickStart.poll();
            }

            if (candidates.isEmpty() || out.size() >= efOutwardSearch) {
                // break the refill loop
                return AsyncUtil.READY_FALSE;
            }

            final NodeReferenceWithDistance candidate = candidates.poll();
            if (spatialRestrictions.isGreaterThanMinimum(candidate) &&
                    !quickStartPrimaryKeys.contains(candidate.getPrimaryKey())) {
                out.add(candidate);
            }

            return primitives.fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, 0, candidate, nodeCache)
                    .thenApply(AbstractNode::getNeighbors)
                    .thenCompose(neighborReferences ->
                            primitives.fetchNeighborhoodReferences(storageAdapter, readTransaction,
                                    storageTransform, 0, neighborReferences, nodeCache))
                    .thenApply(neighborReferences -> {
                        for (final NodeReferenceWithVector current : neighborReferences) {
                            final Tuple primaryKey = current.getPrimaryKey();
                            final double distance =
                                    distanceEstimator.distance(transformedCenterVector, current.getVector());
                            final NodeReferenceWithDistance nodeReferenceWithDistance =
                                    new NodeReferenceWithDistance(primaryKey, current.getVector(), distance);

                            if (spatialRestrictions.shouldBeAdded(nodeReferenceWithDistance)) {
                                spatialRestrictions.add(nodeReferenceWithDistance);
                                candidates.add(nodeReferenceWithDistance);
                            }
                        }
                        return true;
                    });
        }).thenCompose(ignored -> {
            final NodeReferenceWithDistance nodeReference;
            if (!quickStart.isEmpty()) {
                nodeReference = quickStart.poll();
            } else {
                if (out.isEmpty()) {
                    Verify.verify(candidates.isEmpty());
                    return CompletableFuture.completedFuture(null);
                }
                nodeReference = out.poll();
            }
            return primitives.fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, 0, nodeReference, nodeCache)
                    .thenApply(node -> new NodeReferenceAndNode<>(nodeReference, node));
        }).thenApply(nextNodeReferenceAndNode -> {
            if (logger.isTraceEnabled()) {
                logger.trace("iterating for efOutwardSearch={} with result=={}", efOutwardSearch,
                        nextNodeReferenceAndNode == null ? null : nextNodeReferenceAndNode.getNodeReference().getPrimaryKey());
            }
            return nextNodeReferenceAndNode;
        });
    }

    @Override
    public boolean hasNext() {
        return onHasNext().join();
    }

    @Override
    public NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference> next() {
        if (hasNext()) {
            // underlying has already completed
            final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference> nextNodeReferenceAndNode =
                    Objects.requireNonNull(nextFuture).join();
            nextFuture = null;
            return nextNodeReferenceAndNode;
        }
        throw new NoSuchElementException("called next() on exhausted iterator");
    }

    @Override
    public void cancel() {
        if (nextFuture != null) {
            nextFuture.cancel(false);
        }
    }

    static class OutwardTraversalState {
        private final StorageTransform storageTransform;
        private final DistanceEstimator distanceEstimator;
        private final Transformed<RealVector> transformedCenterVector;
        private final Queue<NodeReferenceWithDistance> candidates;
        private final SpatialRestrictions spatialRestrictions;
        private final Queue<NodeReferenceWithDistance> out;
        private final Queue<NodeReferenceWithDistance> quickStart;
        private final Set<Tuple> quickStartPrimaryKeys;
        private final Map<Tuple, AbstractNode<NodeReference>> nodeCache;

        public OutwardTraversalState(final StorageTransform storageTransform,
                                     final DistanceEstimator distanceEstimator,
                                     final Transformed<RealVector> transformedCenterVector,
                                     final Queue<NodeReferenceWithDistance> candidates,
                                     final SpatialRestrictions spatialRestrictions,
                                     final Queue<NodeReferenceWithDistance> out,
                                     final Queue<NodeReferenceWithDistance> quickStart,
                                     final Map<Tuple, AbstractNode<NodeReference>> nodeCache) {
            this.storageTransform = storageTransform;
            this.distanceEstimator = distanceEstimator;
            this.transformedCenterVector = transformedCenterVector;
            this.candidates = candidates;
            this.spatialRestrictions = spatialRestrictions;
            this.out = out;
            this.quickStart = quickStart;
            final ImmutableSet.Builder<Tuple> quickStartPrimaryKeysBuilder = ImmutableSet.builder();
            for (final NodeReferenceWithDistance nodeReferenceWithDistance : quickStart) {
                quickStartPrimaryKeysBuilder.add(nodeReferenceWithDistance.getPrimaryKey());
            }
            this.quickStartPrimaryKeys = quickStartPrimaryKeysBuilder.build();
            this.nodeCache = nodeCache;
        }

        public StorageTransform getStorageTransform() {
            return storageTransform;
        }

        public DistanceEstimator getEstimator() {
            return distanceEstimator;
        }

        public Transformed<RealVector> getTransformedCenterVector() {
            return transformedCenterVector;
        }

        public Queue<NodeReferenceWithDistance> getCandidates() {
            return candidates;
        }

        public SpatialRestrictions getSpatialRestrictions() {
            return spatialRestrictions;
        }

        public Queue<NodeReferenceWithDistance> getOut() {
            return out;
        }

        public Queue<NodeReferenceWithDistance> getQuickStart() {
            return quickStart;
        }

        public Set<Tuple> getQuickStartPrimaryKeys() {
            return quickStartPrimaryKeys;
        }

        public Map<Tuple, AbstractNode<NodeReference>> getNodeCache() {
            return nodeCache;
        }
    }
}
