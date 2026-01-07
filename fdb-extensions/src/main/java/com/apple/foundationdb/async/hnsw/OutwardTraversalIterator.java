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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Async iterator to iterate outwards starting from a given {@code (minimumRadius, minimumPrimaryKey)} (exclusive)
 * where {@code minimumRadius} is measured as the distance of a vector to the given {@code centerVector}.
 */
class OutwardTraversalIterator implements AsyncIterator<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(OutwardTraversalIterator.class);

    @Nonnull
    private final Locator locator;
    @Nonnull
    private final StorageAdapter<NodeReference> storageAdapter;
    @Nonnull
    private final ReadTransaction readTransaction;
    @Nonnull
    private final CompletableFuture<Searcher.SearchResult> zoomInResultFuture;
    @Nonnull
    private final RealVector centerVector;
    private final double minimumRadius;
    @Nullable
    private final Tuple minimumPrimaryKey;
    private final int efOutwardSearch;

    /**
     * State of the iteration. All structures within the state are mutable (and in fact updates frequently)
     * as part of {@link  #computeNextRecord()}.
     */
    @Nullable
    private OutwardTraversalState traversalState;

    @Nullable
    private CompletableFuture<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> nextFuture;

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public OutwardTraversalIterator(@Nonnull final Locator locator,
                                    @Nonnull final StorageAdapter<NodeReference> storageAdapter,
                                    @Nonnull final ReadTransaction readTransaction,
                                    @Nonnull final CompletableFuture<Searcher.SearchResult> zoomInResultFuture,
                                    @Nonnull final RealVector centerVector,
                                    final double minimumRadius,
                                    @Nullable final Tuple minimumPrimaryKey,
                                    final int efOutwardSearch) {
        this.locator = locator;
        this.storageAdapter = storageAdapter;
        this.readTransaction = readTransaction;
        this.zoomInResultFuture = zoomInResultFuture;
        this.centerVector = centerVector;
        this.minimumRadius = minimumRadius;
        this.minimumPrimaryKey = minimumPrimaryKey;
        this.efOutwardSearch = efOutwardSearch;

        this.traversalState = null;
        this.nextFuture = null;
    }

    @Nonnull
    public OutwardTraversalState getTraversalState() {
        return Objects.requireNonNull(traversalState);
    }

    @Nonnull
    private Config getConfig() {
        return locator.getConfig();
    }

    @Nonnull
    private Primitives primitives() {
        return locator.primitives();
    }

    private Searcher searcher() {
        return locator.searcher();
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

    @Nonnull
    private OutwardTraversalState initialTravelState(@Nonnull final Searcher.SearchResult zoomInResult) {
        final StorageTransform storageTransform = zoomInResult.getStorageTransform();
        final Transformed<RealVector> transformedCenterVector = storageTransform.transform(centerVector);
        final PriorityQueue<NodeReferenceWithDistance> candidates =
                // This initial capacity is somewhat arbitrary as m is not necessarily
                // a limit, but it gives us a number that is better than the default.
                new PriorityQueue<>(getConfig().getM(), NodeReferenceWithDistance.comparator());
        final SpatialRestrictions visited = new SpatialRestrictions(1, minimumRadius, minimumPrimaryKey);

        final PriorityQueue<NodeReferenceWithDistance> out =
                new PriorityQueue<>(efOutwardSearch + 1, // prevent reallocation further down
                        NodeReferenceWithDistance.comparator());

        final Estimator estimator = primitives().quantizer(zoomInResult.getAccessInfo()).estimator();

        // rekey the distances to distance around the center
        for (final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference> referenceAndNode : zoomInResult.getNearestReferenceAndNodes()) {
            final Transformed<RealVector> vector = referenceAndNode.getNodeReference().getVector();
            final double distance = estimator.distance(transformedCenterVector, vector);
            final Tuple primaryKey = referenceAndNode.getNode().getPrimaryKey();

            final NodeReferenceWithDistance nodeReferenceWithDistance =
                    new NodeReferenceWithDistance(primaryKey, vector, distance);
            visited.add(nodeReferenceWithDistance);
            candidates.add(nodeReferenceWithDistance);
        }

        return new OutwardTraversalState(storageTransform, estimator,
                transformedCenterVector, candidates, visited, out, zoomInResult.getNodeCache());
    }

    @Nonnull
    private CompletableFuture<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> computeNextRecord() {
        final Primitives primitives = primitives();
        final OutwardTraversalState localTraversalState = getTraversalState();
        final StorageTransform storageTransform = localTraversalState.getStorageTransform();
        final Estimator estimator = localTraversalState.getEstimator();
        final Transformed<RealVector> transformedCenterVector = localTraversalState.getTransformedCenterVector();
        final Queue<NodeReferenceWithDistance> candidates = localTraversalState.getCandidates();
        final SpatialRestrictions spatialRestrictions = localTraversalState.getSpatialRestrictions();
        final Queue<NodeReferenceWithDistance> out = localTraversalState.getOut();
        final Map<Tuple, AbstractNode<NodeReference>> nodeCache = localTraversalState.getNodeCache();

        return AsyncUtil.whileTrue(() -> {
            if (candidates.isEmpty() || out.size() >= efOutwardSearch) {
                // break the refill loop
                return AsyncUtil.READY_FALSE;
            }

            final NodeReferenceWithDistance candidate = candidates.poll();
            if (spatialRestrictions.isGreaterThanMinimum(candidate)) {
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
                                    estimator.distance(transformedCenterVector, current.getVector());
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
            if (out.isEmpty()) {
                Verify.verify(candidates.isEmpty());
                return CompletableFuture.completedFuture(null);
            }
            final NodeReferenceWithDistance nodeReference = out.poll();
            return primitives.fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, 0, nodeReference, nodeCache)
                    .thenApply(node -> new NodeReferenceAndNode<>(nodeReference, node));
        }).thenApply(nextNodeReferenceAndNode -> {
            if (logger.isTraceEnabled()) {
                logger.trace("iterating for efOutwardSearch={} with result=={}", efOutwardSearch,
                        nextNodeReferenceAndNode.getNodeReference().getPrimaryKey());
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
        @Nonnull
        private final StorageTransform storageTransform;
        @Nonnull
        private final Estimator estimator;
        @Nonnull
        private final Transformed<RealVector> transformedCenterVector;
        @Nonnull
        private final Queue<NodeReferenceWithDistance> candidates;
        @Nonnull
        private final SpatialRestrictions spatialRestrictions;
        @Nonnull
        private final Queue<NodeReferenceWithDistance> out;
        @Nonnull
        private final Map<Tuple, AbstractNode<NodeReference>> nodeCache;

        public OutwardTraversalState(@Nonnull final StorageTransform storageTransform,
                                     @Nonnull final Estimator estimator,
                                     @Nonnull final Transformed<RealVector> transformedCenterVector,
                                     @Nonnull final Queue<NodeReferenceWithDistance> candidates,
                                     @Nonnull final SpatialRestrictions spatialRestrictions,
                                     @Nonnull final Queue<NodeReferenceWithDistance> out,
                                     @Nonnull final Map<Tuple, AbstractNode<NodeReference>> nodeCache) {
            this.storageTransform = storageTransform;
            this.estimator = estimator;
            this.transformedCenterVector = transformedCenterVector;
            this.candidates = candidates;
            this.spatialRestrictions = spatialRestrictions;
            this.out = out;
            this.nodeCache = nodeCache;
        }

        @Nonnull
        public StorageTransform getStorageTransform() {
            return storageTransform;
        }

        @Nonnull
        public Estimator getEstimator() {
            return estimator;
        }

        @Nonnull
        public Transformed<RealVector> getTransformedCenterVector() {
            return transformedCenterVector;
        }

        @Nonnull
        public Queue<NodeReferenceWithDistance> getCandidates() {
            return candidates;
        }

        @Nonnull
        public SpatialRestrictions getSpatialRestrictions() {
            return spatialRestrictions;
        }

        @Nonnull
        public Queue<NodeReferenceWithDistance> getOut() {
            return out;
        }

        @Nonnull
        public Map<Tuple, AbstractNode<NodeReference>> getNodeCache() {
            return nodeCache;
        }
    }
}
