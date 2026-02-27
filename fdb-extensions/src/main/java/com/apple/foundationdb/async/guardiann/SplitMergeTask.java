/*
 * SplitMergeTask.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.guardiann.Primitives.NeighborhoodsResult;
import com.apple.foundationdb.kmeans.BoundedKMeans;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SplitMergeTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(SplitMergeTask.class);

    @Nonnull
    private final UUID clusterId;
    @Nonnull
    private final Transformed<RealVector> centroid;

    private SplitMergeTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                           @Nonnull final Transformed<RealVector> centroid) {
        super(locator, accessInfo, taskId);
        this.clusterId = clusterId;
        this.centroid = centroid;
    }

    @Nonnull
    public UUID getClusterId() {
        return clusterId;
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        final Quantizer quantizer = getLocator().primitives().quantizer(getAccessInfo());
        final Transformed<RealVector> encodedVector = quantizer.encode(getCentroid());
        return Tuple.from(getKind().getCode(), getClusterId(), encodedVector.getUnderlyingVector().getRawData());
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.SPLIT_MERGE;
    }

    @Nonnull
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        final Config config = getLocator().getConfig();
        final Primitives primitives = getLocator().primitives();

        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final RealVector untransformedCentroid = storageTransform.untransform(getCentroid());

        return primitives.fetchClusterMetadata(transaction, getClusterId())
                .thenCompose(clusterMetadata -> {
                    if (clusterMetadata == null || !clusterMetadata.getStates().contains(ClusterMetadata.State.SPLIT_MERGE)) {
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumVectors() >= config.getClusterMin() ||
                            clusterMetadata.getNumVectors() <= config.getClusterMax()) {
                        // false alarm
                        final EnumSet<ClusterMetadata.State> newStates = EnumSet.copyOf(clusterMetadata.getStates());
                        newStates.remove(ClusterMetadata.State.SPLIT_MERGE);
                        primitives.writeClusterMetadata(transaction, new ClusterMetadata(clusterMetadata.getId(),
                                clusterMetadata.getNumVectors(), newStates));
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumVectors() > config.getClusterMax()) {
                        return split(transaction, clusterMetadata, untransformedCentroid);
                    } else {
                        Verify.verify(clusterMetadata.getNumVectors() < config.getClusterMin());
                        return merge(transaction, clusterMetadata, untransformedCentroid);
                    }
                });
    }

    @Nonnull
    private CompletableFuture<Void> merge(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata targetClusterMetadata,
                                          @Nonnull final RealVector targetClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(targetClusterMetadata.getId());
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = 3;
        final int numOuterNeighborhood = 3;

        final CompletableFuture<NeighborhoodsResult> neighborhoodsFuture =
                primitives.neighborhoods(transaction, storageTransform, targetClusterMetadata, targetClusterCentroid,
                        numInnerNeighborhood, numOuterNeighborhood);

        return neighborhoodsFuture.thenCompose(neighborhoods -> {
            final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.getInnerNeighborhood();
            final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.getOuterNeighborhood();

            //
            // At this point innerNeighborhood contains the clusters we want to split into
            // innerNeighborhood.size() - 1 number of clusters and outerNeighborhood contains all clusters we
            // may assign some vectors from innerNeighborhood to.
            //
            return primitives.fetchInnerClusters(transaction, innerNeighborhood, storageTransform)
                    .thenCompose(innerClusters -> primitives.cleanUpVectorReferences(transaction, innerClusters))
                    .thenApply(cleanedUpVectorReferences ->
                            assignVectorReferences(random, estimator, outerNeighborhood, cleanedUpVectorReferences,
                                    innerNeighborhood.size() - 1))
                    .thenAccept(assignmentResult ->
                            updateAssignments(transaction, innerNeighborhood, assignmentResult, quantizer));
        });
    }

    @Nonnull
    private CompletableFuture<Void> split(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata targetClusterMetadata,
                                          @Nonnull final RealVector targetClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(targetClusterMetadata.getId());
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = 2;
        final int numOuterNeighborhood = 3;

        final CompletableFuture<NeighborhoodsResult> neighborhoodsFuture =
                primitives.neighborhoods(transaction, storageTransform, targetClusterMetadata, targetClusterCentroid,
                        numInnerNeighborhood, numOuterNeighborhood);

        return neighborhoodsFuture.thenCompose(neighborhoods -> {
            final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.getInnerNeighborhood();
            final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.getOuterNeighborhood();

            //
            // At this point innerNeighborhood contains the clusters we want to split into
            // innerNeighborhood.size() + 1 number of clusters and outerNeighborhood contains all clusters we
            // may assign some vectors from innerNeighborhood to.
            //
            return primitives.fetchInnerClusters(transaction, innerNeighborhood, storageTransform)
                    .thenCompose(innerClusters -> primitives.cleanUpVectorReferences(transaction, innerClusters))
                    .thenApply(cleanedUpVectorReferences ->
                            assignVectorReferences(random, estimator, outerNeighborhood, cleanedUpVectorReferences,
                                    innerNeighborhood.size() + 1))
                    .thenAccept(assignmentResult ->
                            updateAssignments(transaction, innerNeighborhood, assignmentResult, quantizer));
        });
    }

    @Nonnull
    private AssignmentResult assignVectorReferences(@Nonnull final SplittableRandom random,
                                                    @Nonnull final Estimator estimator,
                                                    @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood,
                                                    @Nonnull final List<VectorReference> vectorReferences,
                                                    final int targetNumPartitions) {
        final Config config = getConfig();
        final ImmutableList.Builder<VectorReference> primaryVectorReferencesBuilder = ImmutableList.builder();
        for (final VectorReference vectorReference : vectorReferences) {
            if (vectorReference.isPrimaryCopy()) {
                primaryVectorReferencesBuilder.add(vectorReference);
            }
        }
        final ImmutableList<VectorReference> primaryVectorReferences = primaryVectorReferencesBuilder.build();

        final BoundedKMeans.Result<Transformed<RealVector>> kMeansResult =
                BoundedKMeans.fit(random, estimator, VectorReference.vectorLens(),
                        Transformed.underlyingLens(), primaryVectorReferences, targetNumPartitions, 3,
                        1, 0.05, BoundedKMeans.overflowQuadraticPenalty(),
                        true);
        final List<Transformed<RealVector>> clusterCentroids =
                kMeansResult.getClusterCentroids();
        Verify.verify(clusterCentroids.size() == targetNumPartitions);

        final ImmutableMap.Builder<UUID, ClusterMetadataWithDistance> clusterIdMetadataMapBuilder =
                ImmutableMap.builder();
        final ImmutableSet.Builder<UUID> newClusterIdsBuilder =
                ImmutableSet.builder();
        for (int i = 0; i < targetNumPartitions; i++) {
            final UUID newClusterId = UUID.randomUUID();
            newClusterIdsBuilder.add(newClusterId);

            clusterIdMetadataMapBuilder.put(newClusterId,
                    new ClusterMetadataWithDistance(
                            new ClusterMetadata(newClusterId,
                                    0,
                                    EnumSet.noneOf(ClusterMetadata.State.class)),
                            clusterCentroids.get(i), 0.0d));
        }
        final Set<UUID> newClusterIds = newClusterIdsBuilder.build();

        for (final ClusterMetadataWithDistance clusterMetadata : outerNeighborhood) {
            clusterIdMetadataMapBuilder.put(clusterMetadata.getClusterMetadata().getId(), clusterMetadata);
        }
        final ImmutableMap<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                clusterIdMetadataMapBuilder.build();

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignedVectorsMultimapBuilder =
                ImmutableListMultimap.builder();
        for (final VectorReference vectorReference : vectorReferences) {
            final PriorityQueue<ClusterMetadataWithDistance> nearestClusters =
                    new PriorityQueue<>(targetNumPartitions + outerNeighborhood.size(),
                            Comparator.comparing(ClusterMetadataWithDistance::getDistance));
            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : clusterIdMetadataMap.values()) {
                final double distance =
                        estimator.distance(vectorReference.getVector(),
                                clusterMetadataWithDistance.getCentroid());

                nearestClusters.add(clusterMetadataWithDistance.withNewDistance(distance));
            }

            final ClusterMetadataWithDistance primaryCluster =
                    Objects.requireNonNull(nearestClusters.poll());
            final double distanceToPrimaryCentroid = primaryCluster.getDistance();
            Verify.verify(Double.isFinite(distanceToPrimaryCentroid));
            assignedVectorsMultimapBuilder.put(
                    primaryCluster.getClusterMetadata().getId(),
                    vectorReference.toPrimaryCopy());

            while (!nearestClusters.isEmpty()) {
                final ClusterMetadataWithDistance replicatedCluster =
                        Objects.requireNonNull(nearestClusters.poll());
                final double distance = primaryCluster.getDistance();
                Verify.verify(Double.isFinite(distance));

                //
                // Distance should be greater than the distance to the primary cluster's
                // centroid. So the fraction on the left should always be greater or equal
                // to 1.0d. The config provides some fuzziness to replicate the new vector
                // into other clusters if it happens to be at the border between two (or more)
                // clusters.
                //
                if (distance / distanceToPrimaryCentroid <= 1.0d + config.getClusterOverlap()) {
                    assignedVectorsMultimapBuilder.put(
                            replicatedCluster.getClusterMetadata().getId(),
                            vectorReference.toReplicatedCopy());
                } else {
                    break;
                }
            }
        }
        return new AssignmentResult(newClusterIds, clusterIdMetadataMap, assignedVectorsMultimapBuilder.build());
    }

    private void updateAssignments(@Nonnull final Transaction transaction,
                                   @Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood,
                                   @Nonnull final AssignmentResult assignmentResult,
                                   @Nonnull final Quantizer quantizer) {
        final Config config = getConfig();
        final Primitives primitives = getLocator().primitives();

        // delete old clusters
        for (final ClusterMetadataWithDistance clusterMetadata : innerNeighborhood) {
            final UUID toBeDeleted = clusterMetadata.getClusterMetadata().getId();
            primitives.deleteVectorReferencesForCluster(transaction, toBeDeleted);
            primitives.deleteClusterMetadata(transaction, toBeDeleted);
        }

        // write all vector references
        final var assignmentMultiMap =
                assignmentResult.getAssignmentMultimap();
        for (final Map.Entry<UUID, VectorReference> entry : assignmentMultiMap.entries()) {
            primitives.writeVectorReference(transaction, quantizer, entry.getKey(),
                    entry.getValue());
        }

        // update all affected cluster metadata
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                assignmentResult.getClusterIdMetadataMap();
        final Set<UUID> newClusterIds = assignmentResult.getNewClusterIds();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance =
                    entry.getValue();
            final ClusterMetadata clusterMetadata =
                    clusterMetadataWithDistance.getClusterMetadata();
            final boolean isNewCluster = newClusterIds.contains(clusterMetadata.getId());
            if (assignmentMultiMap.containsKey(toBeWritten)) {
                final int numVectorsAdded = assignmentMultiMap.get(toBeWritten).size();
                ClusterMetadata newClusterMetadata;
                final int numTotalVectors = clusterMetadata.getNumVectors() + numVectorsAdded;
                if (!clusterMetadata.getStates().contains(ClusterMetadata.State.SPLIT_MERGE) &&
                        numTotalVectors > config.getClusterMax()) {
                    if (isNewCluster) {
                        logger.warn("number of vectors after split is greater than cluster max; clusterId={}; numTotalVectors={}",
                                toBeWritten, numTotalVectors);
                    }

                    // create a split/merge task
                    primitives.writeDeferredTask(transaction,
                            SplitMergeTask.of(getLocator(), getAccessInfo(), UUID.randomUUID(),
                                    clusterId, clusterMetadataWithDistance.getCentroid()));

                    newClusterMetadata =
                            clusterMetadata.withAdditionalVectorsAndNewStates(numVectorsAdded,
                                    ClusterMetadata.State.SPLIT_MERGE);
                } else {
                    //
                    // Note that we treat these cases in a slightly asymmetric way. If there are too many vectors in
                    // a new cluster, we still split, but if there are too few we do not merge.
                    //
                    if (isNewCluster && numTotalVectors < config.getClusterMin()) {
                        logger.warn("number of vectors after split is less than cluster min; clusterId={}; numTotalVectors={}",
                                toBeWritten, numTotalVectors);
                    }

                    newClusterMetadata =
                            clusterMetadata.withAdditionalVectors(numVectorsAdded);
                }

                if (!isNewCluster) {
                    newClusterMetadata = newClusterMetadata.withNewStates(ClusterMetadata.State.REASSIGN);

                    // create a split/merge task
                    primitives.writeDeferredTask(transaction,
                            ReassignTask.of(getLocator(), getAccessInfo(), UUID.randomUUID(),
                                    clusterId, clusterMetadataWithDistance.getCentroid()));
                }

                primitives.writeClusterMetadata(transaction, newClusterMetadata);
            }
        }
    }

    @Nonnull
    static SplitMergeTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                     @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.SPLIT_MERGE);
        final StorageTransform storageTransform = locator.primitives().storageTransform(accessInfo);
        final Transformed<RealVector> centroid = storageTransform.transform(
                StorageHelpers.vectorFromBytes(locator.getConfig(), valueTuple.getBytes(2)));

        return new SplitMergeTask(locator, accessInfo, keyTuple.getUUID(1),
                valueTuple.getUUID(1), centroid);
    }

    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid) {
        return new SplitMergeTask(locator, accessInfo, taskId, clusterId, centroid);
    }

    private static class AssignmentResult {
        @Nonnull
        private final Set<UUID> newClusterIds;
        @Nonnull
        private final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap;
        @Nonnull
        private final ImmutableListMultimap<UUID, VectorReference> assignmentMultimap;

        public AssignmentResult(@Nonnull final Set<UUID> newClusterIds,
                                @Nonnull final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                @Nonnull final ImmutableListMultimap<UUID, VectorReference> assignmentMultimap) {
            this.newClusterIds = newClusterIds;
            this.clusterIdMetadataMap = clusterIdMetadataMap;
            this.assignmentMultimap = assignmentMultimap;
        }

        @Nonnull
        public Set<UUID> getNewClusterIds() {
            return newClusterIds;
        }

        @Nonnull
        public Map<UUID, ClusterMetadataWithDistance> getClusterIdMetadataMap() {
            return clusterIdMetadataMap;
        }

        @Nonnull
        public ImmutableListMultimap<UUID, VectorReference> getAssignmentMultimap() {
            return assignmentMultimap;
        }
    }
}
