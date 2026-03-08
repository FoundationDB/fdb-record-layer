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
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.guardiann.Primitives.NeighborhoodsResult;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.kmeans.BoundedKMeans;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.ReservoirSampler;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
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
    private final Transformed<RealVector> centroid;

    private SplitMergeTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                           @Nonnull final Transformed<RealVector> centroid) {
        super(locator, accessInfo, taskId, ImmutableSet.of(targetClusterId));
        this.centroid = centroid;
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.SPLIT_MERGE;
    }

    @Nonnull
    private UUID getTargetClusterId() {
        return Iterables.getOnlyElement(getTargetClusterIds());
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        final Quantizer quantizer = primitives().quantizer(getAccessInfo());
        final Transformed<RealVector> encodedVector = quantizer.encode(getCentroid());
        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                encodedVector.getUnderlyingVector().getRawData());
    }

    @Nonnull
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        logStart(logger);

        final Config config = getConfig();
        final Primitives primitives = primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final RealVector untransformedCentroid = storageTransform.untransform(getCentroid());

        return primitives.fetchClusterMetadata(transaction, getTargetClusterId())
                .thenCompose(clusterMetadata -> {
                    if (clusterMetadata == null || !clusterMetadata.getStates().contains(ClusterMetadata.State.SPLIT_MERGE)) {
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumPrimaryVectors() >= config.getPrimaryClusterMin() &&
                            clusterMetadata.getNumPrimaryVectors() <= config.getPrimaryClusterMax()) {
                        // false alarm
                        final EnumSet<ClusterMetadata.State> newStates = EnumSet.copyOf(clusterMetadata.getStates());
                        newStates.remove(ClusterMetadata.State.SPLIT_MERGE);
                        primitives.writeClusterMetadata(transaction, new ClusterMetadata(clusterMetadata.getId(),
                                clusterMetadata.getNumPrimaryVectors(), clusterMetadata.getNumPrimaryUnderreplicatedVectors(),
                                clusterMetadata.getNumReplicatedVectors(), newStates));
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumPrimaryVectors() > config.getPrimaryClusterMax()) {
                        return split(transaction, clusterMetadata, untransformedCentroid);
                    } else {
                        Verify.verify(clusterMetadata.getNumPrimaryVectors() < config.getPrimaryClusterMin());
                        return merge(transaction, clusterMetadata, untransformedCentroid);
                    }
                }).thenAccept(ignored -> logSuccessful(logger));
    }

    @Nonnull
    private CompletableFuture<Void> split(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata targetClusterMetadata,
                                          @Nonnull final RealVector targetClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(targetClusterMetadata.getId());
        final Primitives primitives = primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = 2;
        final int numOuterNeighborhood = 12;

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
                    .thenCompose(assignmentResult ->
                            updateHnsw(transaction, storageTransform, innerNeighborhood, assignmentResult))
                    .thenAccept(assignmentResult ->
                            updateAssignments(transaction, random, innerNeighborhood, assignmentResult, quantizer));
        });
    }

    @Nonnull
    private CompletableFuture<Void> merge(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata targetClusterMetadata,
                                          @Nonnull final RealVector targetClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(targetClusterMetadata.getId());
        final Primitives primitives = primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = 3;
        final int numOuterNeighborhood = 8;

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
                    .thenCompose(assignmentResult ->
                            updateHnsw(transaction, storageTransform, innerNeighborhood, assignmentResult))
                    .thenAccept(assignmentResult ->
                            updateAssignments(transaction, random, innerNeighborhood, assignmentResult, quantizer));
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

        // re-fit only the primary vectors
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
        final ImmutableSet.Builder<UUID> newClusterIdsBuilder = ImmutableSet.builder();
        for (int i = 0; i < targetNumPartitions; i++) {
            final UUID newClusterId = RandomHelpers.nextUuid(random, config.isPersistSequentialUuids());
            newClusterIdsBuilder.add(newClusterId);

            clusterIdMetadataMapBuilder.put(newClusterId,
                    new ClusterMetadataWithDistance(
                            new ClusterMetadata(newClusterId,
                                    0, 0, 0,
                                    EnumSet.noneOf(ClusterMetadata.State.class)),
                            clusterCentroids.get(i), 0.0d));
        }
        final Set<UUID> newClusterIds = newClusterIdsBuilder.build();

        for (final ClusterMetadataWithDistance clusterMetadata : outerNeighborhood) {
            clusterIdMetadataMapBuilder.put(clusterMetadata.getClusterMetadata().getId(), clusterMetadata);
        }
        final ImmutableMap<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                clusterIdMetadataMapBuilder.build();

        //
        // At this point clusterIdMetadataMap contains the new clusters after the split and all the clusters
        // from the outer neighborhood.
        //

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignmentMultimapBuilder =
                ImmutableListMultimap.builder();
        final Map<UUID, ReservoirSampler<VectorReference>> replicatedAssignmentSamplerMap = Maps.newHashMap();

        // only considering primary copies here -- this will prune the replicated vectors
        for (final VectorReference vectorReference : primaryVectorReferences) {
            final PriorityQueue<ClusterMetadataWithDistance> nearestClusters =
                    new PriorityQueue<>(targetNumPartitions + outerNeighborhood.size(),
                            Comparator.comparing(ClusterMetadataWithDistance::getDistance));
            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : clusterIdMetadataMap.values()) {
                final double distance =
                        estimator.distance(vectorReference.getVector(),
                                clusterMetadataWithDistance.getCentroid());

                nearestClusters.add(clusterMetadataWithDistance.withNewDistance(distance));
            }

            final ClusterMetadataWithDistance primaryCluster = Objects.requireNonNull(nearestClusters.poll());
            final double distanceToPrimaryCentroid = primaryCluster.getDistance();
            Verify.verify(Double.isFinite(distanceToPrimaryCentroid));

            final UUID primaryClusterId = primaryCluster.getClusterMetadata().getId();
            if (!newClusterIds.contains(primaryClusterId)) {
                assignmentMultimapBuilder.put(
                        primaryClusterId,
                        vectorReference.toPrimaryUnderreplicatedCopy());
                // skip writing replicas as the new clusters do not "own" this vector anymore
                continue;
            }

            // add primary to one of the new clusters and in the remainder populate the replicated clusters

            assignmentMultimapBuilder.put(primaryClusterId, vectorReference);

            while (!nearestClusters.isEmpty()) {
                final ClusterMetadataWithDistance replicatedCluster =
                        Objects.requireNonNull(nearestClusters.poll());
                final double distance = replicatedCluster.getDistance();
                Verify.verify(Double.isFinite(distance));

                //
                // Distance should be greater than the distance to the primary cluster's
                // centroid. So the fraction on the left should always be greater or equal
                // to 1.0d. The config provides some fuzziness to replicate the new vector
                // into other clusters if it happens to be at the border between two (or more)
                // clusters.
                //
                if (distance / distanceToPrimaryCentroid <= 1.0d + config.getClusterOverlap()) {
                    final VectorReference newVectorReference = vectorReference.toReplicatedCopy();
                    if (newClusterIds.contains(replicatedCluster.getClusterMetadata().getId())) {
                        final var reservoirSampler =
                                replicatedAssignmentSamplerMap.computeIfAbsent(replicatedCluster.getClusterMetadata().getId(),
                                        ignored -> new ReservoirSampler<>(config.getReplicatedClusterTarget(), random));
                        reservoirSampler.add(newVectorReference);
                    } else {
                        assignmentMultimapBuilder.put(replicatedCluster.getClusterMetadata().getId(),
                                newVectorReference);
                    }
                } else {
                    break;
                }
            }
        }

        for (final Map.Entry<UUID, ReservoirSampler<VectorReference>> entry : replicatedAssignmentSamplerMap.entrySet()) {
            assignmentMultimapBuilder.putAll(entry.getKey(), entry.getValue().sample());
        }

        return new AssignmentResult(newClusterIds, clusterIdMetadataMap, assignmentMultimapBuilder.build());
    }

    @Nonnull
    private CompletableFuture<AssignmentResult> updateHnsw(@Nonnull final Transaction transaction,
                                                           @Nonnull final StorageTransform storageTransform,
                                                           @Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood,
                                                           @Nonnull final AssignmentResult assignmentResult) {
        final Primitives primitives = primitives();
        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap = assignmentResult.getClusterIdMetadataMap();
        final Set<UUID> newClusterIds = assignmentResult.getNewClusterIds();

        // delete first
        final CompletableFuture<List<Void>> deletedCentroidsFuture =
                MoreAsyncUtil.forEach(innerNeighborhood,
                        clusterMetadataWithDistance -> {
                            final UUID toBeDeletedClusterId = clusterMetadataWithDistance.getClusterMetadata().getId();
                            return centroidsHnsw.delete(transaction,
                                            StorageAdapter.tupleFromClusterId(toBeDeletedClusterId))
                                    .thenAccept(ignored -> logger.info("cluster deleted, clusterId={}", toBeDeletedClusterId));
                        },
                        1,
                        getLocator().getExecutor());

        // insert
        return deletedCentroidsFuture.thenCompose(ignored ->
                        MoreAsyncUtil.forEach(newClusterIds,
                                newClusterId -> {
                                    final ClusterMetadataWithDistance clusterMetadataWithDistance =
                                            Objects.requireNonNull(clusterIdMetadataMap.get(newClusterId));
                                    final RealVector centroidBVector =
                                            storageTransform.untransform(clusterMetadataWithDistance.getCentroid());
                                    return centroidsHnsw.insert(transaction, StorageAdapter.tupleFromClusterId(newClusterId),
                                            centroidBVector, null)
                                            .thenAccept(ignored2 ->
                                                    logger.info("new cluster inserted; clusterId={}", newClusterId));
                                },
                                1,
                                getLocator().getExecutor()))
                .thenApply(ignored -> assignmentResult);
    }

    private void updateAssignments(@Nonnull final Transaction transaction,
                                   @Nonnull final SplittableRandom random,
                                   @Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood,
                                   @Nonnull final AssignmentResult assignmentResult,
                                   @Nonnull final Quantizer quantizer) {
        final Primitives primitives = primitives();

        // delete old clusters
        final Set<UUID> newClusterIds = assignmentResult.getNewClusterIds();
        for (final ClusterMetadataWithDistance clusterMetadata : innerNeighborhood) {
            final UUID toBeDeleted = clusterMetadata.getClusterMetadata().getId();
            primitives.deleteVectorReferencesForCluster(transaction, toBeDeleted);
            primitives.deleteClusterMetadata(transaction, toBeDeleted);
        }

        // write all vector references
        final ListMultimap<UUID, VectorReference> assignmentMultiMap = assignmentResult.getAssignmentMultimap();
        final Map<UUID, Integer> clusterIdToNumPrimaryVectorsAdded = Maps.newHashMap();
        final Map<UUID, Integer> clusterIdToNumPrimaryUnderreplicatedVectorsAdded = Maps.newHashMap();
        final Map<UUID, Integer> clusterIdToNumReplicatedVectorsAdded = Maps.newHashMap();
        for (final Map.Entry<UUID, VectorReference> entry : assignmentMultiMap.entries()) {
            final UUID clusterId = entry.getKey();
            final VectorReference vectorReference = entry.getValue();
            primitives.writeVectorReference(transaction, quantizer, clusterId, vectorReference);
            if (vectorReference.isPrimaryCopy()) {
                incrementCounter(clusterIdToNumPrimaryVectorsAdded, clusterId);
                if (vectorReference.isUnderreplicated()) {
                    incrementCounter(clusterIdToNumPrimaryUnderreplicatedVectorsAdded, clusterId);
                }
            } else {
                incrementCounter(clusterIdToNumReplicatedVectorsAdded, clusterId);
            }
        }

        // update all affected cluster metadata
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                assignmentResult.getClusterIdMetadataMap();
        final ImmutableSet.Builder<UUID> newDependentTaskIdsBuilder = ImmutableSet.builder();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.getClusterMetadata();
            final int numPrimaryVectorsAdded = clusterIdToNumPrimaryVectorsAdded.getOrDefault(toBeWritten, 0);
            final int numPrimaryUnderreplicatedVectorsAdded = clusterIdToNumPrimaryUnderreplicatedVectorsAdded.getOrDefault(toBeWritten, 0);
            final int numReplicatedVectorsAdded = clusterIdToNumReplicatedVectorsAdded.getOrDefault(toBeWritten, 0);

            primitives.writeDeferredTaskMaybe(transaction, random, clusterMetadata,
                            clusterMetadataWithDistance.getCentroid(), getAccessInfo(),
                            numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
                            newClusterIds)
                    .ifPresent(newDependentTaskIdsBuilder::add);
            if (logger.isInfoEnabled()) {
                logger.info("pushing vectors during split; isNewCluster={}; clusterId={}; numTotalPrimaryVectors={}, numPrimaryVectorsAdded={}, " +
                                "numTotalPrimaryUnderreplicatedReplicatedVectors={}, numPrimaryUnderreplicatedVectorsAdded={}, " +
                                "numTotalReplicatedVectors={}, numReplicatedVectorsAdded={}",
                        newClusterIds.contains(clusterMetadata.getId()),
                        clusterMetadata.getId(),
                        clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded, numPrimaryVectorsAdded,
                        clusterMetadata.getNumPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded, numPrimaryUnderreplicatedVectorsAdded,
                        clusterMetadata.getNumReplicatedVectors() + numReplicatedVectorsAdded, numReplicatedVectorsAdded);
            }
        }

        final Set<UUID> newDependentTaskIds = newDependentTaskIdsBuilder.build();
        if (!newDependentTaskIds.isEmpty()) {
            final BounceReassignTask newBounceReassignTask =
                    BounceReassignTask.of(getLocator(), getAccessInfo(),
                            RandomHelpers.randomUUID(random), newClusterIds,
                            newDependentTaskIds);
            primitives.writeDeferredTask(transaction, newBounceReassignTask);

            if (logger.isInfoEnabled()) {
                logger.info("enqueuing BOUNCE_REASSIGN; taskId={}; targetClusterIds={}; newDependentTaskIds={}",
                        newBounceReassignTask.getTaskId(),
                        newBounceReassignTask.getTargetClusterIds(),
                        newBounceReassignTask.getDependentTaskIds());
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

        return new SplitMergeTask(locator, accessInfo,
                keyTuple.getUUID(0), valueTuple.getUUID(1), centroid);
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
        private final ListMultimap<UUID, VectorReference> assignmentMultimap;

        public AssignmentResult(@Nonnull final Set<UUID> newClusterIds,
                                @Nonnull final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                @Nonnull final ListMultimap<UUID, VectorReference> assignmentMultimap) {
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
        public ListMultimap<UUID, VectorReference> getAssignmentMultimap() {
            return assignmentMultimap;
        }
    }
}
