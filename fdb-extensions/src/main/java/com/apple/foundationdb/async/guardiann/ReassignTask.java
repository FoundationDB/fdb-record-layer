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
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ReassignTask extends AbstractDeferredTask {
    @Nonnull
    private final UUID clusterId;
    @Nonnull
    private final Transformed<RealVector> centroid;

    private ReassignTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
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
        return Kind.REASSIGN;
    }
    
    @Nonnull
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        final Primitives primitives = getLocator().primitives();

        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final RealVector untransformedCentroid = storageTransform.untransform(getCentroid());

        return primitives.fetchClusterMetadata(transaction, getClusterId())
                .thenCompose(clusterMetadata -> {
                    if (clusterMetadata == null) {
                        return AsyncUtil.DONE;
                    }

                    final EnumSet<ClusterMetadata.State> states = clusterMetadata.getStates();
                    if (!states.contains(ClusterMetadata.State.REASSIGN) ||
                            states.contains(ClusterMetadata.State.SPLIT_MERGE)) {
                        return AsyncUtil.DONE;
                    }

                    return reassign(transaction, clusterMetadata, untransformedCentroid);
                });
    }

    @Nonnull
    private CompletableFuture<Void> reassign(@Nonnull final Transaction transaction,
                                             @Nonnull final ClusterMetadata targetClusterMetadata,
                                             @Nonnull final RealVector targetClusterCentroid) {
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = 1;
        final int numOuterNeighborhood = 3;

        final CompletableFuture<Primitives.NeighborhoodsResult> neighborhoodsFuture =
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
                    .thenCompose(innerClusters -> primitives.cleanUpVectorReferences(transaction, innerClusters)
                            .thenAccept(cleanedUpVectorReferences -> {
                                final ReassignmentResult reassignmentResult =
                                        reassignVectorReferences(estimator, Iterables.getOnlyElement(innerNeighborhood),
                                                outerNeighborhood, cleanedUpVectorReferences);
                                final ImmutableListMultimap<UUID, VectorReference> assignmentMultimap = reassignmentResult.getAssignmentMultimap();
                                final ImmutableList<VectorReference> targetClusterAssignedVectors =
                                        assignmentMultimap.get(targetClusterMetadata.getId());
                                final ImmutableMap.Builder<Tuple, VectorReference> targetClusterAssignedVectorsAsMapBuilder = ImmutableMap.builder();
                                for (final VectorReference targetClusterAssignedVector : targetClusterAssignedVectors) {
                                    targetClusterAssignedVectorsAsMapBuilder.put(targetClusterAssignedVector.getId().getPrimaryKey(),
                                            targetClusterAssignedVector);
                                }
                                final ImmutableMap<Tuple, VectorReference> targetClusterAssignedVectorsAsMap =
                                        targetClusterAssignedVectorsAsMapBuilder.build();
                                final Cluster targetCluster = Iterables.getOnlyElement(innerClusters);

                                final ImmutableList.Builder<Tuple> deleteTargetClusterAssignedVectorsBuilder =
                                        ImmutableList.builder();
                                final ImmutableList.Builder<VectorReference> writeTargetClusterAssignedVectorsBuilder =
                                        ImmutableList.builder();

                                for (final VectorReference vectorReference : targetCluster.getVectorReferences()) {
                                    final Tuple primaryKey = vectorReference.getId().getPrimaryKey();
                                    final VectorReference assignedVectorReference =
                                            targetClusterAssignedVectorsAsMap.get(primaryKey);

                                    if (assignedVectorReference != null) {
                                        //
                                        // Compare the version from the cluster with the version from the cleaned-up
                                        // set. At this point, it should not happen that they are different, so it's
                                        // more of a sanity check.
                                        //
                                        Verify.verify(assignedVectorReference.getId().getUuid()
                                                .equals(vectorReference.getId().getUuid()));

                                        //
                                        // What can happen is that a reference can toggle between primary and
                                        // replicated copy.
                                        //
                                        if (assignedVectorReference.isPrimaryCopy() != vectorReference.isPrimaryCopy()) {
                                            writeTargetClusterAssignedVectorsBuilder.add(assignedVectorReference);
                                        }
                                    } else {
                                        // add to delete list
                                        deleteTargetClusterAssignedVectorsBuilder.add(primaryKey);
                                    }
                                }

                                final ImmutableList<VectorReference> writeTargetClusterAssignedVectors =
                                        writeTargetClusterAssignedVectorsBuilder.build();

                                final ImmutableList<Tuple> deleteTargetClusterAssignedVectors =
                                        deleteTargetClusterAssignedVectorsBuilder.build();

                                updateAssignments(transaction, targetClusterMetadata, reassignmentResult,
                                        writeTargetClusterAssignedVectors, deleteTargetClusterAssignedVectors,
                                        quantizer);
                            }));
        });
    }

    @Nonnull
    private ReassignmentResult reassignVectorReferences(@Nonnull final Estimator estimator,
                                                        @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                                        @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood,
                                                        @Nonnull final List<VectorReference> vectorReferences) {
        final Config config = getConfig();

        final ImmutableMap.Builder<UUID, ClusterMetadataWithDistance> clusterIdMetadataMapBuilder =
                ImmutableMap.builder();

        clusterIdMetadataMapBuilder.put(targetClusterMetadataWithDistance.getClusterMetadata().getId(),
                targetClusterMetadataWithDistance);
        for (final ClusterMetadataWithDistance clusterMetadataWithDistance : outerNeighborhood) {
            clusterIdMetadataMapBuilder.put(clusterMetadataWithDistance.getClusterMetadata().getId(),
                    clusterMetadataWithDistance);
        }
        final ImmutableMap<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                clusterIdMetadataMapBuilder.build();

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignedVectorsMultimapBuilder =
                ImmutableListMultimap.builder();
        for (final VectorReference vectorReference : vectorReferences) {
            final PriorityQueue<ClusterMetadataWithDistance> nearestClusters =
                    new PriorityQueue<>(1 + outerNeighborhood.size(),
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
        return new ReassignmentResult(clusterIdMetadataMap, assignedVectorsMultimapBuilder.build());
    }

    private void updateAssignments(@Nonnull final Transaction transaction,
                                   @Nonnull final ClusterMetadata targetClusterMetadata,
                                   @Nonnull final ReassignmentResult reassignmentResult,
                                   @Nonnull final ImmutableList<VectorReference> writeTargetClusterAssignedVectors,
                                   @Nonnull final ImmutableList<Tuple> deleteTargetClusterAssignedVectors,
                                   @Nonnull final Quantizer quantizer) {
        final Config config = getConfig();
        final Primitives primitives = getLocator().primitives();

        // write all vector references
        final var assignmentMultiMap =
                reassignmentResult.getAssignmentMultimap();
        for (final Map.Entry<UUID, VectorReference> entry : assignmentMultiMap.entries()) {
            if (!entry.getKey().equals(targetClusterMetadata.getId())) {
                primitives.writeVectorReference(transaction, quantizer, entry.getKey(),
                        entry.getValue());
            }
        }

        // delete vectors that have been assigned out
        // write updates
        for (final Tuple primaryKey : deleteTargetClusterAssignedVectors) {
            primitives.deleteVectorReference(transaction, targetClusterMetadata.getId(), primaryKey);
        }

        // write updated vector references
        for (final VectorReference vectorReference : writeTargetClusterAssignedVectors) {
            primitives.writeVectorReference(transaction, quantizer, targetClusterMetadata.getId(), vectorReference);
        }

        // update all affected cluster metadata
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                reassignmentResult.getClusterIdMetadataMap();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.getClusterMetadata();
            if (assignmentMultiMap.containsKey(toBeWritten)) {
                final int numVectorsAdded = assignmentMultiMap.get(toBeWritten).size();
                ClusterMetadata newClusterMetadata;
                final int numTotalVectors = clusterMetadata.getNumVectors() + numVectorsAdded;
                if (!clusterMetadata.getStates().contains(ClusterMetadata.State.SPLIT_MERGE) &&
                        (numTotalVectors > config.getClusterMax() ||
                                 numTotalVectors < config.getClusterMin())) {
                    // create a split/merge task
                    primitives.writeDeferredTask(transaction,
                            SplitMergeTask.of(getLocator(), getAccessInfo(), UUID.randomUUID(),
                                    clusterId, clusterMetadataWithDistance.getCentroid()));

                    newClusterMetadata =
                            clusterMetadata.withAdditionalVectorsAndNewStates(numVectorsAdded,
                                    ClusterMetadata.State.SPLIT_MERGE);
                } else {
                    newClusterMetadata =
                            clusterMetadata.withAdditionalVectors(numVectorsAdded);
                }

                primitives.writeClusterMetadata(transaction, newClusterMetadata);
            }
        }
    }

    @Nonnull
    static ReassignTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                   @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.SPLIT_MERGE);
        final StorageTransform storageTransform = locator.primitives().storageTransform(accessInfo);
        final Transformed<RealVector> centroid = storageTransform.transform(
                StorageHelpers.vectorFromBytes(locator.getConfig(), valueTuple.getBytes(2)));

        return new ReassignTask(locator, accessInfo, keyTuple.getUUID(1),
                valueTuple.getUUID(1), centroid);
    }

    @Nonnull
    static ReassignTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                           @Nonnull final Transformed<RealVector> centroid) {
        return new ReassignTask(locator, accessInfo, taskId, clusterId, centroid);
    }

    private static class ReassignmentResult {
        @Nonnull
        private final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap;
        @Nonnull
        private final ImmutableListMultimap<UUID, VectorReference> assignmentMultimap;

        public ReassignmentResult(@Nonnull final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                  @Nonnull final ImmutableListMultimap<UUID, VectorReference> assignmentMultimap) {
            this.clusterIdMetadataMap = clusterIdMetadataMap;
            this.assignmentMultimap = assignmentMultimap;
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
