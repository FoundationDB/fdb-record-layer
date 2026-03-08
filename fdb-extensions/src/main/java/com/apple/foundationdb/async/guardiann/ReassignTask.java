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

public class ReassignTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ReassignTask.class);

    @Nonnull
    private final Transformed<RealVector> centroid;

    @Nonnull
    private final Set<UUID> causeClusterIds;

    private ReassignTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                         @Nonnull final Transformed<RealVector> centroid,
                         @Nonnull final Set<UUID> causeClusterIds) {
        super(locator, accessInfo, taskId, targetClusterId);
        this.centroid = centroid;
        this.causeClusterIds = ImmutableSet.copyOf(causeClusterIds);
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    public Set<UUID> getCauseClusterIds() {
        return causeClusterIds;
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        final Quantizer quantizer = getLocator().primitives().quantizer(getAccessInfo());
        final Transformed<RealVector> encodedVector = quantizer.encode(getCentroid());
        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                encodedVector.getUnderlyingVector().getRawData(),
                StorageAdapter.tupleFromClusterIds(getCauseClusterIds()));
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.REASSIGN;
    }

    @Nonnull
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        logStart(logger);

        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final RealVector untransformedCentroid = storageTransform.untransform(getCentroid());

        return primitives.fetchClusterMetadata(transaction, getTargetClusterId())
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
                }).thenAccept(ignored -> logSuccessful(logger));
    }

    @Nonnull
    private CompletableFuture<Void> reassign(@Nonnull final Transaction transaction,
                                             @Nonnull final ClusterMetadata targetClusterMetadata,
                                             @Nonnull final RealVector targetClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(targetClusterMetadata.getId());
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
                                        reassignVectorReferences(random, estimator, Iterables.getOnlyElement(innerNeighborhood),
                                                outerNeighborhood, cleanedUpVectorReferences);
                                final ImmutableListMultimap<UUID, VectorReference> primaryAssignmentMultimap =
                                        reassignmentResult.getPrimaryAssignmentMultimap();
                                final ImmutableList<VectorReference> targetClusterPrimaryAssignedVectors =
                                        primaryAssignmentMultimap.get(targetClusterMetadata.getId());
                                final ImmutableListMultimap<UUID, VectorReference> replicatedAssignmentMultimap =
                                        reassignmentResult.getReplicatedAssignmentMultimap();
                                final ImmutableList<VectorReference> targetClusterReplicatedAssignedVectors =
                                        replicatedAssignmentMultimap.get(targetClusterMetadata.getId());
                                final ImmutableMap.Builder<Tuple, VectorReference> targetClusterAssignedVectorsAsMapBuilder = ImmutableMap.builder();
                                for (final VectorReference targetClusterAssignedVector :
                                        Iterables.concat(targetClusterPrimaryAssignedVectors, targetClusterReplicatedAssignedVectors)) {
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

                                updateAssignments(transaction, random, targetClusterMetadata, reassignmentResult,
                                        writeTargetClusterAssignedVectors, deleteTargetClusterAssignedVectors,
                                        quantizer);
                            }));
        });
    }

    @Nonnull
    private ReassignmentResult reassignVectorReferences(@Nonnull final SplittableRandom random,
                                                        @Nonnull final Estimator estimator,
                                                        @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                                        @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood,
                                                        @Nonnull final List<VectorReference> vectorReferences) {
        final Config config = getConfig();

        final ImmutableMap.Builder<UUID, ClusterMetadataWithDistance> clusterIdMetadataMapBuilder =
                ImmutableMap.builder();

        final UUID targetClusterId = targetClusterMetadataWithDistance.getClusterMetadata().getId();
        clusterIdMetadataMapBuilder.put(targetClusterId,
                targetClusterMetadataWithDistance);
        for (final ClusterMetadataWithDistance clusterMetadataWithDistance : outerNeighborhood) {
            clusterIdMetadataMapBuilder.put(clusterMetadataWithDistance.getClusterMetadata().getId(),
                    clusterMetadataWithDistance);
        }
        final ImmutableMap<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                clusterIdMetadataMapBuilder.build();

        final ImmutableListMultimap.Builder<UUID, VectorReference> primaryAssignmentBuilder =
                ImmutableListMultimap.builder();
        final ImmutableListMultimap.Builder<UUID, VectorReference> replicatedAssignmentBuilder =
                ImmutableListMultimap.builder();
        final ReservoirSampler<VectorReference> replicatedAssignmentSampler =
                new ReservoirSampler<>(config.getReplicatedClusterTarget(), random);

        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy()) {
                replicatedAssignmentSampler.add(vectorReference);
                continue;
            }

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

            primaryAssignmentBuilder.put(
                    primaryCluster.getClusterMetadata().getId(),
                    vectorReference.toPrimaryCopy());

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
                    if (targetClusterId.equals(replicatedCluster.getClusterMetadata().getId())) {
                        replicatedAssignmentSampler.add(newVectorReference);
                    } else {
                        replicatedAssignmentBuilder.put(
                                replicatedCluster.getClusterMetadata().getId(),
                                newVectorReference);
                    }
                } else {
                    break;
                }
            }
        }

        replicatedAssignmentBuilder.putAll(targetClusterId,
                replicatedAssignmentSampler.sample());

        return new ReassignmentResult(clusterIdMetadataMap, primaryAssignmentBuilder.build(),
                replicatedAssignmentBuilder.build());
    }

    private void updateAssignments(@Nonnull final Transaction transaction,
                                   @Nonnull final SplittableRandom random,
                                   @Nonnull final ClusterMetadata targetClusterMetadata,
                                   @Nonnull final ReassignmentResult reassignmentResult,
                                   @Nonnull final ImmutableList<VectorReference> writeTargetClusterAssignedVectors,
                                   @Nonnull final ImmutableList<Tuple> deleteTargetClusterAssignedVectors,
                                   @Nonnull final Quantizer quantizer) {
        final Primitives primitives = getLocator().primitives();

        int numPrimaryPushedOut = 0;
        // write all vector references
        final var primaryAssignmentMultiMap =
                reassignmentResult.getPrimaryAssignmentMultimap();
        for (final Map.Entry<UUID, VectorReference> entry : primaryAssignmentMultiMap.entries()) {
            if (!entry.getKey().equals(targetClusterMetadata.getId())) {
                primitives.writeVectorReference(transaction, quantizer, entry.getKey(),
                        entry.getValue());
                numPrimaryPushedOut++;
            }
        }

        int numReplicatedPushedOut = 0;
        final var replicatedAssignmentMultiMap =
                reassignmentResult.getReplicatedAssignmentMultimap();
        for (final Map.Entry<UUID, VectorReference> entry : replicatedAssignmentMultiMap.entries()) {
            if (!entry.getKey().equals(targetClusterMetadata.getId())) {
                primitives.writeVectorReference(transaction, quantizer, entry.getKey(),
                        entry.getValue());
                numReplicatedPushedOut++;
            }
        }

        // delete vectors that have been assigned out
        // write updates
        int numDeleted = 0;
        for (final Tuple primaryKey : deleteTargetClusterAssignedVectors) {
            primitives.deleteVectorReference(transaction, targetClusterMetadata.getId(), primaryKey);
            numDeleted++;
        }

        // write updated vector references
        int numUpdated = 0;
        for (final VectorReference vectorReference : writeTargetClusterAssignedVectors) {
            primitives.writeVectorReference(transaction, quantizer, targetClusterMetadata.getId(), vectorReference);
            numUpdated++;
        }

        ClusterMetadata newTargetClusterMetadata = null;

        // update all affected cluster metadata
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                reassignmentResult.getClusterIdMetadataMap();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.getClusterMetadata();

            final int numPrimaryVectorsAdded = primaryAssignmentMultiMap.get(toBeWritten).size();
            final int numReplicatedVectorsAdded = replicatedAssignmentMultiMap.get(toBeWritten).size();

            if (targetClusterMetadata.getId().equals(clusterMetadata.getId())) {
                newTargetClusterMetadata = clusterMetadata.withNewVectors(numPrimaryVectorsAdded,
                        numReplicatedVectorsAdded, EnumSet.noneOf(ClusterMetadata.State.class));
                primitives.writeClusterMetadata(transaction, newTargetClusterMetadata);
            } else {
                primitives.writeDeferredTaskMaybe(transaction, random, clusterMetadata,
                                clusterMetadataWithDistance.getCentroid(), getAccessInfo(), numPrimaryVectorsAdded,
                                numReplicatedVectorsAdded, ImmutableSet.of());
                if (logger.isInfoEnabled()) {
                    logger.info("pushing vectors; clusterId={}; numTotalPrimaryVectors={}, numPrimaryVectorsAdded={}, " +
                                    "numTotalReplicatedVectors={}, numReplicatedVectorsAdded={}",
                            clusterMetadata.getId(),
                            clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded, numPrimaryVectorsAdded,
                            clusterMetadata.getNumReplicatedVectors() + numReplicatedVectorsAdded, numReplicatedVectorsAdded);
                }
            }
        }

        // log everything
        if (logger.isInfoEnabled()) {
            Objects.requireNonNull(newTargetClusterMetadata);
            logger.info("reassign stats; old.numPrimary={}, new.numPrimary={}, old.numReplicated={}, " +
                    "new.numReplicated={}, numDeleted={}, numUpdated={}, numPrimaryPushedOut={}, " +
                    "numReplicatedPushedOut={}", targetClusterMetadata.getNumPrimaryVectors(),
                    newTargetClusterMetadata.getNumPrimaryVectors(), targetClusterMetadata.getNumReplicatedVectors(),
                    newTargetClusterMetadata.getNumReplicatedVectors(), numDeleted, numUpdated, numPrimaryPushedOut,
                    numReplicatedPushedOut);
        }
    }

    @Nonnull
    static ReassignTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                   @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.REASSIGN);
        final StorageTransform storageTransform = locator.primitives().storageTransform(accessInfo);

        final UUID taskUuid = valueTuple.getUUID(1);
        final Transformed<RealVector> centroid = storageTransform.transform(
                StorageHelpers.vectorFromBytes(locator.getConfig(), valueTuple.getBytes(2)));

        final Set<UUID> causeClusterIds = StorageAdapter.clusterIdsFromTuple(valueTuple.getNestedTuple(3));
        return new ReassignTask(locator, accessInfo, keyTuple.getUUID(0),
                taskUuid, centroid, causeClusterIds);
    }

    @Nonnull
    static ReassignTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                           @Nonnull final Transformed<RealVector> centroid,
                           @Nonnull final Set<UUID> causeClusterIds) {
        return new ReassignTask(locator, accessInfo, taskId, clusterId, centroid, causeClusterIds);
    }

    private static class ReassignmentResult {
        @Nonnull
        private final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap;
        @Nonnull
        private final ImmutableListMultimap<UUID, VectorReference> primaryAssignmentMultimap;
        @Nonnull
        private final ImmutableListMultimap<UUID, VectorReference> replicatedAssignmentMultimap;

        public ReassignmentResult(@Nonnull final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                  @Nonnull final ImmutableListMultimap<UUID, VectorReference> primaryAssignmentMultimap,
                                  @Nonnull final ImmutableListMultimap<UUID, VectorReference> replicatedAssignmentMultimap) {
            this.clusterIdMetadataMap = clusterIdMetadataMap;
            this.primaryAssignmentMultimap = primaryAssignmentMultimap;
            this.replicatedAssignmentMultimap = replicatedAssignmentMultimap;
        }

        @Nonnull
        public Map<UUID, ClusterMetadataWithDistance> getClusterIdMetadataMap() {
            return clusterIdMetadataMap;
        }

        @Nonnull
        public ImmutableListMultimap<UUID, VectorReference> getPrimaryAssignmentMultimap() {
            return primaryAssignmentMultimap;
        }

        @Nonnull
        public ImmutableListMultimap<UUID, VectorReference> getReplicatedAssignmentMultimap() {
            return replicatedAssignmentMultimap;
        }
    }
}
