/*
 * ReassignTask.java
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
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ReassignTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ReassignTask.class);

    @Nonnull
    private final Transformed<RealVector> centroid;

    @Nonnull
    private final Set<UUID> causeClusterIds;
    @Nonnull
    private final List<ClusterIdAndCentroid> neighborhood;

    private ReassignTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                         @Nonnull final Transformed<RealVector> centroid,
                         @Nonnull final Set<UUID> causeClusterIds,
                         @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        super(locator, accessInfo, taskId, ImmutableSet.of(targetClusterId));
        this.centroid = centroid;
        this.causeClusterIds = ImmutableSet.copyOf(causeClusterIds);
        this.neighborhood = ImmutableList.copyOf(neighborhood);
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
    private List<ClusterIdAndCentroid> getNeighborhood() {
        return neighborhood;
    }

    @Nonnull
    public UUID getTargetClusterId() {
        return Iterables.getOnlyElement(getTargetClusterIds());
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        final Quantizer quantizer = getLocator().primitives().quantizer(getAccessInfo());
        final Transformed<RealVector> encodedVector = quantizer.encode(getCentroid());

        final ImmutableList.Builder<Object> neighborhoodTuplesBuilder = ImmutableList.builder();
        for (final ClusterIdAndCentroid clusterMetadataWithDistance : getNeighborhood()) {
            neighborhoodTuplesBuilder.add(
                    StorageAdapter.valueTupleFromClusterIdAndCentroid(quantizer,
                            clusterMetadataWithDistance));
        }

        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                StorageHelpers.bytesFromVector(encodedVector),
                StorageAdapter.tupleFromClusterIds(getCauseClusterIds()),
                Tuple.fromItems(neighborhoodTuplesBuilder.build()));
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
        final Config config = getConfig();
        final Executor executor = getLocator().getExecutor();
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = 1;
        final int numOuterNeighborhood = 31;
        final int numNeighborhood = numInnerNeighborhood + numOuterNeighborhood;

        final List<ClusterIdAndCentroid> neighborhood = getNeighborhood();
        if (neighborhood.isEmpty()) {
            return primitives.fetchNeighborhoodClusterMetadata(transaction, targetClusterMetadata,
                            targetClusterCentroid, storageTransform, numNeighborhood)
                    .thenAccept(fetchedNeighborhood -> {
                        final ReassignTask reassignTask = withHighPriorityAndNeighborhood(random,
                                config.isDeterministicRandomness(),
                                ClusterIdAndCentroid.fromClusterMetadataAndDistances(fetchedNeighborhood));
                        primitives.writeDeferredTask(transaction, reassignTask);
                        if (logger.isInfoEnabled()) {
                            logger.info("enqueuing high priority REASSIGN; taskId={}; clusterId={}; neighborhoodSize={}",
                                    AbstractDeferredTask.taskIdToString(reassignTask.getTaskId()),
                                    reassignTask.getTargetClusterId(),
                                    reassignTask.getNeighborhood().size());
                        }
                    });
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("using precomputed neighborhood; taskId={}; neighborhoodSize={}",
                        taskIdToString(getTaskId()), getNeighborhood().size());
            }
        }

        return MoreAsyncUtil.forEach(neighborhood,
                        clusterIdAndCentroid -> primitives.fetchClusterMetadataWithDistance(transaction,
                                clusterIdAndCentroid.getClusterId(), clusterIdAndCentroid.getCentroid(), 0.0d),
                        10, executor)
                .thenCompose(neighborhoodClusterMetadataWithDistances -> {

                    final NeighborhoodsResult neighborhoods =
                            primitives.neighborhoods(storageTransform, neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(), numInnerNeighborhood, numOuterNeighborhood);

                    final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.getInnerNeighborhood();
                    final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.getOuterNeighborhood();

                    //
                    // At this point innerNeighborhood contains the clusters we want to split into
                    // innerNeighborhood.size() - 1 number of clusters and outerNeighborhood contains all clusters we
                    // may assign some vectors from innerNeighborhood to.
                    //
                    return primitives.fetchInnerClusters(transaction, innerNeighborhood, storageTransform)
                            .thenCompose(innerClusters -> primitives.cleanUpVectorReferences(transaction,
                                            innerClusters, false)
                                    .thenAccept(cleanedUpVectorReferences -> {
                                        final ReassignmentResult reassignmentResult =
                                                reassignVectorReferences(estimator, Iterables.getOnlyElement(innerNeighborhood),
                                                        outerNeighborhood, cleanedUpVectorReferences);
                                        final ListMultimap<UUID, VectorReference> assignmentMultimap =
                                                reassignmentResult.getAssignmentMultimap();
                                        final List<VectorReference> targetClusterAssignedVectors =
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
                                                // replicated copy or primary and underreplicated primary, etc.
                                                //
                                                if (assignedVectorReference.isPrimaryCopy() != vectorReference.isPrimaryCopy() ||
                                                        assignedVectorReference.isUnderreplicated() != vectorReference.isUnderreplicated()) {
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
    private ReassignmentResult reassignVectorReferences(@Nonnull final Estimator estimator,
                                                        @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                                        @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood,
                                                        @Nonnull final List<VectorReference> vectorReferences) {
        final Config config = getConfig();

        final ImmutableMap.Builder<UUID, ClusterMetadataWithDistance> clusterIdMetadataMapBuilder =
                ImmutableMap.builder();

        final UUID targetClusterId = targetClusterMetadataWithDistance.getClusterMetadata().getId();
        clusterIdMetadataMapBuilder.put(targetClusterId, targetClusterMetadataWithDistance);
        for (final ClusterMetadataWithDistance clusterMetadataWithDistance : outerNeighborhood) {
            clusterIdMetadataMapBuilder.put(clusterMetadataWithDistance.getClusterMetadata().getId(),
                    clusterMetadataWithDistance);
        }
        final ImmutableMap<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                clusterIdMetadataMapBuilder.build();

        //
        // At this point clusterIdMetadataMap contains the new clusters after the split and all the clusters
        // from the outer neighborhood.
        //

        //
        // Initialize a map we need to use to keep track for the correct most up-to-date count, mean, and
        // standard deviations for distances.
        //
        final Map<UUID, RunningStandardDeviation> updatedStandardDeviationMap = Maps.newHashMap();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID clusterId = entry.getKey();
            if (targetClusterId.equals(clusterId)) {
                updatedStandardDeviationMap.put(clusterId, RunningStandardDeviation.identity());
            } else {
                // Don't add the target as we re-add all vectors.
                updatedStandardDeviationMap.put(clusterId,
                        entry.getValue().getClusterMetadata().getRunningStandardDeviation());
            }
        }

        final ImmutableListMultimap.Builder<UUID, ClusterMetadataWithDistance> invertedAssignmentsMapBuilder =
                ImmutableListMultimap.builder();
        for (final VectorReference vectorReference : vectorReferences) {
            // only considering primary copies here
            if (!vectorReference.isPrimaryCopy()) {
                continue;
            }

            //
            // Only for primary copies
            //

            final TopK<ClusterMetadataWithDistance> nearestClusters =
                    new TopK<>(Comparator.comparing(ClusterMetadataWithDistance::getDistance).reversed(), 32);
            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : clusterIdMetadataMap.values()) {
                final double distance =
                        estimator.distance(vectorReference.getVector(), clusterMetadataWithDistance.getCentroid());
                nearestClusters.add(clusterMetadataWithDistance.withNewDistance(distance));
            }

            final List<ClusterMetadataWithDistance> sortedNearestClusters = nearestClusters.toSortedList();
            Verify.verify(!sortedNearestClusters.isEmpty());

            final ClusterMetadataWithDistance primaryClusterMetadataWithDistance = sortedNearestClusters.get(0);
            final ClusterMetadata primaryClusterMetadata = primaryClusterMetadataWithDistance.getClusterMetadata();

            updateRunningStandardDeviationsMap(updatedStandardDeviationMap,
                    primaryClusterMetadata.getId(), primaryClusterMetadataWithDistance.getDistance());

            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : sortedNearestClusters) {
                invertedAssignmentsMapBuilder.put(vectorReference.getId().getUuid(),
                        clusterMetadataWithDistance);
            }
        }
        final ImmutableListMultimap<UUID, ClusterMetadataWithDistance> invertedAssignmentsMap =
                invertedAssignmentsMapBuilder.build();

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignmentBuilder =
                ImmutableListMultimap.builder();
        final TopK<VectorReference> replicatedTopK =
                new TopK<>(Comparator.comparing(VectorReference::getReplicationPriority),
                        config.getReplicatedClusterTarget());

        RunningStandardDeviation replicationPriorityStandardDeviation = RunningStandardDeviation.identity();
        int numReplicated = 0;
        int numOccluded = 0;
        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy()) {
                replicatedTopK.add(vectorReference);
                continue;
            }

            final var nearestClusters =
                    Objects.requireNonNull(invertedAssignmentsMap.get(vectorReference.getId().getUuid()));
            Verify.verify(!nearestClusters.isEmpty());
            final ClusterMetadataWithDistance primaryCluster = Objects.requireNonNull(nearestClusters.get(0));
            final double distanceToPrimaryCentroid = primaryCluster.getDistance();
            Verify.verify(Double.isFinite(distanceToPrimaryCentroid));

            final UUID primaryClusterId = primaryCluster.getClusterMetadata().getId();
            if (!targetClusterId.equals(primaryClusterId)) {
                assignmentBuilder.put(
                        primaryClusterId,
                        vectorReference.toPrimaryUnderreplicatedCopy());
                // skip writing replicated copies as the new clusters do not "own" this vector anymore
                continue;
            }

            assignmentBuilder.put(primaryClusterId, vectorReference.toPrimaryCopy());

            final Set<UUID> causeClusterIds = getCauseClusterIds();

            final ImmutableList<ClusterMetadataWithDistance> replicationCandidates =
                    nearestClusters.subList(1, nearestClusters.size());
            final List<ClusterMetadataWithDistance> selectedReplicationClusters =
                    Lists.newArrayListWithExpectedSize(replicationCandidates.size());

            for (final ClusterMetadataWithDistance replicationCandidate : replicationCandidates) {
                final double distance = replicationCandidate.getDistance();
                Verify.verify(Double.isFinite(distance));

                final ClusterMetadata replicationCandidateClusterMetadata =
                        replicationCandidate.getClusterMetadata();

                //
                // The following test is written in a slightly more wordy but (I think) better to understand form.
                // We need to create a replicated reference in a cluster if we either encounter an underreplicated
                // primary vector OR if this REASSIGN-task as caused by a SPLIT task, and we need to repopulate its
                // new cluster's replicated vectors.
                //
                if (!(vectorReference.isUnderreplicated() ||
                              causeClusterIds.contains(replicationCandidateClusterMetadata.getId()))) {
                    continue;
                }

                final RunningStandardDeviation updatedStandardDeviation =
                        Objects.requireNonNull(
                                updatedStandardDeviationMap.get(replicationCandidateClusterMetadata.getId()));

                final double replicationPriority =
                        StorageAdapter.replicationPriority(distance, distanceToPrimaryCentroid,
                                Math.toIntExact(updatedStandardDeviation.getNumElements()),
                                updatedStandardDeviation.mean(),
                                updatedStandardDeviation.populationStandardDeviation());
                replicationPriorityStandardDeviation = replicationPriorityStandardDeviation.add(replicationPriority);
                if (replicationPriority >= config.getReplicationPriorityMin()) {
                    if (StorageAdapter.isOccluded(estimator, replicationCandidate, selectedReplicationClusters)) {
                        numOccluded++;
                        continue;
                    }

                    final VectorReference newVectorReference =
                            vectorReference.toReplicatedCopy(replicationPriority);
                    if (targetClusterId.equals(replicationCandidateClusterMetadata.getId())) {
                        replicatedTopK.add(newVectorReference); // TODO remove this as this should never happen
                    } else {
                        assignmentBuilder.put(
                                replicationCandidateClusterMetadata.getId(),
                                newVectorReference);
                    }
                    selectedReplicationClusters.add(replicationCandidate);
                    numReplicated++;
                }
            }
        }

        assignmentBuilder.putAll(targetClusterId, replicatedTopK.toUnsortedList());

        if (logger.isInfoEnabled()) {
            logger.info("replication priority num={}. mean={}, standard deviation={}, numReplicated={}, numOccluded={}, lowestReplicationPriority={}",
                    replicationPriorityStandardDeviation.getNumElements(),
                    replicationPriorityStandardDeviation.mean(),
                    replicationPriorityStandardDeviation.populationStandardDeviation(),
                    numReplicated, numOccluded, replicatedTopK.worstElement()
                            .map(VectorReference::getReplicationPriority).orElse(0.0d));
        }

        return new ReassignmentResult(clusterIdMetadataMap, assignmentBuilder.build(), updatedStandardDeviationMap);
    }

    private void updateAssignments(@Nonnull final Transaction transaction,
                                   @Nonnull final SplittableRandom random,
                                   @Nonnull final ClusterMetadata targetClusterMetadata,
                                   @Nonnull final ReassignmentResult reassignmentResult,
                                   @Nonnull final ImmutableList<VectorReference> writeTargetClusterAssignedVectors,
                                   @Nonnull final ImmutableList<Tuple> deleteTargetClusterAssignedVectors,
                                   @Nonnull final Quantizer quantizer) {
        final Primitives primitives = getLocator().primitives();

        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                reassignmentResult.getClusterIdMetadataMap();
        final ListMultimap<UUID, VectorReference> assignmentMultiMap = reassignmentResult.getAssignmentMultimap();

        final Map<UUID, Integer> clusterIdToNumPrimaryVectorsAdded = Maps.newHashMap();
        final Map<UUID, Integer> clusterIdToNumPrimaryUnderreplicatedVectorsAdded = Maps.newHashMap();
        final Map<UUID, Integer> clusterIdToNumReplicatedVectorsAdded = Maps.newHashMap();

        int numPrimaryPushedOut = 0;
        int numReplicatedPushedOut = 0;

        // write all vector references outside the target cluster
        for (final Map.Entry<UUID, VectorReference> entry : assignmentMultiMap.entries()) {
            final UUID clusterId = entry.getKey();
            final VectorReference vectorReference = entry.getValue();
            if (!clusterId.equals(targetClusterMetadata.getId())) {
                primitives.writeVectorReference(transaction, quantizer, clusterId,
                        vectorReference);
                if (vectorReference.isPrimaryCopy()) {
                    Verify.verify(vectorReference.isUnderreplicated());
                    numPrimaryPushedOut++;
                } else {
                    numReplicatedPushedOut++;
                }
            }
            if (vectorReference.isPrimaryCopy()) {
                incrementCounter(clusterIdToNumPrimaryVectorsAdded, clusterId);
                if (vectorReference.isUnderreplicated()) {
                    incrementCounter(clusterIdToNumPrimaryUnderreplicatedVectorsAdded, clusterId);
                }
            } else {
                incrementCounter(clusterIdToNumReplicatedVectorsAdded, clusterId);
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

        final Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap =
                reassignmentResult.getUpdatedStandardDeviationsMap();
        ClusterMetadata newTargetClusterMetadata = null;
        // update all affected cluster metadata
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.getClusterMetadata();

            final int numPrimaryVectorsAdded = clusterIdToNumPrimaryVectorsAdded.getOrDefault(toBeWritten, 0);
            final int numPrimaryUnderreplicatedVectorsAdded = clusterIdToNumPrimaryUnderreplicatedVectorsAdded.getOrDefault(toBeWritten, 0);
            final int numReplicatedVectorsAdded = clusterIdToNumReplicatedVectorsAdded.getOrDefault(toBeWritten, 0);
            final RunningStandardDeviation updatedStandardDeviation =
                    Objects.requireNonNull(updatedStandardDeviationsMap.get(toBeWritten));

            if (targetClusterMetadata.getId().equals(clusterMetadata.getId())) {
                Verify.verify(numPrimaryUnderreplicatedVectorsAdded == 0);
                newTargetClusterMetadata =
                        clusterMetadata.withNewVectors(0, numReplicatedVectorsAdded,
                                updatedStandardDeviation, EnumSet.noneOf(ClusterMetadata.State.class));
                primitives.writeClusterMetadata(transaction, newTargetClusterMetadata);
            } else {
                primitives.writeDeferredTaskMaybe(transaction, random, clusterMetadata,
                        clusterMetadataWithDistance.getCentroid(), getAccessInfo(),
                        numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
                        updatedStandardDeviation, ImmutableSet.of());
                if (logger.isTraceEnabled()) {
                    logger.trace("pushing vectors during reassign; clusterId={}; numTotalPrimaryVectors={}, numPrimaryVectorsAdded={}, " +
                                    "numTotalPrimaryUnderreplicatedReplicatedVectors={}, numPrimaryUnderreplicatedVectorsAdded={}, " +
                                    "numTotalReplicatedVectors={}, numReplicatedVectorsAdded={}",
                            clusterMetadata.getId(),
                            clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded, numPrimaryVectorsAdded,
                            clusterMetadata.getNumPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded, numPrimaryUnderreplicatedVectorsAdded,
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
    private ReassignTask withHighPriorityAndNeighborhood(@Nonnull final SplittableRandom random,
                                                         final boolean deterministicRandomness,
                                                         @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        return ReassignTask.of(getLocator(), getAccessInfo(),
                randomHighPriorityTaskId(random, deterministicRandomness), getTargetClusterId(),
                getCentroid(), getCauseClusterIds(), neighborhood);
    }

    @Nonnull
    static ReassignTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                   @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.REASSIGN);
        final StorageTransform storageTransform = locator.primitives().storageTransform(accessInfo);

        final UUID targetClusterId = valueTuple.getUUID(1);
        final Transformed<RealVector> centroid = storageTransform.transform(
                StorageHelpers.vectorFromBytes(locator.getConfig(), valueTuple.getBytes(2)));
        final Set<UUID> causeClusterIds = StorageAdapter.clusterIdsFromTuple(valueTuple.getNestedTuple(3));
        final ImmutableList.Builder<ClusterIdAndCentroid> neighborhoodsBuilder = ImmutableList.builder();
        final Tuple neighborhoodsTuple = valueTuple.getNestedTuple(4);
        for (int i = 0; i < neighborhoodsTuple.size(); i ++) {
            final Tuple clusterMetadataWithDistanceTuple = neighborhoodsTuple.getNestedTuple(i);
            neighborhoodsBuilder.add(StorageAdapter.clusterIdAndCentroidFromTuple(locator.getConfig(),
                    storageTransform, clusterMetadataWithDistanceTuple));
        }

        return new ReassignTask(locator, accessInfo, keyTuple.getUUID(0), targetClusterId, centroid,
                causeClusterIds, neighborhoodsBuilder.build());
    }

    @Nonnull
    static ReassignTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                           @Nonnull final Transformed<RealVector> centroid,
                           @Nonnull final Set<UUID> causeClusterIds) {
        return of(locator, accessInfo, taskId, clusterId, centroid, causeClusterIds, ImmutableList.of());
    }

    @Nonnull
    static ReassignTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                           @Nonnull final Transformed<RealVector> centroid,
                           @Nonnull final Set<UUID> causeClusterIds,
                           @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        return new ReassignTask(locator, accessInfo, taskId, clusterId, centroid, causeClusterIds, neighborhood);
    }

    private static class ReassignmentResult {
        @Nonnull
        private final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap;
        @Nonnull
        private final ListMultimap<UUID, VectorReference> assignmentMultimap;
        @Nonnull
        private final Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap;

        public ReassignmentResult(@Nonnull final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                  @Nonnull final ListMultimap<UUID, VectorReference> assignmentMultimap,
                                  @Nonnull final Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap) {
            this.clusterIdMetadataMap = clusterIdMetadataMap;
            this.assignmentMultimap = assignmentMultimap;
            this.updatedStandardDeviationsMap = updatedStandardDeviationsMap;
        }

        @Nonnull
        public Map<UUID, ClusterMetadataWithDistance> getClusterIdMetadataMap() {
            return clusterIdMetadataMap;
        }

        @Nonnull
        public ListMultimap<UUID, VectorReference> getAssignmentMultimap() {
            return assignmentMultimap;
        }

        @Nonnull
        public Map<UUID, RunningStandardDeviation> getUpdatedStandardDeviationsMap() {
            return updatedStandardDeviationsMap;
        }
    }
}
