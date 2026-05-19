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
    private final List<ClusterReference> neighborhood;

    private ReassignTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                         @Nonnull final Transformed<RealVector> centroid,
                         @Nonnull final Set<UUID> causeClusterIds,
                         @Nonnull final List<ClusterReference> neighborhood) {
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
    private List<ClusterReference> getNeighborhood() {
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
        for (final ClusterReference clusterMetadataWithDistance : getNeighborhood()) {
            neighborhoodTuplesBuilder.add(
                    StorageAdapter.valueTupleFromClusterReference(quantizer,
                            clusterMetadataWithDistance));
        }

        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                StorageHelpers.bytesFromVector(encodedVector),
                StorageAdapter.tupleFromClusterIds(getCauseClusterIds()),
                Tuple.fromItems(neighborhoodTuplesBuilder.build()));
    }

    @Override
    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        super.writeDeferredTask(transaction);
        if (logger.isInfoEnabled()) {
            logger.info("enqueuing REASSIGN; taskId={}; clusterId={}",
                    AbstractDeferredTask.taskIdToString(getTaskId()), getTargetClusterId());
        }
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

                    final EnumSet<ClusterMetadata.State> states = clusterMetadata.states();
                    if (!states.contains(ClusterMetadata.State.REASSIGN) ||
                            states.contains(ClusterMetadata.State.SPLIT_MERGE) ||
                            states.contains(ClusterMetadata.State.COLLAPSE)) {
                        return AsyncUtil.DONE;
                    }

                    return reassign(transaction, clusterMetadata, untransformedCentroid);
                }).thenAccept(ignored -> logSuccessful(logger));
    }

    @Nonnull
    private CompletableFuture<Void> reassign(@Nonnull final Transaction transaction,
                                             @Nonnull final ClusterMetadata targetClusterMetadata,
                                             @Nonnull final RealVector targetClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(getTaskId());
        final Config config = getConfig();
        final Executor executor = getLocator().getExecutor();
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = config.reassignInnerNeighborhoodSize();
        final int numOuterNeighborhood = config.reassignOuterNeighborhoodSize();
        final int numNeighborhood = numInnerNeighborhood + numOuterNeighborhood;

        final List<ClusterReference> neighborhood = getNeighborhood();
        if (neighborhood.isEmpty()) {
            return primitives.fetchNeighborhoodClusterMetadata(transaction, targetClusterMetadata,
                            targetClusterCentroid, storageTransform, numNeighborhood)
                    .thenAccept(fetchedNeighborhood -> {
                        final ReassignTask reassignTask = withHighPriorityAndNeighborhood(random,
                                ClusterReference.fromClusterMetadataAndDistances(fetchedNeighborhood));
                        reassignTask.writeDeferredTask(transaction);
                        if (logger.isTraceEnabled()) {
                            logger.trace("enqueuing high priority REASSIGN due to refetch of neighborhood; taskId={}; neighborhoodSize={}",
                                    AbstractDeferredTask.taskIdToString(reassignTask.getTaskId()),
                                    reassignTask.getNeighborhood().size());
                        }
                    });
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("using precomputed neighborhood; taskId={}; neighborhoodSize={}",
                        taskIdToString(getTaskId()), getNeighborhood().size());
            }
        }

        return MoreAsyncUtil.forEach(neighborhood,
                        clusterIdAndCentroid -> primitives.fetchClusterMetadataWithDistance(transaction,
                                clusterIdAndCentroid.clusterId(), clusterIdAndCentroid.centroid(), 0.0d),
                        config.reassignConcurrency(), executor)
                .thenCompose(neighborhoodClusterMetadataWithDistances -> {

                    final Neighborhoods neighborhoods =
                            neighborhoods(neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(), numInnerNeighborhood, numOuterNeighborhood);

                    final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.innerNeighborhood();
                    final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.outerNeighborhood();

                    //
                    // At this point innerNeighborhood contains the clusters we want to split into
                    // innerNeighborhood.size() - 1 number of clusters and outerNeighborhood contains all clusters we
                    // may assign some vectors from innerNeighborhood to.
                    //
                    return primitives.fetchInnerClusters(transaction, innerNeighborhood, storageTransform)
                            .thenCompose(innerClusters -> primitives.cleanUpVectorReferences(transaction,
                                            innerClusters, false)
                                    .thenAccept(cleanedUpVectorReferences -> {
                                        final Reassignment reassignment =
                                                reassignVectorReferences(estimator, Iterables.getOnlyElement(innerNeighborhood),
                                                        outerNeighborhood, cleanedUpVectorReferences);
                                        final Cluster targetCluster = Iterables.getOnlyElement(innerClusters);
                                        final TargetClusterDelta delta =
                                                computeTargetClusterDelta(targetCluster, reassignment,
                                                        targetClusterMetadata.id());
                                        persistReassignment(transaction, random, targetClusterMetadata,
                                                reassignment, delta, quantizer);
                                    }));
                });
    }

    @Nonnull
    private Reassignment reassignVectorReferences(@Nonnull final Estimator estimator,
                                                  @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                                  @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood,
                                                  @Nonnull final List<VectorReference> vectorReferences) {
        final Config config = getConfig();

        final ImmutableMap.Builder<UUID, ClusterMetadataWithDistance> clusterIdMetadataMapBuilder =
                ImmutableMap.builder();

        final UUID targetClusterId = targetClusterMetadataWithDistance.clusterMetadata().id();
        clusterIdMetadataMapBuilder.put(targetClusterId, targetClusterMetadataWithDistance);
        for (final ClusterMetadataWithDistance clusterMetadataWithDistance : outerNeighborhood) {
            clusterIdMetadataMapBuilder.put(clusterMetadataWithDistance.clusterMetadata().id(),
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
        final Map<UUID, RunningStats> standardDeviationsMap = Maps.newHashMap();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID clusterId = entry.getKey();
            if (targetClusterId.equals(clusterId)) {
                standardDeviationsMap.put(clusterId, RunningStats.identity());
            } else {
                // Don't add the target as we re-add all vectors.
                standardDeviationsMap.put(clusterId,
                        entry.getValue().clusterMetadata().runningStandardDeviation());
            }
        }

        final NearestClustersResult nearestClustersResult =
                computeNearestClusters(estimator, vectorReferences, clusterIdMetadataMap);
        mergeStandardDeviationUpdates(standardDeviationsMap,
                nearestClustersResult.standardDeviationUpdates());
        final ImmutableListMultimap<UUID, ClusterMetadataWithDistance> invertedAssignmentsMap =
                nearestClustersResult.invertedAssignments();

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignmentBuilder =
                ImmutableListMultimap.builder();
        final TopK<VectorReference> replicatedTopK =
                TopK.max(Comparator.comparing(VectorReference::replicationPriority),
                        config.replicatedClusterTarget());

        RunningStats replicationPriorityStandardDeviation = RunningStats.identity();
        int numReplicated = 0;
        int numOccluded = 0;
        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy()) {
                replicatedTopK.add(vectorReference);
                continue;
            }

            final var nearestClusters =
                    Objects.requireNonNull(invertedAssignmentsMap.get(vectorReference.id().getUuid()));
            Verify.verify(!nearestClusters.isEmpty());
            final ClusterMetadataWithDistance primaryCluster = Objects.requireNonNull(nearestClusters.get(0));
            final double distanceToPrimaryCentroid = primaryCluster.distance();
            Verify.verify(Double.isFinite(distanceToPrimaryCentroid));

            final UUID primaryClusterId = primaryCluster.clusterMetadata().id();
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
                final double distance = replicationCandidate.distance();
                Verify.verify(Double.isFinite(distance));

                final ClusterMetadata replicationCandidateClusterMetadata =
                        replicationCandidate.clusterMetadata();

                //
                // The following test is written in a slightly more wordy but (I think) better to understand form.
                // We need to create a replicated reference in a cluster if we either encounter an underreplicated
                // primary vector OR if this REASSIGN-task as caused by a SPLIT task, and we need to repopulate its
                // new cluster's replicated vectors.
                //
                if (!(vectorReference.isUnderreplicated() ||
                              causeClusterIds.contains(replicationCandidateClusterMetadata.id()))) {
                    continue;
                }

                final RunningStats updatedStandardDeviation =
                        Objects.requireNonNull(
                                standardDeviationsMap.get(replicationCandidateClusterMetadata.id()));

                final double replicationPriority =
                        StorageAdapter.replicationPriority(distance, distanceToPrimaryCentroid,
                                Math.toIntExact(updatedStandardDeviation.numElements()),
                                updatedStandardDeviation.mean(),
                                updatedStandardDeviation.populationStandardDeviation());
                replicationPriorityStandardDeviation = replicationPriorityStandardDeviation.add(replicationPriority);
                if (replicationPriority >= config.replicationPriorityMin()) {
                    if (StorageAdapter.isOccluded(estimator, replicationCandidate, selectedReplicationClusters)) {
                        numOccluded++;
                        continue;
                    }

                    final VectorReference newVectorReference =
                            vectorReference.toReplicatedCopy(replicationPriority);
                    if (targetClusterId.equals(replicationCandidateClusterMetadata.id())) {
                        replicatedTopK.add(newVectorReference); // TODO remove this as this should never happen
                    } else {
                        assignmentBuilder.put(
                                replicationCandidateClusterMetadata.id(),
                                newVectorReference);
                    }
                    selectedReplicationClusters.add(replicationCandidate);
                    numReplicated++;
                }
            }
        }

        assignmentBuilder.putAll(targetClusterId, replicatedTopK.toUnsortedList());

        if (logger.isTraceEnabled()) {
            logger.trace("replication priority num={}. mean={}, standard deviation={}, numReplicated={}, numOccluded={}, lowestReplicationPriority={}",
                    replicationPriorityStandardDeviation.numElements(),
                    replicationPriorityStandardDeviation.mean(),
                    replicationPriorityStandardDeviation.populationStandardDeviation(),
                    numReplicated, numOccluded, replicatedTopK.worstElement()
                            .map(VectorReference::replicationPriority).orElse(0.0d));
        }

        return new Reassignment(clusterIdMetadataMap, assignmentBuilder.build(), standardDeviationsMap);
    }

    @Nonnull
    private TargetClusterDelta computeTargetClusterDelta(@Nonnull final Cluster targetCluster,
                                                         @Nonnull final Reassignment reassignment,
                                                         @Nonnull final UUID targetClusterId) {
        final List<VectorReference> targetClusterAssignedVectors =
                reassignment.assignmentMultimap().get(targetClusterId);
        return AbstractDeferredTask.computeTargetClusterDelta(targetCluster, targetClusterAssignedVectors);
    }

    private void persistReassignment(@Nonnull final Transaction transaction,
                                     @Nonnull final SplittableRandom random,
                                     @Nonnull final ClusterMetadata targetClusterMetadata,
                                     @Nonnull final Reassignment reassignment,
                                     @Nonnull final TargetClusterDelta delta,
                                     @Nonnull final Quantizer quantizer) {
        final WriteCounters counters = countAssignments(targetClusterMetadata, reassignment);
        writeOuterClusterVectors(transaction, quantizer, targetClusterMetadata, reassignment);
        persistTargetClusterDelta(transaction, quantizer, targetClusterMetadata.id(), delta);
        writeClusterMetadata(transaction, random, targetClusterMetadata, reassignment, counters, delta);
    }

    @Nonnull
    private WriteCounters countAssignments(@Nonnull final ClusterMetadata targetClusterMetadata,
                                           @Nonnull final Reassignment reassignment) {
        final ListMultimap<UUID, VectorReference> assignmentMultiMap = reassignment.assignmentMultimap();

        final Map<UUID, Integer> clusterIdToNumPrimaryVectorsAdded = Maps.newHashMap();
        final Map<UUID, Integer> clusterIdToNumPrimaryUnderreplicatedVectorsAdded = Maps.newHashMap();
        final Map<UUID, Integer> clusterIdToNumReplicatedVectorsAdded = Maps.newHashMap();

        int numPrimaryPushedOut = 0;
        int numReplicatedPushedOut = 0;

        for (final Map.Entry<UUID, VectorReference> entry : assignmentMultiMap.entries()) {
            final UUID clusterId = entry.getKey();
            final VectorReference vectorReference = entry.getValue();
            if (!clusterId.equals(targetClusterMetadata.id())) {
                if (vectorReference.isPrimaryCopy()) {
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

        return new WriteCounters(clusterIdToNumPrimaryVectorsAdded,
                clusterIdToNumPrimaryUnderreplicatedVectorsAdded,
                clusterIdToNumReplicatedVectorsAdded,
                numPrimaryPushedOut, numReplicatedPushedOut);
    }

    private void writeOuterClusterVectors(@Nonnull final Transaction transaction,
                                          @Nonnull final Quantizer quantizer,
                                          @Nonnull final ClusterMetadata targetClusterMetadata,
                                          @Nonnull final Reassignment reassignment) {
        final Primitives primitives = getLocator().primitives();
        final ListMultimap<UUID, VectorReference> assignmentMultiMap = reassignment.assignmentMultimap();

        for (final Map.Entry<UUID, VectorReference> entry : assignmentMultiMap.entries()) {
            final UUID clusterId = entry.getKey();
            if (!clusterId.equals(targetClusterMetadata.id())) {
                final VectorReference vectorReference = entry.getValue();
                if (vectorReference.isPrimaryCopy()) {
                    Verify.verify(vectorReference.isUnderreplicated());
                }
                primitives.writeVectorReference(transaction, quantizer, clusterId, vectorReference);
            }
        }
    }

    private void persistTargetClusterDelta(@Nonnull final Transaction transaction,
                                           @Nonnull final Quantizer quantizer,
                                           @Nonnull final UUID targetClusterId,
                                           @Nonnull final TargetClusterDelta delta) {
        final Primitives primitives = getLocator().primitives();

        for (final Tuple primaryKey : delta.toDelete()) {
            primitives.deleteVectorReference(transaction, targetClusterId, primaryKey);
        }

        for (final VectorReference vectorReference : delta.toWrite()) {
            primitives.writeVectorReference(transaction, quantizer, targetClusterId, vectorReference);
        }
    }

    private void writeClusterMetadata(@Nonnull final Transaction transaction,
                                      @Nonnull final SplittableRandom random,
                                      @Nonnull final ClusterMetadata targetClusterMetadata,
                                      @Nonnull final Reassignment reassignment,
                                      @Nonnull final WriteCounters counters,
                                      @Nonnull final TargetClusterDelta delta) {
        final Primitives primitives = getLocator().primitives();
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                reassignment.clusterIdMetadataMap();
        final Map<UUID, RunningStats> updatedStandardDeviationsMap =
                reassignment.updatedStandardDeviationsMap();

        ClusterMetadata newTargetClusterMetadata = null;
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.clusterMetadata();

            final int numPrimaryVectorsAdded = counters.numPrimaryVectorsAdded().getOrDefault(toBeWritten, 0);
            final int numPrimaryUnderreplicatedVectorsAdded = counters.numPrimaryUnderreplicatedVectorsAdded().getOrDefault(toBeWritten, 0);
            final int numReplicatedVectorsAdded = counters.numReplicatedVectorsAdded().getOrDefault(toBeWritten, 0);
            final RunningStats updatedStandardDeviation =
                    Objects.requireNonNull(updatedStandardDeviationsMap.get(toBeWritten));

            if (targetClusterMetadata.id().equals(clusterMetadata.id())) {
                Verify.verify(numPrimaryUnderreplicatedVectorsAdded == 0);
                newTargetClusterMetadata =
                        clusterMetadata.withNewVectors(0, numReplicatedVectorsAdded,
                                updatedStandardDeviation, EnumSet.noneOf(ClusterMetadata.State.class));
                primitives.writeClusterMetadata(transaction, newTargetClusterMetadata);
            } else {
                primitives.updateClusterMetadataAndEnqueueTaskMaybe(transaction, random, clusterMetadata,
                        clusterMetadataWithDistance.centroid(), getAccessInfo(),
                        numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
                        updatedStandardDeviation, ImmutableSet.of());
                if (logger.isTraceEnabled()) {
                    logger.trace("pushing vectors during reassign; clusterId={}; numTotalPrimaryVectors={}, numPrimaryVectorsAdded={}, " +
                                    "numTotalPrimaryUnderreplicatedReplicatedVectors={}, numPrimaryUnderreplicatedVectorsAdded={}, " +
                                    "numTotalReplicatedVectors={}, numReplicatedVectorsAdded={}",
                            clusterMetadata.id(),
                            clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded, numPrimaryVectorsAdded,
                            clusterMetadata.numPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded, numPrimaryUnderreplicatedVectorsAdded,
                            clusterMetadata.numReplicatedVectors() + numReplicatedVectorsAdded, numReplicatedVectorsAdded);
                }
            }
        }

        if (logger.isInfoEnabled()) {
            Objects.requireNonNull(newTargetClusterMetadata);
            logger.info("reassign stats; old.numPrimary={}, new.numPrimary={}, old.numReplicated={}, " +
                    "new.numReplicated={}, numDeleted={}, numUpdated={}, numPrimaryPushedOut={}, " +
                    "numReplicatedPushedOut={}", targetClusterMetadata.getNumPrimaryVectors(),
                    newTargetClusterMetadata.getNumPrimaryVectors(), targetClusterMetadata.numReplicatedVectors(),
                    newTargetClusterMetadata.numReplicatedVectors(), delta.toDelete().size(), delta.toWrite().size(),
                    counters.numPrimaryPushedOut(), counters.numReplicatedPushedOut());
        }
    }

    @Nonnull
    private ReassignTask withHighPriorityAndNeighborhood(@Nonnull final SplittableRandom random,
                                                         @Nonnull final List<ClusterReference> neighborhood) {
        return ReassignTask.of(getLocator(), getAccessInfo(),
                randomHighPriorityTaskId(random, getConfig().deterministicRandomness()), getTargetClusterId(),
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
        final ImmutableList.Builder<ClusterReference> neighborhoodsBuilder = ImmutableList.builder();
        final Tuple neighborhoodsTuple = valueTuple.getNestedTuple(4);
        for (int i = 0; i < neighborhoodsTuple.size(); i ++) {
            final Tuple clusterMetadataWithDistanceTuple = neighborhoodsTuple.getNestedTuple(i);
            neighborhoodsBuilder.add(StorageAdapter.clusterReferenceFromTuple(locator.getConfig(),
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
                           @Nonnull final List<ClusterReference> neighborhood) {
        return new ReassignTask(locator, accessInfo, taskId, clusterId, centroid, causeClusterIds, neighborhood);
    }

    private record Reassignment(@Nonnull Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                @Nonnull ListMultimap<UUID, VectorReference> assignmentMultimap,
                                @Nonnull Map<UUID, RunningStats> updatedStandardDeviationsMap) {
    }

    private record WriteCounters(@Nonnull Map<UUID, Integer> numPrimaryVectorsAdded,
                                 @Nonnull Map<UUID, Integer> numPrimaryUnderreplicatedVectorsAdded,
                                 @Nonnull Map<UUID, Integer> numReplicatedVectorsAdded,
                                 int numPrimaryPushedOut,
                                 int numReplicatedPushedOut) {
    }
}
