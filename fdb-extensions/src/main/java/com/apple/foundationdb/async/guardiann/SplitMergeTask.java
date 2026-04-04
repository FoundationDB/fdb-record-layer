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
import com.apple.foundationdb.async.guardiann.SplitMergeEvaluator.UpgradeResult;
import com.apple.foundationdb.async.hnsw.HNSW;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

public class SplitMergeTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(SplitMergeTask.class);

    @Nonnull
    private final Transformed<RealVector> centroid;
    @Nonnull
    private final List<ClusterIdAndCentroid> neighborhood;

    private SplitMergeTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                           @Nonnull final Transformed<RealVector> centroid,
                           @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        super(locator, accessInfo, taskId, ImmutableSet.of(targetClusterId));
        this.centroid = centroid;
        this.neighborhood = ImmutableList.copyOf(neighborhood);
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    private List<ClusterIdAndCentroid> getNeighborhood() {
        return neighborhood;
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

        final ImmutableList.Builder<Object> neighborhoodTuplesBuilder = ImmutableList.builder();
        for (final ClusterIdAndCentroid clusterMetadataWithDistance : getNeighborhood()) {
            neighborhoodTuplesBuilder.add(
                    StorageAdapter.valueTupleFromClusterIdAndCentroid(quantizer, clusterMetadataWithDistance));
        }

        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                StorageHelpers.bytesFromVector(encodedVector), Tuple.fromItems(neighborhoodTuplesBuilder.build()));
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
                        primitives.writeClusterMetadata(transaction, clusterMetadata.withNewStates(newStates));
                        clusterMetadata.withNewStates(newStates);
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
        final Config config = getConfig();
        final SplittableRandom random = RandomHelpers.random(targetClusterMetadata.getId());
        final Primitives primitives = primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numNeighborhood = 16;

        final List<ClusterIdAndCentroid> neighborhood = getNeighborhood();
        if (neighborhood.isEmpty()) {
            // if the neighborhoods are not set yet, fetch the neighborhood and write an urgent new task
            return primitives.fetchNeighborhoodClusterMetadata(transaction, targetClusterMetadata,
                    targetClusterCentroid, storageTransform, numNeighborhood)
                    .thenAccept(fetchedNeighborhood -> {
                        final SplitMergeTask splitMergeTask =
                                withHighPriorityAndNeighborhood(random,
                                        config.isDeterministicRandomness(),
                                        ClusterIdAndCentroid.fromClusterMetadataAndDistances(fetchedNeighborhood));
                        primitives.writeDeferredTask(transaction, splitMergeTask);
                        if (logger.isInfoEnabled()) {
                            logger.info("enqueuing high priority SPLIT_MERGE; taskId={}; clusterId={}; neighborhoodSize={}",
                                    AbstractDeferredTask.taskIdToString(splitMergeTask.getTaskId()),
                                    splitMergeTask.getTargetClusterId(),
                                    splitMergeTask.getNeighborhood().size());
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
                    final NeighborhoodsResult neighborhoods1To2 =
                            primitives.neighborhoods(storageTransform, neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(),
                                    1, numNeighborhood - 1);
                    final NeighborhoodsResult neighborhoods2To3 =
                            neighborhood.size() < 2
                            ? null
                            : primitives.neighborhoods(storageTransform, neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(),
                                    2, numNeighborhood - 2);

                    final List<NeighborhoodsResult> allNeighborhoods =
                            Lists.newArrayList(neighborhoods1To2, neighborhoods2To3);

                    return primitives.fetchInnerClusters(transaction,
                                    maxInner(neighborhoods1To2, neighborhoods2To3), storageTransform)
                            .thenCompose(innerClusters -> RandomHelpers.forEach(random, allNeighborhoods,
                                            (neighborhoods, nestedRandom) -> {
                                                if (neighborhoods == null) {
                                                    return CompletableFuture.completedFuture(null);
                                                }
                                                final var innerNeighborhood = neighborhoods.getInnerNeighborhood();
                                                final var clampedInnerClusters =
                                                        innerNeighborhood.size() == innerClusters.size()
                                                        ? innerClusters
                                                        : innerClusters.subList(0, innerNeighborhood.size());

                                                return primitives.cleanUpVectorReferences(transaction, clampedInnerClusters, true)
                                                        .thenApply(vectorReferences ->
                                                                kMeans(neighborhoods, vectorReferences, nestedRandom, estimator));
                                            }, 10, executor)
                                    .thenApply(assignmentCandidates -> {
                                        final AssignmentCandidate split1to2Candidate =
                                                Objects.requireNonNull(assignmentCandidates.get(0));
                                        final UpgradeResult upgradeResult1to2 =
                                                evaluateNewPartition(estimator, ImmutableList.of(innerClusters.get(0)),
                                                        split1to2Candidate);
                                        final AssignmentCandidate split2to3Candidate = assignmentCandidates.get(1);
                                        if (split2to3Candidate != null) {
                                            Verify.verify(innerClusters.size() > 1);
                                            final UpgradeResult upgradeResult2to3 =
                                                    evaluateNewPartition(estimator, innerClusters,
                                                            split2to3Candidate);
                                            return upgradeResult1to2.getScoreGain() > upgradeResult2to3.getScoreGain()
                                                   ? split1to2Candidate : split2to3Candidate;
                                        }
                                        return split1to2Candidate;
                                    }))
                            .thenCompose(assignmentCandidate -> {
                                if (assignmentCandidate == null) {
                                    return null;
                                }

                                final NeighborhoodsResult neighborhoods = assignmentCandidate.getNeighborhoods();
                                final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.getInnerNeighborhood();
                                final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.getOuterNeighborhood();

                                final AssignmentResult assignmentResult =
                                        assignPrimaryVectorReferences(estimator, outerNeighborhood,
                                                assignmentCandidate.getPrimaryVectorReferences(),
                                                assignmentCandidate.getkMeansResult(),
                                                innerNeighborhood.size() + 1);
                                return updateHnsw(transaction, storageTransform, innerNeighborhood, assignmentResult)
                                        .thenAccept(ignored ->
                                                updateAssignments(transaction, random, innerNeighborhood, assignmentResult, quantizer));
                            });
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
                primitives.neighborhoods(transaction, storageTransform, targetClusterMetadata, getCentroid(),
                        targetClusterCentroid, numInnerNeighborhood, numOuterNeighborhood);

        return neighborhoodsFuture.thenCompose(neighborhoods -> {
            final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.getInnerNeighborhood();
            final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.getOuterNeighborhood();

            //
            // At this point innerNeighborhood contains the clusters we want to split into
            // innerNeighborhood.size() - 1 number of clusters and outerNeighborhood contains all clusters we
            // may assign some vectors from innerNeighborhood to.
            //
            return primitives.fetchInnerClusters(transaction, innerNeighborhood, storageTransform)
                    .thenCompose(innerClusters -> primitives.cleanUpVectorReferences(transaction, innerClusters, true))
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
                        Transformed.underlyingLens(), primaryVectorReferences, targetNumPartitions, 8,
                        3, 0.00, BoundedKMeans.overflowQuadraticPenalty(),
                        true);

        return assignPrimaryVectorReferences(estimator, outerNeighborhood, primaryVectorReferences,
                kMeansResult, targetNumPartitions);
    }

    @Nonnull
    private AssignmentResult assignPrimaryVectorReferences(@Nonnull final Estimator estimator,
                                                           @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood,
                                                           @Nonnull final List<VectorReference> primaryVectorReferences,
                                                           @Nonnull final BoundedKMeans.Result<Transformed<RealVector>> kMeansResult,
                                                           final int targetNumPartitions) {
        final Config config = getConfig();

        final List<Transformed<RealVector>> clusterCentroids =
                kMeansResult.getClusterCentroids();
        Verify.verify(clusterCentroids.size() == targetNumPartitions);

        final ImmutableMap.Builder<UUID, ClusterMetadataWithDistance> clusterIdMetadataMapBuilder =
                ImmutableMap.builder();
        final ImmutableSet.Builder<UUID> newClusterIdsBuilder = ImmutableSet.builder();
        for (int i = 0; i < targetNumPartitions; i++) {
            final UUID newClusterId = RandomHelpers.randomUuid(config.isDeterministicRandomness());
            newClusterIdsBuilder.add(newClusterId);

            clusterIdMetadataMapBuilder.put(newClusterId,
                    new ClusterMetadataWithDistance(
                            new ClusterMetadata(newClusterId,
                                    0, 0,
                                    RunningStandardDeviation.identity(),
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

        //
        // Initialize a map we need to use to keep track for the correct most up-to-date count, mean, and
        // standard deviations for distances.
        //
        final Map<UUID, RunningStandardDeviation> updatedStandardDeviationMap = Maps.newHashMap();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            updatedStandardDeviationMap.put(entry.getKey(),
                    entry.getValue().getClusterMetadata().getRunningStandardDeviation());
        }

        final ImmutableListMultimap.Builder<UUID, ClusterMetadataWithDistance> invertedAssignmentsMapBuilder =
                ImmutableListMultimap.builder();
        // only considering primary copies here -- this will prune the replicated vectors
        for (final VectorReference vectorReference : primaryVectorReferences) {
            final TopK<ClusterMetadataWithDistance> nearestClusters =
                    new TopK<>(Comparator.comparing(ClusterMetadataWithDistance::getDistance).reversed(), 24);
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

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignmentMultimapBuilder =
                ImmutableListMultimap.builder();
        final Map<UUID, TopK<VectorReference>> replicatedAssignmentSamplerMap = Maps.newHashMap();

        RunningStandardDeviation replicationPriorityStandardDeviation = RunningStandardDeviation.identity();
        int numReplicated = 0;
        int numNonReplicated = 0;
        int numPrioritySplit = 0;
        // only considering primary copies here -- this will prune the replicated vectors
        for (final VectorReference vectorReference : primaryVectorReferences) {
            final var nearestClusters =
                    Objects.requireNonNull(invertedAssignmentsMap.get(vectorReference.getId().getUuid()));
            Verify.verify(!nearestClusters.isEmpty());
            final ClusterMetadataWithDistance primaryCluster = Objects.requireNonNull(nearestClusters.get(0));
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
            assignmentMultimapBuilder.put(primaryClusterId, vectorReference.toPrimaryCopy());

            final ImmutableList<ClusterMetadataWithDistance> replicatedClusters =
                    nearestClusters.subList(1, nearestClusters.size());
            for (final ClusterMetadataWithDistance replicatedClusterMetadataWithDistance : replicatedClusters) {
                final double distance = replicatedClusterMetadataWithDistance.getDistance();
                Verify.verify(Double.isFinite(distance));

                final ClusterMetadata replicatedClusterMetadata =
                        replicatedClusterMetadataWithDistance.getClusterMetadata();

                final RunningStandardDeviation updatedStandardDeviation =
                        Objects.requireNonNull(
                                updatedStandardDeviationMap.get(replicatedClusterMetadata.getId()));

                final double replicationPriorityOld =
                        StorageAdapter.replicationPriorityOld(distance, distanceToPrimaryCentroid,
                                Math.toIntExact(updatedStandardDeviation.getNumElements()),
                                updatedStandardDeviation.mean(),
                                updatedStandardDeviation.populationStandardDeviation());

                final double replicationPriority =
                        StorageAdapter.replicationPriority(distance, distanceToPrimaryCentroid,
                                Math.toIntExact(updatedStandardDeviation.getNumElements()),
                                updatedStandardDeviation.mean(),
                                updatedStandardDeviation.populationStandardDeviation());
                replicationPriorityStandardDeviation = replicationPriorityStandardDeviation.add(replicationPriority);

                if (replicationPriorityOld < config.getReplicationPriorityMin() &&
                        replicationPriority > config.getReplicationPriorityMin()) {
                    numPrioritySplit ++;
                }

                if (replicationPriority >= config.getReplicationPriorityMin()) {
                    final VectorReference newVectorReference =
                            vectorReference.toReplicatedCopy(replicationPriority);
                    if (newClusterIds.contains(replicatedClusterMetadata.getId())) {
                        final var reservoirSampler =
                                replicatedAssignmentSamplerMap.computeIfAbsent(replicatedClusterMetadata.getId(),
                                        ignored -> new TopK<>(Comparator.comparing(VectorReference::getReplicationPriority),
                                                config.getReplicatedClusterTarget()));
                        reservoirSampler.add(newVectorReference);
                    } else {
                        assignmentMultimapBuilder.put(replicatedClusterMetadata.getId(), newVectorReference);
                    }
                    numReplicated++;
                } else {
                    numNonReplicated++;
                }
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info("replication priority num={}. mean={}, standard deviation={}, numReplicated={}, numNonReplicated={}, numPrioritySplit={}",
                    replicationPriorityStandardDeviation.getNumElements(),
                    replicationPriorityStandardDeviation.mean(),
                    replicationPriorityStandardDeviation.populationStandardDeviation(),
                    numReplicated, numNonReplicated, numPrioritySplit);
        }

        for (final Map.Entry<UUID, TopK<VectorReference>> entry : replicatedAssignmentSamplerMap.entrySet()) {
            assignmentMultimapBuilder.putAll(entry.getKey(), entry.getValue().toUnsortedList());
        }

        return new AssignmentResult(newClusterIds, clusterIdMetadataMap,
                assignmentMultimapBuilder.build(), updatedStandardDeviationMap);
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
        final Config config = getConfig();
        final Primitives primitives = primitives();

        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                assignmentResult.getClusterIdMetadataMap();

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

        final Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap =
                assignmentResult.getUpdatedStandardDeviationsMap();

        // update all affected cluster metadata
        final ImmutableSet.Builder<UUID> newDependentTaskIdsBuilder = ImmutableSet.builder();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.getClusterMetadata();
            final int numPrimaryVectorsAdded = clusterIdToNumPrimaryVectorsAdded.getOrDefault(toBeWritten, 0);
            final int numPrimaryUnderreplicatedVectorsAdded = clusterIdToNumPrimaryUnderreplicatedVectorsAdded.getOrDefault(toBeWritten, 0);
            final int numReplicatedVectorsAdded = clusterIdToNumReplicatedVectorsAdded.getOrDefault(toBeWritten, 0);
            final RunningStandardDeviation updatedStandardDeviation =
                    Objects.requireNonNull(updatedStandardDeviationsMap.get(toBeWritten));

            primitives.writeDeferredTaskMaybe(transaction, random, clusterMetadata,
                            clusterMetadataWithDistance.getCentroid(), getAccessInfo(),
                            numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
                            updatedStandardDeviation, newClusterIds)
                    .ifPresent(newDependentTaskIdsBuilder::add);
            if (logger.isInfoEnabled()) {
                logger.info("pushing vectors during split; isNewCluster={}; clusterId={}; numTotalPrimaryVectors={}, numPrimaryVectorsAdded={}, " +
                                "numTotalPrimaryUnderreplicatedVectors={}, numPrimaryUnderreplicatedVectorsAdded={}, " +
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
                            randomNormalPriorityTaskId(random, config.isDeterministicRandomness()), newClusterIds,
                            newDependentTaskIds);
            primitives.writeDeferredTask(transaction, newBounceReassignTask);

            if (logger.isInfoEnabled()) {
                logger.info("enqueuing BOUNCE_REASSIGN; taskId={}; targetClusterIds={}; newDependentTaskIds={}",
                        taskIdToString(newBounceReassignTask.getTaskId()),
                        newBounceReassignTask.getTargetClusterIds(),
                        newBounceReassignTask.getDependentTaskIds());
            }
        }
    }

    @Nonnull
    private SplitMergeTask withHighPriorityAndNeighborhood(@Nonnull final SplittableRandom random,
                                                           final boolean deterministicRandomness,
                                                           @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        return SplitMergeTask.of(getLocator(), getAccessInfo(),
                randomHighPriorityTaskId(random, deterministicRandomness), getTargetClusterId(),
                getCentroid(), neighborhood);
    }

    @Nonnull
    static SplitMergeTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                     @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.SPLIT_MERGE);
        final StorageTransform storageTransform = locator.primitives().storageTransform(accessInfo);
        final Transformed<RealVector> centroid = storageTransform.transform(
                StorageHelpers.vectorFromBytes(locator.getConfig(), valueTuple.getBytes(2)));
        final ImmutableList.Builder<ClusterIdAndCentroid> neighborhoodsBuilder = ImmutableList.builder();
        final Tuple neighborhoodsTuple = valueTuple.getNestedTuple(3);
        for (int i = 0; i < neighborhoodsTuple.size(); i ++) {
            final Tuple clusterMetadataWithDistanceTuple = neighborhoodsTuple.getNestedTuple(i);
            neighborhoodsBuilder.add(StorageAdapter.clusterIdAndCentroidFromTuple(locator.getConfig(),
                    storageTransform, clusterMetadataWithDistanceTuple));
        }

        return new SplitMergeTask(locator, accessInfo,
                keyTuple.getUUID(0), valueTuple.getUUID(1), centroid, neighborhoodsBuilder.build());
    }

    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid) {
        return of(locator, accessInfo, taskId, clusterId, centroid, ImmutableList.of());
    }

    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid,
                             @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        return new SplitMergeTask(locator, accessInfo, taskId, clusterId, centroid, neighborhood);
    }

    @Nonnull
    private static List<ClusterMetadataWithDistance> maxInner(@Nullable final NeighborhoodsResult neighborhoods1,
                                                              @Nullable final NeighborhoodsResult neighborhoods2) {
        if (neighborhoods1 == null) {
            return Objects.requireNonNull(neighborhoods2).getInnerNeighborhood();
        }
        if (neighborhoods2 == null) {
            return Objects.requireNonNull(neighborhoods1).getInnerNeighborhood();
        }
        final List<ClusterMetadataWithDistance> inner1 = neighborhoods1.getInnerNeighborhood();
        final List<ClusterMetadataWithDistance> inner2 = neighborhoods2.getInnerNeighborhood();

        if (inner1.equals(inner2)) {
            return inner1;
        }
        if (inner1.size() > inner2.size()) {
            Verify.verify(inner1.subList(0, inner2.size()).equals(inner2));
            return inner1;
        }

        Verify.verify(inner2.subList(0, inner1.size()).equals(inner1));
        return inner2;
    }

    @Nonnull
    @SuppressWarnings("checkstyle:MethodName")
    private static AssignmentCandidate kMeans(@Nonnull final NeighborhoodsResult neighborhoods,
                                              @Nonnull final List<VectorReference> vectorReferences,
                                              @Nonnull final SplittableRandom random,
                                              @Nonnull final Estimator estimator) {
        final ImmutableList.Builder<VectorReference> primaryVectorReferencesBuilder = ImmutableList.builder();
        for (final VectorReference vectorReference : vectorReferences) {
            if (vectorReference.isPrimaryCopy()) {
                primaryVectorReferencesBuilder.add(vectorReference);
            }
        }
        final ImmutableList<VectorReference> primaryVectorReferences =
                primaryVectorReferencesBuilder.build();

        // re-fit only the primary vectors
        return new AssignmentCandidate(neighborhoods, primaryVectorReferences,
                BoundedKMeans.fit(random, estimator, VectorReference.vectorLens(),
                        Transformed.underlyingLens(), primaryVectorReferences,
                        neighborhoods.getInnerNeighborhood().size() + 1, 8,
                        3, 0.00, BoundedKMeans.overflowQuadraticPenalty(),
                        true));
    }

    @Nonnull
    private static UpgradeResult
            evaluateNewPartition(@Nonnull final Estimator estimator,
                                 @Nonnull final List<Cluster> currentClusters,
                                 @Nonnull final AssignmentCandidate assignmentCandidate) {
        int vectorCount = 0;
        final ImmutableList.Builder<Transformed<RealVector>> clusterCentroidsBuilder =
                ImmutableList.builder();
        for (final Cluster innerCluster : currentClusters) {
            for (final VectorReference vectorReference : innerCluster.getVectorReferences()) {
                if (vectorReference.isPrimaryCopy()) {
                    vectorCount++;
                }
            }
            clusterCentroidsBuilder.add(innerCluster.getCentroid());
        }
        final int[] assignment = new int[vectorCount];
        final ImmutableList.Builder<VectorReference> primaryVectorReferencesBuilder =
                ImmutableList.builder();
        for (int c = 0, currentIndex = 0; c < currentClusters.size(); c++) {
            final Cluster innerCluster = currentClusters.get(c);
            for (final VectorReference vectorReference : innerCluster.getVectorReferences()) {
                if (vectorReference.isPrimaryCopy()) {
                    primaryVectorReferencesBuilder.add(vectorReference);
                    assignment[currentIndex++] = c;
                }
            }
        }

        final SplitMergeEvaluator.Partition<Transformed<RealVector>> currentPartition =
                new SplitMergeEvaluator.Partition<>(clusterCentroidsBuilder.build(),
                        Transformed.underlyingLens(), assignment);
        final var kMeansResult = assignmentCandidate.getkMeansResult();
        return SplitMergeEvaluator.evaluateUpgrade(primaryVectorReferencesBuilder.build(),
                currentPartition,
                assignmentCandidate.getPrimaryVectorReferences(),
                new SplitMergeEvaluator.Partition<>(kMeansResult.getClusterCentroids(),
                        Transformed.underlyingLens(), kMeansResult.getAssignment()),
                VectorReference.vectorLens(),
                new SplitMergeEvaluator.Parameters(estimator));
    }

    private static class AssignmentCandidate {
        @Nonnull
        private final NeighborhoodsResult neighborhoodsResult;
        @Nonnull
        private final List<VectorReference> primaryVectorReferences;
        @Nonnull
        @SuppressWarnings("checkstyle:MemberName")
        private final BoundedKMeans.Result<Transformed<RealVector>> kMeansResult;

        public AssignmentCandidate(@Nonnull final NeighborhoodsResult neighborhoodsResult,
                                   @Nonnull final List<VectorReference> primaryVectorReferences,
                                   @Nonnull final BoundedKMeans.Result<Transformed<RealVector>> kMeansResult) {
            this.neighborhoodsResult = neighborhoodsResult;
            this.primaryVectorReferences = primaryVectorReferences;
            this.kMeansResult = kMeansResult;
        }

        @Nonnull
        public NeighborhoodsResult getNeighborhoods() {
            return neighborhoodsResult;
        }

        @Nonnull
        public List<VectorReference> getPrimaryVectorReferences() {
            return primaryVectorReferences;
        }

        @Nonnull
        public BoundedKMeans.Result<Transformed<RealVector>> getkMeansResult() {
            return kMeansResult;
        }
    }

    private static class AssignmentResult {
        @Nonnull
        private final Set<UUID> newClusterIds;
        @Nonnull
        private final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap;
        @Nonnull
        private final ListMultimap<UUID, VectorReference> assignmentMultimap;
        @Nonnull
        private final Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap;

        public AssignmentResult(@Nonnull final Set<UUID> newClusterIds,
                                @Nonnull final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                @Nonnull final ListMultimap<UUID, VectorReference> assignmentMultimap,
                                @Nonnull final Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap) {
            this.newClusterIds = newClusterIds;
            this.clusterIdMetadataMap = clusterIdMetadataMap;
            this.assignmentMultimap = assignmentMultimap;
            this.updatedStandardDeviationsMap = updatedStandardDeviationsMap;
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

        @Nonnull
        public Map<UUID, RunningStandardDeviation> getUpdatedStandardDeviationsMap() {
            return updatedStandardDeviationsMap;
        }
    }
}
