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
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A deferred task that maintains cluster size invariants by splitting oversized clusters or merging
 * undersized ones. When a cluster's primary vector count exceeds {@link Config#primaryClusterMax()},
 * the cluster is partitioned into multiple new clusters using k-means. When the count falls below
 * {@link Config#primaryClusterMin()}, neighboring clusters are merged to reduce the total cluster count.
 *
 * <p>
 * The task operates in two phases: first, it evaluates candidate repartitionings (potentially trying
 * both 1-to-2 and 2-to-3 splits) and selects the best one using {@link SplitMergeEvaluator}. Second,
 * it executes the chosen repartitioning by reassigning vectors, updating the HNSW centroid index,
 * and propagating replication information. If no valid repartitioning exists (e.g., due to duplicate
 * vectors), the task may fall back to enqueuing a {@link CollapseTask} instead.
 * </p>
 */
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

    @Override
    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        super.writeDeferredTask(transaction);
        if (logger.isInfoEnabled()) {
            logger.info("enqueuing SPLIT_MERGE; taskId={}; clusterId={}",
                    AbstractDeferredTask.taskIdToString(getTaskId()), getTargetClusterId());
        }
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

    /**
     * Serializes this task into a {@link Tuple} for persistent storage in the deferred task queue.
     * Encodes the centroid and the precomputed neighborhood so the task can be resumed without
     * re-fetching from the HNSW index.
     */
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

    /**
     * Executes the split-or-merge decision for the target cluster. Fetches the cluster's current metadata
     * and checks whether it still requires intervention (the state may have resolved concurrently). If the
     * cluster size is within bounds, the {@link ClusterMetadata.State#SPLIT_MERGE} flag is cleared as a
     * false alarm. Otherwise, delegates to {@link #split} or {@link #merge} based on the direction of the
     * size violation.
     *
     * @param transaction the FDB transaction to operate within
     * @return a future that completes when the task has finished
     */
    @Nonnull
    @Override
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        logStart(logger);

        final Config config = getConfig();
        final Primitives primitives = primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final RealVector untransformedCentroid = storageTransform.untransform(getCentroid());

        return primitives.fetchClusterMetadata(transaction, getTargetClusterId())
                .thenCompose(clusterMetadata -> {
                    if (clusterMetadata == null ||
                            !clusterMetadata.states().contains(ClusterMetadata.State.SPLIT_MERGE) ||
                            clusterMetadata.states().contains(ClusterMetadata.State.COLLAPSE)) {
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumPrimaryVectors() >= config.primaryClusterMin() &&
                            clusterMetadata.getNumPrimaryVectors() <= config.primaryClusterMax()) {
                        // false alarm
                        final EnumSet<ClusterMetadata.State> newStates = EnumSet.copyOf(clusterMetadata.states());
                        newStates.remove(ClusterMetadata.State.SPLIT_MERGE);
                        primitives.writeClusterMetadata(transaction, clusterMetadata.withNewStates(newStates));
                        clusterMetadata.withNewStates(newStates);
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumPrimaryVectors() > config.primaryClusterMax()) {
                        return split(transaction, clusterMetadata, untransformedCentroid);
                    } else {
                        Verify.verify(clusterMetadata.getNumPrimaryVectors() < config.primaryClusterMin());
                        return merge(transaction, clusterMetadata, untransformedCentroid);
                    }
                }).thenAccept(ignored -> logSuccessful(logger));
    }

    /**
     * Splits an oversized cluster into multiple smaller clusters. The algorithm proceeds as follows:
     * <ol>
     *   <li>If the neighborhood is not precomputed, fetches it from the HNSW centroid index and
     *       re-enqueues a high-priority task with the neighborhood attached (early return).</li>
     *   <li>Evaluates two candidate repartitionings: a 1-to-2 split (splitting the target cluster into 2)
     *       and, if the neighborhood is large enough, a 2-to-3 split (splitting the target and its nearest
     *       neighbor into 3).</li>
     *   <li>Runs bounded k-means on the primary vectors to compute new cluster centroids for each candidate.</li>
     *   <li>Selects the best valid candidate via {@link SplitMergeEvaluator}, or falls back to a collapse
     *       task if no valid split exists.</li>
     *   <li>Assigns vectors to new clusters, updates the HNSW centroid index, and writes the new
     *       cluster metadata.</li>
     * </ol>
     *
     * @param transaction the FDB transaction
     * @param targetClusterMetadata metadata of the cluster being split
     * @param targetClusterCentroid untransformed centroid of the target cluster
     * @return a future that completes when the split is done
     */
    @Nonnull
    private CompletableFuture<Void> split(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata targetClusterMetadata,
                                          @Nonnull final RealVector targetClusterCentroid) {
        final Config config = getConfig();
        final SplittableRandom random = RandomHelpers.random(getTaskId());
        final Primitives primitives = primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numNeighborhood = 32;

        final List<ClusterIdAndCentroid> neighborhood = getNeighborhood();
        if (neighborhood.isEmpty()) {
            // if the neighborhoods are not set yet, fetch the neighborhood and write an urgent new task
            return primitives.fetchNeighborhoodClusterMetadata(transaction, targetClusterMetadata,
                    targetClusterCentroid, storageTransform, numNeighborhood)
                    .thenAccept(fetchedNeighborhood -> {
                        final SplitMergeTask splitMergeTask =
                                withHighPriorityAndNeighborhood(random,
                                        ClusterIdAndCentroid.fromClusterMetadataAndDistances(fetchedNeighborhood));
                        splitMergeTask.writeDeferredTask(transaction);
                        if (logger.isTraceEnabled()) {
                            logger.info("enqueued high priority SPLIT_MERGE due to refetch of the neighborhood; taskId={}; neighborhoodSize={}",
                                    AbstractDeferredTask.taskIdToString(splitMergeTask.getTaskId()),
                                    splitMergeTask.getNeighborhood().size());
                        }
                    });
        } else {
            if (logger.isTraceEnabled()) {
                logger.info("using precomputed neighborhood; taskId={}; neighborhoodSize={}",
                        taskIdToString(getTaskId()), getNeighborhood().size());
            }
        }

        return MoreAsyncUtil.forEach(neighborhood,
                        clusterIdAndCentroid -> primitives.fetchClusterMetadataWithDistance(transaction,
                                clusterIdAndCentroid.clusterId(), clusterIdAndCentroid.centroid(), 0.0d),
                        10, executor)
                .thenCompose(neighborhoodClusterMetadataWithDistances -> {
                    // Compute two candidate split configurations:
                    // 1-to-2: split the target into 2 clusters (1 inner + rest outer)
                    // 2-to-3: split the target and its nearest neighbor into 3 (2 inner + rest outer)
                    final Neighborhoods neighborhoods1To2 =
                            neighborhoods(neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(),
                                    1, numNeighborhood - 1);
                    final Neighborhoods neighborhoods2To3 =
                            neighborhood.size() < 2
                            ? null
                            : neighborhoods(neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(),
                                    2, numNeighborhood - 2);

                    final List<Neighborhoods> allNeighborhoods =
                            Lists.newArrayList(neighborhoods1To2, neighborhoods2To3);

                    return primitives.fetchInnerClusters(transaction,
                                    largestInnerNeighborhood(neighborhoods1To2, neighborhoods2To3), storageTransform)
                            .thenCompose(innerClusters -> RandomHelpers.forEach(random, allNeighborhoods,
                                            (neighborhoods, nestedRandom) -> {
                                                if (neighborhoods == null) {
                                                    return CompletableFuture.completedFuture(null);
                                                }
                                                final var innerNeighborhood = neighborhoods.innerNeighborhood();
                                                final var clampedInnerClusters =
                                                        innerNeighborhood.size() == innerClusters.size()
                                                        ? innerClusters
                                                        : innerClusters.subList(0, innerNeighborhood.size());

                                                return primitives.cleanUpVectorReferences(transaction, clampedInnerClusters, true)
                                                        .thenApply(vectorReferences ->
                                                                kMeans(neighborhoods, vectorReferences, nestedRandom, estimator));
                                            }, 10, executor)
                                    .<RepartitioningCandidate>thenApply(assignmentCandidates -> {
                                        final Map<RepartitioningCandidate, UpgradeResult> candidateToUpgradeResultMap = Maps.newIdentityHashMap();
                                        final RepartitioningCandidate split1to2Candidate =
                                                Objects.requireNonNull(assignmentCandidates.get(0));
                                        final UpgradeResult upgradeResult1to2 =
                                                scoreCandidate(estimator, ImmutableList.of(innerClusters.get(0)),
                                                        split1to2Candidate);
                                        candidateToUpgradeResultMap.put(split1to2Candidate, upgradeResult1to2);
                                        final RepartitioningCandidate split2to3Candidate = assignmentCandidates.get(1);
                                        if (split2to3Candidate != null) {
                                            Verify.verify(innerClusters.size() > 1);
                                            final UpgradeResult upgradeResult2to3 =
                                                    scoreCandidate(estimator, innerClusters,
                                                            split2to3Candidate);
                                            candidateToUpgradeResultMap.put(split2to3Candidate, upgradeResult2to3);
                                        }
                                        final Optional<RepartitioningCandidate> bestValidCandidateOptional =
                                                selectBestCandidateMaybe(candidateToUpgradeResultMap);
                                        if (bestValidCandidateOptional.isEmpty()) {
                                            if (enqueueCollapseIfNecessary(transaction, random,
                                                    split1to2Candidate.primaryVectorReferences(),
                                                    getTargetClusterId(), centroid)) {
                                                primitives.writeClusterMetadata(transaction,
                                                        targetClusterMetadata.withNewStates(EnumSet.of(ClusterMetadata.State.COLLAPSE)));
                                                return null;
                                            }
                                        }
                                        return bestValidCandidateOptional.orElseThrow();
                                    }))
                            .thenCompose(repartitioningCandidate -> {
                                if (repartitioningCandidate == null) {
                                    return AsyncUtil.DONE;
                                }

                                final Neighborhoods neighborhoods = repartitioningCandidate.neighborhoods();
                                final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.innerNeighborhood();
                                final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.outerNeighborhood();

                                final Repartitioning repartitioning =
                                        assignPrimaryVectorReferences(estimator, outerNeighborhood,
                                                repartitioningCandidate.primaryVectorReferences(),
                                                repartitioningCandidate.kMeansResult(),
                                                innerNeighborhood.size() + 1);
                                return replaceCentroidsInHnsw(transaction, storageTransform, innerNeighborhood, repartitioning)
                                        .thenAccept(ignored ->
                                                persistRepartitioning(transaction, random, innerNeighborhood, repartitioning, quantizer));
                            });
                });
    }

    /**
     * Merges an undersized cluster with its nearest neighbors. Fetches the inner neighborhood
     * (clusters to be dissolved) and outer neighborhood (clusters that may absorb overflow vectors),
     * then runs k-means to repartition the combined vector set into {@code innerNeighborhood.size() - 1}
     * clusters — effectively reducing the total cluster count by one.
     *
     * @param transaction the FDB transaction
     * @param targetClusterMetadata metadata of the undersized cluster
     * @param targetClusterCentroid untransformed centroid of the target cluster
     * @return a future that completes when the merge is done
     */
    @Nonnull
    private CompletableFuture<Void> merge(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata targetClusterMetadata,
                                          @Nonnull final RealVector targetClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(getTaskId());
        final Primitives primitives = primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = 3;
        final int numOuterNeighborhood = 8;

        final CompletableFuture<Neighborhoods> neighborhoodsFuture =
                primitives.fetchNeighborhoodClusterMetadata(transaction, targetClusterMetadata, targetClusterCentroid,
                        storageTransform, numInnerNeighborhood + numOuterNeighborhood)
                        .thenApply(clusterMetadataWithDistances ->
                                neighborhoods(clusterMetadataWithDistances, targetClusterMetadata,
                                        getCentroid(), numInnerNeighborhood, numOuterNeighborhood));

        return neighborhoodsFuture.thenCompose(neighborhoods -> {
            final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.innerNeighborhood();
            final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.outerNeighborhood();

            //
            // At this point innerNeighborhood contains the clusters we want to split into
            // innerNeighborhood.size() - 1 number of clusters and outerNeighborhood contains all clusters we
            // may assign some vectors from innerNeighborhood to.
            //
            return primitives.fetchInnerClusters(transaction, innerNeighborhood, storageTransform)
                    .thenCompose(innerClusters ->
                            primitives.cleanUpVectorReferences(transaction, innerClusters,
                                    true))
                    .thenApply(cleanedUpVectorReferences ->
                            repartitionVectors(random, estimator, outerNeighborhood, cleanedUpVectorReferences,
                                    innerNeighborhood.size() - 1))
                    .thenCompose(repartitioning ->
                            replaceCentroidsInHnsw(transaction, storageTransform, innerNeighborhood, repartitioning))
                    .thenAccept(repartitioning ->
                            persistRepartitioning(transaction, random, innerNeighborhood, repartitioning, quantizer));
        });
    }

    /**
     * Runs k-means on the combined primary vectors from the inner neighborhood clusters, then produces
     * an {@link Repartitioning} that maps each vector to its new target cluster. Used by the merge path
     * where all vectors need to be repartitioned from scratch.
     *
     * @param random source of randomness for k-means initialization
     * @param estimator distance estimator for the vector space
     * @param outerNeighborhood clusters that may receive overflow assignments
     * @param vectorReferences all vector references (primary and replicated) from the inner clusters
     * @param targetNumPartitions desired number of output clusters
     * @return the computed assignment of vectors to clusters
     */
    @Nonnull
    private Repartitioning repartitionVectors(@Nonnull final SplittableRandom random,
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

    /**
     * Assigns each primary vector reference to its nearest cluster (from new + outer clusters) and
     * determines replication targets based on distance-based priority scoring. Vectors whose primary
     * assignment falls outside the new clusters are marked as underreplicated. Replicated copies are
     * subject to occlusion filtering and bounded by {@link Config#replicatedClusterTarget()} via
     * reservoir sampling.
     *
     * @param estimator distance estimator for the vector space
     * @param outerNeighborhood clusters outside the split/merge region that may receive vectors
     * @param primaryVectorReferences the primary vector references to assign
     * @param kMeansResult the k-means clustering result providing centroids and initial assignments
     * @param targetNumPartitions number of new clusters being created
     * @return an assignment result containing the vector-to-cluster mapping and updated statistics
     */
    @Nonnull
    private Repartitioning assignPrimaryVectorReferences(@Nonnull final Estimator estimator,
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
            final UUID newClusterId = RandomHelpers.randomUuid(config.deterministicRandomness());
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
            clusterIdMetadataMapBuilder.put(clusterMetadata.clusterMetadata().id(), clusterMetadata);
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
        final Map<UUID, RunningStandardDeviation> standardDeviationsMap = Maps.newHashMap();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            standardDeviationsMap.put(entry.getKey(),
                    entry.getValue().clusterMetadata().runningStandardDeviation());
        }

        final NearestClustersResult nearestClustersResult =
                computeNearestClusters(estimator, primaryVectorReferences, clusterIdMetadataMap);
        mergeStandardDeviationUpdates(standardDeviationsMap,
                nearestClustersResult.standardDeviationUpdates());
        final ImmutableListMultimap<UUID, ClusterMetadataWithDistance> invertedAssignmentsMap =
                nearestClustersResult.invertedAssignments();

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignmentMultimapBuilder =
                ImmutableListMultimap.builder();
        final Map<UUID, TopK<VectorReference>> replicatedAssignmentSamplerMap = Maps.newHashMap();

        RunningStandardDeviation replicationPriorityStandardDeviation = RunningStandardDeviation.identity();
        int numReplicated = 0;
        int numOccluded = 0;
        // only considering primary copies here -- this will prune the replicated vectors
        for (final VectorReference vectorReference : primaryVectorReferences) {
            final var nearestClusters =
                    Objects.requireNonNull(invertedAssignmentsMap.get(vectorReference.getId().getUuid()));
            Verify.verify(!nearestClusters.isEmpty());
            final ClusterMetadataWithDistance primaryCluster = Objects.requireNonNull(nearestClusters.get(0));
            final double distanceToPrimaryCentroid = primaryCluster.distance();
            Verify.verify(Double.isFinite(distanceToPrimaryCentroid));

            final UUID primaryClusterId = primaryCluster.clusterMetadata().id();
            if (!newClusterIds.contains(primaryClusterId)) {
                // Vector's nearest cluster is in the outer neighborhood — it migrated away from
                // the split region. Mark as underreplicated so a future reassign task can fix replication.
                assignmentMultimapBuilder.put(
                        primaryClusterId,
                        vectorReference.toPrimaryUnderreplicatedCopy());
                // skip assigning replicas as the new clusters do not "own" this vector anymore
                continue;
            }

            // add primary to one of the new clusters and in the remainder populate the replicated clusters
            assignmentMultimapBuilder.put(primaryClusterId, vectorReference.toPrimaryCopy());

            final ImmutableList<ClusterMetadataWithDistance> replicationCandidates =
                    nearestClusters.subList(1, nearestClusters.size());
            final List<ClusterMetadataWithDistance> selectedReplicationClusters =
                    Lists.newArrayListWithExpectedSize(replicationCandidates.size());

            for (final ClusterMetadataWithDistance replicationCandidate : replicationCandidates) {
                final double distance = replicationCandidate.distance();
                Verify.verify(Double.isFinite(distance));

                final ClusterMetadata replicationCandidateClusterMetadata = replicationCandidate.clusterMetadata();

                final RunningStandardDeviation updatedStandardDeviation =
                        Objects.requireNonNull(
                                standardDeviationsMap.get(replicationCandidateClusterMetadata.id()));

                final double replicationPriority =
                        StorageAdapter.replicationPriority(distance, distanceToPrimaryCentroid,
                                Math.toIntExact(updatedStandardDeviation.getNumElements()),
                                updatedStandardDeviation.mean(),
                                updatedStandardDeviation.populationStandardDeviation());
                replicationPriorityStandardDeviation = replicationPriorityStandardDeviation.add(replicationPriority);

                if (replicationPriority >= config.replicationPriorityMin()) {
                    if (StorageAdapter.isOccluded(estimator, replicationCandidate, selectedReplicationClusters)) {
                        numOccluded ++;
                        continue;
                    }

                    final VectorReference newVectorReference =
                            vectorReference.toReplicatedCopy(replicationPriority);
                    if (newClusterIds.contains(replicationCandidateClusterMetadata.id())) {
                        final var reservoirSampler =
                                replicatedAssignmentSamplerMap.computeIfAbsent(replicationCandidateClusterMetadata.id(),
                                        ignored -> TopK.max(Comparator.comparing(VectorReference::getReplicationPriority),
                                                config.replicatedClusterTarget()));
                        reservoirSampler.add(newVectorReference);
                    } else {
                        assignmentMultimapBuilder.put(replicationCandidateClusterMetadata.id(), newVectorReference);
                    }
                    selectedReplicationClusters.add(replicationCandidate);
                    numReplicated++;
                }
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("replication priority num={}. mean={}, standard deviation={}, numReplicated={}, numOccluded={}",
                    replicationPriorityStandardDeviation.getNumElements(),
                    replicationPriorityStandardDeviation.mean(),
                    replicationPriorityStandardDeviation.populationStandardDeviation(),
                    numReplicated, numOccluded);
        }

        for (final Map.Entry<UUID, TopK<VectorReference>> entry : replicatedAssignmentSamplerMap.entrySet()) {
            assignmentMultimapBuilder.putAll(entry.getKey(), entry.getValue().toUnsortedList());
        }

        return new Repartitioning(newClusterIds, clusterIdMetadataMap,
                assignmentMultimapBuilder.build(), standardDeviationsMap);
    }

    /**
     * Updates the HNSW centroid index by removing the old inner neighborhood clusters and inserting
     * the newly created cluster centroids. Deletions and insertions are performed sequentially
     * (delete-then-insert) to avoid conflicts within the HNSW graph.
     *
     * @param transaction the FDB transaction
     * @param storageTransform transform used to untransform centroids for HNSW insertion
     * @param innerNeighborhood the clusters being removed from the index
     * @param repartitioning the result containing the new cluster IDs and their centroids
     * @return a future completing with the assignment result (passed through for chaining)
     */
    @Nonnull
    private CompletableFuture<Repartitioning> replaceCentroidsInHnsw(@Nonnull final Transaction transaction,
                                                                     @Nonnull final StorageTransform storageTransform,
                                                                     @Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood,
                                                                     @Nonnull final Repartitioning repartitioning) {
        final Primitives primitives = primitives();
        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap = repartitioning.clusterIdMetadataMap();
        final Set<UUID> newClusterIds = repartitioning.newClusterIds();

        // delete first
        final CompletableFuture<List<Void>> deletedCentroidsFuture =
                MoreAsyncUtil.forEach(innerNeighborhood,
                        clusterMetadataWithDistance -> {
                            final UUID toBeDeletedClusterId = clusterMetadataWithDistance.clusterMetadata().id();
                            return centroidsHnsw.delete(transaction,
                                            StorageAdapter.tupleFromClusterId(toBeDeletedClusterId))
                                    .thenAccept(ignored -> {
                                        if (logger.isTraceEnabled()) {
                                            logger.trace("cluster deleted, clusterId={}", toBeDeletedClusterId);
                                        }
                                    });
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
                                            storageTransform.untransform(clusterMetadataWithDistance.centroid());
                                    return centroidsHnsw.insert(transaction, StorageAdapter.tupleFromClusterId(newClusterId),
                                            centroidBVector, null)
                                            .thenAccept(ignored2 -> {
                                                if (logger.isTraceEnabled()) {
                                                    logger.trace("new cluster inserted; clusterId={}", newClusterId);
                                                }
                                            });
                                },
                                1,
                                getLocator().getExecutor()))
                .thenApply(ignored -> repartitioning);
    }

    /**
     * Persists the final vector assignments to storage. Deletes old cluster data for the inner
     * neighborhood, writes each vector reference to its assigned cluster, updates cluster metadata
     * with new vector counts and running standard deviations, and enqueues follow-up
     * {@link ReassignTask}s (via a {@link BounceTask}) for any new clusters that may themselves
     * need rebalancing.
     *
     * @param transaction the FDB transaction
     * @param random source of randomness for task ID generation
     * @param innerNeighborhood the clusters being dissolved
     * @param repartitioning the computed vector-to-cluster mapping
     * @param quantizer quantizer used to encode vectors for storage
     */
    private void persistRepartitioning(@Nonnull final Transaction transaction,
                                       @Nonnull final SplittableRandom random,
                                       @Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood,
                                       @Nonnull final Repartitioning repartitioning,
                                       @Nonnull final Quantizer quantizer) {
        deleteDissolvedClusters(transaction, innerNeighborhood);
        final VectorWriteCounters counters = writeVectorReferences(transaction, quantizer, repartitioning);
        final Set<UUID> dependentTaskIds = writeClusterMetadataAndEnqueueTasks(transaction, random, repartitioning, counters);
        enqueueBounceIfNeeded(transaction, random, repartitioning.newClusterIds(), dependentTaskIds);
    }

    /**
     * Removes all vector references and metadata for the clusters being dissolved by this
     * split or merge operation.
     */
    private void deleteDissolvedClusters(@Nonnull final Transaction transaction,
                                         @Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood) {
        final Primitives primitives = primitives();
        for (final ClusterMetadataWithDistance clusterMetadata : innerNeighborhood) {
            final UUID toBeDeleted = clusterMetadata.clusterMetadata().id();
            primitives.deleteVectorReferencesForCluster(transaction, toBeDeleted);
            primitives.deleteClusterMetadata(transaction, toBeDeleted);
        }
    }

    /**
     * Writes all vector references from the repartitioning to their assigned clusters and returns
     * per-cluster counters of how many primary, underreplicated, and replicated vectors were added.
     *
     * @param transaction the FDB transaction
     * @param quantizer quantizer used to encode vectors for storage
     * @param repartitioning the computed vector-to-cluster assignments
     * @return counters broken down by cluster ID and vector type
     */
    @Nonnull
    private VectorWriteCounters writeVectorReferences(@Nonnull final Transaction transaction,
                                                      @Nonnull final Quantizer quantizer,
                                                      @Nonnull final Repartitioning repartitioning) {
        final Primitives primitives = primitives();
        final ListMultimap<UUID, VectorReference> assignmentMultiMap = repartitioning.assignmentMultimap();
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
        return new VectorWriteCounters(clusterIdToNumPrimaryVectorsAdded,
                clusterIdToNumPrimaryUnderreplicatedVectorsAdded, clusterIdToNumReplicatedVectorsAdded);
    }

    /**
     * Writes updated metadata for all affected clusters (both new and outer) and conditionally
     * enqueues follow-up deferred tasks for clusters that now violate size or replication invariants.
     *
     * @param transaction the FDB transaction
     * @param random source of randomness for task ID generation
     * @param repartitioning the repartitioning providing cluster metadata and statistics
     * @param counters per-cluster write counts from the preceding vector write phase
     * @return the set of task IDs for any newly enqueued dependent tasks
     */
    @Nonnull
    private Set<UUID> writeClusterMetadataAndEnqueueTasks(@Nonnull final Transaction transaction,
                                                          @Nonnull final SplittableRandom random,
                                                          @Nonnull final Repartitioning repartitioning,
                                                          @Nonnull final VectorWriteCounters counters) {
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap = repartitioning.clusterIdMetadataMap();
        final Set<UUID> newClusterIds = repartitioning.newClusterIds();
        final Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap =
                repartitioning.updatedStandardDeviationsMap();

        final ImmutableSet.Builder<UUID> newDependentTaskIdsBuilder = ImmutableSet.builder();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.clusterMetadata();
            final int numPrimaryVectorsAdded = counters.numPrimaryVectorsAdded().getOrDefault(toBeWritten, 0);
            final int numPrimaryUnderreplicatedVectorsAdded = counters.numPrimaryUnderreplicatedVectorsAdded().getOrDefault(toBeWritten, 0);
            final int numReplicatedVectorsAdded = counters.numReplicatedVectorsAdded().getOrDefault(toBeWritten, 0);
            final RunningStandardDeviation updatedStandardDeviation =
                    Objects.requireNonNull(updatedStandardDeviationsMap.get(toBeWritten));

            Verify.verify(clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded > 0);

            primitives().writeDeferredTaskMaybe(transaction, random, clusterMetadata,
                            clusterMetadataWithDistance.centroid(), getAccessInfo(),
                            numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
                            updatedStandardDeviation, newClusterIds)
                    .ifPresent(newDependentTaskIdsBuilder::add);
            if (logger.isTraceEnabled()) {
                logger.trace("pushing vectors during split; isNewCluster={}; clusterId={}; numTotalPrimaryVectors={}, numPrimaryVectorsAdded={}, " +
                                "numTotalPrimaryUnderreplicatedVectors={}, numPrimaryUnderreplicatedVectorsAdded={}, " +
                                "numTotalReplicatedVectors={}, numReplicatedVectorsAdded={}",
                        newClusterIds.contains(clusterMetadata.id()),
                        clusterMetadata.id(),
                        clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded, numPrimaryVectorsAdded,
                        clusterMetadata.numPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded, numPrimaryUnderreplicatedVectorsAdded,
                        clusterMetadata.numReplicatedVectors() + numReplicatedVectorsAdded, numReplicatedVectorsAdded);
            }
        }
        return newDependentTaskIdsBuilder.build();
    }

    /**
     * Enqueues a {@link BounceTask} that will trigger {@link ReassignTask}s for the new clusters once
     * all dependent tasks have completed. No-op if no dependent tasks were created.
     */
    private void enqueueBounceIfNeeded(@Nonnull final Transaction transaction,
                                       @Nonnull final SplittableRandom random,
                                       @Nonnull final Set<UUID> newClusterIds,
                                       @Nonnull final Set<UUID> dependentTaskIds) {
        if (!dependentTaskIds.isEmpty()) {
            final Config config = getConfig();
            final BounceTask newBounceTask =
                    BounceTask.of(getLocator(), getAccessInfo(),
                            randomNormalPriorityTaskId(random, config.deterministicRandomness()), newClusterIds,
                            dependentTaskIds, Kind.REASSIGN);
            newBounceTask.writeDeferredTask(transaction);
        }
    }

    /**
     * Creates a copy of this task with high priority and the given precomputed neighborhood, used
     * when the initial task did not have the neighborhood attached and needs to be re-enqueued
     * after fetching it.
     */
    @Nonnull
    private SplitMergeTask withHighPriorityAndNeighborhood(@Nonnull final SplittableRandom random,
                                                           @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        return SplitMergeTask.of(getLocator(), getAccessInfo(),
                randomHighPriorityTaskId(random, getConfig().deterministicRandomness()), getTargetClusterId(),
                getCentroid(), neighborhood);
    }

    /**
     * Deserializes a {@code SplitMergeTask} from its key and value tuple representation as stored in the
     * deferred task queue.
     *
     * @param locator the locator providing access to primitives and configuration
     * @param accessInfo access context for the current operation
     * @param keyTuple the key tuple containing the task ID
     * @param valueTuple the value tuple containing the serialized task data
     * @return the deserialized task
     */
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

    /**
     * Creates a new {@code SplitMergeTask} without a precomputed neighborhood. The neighborhood will
     * be fetched from the HNSW centroid index when the task executes.
     */
    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid) {
        return of(locator, accessInfo, taskId, clusterId, centroid, ImmutableList.of());
    }

    /**
     * Creates a new {@code SplitMergeTask} with a precomputed neighborhood, avoiding an additional
     * HNSW lookup at execution time.
     */
    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid,
                             @Nonnull final List<ClusterIdAndCentroid> neighborhood) {
        return new SplitMergeTask(locator, accessInfo, taskId, clusterId, centroid, neighborhood);
    }

    /**
     * Returns the larger of the two inner neighborhoods. Since the 2-to-3 neighborhood is a superset
     * of the 1-to-2 neighborhood, this is used to determine the maximum set of clusters whose vectors
     * need to be fetched (avoiding duplicate reads).
     */
    @Nonnull
    private static List<ClusterMetadataWithDistance> largestInnerNeighborhood(@Nullable final Neighborhoods neighborhoods1,
                                                                              @Nullable final Neighborhoods neighborhoods2) {
        if (neighborhoods1 == null) {
            return Objects.requireNonNull(neighborhoods2).innerNeighborhood();
        }
        if (neighborhoods2 == null) {
            return Objects.requireNonNull(neighborhoods1).innerNeighborhood();
        }
        final List<ClusterMetadataWithDistance> inner1 = neighborhoods1.innerNeighborhood();
        final List<ClusterMetadataWithDistance> inner2 = neighborhoods2.innerNeighborhood();

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

    /**
     * Runs bounded k-means on the primary vectors from the given vector references, partitioning them
     * into {@code innerNeighborhood.size() + 1} clusters. This is the split path's per-candidate
     * clustering step.
     *
     * @param neighborhoods the neighborhood context (determines target partition count)
     * @param vectorReferences all vector references (filtered to primary copies internally)
     * @param random source of randomness for k-means initialization
     * @param estimator distance estimator for the vector space
     * @return an assignment candidate wrapping the k-means result and primary vectors
     */
    @Nonnull
    @SuppressWarnings("checkstyle:MethodName")
    private static RepartitioningCandidate kMeans(@Nonnull final Neighborhoods neighborhoods,
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
        return new RepartitioningCandidate(neighborhoods, primaryVectorReferences,
                BoundedKMeans.fit(random, estimator, VectorReference.vectorLens(),
                        Transformed.underlyingLens(), primaryVectorReferences,
                        neighborhoods.innerNeighborhood().size() + 1, 8,
                        3, 0.00, BoundedKMeans.overflowQuadraticPenalty(),
                        true));
    }

    /**
     * Evaluates a candidate repartitioning against the current cluster layout. Compares the k-means
     * assignment quality (intra-cluster distances) to the existing partition to determine whether the
     * proposed split actually improves the index structure.
     *
     * @param estimator distance estimator for score computation
     * @param currentClusters the clusters as they exist before the split (used as baseline)
     * @param repartitioningCandidate the proposed new partition from k-means
     * @return the evaluation result containing the decision and score gain
     */
    @Nonnull
    private static UpgradeResult
            scoreCandidate(@Nonnull final Estimator estimator,
                           @Nonnull final List<Cluster> currentClusters,
                           @Nonnull final RepartitioningCandidate repartitioningCandidate) {
        int vectorCount = 0;
        final ImmutableList.Builder<Transformed<RealVector>> clusterCentroidsBuilder =
                ImmutableList.builder();
        for (final Cluster innerCluster : currentClusters) {
            for (final VectorReference vectorReference : innerCluster.vectorReferences()) {
                if (vectorReference.isPrimaryCopy()) {
                    vectorCount++;
                }
            }
            clusterCentroidsBuilder.add(innerCluster.centroid());
        }
        final int[] assignment = new int[vectorCount];
        final ImmutableList.Builder<VectorReference> primaryVectorReferencesBuilder =
                ImmutableList.builder();
        for (int c = 0, currentIndex = 0; c < currentClusters.size(); c++) {
            final Cluster innerCluster = currentClusters.get(c);
            for (final VectorReference vectorReference : innerCluster.vectorReferences()) {
                if (vectorReference.isPrimaryCopy()) {
                    primaryVectorReferencesBuilder.add(vectorReference);
                    assignment[currentIndex++] = c;
                }
            }
        }

        final SplitMergeEvaluator.Partition<Transformed<RealVector>> currentPartition =
                new SplitMergeEvaluator.Partition<>(clusterCentroidsBuilder.build(),
                        Transformed.underlyingLens(), assignment);
        final var kMeansResult = repartitioningCandidate.kMeansResult();
        return SplitMergeEvaluator.evaluateUpgrade(primaryVectorReferencesBuilder.build(),
                currentPartition,
                repartitioningCandidate.primaryVectorReferences(),
                new SplitMergeEvaluator.Partition<>(kMeansResult.getClusterCentroids(),
                        Transformed.underlyingLens(), kMeansResult.getAssignment()),
                VectorReference.vectorLens(),
                new SplitMergeEvaluator.Parameters(estimator));
    }

    /**
     * Selects the best valid candidate from the evaluated candidates map. A candidate is valid if
     * the evaluator did not mark it as {@link SplitMergeEvaluator.Decision#INVALID_CANDIDATE}.
     * Among valid candidates, the one with the highest score gain is preferred.
     *
     * @param candidateToUpgradeResultMap map of candidates to their evaluation results
     * @return the best valid candidate, or empty if no valid candidate exists
     */
    @Nonnull
    private Optional<RepartitioningCandidate> selectBestCandidateMaybe(@Nonnull final Map<RepartitioningCandidate, UpgradeResult> candidateToUpgradeResultMap) {
        RepartitioningCandidate bestCandidate = null;
        UpgradeResult bestUpgradeResult = null;
        for (final Map.Entry<RepartitioningCandidate, UpgradeResult> entry : candidateToUpgradeResultMap.entrySet()) {
            final RepartitioningCandidate candidate = entry.getKey();
            final UpgradeResult upgradeResult = entry.getValue();
            if (upgradeResult.getDecision() != SplitMergeEvaluator.Decision.INVALID_CANDIDATE) {
                if (bestUpgradeResult == null) {
                    bestUpgradeResult = upgradeResult;
                    bestCandidate = candidate;
                } else {
                    if (upgradeResult.getScoreGain() > bestUpgradeResult.getScoreGain()) {
                        bestUpgradeResult = upgradeResult;
                        bestCandidate = candidate;
                    }
                }
            }
        }
        return Optional.ofNullable(bestCandidate);
    }

    /**
     * Bundles a candidate repartitioning: the neighborhood context, the primary vectors participating
     * in the split, and the k-means result that defines the proposed new cluster boundaries.
     */
    private record RepartitioningCandidate(@Nonnull Neighborhoods neighborhoods,
                                           @Nonnull List<VectorReference> primaryVectorReferences,
                                           @Nonnull @SuppressWarnings("checkstyle:MemberName") BoundedKMeans.Result<Transformed<RealVector>> kMeansResult) {
    }

    /**
     * The outcome of the vector assignment phase: tracks which clusters are newly created, provides
     * a map from cluster ID to metadata, the multimap of vector-to-cluster assignments, and updated
     * running standard deviations for distance tracking.
     */
    private record Repartitioning(@Nonnull Set<UUID> newClusterIds,
                                  @Nonnull Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                  @Nonnull ListMultimap<UUID, VectorReference> assignmentMultimap,
                                  @Nonnull Map<UUID, RunningStandardDeviation> updatedStandardDeviationsMap) {
    }

    /**
     * Per-cluster counters produced by {@link #writeVectorReferences}, used by
     * {@link #writeClusterMetadataAndEnqueueTasks} to compute final cluster sizes.
     */
    private record VectorWriteCounters(@Nonnull Map<UUID, Integer> numPrimaryVectorsAdded,
                                       @Nonnull Map<UUID, Integer> numPrimaryUnderreplicatedVectorsAdded,
                                       @Nonnull Map<UUID, Integer> numReplicatedVectorsAdded) {
    }
}
