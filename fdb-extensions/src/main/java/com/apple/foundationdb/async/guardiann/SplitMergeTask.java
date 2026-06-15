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
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.kmeans.KMeans;
import com.apple.foundationdb.kmeans.PartitionEvaluator;
import com.apple.foundationdb.kmeans.PartitionEvaluator.EvaluationResult;
import com.apple.foundationdb.linear.DistanceEstimator;
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
 * both 1-to-2 and 2-to-3 splits) and selects the best one using {@link PartitionEvaluator}. Second,
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
    private final List<ClusterReference> neighborhood;

    private SplitMergeTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                           @Nonnull final Transformed<RealVector> centroid,
                           @Nonnull final List<ClusterReference> neighborhood) {
        super(locator, accessInfo, taskId, ImmutableSet.of(targetClusterId));
        this.centroid = centroid;
        this.neighborhood = ImmutableList.copyOf(neighborhood);
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    private List<ClusterReference> getNeighborhood() {
        return neighborhood;
    }

    @Override
    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        super.writeDeferredTask(transaction);
        if (logger.isDebugEnabled()) {
            logger.debug("enqueuing SPLIT_MERGE; taskId={}; clusterId={}",
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
        for (final ClusterReference clusterMetadataWithDistance : getNeighborhood()) {
            neighborhoodTuplesBuilder.add(
                    StorageAdapter.valueTupleFromClusterReference(quantizer, clusterMetadataWithDistance));
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
     *   <li>Selects the best valid candidate via {@link PartitionEvaluator}, or falls back to a collapse
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
        final DistanceEstimator estimator = quantizer.estimator();

        final int numNeighborhood = config.splitNeighborhoodSize();

        final List<ClusterReference> neighborhood = getNeighborhood();
        if (neighborhood.isEmpty()) {
            // if the neighborhoods are not set yet, fetch the neighborhood and write an urgent new task
            return primitives.fetchNeighborhoodClusterMetadata(transaction, targetClusterMetadata,
                    targetClusterCentroid, storageTransform, numNeighborhood)
                    .thenAccept(fetchedNeighborhood -> {
                        final SplitMergeTask splitMergeTask =
                                withHighPriorityAndNeighborhood(random,
                                        ClusterReference.fromClusterMetadataAndDistances(fetchedNeighborhood));
                        splitMergeTask.writeDeferredTask(transaction);
                        if (logger.isDebugEnabled()) {
                            logger.debug("enqueued high priority SPLIT_MERGE due to refetch of the neighborhood; taskId={}; neighborhoodSize={}",
                                    AbstractDeferredTask.taskIdToString(splitMergeTask.getTaskId()),
                                    splitMergeTask.getNeighborhood().size());
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
                        config.splitMergeConcurrency(), executor)
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
                                                                kMeans(neighborhoods, vectorReferences, nestedRandom, estimator,
                                                                        neighborhoods.innerNeighborhood().size() + 1,
                                                                        config.kMeansMaxIterations(), config.kMeansMaxRestarts()));
                                            }, config.splitMergeConcurrency(), executor)
                                    .<RepartitioningCandidate>thenApply(assignmentCandidates -> {
                                        final Map<RepartitioningCandidate, EvaluationResult> candidateToEvaluationResultMap = Maps.newIdentityHashMap();
                                        final RepartitioningCandidate split1to2Candidate =
                                                Objects.requireNonNull(assignmentCandidates.get(0));
                                        final EvaluationResult evaluationResult1to2 =
                                                scoreCandidate(estimator, ImmutableList.of(innerClusters.get(0)),
                                                        split1to2Candidate);
                                        candidateToEvaluationResultMap.put(split1to2Candidate, evaluationResult1to2);
                                        final RepartitioningCandidate split2to3Candidate = assignmentCandidates.get(1);
                                        if (split2to3Candidate != null) {
                                            Verify.verify(innerClusters.size() > 1);
                                            final EvaluationResult evaluationResult2to3 =
                                                    scoreCandidate(estimator, innerClusters,
                                                            split2to3Candidate);
                                            candidateToEvaluationResultMap.put(split2to3Candidate, evaluationResult2to3);
                                        }
                                        final Optional<RepartitioningCandidate> bestValidCandidateOptional =
                                                selectBestCandidateMaybe(candidateToEvaluationResultMap);
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
     * Merges an undersized cluster with its nearest neighbors. Analogous to {@link #split}, operates
     * in the following stages:
     * <ol>
     *   <li>If the neighborhood is not precomputed, fetches it from the HNSW centroid index and
     *       re-enqueues a high-priority task with the neighborhood attached (early return).</li>
     *   <li>Evaluates two candidate repartitionings: a 2-to-1 merge (dissolving the target into its
     *       nearest neighbor) and, if the neighborhood is large enough, a 3-to-2 merge (dissolving
     *       3 clusters into 2).</li>
     *   <li>Runs bounded k-means on the primary vectors for each candidate.</li>
     *   <li>Selects the best valid candidate via {@link PartitionEvaluator}.</li>
     *   <li>Assigns vectors to the reduced set of clusters, updates the HNSW centroid index, and
     *       writes new cluster metadata.</li>
     * </ol>
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
        final Config config = getConfig();
        final SplittableRandom random = RandomHelpers.random(getTaskId());
        final Primitives primitives = primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final DistanceEstimator estimator = quantizer.estimator();

        final int numInnerNeighborhood = config.mergeInnerNeighborhoodSize();
        final int numOuterNeighborhood = config.mergeOuterNeighborhoodSize();
        final int numNeighborhood = numInnerNeighborhood + numOuterNeighborhood;

        final List<ClusterReference> neighborhood = getNeighborhood();
        if (neighborhood.isEmpty()) {
            return primitives.fetchNeighborhoodClusterMetadata(transaction, targetClusterMetadata,
                    targetClusterCentroid, storageTransform, numNeighborhood)
                    .thenAccept(fetchedNeighborhood -> {
                        final SplitMergeTask splitMergeTask =
                                withHighPriorityAndNeighborhood(random,
                                        ClusterReference.fromClusterMetadataAndDistances(fetchedNeighborhood));
                        splitMergeTask.writeDeferredTask(transaction);
                        if (logger.isDebugEnabled()) {
                            logger.debug("enqueued high priority SPLIT_MERGE (merge) due to refetch of the neighborhood; taskId={}; neighborhoodSize={}",
                                    AbstractDeferredTask.taskIdToString(splitMergeTask.getTaskId()),
                                    splitMergeTask.getNeighborhood().size());
                        }
                    });
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("using precomputed neighborhood for merge; taskId={}; neighborhoodSize={}",
                        taskIdToString(getTaskId()), getNeighborhood().size());
            }
        }

        return MoreAsyncUtil.forEach(neighborhood,
                        clusterIdAndCentroid -> primitives.fetchClusterMetadataWithDistance(transaction,
                                clusterIdAndCentroid.clusterId(), clusterIdAndCentroid.centroid(), 0.0d),
                        config.splitMergeConcurrency(), executor)
                .thenCompose(neighborhoodClusterMetadataWithDistances -> {
                    // Compute two candidate merge configurations:
                    // 2-to-1: merge the target and its nearest neighbor into 1 cluster (2 inner + rest outer)
                    // 3-to-2: merge 3 nearest clusters into 2 (3 inner + rest outer)
                    final Neighborhoods neighborhoods2To1 =
                            neighborhoods(neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(),
                                    2, numNeighborhood - 2);

                    // 2->1 is the required fallback merge. Its inner neighborhood is clamped to the
                    // clusters actually available, so it collapses to just the target (size 1) when
                    // there is no mergeable neighbor — e.g. the target is the last/only cluster, as
                    // happens when a structure is drained toward empty. The delete path normally avoids
                    // enqueuing a merge for a lone cluster (it gates on the centroid index cardinality),
                    // so reaching here means the only neighbor disappeared between enqueue and execution.
                    // No merge is possible at all (k-means would be asked for k == 0), so as a backstop we
                    // clear the SPLIT_MERGE flag (as runTask's false-alarm branch does) and stop.
                    if (neighborhoods2To1.innerNeighborhood().size() < 2) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("skipping merge: no mergeable neighbor for cluster {}; taskId={}",
                                    targetClusterMetadata.id(), taskIdToString(getTaskId()));
                        }
                        final EnumSet<ClusterMetadata.State> newStates =
                                EnumSet.copyOf(targetClusterMetadata.states());
                        newStates.remove(ClusterMetadata.State.SPLIT_MERGE);
                        primitives.writeClusterMetadata(transaction, targetClusterMetadata.withNewStates(newStates));
                        return AsyncUtil.DONE;
                    }

                    final Neighborhoods neighborhoods3To2 =
                            neighborhood.size() < 3
                            ? null
                            : neighborhoods(neighborhoodClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(),
                                    3, numNeighborhood - 3);

                    final List<Neighborhoods> allNeighborhoods =
                            Lists.newArrayList(neighborhoods2To1, neighborhoods3To2);

                    return primitives.fetchInnerClusters(transaction,
                                    largestInnerNeighborhood(neighborhoods2To1, neighborhoods3To2), storageTransform)
                            .thenCompose(innerClusters -> RandomHelpers.forEach(random, allNeighborhoods,
                                            (neighborhoods, nestedRandom) -> {
                                                // Skip non-viable candidates: a merge needs at least
                                                // two clusters in the inner neighborhood (k = size - 1
                                                // must be >= 1). null is a candidate that wasn't built
                                                // (3->2 when there are fewer than 3 clusters).
                                                if (neighborhoods == null
                                                        || neighborhoods.innerNeighborhood().size() < 2) {
                                                    return CompletableFuture.completedFuture(null);
                                                }
                                                final var innerNeighborhood = neighborhoods.innerNeighborhood();
                                                final var clampedInnerClusters =
                                                        innerNeighborhood.size() == innerClusters.size()
                                                        ? innerClusters
                                                        : innerClusters.subList(0, innerNeighborhood.size());

                                                return primitives.cleanUpVectorReferences(transaction, clampedInnerClusters, true)
                                                        .thenApply(vectorReferences ->
                                                                kMeans(neighborhoods, vectorReferences, nestedRandom, estimator,
                                                                        neighborhoods.innerNeighborhood().size() - 1,
                                                                        config.kMeansMaxIterations(), config.kMeansMaxRestarts()));
                                            }, config.splitMergeConcurrency(), executor)
                                    .<RepartitioningCandidate>thenApply(mergeCandidates -> {
                                        final Map<RepartitioningCandidate, EvaluationResult> candidateToEvaluationResultMap = Maps.newIdentityHashMap();
                                        final RepartitioningCandidate merge2to1Candidate =
                                                Objects.requireNonNull(mergeCandidates.get(0));
                                        final EvaluationResult evaluationResult2to1 =
                                                scoreCandidate(estimator,
                                                        innerClusters.subList(0, neighborhoods2To1.innerNeighborhood().size()),
                                                        merge2to1Candidate);
                                        candidateToEvaluationResultMap.put(merge2to1Candidate, evaluationResult2to1);
                                        final RepartitioningCandidate merge3to2Candidate = mergeCandidates.get(1);
                                        if (merge3to2Candidate != null) {
                                            Verify.verify(innerClusters.size() > 2);
                                            final EvaluationResult evaluationResult3to2 =
                                                    scoreCandidate(estimator, innerClusters,
                                                            merge3to2Candidate);
                                            candidateToEvaluationResultMap.put(merge3to2Candidate, evaluationResult3to2);
                                        }
                                        final Optional<RepartitioningCandidate> bestValidCandidateOptional =
                                                selectBestCandidateMaybe(candidateToEvaluationResultMap);
                                        return bestValidCandidateOptional.orElse(merge2to1Candidate);
                                    }))
                            .thenCompose(repartitioningCandidate -> {
                                final Neighborhoods neighborhoods = repartitioningCandidate.neighborhoods();
                                final List<ClusterMetadataWithDistance> innerNeighborhood = neighborhoods.innerNeighborhood();
                                final List<ClusterMetadataWithDistance> outerNeighborhood = neighborhoods.outerNeighborhood();

                                final Repartitioning repartitioning =
                                        assignPrimaryVectorReferences(estimator, outerNeighborhood,
                                                repartitioningCandidate.primaryVectorReferences(),
                                                repartitioningCandidate.kMeansResult(),
                                                innerNeighborhood.size() - 1);
                                return replaceCentroidsInHnsw(transaction, storageTransform, innerNeighborhood, repartitioning)
                                        .thenAccept(ignored ->
                                                persistRepartitioning(transaction, random, innerNeighborhood, repartitioning, quantizer));
                            });
                });
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
    private Repartitioning assignPrimaryVectorReferences(@Nonnull final DistanceEstimator estimator,
                                                         @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood,
                                                         @Nonnull final List<VectorReference> primaryVectorReferences,
                                                         @Nonnull final KMeans.Result<Transformed<RealVector>> kMeansResult,
                                                         final int targetNumPartitions) {
        final Config config = getConfig();

        final List<Transformed<RealVector>> clusterCentroids =
                kMeansResult.clusterCentroids();
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
                                    RunningStats.identity(),
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
        // At this point clusterIdMetadataMap contains the new clusters after the split/merge and all the clusters
        // from the outer neighborhood.
        //

        //
        // Initialize a map we need to use to keep track for the correct most up-to-date count, mean, and
        // standard deviations for distances.
        //
        final Map<UUID, RunningStats> standardDeviationsMap = Maps.newHashMap();
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

        RunningStats replicationPriorityStandardDeviation = RunningStats.identity();
        int numReplicated = 0;
        int numOccluded = 0;
        // only considering primary copies here -- this will prune the replicated vectors
        for (final VectorReference vectorReference : primaryVectorReferences) {
            final var nearestClusters =
                    Objects.requireNonNull(invertedAssignmentsMap.get(vectorReference.id().getUuid()));
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
                        numOccluded ++;
                        continue;
                    }

                    final VectorReference newVectorReference =
                            vectorReference.toReplicatedCopy(replicationPriority);
                    if (newClusterIds.contains(replicationCandidateClusterMetadata.id())) {
                        final var reservoirSampler =
                                replicatedAssignmentSamplerMap.computeIfAbsent(replicationCandidateClusterMetadata.id(),
                                        ignored -> TopK.max(Comparator.comparing(VectorReference::replicationPriority),
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
                    replicationPriorityStandardDeviation.numElements(),
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
        final Map<UUID, RunningStats> updatedStandardDeviationsMap =
                repartitioning.updatedStandardDeviationsMap();

        final ImmutableSet.Builder<UUID> newDependentTaskIdsBuilder = ImmutableSet.builder();
        for (final Map.Entry<UUID, ClusterMetadataWithDistance> entry : clusterIdMetadataMap.entrySet()) {
            final UUID toBeWritten = entry.getKey();
            final ClusterMetadataWithDistance clusterMetadataWithDistance = entry.getValue();
            final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.clusterMetadata();
            final int numPrimaryVectorsAdded = counters.numPrimaryVectorsAdded().getOrDefault(toBeWritten, 0);
            final int numPrimaryUnderreplicatedVectorsAdded = counters.numPrimaryUnderreplicatedVectorsAdded().getOrDefault(toBeWritten, 0);
            final int numReplicatedVectorsAdded = counters.numReplicatedVectorsAdded().getOrDefault(toBeWritten, 0);
            final RunningStats updatedStandardDeviation =
                    Objects.requireNonNull(updatedStandardDeviationsMap.get(toBeWritten));

            Verify.verify(clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded > 0);

            primitives().updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe(transaction, random, clusterMetadata,
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
                                                           @Nonnull final List<ClusterReference> neighborhood) {
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
        final ImmutableList.Builder<ClusterReference> neighborhoodsBuilder = ImmutableList.builder();
        final Tuple neighborhoodsTuple = valueTuple.getNestedTuple(3);
        for (int i = 0; i < neighborhoodsTuple.size(); i ++) {
            final Tuple clusterMetadataWithDistanceTuple = neighborhoodsTuple.getNestedTuple(i);
            neighborhoodsBuilder.add(StorageAdapter.clusterReferenceFromTuple(locator.getConfig(),
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
                             @Nonnull final List<ClusterReference> neighborhood) {
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
     * into {@code targetNumPartitions} clusters. Used by both split and merge paths as the per-candidate
     * clustering step.
     *
     * @param neighborhoods the neighborhood context
     * @param vectorReferences all vector references (filtered to primary copies internally)
     * @param random source of randomness for k-means initialization
     * @param estimator distance estimator for the vector space
     * @param targetNumPartitions the desired number of output clusters
     * @param maxIterations maximum Lloyd's iterations per k-means restart
     * @param maxRestarts maximum number of random restarts
     * @return an assignment candidate wrapping the k-means result and primary vectors
     */
    @Nonnull
    @SuppressWarnings("checkstyle:MethodName")
    private static RepartitioningCandidate kMeans(@Nonnull final Neighborhoods neighborhoods,
                                                  @Nonnull final List<VectorReference> vectorReferences,
                                                  @Nonnull final SplittableRandom random,
                                                  @Nonnull final DistanceEstimator estimator,
                                                  final int targetNumPartitions,
                                                  final int maxIterations,
                                                  final int maxRestarts) {
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
                KMeans.fit(random, estimator, VectorReference.vectorLens(),
                        Transformed.underlyingLens(), primaryVectorReferences,
                        targetNumPartitions, maxIterations,
                        maxRestarts, 0.00, KMeans.overflowQuadraticPenalty()));
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
    private static EvaluationResult
            scoreCandidate(@Nonnull final DistanceEstimator estimator,
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

        final PartitionEvaluator.Partition<Transformed<RealVector>> currentPartition =
                new PartitionEvaluator.Partition<>(clusterCentroidsBuilder.build(),
                        Transformed.underlyingLens(), assignment);
        final var kMeansResult = repartitioningCandidate.kMeansResult();
        final PartitionEvaluator.Partition<Transformed<RealVector>> candidatePartition =
                new PartitionEvaluator.Partition<>(kMeansResult.clusterCentroids(),
                        Transformed.underlyingLens(), kMeansResult.assignment());
        return PartitionEvaluator.evaluate(primaryVectorReferencesBuilder.build(),
                currentPartition,
                repartitioningCandidate.primaryVectorReferences(),
                candidatePartition,
                VectorReference.vectorLens(),
                parametersFor(estimator, currentPartition.k(), candidatePartition.k()));
    }

    /**
     * Returns the {@link PartitionEvaluator.Parameters} appropriate for the given transition. The
     * generalized {@link PartitionEvaluator.Parameters} record has a single {@code minSmallestFrac}
     * / {@code maxLargestFrac} pair, so the caller picks values per transition kind:
     * <ul>
     *   <li>{@code 1 → 2}: {@code minSmallestFrac=0.03}, no upper bound on the largest cluster.
     *   <li>{@code 2 → 3}: {@code minSmallestFrac=0.015}, {@code maxLargestFrac=0.55}.
     *   <li>{@code 2 → 1} / {@code 3 → 2} merges: permissive (no smallest/largest constraints).
     * </ul>
     */
    @Nonnull
    private static PartitionEvaluator.Parameters parametersFor(@Nonnull final DistanceEstimator estimator,
                                                               final int currentK,
                                                               final int candidateK) {
        final PartitionEvaluator.Parameters defaults = new PartitionEvaluator.Parameters(estimator);
        final double minSmallestFrac;
        final double maxLargestFrac;
        if (currentK == 1 && candidateK == 2) {
            minSmallestFrac = 0.03d;
            maxLargestFrac = 1.0d;
        } else if (currentK == 2 && candidateK == 3) {
            minSmallestFrac = 0.015d;
            maxLargestFrac = 0.55d;
        } else {
            // merges (2 → 1, 3 → 2): permissive
            minSmallestFrac = 0.0d;
            maxLargestFrac = 1.0d;
        }
        return new PartitionEvaluator.Parameters(estimator,
                defaults.minRelativeSseGain(), defaults.minSeparation(), defaults.maxLowMarginRate(),
                minSmallestFrac, maxLargestFrac, defaults.lowMarginThreshold(),
                defaults.alphaSseGain(), defaults.betaSeparationGain(),
                defaults.gammaImbalancePenalty(), defaults.deltaLowMarginPenalty(),
                defaults.minScoreGain());
    }

    /**
     * Selects the best valid candidate from the evaluated candidates map. A candidate is valid if
     * the evaluator did not mark it as {@link PartitionEvaluator.Decision#INVALID_CANDIDATE}.
     * Among valid candidates, the one with the highest score gain is preferred.
     *
     * @param candidateToEvaluationResultMap map of candidates to their evaluation results
     * @return the best valid candidate, or empty if no valid candidate exists
     */
    @Nonnull
    private Optional<RepartitioningCandidate> selectBestCandidateMaybe(@Nonnull final Map<RepartitioningCandidate, EvaluationResult> candidateToEvaluationResultMap) {
        RepartitioningCandidate bestCandidate = null;
        EvaluationResult bestEvaluationResult = null;
        for (final Map.Entry<RepartitioningCandidate, EvaluationResult> entry : candidateToEvaluationResultMap.entrySet()) {
            final RepartitioningCandidate candidate = entry.getKey();
            final EvaluationResult evaluationResult = entry.getValue();
            if (evaluationResult.decision() != PartitionEvaluator.Decision.INVALID_CANDIDATE) {
                if (bestEvaluationResult == null) {
                    bestEvaluationResult = evaluationResult;
                    bestCandidate = candidate;
                } else {
                    if (evaluationResult.scoreGain() > bestEvaluationResult.scoreGain()) {
                        bestEvaluationResult = evaluationResult;
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
                                           @Nonnull @SuppressWarnings("checkstyle:MemberName") KMeans.Result<Transformed<RealVector>> kMeansResult) {
    }

    /**
     * The outcome of the vector assignment phase: tracks which clusters are newly created, provides
     * a map from cluster ID to metadata, the multimap of vector-to-cluster assignments, and updated
     * running standard deviations for distance tracking.
     */
    private record Repartitioning(@Nonnull Set<UUID> newClusterIds,
                                  @Nonnull Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                  @Nonnull ListMultimap<UUID, VectorReference> assignmentMultimap,
                                  @Nonnull Map<UUID, RunningStats> updatedStandardDeviationsMap) {
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
