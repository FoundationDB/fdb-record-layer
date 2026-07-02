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
import com.apple.foundationdb.async.common.TopK;
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
    private final List<ClusterReference> nearestClusters;

    private SplitMergeTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                           @Nonnull final Transformed<RealVector> centroid,
                           @Nonnull final List<ClusterReference> nearestClusters) {
        super(locator, accessInfo, taskId, ImmutableSet.of(targetClusterId));
        this.centroid = centroid;
        this.nearestClusters = ImmutableList.copyOf(nearestClusters);
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    private List<ClusterReference> getNearestClusters() {
        return nearestClusters;
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
     * Encodes the centroid and the precomputed nearest clusters so the task can be resumed without
     * re-fetching from the HNSW index.
     */
    @Nonnull
    @Override
    public Tuple valueTuple() {
        final Quantizer quantizer = primitives().quantizer(getAccessInfo());
        final Transformed<RealVector> encodedVector = quantizer.encode(getCentroid());

        final ImmutableList.Builder<Object> nearestClustersTuplesBuilder = ImmutableList.builder();
        for (final ClusterReference clusterMetadataWithDistance : getNearestClusters()) {
            nearestClustersTuplesBuilder.add(
                    StorageAdapter.valueTupleFromClusterReference(quantizer, clusterMetadataWithDistance));
        }

        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                StorageHelpers.bytesFromVector(encodedVector), Tuple.fromItems(nearestClustersTuplesBuilder.build()));
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
     *   <li>If the nearest clusters are not precomputed, fetches them from the HNSW centroid index and
     *       re-enqueues a high-priority task with the nearest clusters attached (early return).</li>
     *   <li>Evaluates two candidate repartitionings: a 1-to-2 split (splitting the target cluster into 2)
     *       and, if the set of nearest clusters is large enough, a 2-to-3 split (splitting the target and its nearest
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

        final int numNearestClusters = config.splitNumNearestClusters();

        final CompletableFuture<Void> reenqueue = reenqueueWithFetchedNearestClustersIfEmpty(
                transaction, targetClusterMetadata, targetClusterCentroid, random, storageTransform, numNearestClusters);
        if (reenqueue != null) {
            return reenqueue;
        }
        final List<ClusterReference> nearestClusters = getNearestClusters();

        return fetchNearestClustersMetadata(transaction, nearestClusters, executor)
                .thenCompose(nearestClusterMetadataWithDistances ->
                        selectSplitCandidate(transaction, random, storageTransform, estimator, numNearestClusters,
                                targetClusterMetadata, nearestClusters, nearestClusterMetadataWithDistances))
                .thenCompose(repartitioningCandidate ->
                        applyRepartitioning(transaction, random, storageTransform, quantizer, estimator,
                                repartitioningCandidate));
    }

    /**
     * If the nearest clusters have not been precomputed yet, fetches them and re-enqueues this work as a high-priority
     * task carrying them, returning the (non-null) re-enqueue future so the caller can return it as an
     * early-out. If precomputed nearest clusters are already present, returns {@code null} and the caller proceeds.
     */
    @Nullable
    private CompletableFuture<Void> reenqueueWithFetchedNearestClustersIfEmpty(@Nonnull final Transaction transaction,
                                                                               @Nonnull final ClusterMetadata targetClusterMetadata,
                                                                               @Nonnull final RealVector targetClusterCentroid,
                                                                               @Nonnull final SplittableRandom random,
                                                                               @Nonnull final StorageTransform storageTransform,
                                                                               final int numNearestClusters) {
        if (!getNearestClusters().isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("using precomputed nearest clusters; taskId={}; numNearestClusters={}",
                        taskIdToString(getTaskId()), getNearestClusters().size());
            }
            return null;
        }
        // the nearest clusters are not set yet: fetch them and write an urgent new task carrying them
        return primitives().fetchNearestClusterMetadata(transaction, targetClusterMetadata,
                        targetClusterCentroid, storageTransform, numNearestClusters)
                .thenAccept(fetchedNearestClusters -> {
                    final SplitMergeTask splitMergeTask =
                            withHighPriorityAndNearestClusters(random,
                                    ClusterReference.fromClusterMetadataAndDistances(fetchedNearestClusters));
                    splitMergeTask.writeDeferredTask(transaction);
                    if (logger.isDebugEnabled()) {
                        logger.debug("enqueued high priority SPLIT_MERGE due to refetch of the nearest clusters; taskId={}; numNearestClusters={}",
                                AbstractDeferredTask.taskIdToString(splitMergeTask.getTaskId()),
                                splitMergeTask.getNearestClusters().size());
                    }
                });
    }

    @Nonnull
    private CompletableFuture<List<ClusterMetadataWithDistance>> fetchNearestClustersMetadata(@Nonnull final Transaction transaction,
                                                                                              @Nonnull final List<ClusterReference> nearestClusters,
                                                                                              @Nonnull final Executor executor) {
        return MoreAsyncUtil.forEach(nearestClusters,
                clusterIdAndCentroid -> primitives().fetchClusterMetadataWithDistance(transaction,
                        clusterIdAndCentroid.clusterId(), clusterIdAndCentroid.centroid(), 0.0d),
                getConfig().splitMergeConcurrency(), executor);
    }

    /**
     * Applies a chosen repartitioning: assigns primaries to the new clusters, replaces the core-cluster
     * centroids in the HNSW index, and persists the result. A {@code null} candidate (split chose to collapse, or
     * merge found no mergeable neighbor) is a no-op.
     */
    @Nonnull
    private CompletableFuture<Void> applyRepartitioning(@Nonnull final Transaction transaction,
                                                        @Nonnull final SplittableRandom random,
                                                        @Nonnull final StorageTransform storageTransform,
                                                        @Nonnull final Quantizer quantizer,
                                                        @Nonnull final DistanceEstimator estimator,
                                                        @Nullable final RepartitioningCandidate repartitioningCandidate) {
        if (repartitioningCandidate == null) {
            return AsyncUtil.DONE;
        }

        final ClusterClassification classification = repartitioningCandidate.classification();
        final List<ClusterMetadataWithDistance> coreClusters = classification.coreClusters();
        final List<ClusterMetadataWithDistance> neighboringClusters = classification.neighboringClusters();

        final Repartitioning repartitioning =
                assignPrimaryVectorReferences(random, estimator, neighboringClusters,
                        repartitioningCandidate.primaryVectorReferences(),
                        repartitioningCandidate.kMeansResult(),
                        repartitioningCandidate.kMeansResult().clusterCentroids().size());
        return replaceCentroidsInHnsw(transaction, storageTransform, coreClusters, repartitioning)
                .thenAccept(ignored ->
                        persistRepartitioning(transaction, random, coreClusters, repartitioning, quantizer));
    }

    /**
     * Builds the split repartitioning candidates (1&#8594;2, and 2&#8594;3 when there are at least two clusters),
     * scores them, and returns the best; returns a future of {@code null} (after enqueueing a collapse when
     * warranted) when neither candidate is viable.
     */
    @Nonnull
    private CompletableFuture<RepartitioningCandidate> selectSplitCandidate(@Nonnull final Transaction transaction,
                                                                            @Nonnull final SplittableRandom random,
                                                                            @Nonnull final StorageTransform storageTransform,
                                                                            @Nonnull final DistanceEstimator estimator,
                                                                            final int numNearestClusters,
                                                                            @Nonnull final ClusterMetadata targetClusterMetadata,
                                                                            @Nonnull final List<ClusterReference> nearestClusters,
                                                                            @Nonnull final List<ClusterMetadataWithDistance> nearestClusterMetadataWithDistances) {
        final Config config = getConfig();
        final Executor executor = getLocator().getExecutor();
        final Primitives primitives = primitives();

        // Compute two candidate split configurations:
        // 1-to-2: split the target into 2 clusters (1 inner + rest outer)
        // 2-to-3: split the target and its nearest neighbor into 3 (2 inner + rest outer)
        final ClusterClassification classification1To2 =
                classifyClusters(nearestClusterMetadataWithDistances,
                        targetClusterMetadata, getCentroid(),
                        1, numNearestClusters - 1);
        final ClusterClassification classification2To3 =
                nearestClusters.size() < 2
                ? null
                : classifyClusters(nearestClusterMetadataWithDistances,
                        targetClusterMetadata, getCentroid(),
                        2, numNearestClusters - 2);

        final List<ClusterClassification> allClassifications =
                Lists.newArrayList(classification1To2, classification2To3);

        return primitives.fetchCoreClusters(transaction,
                        largestCoreClusters(classification1To2, classification2To3), storageTransform)
                .thenCompose(innerClusters -> RandomHelpers.forEach(random, allClassifications,
                                (classification, nestedRandom) -> {
                                    if (classification == null) {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                    final List<ClusterMetadataWithDistance> coreClusters = classification.coreClusters();
                                    final List<Cluster> clampedInnerClusters =
                                            coreClusters.size() == innerClusters.size()
                                            ? innerClusters
                                            : innerClusters.subList(0, coreClusters.size());

                                    return primitives.cleanUpVectorReferences(transaction, clampedInnerClusters, true)
                                            .thenApply(vectorReferences ->
                                                    kMeans(classification, vectorReferences, nestedRandom, estimator,
                                                            classification.coreClusters().size() + 1,
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
                        }));
    }

    /**
     * Merges an undersized cluster with its nearest neighbors. Analogous to {@link #split}, operates
     * in the following stages:
     * <ol>
     *   <li>If the nearest clusters are not precomputed, fetches them from the HNSW centroid index and
     *       re-enqueues a high-priority task with the nearest clusters attached (early return).</li>
     *   <li>Evaluates two candidate repartitionings: a 2-to-1 merge (dissolving the target into its
     *       nearest neighbor) and, if the set of nearest clusters is large enough, a 3-to-2 merge (dissolving
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

        final int numNearestClusters = config.mergeNumNearestClusters();

        final CompletableFuture<Void> reenqueue = reenqueueWithFetchedNearestClustersIfEmpty(
                transaction, targetClusterMetadata, targetClusterCentroid, random, storageTransform, numNearestClusters);
        if (reenqueue != null) {
            return reenqueue;
        }
        final List<ClusterReference> nearestClusters = getNearestClusters();

        return fetchNearestClustersMetadata(transaction, nearestClusters, executor)
                .thenCompose(nearestClusterMetadataWithDistances ->
                        selectMergeCandidate(transaction, random, storageTransform, estimator, numNearestClusters,
                                targetClusterMetadata, nearestClusters, nearestClusterMetadataWithDistances))
                .thenCompose(repartitioningCandidate ->
                        applyRepartitioning(transaction, random, storageTransform, quantizer, estimator,
                                repartitioningCandidate));
    }

    /**
     * Builds the merge repartitioning candidates (2&#8594;1, and 3&#8594;2 when there are at least three clusters),
     * scores them, and returns the best (falling back to the 2&#8594;1 merge); returns a future of {@code null}
     * (after clearing {@code SPLIT_MERGE}) when there is no mergeable neighbor at all.
     */
    @Nonnull
    private CompletableFuture<RepartitioningCandidate> selectMergeCandidate(@Nonnull final Transaction transaction,
                                                                            @Nonnull final SplittableRandom random,
                                                                            @Nonnull final StorageTransform storageTransform,
                                                                            @Nonnull final DistanceEstimator estimator,
                                                                            final int numNearestClusters,
                                                                            @Nonnull final ClusterMetadata targetClusterMetadata,
                                                                            @Nonnull final List<ClusterReference> nearestClusters,
                                                                            @Nonnull final List<ClusterMetadataWithDistance> nearestClusterMetadataWithDistances) {
        final Config config = getConfig();
        final Executor executor = getLocator().getExecutor();
        final Primitives primitives = primitives();

        // Compute two candidate merge configurations:
        // 2-to-1: merge the target and its nearest neighbor into 1 cluster (2 inner + rest outer)
        // 3-to-2: merge 3 nearest clusters into 2 (3 inner + rest outer)
        final ClusterClassification classification2To1 =
                classifyClusters(nearestClusterMetadataWithDistances,
                        targetClusterMetadata, getCentroid(),
                        2, numNearestClusters - 2);

        // 2->1 is the required fallback merge. Its core clusters are clamped to the
        // clusters actually available, so it collapses to just the target (size 1) when
        // there is no mergeable neighbor — e.g. the target is the last/only cluster, as
        // happens when a structure is drained toward empty. The delete path normally avoids
        // enqueuing a merge for a lone cluster (it gates on the centroid HNSW cardinality),
        // so reaching here means the only neighbor disappeared between enqueue and execution.
        // No merge is possible at all (k-means would be asked for k == 0), so as a backstop we
        // clear the SPLIT_MERGE flag (as runTask's false-alarm branch does) and stop.
        if (classification2To1.coreClusters().size() < 2) {
            if (logger.isDebugEnabled()) {
                logger.debug("skipping merge: no mergeable neighbor for cluster {}; taskId={}",
                        targetClusterMetadata.id(), taskIdToString(getTaskId()));
            }
            final EnumSet<ClusterMetadata.State> newStates =
                    EnumSet.copyOf(targetClusterMetadata.states());
            newStates.remove(ClusterMetadata.State.SPLIT_MERGE);
            primitives.writeClusterMetadata(transaction, targetClusterMetadata.withNewStates(newStates));
            return CompletableFuture.completedFuture(null);
        }

        final ClusterClassification classification3To2 =
                nearestClusters.size() < 3
                ? null
                : classifyClusters(nearestClusterMetadataWithDistances,
                        targetClusterMetadata, getCentroid(),
                        3, numNearestClusters - 3);

        final List<ClusterClassification> allClassifications =
                Lists.newArrayList(classification2To1, classification3To2);

        return primitives.fetchCoreClusters(transaction,
                        largestCoreClusters(classification2To1, classification3To2), storageTransform)
                .thenCompose(innerClusters -> RandomHelpers.forEach(random, allClassifications,
                                (classification, nestedRandom) -> {
                                    // Skip non-viable candidates: a merge needs at least
                                    // two clusters in the core clusters (k = size - 1
                                    // must be >= 1). null is a candidate that wasn't built
                                    // (3->2 when there are fewer than 3 clusters).
                                    if (classification == null
                                            || classification.coreClusters().size() < 2) {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                    final List<ClusterMetadataWithDistance> coreClusters = classification.coreClusters();
                                    final List<Cluster> clampedInnerClusters =
                                            coreClusters.size() == innerClusters.size()
                                            ? innerClusters
                                            : innerClusters.subList(0, coreClusters.size());

                                    return primitives.cleanUpVectorReferences(transaction, clampedInnerClusters, true)
                                            .thenApply(vectorReferences ->
                                                    kMeans(classification, vectorReferences, nestedRandom, estimator,
                                                            classification.coreClusters().size() - 1,
                                                            config.kMeansMaxIterations(), config.kMeansMaxRestarts()));
                                }, config.splitMergeConcurrency(), executor)
                        .<RepartitioningCandidate>thenApply(mergeCandidates -> {
                            final Map<RepartitioningCandidate, EvaluationResult> candidateToEvaluationResultMap = Maps.newIdentityHashMap();
                            final RepartitioningCandidate merge2to1Candidate =
                                    Objects.requireNonNull(mergeCandidates.get(0));
                            final EvaluationResult evaluationResult2to1 =
                                    scoreCandidate(estimator,
                                            innerClusters.subList(0, classification2To1.coreClusters().size()),
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
                        }));
    }

    /**
     * Assigns each primary vector reference to its nearest cluster (from new + outer clusters) and
     * determines replication targets based on distance-based priority scoring. Vectors whose primary
     * assignment falls outside the new clusters are marked as underreplicated. Replicated copies are
     * subject to occlusion filtering and bounded by {@link Config#replicatedClusterTarget()} via a
     * bounded top-K by replication priority.
     *
     * @param random the deterministic random source used to mint the new cluster ids
     * @param estimator distance estimator for the vector space
     * @param neighboringClusters clusters outside the split/merge region that may receive vectors
     * @param primaryVectorReferences the primary vector references to assign
     * @param kMeansResult the k-means clustering result providing centroids and initial assignments
     * @param targetNumPartitions number of new clusters being created
     * @return an assignment result containing the vector-to-cluster mapping and updated statistics
     */
    @Nonnull
    private Repartitioning assignPrimaryVectorReferences(@Nonnull final SplittableRandom random,
                                                         @Nonnull final DistanceEstimator estimator,
                                                         @Nonnull final List<ClusterMetadataWithDistance> neighboringClusters,
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
            final UUID newClusterId = RandomHelpers.randomUuid(random, config.deterministicRandomness());
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

        for (final ClusterMetadataWithDistance clusterMetadata : neighboringClusters) {
            clusterIdMetadataMapBuilder.put(clusterMetadata.clusterMetadata().id(), clusterMetadata);
        }
        final ImmutableMap<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                clusterIdMetadataMapBuilder.build();

        //
        // At this point clusterIdMetadataMap contains the new clusters after the split/merge and all the
        // neighboring clusters.
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
                computeNearestClusters(estimator, primaryVectorReferences, clusterIdMetadataMap.values());
        mergeStandardDeviationUpdates(standardDeviationsMap,
                nearestClustersResult.standardDeviationUpdates());
        final ImmutableListMultimap<UUID, ClusterMetadataWithDistance> invertedAssignmentsMap =
                nearestClustersResult.invertedAssignments();

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignmentMultimapBuilder =
                ImmutableListMultimap.builder();
        final Map<UUID, TopK<VectorReference>> replicatedAssignmentTopKMap = Maps.newHashMap();

        ReplicationStats stats = ReplicationStats.identity();
        // only considering primary copies here -- this will prune the replicated vectors
        for (final VectorReference vectorReference : primaryVectorReferences) {
            final ImmutableList<ClusterMetadataWithDistance> nearestClusters =
                    Objects.requireNonNull(invertedAssignmentsMap.get(vectorReference.id().uuid()));
            Verify.verify(!nearestClusters.isEmpty());
            final ClusterMetadataWithDistance primaryCluster = Objects.requireNonNull(nearestClusters.get(0));
            final double distanceToPrimaryCentroid = primaryCluster.distance();
            Verify.verify(Double.isFinite(distanceToPrimaryCentroid));

            final UUID primaryClusterId = primaryCluster.clusterMetadata().id();
            if (!newClusterIds.contains(primaryClusterId)) {
                // Vector's nearest cluster is one of the neighboring clusters — it migrated away from
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
            final ReplicaSelection selection = selectReplicationAssignments(estimator, vectorReference,
                    distanceToPrimaryCentroid, replicationCandidates, standardDeviationsMap);
            for (final Map.Entry<UUID, VectorReference> placement : selection.replicasByCluster().entries()) {
                final UUID clusterId = placement.getKey();
                if (newClusterIds.contains(clusterId)) {
                    replicatedAssignmentTopKMap.computeIfAbsent(clusterId,
                                    ignored -> TopK.max(Comparator.comparing(VectorReference::replicationPriority)
                                            .thenComparing(VectorReference::id), config.replicatedClusterTarget()))
                            .add(placement.getValue());
                } else {
                    assignmentMultimapBuilder.put(clusterId, placement.getValue());
                }
            }
            stats = stats.combine(selection.stats());
        }

        if (logger.isTraceEnabled()) {
            logger.trace("replication priority num={}. mean={}, standard deviation={}, numReplicated={}, numOccluded={}",
                    stats.replicationPriorityStandardDeviation().numElements(),
                    stats.replicationPriorityStandardDeviation().mean(),
                    stats.replicationPriorityStandardDeviation().populationStandardDeviation(),
                    stats.numReplicated(), stats.numOccluded());
        }

        for (final Map.Entry<UUID, TopK<VectorReference>> entry : replicatedAssignmentTopKMap.entrySet()) {
            assignmentMultimapBuilder.putAll(entry.getKey(), entry.getValue().toUnsortedList());
        }

        return new Repartitioning(newClusterIds, clusterIdMetadataMap,
                assignmentMultimapBuilder.build(), standardDeviationsMap);
    }

    /**
     * Selects the replication assignments for a single primary {@code vectorReference}: for each nearby candidate
     * cluster that should hold a replica, records a replicated copy keyed by that cluster. Pure — returns the
     * replicas to place together with this primary's contribution to the replication trace counters.
     */
    @Nonnull
    private ReplicaSelection selectReplicationAssignments(@Nonnull final DistanceEstimator estimator,
                                                          @Nonnull final VectorReference vectorReference,
                                                          final double distanceToPrimaryCentroid,
                                                          @Nonnull final List<ClusterMetadataWithDistance> replicationCandidates,
                                                          @Nonnull final Map<UUID, RunningStats> standardDeviationsMap) {
        final Config config = getConfig();
        final List<ClusterMetadataWithDistance> selectedReplicationClusters =
                Lists.newArrayListWithExpectedSize(replicationCandidates.size());
        final ImmutableListMultimap.Builder<UUID, VectorReference> replicasByCluster = ImmutableListMultimap.builder();

        RunningStats replicationPriorityStandardDeviation = RunningStats.identity();
        int numReplicated = 0;
        int numOccluded = 0;

        for (final ClusterMetadataWithDistance replicationCandidate : replicationCandidates) {
            final double distance = replicationCandidate.distance();
            Verify.verify(Double.isFinite(distance));

            final ClusterMetadata replicationCandidateClusterMetadata = replicationCandidate.clusterMetadata();

            final RunningStats updatedStandardDeviation =
                    Objects.requireNonNull(
                            standardDeviationsMap.get(replicationCandidateClusterMetadata.id()));

            final double replicationPriority =
                    StorageAdapter.replicationPriority(config, distance, distanceToPrimaryCentroid,
                            Math.toIntExact(updatedStandardDeviation.numElements()),
                            updatedStandardDeviation.mean(),
                            updatedStandardDeviation.populationStandardDeviation());
            replicationPriorityStandardDeviation = replicationPriorityStandardDeviation.add(replicationPriority);

            if (replicationPriority >= config.replicationPriorityMin()) {
                if (StorageAdapter.isOccluded(estimator, replicationCandidate, selectedReplicationClusters)) {
                    numOccluded++;
                    continue;
                }

                replicasByCluster.put(replicationCandidateClusterMetadata.id(),
                        vectorReference.toReplicatedCopy(replicationPriority));
                selectedReplicationClusters.add(replicationCandidate);
                numReplicated++;
            }
        }
        return new ReplicaSelection(replicasByCluster.build(),
                new ReplicationStats(replicationPriorityStandardDeviation, numReplicated, numOccluded));
    }

    /**
     * Updates the HNSW centroid index by removing the old core clusters and inserting
     * the newly created cluster centroids. Deletions and insertions are performed sequentially
     * (delete-then-insert) to avoid conflicts within the HNSW graph.
     *
     * @param transaction the FDB transaction
     * @param storageTransform transform used to untransform centroids for HNSW insertion
     * @param coreClusters the clusters being removed from the index
     * @param repartitioning the result containing the new cluster IDs and their centroids
     * @return a future completing with the assignment result (passed through for chaining)
     */
    @Nonnull
    private CompletableFuture<Repartitioning> replaceCentroidsInHnsw(@Nonnull final Transaction transaction,
                                                                     @Nonnull final StorageTransform storageTransform,
                                                                     @Nonnull final List<ClusterMetadataWithDistance> coreClusters,
                                                                     @Nonnull final Repartitioning repartitioning) {
        final Primitives primitives = primitives();
        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();
        final Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap = repartitioning.clusterIdMetadataMap();
        final Set<UUID> newClusterIds = repartitioning.newClusterIds();

        // delete first
        final CompletableFuture<List<Void>> deletedCentroidsFuture =
                MoreAsyncUtil.forEach(coreClusters,
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
     * Persists the final vector assignments to storage. Deletes old cluster data for the core
     * clusters, writes each vector reference to its assigned cluster, updates cluster metadata
     * with new vector counts and running standard deviations, and enqueues follow-up
     * {@link ReassignTask}s (via a {@link BounceTask}) for any new clusters that may themselves
     * need rebalancing.
     *
     * @param transaction the FDB transaction
     * @param random source of randomness for task ID generation
     * @param coreClusters the clusters being dissolved
     * @param repartitioning the computed vector-to-cluster mapping
     * @param quantizer quantizer used to encode vectors for storage
     */
    private void persistRepartitioning(@Nonnull final Transaction transaction,
                                       @Nonnull final SplittableRandom random,
                                       @Nonnull final List<ClusterMetadataWithDistance> coreClusters,
                                       @Nonnull final Repartitioning repartitioning,
                                       @Nonnull final Quantizer quantizer) {
        deleteDissolvedClusters(transaction, coreClusters);
        final VectorWriteCounters counters = writeVectorReferences(transaction, quantizer, repartitioning);
        final Set<UUID> dependentTaskIds = writeClusterMetadataAndEnqueueTasks(transaction, random, repartitioning, counters);
        enqueueBounceIfNeeded(transaction, random, repartitioning.newClusterIds(), dependentTaskIds);
    }

    /**
     * Removes all vector references and metadata for the clusters being dissolved by this
     * split or merge operation.
     *
     * @param transaction the transaction to delete from
     * @param coreClusters the clusters being dissolved, whose vector references and metadata are removed
     */
    private void deleteDissolvedClusters(@Nonnull final Transaction transaction,
                                         @Nonnull final List<ClusterMetadataWithDistance> coreClusters) {
        final Primitives primitives = primitives();
        for (final ClusterMetadataWithDistance clusterMetadata : coreClusters) {
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
     *
     * @param transaction the transaction to write the bounce task into
     * @param random source of randomness for the bounce task id
     * @param newClusterIds the ids of the clusters created by this split/merge that the bounce will reassign
     * @param dependentTaskIds the ids of the tasks the bounce must wait for; no task is enqueued if this is empty
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
     * Creates a copy of this task with high priority and the given precomputed nearest clusters, used
     * when the initial task did not have the nearest clusters attached and needs to be re-enqueued
     * after fetching them.
     *
     * @param random source of randomness for the high-priority task id
     * @param nearestClusters the precomputed nearest clusters to attach to the re-enqueued task
     *
     * @return a high-priority copy of this task carrying the given nearest clusters
     */
    @Nonnull
    private SplitMergeTask withHighPriorityAndNearestClusters(@Nonnull final SplittableRandom random,
                                                           @Nonnull final List<ClusterReference> nearestClusters) {
        return SplitMergeTask.of(getLocator(), getAccessInfo(),
                randomHighPriorityTaskId(random, getConfig().deterministicRandomness()), getTargetClusterId(),
                getCentroid(), nearestClusters);
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
        final ImmutableList.Builder<ClusterReference> nearestClustersBuilder = ImmutableList.builder();
        final Tuple nearestClustersTuple = valueTuple.getNestedTuple(3);
        for (int i = 0; i < nearestClustersTuple.size(); i ++) {
            final Tuple clusterMetadataWithDistanceTuple = nearestClustersTuple.getNestedTuple(i);
            nearestClustersBuilder.add(StorageAdapter.clusterReferenceFromTuple(locator.getConfig(),
                    storageTransform, clusterMetadataWithDistanceTuple));
        }

        return new SplitMergeTask(locator, accessInfo,
                keyTuple.getUUID(0), valueTuple.getUUID(1), centroid, nearestClustersBuilder.build());
    }

    /**
     * Creates a new {@code SplitMergeTask} without precomputed nearest clusters. The nearest clusters will
     * be fetched from the HNSW centroid index when the task executes.
     *
     * @param locator the locator providing access to primitives and configuration
     * @param accessInfo the access context for the current operation
     * @param taskId the id to assign to the new task
     * @param clusterId the id of the cluster to split or merge
     * @param centroid the transformed centroid of the target cluster
     *
     * @return a new task without precomputed nearest clusters
     */
    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid) {
        return of(locator, accessInfo, taskId, clusterId, centroid, ImmutableList.of());
    }

    /**
     * Creates a new {@code SplitMergeTask} with precomputed nearest clusters, avoiding an additional
     * HNSW lookup at execution time.
     *
     * @param locator the locator providing access to primitives and configuration
     * @param accessInfo the access context for the current operation
     * @param taskId the id to assign to the new task
     * @param clusterId the id of the cluster to split or merge
     * @param centroid the transformed centroid of the target cluster
     * @param nearestClusters the precomputed nearest clusters of the target cluster
     *
     * @return a new task carrying the given precomputed nearest clusters
     */
    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid,
                             @Nonnull final List<ClusterReference> nearestClusters) {
        return new SplitMergeTask(locator, accessInfo, taskId, clusterId, centroid, nearestClusters);
    }

    /**
     * Returns the larger of the two core-cluster lists. Since the 2-to-3 classification is a superset
     * of the 1-to-2 classification, this is used to determine the maximum set of clusters whose vectors
     * need to be fetched (avoiding duplicate reads).
     *
     * @param classification1 the first ({@code 1 → 2}) cluster classification, or {@code null} if not applicable
     * @param classification2 the second ({@code 2 → 3}) cluster classification, or {@code null} if not applicable
     *
     * @return the larger of the two core-cluster lists
     */
    @Nonnull
    private static List<ClusterMetadataWithDistance> largestCoreClusters(@Nullable final ClusterClassification classification1,
                                                                              @Nullable final ClusterClassification classification2) {
        if (classification1 == null) {
            return Objects.requireNonNull(classification2).coreClusters();
        }
        if (classification2 == null) {
            return Objects.requireNonNull(classification1).coreClusters();
        }
        final List<ClusterMetadataWithDistance> inner1 = classification1.coreClusters();
        final List<ClusterMetadataWithDistance> inner2 = classification2.coreClusters();

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
     * @param classification the cluster classification context
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
    private static RepartitioningCandidate kMeans(@Nonnull final ClusterClassification classification,
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
        return new RepartitioningCandidate(classification, primaryVectorReferences,
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
        int currentIndex = 0;
        for (int c = 0; c < currentClusters.size(); c++) {
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
        final KMeans.Result<Transformed<RealVector>> kMeansResult = repartitioningCandidate.kMeansResult();
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
     *
     * @param estimator the distance estimator the evaluator uses to score partitions
     * @param currentK the current number of clusters in the nearest clusters
     * @param candidateK the proposed number of clusters after the transition
     *
     * @return the evaluator parameters tuned for the {@code currentK → candidateK} transition
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
     * Bundles a candidate repartitioning: the cluster classification context, the primary vectors participating
     * in the split, and the k-means result that defines the proposed new cluster boundaries.
     *
     * @param classification the core/neighboring cluster classification context of the repartitioning
     * @param primaryVectorReferences the primary vectors participating in the split
     * @param kMeansResult the k-means result defining the proposed new cluster boundaries
     */
    private record RepartitioningCandidate(@Nonnull ClusterClassification classification,
                                           @Nonnull List<VectorReference> primaryVectorReferences,
                                           @Nonnull @SuppressWarnings("checkstyle:MemberName") KMeans.Result<Transformed<RealVector>> kMeansResult) {
    }

    /**
     * The outcome of the vector assignment phase: tracks which clusters are newly created, provides
     * a map from cluster ID to metadata, the multimap of vector-to-cluster assignments, and updated
     * running standard deviations for distance tracking.
     *
     * @param newClusterIds the ids of the clusters newly created by the repartitioning
     * @param clusterIdMetadataMap a map from cluster id to the cluster's metadata
     * @param assignmentMultimap the assignments of vectors to clusters
     * @param updatedStandardDeviationsMap a map from cluster id to its updated running distance statistics
     */
    private record Repartitioning(@Nonnull Set<UUID> newClusterIds,
                                  @Nonnull Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                  @Nonnull ListMultimap<UUID, VectorReference> assignmentMultimap,
                                  @Nonnull Map<UUID, RunningStats> updatedStandardDeviationsMap) {
    }

    /**
     * Per-cluster counters produced by {@link #writeVectorReferences}, used by
     * {@link #writeClusterMetadataAndEnqueueTasks} to compute final cluster sizes.
     *
     * @param numPrimaryVectorsAdded a map from cluster id to the number of primary vectors written to it
     * @param numPrimaryUnderreplicatedVectorsAdded a map from cluster id to the number of primary underreplicated vectors written to it
     * @param numReplicatedVectorsAdded a map from cluster id to the number of replicated vectors written to it
     */
    private record VectorWriteCounters(@Nonnull Map<UUID, Integer> numPrimaryVectorsAdded,
                                       @Nonnull Map<UUID, Integer> numPrimaryUnderreplicatedVectorsAdded,
                                       @Nonnull Map<UUID, Integer> numReplicatedVectorsAdded) {
    }
}
