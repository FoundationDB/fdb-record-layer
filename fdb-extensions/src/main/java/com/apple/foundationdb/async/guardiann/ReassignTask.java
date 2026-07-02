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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
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
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A deferred task that repairs Guardiann's <em>replication/assignment</em> layer around a cluster after potentially
 * detrimental event has occurred — most often in the wake of a {@link SplitMergeTask} split or merge (whose new/changed
 * clusters are then named in {@link #getCauseClusterIds() causeClusterIds}), or when a cluster has accumulated too
 * many under-replicated primaries or replicated writes. A split/merge decides which cluster <em>owns</em> each vector
 * (its primary posting); The concern of a reassign is three-fold: First, the replica copies that coinhabit a cluster —
 * the replicas referencing neighboring clusters keep for recall need to be maintained in a way that only
 * the most ambiguous vectors are actively being replicated. Second, it will re-home a primary that has drifted to a
 * nearer cluster (marking it under-replicated so a later reassign repairs it in turn). Third, it will rereplicate
 * underreplicated vectors.
 *
 * <p>
 * The task targets a single cluster ({@link #getTargetClusterId()}) and carries its {@link #getCentroid() centroid},
 * the optional {@link #getCauseClusterIds() causeClusterIds}, and a precomputed list of {@link ClusterReference}
 * nearest clusters (or, when that list is empty, re-enqueues itself once they have been fetched). During execution
 * it recomputes, for each of the target's primaries, its nearest clusters over the target plus its
 * {@link Config#reassignNumNeighboringClusters()} neighbors, then rewrites the primary and replicated copies
 * accordingly.
 *
 * <p>
 * <b>Example.</b> Suppose a split turns an over-full cluster into two new clusters {@code C1} and {@code C2}.
 * The split has already written every vector's primary copy into {@code C1} or {@code C2} — those postings are done —
 * but {@code C1}/{@code C2} hold no replicas yet, and their neighbors still hold replica references from before the
 * split. To repair this a {@code REASSIGN} is enqueued for an affected cluster, say {@code T}, with
 * {@code causeClusterIds = {C1, C2}} and {@code nearestClusters} covering {@code T}'s surroundings (including
 * {@code C1} and {@code C2}). When {@code T}'s reassign runs, for each primary vector {@code v} that {@code T} owns:
 * <ul>
 *   <li><b>Primary (the posting):</b> {@code v}'s nearest cluster is recomputed over {@code T} and its neighbors. If
 *       {@code T} is still nearest, {@code T} keeps {@code v} as a {@link PrimaryCopy}. If a neighbor is now nearer
 *       (a vector sitting near the new {@code C1}/{@code C2} border), {@code v} is moved there as an
 *       {@link VectorReference#toPrimaryUnderreplicatedCopy() under-replicated primary} — no replicas are written for
 *       it now; the receiving cluster's own later reassign will propagate them. This is a bounded correction, not a
 *       re-partitioning.</li>
 *   <li><b>Replicas (the neighbors — the point of the task):</b> for a {@code v} that stays in {@code T}, a nearby
 *       candidate cluster {@code K} receives a {@link VectorReference#toReplicatedCopy(double) replicated copy} of
 *       {@code v} <em>only</em> when {@code v} is under-replicated <em>or</em> {@code K} is one of the
 *       {@code causeClusterIds}. So {@code T} seeds the freshly split {@code C1}/{@code C2} with replicas of its own
 *       vectors, and tops up any under-replicated primary — exactly the neighbor replicas the split left stale —
 *       while leaving unrelated neighbors alone. Candidates that are occluded by a closer already-chosen replica, or
 *       whose score is below {@link Config#replicationPriorityMin()}, are skipped.</li>
 * </ul>
 * Finally the kept replicas are truncated to {@link Config#replicatedClusterTarget()} (folding duplicates into a
 * single collapsed-area reference), {@code T}'s stored references are updated by delta, the new replicas are written
 * into their neighbor clusters, and — on the production path — follow-up split/reassign tasks are enqueued for any
 * neighbor that grew past its bounds.
 * <p>
 * Note that if {@link #getCauseClusterIds()} is empty, reassign will indiscriminately push primary vectors to all
 * neighboring clusters (not just the ones in the cause cluster id set).
 *
 */
public class ReassignTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ReassignTask.class);

    @Nonnull
    private final Transformed<RealVector> centroid;

    @Nonnull
    private final Set<UUID> causeClusterIds;
    @Nonnull
    private final List<ClusterReference> nearestClusters;

    private ReassignTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId, @Nonnull final UUID targetClusterId,
                         @Nonnull final Transformed<RealVector> centroid,
                         @Nonnull final Set<UUID> causeClusterIds,
                         @Nonnull final List<ClusterReference> nearestClusters) {
        super(locator, accessInfo, taskId, ImmutableSet.of(targetClusterId));
        this.centroid = centroid;
        this.causeClusterIds = ImmutableSet.copyOf(causeClusterIds);
        this.nearestClusters = ImmutableList.copyOf(nearestClusters);
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
    private List<ClusterReference> getNearestClusters() {
        return nearestClusters;
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

        final ImmutableList.Builder<Object> nearestClustersTuplesBuilder = ImmutableList.builder();
        for (final ClusterReference clusterMetadataWithDistance : getNearestClusters()) {
            nearestClustersTuplesBuilder.add(
                    StorageAdapter.valueTupleFromClusterReference(quantizer,
                            clusterMetadataWithDistance));
        }

        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                StorageHelpers.bytesFromVector(encodedVector),
                StorageAdapter.tupleFromClusterIds(getCauseClusterIds()),
                Tuple.fromItems(nearestClustersTuplesBuilder.build()));
    }

    @Override
    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        super.writeDeferredTask(transaction);
        if (logger.isDebugEnabled()) {
            logger.debug("enqueuing REASSIGN; taskId={}; clusterId={}",
                    AbstractDeferredTask.taskIdToString(getTaskId()), getTargetClusterId());
        }
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.REASSIGN;
    }

    @Nonnull
    @Override
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

                    return reassign(transaction, clusterMetadata, untransformedCentroid, true);
                }).thenAccept(ignored -> logSuccessful(logger));
    }

    /**
     * Reassigns the target cluster's vectors across its nearest clusters — re-homing each primary to its nearest
     * cluster and re-establishing replication. When {@code enqueueFollowUpTasks} is {@code true} (the production
     * path via {@link #runTask}) it may re-enqueue itself to fetch missing nearest clusters and may enqueue
     * split/reassign follow-ups for outer clusters that received vectors. When {@code false} (used by controlled
     * tests that drive reassign directly) it enqueues nothing: it requires precomputed nearest clusters and writes
     * outer-cluster metadata updates without enqueuing any follow-up tasks.
     *
     * @param transaction the transaction
     * @param targetClusterMetadata the metadata of the cluster being reassigned
     * @param targetClusterCentroid the cluster's centroid in client (untransformed) space
     * @param enqueueFollowUpTasks whether to enqueue follow-up deferred tasks (always {@code true} in production)
     *
     * @return a future that completes when the reassignment has been persisted
     */
    @Nonnull
    CompletableFuture<Void> reassign(@Nonnull final Transaction transaction,
                                     @Nonnull final ClusterMetadata targetClusterMetadata,
                                     @Nonnull final RealVector targetClusterCentroid,
                                     final boolean enqueueFollowUpTasks) {
        final SplittableRandom random = RandomHelpers.random(getTaskId());
        final Config config = getConfig();
        final Executor executor = getLocator().getExecutor();
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final DistanceEstimator estimator = quantizer.estimator();

        final int numCoreClusters = 1; // reassign always dissolves exactly the single target cluster
        final int numNeighboringClusters = config.reassignNumNeighboringClusters();
        final int numNearestClusters = numCoreClusters + numNeighboringClusters;

        final CompletableFuture<Void> reenqueue = reenqueueWithFetchedNearestClustersIfEmpty(
                transaction, targetClusterMetadata, targetClusterCentroid, random, storageTransform,
                numNearestClusters, enqueueFollowUpTasks);
        if (reenqueue != null) {
            return reenqueue;
        }
        final List<ClusterReference> nearestClusters = getNearestClusters();

        return MoreAsyncUtil.forEach(nearestClusters,
                        clusterIdAndCentroid -> primitives.fetchClusterMetadataWithDistance(transaction,
                                clusterIdAndCentroid.clusterId(), clusterIdAndCentroid.centroid(), 0.0d),
                        config.reassignConcurrency(), executor)
                .thenCompose(nearestClusterMetadataWithDistances -> {

                    final ClusterClassification classification =
                            classifyClusters(nearestClusterMetadataWithDistances,
                                    targetClusterMetadata, getCentroid(), numCoreClusters, numNeighboringClusters);

                    final List<ClusterMetadataWithDistance> coreClusters = classification.coreClusters();
                    final List<ClusterMetadataWithDistance> neighboringClusters = classification.neighboringClusters();
                    Verify.verify(coreClusters.size() == 1,
                            "reassign expects exactly one inner (target) cluster, got %s", coreClusters.size());

                    //
                    // At this point coreClusters is the single target cluster whose vectors will be
                    // reassigned; neighboringClusters holds the candidate clusters those vectors may move or
                    // replicate to.
                    //
                    return primitives.fetchCoreClusters(transaction, coreClusters, storageTransform)
                            .thenCompose(innerClusters -> primitives.cleanUpVectorReferences(transaction,
                                            innerClusters, false)
                                    .thenCompose(cleanedUpVectorReferences -> {
                                        final Reassignment partialReassignment =
                                                reassignVectorReferences(estimator, Iterables.getOnlyElement(coreClusters),
                                                        neighboringClusters, cleanedUpVectorReferences);
                                        final Cluster targetCluster = Iterables.getOnlyElement(innerClusters);
                                        return foldCollapsedReplicas(transaction, partialReassignment,
                                                        targetClusterMetadata.id(), config.replicatedClusterTarget())
                                                .thenAccept(reassignment -> {
                                                    final TargetClusterDelta delta =
                                                            computeTargetClusterDelta(targetCluster, reassignment,
                                                                    targetClusterMetadata.id());
                                                    persistReassignment(transaction, random, targetClusterMetadata,
                                                            reassignment, delta, quantizer, enqueueFollowUpTasks);
                                                });
                                    }));
                });
    }

    /**
     * If the nearest clusters have not been precomputed yet, fetches them and re-enqueues this work as a high-priority
     * REASSIGN carrying those nearest clusters, returning the (non-null) re-enqueue future so the caller can return it as
     * an early-out. If precomputed nearest clusters are already present, returns {@code null} and the caller proceeds.
     * Requires {@code enqueueFollowUpTasks}: the test-only direct-drive path ({@code false}) must supply
     * precomputed nearest clusters rather than re-enqueue.
     */
    @Nullable
    private CompletableFuture<Void> reenqueueWithFetchedNearestClustersIfEmpty(@Nonnull final Transaction transaction,
                                                                            @Nonnull final ClusterMetadata targetClusterMetadata,
                                                                            @Nonnull final RealVector targetClusterCentroid,
                                                                            @Nonnull final SplittableRandom random,
                                                                            @Nonnull final StorageTransform storageTransform,
                                                                            final int numNearestClusters,
                                                                            final boolean enqueueFollowUpTasks) {
        if (!getNearestClusters().isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("using precomputed nearest clusters; taskId={}; numNearestClusters={}",
                        taskIdToString(getTaskId()), getNearestClusters().size());
            }
            return null;
        }
        Verify.verify(enqueueFollowUpTasks,
                "reassign with enqueueFollowUpTasks=false requires precomputed (non-empty) nearest clusters");
        return primitives().fetchNearestClusterMetadata(transaction, targetClusterMetadata,
                        targetClusterCentroid, storageTransform, numNearestClusters)
                .thenAccept(fetchedNearestClusters -> {
                    final ReassignTask reassignTask = withHighPriorityAndNearestClusters(random,
                            ClusterReference.fromClusterMetadataAndDistances(fetchedNearestClusters));
                    reassignTask.writeDeferredTask(transaction);
                    if (logger.isDebugEnabled()) {
                        logger.debug("enqueuing high priority REASSIGN due to refetch of nearest clusters; taskId={}; numNearestClusters={}",
                                AbstractDeferredTask.taskIdToString(reassignTask.getTaskId()),
                                reassignTask.getNearestClusters().size());
                    }
                });
    }

    @Nonnull
    private Reassignment reassignVectorReferences(@Nonnull final DistanceEstimator estimator,
                                                  @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                                  @Nonnull final List<ClusterMetadataWithDistance> neighboringClusters,
                                                  @Nonnull final List<VectorReference> vectorReferences) {
        final ImmutableMap.Builder<UUID, ClusterMetadataWithDistance> clusterIdMetadataMapBuilder =
                ImmutableMap.builder();

        final UUID targetClusterId = targetClusterMetadataWithDistance.clusterMetadata().id();
        clusterIdMetadataMapBuilder.put(targetClusterId, targetClusterMetadataWithDistance);
        for (final ClusterMetadataWithDistance clusterMetadataWithDistance : neighboringClusters) {
            clusterIdMetadataMapBuilder.put(clusterMetadataWithDistance.clusterMetadata().id(),
                    clusterMetadataWithDistance);
        }
        final ImmutableMap<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap =
                clusterIdMetadataMapBuilder.build();

        //
        // At this point clusterIdMetadataMap contains the new clusters after the split and all the clusters
        // from the neighboring clusters.
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
                computeNearestClusters(estimator, vectorReferences, clusterIdMetadataMap.values());
        mergeStandardDeviationUpdates(standardDeviationsMap,
                nearestClustersResult.standardDeviationUpdates());
        final ImmutableListMultimap<UUID, ClusterMetadataWithDistance> invertedAssignmentsMap =
                nearestClustersResult.invertedAssignments();

        final ImmutableListMultimap.Builder<UUID, VectorReference> assignmentBuilder =
                ImmutableListMultimap.builder();
        //
        // Collect the target cluster's replicated references into a priority queue ordered by replication priority,
        // highest first. It is intentionally unbounded; the top-k cutoff plus the collapse-fold/dedup happens later,
        // asynchronously, in foldCollapsedReplicas (it issues collapsed-store reads, which this synchronous method
        // must not). We return the drained, ordered (but unprocessed) references on the Reassignment.
        //
        final PriorityQueue<VectorReference> replicaQueue =
                new PriorityQueue<>(Comparator.comparing(VectorReference::replicationPriority)
                        .thenComparing(VectorReference::id)
                        .reversed());

        ReplicationStats stats = ReplicationStats.identity();
        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy()) {
                replicaQueue.add(vectorReference);
                continue;
            }

            final ImmutableList<ClusterMetadataWithDistance> nearestClusters =
                    Objects.requireNonNull(invertedAssignmentsMap.get(vectorReference.id().uuid()));
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

            final ReplicaSelection selection = selectReplicationAssignments(estimator, vectorReference,
                    distanceToPrimaryCentroid, replicationCandidates, causeClusterIds, targetClusterId,
                    standardDeviationsMap);
            assignmentBuilder.putAll(selection.replicasByCluster());
            stats = stats.combine(selection.stats());
        }

        if (logger.isTraceEnabled()) {
            logger.trace("replication priority num={}. mean={}, standard deviation={}, numReplicated={}, numOccluded={}",
                    stats.replicationPriorityStandardDeviation().numElements(),
                    stats.replicationPriorityStandardDeviation().mean(),
                    stats.replicationPriorityStandardDeviation().populationStandardDeviation(),
                    stats.numReplicated(), stats.numOccluded());
        }

        //
        // Drain the queue into an ordered list (highest priority first). The collapse-fold/dedup and the final
        // top-k cutoff are applied by foldCollapsedReplicas, which the caller runs before persisting.
        //
        final ImmutableList.Builder<VectorReference> orderedReplicatedReferencesBuilder = ImmutableList.builder();
        while (!replicaQueue.isEmpty()) {
            orderedReplicatedReferencesBuilder.add(replicaQueue.poll());
        }

        return new Reassignment(clusterIdMetadataMap, assignmentBuilder.build(), standardDeviationsMap,
                orderedReplicatedReferencesBuilder.build());
    }

    /**
     * Folds the target cluster's collapsed replicas and applies the top-k cutoff. Walks {@code reassignment}'s
     * ordered (highest-priority-first) replicated references and, for each one whose primary has since been folded
     * into a collapsed set (a per-replica point lookup in the collapsed store), replaces it with a single reference
     * to the collapsed area; a later replica that resolves to the same collapsed area is dropped. Because the list is
     * in descending priority, a collapsed area inherits the highest priority among its members, and duplicates are
     * recognised by their (synchronously computed) signature without an extra read. Walking stops once {@code k}
     * references are kept, so the low-priority tail is never read. Returns a {@link Reassignment} whose
     * {@code assignmentMultimap} additionally holds the kept references under {@code targetClusterId}.
     *
     * @param readTransaction the read transaction
     * @param reassignment the synchronous reassignment, carrying the ordered, unprocessed replicated references
     * @param targetClusterId the cluster the kept references are assigned to
     * @param k the maximum number of references to keep
     * @return a future of the finalized reassignment
     */
    @Nonnull
    private CompletableFuture<Reassignment> foldCollapsedReplicas(@Nonnull final ReadTransaction readTransaction,
                                                                  @Nonnull final Reassignment reassignment,
                                                                  @Nonnull final UUID targetClusterId,
                                                                  final int k) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final List<VectorReference> orderedReplicatedReferences = reassignment.orderedReplicatedReferences();
        final List<VectorReference> keptReplicas = Lists.newArrayList();
        final Map<UUID, VectorReference> collapsedReplicasBySignature = Maps.newHashMap();

        return MoreAsyncUtil.<Void>forLoop(0, null,
                        i -> i < orderedReplicatedReferences.size() && keptReplicas.size() < k,
                        i -> i + 1,
                        (i, ignored) -> {
                            final VectorReference replica = orderedReplicatedReferences.get(i);
                            final UUID signature = StorageAdapter.signatureUuid(replica.vector());
                            if (collapsedReplicasBySignature.containsKey(signature)) {
                                // already represented by a collapsed-area reference we kept: this is a duplicate, drop it
                                return AsyncUtil.DONE;
                            }
                            return primitives.fetchCollapsedVectorId(readTransaction, signature,
                                            replica.id().primaryKey())
                                    .thenAccept(storedVectorId -> {
                                        if (storedVectorId == null) {
                                            keptReplicas.add(replica);
                                        } else {
                                            // The replica's primary has since been folded into a collapsed set.
                                            // cleanUpVectorReferences has already removed stale references, so the
                                            // stored id must equal this replica's id.
                                            Verify.verify(storedVectorId.equals(replica.id()),
                                                    "collapsed-store entry %s does not match replica %s",
                                                    storedVectorId, replica.id());
                                            final VectorReference collapsedReplica =
                                                    replica.toCollapsed(signature, signature);
                                            collapsedReplicasBySignature.put(signature, collapsedReplica);
                                            keptReplicas.add(collapsedReplica);
                                        }
                                    });
                        },
                        executor)
                .thenApply(ignored -> {
                    final ImmutableListMultimap<UUID, VectorReference> assignmentMultimap =
                            ImmutableListMultimap.<UUID, VectorReference>builder()
                                    .putAll(reassignment.assignmentMultimap())
                                    .putAll(targetClusterId, keptReplicas)
                                    .build();
                    return new Reassignment(reassignment.clusterIdMetadataMap(), assignmentMultimap,
                            reassignment.updatedStandardDeviationsMap(), ImmutableList.of());
                });
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
                                                          @Nonnull final Set<UUID> causeClusterIds,
                                                          @Nonnull final UUID targetClusterId,
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

                final VectorReference newVectorReference =
                        vectorReference.toReplicatedCopy(replicationPriority);
                // A replication candidate can never be the target cluster: we only reach this loop when the
                // target is the vector's nearest cluster (index 0 of nearestClusters), the candidates are
                // nearestClusters.subList(1, ...) which excludes index 0, and each cluster appears at most
                // once in that distance-sorted list.
                Verify.verify(!targetClusterId.equals(replicationCandidateClusterMetadata.id()),
                        "a replication candidate must never be the target cluster");
                replicasByCluster.put(
                        replicationCandidateClusterMetadata.id(),
                        newVectorReference);
                selectedReplicationClusters.add(replicationCandidate);
                numReplicated++;
            }
        }
        return new ReplicaSelection(replicasByCluster.build(),
                new ReplicationStats(replicationPriorityStandardDeviation, numReplicated, numOccluded));
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
                                     @Nonnull final Quantizer quantizer,
                                     final boolean enqueueFollowUpTasks) {
        final WriteCounters counters = countAssignments(targetClusterMetadata, reassignment);
        writeOuterClusterVectors(transaction, quantizer, targetClusterMetadata, reassignment);
        persistTargetClusterDelta(transaction, quantizer, targetClusterMetadata.id(), delta);
        writeClusterMetadata(transaction, random, targetClusterMetadata, reassignment, counters, delta,
                enqueueFollowUpTasks);
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
                                      @Nonnull final TargetClusterDelta delta,
                                      final boolean enqueueFollowUpTasks) {
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
            } else if (enqueueFollowUpTasks) {
                primitives.updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe(transaction, random, clusterMetadata,
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
            } else if (numPrimaryVectorsAdded != 0 || numReplicatedVectorsAdded != 0) {
                // Controlled-reassign mode (enqueueFollowUpTasks=false): update the outer cluster's metadata but
                // enqueue no split/reassign follow-up. Mirrors the no-enqueue tail of
                // Primitives.updateClusterMetadataAndEnqueueReassignTaskMaybe.
                primitives.writeClusterMetadata(transaction,
                        clusterMetadata.withAdditionalVectors(numPrimaryUnderreplicatedVectorsAdded,
                                numReplicatedVectorsAdded, updatedStandardDeviation));
            }
        }

        if (logger.isTraceEnabled()) {
            Objects.requireNonNull(newTargetClusterMetadata);
            logger.trace("reassign stats; old.numPrimary={}, new.numPrimary={}, old.numReplicated={}, " +
                    "new.numReplicated={}, numDeleted={}, numUpdated={}, numPrimaryPushedOut={}, " +
                    "numReplicatedPushedOut={}", targetClusterMetadata.getNumPrimaryVectors(),
                    newTargetClusterMetadata.getNumPrimaryVectors(), targetClusterMetadata.numReplicatedVectors(),
                    newTargetClusterMetadata.numReplicatedVectors(), delta.toDelete().size(), delta.toWrite().size(),
                    counters.numPrimaryPushedOut(), counters.numReplicatedPushedOut());
        }
    }

    @Nonnull
    private ReassignTask withHighPriorityAndNearestClusters(@Nonnull final SplittableRandom random,
                                                         @Nonnull final List<ClusterReference> nearestClusters) {
        return ReassignTask.of(getLocator(), getAccessInfo(),
                randomHighPriorityTaskId(random, getConfig().deterministicRandomness()), getTargetClusterId(),
                getCentroid(), getCauseClusterIds(), nearestClusters);
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
        final ImmutableList.Builder<ClusterReference> nearestClustersBuilder = ImmutableList.builder();
        final Tuple nearestClustersTuple = valueTuple.getNestedTuple(4);
        for (int i = 0; i < nearestClustersTuple.size(); i ++) {
            final Tuple clusterMetadataWithDistanceTuple = nearestClustersTuple.getNestedTuple(i);
            nearestClustersBuilder.add(StorageAdapter.clusterReferenceFromTuple(locator.getConfig(),
                    storageTransform, clusterMetadataWithDistanceTuple));
        }

        return new ReassignTask(locator, accessInfo, keyTuple.getUUID(0), targetClusterId, centroid,
                causeClusterIds, nearestClustersBuilder.build());
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
                           @Nonnull final List<ClusterReference> nearestClusters) {
        return new ReassignTask(locator, accessInfo, taskId, clusterId, centroid, causeClusterIds, nearestClusters);
    }

    /**
     * The outcome of computing a reassignment: the metadata of the clusters involved, the new vector-to-cluster
     * assignments, and the updated running distance statistics per cluster.
     *
     * @param clusterIdMetadataMap a map from cluster id to that cluster's metadata (with distance)
     * @param assignmentMultimap the new assignments of vectors to clusters
     * @param updatedStandardDeviationsMap a map from cluster id to its updated running distance statistics
     * @param orderedReplicatedReferences the target cluster's replicated references, ordered by descending
     *        replication priority but not yet top-k-truncated, deduplicated, or collapse-folded; consumed by
     *        {@link #foldCollapsedReplicas}
     */
    private record Reassignment(@Nonnull Map<UUID, ClusterMetadataWithDistance> clusterIdMetadataMap,
                                @Nonnull ListMultimap<UUID, VectorReference> assignmentMultimap,
                                @Nonnull Map<UUID, RunningStats> updatedStandardDeviationsMap,
                                @Nonnull List<VectorReference> orderedReplicatedReferences) {
    }

    /**
     * Per-cluster counters produced while writing a reassignment: the number of primary, primary-underreplicated,
     * and replicated vectors added to each cluster, together with the totals pushed out of the target cluster.
     *
     * @param numPrimaryVectorsAdded a map from cluster id to the number of primary vectors written to it
     * @param numPrimaryUnderreplicatedVectorsAdded a map from cluster id to the number of primary underreplicated vectors written to it
     * @param numReplicatedVectorsAdded a map from cluster id to the number of replicated vectors written to it
     * @param numPrimaryPushedOut the number of primary vectors moved out of the target cluster
     * @param numReplicatedPushedOut the number of replicated vectors moved out of the target cluster
     */
    private record WriteCounters(@Nonnull Map<UUID, Integer> numPrimaryVectorsAdded,
                                 @Nonnull Map<UUID, Integer> numPrimaryUnderreplicatedVectorsAdded,
                                 @Nonnull Map<UUID, Integer> numReplicatedVectorsAdded,
                                 int numPrimaryPushedOut,
                                 int numReplicatedPushedOut) {
    }
}
