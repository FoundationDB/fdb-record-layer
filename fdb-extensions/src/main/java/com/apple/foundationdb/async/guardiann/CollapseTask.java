/*
 * CollapseTask.java
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
import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
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
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A deferred task that collapses the vectors of a single target cluster: it folds groups of equivalent vectors into
 * stored "collapsed" {@link VectorReference}s and records the absorbed vectors as collapsed-id mappings so they
 * remain resolvable at query time. It then applies the resulting writes and deletions to the cluster, updates the
 * cluster's metadata, and clears its {@link ClusterMetadata.State#COLLAPSE} state. The task is a no-op if the target
 * cluster no longer exists or is no longer marked for collapse.
 */
public class CollapseTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(CollapseTask.class);

    @Nonnull
    private final Transformed<RealVector> centroid;

    private CollapseTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
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
    public UUID getTargetClusterId() {
        return Iterables.getOnlyElement(getTargetClusterIds());
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        final Quantizer quantizer = getLocator().primitives().quantizer(getAccessInfo());
        final Transformed<RealVector> encodedVector = quantizer.encode(getCentroid());

        return Tuple.from(getKind().getCode(), getTargetClusterId(),
                StorageHelpers.bytesFromVector(encodedVector));
    }

    @Override
    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        super.writeDeferredTask(transaction);
        if (logger.isDebugEnabled()) {
            logger.debug("enqueuing COLLAPSE; taskId={}; targetClusterIds={}",
                    taskIdToString(getTaskId()), getTargetClusterIds());
        }
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.COLLAPSE;
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
                    if (!states.contains(ClusterMetadata.State.COLLAPSE)) {
                        return AsyncUtil.DONE;
                    }

                    return collapse(transaction, clusterMetadata, untransformedCentroid);
                }).thenAccept(ignored -> logSuccessful(logger));
    }

    @Nonnull
    private CompletableFuture<Void> collapse(@Nonnull final Transaction transaction,
                                             @Nonnull final ClusterMetadata targetClusterMetadata,
                                             @Nonnull final RealVector targetClusterCentroid) {
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final DistanceEstimator estimator = quantizer.estimator();
        final SplittableRandom random = RandomHelpers.random(getTaskId());

        // The target cluster metadata was already fetched in runTask and passed in, so wrap it directly
        // instead of re-reading it via fetchClusterMetadataWithDistance.
        final ClusterMetadataWithDistance targetClusterMetadataWithDistance =
                new ClusterMetadataWithDistance(targetClusterMetadata,
                        storageTransform.transform(targetClusterCentroid), 0.0d);
        return primitives.fetchInnerClusters(transaction,
                        ImmutableList.of(targetClusterMetadataWithDistance), storageTransform)
                .thenAccept(innerClusters -> {
                    final Cluster targetCluster = Iterables.getOnlyElement(innerClusters);
                    final CollapseAssignments collapseAssignments =
                            computeCollapseAssignments(random, estimator, targetClusterMetadataWithDistance,
                                    targetCluster.vectorReferences());
                    final TargetClusterDelta delta =
                            AbstractDeferredTask.computeTargetClusterDelta(targetCluster,
                                    collapseAssignments.assignments());
                    persistCollapse(transaction, targetClusterMetadataWithDistance,
                            collapseAssignments, delta, quantizer);
                });
    }

    @Nonnull
    private CollapseAssignments computeCollapseAssignments(@Nonnull final SplittableRandom random,
                                                           @Nonnull final DistanceEstimator estimator,
                                                           @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                                           @Nonnull final List<VectorReference> vectorReferences) {
        final Config config = getConfig();
        final Transformed<RealVector> targetClusterCentroid = targetClusterMetadataWithDistance.centroid();

        //
        // At this point clusterIdMetadataMap contains the target cluster and all the clusters
        // from the outer neighborhood.
        //
        final ImmutableSetMultimap<UUID, UUID> vectorReferenceToVectorSignatureMap =
                vectorReferenceToSignatureMap(vectorReferences);
        final ImmutableSetMultimap<UUID, UUID> vectorSignatureToVectorUuidMap =
                vectorReferenceToVectorSignatureMap.inverse();

        final Map<UUID, VectorReference> blackHoleMap = Maps.newHashMap();
        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy() || !vectorReference.isCollapsed()) {
                continue;
            }

            //
            // Only for primary copies and for already collapsed references.
            //
            final UUID signature =
                    Iterables.getOnlyElement(
                            vectorReferenceToVectorSignatureMap.get(vectorReference.id().getUuid()));

            blackHoleMap.putIfAbsent(signature, vectorReference);
        }


        final ImmutableList.Builder<VectorReference> targetAssignmentBuilder = ImmutableList.builder();
        final ImmutableListMultimap.Builder<UUID, VectorId> collapsedAssignmentsMapBuilder =
                ImmutableListMultimap.builder();
        final TopK<VectorReference> replicatedTopK =
                TopK.max(Comparator.comparing(VectorReference::replicationPriority)
                        .thenComparing(VectorReference::id),
                        config.replicatedClusterTarget());
        RunningStats standardDeviation = RunningStats.identity();

        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy()) {
                //
                // If this is a replica we may as well subsample it like reassign would normally do it.
                // Collapsing a cluster should also work in an identical way if we didn't do that here.
                //
                replicatedTopK.add(vectorReference);
                continue;
            }

            final double distanceToCentroid =
                    estimator.distance(vectorReference.vector(), targetClusterCentroid);

            if (!vectorReference.isCollapsed()) {
                //
                // Only for primary copies and for regular references
                //
                final UUID signature =
                        Iterables.getOnlyElement(
                                vectorReferenceToVectorSignatureMap.get(vectorReference.id().getUuid()));

                final ImmutableSet<UUID> identicalVectors = vectorSignatureToVectorUuidMap.get(signature);
                if (identicalVectors.size() > config.collapseMinDuplicates() && !blackHoleMap.containsKey(signature)) {
                    //
                    // This reference should be collapsed but is not collapsed (yet).
                    //
                    final UUID collapsedReferenceUuid = RandomHelpers.randomUuid(random, config.deterministicRandomness());
                    final VectorReference collapsedReference =
                            vectorReference.toCollapsed(signature, collapsedReferenceUuid);
                    blackHoleMap.put(signature, collapsedReference);
                    // account for the collapsed reference's distance exactly once in the running stats
                    standardDeviation = standardDeviation.add(distanceToCentroid);

                    // add the collapsed reference to the regular assignments data structure
                    targetAssignmentBuilder.add(collapsedReference);
                    // add the vector id to the collapsed set
                    collapsedAssignmentsMapBuilder.put(signature, vectorReference.id());
                    continue;
                }

                if (blackHoleMap.containsKey(signature)) {
                    collapsedAssignmentsMapBuilder.put(signature, vectorReference.id());
                    continue;
                }
            }

            standardDeviation = standardDeviation.add(distanceToCentroid);
            targetAssignmentBuilder.add(vectorReference);
        }

        targetAssignmentBuilder.addAll(replicatedTopK.toUnsortedList());

        if (logger.isTraceEnabled()) {
            logger.trace("collapsed num={}, mean={}, standard deviation={}, lowestReplicationPriority={}",
                    standardDeviation.numElements(),
                    standardDeviation.mean(),
                    standardDeviation.populationStandardDeviation(),
                    replicatedTopK.worstElement()
                            .map(VectorReference::replicationPriority).orElse(0.0d));
        }

        return new CollapseAssignments(targetAssignmentBuilder.build(), standardDeviation,
                collapsedAssignmentsMapBuilder.build());
    }

    /**
     * Persists the collapse results: applies the target cluster delta, writes collapsed vector IDs,
     * and updates the cluster metadata.
     *
     * @param transaction the transaction to write into
     * @param targetClusterMetadataWithDistance the cluster receiving the collapsed vectors, with its centroid distance
     * @param collapseAssignments the vectors to absorb and their collapsed-reference mappings
     * @param delta the vectors to write to and delete from the target cluster
     * @param quantizer the quantizer used to encode vectors before persisting
     */
    private void persistCollapse(@Nonnull final Transaction transaction,
                                 @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                 @Nonnull final CollapseAssignments collapseAssignments,
                                 @Nonnull final TargetClusterDelta delta,
                                 @Nonnull final Quantizer quantizer) {
        final CollapseCounters counters = countAssignments(collapseAssignments);
        persistTargetClusterDelta(transaction, quantizer, targetClusterMetadataWithDistance.clusterMetadata().id(), delta);
        writeCollapsedVectorIds(transaction, collapseAssignments);
        writeClusterMetadata(transaction, targetClusterMetadataWithDistance, collapseAssignments, counters, delta);
    }

    /**
     * Counts underreplicated and replicated vectors in the collapse assignments for use
     * in the metadata update.
     *
     * @param collapseAssignments the assignments whose vectors are tallied
     *
     * @return the primary-underreplicated and replicated vector counts for the assignments
     */
    @Nonnull
    private CollapseCounters countAssignments(@Nonnull final CollapseAssignments collapseAssignments) {
        int numPrimaryUnderreplicatedVectors = 0;
        int numReplicatedVectors = 0;

        for (final VectorReference vectorReference : collapseAssignments.assignments()) {
            if (vectorReference.isPrimaryCopy()) {
                if (vectorReference.isUnderreplicated()) {
                    numPrimaryUnderreplicatedVectors++;
                }
            } else {
                numReplicatedVectors++;
            }
        }

        return new CollapseCounters(numPrimaryUnderreplicatedVectors, numReplicatedVectors);
    }

    /**
     * Applies the target cluster delta by deleting removed vectors and writing changed/new vectors.
     *
     * @param transaction the transaction to write into
     * @param quantizer the quantizer used to encode written vectors
     * @param targetClusterId the id of the cluster the delta is applied to
     * @param delta the vectors to delete from and write to the target cluster
     */
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

    /**
     * Writes the collapsed vector ID mappings (signature → vector ID) for all vectors that
     * were absorbed into a collapsed reference.
     *
     * @param transaction the transaction to write into
     * @param collapseAssignments the assignments holding the collapsed vector-id mappings to persist
     */
    private void writeCollapsedVectorIds(@Nonnull final Transaction transaction,
                                         @Nonnull final CollapseAssignments collapseAssignments) {
        final Primitives primitives = getLocator().primitives();
        final ListMultimap<UUID, VectorId> collapsedAssignmentsMap = collapseAssignments.collapsedAssignmentsMap();
        for (final Map.Entry<UUID, VectorId> entry : collapsedAssignmentsMap.entries()) {
            primitives.writeCollapsedVectorId(transaction, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Updates the target cluster metadata with new vector counts, clears the
     * {@link ClusterMetadata.State#COLLAPSE} state, and logs the result.
     *
     * @param transaction the transaction to write into
     * @param targetClusterMetadataWithDistance the target cluster whose metadata is updated, with its centroid distance
     * @param collapseAssignments the collapse assignments supplying the updated standard deviation
     * @param counters the primary-underreplicated and replicated vector counts to store
     * @param delta the applied delta, used only for trace logging of deleted/written counts
     */
    private void writeClusterMetadata(@Nonnull final Transaction transaction,
                                      @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                      @Nonnull final CollapseAssignments collapseAssignments,
                                      @Nonnull final CollapseCounters counters,
                                      @Nonnull final TargetClusterDelta delta) {
        final Primitives primitives = getLocator().primitives();
        final ClusterMetadata targetClusterMetadata = targetClusterMetadataWithDistance.clusterMetadata();

        final RunningStats updatedStandardDeviation =
                collapseAssignments.updatedStandardDeviation();

        final EnumSet<ClusterMetadata.State> newStates = EnumSet.copyOf(targetClusterMetadata.states());
        newStates.remove(ClusterMetadata.State.COLLAPSE);

        // withNewVectors REPLACES the cluster's counts, so these are full totals over the collapse
        // assignments, not deltas.
        final ClusterMetadata newTargetClusterMetadata =
                targetClusterMetadata.withNewVectors(counters.numPrimaryUnderreplicatedVectors(),
                        counters.numReplicatedVectors(),
                        updatedStandardDeviation, newStates);
        primitives.writeClusterMetadata(transaction, newTargetClusterMetadata);

        if (logger.isTraceEnabled()) {
            logger.trace("collapse stats; old.numPrimary={}, new.numPrimary={}, old.numReplicated={}, " +
                    "new.numReplicated={}, numDeleted={}, numWritten={}",
                    targetClusterMetadata.getNumPrimaryVectors(),
                    newTargetClusterMetadata.getNumPrimaryVectors(), targetClusterMetadata.numReplicatedVectors(),
                    newTargetClusterMetadata.numReplicatedVectors(), delta.toDelete().size(), delta.toWrite().size());
        }
    }

    /**
     * The vector counts tallied from a set of collapse assignments, used to update the target cluster's metadata.
     *
     * @param numPrimaryUnderreplicatedVectors the number of primary underreplicated vectors among the assignments
     * @param numReplicatedVectors the number of replicated vectors among the assignments
     */
    private record CollapseCounters(int numPrimaryUnderreplicatedVectors,
                                    int numReplicatedVectors) {
    }

    @Nonnull
    static CollapseTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                   @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.COLLAPSE);
        final StorageTransform storageTransform = locator.primitives().storageTransform(accessInfo);

        final UUID targetClusterId = valueTuple.getUUID(1);
        final Transformed<RealVector> centroid = storageTransform.transform(
                StorageHelpers.vectorFromBytes(locator.getConfig(), valueTuple.getBytes(2)));

        return new CollapseTask(locator, accessInfo, keyTuple.getUUID(0), targetClusterId, centroid);
    }

    @Nonnull
    static CollapseTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                           @Nonnull final Transformed<RealVector> centroid) {
        return new CollapseTask(locator, accessInfo, taskId, clusterId, centroid);
    }

    static int maxDuplicateCount(final Map<UUID, Integer> countersMap) {
        int maximumCollapsiblePerDuplicate = 0;
        for (final Map.Entry<UUID, Integer> entry : countersMap.entrySet()) {
            final int counter = entry.getValue();
            if (counter > maximumCollapsiblePerDuplicate) {
                maximumCollapsiblePerDuplicate = counter;
            }
        }
        return maximumCollapsiblePerDuplicate;
    }

    @Nonnull
    static Map<UUID, Integer> collapsibleVectorsCountersMap(@Nonnull final List<VectorReference> vectorReferences) {
        final Map<UUID, Integer> countersMap = Maps.newHashMapWithExpectedSize(vectorReferences.size());
        for (final VectorReference vectorReference : vectorReferences) {
            if (vectorReference.isPrimaryCopy()) {
                incrementCounter(countersMap, StorageAdapter.signatureUuid(vectorReference.vector()));
            }
        }

        return countersMap;
    }

    @Nonnull
    static ImmutableSetMultimap<UUID, UUID> vectorReferenceToSignatureMap(@Nonnull final List<VectorReference> vectorReferences) {
        final ImmutableSetMultimap.Builder<UUID, UUID> resultMapBuilder = ImmutableSetMultimap.builder();
        for (final VectorReference vectorReference : vectorReferences) {
            if (vectorReference.isPrimaryCopy()) {
                resultMapBuilder.put(vectorReference.id().getUuid(), StorageAdapter.signatureUuid(vectorReference.vector()));
            }
        }

        return resultMapBuilder.build();
    }

    /**
     * The computed outcome of a collapse: the vector references the cluster retains, the updated distance
     * statistics, and the mapping recording which vector ids were absorbed into each collapsed reference.
     *
     * @param assignments the vector references the cluster retains after collapsing
     * @param updatedStandardDeviation the recomputed running statistics of member distances to the centroid
     * @param collapsedAssignmentsMap a multimap from a collapsed reference's id to the {@link VectorId}s absorbed into it
     */
    private record CollapseAssignments(@Nonnull List<VectorReference> assignments,
                                       @Nonnull RunningStats updatedStandardDeviation,
                                       @Nonnull ListMultimap<UUID, VectorId> collapsedAssignmentsMap) {
    }
}
