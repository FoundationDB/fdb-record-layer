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
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
        if (logger.isInfoEnabled()) {
            logger.info("enqueuing COLLAPSE; taskId={}; targetClusterIds={}",
                    taskIdToString(getTaskId()), getTargetClusterIds());
        }
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.COLLAPSE;
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
        final SplittableRandom random = RandomHelpers.random(targetClusterMetadata.getId());
        final Config config = getConfig();
        final Executor executor = getLocator().getExecutor();
        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

        return primitives.fetchClusterMetadataWithDistance(transaction,
                                targetClusterMetadata.getId(),
                        storageTransform.transform(targetClusterCentroid), 0.0d)
                .thenCompose(targetClusterMetadataWithDistance -> {
                    return primitives.fetchInnerClusters(transaction, ImmutableList.of(targetClusterMetadataWithDistance), storageTransform)
                            .thenAccept(innerClusters -> {
                                final Cluster targetCluster = Iterables.getOnlyElement(innerClusters);
                                final CollapseResult collapseResult =
                                        collapseVectorReferences(estimator, targetClusterMetadataWithDistance,
                                                targetCluster.getVectorReferences());
                                final List<VectorReference> targetClusterAssignedVectors =
                                        collapseResult.getAssignments();
                                final ImmutableMap.Builder<Tuple, VectorReference> targetClusterAssignedVectorsAsMapBuilder = ImmutableMap.builder();
                                for (final VectorReference targetClusterAssignedVector : targetClusterAssignedVectors) {
                                    targetClusterAssignedVectorsAsMapBuilder.put(
                                            targetClusterAssignedVector.getId().getPrimaryKey(),
                                            targetClusterAssignedVector);
                                }
                                final ImmutableMap<Tuple, VectorReference> targetClusterAssignedVectorsAsMap =
                                        targetClusterAssignedVectorsAsMapBuilder.build();

                                final ImmutableList.Builder<Tuple> deleteTargetClusterAssignedVectorsBuilder =
                                        ImmutableList.builder();
                                final ImmutableList.Builder<VectorReference> writeTargetClusterAssignedVectorsBuilder =
                                        ImmutableList.builder();
                                final ImmutableMap.Builder<Tuple, VectorReference> primaryKeyToVectorReferencesMapBuilder = ImmutableMap.builder();

                                for (final VectorReference vectorReference : targetCluster.getVectorReferences()) {
                                    primaryKeyToVectorReferencesMapBuilder.put(vectorReference.getId().getPrimaryKey(), vectorReference);
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

                                final ImmutableMap<Tuple, VectorReference> primaryKeyToVectorReferencesMap =
                                        primaryKeyToVectorReferencesMapBuilder.build();

                                for (final Map.Entry<Tuple, VectorReference> entry : targetClusterAssignedVectorsAsMap.entrySet()) {
                                    if (!primaryKeyToVectorReferencesMap.containsKey(entry.getKey())) {
                                        writeTargetClusterAssignedVectorsBuilder.add(entry.getValue());
                                    }
                                }

                                final ImmutableList<VectorReference> writeTargetClusterAssignedVectors =
                                        writeTargetClusterAssignedVectorsBuilder.build();

                                final ImmutableList<Tuple> deleteTargetClusterAssignedVectors =
                                        deleteTargetClusterAssignedVectorsBuilder.build();

                                updateAssignments(transaction, random, targetClusterMetadataWithDistance,
                                        collapseResult, writeTargetClusterAssignedVectors,
                                        deleteTargetClusterAssignedVectors, quantizer);
                            });
                });
    }

    @Nonnull
    private CollapseResult collapseVectorReferences(@Nonnull final Estimator estimator,
                                                    @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                                    @Nonnull final List<VectorReference> vectorReferences) {
        final Config config = getConfig();
        final Transformed<RealVector> targetClusterCentroid = targetClusterMetadataWithDistance.getCentroid();

        //
        // At this point clusterIdMetadataMap contains the target cluster and all the clusters
        // from the outer neighborhood.
        //
        final ImmutableSetMultimap<UUID, UUID> vectorReferenceToVectorSignatureMap =
                vectorReferenceToVectorSignatureMap(vectorReferences);
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
                            vectorReferenceToVectorSignatureMap.get(vectorReference.getId().getUuid()));

            blackHoleMap.putIfAbsent(signature, vectorReference);
        }


        final ImmutableList.Builder<VectorReference> targetAssignmentBuilder = ImmutableList.builder();
        final ImmutableListMultimap.Builder<UUID, VectorId> collapsedAssignmentsMapBuilder =
                ImmutableListMultimap.builder();
        final TopK<VectorReference> replicatedTopK =
                new TopK<>(Comparator.comparing(VectorReference::getReplicationPriority),
                        config.getReplicatedClusterTarget());
        RunningStandardDeviation standardDeviation = RunningStandardDeviation.identity();

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
                    estimator.distance(vectorReference.getVector(), targetClusterCentroid);

            if (!vectorReference.isCollapsed()) {
                //
                // Only for primary copies and for regular references
                //
                final UUID signature =
                        Iterables.getOnlyElement(
                                vectorReferenceToVectorSignatureMap.get(vectorReference.getId().getUuid()));

                final ImmutableSet<UUID> identicalVectors = vectorSignatureToVectorUuidMap.get(signature);
                if (identicalVectors.size() > 100 && !blackHoleMap.containsKey(signature)) {
                    //
                    // This reference should be collapsed but is not collapsed (yet).
                    //
                    final UUID collapsedReferenceUuid = RandomHelpers.randomUuid(config.isDeterministicRandomness());
                    final VectorReference collapsedReference =
                            vectorReference.toCollapsed(signature, collapsedReferenceUuid);
                    blackHoleMap.put(signature, collapsedReference);
                    // add the distance exactly one for the collapsed set
                    standardDeviation = standardDeviation.add(distanceToCentroid);

                    // add the collapsed reference to the regular assignments data structure
                    targetAssignmentBuilder.add(collapsedReference);
                    // add the vector id to the collapsed set
                    collapsedAssignmentsMapBuilder.put(signature, vectorReference.getId());
                    continue;
                }

                if (blackHoleMap.containsKey(signature)) {
                    collapsedAssignmentsMapBuilder.put(signature, vectorReference.getId());
                    continue;
                }
            }

            standardDeviation = standardDeviation.add(distanceToCentroid);
            targetAssignmentBuilder.add(vectorReference);
        }

        targetAssignmentBuilder.addAll(replicatedTopK.toUnsortedList());

        if (logger.isInfoEnabled()) {
            logger.info("collapsed num={}, mean={}, standard deviation={}, lowestReplicationPriority={}",
                    standardDeviation.getNumElements(),
                    standardDeviation.mean(),
                    standardDeviation.populationStandardDeviation(),
                    replicatedTopK.worstElement()
                            .map(VectorReference::getReplicationPriority).orElse(0.0d));
        }

        return new CollapseResult(targetAssignmentBuilder.build(), standardDeviation,
                collapsedAssignmentsMapBuilder.build());
    }

    private void updateAssignments(@Nonnull final Transaction transaction,
                                   @Nonnull final SplittableRandom random,
                                   @Nonnull final ClusterMetadataWithDistance targetClusterMetadataWithDistance,
                                   @Nonnull final CollapseResult collapseResult,
                                   @Nonnull final ImmutableList<VectorReference> writeTargetClusterAssignedVectors,
                                   @Nonnull final ImmutableList<Tuple> deleteTargetClusterAssignedVectors,
                                   @Nonnull final Quantizer quantizer) {
        final Primitives primitives = getLocator().primitives();

        final List<VectorReference> assignments = collapseResult.getAssignments();

        int numPrimaryUnderreplicatedVectorsAdded = 0;
        int numReplicatedVectorsAdded = 0;

        // write all vector references outside the target cluster
        for (final var vectorReference : assignments) {
            if (vectorReference.isPrimaryCopy()) {
                if (vectorReference.isUnderreplicated()) {
                    numPrimaryUnderreplicatedVectorsAdded++;
                }
            } else {
                numReplicatedVectorsAdded++;
            }
        }

        final ClusterMetadata targetClusterMetadata = targetClusterMetadataWithDistance.getClusterMetadata();

        // delete vectors that have been assigned out
        // write updates
        int numDeleted = 0;
        for (final Tuple primaryKey : deleteTargetClusterAssignedVectors) {
            primitives.deleteVectorReference(transaction, targetClusterMetadata.getId(), primaryKey);
            numDeleted++;
        }

        // write updated vector references
        int numWritten = 0;
        for (final VectorReference vectorReference : writeTargetClusterAssignedVectors) {
            primitives.writeVectorReference(transaction, quantizer, targetClusterMetadata.getId(), vectorReference);
            numWritten++;
        }

        //
        // Write all collapsed assignments.
        //
        final ListMultimap<UUID, VectorId> collapsedAssignmentsMap = collapseResult.getCollapsedAssignmentsMap();
        for (final Map.Entry<UUID, VectorId> entry : collapsedAssignmentsMap.entries()) {
            primitives.writeCollapsedVectorId(transaction, entry.getKey(), entry.getValue());
        }

        final RunningStandardDeviation updatedStandardDeviation =
                collapseResult.getUpdatedStandardDeviation();

        final EnumSet<ClusterMetadata.State> newStates = EnumSet.copyOf(targetClusterMetadata.getStates());
        newStates.remove(ClusterMetadata.State.COLLAPSE);

        final ClusterMetadata newTargetClusterMetadata =
                targetClusterMetadata.withNewVectors(numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
                        updatedStandardDeviation, newStates);
        primitives.writeClusterMetadata(transaction, newTargetClusterMetadata);

        // log everything
        if (logger.isInfoEnabled()) {
            Objects.requireNonNull(newTargetClusterMetadata);
            logger.info("collapse stats; old.numPrimary={}, new.numPrimary={}, old.numReplicated={}, " +
                    "new.numReplicated={}, numDeleted={}, numWritten={}",
                    targetClusterMetadata.getNumPrimaryVectors(),
                    newTargetClusterMetadata.getNumPrimaryVectors(), targetClusterMetadata.getNumReplicatedVectors(),
                    newTargetClusterMetadata.getNumReplicatedVectors(), numDeleted, numWritten);
        }
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

    static int maximumNumberCollapsibleVectorsPerDuplicate(final Map<UUID, Integer> countersMap) {
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
                incrementCounter(countersMap, uuidFromBytes(keyOf(vectorReference.getVector())));
            }
        }

        return countersMap;
    }

    @Nonnull
    static ImmutableSetMultimap<UUID, UUID> vectorReferenceToVectorSignatureMap(@Nonnull final List<VectorReference> vectorReferences) {
        final ImmutableSetMultimap.Builder<UUID, UUID> resultMapBuilder = ImmutableSetMultimap.builder();
        for (final VectorReference vectorReference : vectorReferences) {
            if (vectorReference.isPrimaryCopy()) {
                resultMapBuilder.put(vectorReference.getId().getUuid(), uuidFromBytes(keyOf(vectorReference.getVector())));
            }
        }

        return resultMapBuilder.build();
    }

    @Nonnull
    private static UUID uuidFromBytes(@Nonnull final byte[] keyAsBytes) {
        if (keyAsBytes.length != 16) {
            throw new IllegalArgumentException("Expected 16 bytes, got " + keyAsBytes.length);
        }
        final long hi = readLongBigEndian(keyAsBytes, 0);
        final long lo = readLongBigEndian(keyAsBytes, 8);
        return new UUID(hi, lo);
    }

    private static long readLongBigEndian(byte[] b, int off) {
        return ((long) (b[off]     & 0xff) << 56) |
                ((long) (b[off + 1] & 0xff) << 48) |
                ((long) (b[off + 2] & 0xff) << 40) |
                ((long) (b[off + 3] & 0xff) << 32) |
                ((long) (b[off + 4] & 0xff) << 24) |
                ((long) (b[off + 5] & 0xff) << 16) |
                ((long) (b[off + 6] & 0xff) <<  8) |
                ((long) (b[off + 7] & 0xff));
    }

    @Nonnull
    private static byte[] keyOf(@Nonnull final Transformed<RealVector> vector) {
        byte[] full = sha256(vector.getUnderlyingVector());
        return Arrays.copyOf(full, 16);
    }

    @Nonnull
    private static byte[] keyOf(@Nonnull final RealVector vector) {
        byte[] full = sha256(vector);
        return Arrays.copyOf(full, 16);
    }

    @Nonnull
    private static byte[] sha256(@Nonnull final RealVector vector) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(vector.getRawData());
            return md.digest();
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static class CollapseResult {
        @Nonnull
        private final List<VectorReference> assignments;
        @Nonnull
        private final RunningStandardDeviation updatedStandardDeviation;
        @Nonnull
        private final ListMultimap<UUID, VectorId> collapsedAssignmentsMap;

        public CollapseResult(@Nonnull final List<VectorReference> assignments,
                              @Nonnull final RunningStandardDeviation updatedStandardDeviation,
                              @Nonnull final ListMultimap<UUID, VectorId> collapsedAssignmentsMap) {
            this.assignments = assignments;
            this.updatedStandardDeviation = updatedStandardDeviation;
            this.collapsedAssignmentsMap = collapsedAssignmentsMap;
        }

        @Nonnull
        public List<VectorReference> getAssignments() {
            return assignments;
        }

        @Nonnull
        public RunningStandardDeviation getUpdatedStandardDeviation() {
            return updatedStandardDeviation;
        }

        @Nonnull
        public ListMultimap<UUID, VectorId> getCollapsedAssignmentsMap() {
            return collapsedAssignmentsMap;
        }
    }
}
