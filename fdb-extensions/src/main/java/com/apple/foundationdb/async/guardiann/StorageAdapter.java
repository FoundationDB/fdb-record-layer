/*
 * StorageAdapter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * TODO.
 */
class StorageAdapter {
    private static double EPS = 1.0e-12;

    /**
     * Subspace for the access info.
     */
    private static final long SUBSPACE_PREFIX_ACCESS_INFO = 0x00;

    /**
     * Subspace for the cluster data, that is the centroids currently in use.
     */
    private static final long SUBSPACE_PREFIX_CLUSTER_CENTROIDS = 0x01;

    /**
     * Subspace for the cluster data, that is the centroids currently in use.
     */
    private static final long SUBSPACE_PREFIX_CLUSTER_METADATA = 0x02;

    /**
     * Subspace for the vector entries.
     */
    private static final long SUBSPACE_PREFIX_VECTOR_REFERENCES = 0x03;

    /**
     * Subspace for the vector entries.
     */
    private static final long SUBSPACE_PREFIX_VECTOR_METADATA = 0x04;

    /**
     * Subspace for (mostly) statistical analysis (like finding a centroid, etc.). Contains samples of vectors.
     */
    private static final long SUBSPACE_PREFIX_SAMPLES = 0x05;

    /**
     * Subspace for outstanding tasks.
     */
    private static final long SUBSPACE_PREFIX_TASKS = 0x06;

    @Nonnull
    private final Config config;
    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    @Nonnull
    private final Supplier<Subspace> accessInfoSubspaceSupplier;
    @Nonnull
    private final Supplier<Subspace> clusterCentroidsSubspaceSupplier;
    @Nonnull
    private final Supplier<Subspace> clusterMetadataSubspaceSupplier;
    @Nonnull
    private final Supplier<Subspace> vectorReferencesSubspaceSupplier;
    @Nonnull
    private final Supplier<Subspace> vectorMetadataSubspaceSupplier;
    @Nonnull
    private final Supplier<Subspace> samplesSubspaceSupplier;
    @Nonnull
    private final Supplier<Subspace> tasksSubspaceSupplier;

    @Nonnull
    private final Supplier<com.apple.foundationdb.async.hnsw.Config> clusterCentroidsHnswConfigSupplier;

    /**
     * Constructs a new {@code StorageAdapter}.
     * <p>
     * This constructor initializes the adapter with the necessary configuration,
     * factories, and listeners for managing a guardian structure. It also sets up a
     * dedicated data subspace within the provided main subspace for storing node data.
     *
     * @param config the HNSW graph configuration
     * @param subspace the primary subspace for storing all graph-related data
     * @param onWriteListener the listener to be called on write operations
     * @param onReadListener the listener to be called on read operations
     */
    StorageAdapter(@Nonnull final Config config,
                   @Nonnull final Subspace subspace,
                   @Nonnull final OnWriteListener onWriteListener,
                   @Nonnull final OnReadListener onReadListener) {
        this.config = config;
        this.subspace = subspace;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
        this.accessInfoSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_ACCESS_INFO)));
        this.clusterCentroidsSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_CLUSTER_CENTROIDS)));
        this.clusterMetadataSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_CLUSTER_METADATA)));
        this.vectorReferencesSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_VECTOR_REFERENCES)));
        this.vectorMetadataSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_VECTOR_METADATA)));
        this.samplesSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_SAMPLES)));
        this.tasksSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_TASKS)));

        this.clusterCentroidsHnswConfigSupplier = Suppliers.memoize(this::computeClusterCentroidHnswConfig);
    }

    @Nonnull
    Config getConfig() {
        return config;
    }

    @Nonnull
    Subspace getSubspace() {
        return subspace;
    }

    @Nonnull
    public Subspace getAccessInfoSubspace() {
        return accessInfoSubspaceSupplier.get();
    }

    @Nonnull
    Subspace getClusterCentroidsSubspace() {
        return clusterCentroidsSubspaceSupplier.get();
    }

    @Nonnull
    public Subspace getClusterMetadataSubspace() {
        return clusterMetadataSubspaceSupplier.get();
    }

    @Nonnull
    public Subspace getVectorReferencesSubspace() {
        return vectorReferencesSubspaceSupplier.get();
    }

    @Nonnull
    public Subspace getVectorMetadataSubspace() {
        return vectorMetadataSubspaceSupplier.get();
    }

    @Nonnull
    public Subspace getSamplesSubspace() {
        return samplesSubspaceSupplier.get();
    }

    @Nonnull
    public Subspace getTasksSubspace() {
        return tasksSubspaceSupplier.get();
    }

    @Nonnull
    OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    @Nonnull
    OnReadListener getOnReadListener() {
        return onReadListener;
    }

    @Nonnull
    com.apple.foundationdb.async.hnsw.Config getClusterCentroidsHnswConfig() {
        return clusterCentroidsHnswConfigSupplier.get();
    }

    @Nonnull
    private com.apple.foundationdb.async.hnsw.Config computeClusterCentroidHnswConfig() {
        final Config config = getConfig();
        return HNSW.newConfigBuilder()
                .setMetric(config.getMetric())
                .setUseInlining(false)
                .setEfRepair(64)
                .setExtendCandidates(false)
                .setKeepPrunedConnections(false)
                .setUseRaBitQ(false)
                .setM(16)
                .setMMax(24)
                .setMMax0(32)
                .build(config.getNumDimensions());
    }

    @Nonnull
    static AccessInfo accessInfoFromTuple(@Nonnull final Config config, @Nonnull final Tuple valueTuple) {
        final long rotatorSeed = valueTuple.getLong(0);
        final Tuple centroidVectorTuple = valueTuple.getNestedTuple(1);
        return new AccessInfo(rotatorSeed,
                centroidVectorTuple == null ? null : StorageHelpers.vectorFromTuple(config, centroidVectorTuple));
    }

    @Nonnull
    static Tuple tupleFromAccessInfo(@Nonnull final AccessInfo accessInfo) {
        final RealVector centroid = accessInfo.getNegatedCentroid();
        return Tuple.from(accessInfo.getRotatorSeed(),
                centroid == null ? null : StorageHelpers.tupleFromVector(centroid));
    }

    @Nonnull
    static VectorMetadata vectorMetadataFromTuple(@Nonnull final Tuple primaryKey, @Nonnull final Tuple valueTuple) {
        return new VectorMetadata(primaryKey, valueTuple.getUUID(0), valueTuple.getNestedTuple(1));
    }

    @Nonnull
    static Tuple valueTupleFromVectorMetadata(@Nonnull final VectorMetadata vectorMetadata) {
        return Tuple.from(vectorMetadata.getUuid(), vectorMetadata.getAdditionalValues());
    }

    @Nonnull
    static UUID clusterIdFromTuple(@Nonnull final Tuple tuple) {
        return tuple.getUUID(0);
    }

    @Nonnull
    static Tuple tupleFromClusterId(@Nonnull final UUID clusterId) {
        return Tuple.from(clusterId);
    }

    @Nonnull
    public static Tuple tupleFromClusterIds(@Nonnull final Set<UUID> clusterIds) {
        return tupleFromUuids(clusterIds);
    }

    @Nonnull
    public static Set<UUID> clusterIdsFromTuple(@Nonnull final Tuple clusterIdsAsTuple) {
        return uuidsFromTuple(clusterIdsAsTuple);
    }

    @Nonnull
    public static Tuple tupleFromTaskIds(@Nonnull final Set<UUID> taskIds) {
        return tupleFromUuids(taskIds);
    }

    @Nonnull
    public static Set<UUID> taskIdsFromTuple(@Nonnull final Tuple taskIdsAsTuple) {
        return uuidsFromTuple(taskIdsAsTuple);
    }

    @Nonnull
    private static Tuple tupleFromUuids(@Nonnull final Set<UUID> uuids) {
        return Tuple.fromItems(uuids);
    }

    @Nonnull
    private static Set<UUID> uuidsFromTuple(@Nonnull final Tuple uuidsAsTuple) {
        final ImmutableSet.Builder<UUID> resultBuilder = ImmutableSet.builder();
        for (int i = 0; i < uuidsAsTuple.size(); i ++) {
            resultBuilder.add(uuidsAsTuple.getUUID(i));
        }
        return resultBuilder.build();
    }

    @Nonnull
    static ClusterMetadata clusterMetadataFromTuple(@Nonnull final Tuple valueTuple) {
        return new ClusterMetadata(valueTuple.getUUID(0),
                Math.toIntExact(valueTuple.getLong(1)),
                Math.toIntExact(valueTuple.getLong(2)),
                runningStandardDeviationFromTuple(valueTuple.getNestedTuple(3)),
                Math.toIntExact(valueTuple.getLong(4)));
    }

    @Nonnull
    static Tuple valueTupleFromClusterMetadata(@Nonnull final ClusterMetadata clusterMetadata) {
        return Tuple.from(clusterMetadata.getId(),
                clusterMetadata.getNumPrimaryUnderreplicatedVectors(), clusterMetadata.getNumReplicatedVectors(),
                valueTupleFromRunningStandardDeviation(clusterMetadata.getRunningStandardDeviation()),
                clusterMetadata.getStatesCode());
    }

    @Nonnull
    static RunningStandardDeviation runningStandardDeviationFromTuple(@Nonnull final Tuple valueTuple) {
        return new RunningStandardDeviation(valueTuple.getLong(0), valueTuple.getDouble(1),
                valueTuple.getDouble(2));
    }

    @Nonnull
    static Tuple valueTupleFromRunningStandardDeviation(@Nonnull final RunningStandardDeviation runningStandardDeviation) {
        return Tuple.from(runningStandardDeviation.getNumElements(), runningStandardDeviation.getRunningMean(),
                runningStandardDeviation.getRunningSumSquaredDeviations());
    }

    @Nonnull
    static ClusterIdAndCentroid clusterIdAndCentroidFromTuple(@Nonnull final Config config,
                                                              @Nonnull final StorageTransform storageTransform,
                                                              @Nonnull final Tuple valueTuple) {
        return new ClusterIdAndCentroid(valueTuple.getUUID(0),
                storageTransform.transform(StorageHelpers.vectorFromBytes(config, valueTuple.getBytes(1))));
    }

    @Nonnull
    static Tuple valueTupleFromClusterIdAndCentroid(@Nonnull final Quantizer quantizer,
                                                    @Nonnull final ClusterIdAndCentroid clusterIdAndCentroid) {
        return Tuple.from(clusterIdAndCentroid.getClusterId(),
                StorageHelpers.bytesFromVector(quantizer.encode(clusterIdAndCentroid.getCentroid())));
    }

    @Nonnull
    static VectorReference vectorReferenceFromTuples(@Nonnull final Config config,
                                                     @Nonnull final StorageTransform storageTransform,
                                                     @Nonnull final Tuple primaryKey,
                                                     @Nonnull final Tuple valueTuple) {
        final VectorId vectorId = new VectorId(primaryKey, valueTuple.getUUID(0));

        final boolean isPrimaryCopy = valueTuple.getBoolean(1);
        final boolean isUnderreplicated = valueTuple.getBoolean(2);
        final Transformed<RealVector> vector =
                storageTransform.transform(StorageHelpers.vectorFromBytes(config, valueTuple.getBytes(3)));
        final double replicationPriority = isPrimaryCopy ? -1 : valueTuple.getDouble(4);
        return new VectorReference(vectorId, isPrimaryCopy, isUnderreplicated, vector, replicationPriority);
    }

    @Nonnull
    static Tuple valueTupleFromVectorReference(@Nonnull final Quantizer quantizer,
                                               @Nonnull final VectorReference vectorReference) {
        final VectorId vectorId = vectorReference.getId();
        final Transformed<RealVector> encodedVector = quantizer.encode(vectorReference.getVector());
        return Tuple.from(vectorId.getUuid(), vectorReference.isPrimaryCopy(),
                vectorReference.isUnderreplicated(), encodedVector.getUnderlyingVector().getRawData(),
                vectorReference.isPrimaryCopy() ? null : vectorReference.getReplicationPriority());
    }

    static double replicationPriority(final double distance, final double distanceToPrimaryCentroid,
                                      final int num, final double mean, final double standardDeviation) {
        final double r = distanceToPrimaryCentroid / (distance + EPS);
        final double z =
                num < 200 ? 0 : Math.max(0, (distanceToPrimaryCentroid - mean) / (standardDeviation + EPS));
        return 1.0d * r + 0.00d * z;
    }

    static boolean isOccluded(@Nonnull final Estimator estimator,
                              @Nonnull final ClusterMetadataWithDistance replicationCandidate,
                              @Nonnull final List<ClusterMetadataWithDistance> selectedReplicationClusters) {
        final double vectorToCentroidDistance = replicationCandidate.getDistance();
        if (!selectedReplicationClusters.isEmpty()) {
            final Transformed<RealVector> replicationCandidateCentroid =
                    replicationCandidate.getCentroid();
            boolean occluded = false;
            for (final ClusterMetadataWithDistance selectedReplicationCluster : selectedReplicationClusters) {
                final double selectedCentroidToCandidateCentroidDistance =
                        estimator.distance(replicationCandidateCentroid, selectedReplicationCluster.getCentroid());
                if (vectorToCentroidDistance > selectedCentroidToCandidateCentroidDistance) {
                    occluded = true;
                    break;
                }
            }
            if (occluded) {
                return true;
            }
        }
        return false;
    }

    static double replicationPriorityOld(final double distance, final double distanceToPrimaryCentroid,
                                         final int num, final double mean, final double standardDeviation) {
        final double r = distanceToPrimaryCentroid / (distance + EPS);
        return 1.0d * r;
    }

    @Nonnull
    static <T> CompletableFuture<T> requireNonNull(@Nonnull final CompletableFuture<T> future) {
        return future.thenApply(Objects::requireNonNull);
    }
}
