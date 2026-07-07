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
import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Encapsulates the FoundationDB key/value layout for a Guardiann vector structure: it owns the subspaces for
 * access info, cluster centroids, cluster metadata, vector references, collapsed vector ids, vector metadata,
 * samples and deferred tasks, and provides the (de)serialization helpers between those records and FDB tuples.
 */
class StorageAdapter {
    private static final double EPS = 1.0e-12;

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
     * Subspace for vector references.
     */
    private static final long SUBSPACE_PREFIX_VECTOR_REFERENCES = 0x03;

    /**
     * Subspace for vector ids.
     */
    private static final long SUBSPACE_PREFIX_COLLAPSED_VECTOR_IDS = 0x04;

    /**
     * Subspace for vector metadata.
     */
    private static final long SUBSPACE_PREFIX_VECTOR_METADATA = 0x05;

    /**
     * Subspace for (mostly) statistical analysis (like finding a centroid, etc.). Contains samples of vectors.
     */
    private static final long SUBSPACE_PREFIX_SAMPLES = 0x06;

    /**
     * Subspace for outstanding tasks.
     */
    private static final long SUBSPACE_PREFIX_TASKS = 0x07;

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
    private final Supplier<Subspace> collapsedVectorIdsSubspaceSupplier;
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
     * This constructor initializes the adapter with the necessary configuration and listeners for managing a
     * Guardiann structure, and sets up the dedicated subspaces within the provided main subspace.
     *
     * @param config the Guardiann configuration
     * @param subspace the primary subspace for storing all Guardiann data
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
        this.collapsedVectorIdsSubspaceSupplier =
                Suppliers.memoize(() -> subspace.subspace(Tuple.from(SUBSPACE_PREFIX_COLLAPSED_VECTOR_IDS)));
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
    public Subspace getCollapsedVectorIdsSubspace() {
        return collapsedVectorIdsSubspaceSupplier.get();
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
                .setMetric(config.metric())
                .setUseInlining(false)
                .setEfRepair(64)
                .setExtendCandidates(false)
                .setKeepPrunedConnections(false)
                .setUseRaBitQ(false)
                .setM(16)
                .setMMax(24)
                .setMMax0(32)
                .build(config.numDimensions());
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
        return Tuple.from(accessInfo.rotatorSeed(),
                accessInfo.canUseRaBitQ() ? StorageHelpers.tupleFromVector(accessInfo.negatedCentroid()) : null);
    }

    @Nonnull
    static VectorMetadata vectorMetadataFromTuple(@Nonnull final Tuple primaryKey, @Nonnull final Tuple valueTuple) {
        return new VectorMetadata(primaryKey, valueTuple.getUUID(0), valueTuple.getNestedTuple(1));
    }

    @Nonnull
    static Tuple valueTupleFromVectorMetadata(@Nonnull final VectorMetadata vectorMetadata) {
        return Tuple.from(vectorMetadata.vectorId().uuid(), vectorMetadata.additionalValues());
    }

    @Nonnull
    static UUID clusterIdFromTuple(@Nonnull final Tuple tuple) {
        return tuple.getUUID(0);
    }

    @Nonnull
    static Tuple tupleFromClusterId(@Nonnull final UUID clusterId) {
        return Tuple.from(clusterId);
    }

    /**
     * Serializes a set of cluster ids into a {@link Tuple} for storage.
     *
     * @param clusterIds the cluster ids to serialize
     *
     * @return a tuple encoding the cluster ids
     */
    @Nonnull
    public static Tuple tupleFromClusterIds(@Nonnull final Set<UUID> clusterIds) {
        return tupleFromUuids(clusterIds);
    }

    /**
     * Deserializes a set of cluster ids from their {@link Tuple} representation.
     *
     * @param clusterIdsAsTuple the tuple holding the encoded cluster ids
     *
     * @return the decoded set of cluster ids
     */
    @Nonnull
    public static Set<UUID> clusterIdsFromTuple(@Nonnull final Tuple clusterIdsAsTuple) {
        return uuidsFromTuple(clusterIdsAsTuple);
    }

    /**
     * Serializes a set of task ids into a {@link Tuple} for storage.
     *
     * @param taskIds the task ids to serialize
     *
     * @return a tuple encoding the task ids
     */
    @Nonnull
    public static Tuple tupleFromTaskIds(@Nonnull final Set<UUID> taskIds) {
        return tupleFromUuids(taskIds);
    }

    /**
     * Deserializes a set of task ids from their {@link Tuple} representation.
     *
     * @param taskIdsAsTuple the tuple holding the encoded task ids
     *
     * @return the decoded set of task ids
     */
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
        return Tuple.from(clusterMetadata.id(),
                clusterMetadata.numPrimaryUnderreplicatedVectors(), clusterMetadata.numReplicatedVectors(),
                valueTupleFromRunningStats(clusterMetadata.runningStandardDeviation()),
                clusterMetadata.getStatesCode());
    }

    @Nonnull
    static RunningStats runningStandardDeviationFromTuple(@Nonnull final Tuple valueTuple) {
        return new RunningStats(valueTuple.getLong(0), valueTuple.getDouble(1),
                valueTuple.getDouble(2), valueTuple.getDouble(3));
    }

    @Nonnull
    static Tuple valueTupleFromRunningStats(@Nonnull final RunningStats runningStandardDeviation) {
        return Tuple.from(runningStandardDeviation.numElements(), runningStandardDeviation.runningMean(),
                runningStandardDeviation.runningSumSquaredDeviations(),
                runningStandardDeviation.runningMaxEver());
    }

    @Nonnull
    static ClusterReference clusterReferenceFromTuple(@Nonnull final Config config,
                                                      @Nonnull final StorageTransform storageTransform,
                                                      @Nonnull final Tuple valueTuple) {
        return new ClusterReference(valueTuple.getUUID(0),
                storageTransform.transform(StorageHelpers.vectorFromBytes(config, valueTuple.getBytes(1))));
    }

    @Nonnull
    static Tuple valueTupleFromClusterReference(@Nonnull final Quantizer quantizer,
                                                @Nonnull final ClusterReference clusterReference) {
        return Tuple.from(clusterReference.clusterId(),
                StorageHelpers.bytesFromVector(quantizer.encode(clusterReference.centroid())));
    }

    @Nonnull
    static VectorReference vectorReferenceFromTuples(@Nonnull final Config config,
                                                     @Nonnull final StorageTransform storageTransform,
                                                     @Nonnull final Tuple primaryKey,
                                                     @Nonnull final Tuple valueTuple) {
        final VectorId vectorId = new VectorId(primaryKey, valueTuple.getUUID(0));
        final VectorReference.Role role = VectorReference.Role.ofCode((int)valueTuple.getLong(1));
        final boolean isCollapsed = valueTuple.getBoolean(2);
        final Transformed<RealVector> vector =
                storageTransform.transform(StorageHelpers.vectorFromBytes(config, valueTuple.getBytes(3)));
        return switch (role) {
            case PRIMARY -> VectorReference.primaryCopy(vectorId, vector, false, isCollapsed);
            case UNDERREPLICATED_PRIMARY -> VectorReference.primaryCopy(vectorId, vector, true, isCollapsed);
            case REPLICATED -> VectorReference.replicatedCopy(vectorId, vector, valueTuple.getDouble(4), isCollapsed);
        };
    }

    @Nonnull
    static Tuple valueTupleFromVectorReference(@Nonnull final Quantizer quantizer,
                                               @Nonnull final VectorReference vectorReference) {
        final UUID uuid = vectorReference.id().uuid();
        final byte[] rawData = quantizer.encode(vectorReference.vector()).getUnderlyingVector().getRawData();
        final boolean isCollapsed = vectorReference.isCollapsed();
        if (vectorReference instanceof ReplicatedCopy replicatedCopy) {
            return Tuple.from(uuid, VectorReference.Role.REPLICATED.getCode(), isCollapsed, rawData,
                    replicatedCopy.replicationPriority());
        }
        // The only other sealed variant is a primary copy; underreplication is folded into the role code.
        final VectorReference.Role role =
                vectorReference.isUnderreplicated()
                ? VectorReference.Role.UNDERREPLICATED_PRIMARY
                : VectorReference.Role.PRIMARY;
        return Tuple.from(uuid, role.getCode(), isCollapsed, rawData);
    }

    @Nonnull
    static VectorId collapsedVectorIdFromValueTuple(@Nonnull final Tuple primaryKey,
                                                    @Nonnull final Tuple valueTuple) {
        return new VectorId(primaryKey, valueTuple.getUUID(0));
    }

    @Nonnull
    static Tuple valueTupleFromCollapsedVectorId(@Nonnull final VectorId vectorId) {
        return Tuple.from(vectorId.uuid());
    }

    /**
     * Scores how worthwhile it is to keep a replica of a vector in a neighboring ("candidate") cluster, given that the
     * vector's authoritative home is a different ("primary") cluster. A higher score argues more strongly for
     * replicating the vector into the candidate; the maintenance paths rank candidates by this score, keep only those
     * at or above {@link Config#replicationPriorityMin()}, and cap the kept count at
     * {@link Config#replicatedClusterTarget()}.
     *
     * <p>
     * The score weighs and combines two independently-motivated terms:
     * <ul>
     *   <li><b>Border ambiguity</b> — the ratio {@code distanceToPrimaryCentroid / distance}, i.e. how far the vector
     *       is from its home centroid relative to the candidate centroid. For a primary vector the home is the nearest
     *       centroid, so this ratio lies in {@code (0, 1]}: it approaches {@code 1} when the vector sits right on the
     *       border, midway between the two centroids, and shrinks toward {@code 0} when the vector is firmly inside its
     *       home cluster. The more border-ambiguous a vector is, the more likely a query near it is routed to the
     *       candidate cluster and would miss it unless a replica lives there — hence the higher priority. (The precise
     *       geometry of "midway" depends on the {@link Config#metric()}, but the monotonicity — nearer the border means
     *       a higher ratio — holds for any metric.) Weighted by {@link Config#replicationDistanceRatioWeight()}.</li>
     *   <li><b>Distributional tail-ness</b> — the one-sided z-score
     *       {@code max(0, (distanceToPrimaryCentroid - mean) / standardDeviation)} of the vector's distance to its home
     *       centroid against the distribution of that distance over <em>all</em> of the home cluster's primary vectors.
     *       Vectors out in the cluster's long tail (much farther from the centroid than their peers) score high;
     *       vectors at or nearer than the mean are clamped to {@code 0} and get no boost. The idea is that a long tail
     *       should raise its members' replication odds, since those far-flung vectors are the ones most likely to sit
     *       near — or past — a neighboring cluster. Weighted by {@link Config#replicationZScoreWeight()}.</li>
     * </ul>
     *
     * <p>
     * The tail-ness term is evaluated only when its weight is non-zero and the home cluster holds at least
     * {@link Config#replicationStatsMinSampleSize()} primary vectors (so its {@code mean}/{@code standardDeviation} are
     * trustworthy); otherwise it contributes {@code 0} — which also avoids a {@code 0.0 * NaN} when the standard
     * deviation is undefined. A small epsilon guards both divisions against a zero denominator.
     *
     * @param config the tuning knobs supplying the two term weights and the minimum sample size
     * @param distance the vector's distance to the candidate (neighboring) cluster's centroid — the cluster being
     *        considered as a replication target
     * @param distanceToPrimaryCentroid the vector's distance to its own (home/primary) cluster's centroid
     * @param num the number of primary vectors in the home cluster (the sample size backing {@code mean} and
     *        {@code standardDeviation})
     * @param mean the mean distance-to-centroid over the home cluster's primary vectors
     * @param standardDeviation the standard deviation of those distances
     * @return the replication priority score; larger values argue more strongly for replicating the vector into the
     *         candidate cluster
     */
    static double replicationPriority(@Nonnull final Config config,
                                      final double distance, final double distanceToPrimaryCentroid,
                                      final int num, final double mean, final double standardDeviation) {
        final double zWeight = config.replicationZScoreWeight();
        final double r = distanceToPrimaryCentroid / (distance + EPS);
        // Skip the z term entirely when its weight is 0 (the default): avoids both the wasted work
        // and the 0.0 * NaN == NaN footgun when a cluster's standard deviation is undefined.
        final double z =
                (zWeight == 0.0d || num < config.replicationStatsMinSampleSize())
                ? 0.0d
                : Math.max(0.0d, (distanceToPrimaryCentroid - mean) / (standardDeviation + EPS));
        return config.replicationDistanceRatioWeight() * r + zWeight * z;
    }

    /**
     * Applies the relative-neighborhood ("occlusion") heuristic used to pick a <em>diverse</em> set of clusters to
     * replicate a vector into: a candidate cluster is <em>occluded</em> — and should be skipped — when some cluster
     * already chosen as a replication target sits closer to the candidate's centroid than the vector itself does.
     * Formally, the candidate is occluded if, for any already-selected cluster {@code s},
     * {@code dist(candidateCentroid, s.centroid) < dist(vector, candidateCentroid)}, where the right-hand distance is
     * the one carried on the candidate as {@link ClusterMetadataWithDistance#distance()}.
     *
     * <p>
     * The idea comes from the SPANN paper (<a href="https://arxiv.org/pdf/2111.08566">arXiv:2111.08566</a>), which
     * replicates boundary postings into nearby clusters using this relative-neighborhood ("closure") rule — the same
     * pruning HNSW applies during neighbor selection. Without it, the few clusters nearest a vector tend to bunch
     * together in one direction, so replicating into all of them is redundant. If an already-selected cluster lies
     * "between" the vector and the candidate (nearer to the candidate than the vector is), that selected cluster
     * already covers the candidate's region and adding the candidate buys little, so it is dropped. The net effect is
     * to spread replicas across distinct neighboring regions rather than pile several near-duplicates in the same
     * direction. When no clusters have been selected yet nothing can occlude, so the candidate is always kept.
     *
     * @param estimator the distance estimator used for the centroid-to-centroid distances (in the transformed space)
     * @param replicationCandidate the candidate cluster under consideration, carrying the vector's distance to its
     *        centroid via {@link ClusterMetadataWithDistance#distance()}
     * @param selectedReplicationClusters the clusters already chosen as replication targets for this vector
     * @return {@code true} if the candidate is occluded by an already-selected cluster (and should be skipped),
     *         {@code false} otherwise
     */
    static boolean isOccluded(@Nonnull final DistanceEstimator estimator,
                              @Nonnull final ClusterMetadataWithDistance replicationCandidate,
                              @Nonnull final List<ClusterMetadataWithDistance> selectedReplicationClusters) {
        final double vectorToCentroidDistance = replicationCandidate.distance();
        if (!selectedReplicationClusters.isEmpty()) {
            final Transformed<RealVector> replicationCandidateCentroid =
                    replicationCandidate.centroid();
            boolean occluded = false;
            for (final ClusterMetadataWithDistance selectedReplicationCluster : selectedReplicationClusters) {
                final double selectedCentroidToCandidateCentroidDistance =
                        estimator.distance(replicationCandidateCentroid, selectedReplicationCluster.centroid());
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

    @Nonnull
    static UUID signatureUuid(@Nonnull final Transformed<RealVector> vector) {
        return uuidFromBytes(signatureOf(vector));
    }

    /**
     * Packs 16 bytes of content hash into an RFC 9562 version-8 ("custom") {@link UUID}: the bytes supply the payload,
     * while the version nibble is forced to {@code 8} and the variant to IETF ({@code 10xx}) — mirroring how
     * {@code RandomHelpers} stamps its v4/v3 ids. The signature is a deterministic content hash, not a random id, so
     * v8 (application-defined) marks it as such and keeps it distinguishable from the v4 random ids and v3 name-based
     * ids used elsewhere in the system. Stamping the version/variant consumes 6 of the 128 bits (leaving 122 bits of
     * entropy), which remains far beyond any realistic collision risk for the dedup signature. The bits are folded in
     * as the longs are assembled, so no unstamped UUID is ever constructed.
     *
     * @param keyAsBytes exactly 16 bytes of hash payload
     * @return the version-8 UUID carrying those bytes
     */
    @Nonnull
    private static UUID uuidFromBytes(@Nonnull final byte[] keyAsBytes) {
        if (keyAsBytes.length != 16) {
            throw new IllegalArgumentException("Expected 16 bytes, got " + keyAsBytes.length);
        }
        final long hi = (readLongBigEndian(keyAsBytes, 0) & 0xffffffffffff0fffL) | 0x0000000000008000L; // version 8
        final long lo = (readLongBigEndian(keyAsBytes, 8) & 0x3fffffffffffffffL) | 0x8000000000000000L; // IETF variant
        return new UUID(hi, lo);
    }

    static long readLongBigEndian(byte[] b, int off) {
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
    static byte[] signatureOf(@Nonnull final Transformed<RealVector> vector) {
        return signatureOf(vector.getUnderlyingVector());
    }

    @Nonnull
    static byte[] signatureOf(@Nonnull final RealVector vector) {
        byte[] full = sha256(vector);
        return Arrays.copyOf(full, 16);
    }

    @Nonnull
    static byte[] sha256(@Nonnull final RealVector vector) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(vector.getRawData());
            return md.digest();
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    @Nonnull
    static <T> CompletableFuture<T> requireNonNull(@Nonnull final CompletableFuture<T> future) {
        return future.thenApply(Objects::requireNonNull);
    }
}
