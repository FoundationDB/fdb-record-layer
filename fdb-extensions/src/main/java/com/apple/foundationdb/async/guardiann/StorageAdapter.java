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

    private final Config config;
    private final Subspace subspace;
    private final OnWriteListener onWriteListener;
    private final OnReadListener onReadListener;

    private final Supplier<Subspace> accessInfoSubspaceSupplier;
    private final Supplier<Subspace> clusterCentroidsSubspaceSupplier;
    private final Supplier<Subspace> clusterMetadataSubspaceSupplier;
    private final Supplier<Subspace> vectorReferencesSubspaceSupplier;
    private final Supplier<Subspace> collapsedVectorIdsSubspaceSupplier;
    private final Supplier<Subspace> vectorMetadataSubspaceSupplier;
    private final Supplier<Subspace> samplesSubspaceSupplier;
    private final Supplier<Subspace> tasksSubspaceSupplier;

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
    StorageAdapter(final Config config,
                   final Subspace subspace,
                   final OnWriteListener onWriteListener,
                   final OnReadListener onReadListener) {
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

    Config getConfig() {
        return config;
    }

    Subspace getSubspace() {
        return subspace;
    }

    public Subspace getAccessInfoSubspace() {
        return accessInfoSubspaceSupplier.get();
    }

    Subspace getClusterCentroidsSubspace() {
        return clusterCentroidsSubspaceSupplier.get();
    }

    public Subspace getClusterMetadataSubspace() {
        return clusterMetadataSubspaceSupplier.get();
    }

    public Subspace getVectorReferencesSubspace() {
        return vectorReferencesSubspaceSupplier.get();
    }

    public Subspace getCollapsedVectorIdsSubspace() {
        return collapsedVectorIdsSubspaceSupplier.get();
    }

    public Subspace getVectorMetadataSubspace() {
        return vectorMetadataSubspaceSupplier.get();
    }

    public Subspace getSamplesSubspace() {
        return samplesSubspaceSupplier.get();
    }

    public Subspace getTasksSubspace() {
        return tasksSubspaceSupplier.get();
    }

    OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    OnReadListener getOnReadListener() {
        return onReadListener;
    }

    com.apple.foundationdb.async.hnsw.Config getClusterCentroidsHnswConfig() {
        return clusterCentroidsHnswConfigSupplier.get();
    }

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

    static AccessInfo accessInfoFromTuple(final Config config, final Tuple valueTuple) {
        final long rotatorSeed = valueTuple.getLong(0);
        final Tuple centroidVectorTuple = valueTuple.getNestedTuple(1);
        return new AccessInfo(rotatorSeed,
                centroidVectorTuple == null ? null : StorageHelpers.vectorFromTuple(config, centroidVectorTuple));
    }

    static Tuple tupleFromAccessInfo(final AccessInfo accessInfo) {
        // Tuple.from(Object...) is from the unannotated fdb-java client library and genuinely supports null
        // elements (a null centroid tuple represents "RaBitQ not in use"), but its varargs parameter is
        // treated as @NonNull by NullAway's defaults.
        @SuppressWarnings("NullAway")
        final Tuple result = Tuple.from(accessInfo.rotatorSeed(),
                accessInfo.canUseRaBitQ() ? StorageHelpers.tupleFromVector(accessInfo.negatedCentroid()) : null);
        return result;
    }

    static VectorMetadata vectorMetadataFromTuple(final Tuple primaryKey, final Tuple valueTuple) {
        return new VectorMetadata(primaryKey, valueTuple.getUUID(0), valueTuple.getNestedTuple(1));
    }

    static Tuple valueTupleFromVectorMetadata(final VectorMetadata vectorMetadata) {
        // Tuple.from(Object...) is from the unannotated fdb-java client library and genuinely supports null
        // elements, but its varargs parameter is treated as @NonNull by NullAway's defaults;
        // additionalValues() may legitimately be absent.
        @SuppressWarnings("NullAway")
        final Tuple result = Tuple.from(vectorMetadata.vectorId().uuid(), vectorMetadata.additionalValues());
        return result;
    }

    static UUID clusterIdFromTuple(final Tuple tuple) {
        return tuple.getUUID(0);
    }

    static Tuple tupleFromClusterId(final UUID clusterId) {
        return Tuple.from(clusterId);
    }

    /**
     * Serializes a set of cluster ids into a {@link Tuple} for storage.
     *
     * @param clusterIds the cluster ids to serialize
     *
     * @return a tuple encoding the cluster ids
     */
    public static Tuple tupleFromClusterIds(final Set<UUID> clusterIds) {
        return tupleFromUuids(clusterIds);
    }

    /**
     * Deserializes a set of cluster ids from their {@link Tuple} representation.
     *
     * @param clusterIdsAsTuple the tuple holding the encoded cluster ids
     *
     * @return the decoded set of cluster ids
     */
    public static Set<UUID> clusterIdsFromTuple(final Tuple clusterIdsAsTuple) {
        return uuidsFromTuple(clusterIdsAsTuple);
    }

    /**
     * Serializes a set of task ids into a {@link Tuple} for storage.
     *
     * @param taskIds the task ids to serialize
     *
     * @return a tuple encoding the task ids
     */
    public static Tuple tupleFromTaskIds(final Set<UUID> taskIds) {
        return tupleFromUuids(taskIds);
    }

    /**
     * Deserializes a set of task ids from their {@link Tuple} representation.
     *
     * @param taskIdsAsTuple the tuple holding the encoded task ids
     *
     * @return the decoded set of task ids
     */
    public static Set<UUID> taskIdsFromTuple(final Tuple taskIdsAsTuple) {
        return uuidsFromTuple(taskIdsAsTuple);
    }

    private static Tuple tupleFromUuids(final Set<UUID> uuids) {
        return Tuple.fromItems(uuids);
    }

    private static Set<UUID> uuidsFromTuple(final Tuple uuidsAsTuple) {
        final ImmutableSet.Builder<UUID> resultBuilder = ImmutableSet.builder();
        for (int i = 0; i < uuidsAsTuple.size(); i ++) {
            resultBuilder.add(uuidsAsTuple.getUUID(i));
        }
        return resultBuilder.build();
    }

    static ClusterMetadata clusterMetadataFromTuple(final Tuple valueTuple) {
        return new ClusterMetadata(valueTuple.getUUID(0),
                Math.toIntExact(valueTuple.getLong(1)),
                Math.toIntExact(valueTuple.getLong(2)),
                runningStandardDeviationFromTuple(valueTuple.getNestedTuple(3)),
                Math.toIntExact(valueTuple.getLong(4)));
    }

    static Tuple valueTupleFromClusterMetadata(final ClusterMetadata clusterMetadata) {
        return Tuple.from(clusterMetadata.id(),
                clusterMetadata.numPrimaryUnderreplicatedVectors(), clusterMetadata.numReplicatedVectors(),
                valueTupleFromRunningStats(clusterMetadata.runningStandardDeviation()),
                clusterMetadata.getStatesCode());
    }

    static RunningStats runningStandardDeviationFromTuple(final Tuple valueTuple) {
        return new RunningStats(valueTuple.getLong(0), valueTuple.getDouble(1),
                valueTuple.getDouble(2), valueTuple.getDouble(3));
    }

    static Tuple valueTupleFromRunningStats(final RunningStats runningStandardDeviation) {
        return Tuple.from(runningStandardDeviation.numElements(), runningStandardDeviation.runningMean(),
                runningStandardDeviation.runningSumSquaredDeviations(),
                runningStandardDeviation.runningMaxEver());
    }

    static ClusterReference clusterReferenceFromTuple(final Config config,
                                                      final StorageTransform storageTransform,
                                                      final Tuple valueTuple) {
        return new ClusterReference(valueTuple.getUUID(0),
                storageTransform.transform(StorageHelpers.vectorFromBytes(config, valueTuple.getBytes(1))));
    }

    static Tuple valueTupleFromClusterReference(final Quantizer quantizer,
                                                final ClusterReference clusterReference) {
        return Tuple.from(clusterReference.clusterId(),
                StorageHelpers.bytesFromVector(quantizer.encode(clusterReference.centroid())));
    }

    static VectorReference vectorReferenceFromTuples(final Config config,
                                                     final StorageTransform storageTransform,
                                                     final Tuple primaryKey,
                                                     final Tuple valueTuple) {
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

    static Tuple valueTupleFromVectorReference(final Quantizer quantizer,
                                               final VectorReference vectorReference) {
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

    static VectorId collapsedVectorIdFromValueTuple(final Tuple primaryKey,
                                                    final Tuple valueTuple) {
        return new VectorId(primaryKey, valueTuple.getUUID(0));
    }

    static Tuple valueTupleFromCollapsedVectorId(final VectorId vectorId) {
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
    static double replicationPriority(final Config config,
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
    static boolean isOccluded(final DistanceEstimator estimator,
                              final ClusterMetadataWithDistance replicationCandidate,
                              final List<ClusterMetadataWithDistance> selectedReplicationClusters) {
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

    static UUID signatureUuid(final Transformed<RealVector> vector) {
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
    private static UUID uuidFromBytes(final byte[] keyAsBytes) {
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

    static byte[] signatureOf(final Transformed<RealVector> vector) {
        return signatureOf(vector.getUnderlyingVector());
    }

    static byte[] signatureOf(final RealVector vector) {
        byte[] full = sha256(vector);
        return Arrays.copyOf(full, 16);
    }

    static byte[] sha256(final RealVector vector) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(vector.getRawData());
            return md.digest();
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    static <T> CompletableFuture<T> requireNonNull(final CompletableFuture<T> future) {
        return future.thenApply(Objects::requireNonNull);
    }
}
