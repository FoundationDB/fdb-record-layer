/*
 * Primitives.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.Cardinality;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.LinearOperator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.rabitq.RaBitQuantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
import java.util.function.Supplier;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;
import static com.apple.foundationdb.async.MoreAsyncUtil.forLoop;

/**
 * An implementation of primitives for the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 */
@API(API.Status.EXPERIMENTAL)
class Primitives {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Primitives.class);

    @Nonnull
    private final Locator locator;

    @Nonnull
    private final Supplier<HNSW> clusterCentroidsHnswSupplier;

    // Traversal flags for the centroid-HNSW walk that ranks clusters around a point. includeVectors must be true
    // because callers compute distances from the returned centroid vectors; quick-start is safe because the walk
    // always starts at radius 0.0, where it cannot produce ordering inversions (see HNSW.orderByDistance). The
    // ring/outward exploration factors are supplied per call: search passes its per-query SearchConfig, while
    // insert/delete/maintenance pass Config.constructionSearchConfig().
    private static final boolean CENTROID_INCLUDE_VECTORS = true;
    private static final boolean CENTROID_SHOULD_QUICK_START = true;

    /**
     * Constructs a new primitives instance.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Primitives(@Nonnull final Locator locator) {
        this.locator = locator;

        this.clusterCentroidsHnswSupplier = Suppliers.memoize(this::computeClusterCentroidsHnsw);
    }

    /**
     * Returns the {@link Locator} that ties this primitives instance to its stored data, configuration, and executor.
     *
     * @return the locator
     */
    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    /**
     * Returns the {@link StorageAdapter} defining this structure's subspace layout and tuple (de)serialization.
     *
     * @return the storage adapter
     */
    @Nonnull
    StorageAdapter getStorageAdapter() {
        return locator.getStorageAdapter();
    }

    /**
     * Get the executor used by this guardiann.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    private Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration.
     * @return the configuration
     */
    @Nonnull
    private Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    private OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    private OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    /**
     * Returns the subspace holding the structure-wide {@link AccessInfo} entry.
     *
     * @return the access-info subspace
     */
    @Nonnull
    Subspace getAccessInfoSubspace() {
        return getStorageAdapter().getAccessInfoSubspace();
    }

    /**
     * Returns the subspace holding the cluster-centroid HNSW (the centroids currently in use).
     *
     * @return the cluster-centroids subspace
     */
    @Nonnull
    Subspace getClusterCentroidsSubspace() {
        return getStorageAdapter().getClusterCentroidsSubspace();
    }

    /**
     * Returns the subspace holding per-cluster {@link ClusterMetadata}, keyed by cluster id.
     *
     * @return the cluster-metadata subspace
     */
    @Nonnull
    Subspace getClusterMetadataSubspace() {
        return getStorageAdapter().getClusterMetadataSubspace();
    }

    /**
     * Returns the subspace holding per-cluster {@link VectorReference} entries, keyed by {@code (clusterId, primaryKey)}.
     *
     * @return the vector-references subspace
     */
    @Nonnull
    Subspace getVectorReferencesSubspace() {
        return getStorageAdapter().getVectorReferencesSubspace();
    }

    /**
     * Returns the subspace holding collapsed {@link VectorId} membership entries, keyed by {@code (signature, primaryKey)}.
     *
     * @return the collapsed-vector-ids subspace
     */
    @Nonnull
    Subspace getCollapsedVectorIdsSubspace() {
        return getStorageAdapter().getCollapsedVectorIdsSubspace();
    }

    /**
     * Returns the subspace holding per-vector {@link VectorMetadata}, keyed by primary key.
     *
     * @return the vector-metadata subspace
     */
    @Nonnull
    Subspace getVectorMetadataSubspace() {
        return getStorageAdapter().getVectorMetadataSubspace();
    }

    /**
     * Returns the subspace holding sampled vectors used for statistical analysis (for example centroid and
     * distance-statistics computation).
     *
     * @return the samples subspace
     */
    @Nonnull
    Subspace getSamplesSubspace() {
        return getStorageAdapter().getSamplesSubspace();
    }

    /**
     * Returns the subspace holding the deferred-task queue, keyed by task id.
     *
     * @return the tasks subspace
     */
    @Nonnull
    Subspace getTasksSubspace() {
        return getStorageAdapter().getTasksSubspace();
    }

    /**
     * Returns the (memoized) cluster-centroid {@link HNSW}, used to find the clusters nearest a vector.
     *
     * @return the cluster-centroids HNSW
     */
    @Nonnull
    HNSW getClusterCentroidsHnsw() {
        return clusterCentroidsHnswSupplier.get();
    }

    /**
     * Builds the cluster-centroid {@link HNSW}, wiring its read/write listeners through to this instance's
     * {@link OnReadListener}/{@link OnWriteListener} so that key/value access on the centroid HNSW is reported
     * alongside the rest of the structure's I/O. Memoized behind {@link #getClusterCentroidsHnsw()}.
     *
     * @return a newly constructed cluster-centroids HNSW
     */
    @Nonnull
    private HNSW computeClusterCentroidsHnsw() {
        final com.apple.foundationdb.async.hnsw.OnWriteListener onWriteListener =
                new com.apple.foundationdb.async.hnsw.OnWriteListener() {
                    @Override
                    public void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
                        getOnWriteListener().onKeyValueWritten(key, value);
                    }

                    @Override
                    public void onKeyDeleted(final int layer, @Nonnull final byte[] key) {
                        getOnWriteListener().onKeyDeleted(key);
                    }

                    @Override
                    public void onRangeDeleted(final int layer, @Nonnull final Range range) {
                        getOnWriteListener().onRangeDeleted(range);
                    }
                };

        final com.apple.foundationdb.async.hnsw.OnReadListener onReadListener =
                new com.apple.foundationdb.async.hnsw.OnReadListener() {
                    @Override
                    public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
                        getOnReadListener().onKeyValueRead(key, value);
                    }
                };

        return new HNSW(getClusterCentroidsSubspace(), getExecutor(),
                getStorageAdapter().getClusterCentroidsHnswConfig(), onWriteListener, onReadListener);
    }

    /**
     * Whether the configured distance {@link Metric} is only meaningful on unit-length vectors, so vectors must be
     * normalized before they are stored and compared. True for cosine distance, which compares directions.
     *
     * @return {@code true} if the configured metric requires normalized vectors
     */
    boolean storesNormalizedVectors() {
        return getConfig().metric() == Metric.COSINE_METRIC;
    }

    /**
     * Resolves the {@link StorageTransform} that maps a vector into the rotated/centered space it is actually stored
     * in. Until an {@link AccessInfo} has been trained (or when RaBitQ cannot be used) there is no centroid to rotate
     * around, so the identity transform is used; otherwise the transform is rebuilt from the access info's rotator
     * seed and negated centroid, normalizing first when the metric demands it.
     *
     * @param accessInfo the trained access context, or {@code null} if none has been established yet
     * @return the transform mapping vectors to and from their stored representation
     */
    @Nonnull
    StorageTransform storageTransform(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return StorageTransform.identity();
        }

        return storageTransform(accessInfo.rotatorSeed(),
                accessInfo.negatedCentroid(),
                storesNormalizedVectors());
    }

    /**
     * Assembles a {@link StorageTransform} straight from its components, for the paths that already hold the raw
     * rotator seed and centroid rather than a whole {@link AccessInfo}. A {@code null} seed means the vectors are
     * not rotated; a {@code null} centroid means they are not centered.
     *
     * @param rotatorSeed the seed selecting the {@link FhtKacRotator}, or {@code null} for no rotation
     * @param negatedCentroid the negated centroid used to center vectors, or {@code null} for no centering
     * @param normalizeVectors whether to normalize vectors as part of the transform
     * @return the assembled storage transform
     */
    @Nonnull
    private StorageTransform storageTransform(@Nullable final Long rotatorSeed,
                                              @Nullable final RealVector negatedCentroid,
                                              final boolean normalizeVectors) {
        final LinearOperator linearOperator =
                rotatorSeed == null
                ? null : new FhtKacRotator(rotatorSeed, getConfig().numDimensions(), 10);

        return new StorageTransform(linearOperator, negatedCentroid, normalizeVectors);
    }

    /**
     * Resolves the {@link Quantizer} used to compress stored vectors for cheap distance estimation. Falls back to a
     * no-op quantizer until an {@link AccessInfo} has been trained, or when RaBitQ is disabled in the config; a
     * trained, RaBitQ-enabled structure uses a {@link RaBitQuantizer}.
     *
     * @param accessInfo the trained access context, or {@code null} if none has been established yet
     * @return the quantizer used to encode and decode stored vectors
     */
    @Nonnull
    Quantizer quantizer(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return Quantizer.noOpQuantizer(getConfig().metric());
        }

        final Config config = getConfig();
        return config.useRaBitQ()
               ? new RaBitQuantizer(config.metric(), config.raBitQNumExBits())
               : Quantizer.noOpQuantizer(config.metric());
    }

    /**
     * Fetches the structure-wide {@link AccessInfo} — the trained per-structure context (rotator seed, centroid)
     * that every transform and quantizer is derived from. Its absence is the signal that the structure holds no
     * vectors yet, so this returns {@code null} in that case rather than failing.
     *
     * @param readTransaction the read transaction
     * @return a future of the access info, or {@code null} if the structure has not been initialized yet
     */
    @Nonnull
    CompletableFuture<AccessInfo> fetchAccessInfo(@Nonnull final ReadTransaction readTransaction) {
        final Subspace accessInfoSubspace = getAccessInfoSubspace();
        final byte[] key = accessInfoSubspace.pack();

        return getOnReadListener().onAsyncRead(readTransaction.get(key))
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null; // not a single vector in the structure
                    }
                    return StorageAdapter.accessInfoFromTuple(getConfig(), Tuple.fromBytes(valueBytes));
                });
    }

    /**
     * Persists the structure-wide {@link AccessInfo}. Written once when the structure is first trained; since every
     * transform and quantizer is derived from it, it changes rarely thereafter.
     *
     * @param transaction the transaction to write within
     * @param accessInfo the access info to persist
     */
    void writeAccessInfo(@Nonnull final Transaction transaction,
                         @Nonnull final AccessInfo accessInfo) {
        final Subspace accessInfoSubspace = getAccessInfoSubspace();
        final byte[] key = accessInfoSubspace.pack();
        final byte[] value = StorageAdapter.tupleFromAccessInfo(accessInfo).pack();
        transaction.set(key, value);
        getOnWriteListener().onKeyValueWritten(key, value);
    }

    /**
     * Tests whether a record identified by its primary key is part of the structure. The per-primary-key
     * {@link VectorMetadata} entry is the single source of truth for membership — the per-cluster references are
     * mutable copies — so existence is decided by probing for that entry.
     *
     * @param readTransaction the read transaction
     * @param primaryKey the primary key to test
     * @return a future completing {@code true} iff a metadata entry exists for the primary key
     */
    @Nonnull
    CompletableFuture<Boolean> primaryKeyExists(@Nonnull final ReadTransaction readTransaction, final Tuple primaryKey) {
        return fetchVectorMetadata(readTransaction, primaryKey).thenApply(Objects::nonNull);
    }

    /**
     * Fetches a vector's {@link VectorMetadata} — the authoritative, per-primary-key record of the vector (its
     * current UUID and any covering values). Search and clean-up rely on it to tell a live reference from a stale
     * one. Returns {@code null} when no vector with that primary key is stored.
     *
     * @param readTransaction the read transaction
     * @param primaryKey the primary key of the vector
     * @return a future of the metadata, or {@code null} if no such vector exists
     */
    @Nonnull
    CompletableFuture<VectorMetadata> fetchVectorMetadata(@Nonnull final ReadTransaction readTransaction,
                                                          @Nonnull final Tuple primaryKey) {
        final Subspace vectorMetadataSubspace = getVectorMetadataSubspace();
        final byte[] key = vectorMetadataSubspace.pack(primaryKey);

        return getOnReadListener().onAsyncRead(readTransaction.get(key))
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null; // unable to find vector
                    }
                    return StorageAdapter.vectorMetadataFromTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    /**
     * Persists a vector's {@link VectorMetadata} under its primary key. Writing this entry is what makes a vector
     * part of the structure (see {@link #primaryKeyExists}).
     *
     * @param transaction the transaction to write within
     * @param vectorMetadata the metadata to persist
     */
    void writeVectorMetadata(@Nonnull final Transaction transaction,
                             @Nonnull final VectorMetadata vectorMetadata) {
        final Subspace vectorMetadataSubspace = getVectorMetadataSubspace();
        final byte[] key = vectorMetadataSubspace.pack(vectorMetadata.vectorId().primaryKey());
        final byte[] value = StorageAdapter.valueTupleFromVectorMetadata(vectorMetadata).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    /**
     * Removes a vector's {@link VectorMetadata}, retiring the vector from the structure. Its per-cluster references
     * are left behind and become stale; they are pruned lazily during search/clean-up rather than here, so a delete
     * stays cheap and bounded.
     *
     * @param transaction the transaction to write within
     * @param primaryKey the primary key whose metadata is removed
     */
    void deleteVectorMetadata(@Nonnull final Transaction transaction,
                              @Nonnull final Tuple primaryKey) {
        final Subspace vectorMetadataSubspace = getVectorMetadataSubspace();
        final byte[] key = vectorMetadataSubspace.pack(primaryKey);
        getLocator().getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    /**
     * Streams cluster centroids nearest-first around a query/insert point by walking the centroid {@link HNSW}.
     * This is the entry point for "which clusters are even worth looking at" — both search and insert start here.
     * The {@code minimumRadius}/{@code minimumPrimaryKey} pair is an exclusive lower bound, so a partially consumed
     * walk can be resumed without revisiting centroids already seen.
     *
     * @param readTransaction the read transaction
     * @param centerVector the query/insert point to order centroids around
     * @param minimumRadius exclusive lower bound on distance to resume from ({@code 0} to start from the nearest)
     * @param minimumPrimaryKey the centroid-key tiebreaker at exactly {@code minimumRadius}, or {@code null} to start fresh
     * @param efRingSearch the ring-search exploration factor for the centroid HNSW walk
     * @param efOutwardSearch the outward-search exploration factor (candidate-queue size) for the centroid HNSW walk
     * @return an iterator of centroids (as {@link ResultEntry}s), nearest first
     */
    @Nonnull
    AsyncIterator<ResultEntry> centroidsOrderedByDistance(@Nonnull final ReadTransaction readTransaction,
                                                          @Nonnull final RealVector centerVector,
                                                          final double minimumRadius,
                                                          @Nullable final Tuple minimumPrimaryKey,
                                                          final int efRingSearch,
                                                          final int efOutwardSearch) {
        final HNSW centroidsHnsw = getClusterCentroidsHnsw();

        return centroidsHnsw.orderByDistance(readTransaction, efRingSearch, efOutwardSearch,
                CENTROID_INCLUDE_VECTORS, centerVector, minimumRadius, minimumPrimaryKey, CENTROID_SHOULD_QUICK_START);
    }

    /**
     * Loads a cluster's {@link ClusterMetadata} and bundles it with a centroid and an already-computed distance, so
     * a caller walking the centroid HNSW can carry the distance forward instead of recomputing it. Fails fast if the
     * metadata is missing — a cluster present in the centroid HNSW is expected to have metadata.
     *
     * @param readTransaction the read transaction
     * @param clusterId the id of the cluster
     * @param centroid the transformed centroid to attach
     * @param distance the already-computed distance to attach
     * @return a future of the metadata bundled with its centroid and distance
     */
    @Nonnull
    CompletableFuture<ClusterMetadataWithDistance> fetchClusterMetadataWithDistance(@Nonnull final ReadTransaction readTransaction,
                                                                                    @Nonnull final UUID clusterId,
                                                                                    @Nonnull final Transformed<RealVector> centroid,
                                                                                    final double distance) {
        return StorageAdapter.requireNonNull(fetchClusterMetadata(readTransaction, clusterId))
                .thenApply(clusterState -> new ClusterMetadataWithDistance(clusterState, centroid, distance));
    }

    /**
     * Convenience overload of {@link #fetchCluster(ReadTransaction, StorageTransform, UUID, Transformed)} for
     * callers holding an untransformed centroid: it applies {@code storageTransform} to the centroid first, then
     * fetches.
     *
     * @param readTransaction the read transaction
     * @param storageTransform the transform used to map the centroid and reconstruct stored vectors
     * @param clusterId the id of the cluster to fetch
     * @param centroid the cluster centroid in the untransformed (client) coordinate space
     * @return a future of the fetched cluster
     */
    @Nonnull
    CompletableFuture<Cluster> fetchCluster(@Nonnull final ReadTransaction readTransaction,
                                            @Nonnull final StorageTransform storageTransform,
                                            @Nonnull final UUID clusterId,
                                            @Nonnull final RealVector centroid) {
        final Transformed<RealVector> transformedCentroid = storageTransform.transform(centroid);
        return fetchCluster(readTransaction, storageTransform, clusterId, transformedCentroid);
    }

    /**
     * Fetches a cluster as a single in-memory {@link Cluster}: its {@link ClusterMetadata} joined with all of its
     * {@link VectorReference}s, which is the unit the maintenance tasks operate on. Fails fast if the metadata is
     * missing, and (for now) asserts that the stored primary-vector count agrees with the references actually read.
     *
     * @param readTransaction the read transaction
     * @param storageTransform the transform used to reconstruct stored vectors
     * @param clusterId the id of the cluster to fetch
     * @param centroid the cluster centroid in the transformed (stored) coordinate space
     * @return a future of the fetched cluster
     */
    @Nonnull
    CompletableFuture<Cluster> fetchCluster(@Nonnull final ReadTransaction readTransaction,
                                            @Nonnull final StorageTransform storageTransform,
                                            @Nonnull final UUID clusterId,
                                            @Nonnull final Transformed<RealVector> centroid) {
        return StorageAdapter.requireNonNull(fetchClusterMetadata(readTransaction, clusterId))
                .thenCombine(fetchVectorReferences(readTransaction, storageTransform, clusterId),
                        (clusterMetadata, vectorReferences) -> {
                            // TODO remove
                            Verify.verify(clusterMetadata.getNumPrimaryVectors() ==
                                    vectorReferences.stream().filter(VectorReference::isPrimaryCopy).count());
                            return new Cluster(clusterMetadata, centroid, vectorReferences);
                        });
    }

    /**
     * Fetches a cluster's {@link ClusterMetadata} — its counts, running distance statistics, and pending-maintenance
     * state — or {@code null} if the cluster no longer exists.
     *
     * @param readTransaction the read transaction
     * @param clusterId the id of the cluster
     * @return a future of the metadata, or {@code null} if the cluster does not exist
     */
    @Nonnull
    CompletableFuture<ClusterMetadata> fetchClusterMetadata(@Nonnull final ReadTransaction readTransaction,
                                                            @Nonnull final UUID clusterId) {
        final byte[] key = getClusterMetadataSubspace().pack(Tuple.from(clusterId));
        return getOnReadListener().onAsyncRead(readTransaction.get(key))
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null;
                    }
                    return StorageAdapter.clusterMetadataFromTuple(Tuple.fromBytes(valueBytes));
                });
    }

    /**
     * Persists a cluster's {@link ClusterMetadata}. This is how vector-count deltas and maintenance-state flags
     * (split/merge/reassign/collapse) become durable.
     *
     * @param transaction the transaction to write within
     * @param clusterMetadata the metadata to persist
     */
    void writeClusterMetadata(@Nonnull final Transaction transaction,
                              @Nonnull final ClusterMetadata clusterMetadata) {
        final Subspace clusterMetadataSubspace = getClusterMetadataSubspace();
        final byte[] key = clusterMetadataSubspace.pack(Tuple.from(clusterMetadata.id()));
        final byte[] value = StorageAdapter.valueTupleFromClusterMetadata(clusterMetadata).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    /**
     * Deletes a cluster's {@link ClusterMetadata} — used when a cluster is dissolved by a split or merge. The
     * cluster's vector references are removed separately via {@link #deleteVectorReferencesForCluster}, since the
     * two live in different subspaces.
     *
     * @param transaction the transaction to write within
     * @param clusterId the id of the dissolved cluster
     */
    void deleteClusterMetadata(@Nonnull final Transaction transaction,
                               @Nonnull final UUID clusterId) {
        final Subspace clusterMetadataSubspace = getClusterMetadataSubspace();
        final byte[] key = clusterMetadataSubspace.pack(Tuple.from(clusterId));

        getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    /**
     * Materializes all of a cluster's {@link VectorReference}s into a list. Prefer
     * {@link #fetchVectorReferencesIterable} when the references can be streamed rather than held at once.
     *
     * @param readTransaction the read transaction
     * @param storageTransform the transform used to reconstruct stored vectors
     * @param clusterId the id of the cluster
     * @return a future of the cluster's vector references
     */
    @Nonnull
    CompletableFuture<List<VectorReference>> fetchVectorReferences(@Nonnull final ReadTransaction readTransaction,
                                                                   @Nonnull final StorageTransform storageTransform,
                                                                   @Nonnull final UUID clusterId) {
        return AsyncUtil.collect(fetchVectorReferencesIterable(readTransaction, storageTransform, clusterId),
                getExecutor());
    }

    /**
     * Streams a cluster's {@link VectorReference}s by scanning its key range. A cluster holds many references — its
     * own primaries plus replicas of neighboring clusters' vectors — so this streaming form is what the maintenance
     * tasks use when they need to walk an entire cluster.
     *
     * @param readTransaction the read transaction
     * @param storageTransform the transform used to reconstruct stored vectors
     * @param clusterId the id of the cluster
     * @return an iterable over the cluster's vector references
     */
    @Nonnull
    AsyncIterable<VectorReference> fetchVectorReferencesIterable(@Nonnull final ReadTransaction readTransaction,
                                                                 @Nonnull final StorageTransform storageTransform,
                                                                 @Nonnull final UUID clusterId) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterId));

        return AsyncUtil.mapIterable(getOnReadListener().onAsyncReadRange(
                        readTransaction.getRange(Range.startsWith(rangeKey),
                                ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)),
                keyValue -> {
                    final Tuple primaryKey = vectorReferencesSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                    final byte[] keyBytes = keyValue.getKey();
                    final byte[] valueBytes = keyValue.getValue();
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    final VectorReference vectorReference =
                            StorageAdapter.vectorReferenceFromTuples(getConfig(), storageTransform,
                                    primaryKey, Tuple.fromBytes(valueBytes));
                    getOnReadListener().onVectorRead(clusterId, vectorReference.id().primaryKey(),
                            vectorReference.id().uuid(), vectorReference.vector());
                    return vectorReference;
                });
    }

    /**
     * Point-fetches one {@link VectorReference} within a cluster, or {@code null} if that cluster holds no reference
     * for the primary key. The same vector can appear in several clusters (one primary copy plus replicas), so a
     * reference is addressed by the {@code (clusterId, primaryKey)} pair, not the primary key alone.
     *
     * @param readTransaction the read transaction
     * @param storageTransform the transform used to reconstruct the stored vector
     * @param clusterId the id of the cluster
     * @param primaryKey the primary key of the reference within the cluster
     * @return a future of the reference, or {@code null} if absent from this cluster
     */
    @Nonnull
    CompletableFuture<VectorReference> fetchVectorReference(@Nonnull final ReadTransaction readTransaction,
                                                            @Nonnull final StorageTransform storageTransform,
                                                            @Nonnull final UUID clusterId,
                                                            @Nonnull final Tuple primaryKey) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, primaryKey));

        return getOnReadListener().onAsyncRead(readTransaction.get(key))
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null;
                    }
                    final VectorReference vectorReference =
                            StorageAdapter.vectorReferenceFromTuples(getConfig(), storageTransform,
                                    primaryKey, Tuple.fromBytes(valueBytes));
                    getOnReadListener().onVectorRead(clusterId, vectorReference.id().primaryKey(),
                            vectorReference.id().uuid(), vectorReference.vector());
                    return vectorReference;
                });
    }

    /**
     * Writes a {@link VectorReference} into a cluster, encoding (quantizing) its vector for storage on the way in.
     * The {@link Quantizer} is supplied by the caller so that a batch of writes can share one resolved quantizer.
     *
     * @param transaction the transaction to write within
     * @param quantizer the quantizer used to encode the stored vector
     * @param clusterId the id of the cluster to write the reference into
     * @param vectorReference the reference to persist
     */
    void writeVectorReference(@Nonnull final Transaction transaction,
                              @Nonnull final Quantizer quantizer,
                              @Nonnull final UUID clusterId,
                              @Nonnull final VectorReference vectorReference) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, vectorReference.id().primaryKey()));
        final byte[] value = StorageAdapter.valueTupleFromVectorReference(quantizer, vectorReference).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    /**
     * Removes one {@link VectorReference} from a cluster — for example when a vector is reassigned to another
     * cluster or a replica is pruned.
     *
     * @param transaction the transaction to write within
     * @param clusterId the id of the cluster
     * @param primaryKey the primary key of the reference to remove
     */
    void deleteVectorReference(@Nonnull final Transaction transaction,
                               @Nonnull final UUID clusterId,
                               @Nonnull final Tuple primaryKey) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, primaryKey));

        getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    /**
     * Drops every {@link VectorReference} of a cluster in a single range clear — the bulk counterpart to
     * {@link #deleteVectorReference}, used when an entire cluster is dissolved rather than edited vector-by-vector.
     *
     * @param transaction the transaction to write within
     * @param clusterId the id of the cluster being dissolved
     */
    void deleteVectorReferencesForCluster(@Nonnull final Transaction transaction,
                                          @Nonnull final UUID clusterId) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterId));
        final Range range = Range.startsWith(rangeKey);

        getOnWriteListener().onRangeDeleted(range);
        transaction.clear(range);
    }

    /**
     * Materializes the members of a collapsed set into a list. Collapsing replaces many identical vectors with a
     * single representative reference; this expands that representative back into its individual member ids.
     *
     * @param readTransaction the read transaction
     * @param signature the content signature identifying the collapsed set
     * @return a future of the collapsed members' vector ids
     */
    @Nonnull
    CompletableFuture<List<VectorId>> fetchCollapsedVectorIds(@Nonnull final ReadTransaction readTransaction,
                                                              @Nonnull final UUID signature) {
        return AsyncUtil.collect(fetchCollapsedVectorIdsIterable(readTransaction, signature),
                getExecutor());
    }

    /**
     * Streams the members of a collapsed set — the individual vectors hidden behind one collapsed representative —
     * by scanning the signature's key range. A set can be large, so this streaming form is what search uses when it
     * has to expand a collapsed reference back into its members.
     *
     * @param readTransaction the read transaction
     * @param signature the content signature identifying the collapsed set
     * @return an iterable over the collapsed members' vector ids
     */
    @Nonnull
    AsyncIterable<VectorId> fetchCollapsedVectorIdsIterable(@Nonnull final ReadTransaction readTransaction,
                                                            @Nonnull final UUID signature) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] rangeKey = collapsedVectorIdsSubspace.pack(Tuple.from(signature));

        return AsyncUtil.mapIterable(getOnReadListener().onAsyncReadRange(
                        readTransaction.getRange(Range.startsWith(rangeKey),
                                ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)),
                keyValue -> {
                    final Tuple primaryKey = collapsedVectorIdsSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                    final byte[] keyBytes = keyValue.getKey();
                    final byte[] valueBytes = keyValue.getValue();
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    return StorageAdapter.collapsedVectorIdFromValueTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    /**
     * Point-fetches the collapsed-store entry for a single {@code (signature, primaryKey)}, returning the stored
     * {@link VectorId} (the primary key plus the collapsed member's uuid) or {@code null} when that vector is not
     * part of a collapsed set. Cheaper than {@link #fetchCollapsedVectorIds} when only one membership needs to be
     * checked, as a collapsed set can hold a large number of members.
     *
     * @param readTransaction the read transaction
     * @param signature the content signature shared by the collapsed members
     * @param primaryKey the primary key whose membership is being checked
     * @return a future of the stored {@link VectorId}, or {@code null} if there is no such collapsed entry
     */
    @Nonnull
    CompletableFuture<VectorId> fetchCollapsedVectorId(@Nonnull final ReadTransaction readTransaction,
                                                       @Nonnull final UUID signature,
                                                       @Nonnull final Tuple primaryKey) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] key = collapsedVectorIdsSubspace.pack(Tuple.from(signature, primaryKey));

        return getOnReadListener().onAsyncRead(readTransaction.get(key))
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null; // not part of a collapsed set
                    }
                    return StorageAdapter.collapsedVectorIdFromValueTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    /**
     * Streams every collapsed-membership entry across all signatures (a full scan of the collapsed-vector-ids
     * subspace). Exposed only for tests/diagnostics that need to inspect the entire collapsed store; production
     * paths always scope the scan to a single signature via {@link #fetchCollapsedVectorIdsIterable}.
     *
     * @param readTransaction the read transaction
     * @return an iterable over all collapsed vector ids in the structure
     */
    @Nonnull
    @VisibleForTesting
    AsyncIterable<VectorId> scanCollapsedVectorIdsIterable(@Nonnull final ReadTransaction readTransaction) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] rangeKey = collapsedVectorIdsSubspace.pack();

        return AsyncUtil.mapIterable(getOnReadListener().onAsyncReadRange(
                        readTransaction.getRange(Range.startsWith(rangeKey),
                                ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)),
                keyValue -> {
                    final Tuple primaryKey = collapsedVectorIdsSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                    final byte[] keyBytes = keyValue.getKey();
                    final byte[] valueBytes = keyValue.getValue();
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    return StorageAdapter.collapsedVectorIdFromValueTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    /**
     * Records that a vector is a member of the collapsed set for {@code signature} — i.e. it is represented by, and
     * folded behind, that signature's collapsed reference rather than carrying its own reference in a cluster.
     *
     * @param transaction the transaction to write within
     * @param signature the content signature of the collapsed set
     * @param vectorId the vector recorded as a collapsed member
     */
    void writeCollapsedVectorId(@Nonnull final Transaction transaction,
                                @Nonnull final UUID signature,
                                @Nonnull final VectorId vectorId) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] key = collapsedVectorIdsSubspace.pack(Tuple.from(signature, vectorId.primaryKey()));
        final byte[] value = StorageAdapter.valueTupleFromCollapsedVectorId(vectorId).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    /**
     * Removes a single collapsed-membership entry — for example when a folded vector is deleted, or re-expanded out
     * of its collapsed set.
     *
     * @param transaction the transaction to write within
     * @param signature the content signature of the collapsed set
     * @param primaryKey the primary key of the member to remove
     */
    void deleteCollapsedVectorId(@Nonnull final Transaction transaction,
                                 @Nonnull final UUID signature,
                                 @Nonnull final Tuple primaryKey) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] key = collapsedVectorIdsSubspace.pack(Tuple.from(signature, primaryKey));

        getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    /**
     * Drains up to {@code numTasks} pending maintenance tasks and runs them inline within this transaction. This is
     * how background structure maintenance is paid for: each foreground insert/delete absorbs a small, bounded slice
     * of queued work rather than relying on a separate sweeper.
     *
     * @param transaction the transaction to fetch and run the tasks within
     * @param accessInfo the access context passed to each rehydrated task
     * @param numTasks the maximum number of tasks to run
     * @return a future that completes when the fetched tasks have all run
     */
    @Nonnull
    CompletableFuture<Void> executeSomeDeferredTasks(@Nonnull final Transaction transaction,
                                                     @Nonnull final AccessInfo accessInfo,
                                                     final int numTasks) {
        return fetchSomeDeferredTasks(transaction, accessInfo, numTasks)
                .thenCompose(deferredTasks ->
                        forLoop(0, null,
                                i -> i < deferredTasks.size(), i -> i + 1,
                                (i, ignored) ->
                                        executeSingleDeferredTask(transaction, deferredTasks.get(i)), getExecutor()));
    }

    /**
     * Runs one deferred task. The task is removed from the queue <em>before</em> it runs (not after), so a task that
     * re-enqueues follow-up work cannot collide with its own still-present queue entry — and so a task is free to
     * reuse its own just-freed slot. On success the {@link OnWriteListener} is notified; on failure the future
     * completes exceptionally and the enclosing transaction will not commit, so the removal is rolled back with it.
     *
     * @param transaction the transaction to run the task within
     * @param deferredTask the task to execute
     * @return a future that completes when the task has run
     */
    @Nonnull
    CompletableFuture<Void> executeSingleDeferredTask(@Nonnull final Transaction transaction,
                                                      @Nonnull final AbstractDeferredTask deferredTask) {
        deleteDeferredTask(transaction, deferredTask);
        return deferredTask.runTask(transaction)
                .whenComplete((ignored, throwable) -> {
                    if (throwable == null) {
                        getOnWriteListener().onTaskExecuted(deferredTask.getKind(),
                                deferredTask.getTaskId(), deferredTask.getTargetClusterIds());
                    }
                });
    }

    /**
     * Fetches up to {@code numTasks} queued tasks in ascending task-id order and rehydrates each from its stored
     * tuples. The order is meaningful: a task id's high bit encodes priority (see {@link AbstractDeferredTask}), so
     * the lowest ids — the high-priority tasks — are returned first.
     *
     * @param readTransaction the read transaction
     * @param accessInfo the access context passed to each rehydrated task
     * @param numTasks the maximum number of tasks to fetch
     * @return a future of the fetched tasks, in queue (priority) order
     */
    @Nonnull
    CompletableFuture<List<AbstractDeferredTask>> fetchSomeDeferredTasks(@Nonnull final ReadTransaction readTransaction,
                                                                         @Nonnull final AccessInfo accessInfo,
                                                                         final int numTasks) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] rangeKey = tasksSubspace.pack();

        return AsyncUtil.collect(getOnReadListener().onAsyncReadRange(
                        readTransaction.getRange(Range.startsWith(rangeKey), numTasks, false,
                                StreamingMode.WANT_ALL)), getExecutor())
                .thenApply(keyValues -> {
                    final ImmutableList.Builder<AbstractDeferredTask> deferredTasksBuilder = ImmutableList.builder();
                    for (final KeyValue keyValue : keyValues) {
                        final byte[] keyBytes = keyValue.getKey();
                        final byte[] valueBytes = keyValue.getValue();
                        final Tuple keyTuple = tasksSubspace.unpack(keyValue.getKey());
                        final Tuple valueTuple = Tuple.fromBytes(valueBytes);
                        deferredTasksBuilder.add(AbstractDeferredTask.newFromTuples(getLocator(), accessInfo,
                                keyTuple, valueTuple));
                        getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    }
                    return deferredTasksBuilder.build();
                });
    }

    /**
     * Point-fetches a queued task by id, or {@code null} if it is no longer enqueued — for example a dependency a
     * {@link BounceTask} is waiting on that has already run. Returning {@code null} rather than failing lets callers
     * treat "already done" as a normal outcome.
     *
     * @param readTransaction the read transaction
     * @param accessInfo the access context passed to the rehydrated task
     * @param taskId the id of the task
     * @return a future of the task, or {@code null} if it is not currently enqueued
     */
    @Nonnull
    CompletableFuture<AbstractDeferredTask> fetchDeferredTask(@Nonnull final ReadTransaction readTransaction,
                                                              @Nonnull final AccessInfo accessInfo,
                                                              @Nonnull final UUID taskId) {
        final Subspace tasksSubspace = getTasksSubspace();
        final Tuple keyTuple = Tuple.from(taskId);
        final byte[] keyBytes = tasksSubspace.pack(keyTuple);

        return getOnReadListener().onAsyncRead(readTransaction.get(keyBytes))
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    if (valueBytes == null) {
                        return null;
                    }

                    final Tuple valueTuple = Tuple.fromBytes(valueBytes);
                    return AbstractDeferredTask.newFromTuples(getLocator(), accessInfo, keyTuple, valueTuple);
                });
    }

    /**
     * Enqueues a deferred task by writing its serialized value under its task id — the key that addresses the task
     * within the task subspace, which behaves as a priority queue ordered by that key. The id's high bit fixes the
     * task's scheduling priority relative to the others (see {@link AbstractDeferredTask}).
     *
     * @param transaction the transaction to write within
     * @param taskId the task id the task is stored under
     * @param valueTuple the task's serialized value
     */
    void writeDeferredTask(@Nonnull final Transaction transaction,
                           @Nonnull final UUID taskId,
                           @Nonnull final Tuple valueTuple) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] key = tasksSubspace.pack(Tuple.from(taskId));
        final byte[] value = valueTuple.pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    /**
     * Applies a set of vector-count changes to a cluster's metadata and, if adding primary vectors pushed the
     * cluster over {@code primaryClusterMax}, enqueues a {@link SplitMergeTask} to split it; otherwise it
     * enqueues a {@link ReassignTask} or simply writes the updated metadata, as warranted.
     * <p>
     * This method <em>never</em> enqueues a merge. A merge is triggered only by deleting a primary vector and
     * is handled separately by {@link #updateClusterMetadataAndEnqueueMergeTaskMaybe}, because deciding whether a
     * merge is even possible requires an asynchronous read of the centroid HNSW. Callers that remove vectors
     * (a replicated delete, or the non-merge fallback of the primary-delete path) may still call this method —
     * it will reassign or plain-write the decrement — but it will not split or merge them.
     *
     * @param transaction the transaction to write the updated metadata and any enqueued task into
     * @param random source of randomness for the id of any enqueued task
     * @param clusterMetadata the current metadata of the cluster being updated
     * @param clusterCentroid the transformed centroid of the cluster, carried into any task that is enqueued
     * @param accessInfo the access context (subspace layout) of the structure
     * @param numPrimaryVectorsAdded the number of primary vectors added to the cluster (must be {@code >= 0} here)
     * @param numPrimaryUnderreplicatedVectorsAdded the change in the number of primary underreplicated vectors
     * @param numReplicatedVectorsAdded the change in the number of replicated vectors
     * @param updatedStandardDeviation the recomputed running statistics of member distances to the centroid
     * @param causeClusterIds the ids of clusters produced by the operation that triggered this update (for example
     *        the clusters a split created); used by the reassign tail to decide whether a {@link ReassignTask} is needed
     *
     * @return the id of an enqueued task, or {@link Optional#empty()} if none was enqueued
     */
    @Nonnull
    Optional<UUID> updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe(@Nonnull final Transaction transaction,
                                                                           @Nonnull final SplittableRandom random,
                                                                           @Nonnull final ClusterMetadata clusterMetadata,
                                                                           @Nonnull final Transformed<RealVector> clusterCentroid,
                                                                           @Nonnull final AccessInfo accessInfo,
                                                                           final int numPrimaryVectorsAdded,
                                                                           final int numPrimaryUnderreplicatedVectorsAdded,
                                                                           final int numReplicatedVectorsAdded,
                                                                           @Nonnull final RunningStats updatedStandardDeviation,
                                                                           @Nonnull final Set<UUID> causeClusterIds) {
        Verify.verify(numPrimaryVectorsAdded >= 0,
                "updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe only handles added primary vectors");
        final Config config = getConfig();

        final int numTotalPrimaryVectors = clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded;
        if (!clusterMetadata.states().contains(ClusterMetadata.State.SPLIT_MERGE) && // not already splitting/merging
                !clusterMetadata.states().contains(ClusterMetadata.State.COLLAPSE) && // not already collapsing
                numPrimaryVectorsAdded > 0 && numTotalPrimaryVectors > config.primaryClusterMax()) {
            // adding primary vectors pushed the cluster over the maximum: enqueue a split
            final UUID newTaskId =
                    updateClusterMetadataAndEnqueueSplitMergeTask(transaction, random, clusterMetadata, clusterCentroid,
                            accessInfo, numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
                            updatedStandardDeviation);
            if (logger.isDebugEnabled()) {
                logger.debug("enqueued SPLIT_MERGE (split) due to number of primary vectors, taskId={}, numPrimaryVectors={}",
                        newTaskId, numTotalPrimaryVectors);
            }
            return Optional.of(newTaskId);
        }

        // Not a split: fall through to the shared reassign / plain-write handling.
        return updateClusterMetadataAndEnqueueReassignTaskMaybe(transaction, random, clusterMetadata,
                clusterCentroid, accessInfo, numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded,
                numReplicatedVectorsAdded, updatedStandardDeviation, causeClusterIds);
    }

    /**
     * Shared tail for {@link #updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe} and
     * {@link #updateClusterMetadataAndEnqueueMergeTaskMaybe} for the case where the change neither splits nor
     * merges the cluster: it enqueues a {@link ReassignTask} if the cluster now violates a replication invariant
     * (or is a cluster we just split into), otherwise it just persists the updated metadata. Unlike the split
     * path, this accepts a negative primary delta (a primary was deleted) and writes it through.
     *
     * @param transaction the transaction to write the updated metadata and any enqueued reassign task into
     * @param random source of randomness for the id of an enqueued reassign task
     * @param clusterMetadata the current metadata of the cluster being updated
     * @param clusterCentroid the transformed centroid of the cluster, carried into an enqueued {@link ReassignTask}
     * @param accessInfo the access context (subspace layout) of the structure
     * @param numPrimaryVectorsAdded the change in the number of primary vectors (may be negative for a deletion)
     * @param numPrimaryUnderreplicatedVectorsAdded the change in the number of primary underreplicated vectors
     * @param numReplicatedVectorsAdded the change in the number of replicated vectors
     * @param updatedStandardDeviation the recomputed running statistics of member distances to the centroid
     * @param causeClusterIds the ids of clusters produced by the operation that triggered this update (for example
     *        the clusters a split created); a cluster in this set is never reassigned, and a non-empty set forces a
     *        reassign of clusters that are not in it
     *
     * @return the id of an enqueued reassign task, or {@link Optional#empty()} if none was enqueued
     */
    @Nonnull
    private Optional<UUID> updateClusterMetadataAndEnqueueReassignTaskMaybe(@Nonnull final Transaction transaction,
                                                                            @Nonnull final SplittableRandom random,
                                                                            @Nonnull final ClusterMetadata clusterMetadata,
                                                                            @Nonnull final Transformed<RealVector> clusterCentroid,
                                                                            @Nonnull final AccessInfo accessInfo,
                                                                            final int numPrimaryVectorsAdded,
                                                                            final int numPrimaryUnderreplicatedVectorsAdded,
                                                                            final int numReplicatedVectorsAdded,
                                                                            @Nonnull final RunningStats updatedStandardDeviation,
                                                                            @Nonnull final Set<UUID> causeClusterIds) {
        final Config config = getConfig();
        final UUID clusterId = clusterMetadata.id();

        final int numTotalPrimaryVectors = clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded;
        int numTotalPrimaryUnderreplicatedVectors = clusterMetadata.numPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded;
        int numTotalReplicatedVectors = clusterMetadata.numReplicatedVectors() + numReplicatedVectorsAdded;

        if (!clusterMetadata.states().contains(ClusterMetadata.State.REASSIGN) &&  // not already reassigning
                !causeClusterIds.contains(clusterId) &&                            // cannot be a cluster we just split into
                (!causeClusterIds.isEmpty() ||                                                                  // either we just split or
                         numTotalReplicatedVectors > config.replicatedClusterMaxWrites() ||                     // we are violating some clean up bounds
                         numTotalPrimaryUnderreplicatedVectors > config.underreplicatedPrimaryClusterMax())) {
            // create a reassign task
            final UUID newTaskId =
                    AbstractDeferredTask.randomNormalPriorityTaskId(random, config.deterministicRandomness());
            final ReassignTask newReassignTask = ReassignTask.of(getLocator(), accessInfo, newTaskId,
                    clusterId, clusterCentroid, causeClusterIds);
            newReassignTask.writeDeferredTask(transaction);

            if (logger.isTraceEnabled()) {
                final String reason = causeClusterIds.isEmpty() ? "due to violated invariance" : "due to SPLIT";
                logger.trace(
                        """
                        enqueued REASSIGN {}; taskId={}; numTotalPrimaryVectors={}, \
                        numTotalReplicatedVectors={}, numTotalPrimaryUnderreplicatedVectors={} \
                        """,
                        reason, AbstractDeferredTask.taskIdToString(newTaskId), numTotalPrimaryVectors,
                        numTotalReplicatedVectors, numTotalPrimaryUnderreplicatedVectors);
            }

            final ClusterMetadata newClusterMetadata =
                    clusterMetadata.withAdditionalVectorsAndStates(numPrimaryUnderreplicatedVectorsAdded,
                            numReplicatedVectorsAdded, updatedStandardDeviation,
                            EnumSet.of(ClusterMetadata.State.REASSIGN));
            writeClusterMetadata(transaction, newClusterMetadata);

            return Optional.of(newTaskId);
        }

        if (numPrimaryVectorsAdded != 0 || numReplicatedVectorsAdded != 0) {
            // write new metadata but do not create a task
            final ClusterMetadata newClusterMetadata =
                    clusterMetadata.withAdditionalVectors(numPrimaryUnderreplicatedVectorsAdded,
                            numReplicatedVectorsAdded, updatedStandardDeviation);
            writeClusterMetadata(transaction, newClusterMetadata);
        }
        return Optional.empty();
    }

    /**
     * Updates a cluster's metadata after a single primary vector has been deleted from it and, when that drops
     * the cluster below {@code primaryClusterMin}, enqueues a merge {@link SplitMergeTask} — but only when a
     * merge is actually possible.
     * <p>
     * A merge needs at least one other cluster to merge with. The clusters are exactly the nodes of the centroid
     * HNSW, so this consults {@link HNSW#cardinality(com.apple.foundationdb.ReadTransaction)} and enqueues a
     * merge only when layer 0 holds two or more nodes ({@link Cardinality#MULTIPLE}). For a lone cluster below
     * the minimum — as happens when a structure is drained toward empty — there is nothing to merge with;
     * enqueuing a merge anyway would be futile (k-means would be asked for {@code k == 0}) and setting the
     * {@link ClusterMetadata.State#SPLIT_MERGE} flag would both stick and churn one impossible task per delete.
     * In that case the decrement is merely persisted via
     * {@link #updateClusterMetadataAndEnqueueReassignTaskMaybe}.
     * <p>
     * This is the only path that can enqueue a merge.
     *
     * @param transaction the transaction to use
     * @param random a source of randomness used to mint a task id
     * @param clusterMetadata the metadata of the cluster the primary was deleted from (pre-decrement)
     * @param clusterCentroid the transformed centroid of that cluster
     * @param accessInfo the current access info
     * @param updatedStandardDeviation the running stats already reflecting the removed primary (this carries the
     *        decremented primary-vector count)
     *
     * @return a future that completes once the metadata has been written and any task enqueued
     */
    @Nonnull
    CompletableFuture<Void> updateClusterMetadataAndEnqueueMergeTaskMaybe(@Nonnull final Transaction transaction,
                                                                         @Nonnull final SplittableRandom random,
                                                                         @Nonnull final ClusterMetadata clusterMetadata,
                                                                         @Nonnull final Transformed<RealVector> clusterCentroid,
                                                                         @Nonnull final AccessInfo accessInfo,
                                                                         @Nonnull final RunningStats updatedStandardDeviation) {
        final Config config = getConfig();

        // A single primary vector was just deleted, i.e. numPrimaryVectorsAdded == -1.
        final int numTotalPrimaryVectors = clusterMetadata.getNumPrimaryVectors() - 1;
        final boolean wantsMerge =
                !clusterMetadata.states().contains(ClusterMetadata.State.SPLIT_MERGE) && // not already splitting/merging
                        !clusterMetadata.states().contains(ClusterMetadata.State.COLLAPSE) && // not already collapsing
                        numTotalPrimaryVectors < config.primaryClusterMin();
        if (!wantsMerge) {
            // Either the cluster is still within bounds or it already has a pending task; just persist the
            // decrement (this may reassign or plain-write, but cannot merge).
            updateClusterMetadataAndEnqueueReassignTaskMaybe(transaction, random, clusterMetadata,
                    clusterCentroid, accessInfo, -1, 0, 0, updatedStandardDeviation, Set.of());
            return AsyncUtil.DONE;
        }

        return getClusterCentroidsHnsw().cardinality(transaction)
                .thenAccept(cardinality -> {
                    if (cardinality == Cardinality.MULTIPLE) {
                        final UUID newTaskId =
                                updateClusterMetadataAndEnqueueSplitMergeTask(transaction, random, clusterMetadata, clusterCentroid,
                                        accessInfo, 0, 0, updatedStandardDeviation);
                        if (logger.isDebugEnabled()) {
                            logger.debug("enqueued SPLIT_MERGE (merge) due to number of primary vectors, taskId={}, numPrimaryVectors={}",
                                    newTaskId, numTotalPrimaryVectors);
                        }
                    } else {
                        // Lone cluster below the minimum: nothing to merge with. Persist the decrement only and
                        // do not set SPLIT_MERGE (avoids a stuck flag and a churn of impossible merge tasks).
                        if (logger.isDebugEnabled()) {
                            logger.debug("skipping merge enqueue: cluster={} is below the minimum but has no mergeable neighbor; centroidCardinality={}",
                                    clusterMetadata.id(), cardinality);
                        }
                        updateClusterMetadataAndEnqueueReassignTaskMaybe(transaction, random, clusterMetadata,
                                clusterCentroid, accessInfo, -1, 0, 0, updatedStandardDeviation, Set.of());
                    }
                });
    }

    /**
     * Enqueues a {@link SplitMergeTask} for the given cluster and writes its metadata with the
     * {@link ClusterMetadata.State#SPLIT_MERGE} state set. This is the shared body used both when adding vectors
     * pushes a cluster over {@code primaryClusterMax} (a split) and when deleting a primary drops it below
     * {@code primaryClusterMin} (a merge); the task itself decides at execution time whether to split or merge
     * based on the cluster's size when it runs.
     *
     * @param transaction the transaction to use
     * @param random a source of randomness used to mint a task id
     * @param clusterMetadata the metadata of the cluster the task is enqueued for
     * @param clusterCentroid the transformed centroid of that cluster
     * @param accessInfo the current access info
     * @param numPrimaryUnderreplicatedVectorsAdded delta of primary-underreplicated vectors to apply
     * @param numReplicatedVectorsAdded delta of replicated vectors to apply
     * @param updatedStandardDeviation the running stats to write (carries the updated primary-vector count)
     *
     * @return the id of the enqueued task
     */
    @Nonnull
    UUID updateClusterMetadataAndEnqueueSplitMergeTask(@Nonnull final Transaction transaction,
                                                       @Nonnull final SplittableRandom random,
                                                       @Nonnull final ClusterMetadata clusterMetadata,
                                                       @Nonnull final Transformed<RealVector> clusterCentroid,
                                                       @Nonnull final AccessInfo accessInfo,
                                                       final int numPrimaryUnderreplicatedVectorsAdded,
                                                       final int numReplicatedVectorsAdded,
                                                       @Nonnull final RunningStats updatedStandardDeviation) {
        final Config config = getConfig();
        final UUID newTaskId =
                AbstractDeferredTask.randomNormalPriorityTaskId(random, config.deterministicRandomness());
        final SplitMergeTask newSplitMergeTask =
                SplitMergeTask.of(getLocator(), accessInfo, newTaskId, clusterMetadata.id(), clusterCentroid);
        newSplitMergeTask.writeDeferredTask(transaction);

        final ClusterMetadata newClusterMetadata =
                clusterMetadata.withAdditionalVectorsAndStates(numPrimaryUnderreplicatedVectorsAdded,
                        numReplicatedVectorsAdded, updatedStandardDeviation,
                        EnumSet.of(ClusterMetadata.State.SPLIT_MERGE));
        writeClusterMetadata(transaction, newClusterMetadata);

        return newTaskId;
    }

    /**
     * Removes a task's queue entry. Used by {@link #executeSingleDeferredTask} to claim a task before running it.
     *
     * @param transaction the transaction to write within
     * @param deferredTask the task whose queue entry is cleared
     */
    void deleteDeferredTask(@Nonnull final Transaction transaction,
                            @Nonnull final AbstractDeferredTask deferredTask) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] key = tasksSubspace.pack(Tuple.from(deferredTask.getTaskId()));

        transaction.clear(key);
        getOnWriteListener().onKeyDeleted(key);
    }

    /**
     * Fetches a target cluster's nearest clusters — that is the {@code numClusters} clusters whose centroids are nearest
     * the target's — as the candidate set a split/merge/reassign repartitions over. Walks the centroid {@link HNSW} in
     * ascending distance order; the target cluster itself is included at distance {@code 0} using the metadata
     * already in hand, avoiding a redundant re-read of the very cluster that triggered the work.
     *
     * @param transaction the read transaction
     * @param targetClusterMetadata the metadata of the cluster whose nearest clusters are being fetched
     * @param targetClusterCentroid the target's centroid in the untransformed (client) coordinate space
     * @param storageTransform the transform mapping fetched centroids into the stored coordinate space
     * @param numClusters the maximum number of nearest clusters to fetch (including the target)
     * @param concurrency the fan-out width for the per-cluster metadata reads
     * @return a future of the nearest clusters, nearest first
     */
    @Nonnull
    CompletableFuture<List<ClusterMetadataWithDistance>>
            fetchNearestClusterMetadata(@Nonnull final ReadTransaction transaction,
                                        @Nonnull final ClusterMetadata targetClusterMetadata,
                                        @Nonnull final RealVector targetClusterCentroid,
                                        @Nonnull final StorageTransform storageTransform,
                                        final int numClusters,
                                        final int concurrency) {
        final Executor executor = getLocator().getExecutor();

        return AsyncUtil.collect(
                MoreAsyncUtil.mapIterablePipelined(executor,
                        MoreAsyncUtil.limitIterable(MoreAsyncUtil.iterableOf(() ->
                                                centroidsOrderedByDistance(transaction,
                                                        targetClusterCentroid, 0.0d, null,
                                                        getConfig().constructionSearchConfig().centroidEfRingSearch(),
                                                        getConfig().constructionSearchConfig().centroidEfOutwardSearch()),
                                        executor),
                                numClusters, executor),
                        resultEntry -> {
                            final UUID clusterId = StorageAdapter.clusterIdFromTuple(resultEntry.primaryKey());
                            final Transformed<RealVector> transformedClusterCentroid =
                                    storageTransform.transform(Objects.requireNonNull(resultEntry.vector()));
                            if (clusterId.equals(targetClusterMetadata.id())) {
                                return CompletableFuture.completedFuture(new ClusterMetadataWithDistance(targetClusterMetadata,
                                        transformedClusterCentroid, 0.0d));
                            }
                            return fetchClusterMetadataWithDistance(transaction,
                                    clusterId,
                                    transformedClusterCentroid,
                                    0.0d);
                        }, concurrency));
    }

    /**
     * Fully loads each of the core clusters — the clusters a split/merge is going to dissolve and
     * repartition — since repartitioning needs their actual vectors, not just their metadata and centroids.
     *
     * @param transaction the transaction
     * @param coreClusters the core clusters to load in full
     * @param storageTransform the transform used to reconstruct stored vectors
     * @param concurrency the fan-out width for the per-cluster loads
     * @return a future of the fully-loaded clusters
     */
    @Nonnull
    CompletableFuture<List<Cluster>> fetchCoreClusters(@Nonnull final Transaction transaction,
                                                        @Nonnull final List<ClusterMetadataWithDistance> coreClusters,
                                                        @Nonnull final StorageTransform storageTransform,
                                                        final int concurrency) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();

        return forEach(coreClusters,
                clusterMetadata ->
                        primitives.fetchCluster(transaction, storageTransform,
                                clusterMetadata.clusterMetadata().id(), clusterMetadata.centroid()),
                concurrency,
                executor);
    }

    /**
     * Collapses the references gathered from several clusters down to one current reference per vector, dropping
     * duplicates and stale copies. This is the correctness filter that keeps repartitioning from acting on vectors
     * that have since moved or been deleted: within a vector UUID, {@link #mergeVectorReference} keeps the best copy
     * (primary over replica, then higher replication priority), and a survivor is retained only if it is collapsed,
     * or its UUID still matches the vector's current {@link VectorMetadata}. When {@code discardReplicatedVectorReferences}
     * is set, replicas are dropped up front so only primary copies are considered.
     *
     * @param transaction the transaction used to re-read current metadata
     * @param clusters the clusters whose references are being reconciled
     * @param discardReplicatedVectorReferences whether to drop replicas and keep only primary copies
     * @param concurrency the fan-out width for the per-vector metadata re-reads
     * @return a future of the surviving, de-duplicated, still-current references
     */
    @Nonnull
    CompletableFuture<List<VectorReference>> cleanUpVectorReferences(@Nonnull final Transaction transaction,
                                                                     @Nonnull final List<Cluster> clusters,
                                                                     final boolean discardReplicatedVectorReferences,
                                                                     final int concurrency) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();

        final Map<VectorId, VectorReference> vectorsByIdMap = Maps.newHashMap();
        for (final Cluster cluster : clusters) {
            for (final VectorReference vectorReference : cluster.vectorReferences()) {
                if (!discardReplicatedVectorReferences || vectorReference.isPrimaryCopy()) {
                    vectorsByIdMap.compute(vectorReference.id(),
                            (vectorId, oldVectorReference) ->
                                    mergeVectorReference(oldVectorReference, vectorReference));
                }
            }
        }

        return forEach(vectorsByIdMap.values(),
                vectorReference ->
                        primitives.fetchVectorMetadata(transaction, vectorReference.id().primaryKey())
                                .thenApply(vectorMetadata ->
                                        vectorReference.isCollapsed() ||
                                                (vectorMetadata != null &&
                                                         vectorMetadata.vectorId().uuid()
                                                                 .equals(vectorReference.id().uuid()))
                                        ? vectorReference : null),
                concurrency,
                executor)
                .thenApply(vectorReferences -> {
                    final ImmutableList.Builder<VectorReference> nonnullReferencesBuilder = ImmutableList.builder();
                    for (final VectorReference vectorReference : vectorReferences) {
                        if (Objects.nonNull(vectorReference)) {
                            nonnullReferencesBuilder.add(vectorReference);
                        }
                    }
                    return nonnullReferencesBuilder.build();
                });
    }

    /**
     * Resolves which of two {@link VectorReference}s sharing the same vector UUID to keep when de-duplicating:
     * prefer a primary over a replica, warn on a duplicate primary, otherwise keep the higher replication priority.
     */
    @Nonnull
    private static VectorReference mergeVectorReference(@Nullable final VectorReference oldVectorReference,
                                                        @Nonnull final VectorReference vectorReference) {
        if (oldVectorReference == null) {
            return vectorReference;
        }
        if (vectorReference.isPrimaryCopy()) {
            if (oldVectorReference.isPrimaryCopy()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("duplicate primary vector references of the same vector reference, vectorReference={}", oldVectorReference);
                }
                return oldVectorReference;
            }
            return vectorReference;
        }
        // both of them are replicated vector references -- take the one with the higher
        // replication priority
        return oldVectorReference.replicationPriority() > vectorReference.replicationPriority()
               ? oldVectorReference
               : vectorReference;
    }

    /**
     * Bundles the resolved {@link AccessInfo} for an operation with whether the relevant centroid node already
     * exists.
     *
     * @param accessInfo the access context, or {@code null} if it could not be resolved
     * @param nodeExists {@code true} if the node already exists in the centroid HNSW
     */
    record AccessInfoAndNodeExistence(@Nullable AccessInfo accessInfo, boolean nodeExists) {
    }
}
