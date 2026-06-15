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

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    @Nonnull
    StorageAdapter getStorageAdapter() {
        return locator.getStorageAdapter();
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    private Subspace getSubspace() {
        return getLocator().getSubspace();
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

    @Nonnull
    Subspace getAccessInfoSubspace() {
        return getStorageAdapter().getAccessInfoSubspace();
    }

    @Nonnull
    Subspace getClusterCentroidsSubspace() {
        return getStorageAdapter().getClusterCentroidsSubspace();
    }

    @Nonnull
    Subspace getClusterMetadataSubspace() {
        return getStorageAdapter().getClusterMetadataSubspace();
    }

    @Nonnull
    Subspace getVectorReferencesSubspace() {
        return getStorageAdapter().getVectorReferencesSubspace();
    }

    @Nonnull
    Subspace getCollapsedVectorIdsSubspace() {
        return getStorageAdapter().getCollapsedVectorIdsSubspace();
    }

    @Nonnull
    Subspace getVectorMetadataSubspace() {
        return getStorageAdapter().getVectorMetadataSubspace();
    }

    @Nonnull
    Subspace getSamplesSubspace() {
        return getStorageAdapter().getSamplesSubspace();
    }

    @Nonnull
    Subspace getTasksSubspace() {
        return getStorageAdapter().getTasksSubspace();
    }

    @Nonnull
    HNSW getClusterCentroidsHnsw() {
        return clusterCentroidsHnswSupplier.get();
    }

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

    boolean isMetricNeedsNormalizedVectors() {
        return getConfig().metric() == Metric.COSINE_METRIC;
    }

    @Nonnull
    StorageTransform storageTransform(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return StorageTransform.identity();
        }

        return storageTransform(accessInfo.rotatorSeed(),
                Objects.requireNonNull(accessInfo.negatedCentroid()),
                isMetricNeedsNormalizedVectors());
    }

    @Nonnull
    StorageTransform storageTransform(@Nullable final Long rotatorSeed,
                                      @Nullable final RealVector negatedCentroid,
                                      final boolean normalizeVectors) {
        final LinearOperator linearOperator =
                rotatorSeed == null
                ? null : new FhtKacRotator(rotatorSeed, getConfig().numDimensions(), 10);

        return new StorageTransform(linearOperator, negatedCentroid, normalizeVectors);
    }

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

    @Nonnull
    CompletableFuture<AccessInfo> fetchAccessInfo(@Nonnull final ReadTransaction readTransaction) {
        final Subspace accessInfoSubspace = getAccessInfoSubspace();
        final byte[] key = accessInfoSubspace.pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null; // not a single vector in the index
                    }
                    return StorageAdapter.accessInfoFromTuple(getConfig(), Tuple.fromBytes(valueBytes));
                });
    }

    void writeAccessInfo(@Nonnull final Transaction transaction,
                         @Nonnull final AccessInfo accessInfo) {
        final Subspace accessInfoSubspace = getAccessInfoSubspace();
        final byte[] key = accessInfoSubspace.pack();
        final byte[] value = StorageAdapter.tupleFromAccessInfo(accessInfo).pack();
        transaction.set(key, value);
        getOnWriteListener().onKeyValueWritten(key, value);
    }

    @Nonnull
    CompletableFuture<Boolean> exists(@Nonnull final ReadTransaction readTransaction, final Tuple primaryKey) {
        return fetchVectorMetadata(readTransaction, primaryKey).thenApply(Objects::nonNull);
    }

    @Nonnull
    CompletableFuture<VectorMetadata> fetchVectorMetadata(@Nonnull final ReadTransaction readTransaction,
                                                          @Nonnull final Tuple primaryKey) {
        final Subspace vectorStatesSubspace = getVectorMetadataSubspace();
        final byte[] key = vectorStatesSubspace.pack(primaryKey);

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null; // unable to find vector
                    }
                    return StorageAdapter.vectorMetadataFromTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    void writeVectorMetadata(@Nonnull final Transaction transaction,
                             @Nonnull final VectorMetadata vectorMetadata) {
        final Subspace vectorMetadataSubspace = getVectorMetadataSubspace();
        final byte[] key = vectorMetadataSubspace.pack(vectorMetadata.getPrimaryKey());
        final byte[] value = StorageAdapter.valueTupleFromVectorMetadata(vectorMetadata).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    void deleteVectorMetadata(@Nonnull final Transaction transaction,
                              @Nonnull final Tuple primaryKey) {
        final Subspace vectorMetadataSubspace = getVectorMetadataSubspace();
        final byte[] key = vectorMetadataSubspace.pack(primaryKey);
        getLocator().getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    @Nonnull
    AsyncIterator<ResultEntry> centroidsOrderedByDistance(@Nonnull final ReadTransaction readTransaction,
                                                          @Nonnull final RealVector centerVector,
                                                          final double minimumRadius,
                                                          @Nullable final Tuple minimumPrimaryKey) {
        final HNSW centroidsHnsw = getClusterCentroidsHnsw();

        return centroidsHnsw.orderByDistance(readTransaction, 100, 400, true,
                centerVector, minimumRadius, minimumPrimaryKey, true);
    }

    @Nonnull
    CompletableFuture<ClusterMetadataWithDistance> fetchClusterMetadataWithDistance(@Nonnull final ReadTransaction readTransaction,
                                                                                    @Nonnull final UUID clusterId,
                                                                                    @Nonnull final Transformed<RealVector> centroid,
                                                                                    final double distance) {
        return fetchClusterMetadata(readTransaction, clusterId)
                .thenApply(clusterState -> new ClusterMetadataWithDistance(clusterState, centroid, distance));
    }

    @Nonnull
    CompletableFuture<Cluster> fetchCluster(@Nonnull final ReadTransaction readTransaction,
                                            @Nonnull final StorageTransform storageTransform,
                                            @Nonnull final UUID clusterId,
                                            @Nonnull final RealVector centroid) {
        final Transformed<RealVector> transformedCentroid = storageTransform.transform(centroid);
        return fetchCluster(readTransaction, storageTransform, clusterId, transformedCentroid);
    }

    @Nonnull
    CompletableFuture<Cluster> fetchCluster(@Nonnull final ReadTransaction readTransaction,
                                            @Nonnull final StorageTransform storageTransform,
                                            @Nonnull final UUID clusterId,
                                            @Nonnull final Transformed<RealVector> centroid) {
        return fetchClusterMetadata(readTransaction, clusterId)
                .thenCombine(fetchVectorReferences(readTransaction, storageTransform, clusterId),
                        (clusterMetadata, vectorReferences) -> {
                            // TODO remove
                            Verify.verify(clusterMetadata.getNumPrimaryVectors() ==
                                    vectorReferences.stream().filter(VectorReference::isPrimaryCopy).count());
                            return new Cluster(clusterMetadata, centroid, vectorReferences);
                        });
    }

    @Nonnull
    CompletableFuture<ClusterMetadata> fetchClusterMetadata(@Nonnull final ReadTransaction readTransaction,
                                                            @Nonnull final UUID clusterId) {
        final byte[] key = getClusterMetadataSubspace().pack(Tuple.from(clusterId));
        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null;
                    }
                    return StorageAdapter.clusterMetadataFromTuple(Tuple.fromBytes(valueBytes));
                });
    }

    void writeClusterMetadata(@Nonnull final Transaction transaction,
                              @Nonnull final ClusterMetadata clusterMetadata) {
        final Subspace clusterMetadataSubspace = getClusterMetadataSubspace();
        final byte[] key = clusterMetadataSubspace.pack(Tuple.from(clusterMetadata.id()));
        final byte[] value = StorageAdapter.valueTupleFromClusterMetadata(clusterMetadata).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    void deleteClusterMetadata(@Nonnull final Transaction transaction,
                               @Nonnull final UUID clusterId) {
        final Subspace clusterMetadataSubspace = getClusterMetadataSubspace();
        final byte[] key = clusterMetadataSubspace.pack(Tuple.from(clusterId));

        getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    @Nonnull
    CompletableFuture<List<VectorReference>> fetchVectorReferences(@Nonnull final ReadTransaction readTransaction,
                                                                   @Nonnull final StorageTransform storageTransform,
                                                                   @Nonnull final UUID clusterId) {
        return AsyncUtil.collect(fetchVectorReferencesIterable(readTransaction, storageTransform, clusterId),
                getExecutor());
    }

    @Nonnull
    AsyncIterable<VectorReference> fetchVectorReferencesIterable(@Nonnull final ReadTransaction readTransaction,
                                                                 @Nonnull final StorageTransform storageTransform,
                                                                 @Nonnull final UUID clusterId) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterId));

        return AsyncUtil.mapIterable(readTransaction.getRange(Range.startsWith(rangeKey),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL),
                keyValue -> {
                    final Tuple primaryKey = vectorReferencesSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                    final byte[] keyBytes = keyValue.getKey();
                    final byte[] valueBytes = keyValue.getValue();
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    return StorageAdapter.vectorReferenceFromTuples(getConfig(), storageTransform,
                            primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    @Nonnull
    CompletableFuture<VectorReference> fetchVectorReference(@Nonnull final ReadTransaction readTransaction,
                                                            @Nonnull final StorageTransform storageTransform,
                                                            @Nonnull final UUID clusterId,
                                                            @Nonnull final Tuple primaryKey) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, primaryKey));

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(key, valueBytes);
                    if (valueBytes == null) {
                        return null;
                    }
                    return StorageAdapter.vectorReferenceFromTuples(getConfig(), storageTransform,
                            primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    void writeVectorReferences(@Nonnull final Transaction transaction,
                               @Nonnull final Quantizer quantizer,
                               @Nonnull final UUID clusterId,
                               @Nonnull final Iterable<VectorReference> vectorReferences) {
        for (final VectorReference vectorReference : vectorReferences) {
            writeVectorReference(transaction, quantizer, clusterId, vectorReference);
        }
    }

    void writeVectorReference(@Nonnull final Transaction transaction,
                              @Nonnull final Quantizer quantizer,
                              @Nonnull final UUID clusterId,
                              @Nonnull final VectorReference vectorReference) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, vectorReference.id().getPrimaryKey()));
        final byte[] value = StorageAdapter.valueTupleFromVectorReference(quantizer, vectorReference).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    void deleteVectorReference(@Nonnull final Transaction transaction,
                               @Nonnull final UUID clusterId,
                               @Nonnull final Tuple primaryKey) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, primaryKey));

        getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    void deleteVectorReferencesForCluster(@Nonnull final Transaction transaction,
                                          @Nonnull final UUID clusterId) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterId));
        final Range range = Range.startsWith(rangeKey);

        getOnWriteListener().onRangeDeleted(range);
        transaction.clear(range);
    }

    @Nonnull
    CompletableFuture<List<VectorId>> fetchCollapsedVectorIds(@Nonnull final ReadTransaction readTransaction,
                                                              @Nonnull final UUID signature) {
        return AsyncUtil.collect(fetchCollapsedVectorIdsIterable(readTransaction, signature),
                getExecutor());
    }

    @Nonnull
    AsyncIterable<VectorId> fetchCollapsedVectorIdsIterable(@Nonnull final ReadTransaction readTransaction,
                                                            @Nonnull final UUID signature) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] rangeKey = collapsedVectorIdsSubspace.pack(Tuple.from(signature));

        return AsyncUtil.mapIterable(readTransaction.getRange(Range.startsWith(rangeKey),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL),
                keyValue -> {
                    final Tuple primaryKey = collapsedVectorIdsSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                    final byte[] keyBytes = keyValue.getKey();
                    final byte[] valueBytes = keyValue.getValue();
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    return StorageAdapter.collapsedVectorIdFromValueTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    @Nonnull
    @VisibleForTesting
    AsyncIterable<VectorId> scanCollapsedVectorIdsIterable(@Nonnull final ReadTransaction readTransaction) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] rangeKey = collapsedVectorIdsSubspace.pack();

        return AsyncUtil.mapIterable(readTransaction.getRange(Range.startsWith(rangeKey),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL),
                keyValue -> {
                    final Tuple primaryKey = collapsedVectorIdsSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                    final byte[] keyBytes = keyValue.getKey();
                    final byte[] valueBytes = keyValue.getValue();
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    return StorageAdapter.collapsedVectorIdFromValueTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    void writeCollapsedVectorId(@Nonnull final Transaction transaction,
                                @Nonnull final UUID signature,
                                @Nonnull final VectorId vectorId) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] key = collapsedVectorIdsSubspace.pack(Tuple.from(signature, vectorId.getPrimaryKey()));
        final byte[] value = StorageAdapter.valueTupleFromCollapsedVectorId(vectorId).pack();

        getOnWriteListener().onKeyValueWritten(key, value);
        transaction.set(key, value);
    }

    void deleteCollapsedVectorId(@Nonnull final Transaction transaction,
                                 @Nonnull final UUID signature,
                                 @Nonnull final Tuple primaryKey) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] key = collapsedVectorIdsSubspace.pack(Tuple.from(signature, primaryKey));

        getOnWriteListener().onKeyDeleted(key);
        transaction.clear(key);
    }

    @Nonnull
    CompletableFuture<Void> doSomeDeferredTasks(@Nonnull final Transaction transaction,
                                                @Nonnull final AccessInfo accessInfo) {
        return fetchSomeDeferredTasks(transaction, accessInfo, 1)
                .thenCompose(deferredTasks ->
                        forLoop(0, null,
                                i -> i < deferredTasks.size(), i -> i + 1,
                                (i, ignored) ->
                                        doDeferredTask(transaction, deferredTasks.get(i)), getExecutor()));
    }

    @Nonnull
    CompletableFuture<Void> doDeferredTask(@Nonnull final Transaction transaction,
                                           @Nonnull final AbstractDeferredTask deferredTask) {
        deleteDeferredTask(transaction, deferredTask);
        try {
            return deferredTask.runTask(transaction);
        } finally {
            getOnWriteListener().onTaskExecuted(deferredTask.getKind(),
                    deferredTask.getTaskId(), deferredTask.getTargetClusterIds());
        }
    }

    @Nonnull
    CompletableFuture<List<AbstractDeferredTask>> fetchSomeDeferredTasks(@Nonnull final ReadTransaction readTransaction,
                                                                         @Nonnull final AccessInfo accessInfo,
                                                                         final int numTasks) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] rangeKey = tasksSubspace.pack();

        return AsyncUtil.collect(readTransaction.getRange(Range.startsWith(rangeKey), numTasks, false,
                        StreamingMode.WANT_ALL), readTransaction.getExecutor())
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

    @Nonnull
    CompletableFuture<AbstractDeferredTask> fetchDeferredTask(@Nonnull final ReadTransaction readTransaction,
                                                              @Nonnull final AccessInfo accessInfo,
                                                              @Nonnull final UUID taskId) {
        final Subspace tasksSubspace = getTasksSubspace();
        final Tuple keyTuple = Tuple.from(taskId);
        final byte[] keyBytes = tasksSubspace.pack(keyTuple);

        return readTransaction.get(keyBytes)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        return null;
                    }

                    final Tuple valueTuple = Tuple.fromBytes(valueBytes);
                    final AbstractDeferredTask task =
                            AbstractDeferredTask.newFromTuples(getLocator(), accessInfo, keyTuple, valueTuple);
                    getOnReadListener().onKeyValueRead(keyBytes, valueBytes);
                    return task;
                });
    }

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
     * merge is even possible requires an asynchronous read of the centroid index. Callers that remove vectors
     * (a replicated delete, or the non-merge fallback of the primary-delete path) may still call this method —
     * it will reassign or plain-write the decrement — but it will not split or merge them.
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
                    enqueueSplitMergeTask(transaction, random, clusterMetadata, clusterCentroid, accessInfo,
                            numPrimaryUnderreplicatedVectorsAdded, numReplicatedVectorsAdded,
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
                        numTotalReplicatedVectors, numPrimaryUnderreplicatedVectorsAdded);
            }

            final ClusterMetadata newClusterMetadata =
                    clusterMetadata.withAdditionalVectorsAndStates(numPrimaryUnderreplicatedVectorsAdded,
                            numReplicatedVectorsAdded, updatedStandardDeviation,
                            ClusterMetadata.State.REASSIGN);
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
                                enqueueSplitMergeTask(transaction, random, clusterMetadata, clusterCentroid,
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
    UUID enqueueSplitMergeTask(@Nonnull final Transaction transaction,
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
                        ClusterMetadata.State.SPLIT_MERGE);
        writeClusterMetadata(transaction, newClusterMetadata);

        return newTaskId;
    }

    void deleteDeferredTask(@Nonnull final Transaction transaction,
                            @Nonnull final AbstractDeferredTask deferredTask) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] key = tasksSubspace.pack(Tuple.from(deferredTask.getTaskId()));

        transaction.clear(key);
    }

    @Nonnull
    CompletableFuture<List<ClusterMetadataWithDistance>>
            fetchNeighborhoodClusterMetadata(@Nonnull final ReadTransaction transaction,
                                             @Nonnull final ClusterMetadata targetClusterMetadata,
                                             @Nonnull final RealVector targetClusterCentroid,
                                             @Nonnull final StorageTransform storageTransform,
                                             final int numClusters) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();

        return AsyncUtil.collect(
                MoreAsyncUtil.mapIterablePipelined(executor,
                        MoreAsyncUtil.limitIterable(MoreAsyncUtil.iterableOf(() ->
                                                primitives.centroidsOrderedByDistance(transaction,
                                                        targetClusterCentroid, 0.0d, null),
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
                            return primitives.fetchClusterMetadataWithDistance(transaction,
                                    clusterId,
                                    transformedClusterCentroid,
                                    0.0d);
                        }, 10));
    }

    @Nonnull
    CompletableFuture<List<Cluster>> fetchInnerClusters(@Nonnull final Transaction transaction,
                                                        @Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood,
                                                        @Nonnull final StorageTransform storageTransform) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();

        return forEach(innerNeighborhood,
                clusterMetadata ->
                        primitives.fetchCluster(transaction, storageTransform,
                                clusterMetadata.clusterMetadata().id(), clusterMetadata.centroid()),
                10,
                executor);
    }

    @Nonnull
    CompletableFuture<List<VectorReference>> cleanUpVectorReferences(@Nonnull final Transaction transaction,
                                                                     @Nonnull final List<Cluster> clusters,
                                                                     final boolean discardReplicatedVectorReferences) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();

        final Map<UUID, VectorReference> vectorsByUuidMap = Maps.newHashMap();
        for (final Cluster cluster : clusters) {
            for (final VectorReference vectorReference : cluster.vectorReferences()) {
                if (!discardReplicatedVectorReferences || vectorReference.isPrimaryCopy()) {
                    vectorsByUuidMap.compute(vectorReference.id().getUuid(),
                            (vectorUuid, oldVectorReference) -> {
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
                            });
                }
            }
        }

        return forEach(vectorsByUuidMap.values(),
                vectorReference ->
                        primitives.fetchVectorMetadata(transaction, vectorReference.id().getPrimaryKey())
                                .thenApply(vectorMetadata ->
                                        vectorReference.isCollapsed() ||
                                                (vectorMetadata != null &&
                                                         vectorMetadata.getUuid().equals(vectorReference.id().getUuid()))
                                        ? vectorReference : null),
                10,
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

    record AccessInfoAndNodeExistence(@Nullable AccessInfo accessInfo, boolean nodeExists) {
    }
}
