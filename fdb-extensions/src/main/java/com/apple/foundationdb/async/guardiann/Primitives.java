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
    Subspace getVectorMatadataSubspace() {
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
                        getOnWriteListener().onKeyValueWritten(layer, key, value);
                    }

                    @Override
                    public void onKeyDeleted(final int layer, @Nonnull final byte[] key) {
                        getOnWriteListener().onKeyDeleted(layer, key);
                    }

                    @Override
                    public void onRangeDeleted(final int layer, @Nonnull final Range range) {
                        getOnWriteListener().onRangeDeleted(layer, range);
                    }
                };

        final com.apple.foundationdb.async.hnsw.OnReadListener onReadListener =
                new com.apple.foundationdb.async.hnsw.OnReadListener() {
                    @Override
                    public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
                        getOnReadListener().onKeyValueRead(layer, key, value);
                    }
                };

        return new HNSW(getClusterCentroidsSubspace(), getExecutor(),
                getStorageAdapter().getClusterCentroidsHnswConfig(), onWriteListener, onReadListener);
    }

    boolean isMetricNeedsNormalizedVectors() {
        return getConfig().getMetric() == Metric.COSINE_METRIC;
    }

    @Nonnull
    StorageTransform storageTransform(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return StorageTransform.identity();
        }

        return storageTransform(accessInfo.getRotatorSeed(),
                Objects.requireNonNull(accessInfo.getNegatedCentroid()),
                isMetricNeedsNormalizedVectors());
    }

    @Nonnull
    StorageTransform storageTransform(@Nullable final Long rotatorSeed,
                                      @Nullable final RealVector negatedCentroid,
                                      final boolean normalizeVectors) {
        final LinearOperator linearOperator =
                rotatorSeed == null
                ? null : new FhtKacRotator(rotatorSeed, getConfig().getNumDimensions(), 10);

        return new StorageTransform(linearOperator, negatedCentroid, normalizeVectors);
    }

    @Nonnull
    Quantizer quantizer(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return Quantizer.noOpQuantizer(getConfig().getMetric());
        }

        final Config config = getConfig();
        return config.isUseRaBitQ()
               ? new RaBitQuantizer(config.getMetric(), config.getRaBitQNumExBits())
               : Quantizer.noOpQuantizer(config.getMetric());
    }

    @Nonnull
    CompletableFuture<AccessInfo> fetchAccessInfo(@Nonnull final ReadTransaction readTransaction) {
        final Subspace accessInfoSubspace = getAccessInfoSubspace();
        final byte[] key = accessInfoSubspace.pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(-1, key, valueBytes);
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
        getOnWriteListener().onKeyValueWritten(-1, key, value);
    }

    @Nonnull
    CompletableFuture<Boolean> exists(@Nonnull final ReadTransaction readTransaction, final Tuple primaryKey) {
        return fetchVectorMetadata(readTransaction, primaryKey).thenApply(Objects::nonNull);
    }

    @Nonnull
    CompletableFuture<VectorMetadata> fetchVectorMetadata(@Nonnull final ReadTransaction readTransaction,
                                                          @Nonnull final Tuple primaryKey) {
        final Subspace vectorStatesSubspace = getVectorMatadataSubspace();
        final byte[] key = vectorStatesSubspace.pack(primaryKey);

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(-1, key, valueBytes);
                    if (valueBytes == null) {
                        return null; // unable to find vector
                    }
                    return StorageAdapter.vectorMetadataFromTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    void writeVectorMetadata(@Nonnull final Transaction transaction,
                             @Nonnull final VectorMetadata vectorMetadata) {
        final Subspace vectorIdsSubspace = getVectorMatadataSubspace();
        final byte[] key = vectorIdsSubspace.pack(vectorMetadata.getPrimaryKey());
        final byte[] value = StorageAdapter.valueTupleFromVectorMetadata(vectorMetadata).pack();

        getOnWriteListener().onKeyValueWritten(-1, key, value);
        transaction.set(key, value);
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
                    getOnReadListener().onKeyValueRead(-1, key, valueBytes);
                    if (valueBytes == null) {
                        return null;
                    }
                    return StorageAdapter.clusterMetadataFromTuple(Tuple.fromBytes(valueBytes));
                });
    }

    void writeClusterMetadata(@Nonnull final Transaction transaction,
                              @Nonnull final ClusterMetadata clusterMetadata) {
        final Subspace clusterMetadataSubspace = getClusterMetadataSubspace();
        final byte[] key = clusterMetadataSubspace.pack(Tuple.from(clusterMetadata.getId()));
        final byte[] value = StorageAdapter.valueTupleFromClusterMetadata(clusterMetadata).pack();

        getOnWriteListener().onKeyValueWritten(-1, key, value);
        transaction.set(key, value);
    }

    void deleteClusterMetadata(@Nonnull final Transaction transaction,
                               @Nonnull final UUID clusterId) {
        final Subspace clusterMetadataSubspace = getClusterMetadataSubspace();
        final byte[] key = clusterMetadataSubspace.pack(Tuple.from(clusterId));

        getOnWriteListener().onKeyDeleted(-1, key);
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
                    getOnReadListener().onKeyValueRead(-1, keyBytes, valueBytes);
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
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, vectorReference.getId().getPrimaryKey()));
        final byte[] value = StorageAdapter.valueTupleFromVectorReference(quantizer, vectorReference).pack();

        getOnWriteListener().onKeyValueWritten(-1, key, value);
        transaction.set(key, value);
    }

    void deleteVectorReference(@Nonnull final Transaction transaction,
                               @Nonnull final UUID clusterId,
                               @Nonnull final Tuple primaryKey) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] key = vectorReferencesSubspace.pack(Tuple.from(clusterId, primaryKey));

        getOnWriteListener().onKeyDeleted(-1, key);
        transaction.clear(key);
    }

    void deleteVectorReferencesForCluster(@Nonnull final Transaction transaction,
                                          @Nonnull final UUID clusterId) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterId));
        final Range range = Range.startsWith(rangeKey);

        getOnWriteListener().onRangeDeleted(-1, range);
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
                    getOnReadListener().onKeyValueRead(-1, keyBytes, valueBytes);
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
                    getOnReadListener().onKeyValueRead(-1, keyBytes, valueBytes);
                    return StorageAdapter.collapsedVectorIdFromValueTuple(primaryKey, Tuple.fromBytes(valueBytes));
                });
    }

    void writeCollapsedVectorId(@Nonnull final Transaction transaction,
                                @Nonnull final UUID signature,
                                @Nonnull final VectorId vectorId) {
        final Subspace collapsedVectorIdsSubspace = getCollapsedVectorIdsSubspace();
        final byte[] key = collapsedVectorIdsSubspace.pack(Tuple.from(signature, vectorId.getPrimaryKey()));
        final byte[] value = StorageAdapter.valueTupleFromCollapsedVectorId(vectorId).pack();

        getOnWriteListener().onKeyValueWritten(-1, key, value);
        transaction.set(key, value);
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
                        getOnReadListener().onKeyValueRead(-1, keyBytes, valueBytes);
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
                    getOnReadListener().onKeyValueRead(-1, keyBytes, valueBytes);
                    return task;
                });
    }

    void writeDeferredTask(@Nonnull final Transaction transaction,
                           @Nonnull final UUID taskId,
                           @Nonnull final Tuple valueTuple) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] key = tasksSubspace.pack(Tuple.from(taskId));
        final byte[] value = valueTuple.pack();

        getOnWriteListener().onKeyValueWritten(-1, key, value);
        transaction.set(key, value);
    }

    @Nonnull
    Optional<UUID> writeDeferredTaskMaybe(@Nonnull final Transaction transaction,
                                          @Nonnull final SplittableRandom random,
                                          @Nonnull final ClusterMetadata clusterMetadata,
                                          @Nonnull final Transformed<RealVector> clusterCentroid,
                                          @Nonnull final AccessInfo accessInfo,
                                          final int numPrimaryVectorsAdded,
                                          final int numPrimaryUnderreplicatedVectorsAdded,
                                          final int numReplicatedVectorsAdded,
                                          @Nonnull final RunningStandardDeviation updatedStandardDeviation,
                                          @Nonnull final Set<UUID> causeClusterIds) {
        final Config config = getConfig();
        final UUID clusterId = clusterMetadata.getId();

        final int numTotalPrimaryVectors = clusterMetadata.getNumPrimaryVectors() + numPrimaryVectorsAdded;
        if (!clusterMetadata.getStates().contains(ClusterMetadata.State.SPLIT_MERGE) && // not already splitting
                !clusterMetadata.getStates().contains(ClusterMetadata.State.COLLAPSE) && // not already collapsing
                ((numPrimaryVectorsAdded > 0 && numTotalPrimaryVectors > config.getPrimaryClusterMax()) ||
                         (numPrimaryVectorsAdded < 0 && numTotalPrimaryVectors < config.getPrimaryClusterMin()))) {
            // create a split/merge task
            final UUID newTaskId =
                    AbstractDeferredTask.randomNormalPriorityTaskId(random, config.isDeterministicRandomness());
            final SplitMergeTask newSplitTask =
                    SplitMergeTask.of(getLocator(), accessInfo, newTaskId, clusterId, clusterCentroid);
            newSplitTask.writeDeferredTask(transaction);
            if (logger.isInfoEnabled()) {
                logger.info("enqueued SPLIT_MERGE due to number of primary vectors, taskId={}, numPrimaryVectors={}",
                        newSplitTask.getTaskId(), numTotalPrimaryVectors);
            }

            final ClusterMetadata newClusterMetadata =
                    clusterMetadata.withAdditionalVectorsAndStates(numPrimaryUnderreplicatedVectorsAdded,
                            numReplicatedVectorsAdded, updatedStandardDeviation,
                            ClusterMetadata.State.SPLIT_MERGE);
            writeClusterMetadata(transaction, newClusterMetadata);

            return Optional.of(newTaskId);
        }

        int numTotalPrimaryUnderreplicatedVectors = clusterMetadata.getNumPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded;
        int numTotalReplicatedVectors = clusterMetadata.getNumReplicatedVectors() + numReplicatedVectorsAdded;

        if (!clusterMetadata.getStates().contains(ClusterMetadata.State.REASSIGN) &&  // not already reassigning
                !causeClusterIds.contains(clusterId) &&                               // cannot be a cluster we just split into
                (!causeClusterIds.isEmpty() ||                                                                     // either we just split or
                         numTotalReplicatedVectors > config.getReplicatedClusterMaxWrites() ||                     // we are violating some clean up bounds
                         numTotalPrimaryUnderreplicatedVectors > config.getUnderreplicatedPrimaryClusterMax())) {
            // create a reassign task
            final UUID newTaskId =
                    AbstractDeferredTask.randomNormalPriorityTaskId(random, config.isDeterministicRandomness());
            final ReassignTask newReassignTask = ReassignTask.of(getLocator(), accessInfo, newTaskId,
                    clusterId, clusterCentroid, causeClusterIds);
            newReassignTask.writeDeferredTask(transaction);

            if (logger.isInfoEnabled()) {
                logger.info("enqueued REASSIGN due to violated invariance; taskId={}; numTotalPrimaryVectors={}, numTotalReplicatedVectors={}, numTotalPrimaryUnderreplicatedVectors={}",
                        AbstractDeferredTask.taskIdToString(newTaskId), numTotalPrimaryVectors,
                        numPrimaryUnderreplicatedVectorsAdded, numTotalReplicatedVectors);
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

    void deleteDeferredTask(@Nonnull final Transaction transaction,
                            @Nonnull final AbstractDeferredTask deferredTask) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] key = tasksSubspace.pack(Tuple.from(deferredTask.getTaskId()));

        transaction.clear(key);
    }

    @Nonnull
    CompletableFuture<NeighborhoodsResult> neighborhoods(@Nonnull final ReadTransaction readTransaction,
                                                         @Nonnull final StorageTransform storageTransform,
                                                         @Nonnull final ClusterMetadata targetClusterMetadata,
                                                         @Nonnull final Transformed<RealVector> transformedClusterCentroid,
                                                         @Nonnull final RealVector targetClusterCentroid,
                                                         final int numInnerNeighborhood,
                                                         final int numOuterNeighborhood) {
        final CompletableFuture<List<ClusterMetadataWithDistance>> neighborhoodClusterMetadataFuture =
                fetchNeighborhoodClusterMetadata(readTransaction, targetClusterMetadata, targetClusterCentroid,
                        storageTransform, numInnerNeighborhood + numOuterNeighborhood);

        return neighborhoodClusterMetadataFuture.thenApply(clusterMetadataWithDistances ->
                neighborhoods(storageTransform, clusterMetadataWithDistances, targetClusterMetadata,
                        transformedClusterCentroid, numInnerNeighborhood, numOuterNeighborhood));
    }

    @Nonnull
    NeighborhoodsResult neighborhoods(@Nonnull final StorageTransform storageTransform,
                                      @Nonnull final List<ClusterMetadataWithDistance> clusterMetadataWithDistances,
                                      @Nonnull final ClusterMetadata targetClusterMetadata,
                                      @Nonnull final Transformed<RealVector> targetClusterCentroid,
                                      final int numInnerNeighborhood,
                                      final int numOuterNeighborhood) {
        //
        // Not having the primary cluster in the neighborhood should be next to impossible. It can happen, however,
        // and we need to build for that rare corner case. Here we look for the primary cluster in the cluster
        // neighborhood and adjust the inner and outer neighborhood accordingly. Also log, if we cannot find the
        // primary cluster as that should be almost indicative of another problem.
        //
        boolean foundPrimaryCluster = false;
        for (final ClusterMetadataWithDistance clusterMetadata : clusterMetadataWithDistances) {
            if (clusterMetadata.getClusterMetadata().getId().equals(targetClusterMetadata.getId())) {
                foundPrimaryCluster = true;
                break;
            }
        }

        //
        // If we are here, we have at least one cluster. However, there may not be enough clusters to properly
        // populate both neighborhoods.
        //

        final List<ClusterMetadataWithDistance> innerNeighborhood;
        final List<ClusterMetadataWithDistance> outerNeighborhood;
        if (foundPrimaryCluster) {
            final int cappedNumInnerNeighborhood = Math.min(numInnerNeighborhood, clusterMetadataWithDistances.size());
            return new NeighborhoodsResult(clusterMetadataWithDistances.subList(0, cappedNumInnerNeighborhood),
                    clusterMetadataWithDistances.subList(cappedNumInnerNeighborhood, clusterMetadataWithDistances.size()));
        }

        final ImmutableList.Builder<ClusterMetadataWithDistance> innerNeighborhoodBuilder = ImmutableList.builder();
        // add the target cluster (which we should have found but did not because of reasons)
        innerNeighborhoodBuilder.add(
                new ClusterMetadataWithDistance(targetClusterMetadata, targetClusterCentroid, 0.0d));
        // now everything shifts
        final int cappedNumInnerNeighborhood = Math.min(numInnerNeighborhood - 1, clusterMetadataWithDistances.size());

        innerNeighborhoodBuilder.addAll(clusterMetadataWithDistances.subList(0, cappedNumInnerNeighborhood));
        innerNeighborhood = innerNeighborhoodBuilder.build();

        final int cappedNumOuterNeighborhood = Math.min(numOuterNeighborhood,
                clusterMetadataWithDistances.size() - cappedNumInnerNeighborhood);
        outerNeighborhood = clusterMetadataWithDistances.subList(cappedNumInnerNeighborhood,
                cappedNumInnerNeighborhood + cappedNumOuterNeighborhood);
        return new NeighborhoodsResult(innerNeighborhood, outerNeighborhood);
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
                            final UUID clusterId = StorageAdapter.clusterIdFromTuple(resultEntry.getPrimaryKey());
                            final Transformed<RealVector> transformedClusterCentroid =
                                    storageTransform.transform(Objects.requireNonNull(resultEntry.getVector()));
                            if (clusterId.equals(targetClusterMetadata.getId())) {
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
                                clusterMetadata.getClusterMetadata().getId(), clusterMetadata.getCentroid()),
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
            for (final VectorReference vectorReference : cluster.getVectorReferences()) {
                if (!discardReplicatedVectorReferences || vectorReference.isPrimaryCopy()) {
                    vectorsByUuidMap.compute(vectorReference.getId().getUuid(),
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
                                return oldVectorReference.getReplicationPriority() > vectorReference.getReplicationPriority()
                                       ? oldVectorReference
                                       : vectorReference;
                            });
                }
            }
        }

        return forEach(vectorsByUuidMap.values(),
                vectorReference ->
                        primitives.fetchVectorMetadata(transaction, vectorReference.getId().getPrimaryKey())
                                .thenApply(vectorMetadata ->
                                        vectorReference.isCollapsed() || vectorMetadata.getUuid().equals(vectorReference.getId().getUuid())
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

    static class AccessInfoAndNodeExistence {
        @Nullable
        private final AccessInfo accessInfo;
        private final boolean nodeExists;

        public AccessInfoAndNodeExistence(@Nullable final AccessInfo accessInfo, final boolean nodeExists) {
            this.accessInfo = accessInfo;
            this.nodeExists = nodeExists;
        }

        @Nullable
        public AccessInfo getAccessInfo() {
            return accessInfo;
        }

        public boolean isNodeExists() {
            return nodeExists;
        }
    }

    static class NeighborhoodsResult {
        @Nonnull
        private final List<ClusterMetadataWithDistance> innerNeighborhood;
        @Nonnull
        private final List<ClusterMetadataWithDistance> outerNeighborhood;

        public NeighborhoodsResult(@Nonnull final List<ClusterMetadataWithDistance> innerNeighborhood,
                                   @Nonnull final List<ClusterMetadataWithDistance> outerNeighborhood) {
            this.innerNeighborhood = innerNeighborhood;
            this.outerNeighborhood = outerNeighborhood;
        }

        @Nonnull
        public List<ClusterMetadataWithDistance> getInnerNeighborhood() {
            return innerNeighborhood;
        }

        @Nonnull
        public List<ClusterMetadataWithDistance> getOuterNeighborhood() {
            return outerNeighborhood;
        }
    }
}
