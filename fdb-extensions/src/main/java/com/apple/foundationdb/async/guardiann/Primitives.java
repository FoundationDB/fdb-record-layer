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
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.async.hnsw.ResultEntry;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.LinearOperator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.rabitq.RaBitQuantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.apple.foundationdb.async.MoreAsyncUtil.forLoop;

/**
 * An implementation of primitives for the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 */
@API(API.Status.EXPERIMENTAL)
public class Primitives {
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
     * Get the executor used by this hnsw.
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
                                                          @Nonnull final RealVector centerVector) {
        final HNSW centroidsHnsw = getClusterCentroidsHnsw();

        return centroidsHnsw.orderByDistance(readTransaction, 100, 400, true,
                centerVector, 0.0d, null, true);
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
                        (clusterMetadata, vectorReferences) ->
                                new Cluster(clusterMetadata, centroid, vectorReferences));
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
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterId));

        return AsyncUtil.collect(readTransaction.getRange(Range.startsWith(rangeKey),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL), readTransaction.getExecutor())
                .thenApply(keyValues -> {
                    final ImmutableList.Builder<VectorReference> vectorReferencesBuilder = ImmutableList.builder();
                    for (final KeyValue keyValue : keyValues) {
                        final Tuple primaryKey = vectorReferencesSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                        final byte[] keyBytes = keyValue.getKey();
                        final byte[] valueBytes = keyValue.getValue();
                        vectorReferencesBuilder.add(
                                StorageAdapter.vectorReferenceFromTuples(getConfig(), storageTransform,
                                        primaryKey, Tuple.fromBytes(valueBytes)));
                        getOnReadListener().onKeyValueRead(-1, keyBytes, valueBytes);
                    }
                    return vectorReferencesBuilder.build();
                });
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

    void deleteVectorReferencesForCluster(@Nonnull final Transaction transaction,
                                          @Nonnull final UUID clusterId) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterId));
        final Range range = Range.startsWith(rangeKey);

        getOnWriteListener().onRangeDeleted(-1, range);
        transaction.clear(range);
    }

    @Nonnull
    CompletableFuture<Void> doSomeDeferredTasks(@Nonnull final Transaction transaction,
                                                @Nonnull final AccessInfo accessInfo) {
        return fetchSomeDeferredTasks(transaction, accessInfo, 2)
                .thenCompose(deferredTasks ->
                        forLoop(0, null,
                                i -> i < deferredTasks.size(), i -> i + 1,
                                (i, ignored) -> {
                                    final AbstractDeferredTask deferredTask = deferredTasks.get(i);
                                    deleteDeferredTask(transaction, deferredTask);
                                    return deferredTask.runTask(transaction);
                                }, getExecutor()));
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

    void writeDeferredTask(@Nonnull final Transaction transaction,
                           @Nonnull final AbstractDeferredTask deferredTask) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] key = tasksSubspace.pack(Tuple.from(deferredTask.getTaskId()));
        final byte[] value = deferredTask.valueTuple().pack();

        getOnWriteListener().onKeyValueWritten(-1, key, value);
        transaction.set(key, value);
    }

    void deleteDeferredTask(@Nonnull final Transaction transaction,
                            @Nonnull final AbstractDeferredTask deferredTask) {
        final Subspace tasksSubspace = getTasksSubspace();
        final byte[] key = tasksSubspace.pack(Tuple.from(deferredTask.getTaskId()));

        transaction.clear(key);
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
}
