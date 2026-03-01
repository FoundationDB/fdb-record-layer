/*
 * Search.java
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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.AggregatedVector;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.guardiann.Primitives.AccessInfoAndNodeExistence;
import com.apple.foundationdb.async.hnsw.ResultEntry;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.async.MoreAsyncUtil.limitIterable;
import static com.apple.foundationdb.async.MoreAsyncUtil.mapIterablePipelined;
import static com.apple.foundationdb.async.MoreAsyncUtil.takeWhileIterable;
import static com.apple.foundationdb.async.common.StorageHelpers.aggregateVectors;
import static com.apple.foundationdb.async.common.StorageHelpers.appendSampledVector;
import static com.apple.foundationdb.async.common.StorageHelpers.consumeSampledVectors;
import static com.apple.foundationdb.async.common.StorageHelpers.deleteAllSampledVectors;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class Search {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Search.class);

    @Nonnull
    private final Locator locator;

    /**
     * This constructor initializes a new insert operations object with the necessary components for storage,
     * execution, configuration, and event handling.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Search(@Nonnull final Locator locator) {
        this.locator = locator;
    }

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    public Subspace getSubspace() {
        return getLocator().getSubspace();
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration of this hnsw.
     * @return hnsw configuration
     */
    @Nonnull
    public Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    public OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    private Primitives primitives() {
        return getLocator().primitives();
    }

    @Nonnull
    private StorageAdapter getStorageAdapter() {
        return getLocator().getStorageAdapter();
    }

    @Nonnull
    private Subspace getSamplesSubspace() {
        return getStorageAdapter().getSamplesSubspace();
    }

    AsyncIterator<ResultEntry> orderByDistance(@Nonnull final ReadTransaction readTransaction,
                                               final int efOutwardSearch,
                                               final boolean includeVectors,
                                               @Nonnull final RealVector centerVector,
                                               final double minimumRadius,
                                               @Nullable final Tuple minimumPrimaryKey,
                                               final boolean shouldQuickStart) {
    }

    /**
     * TODO.
     */
    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction, @Nonnull final Tuple newPrimaryKey,
                                          @Nonnull final RealVector newVector,
                                          @Nullable final Tuple newAdditionalValues) {
        final Config config = getConfig();
        final SplittableRandom random = RandomHelpers.random(newPrimaryKey);
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(transaction)
                .thenCombine(primitives.exists(transaction, newPrimaryKey),
                        (accessInfo, nodeAlreadyExists) -> {
                            if (nodeAlreadyExists) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("new record already exists with key={}", newPrimaryKey);
                                }
                            }
                            return new AccessInfoAndNodeExistence(accessInfo, nodeAlreadyExists);
                        })
                .thenCompose(accessInfoAndNodeExistence -> {
                    final AccessInfo accessInfo = accessInfoAndNodeExistence.getAccessInfo();
                    if (accessInfo == null) {
                        return initialAccessInfoAndFirstCluster(transaction, newVector, random)
                                .thenApply(initialAccessInfo ->
                                        new AccessInfoAndNodeExistence(initialAccessInfo, false));
                    }

                    // do some deferred tasks
                    return primitives.doSomeDeferredTasks(transaction, accessInfo)
                            .thenApply(ignored -> accessInfoAndNodeExistence);
                }).thenCompose(accessInfoAndNodeExistence -> {
                    if (accessInfoAndNodeExistence.isNodeExists()) {
                        return AsyncUtil.DONE;
                    }

                    final AccessInfo accessInfo = Objects.requireNonNull(accessInfoAndNodeExistence.getAccessInfo());
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedNewVector = storageTransform.transform(newVector);
                    final Quantizer quantizer = primitives.quantizer(accessInfo);

                    final AsyncIterable<ResultEntry> clusterCentroidEntriesByDistanceIterable =
                            MoreAsyncUtil.iterableOf(() ->
                                    primitives.centroidsOrderedByDistance(transaction, newVector), getExecutor());

                    final AsyncIterable<ClusterMetadataWithDistance> clusterMetadataIterable =
                            mapIterablePipelined(getExecutor(), clusterCentroidEntriesByDistanceIterable,
                                    resultEntry ->
                                            primitives.fetchClusterMetadata(transaction,
                                                            StorageAdapter.clusterIdFromTuple(resultEntry.getPrimaryKey()))
                                                    .thenApply(clusterMetadata -> {
                                                        final Transformed<RealVector> transformedCentroid =
                                                                storageTransform.transform(Objects.requireNonNull(resultEntry.getVector()));
                                                        return new ClusterMetadataWithDistance(clusterMetadata,
                                                                        transformedCentroid,
                                                                        resultEntry.getDistance());
                                                    }),
                                    10);

                    final AtomicInteger indexAtomic = new AtomicInteger(0);
                    final AtomicReference<UUID> primaryClusterIdAtomic = new AtomicReference<>();
                    final AtomicDouble primaryDistanceAtomic = new AtomicDouble(Double.NaN);

                    final AsyncIterable<ClusterMetadataWithDistance> affectedNeighborhood =
                            takeWhileIterable(limitIterable(clusterMetadataIterable, 3,
                                            getExecutor()),
                                    clusterMetadataWithDistance -> {
                                        final int index = indexAtomic.getAndIncrement();
                                        final double distance = clusterMetadataWithDistance.getDistance();

                                        if (index == 0) {
                                            // first and nearest cluster -- always accept
                                            primaryClusterIdAtomic.set(
                                                    clusterMetadataWithDistance.getClusterMetadata().getId());
                                            primaryDistanceAtomic.set(distance);
                                            return true;
                                        }

                                        final double distanceToPrimaryCentroid = primaryDistanceAtomic.get();
                                        Verify.verify(Double.isFinite(distanceToPrimaryCentroid));

                                        //
                                        // Distance should be greater than the distance to the primary cluster's
                                        // centroid. So the fraction on the left should always be greater or equal
                                        // to 1.0d. The config provides some fuzziness to replicate the new vector
                                        // into other clusters if it happens to be at the border between two (or more)
                                        // clusters.
                                        //
                                        return distance / distanceToPrimaryCentroid <= 1.0d + config.getClusterOverlap();
                                    }, getExecutor());

                    final VectorMetadata newVectorMetadata =
                            new VectorMetadata(newPrimaryKey, UUID.randomUUID(), newAdditionalValues);
                    primitives.writeVectorMetadata(transaction, newVectorMetadata);

                    final AsyncIterable<Void> updatedNeighborhood = mapIterablePipelined(affectedNeighborhood,
                            clusterMetadataWithDistance -> {
                                final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.getClusterMetadata();
                                final ClusterMetadata newClusterMetadata;
                                final UUID clusterId = clusterMetadata.getId();
                                if (!clusterMetadata.getStates().contains(ClusterMetadata.State.SPLIT_MERGE) && // not already splitting
                                        clusterMetadata.getNumVectors() >= config.getClusterMax()) {
                                    // create a split/merge task
                                    primitives.writeDeferredTask(transaction,
                                            SplitMergeTask.of(getLocator(), accessInfo, UUID.randomUUID(),
                                                    clusterId, clusterMetadataWithDistance.getCentroid()));

                                    newClusterMetadata =
                                            clusterMetadata.withAdditionalVectorsAndNewStates(1,
                                                    ClusterMetadata.State.SPLIT_MERGE);
                                } else {
                                    newClusterMetadata =
                                            clusterMetadata.withAdditionalVectors(1);
                                }

                                primitives.writeVectorReference(transaction, quantizer, clusterId,
                                        new VectorReference(newVectorMetadata,
                                                clusterId.equals(primaryClusterIdAtomic.get()), transformedNewVector));
                                primitives.writeClusterMetadata(transaction, newClusterMetadata);
                                return AsyncUtil.DONE;
                            },
                            10);

                    return AsyncUtil.collect(updatedNeighborhood, getExecutor())
                            .thenCompose(results -> {
                                Verify.verify(!results.isEmpty());
                                return addToStatsIfNecessary(random, transaction, accessInfo, transformedNewVector);
                            });
                });
    }

    @Nonnull
    private CompletableFuture<AccessInfo> initialAccessInfoAndFirstCluster(@Nonnull final Transaction transaction,
                                                                           @Nonnull final RealVector newVector,
                                                                           @Nonnull final SplittableRandom random) {
        final Config config = getConfig();
        final Primitives primitives = primitives();
        final long rotatorSeed;
        final RealVector negatedCentroid;

        if (config.isUseRaBitQ() &&
                !config.getMetric().satisfiesPreservedUnderTranslation()) {
            //
            // The metric does not preserve distances under translation of the vectors, but we are supposed to encode
            // the vectors using RaBitQ. There is no point in sampling the centroid as we cannot translate any vectors.
            // Instead, we use RaBitQ immediately under an identity translation.
            //
            rotatorSeed = random.nextLong();
            negatedCentroid = DoubleRealVector.zeroVector(config.getNumDimensions());
        } else {
            rotatorSeed = -1L;
            negatedCentroid = null;
        }

        final AccessInfo initialAccessInfo = new AccessInfo(rotatorSeed, negatedCentroid);
        primitives.writeAccessInfo(transaction, initialAccessInfo);
        if (logger.isTraceEnabled()) {
            logger.trace("written initial access info");
        }

        return primitives.getClusterCentroidsHnsw()
                .insert(transaction, StorageAdapter.tupleFromClusterId(UUID.randomUUID()),
                        newVector, null)
                .thenApply(ignored -> initialAccessInfo);
    }

    /**
     * Method to keep stats if necessary. Stats need to be kept and maintained when the client would like to use
     * e.g. RaBitQ as RaBitQ needs a stable somewhat correct centroid in order to function properly.
     * <p>
     * Specifically for RaBitQ, we add vectors to a set of sampled vectors in a designated subspace of the HNSW
     * structure. The parameter {@link Config#getSampleVectorStatsProbability()} governs when we do sample. Another
     * parameter, {@link Config#getMaintainStatsProbability()}, determines how many times we add-up/replace (consume)
     * vectors from this sampled-vector space and aggregate them in the typical running count/running sum scheme
     * in order to finally compute the centroid if {@link Config#getStatsThreshold()} number of vectors have been
     * sampled and aggregated. That centroid is then used to update the access info.
     *
     * @param random a random to use
     * @param transaction the transaction
     * @param currentAccessInfo this current access info that was fetched as part of an insert
     * @param transformedNewVector the new vector (in the transformed coordinate system) that may be added
     * @return a future that returns {@code null} when completed
     */
    @Nonnull
    private CompletableFuture<Void> addToStatsIfNecessary(@Nonnull final SplittableRandom random,
                                                          @Nonnull final Transaction transaction,
                                                          @Nonnull final AccessInfo currentAccessInfo,
                                                          @Nonnull final Transformed<RealVector> transformedNewVector) {
        final Subspace samplesSubspace = getSamplesSubspace();
        if (getConfig().isUseRaBitQ() &&
                !currentAccessInfo.canUseRaBitQ()) {
            final Primitives primitives = primitives();
            if (shouldSampleVector(random)) {
                appendSampledVector(transaction, samplesSubspace, 1, transformedNewVector,
                        getOnWriteListener());
            }
            if (shouldMaintainStats(random)) {
                return consumeSampledVectors(transaction, samplesSubspace,
                                50, getOnReadListener())
                        .thenApply(sampledVectors -> {
                            final AggregatedVector aggregatedSampledVector =
                                    aggregateVectors(sampledVectors);

                            if (aggregatedSampledVector != null) {
                                final int partialCount = aggregatedSampledVector.getPartialCount();
                                final Transformed<RealVector> partialVector = aggregatedSampledVector.getPartialVector();
                                appendSampledVector(transaction, samplesSubspace, partialCount, partialVector,
                                        getOnWriteListener());
                                if (logger.isTraceEnabled()) {
                                    logger.trace("updated stats with numVectors={}, partialCount={}, partialVector={}",
                                            sampledVectors.size(), partialCount, partialVector);
                                }

                                if (partialCount >= getConfig().getStatsThreshold()) {
                                    final long rotatorSeed = random.nextLong();
                                    final FhtKacRotator rotator =
                                            new FhtKacRotator(rotatorSeed, getConfig().getNumDimensions(), 10);

                                    final Transformed<RealVector> centroid =
                                            partialVector.multiply(-1.0d / partialCount);
                                    final RealVector rotatedCentroid =
                                            rotator.apply(centroid.getUnderlyingVector());

                                    final AccessInfo newAccessInfo =
                                            new AccessInfo(rotatorSeed, rotatedCentroid);
                                    primitives.writeAccessInfo(transaction, newAccessInfo);
                                    deleteAllSampledVectors(transaction, samplesSubspace, getOnWriteListener());
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("established rotatorSeed={}, centroid with count={}, centroid={}",
                                                rotatorSeed, partialCount, rotatedCentroid);
                                    }
                                }
                            }
                            return null;
                        });
            }
        }
        return AsyncUtil.DONE;
    }

    private boolean shouldSampleVector(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getSampleVectorStatsProbability();
    }

    private boolean shouldMaintainStats(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getMaintainStatsProbability();
    }
}
