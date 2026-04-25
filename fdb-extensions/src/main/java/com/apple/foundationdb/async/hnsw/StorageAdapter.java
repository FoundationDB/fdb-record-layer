/*
 * StorageAdapter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the contract for storing and retrieving HNSW graph data to/from a persistent store.
 * <p>
 * This interface provides an abstraction layer over the underlying database, handling the serialization and
 * deserialization of HNSW graph components such as nodes, vectors, and their relationships. Implementations of this
 * interface are responsible for managing the physical layout of data within a given {@link Subspace}.
 * The generic type {@code N} represents the specific type of {@link NodeReference} that this storage adapter manages.
 *
 * @param <N> the type of {@link NodeReference} this storage adapter manages
 */
interface StorageAdapter<N extends NodeReference> {

    /**
     * Subspace for data.
     */
    long SUBSPACE_PREFIX_DATA = 0x00;

    /**
     * Subspace for the access info; contains entry nodes; these are kept separately from the data.
     */
    long SUBSPACE_PREFIX_ACCESS_INFO = 0x01;

    /**
     * Subspace for (mostly) statistical analysis (like finding a centroid, etc.). Contains samples of vectors.
     */
    long SUBSPACE_PREFIX_SAMPLES = 0x02;

    /**
     * Returns the configuration of the HNSW graph.
     * <p>
     * This configuration object contains all the parameters used to build and search the graph,
     * such as the number of neighbors to connect (M), the size of the dynamic list for
     * construction (efConstruction), and the beam width for searching (ef).
     * @return the {@code HNSW.Config} for this graph, never {@code null}.
     */
    @Nonnull
    Config getConfig();

    /**
     * Gets the factory used to create new nodes.
     * <p>
     * This factory is responsible for instantiating new nodes of type {@code N}.
     * @return the non-null factory for creating nodes.
     */
    @Nonnull
    NodeFactory<N> getNodeFactory();

    /**
     * Method that returns {@code true} iff this {@link StorageAdapter} is inlining neighboring vectors (i.e. it is
     * an {@link InliningStorageAdapter}).
     * @return {@code true} iff this {@link StorageAdapter} is inlining neighboring vectors.
     */
    boolean isInliningStorageAdapter();

    /**
     * Method that returns {@code this} object as an {@link InliningStorageAdapter} if this {@link StorageAdapter} is
     * inlining neighboring vectors and is an {@link InliningStorageAdapter}. This method throws an exception if this
     * storage adapter is any other kind of storage adapter. Callers of this method should ensure prior to calling this
     * method that the storage adapter actually is of the right kind (by calling{@link #isInliningStorageAdapter()}.
     * @return {@code this} as an {@link InliningStorageAdapter}
     */
    @Nonnull
    InliningStorageAdapter asInliningStorageAdapter();

    /**
     * Method that returns {@code true} iff this {@link StorageAdapter} is a compact storage adapter which means it is
     * not inlining neighboring vectors (i.e. {@code this} is a {@link CompactStorageAdapter}).
     * @return {@code true} iff this {@link StorageAdapter} is a {@link CompactStorageAdapter}.
     */
    boolean isCompactStorageAdapter();

    /**
     * Method that returns {@code this} object a {@link CompactStorageAdapter} if this {@link StorageAdapter} is
     * a {@link CompactStorageAdapter}. This method throws an exception if this storage adapter is any other kind of
     * storage adapter. Callers of this method should ensure prior to calling this method that the storage adapter
     * actually is of the right kind (by calling{@link #isCompactStorageAdapter()}.
     * @return {@code this} as a {@link CompactStorageAdapter}
     */
    @Nonnull
    CompactStorageAdapter asCompactStorageAdapter();

    /**
     * Get the subspace used to store this HNSW structure.
     * @return the subspace
     */
    @Nonnull
    Subspace getSubspace();

    /**
     * Gets the subspace that contains the data for this object.
     * <p>
     * This subspace represents the portion of the keyspace dedicated to storing the actual data, as opposed to metadata
     * or other system-level information.
     * @return the subspace containing the data, which is guaranteed to be non-null
     */
    @Nonnull
    Subspace getDataSubspace();

    /**
     * Get the on-write listener.
     * @return the on-write listener.
     */
    @Nonnull
    OnWriteListener getOnWriteListener();

    /**
     * Get the on-read listener.
     * @return the on-read listener.
     */
    @Nonnull
    OnReadListener getOnReadListener();

    /**
     * Method that returns the vector associated with node information passed in. Note that depending on the storage
     * layout and therefore the used {@link StorageAdapter}, the vector is either part of the reference
     * (when using {@link InliningStorageAdapter}) or is s part of the {@link AbstractNode} itself (when using
     * {@link CompactStorageAdapter}). This method hides that detail from the caller and correctly resolves the vector
     * for both use cases.
     * @param nodeReference a node reference
     * @param node the accompanying node to {@code nodeReference}
     * @return the associated vector as {@link Transformed} of {@link RealVector}
     */
    @Nonnull
    Transformed<RealVector> getVector(@Nonnull N nodeReference, @Nonnull AbstractNode<N> node);

    /**
     * Asynchronously fetches a node from a specific layer, identified by its primary key.
     * <p>
     * The fetch operation is performed within the scope of the provided {@link ReadTransaction}, ensuring a consistent
     * view of the data. The returned {@link CompletableFuture} will be completed with the node once it has been
     * retrieved from the underlying data store.
     * @param readTransaction the {@link ReadTransaction} context for this read operation
     * @param storageTransform an affine vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param layer the layer from which to fetch the node
     * @param primaryKey the {@link Tuple} representing the primary key of the node to retrieve
     * @return a non-null {@link CompletableFuture} which will complete with the fetched {@link AbstractNode}.
     */
    @Nonnull
    CompletableFuture<AbstractNode<N>> fetchNode(@Nonnull ReadTransaction readTransaction,
                                                 @Nonnull StorageTransform storageTransform,
                                                 int layer,
                                                 @Nonnull Tuple primaryKey);

    /**
     * Writes a node and its neighbor changes to the data store within a given transaction.
     * <p>
     * This method is responsible for persisting the state of a {@link AbstractNode} and applying any modifications to
     * its
     * neighboring nodes as defined in the {@code NeighborsChangeSet}. The entire operation is performed atomically as
     * part of the provided {@link Transaction}.
     *
     * @param transaction the non-null transaction context for this write operation.
     * @param quantizer the quantizer to use
     * @param layer the layer index where the node resides.
     * @param node the non-null node to be written to the data store.
     * @param changeSet the non-null set of changes describing additions or removals of
     *        neighbors for the given {@link AbstractNode}.
     */
    void writeNode(@Nonnull Transaction transaction, @Nonnull Quantizer quantizer, int layer,
                   @Nonnull AbstractNode<N> node, @Nonnull NeighborsChangeSet<N> changeSet);

    /**
     * Deletes a node from a particular layer in the database.
     * @param transaction the transaction to use
     * @param layer the layer the node should be deleted from
     * @param primaryKey the primary key of the node
     */
    void deleteNode(@Nonnull Transaction transaction, int layer, @Nonnull Tuple primaryKey);

    /**
     * Scans a specified layer of the structure, returning an iterable sequence of nodes.
     * <p>
     * This method allows for paginated scanning of a layer. The scan can be started from the beginning of the layer by
     * passing {@code null} for the {@code lastPrimaryKey}, or it can be resumed from a previous point by providing the
     * key of the last item from the prior scan. The number of nodes returned is limited by {@code maxNumRead}.
     *
     * @param readTransaction the transaction to use for the read operation
     * @param layer the index of the layer to scan
     * @param lastPrimaryKey the primary key of the last node from a previous scan,
     *        or {@code null} to start from the beginning of the layer
     * @param maxNumRead the maximum number of nodes to return in this scan
     * @return an {@link AsyncIterable} that provides the nodes found in the specified layer range
     */
    @VisibleForTesting
    Iterable<AbstractNode<N>> scanLayer(@Nonnull ReadTransaction readTransaction, int layer,
                                        @Nullable Tuple lastPrimaryKey, int maxNumRead);

    @Nonnull
    static CompletableFuture<AccessInfo> fetchAccessInfo(@Nonnull final Config config,
                                                         @Nonnull final ReadTransaction readTransaction,
                                                         @Nonnull final Subspace subspace,
                                                         @Nonnull final OnReadListener onReadListener) {
        final Subspace entryNodeSubspace = accessInfoSubspace(subspace);
        final byte[] key = entryNodeSubspace.pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    onReadListener.onKeyValueRead(-1, key, valueBytes);
                    if (valueBytes == null) {
                        return null; // not a single node in the index
                    }

                    final Tuple entryTuple = Tuple.fromBytes(valueBytes);
                    final int layer = (int)entryTuple.getLong(0);
                    final Tuple primaryKey = entryTuple.getNestedTuple(1);
                    final Tuple entryVectorTuple = entryTuple.getNestedTuple(2);
                    final Transformed<RealVector> entryNodeVector =
                            AffineOperator.identity()
                                    .transform(StorageHelpers.vectorFromTuple(config, entryVectorTuple));
                    final EntryNodeReference entryNodeReference =
                            new EntryNodeReference(primaryKey, entryNodeVector, layer);
                    final long rotatorSeed = entryTuple.getLong(3);
                    final Tuple centroidVectorTuple = entryTuple.getNestedTuple(4);
                    return new AccessInfo(entryNodeReference,
                            rotatorSeed,
                            centroidVectorTuple == null
                            ? null
                            : StorageHelpers.vectorFromTuple(config, centroidVectorTuple));
                });
    }

    /**
     * Writes an {@link AccessInfo} to the database within a given transaction and subspace.
     * <p>
     * This method serializes the provided {@link EntryNodeReference} into a key-value pair. The key is determined by
     * a dedicated subspace for entry nodes, and the value is a tuple containing the layer, primary key, and vector from
     * the reference. After writing the data, it notifies the provided {@link OnWriteListener}.
     * @param transaction the database transaction to use for the write operation
     * @param subspace the subspace where the entry node reference will be stored
     * @param accessInfo the {@link AccessInfo} object to write
     * @param onWriteListener the listener to be notified after the key-value pair is written
     */
    static void writeAccessInfo(@Nonnull final Transaction transaction,
                                @Nonnull final Subspace subspace,
                                @Nonnull final AccessInfo accessInfo,
                                @Nonnull final OnWriteListener onWriteListener) {
        final Subspace entryNodeSubspace = accessInfoSubspace(subspace);
        final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
        final RealVector centroid = accessInfo.getNegatedCentroid();
        final byte[] key = entryNodeSubspace.pack();
        final byte[] value = Tuple.from(entryNodeReference.getLayer(),
                entryNodeReference.getPrimaryKey(),
                // getting underlying is okay as it is only written to the database
                StorageHelpers.tupleFromVector(entryNodeReference.getVector()),
                accessInfo.getRotatorSeed(),
                centroid == null ? null : StorageHelpers.tupleFromVector(centroid)).pack();
        transaction.set(key, value);
        onWriteListener.onKeyValueWritten(entryNodeReference.getLayer(), key, value);
    }

    /**
     * Deletes the {@link AccessInfo} from the database within a given transaction and subspace.
     * @param transaction the database transaction to use for the write operation
     * @param subspace the subspace where the entry node reference will be stored
     * @param onWriteListener the listener to be notified after the key-value pair is written
     */
    static void deleteAccessInfo(@Nonnull final Transaction transaction,
                                 @Nonnull final Subspace subspace,
                                 @Nonnull final OnWriteListener onWriteListener) {
        final Subspace entryNodeSubspace = accessInfoSubspace(subspace);
        final byte[] key = entryNodeSubspace.pack();
        transaction.clear(key);
        onWriteListener.onKeyDeleted(-1, key);
    }

    @Nonnull
    static Subspace accessInfoSubspace(@Nonnull final Subspace rootSubspace) {
        return rootSubspace.subspace(Tuple.from(SUBSPACE_PREFIX_ACCESS_INFO));
    }

    @Nonnull
    static Subspace samplesSubspace(@Nonnull final Subspace rootSubspace) {
        return rootSubspace.subspace(Tuple.from(SUBSPACE_PREFIX_SAMPLES));
    }
}
