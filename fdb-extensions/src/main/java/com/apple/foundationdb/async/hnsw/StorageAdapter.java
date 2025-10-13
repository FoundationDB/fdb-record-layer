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
import com.apple.foundationdb.async.rabitq.EncodedVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

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
    ImmutableList<VectorType> VECTOR_TYPES = ImmutableList.copyOf(VectorType.values());

    /**
     * Subspace for entry nodes; these are kept separately from the data.
     */
    byte SUBSPACE_PREFIX_ENTRY_NODE = 0x01;
    /**
     * Subspace for data.
     */
    byte SUBSPACE_PREFIX_DATA = 0x02;

    /**
     * Returns the configuration of the HNSW graph.
     * <p>
     * This configuration object contains all the parameters used to build and search the graph,
     * such as the number of neighbors to connect (M), the size of the dynamic list for
     * construction (efConstruction), and the beam width for searching (ef).
     * @return the {@code HNSW.Config} for this graph, never {@code null}.
     */
    @Nonnull
    HNSW.Config getConfig();

    /**
     * Gets the factory used to create new nodes.
     * <p>
     * This factory is responsible for instantiating new nodes of type {@code N}.
     * @return the non-null factory for creating nodes.
     */
    @Nonnull
    NodeFactory<N> getNodeFactory();

    /**
     * Gets the kind of node this storage adapter manages (and instantiates if needed).
     * @return the kind of this node, never {@code null}
     */
    @Nonnull
    NodeKind getNodeKind();

    /**
     * Returns a view of this object as a {@code StorageAdapter} that is optimized
     * for compact data representation.
     * @return a non-null {@code StorageAdapter} for {@code NodeReference} objects,
     *         optimized for compact storage.
     */
    @Nonnull
    StorageAdapter<NodeReference> asCompactStorageAdapter();

    /**
     * Returns a view of this storage as a {@code StorageAdapter} that handles inlined vectors.
     * <p>
     * The returned adapter is specifically designed to work with {@link NodeReferenceWithVector}, assuming that the
     * vector data is stored directly within the node reference itself.
     * @return a non-null {@link StorageAdapter}
     */
    @Nonnull
    StorageAdapter<NodeReferenceWithVector> asInliningStorageAdapter();

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
     * Asynchronously fetches a node from a specific layer, identified by its primary key.
     * <p>
     * The fetch operation is performed within the scope of the provided {@link ReadTransaction}, ensuring a consistent
     * view of the data. The returned {@link CompletableFuture} will be completed with the node once it has been
     * retrieved from the underlying data store.
     * @param readTransaction the {@link ReadTransaction} context for this read operation
     * @param layer the layer from which to fetch the node
     * @param primaryKey the {@link Tuple} representing the primary key of the node to retrieve
     * @return a non-null {@link CompletableFuture} which will complete with the fetched {@code Node<N>}.
     */
    @Nonnull
    CompletableFuture<Node<N>> fetchNode(@Nonnull ReadTransaction readTransaction,
                                         int layer,
                                         @Nonnull Tuple primaryKey);

    /**
     * Writes a node and its neighbor changes to the data store within a given transaction.
     * <p>
     * This method is responsible for persisting the state of a {@link Node} and applying any modifications to its
     * neighboring nodes as defined in the {@code NeighborsChangeSet}. The entire operation is performed atomically as
     * part of the provided {@link Transaction}.
     * @param transaction the non-null transaction context for this write operation.
     * @param node the non-null node to be written to the data store.
     * @param layer the layer index where the node resides.
     * @param changeSet the non-null set of changes describing additions or removals of
     *        neighbors for the given {@link Node}.
     */
    void writeNode(@Nonnull Transaction transaction, @Nonnull Node<N> node, int layer,
                   @Nonnull NeighborsChangeSet<N> changeSet);

    /**
     * Scans a specified layer of the directory, returning an iterable sequence of nodes.
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
     * @return an {@link Iterable} that provides the nodes found in the specified layer range
     */
    Iterable<Node<N>> scanLayer(@Nonnull ReadTransaction readTransaction, int layer, @Nullable Tuple lastPrimaryKey,
                                int maxNumRead);

    /**
     * Fetches the entry node reference for the HNSW index.
     * <p>
     * This method performs an asynchronous read to retrieve the stored entry point of the index. The entry point
     * information, which includes its primary key, vector, and the layer value, is packed into a single key-value
     * pair within a dedicated subspace. If no entry node is found, it indicates that the index is empty.
     * @param config an HNSW configuration
     * @param readTransaction the transaction to use for the read operation
     * @param subspace the subspace where the HNSW index data is stored
     * @param onReadListener a listener to be notified of the key-value read operation
     * @return a {@link CompletableFuture} that will complete with the {@link EntryNodeReference}
     *         for the index's entry point, or with {@code null} if the index is empty
     */
    @Nonnull
    static CompletableFuture<EntryNodeReference> fetchEntryNodeReference(@Nonnull final HNSW.Config config,
                                                                         @Nonnull final ReadTransaction readTransaction,
                                                                         @Nonnull final Subspace subspace,
                                                                         @Nonnull final OnReadListener onReadListener) {
        final Subspace entryNodeSubspace = subspace.subspace(Tuple.from(SUBSPACE_PREFIX_ENTRY_NODE));
        final byte[] key = entryNodeSubspace.pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        return null; // not a single node in the index
                    }
                    onReadListener.onKeyValueRead(-1, key, valueBytes);

                    final Tuple entryTuple = Tuple.fromBytes(valueBytes);
                    final int layer = (int)entryTuple.getLong(0);
                    final Tuple primaryKey = entryTuple.getNestedTuple(1);
                    final Tuple vectorTuple = entryTuple.getNestedTuple(2);
                    return new EntryNodeReference(primaryKey, StorageAdapter.vectorFromTuple(config, vectorTuple), layer);
                });
    }

    /**
     * Writes an {@code EntryNodeReference} to the database within a given transaction and subspace.
     * <p>
     * This method serializes the provided {@link EntryNodeReference} into a key-value pair. The key is determined by
     * a dedicated subspace for entry nodes, and the value is a tuple containing the layer, primary key, and vector from
     * the reference. After writing the data, it notifies the provided {@link OnWriteListener}.
     * @param transaction the database transaction to use for the write operation
     * @param subspace the subspace where the entry node reference will be stored
     * @param entryNodeReference the {@link EntryNodeReference} object to write
     * @param onWriteListener the listener to be notified after the key-value pair is written
     */
    static void writeEntryNodeReference(@Nonnull final Transaction transaction,
                                        @Nonnull final Subspace subspace,
                                        @Nonnull final EntryNodeReference entryNodeReference,
                                        @Nonnull final OnWriteListener onWriteListener) {
        final Subspace entryNodeSubspace = subspace.subspace(Tuple.from(SUBSPACE_PREFIX_ENTRY_NODE));
        final byte[] key = entryNodeSubspace.pack();
        final byte[] value = Tuple.from(entryNodeReference.getLayer(),
                entryNodeReference.getPrimaryKey(),
                StorageAdapter.tupleFromVector(entryNodeReference.getVector())).pack();
        transaction.set(key,
                value);
        onWriteListener.onKeyValueWritten(entryNodeReference.getLayer(), key, value);
    }

    /**
     * Creates a {@code HalfVector} from a given {@code Tuple}.
     * <p>
     * This method assumes the vector data is stored as a byte array at the first. position (index 0) of the tuple. It
     * extracts this byte array and then delegates to the {@link #vectorFromBytes(HNSW.Config, byte[])} method for the
     * actual conversion.
     * @param config an HNSW configuration
     * @param vectorTuple the tuple containing the vector data as a byte array at index 0. Must not be {@code null}.
     * @return a new {@code HalfVector} instance created from the tuple's data.
     *         This method never returns {@code null}.
     */
    @Nonnull
    static Vector vectorFromTuple(@Nonnull final HNSW.Config config, @Nonnull final Tuple vectorTuple) {
        return vectorFromBytes(config, vectorTuple.getBytes(0));
    }

    /**
     * Creates a {@link Vector} from a byte array.
     * <p>
     * This method interprets the input byte array by interpreting the first byte of the array as the precision shift.
     * The byte array must have the proper size, i.e. the invariant {@code (bytesLength - 1) % precision == 0} must
     * hold.
     * @param config an HNSW config
     * @param vectorBytes the non-null byte array to convert.
     * @return a new {@link Vector} instance created from the byte array.
     * @throws com.google.common.base.VerifyException if the length of {@code vectorBytes} does not meet the invariant
     *         {@code (bytesLength - 1) % precision == 0}
     */
    @Nonnull
    static Vector vectorFromBytes(@Nonnull final HNSW.Config config, @Nonnull final byte[] vectorBytes) {
        final byte vectorTypeOrdinal = vectorBytes[0];
        switch (fromVectorTypeOrdinal(vectorTypeOrdinal)) {
            case HALF:
                return HalfVector.fromBytes(vectorBytes, 1);
            case DOUBLE:
                return DoubleVector.fromBytes(vectorBytes, 1);
            case RABITQ:
                Verify.verify(config.isUseRaBitQ());
                return EncodedVector.fromBytes(vectorBytes, 1, config.getNumDimensions(),
                        config.getRaBitQNumExBits());
            default:
                throw new RuntimeException("unable to serialize vector");
        }
    }

    /**
     * Converts a {@link Vector} into a {@link Tuple}.
     * <p>
     * This method first serializes the given vector into a byte array using the {@link Vector#getRawData()} getter
     * method. It then creates a {@link Tuple} from the resulting byte array.
     * @param vector the vector of {@code Half} precision floating-point numbers to convert. Cannot be null.
     * @return a new, non-null {@code Tuple} instance representing the contents of the vector.
     */
    @Nonnull
    @SuppressWarnings("PrimitiveArrayArgumentToVarargsMethod")
    static Tuple tupleFromVector(final Vector vector) {
        return Tuple.from(vector.getRawData());
    }

    @Nonnull
    static VectorType fromVectorTypeOrdinal(final int ordinal) {
        return VECTOR_TYPES.get(ordinal);
    }

}
