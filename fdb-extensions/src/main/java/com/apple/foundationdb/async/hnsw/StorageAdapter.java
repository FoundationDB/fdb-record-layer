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
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.base.Verify;

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
     *
     * @param readTransaction the transaction to use for the read operation
     * @param subspace the subspace where the HNSW index data is stored
     * @param onReadListener a listener to be notified of the key-value read operation
     * @return a {@link CompletableFuture} that will complete with the {@link EntryNodeReference}
     *         for the index's entry point, or with {@code null} if the index is empty
     */
    @Nonnull
    static CompletableFuture<EntryNodeReference> fetchEntryNodeReference(@Nonnull final ReadTransaction readTransaction,
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
                    return new EntryNodeReference(primaryKey, StorageAdapter.vectorFromTuple(vectorTuple), layer);
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
     * extracts this byte array and then delegates to the {@link #vectorFromBytes(byte[])} method for the actual
     * conversion.
     * @param vectorTuple the tuple containing the vector data as a byte array at index 0. Must not be {@code null}.
     * @return a new {@code HalfVector} instance created from the tuple's data.
     *         This method never returns {@code null}.
     */
    @Nonnull
    static Vector.HalfVector vectorFromTuple(final Tuple vectorTuple) {
        return vectorFromBytes(vectorTuple.getBytes(0));
    }

    /**
     * Creates a {@link Vector.HalfVector} from a byte array.
     * <p>
     * This method interprets the input byte array as a sequence of 16-bit half-precision floating-point numbers. Each
     * consecutive pair of bytes is converted into a {@code Half} value, which then becomes a component of the resulting
     * vector. The byte array must have an even number of bytes.
     * @param vectorBytes the non-null byte array to convert. The length of this array must be even, as each pair of
     *        bytes represents a single {@link Half} component.
     * @return a new {@link Vector.HalfVector} instance created from the byte array.
     * @throws com.google.common.base.VerifyException if the length of {@code vectorBytes} is odd,
     *         as verified by the internal check.
     */
    @Nonnull
    static Vector.HalfVector vectorFromBytes(final byte[] vectorBytes) {
        final int bytesLength = vectorBytes.length;
        Verify.verify(bytesLength % 2 == 0);
        final int componentSize = bytesLength >>> 1;
        final Half[] vectorHalfs = new Half[componentSize];
        for (int i = 0; i < componentSize; i ++) {
            vectorHalfs[i] = Half.shortBitsToHalf(shortFromBytes(vectorBytes, i << 1));
        }
        return new Vector.HalfVector(vectorHalfs);
    }

    /**
     * Converts a {@code Vector<Half>} into a {@code Tuple}.
     * <p>
     * This method first serializes the given vector into a byte array using the {@link #bytesFromVector(Vector)} helper
     * method. It then creates a {@link Tuple} from the resulting byte array.
     * @param vector the vector of {@code Half} precision floating-point numbers to convert. Cannot be null.
     * @return a new, non-null {@code Tuple} instance representing the contents of the vector.
     */
    @Nonnull
    @SuppressWarnings("PrimitiveArrayArgumentToVarargsMethod")
    static Tuple tupleFromVector(final Vector<Half> vector) {
        return Tuple.from(bytesFromVector(vector));
    }

    /**
     * Converts a {@link Vector} of {@link Half} precision floating-point numbers into a byte array.
     * <p>
     * This method iterates through the input vector, converting each {@link Half} element into its 16-bit short
     * representation. It then serializes this short into two bytes, placing them sequentially into the resulting byte
     * array. The final array's length will be {@code 2 * vector.size()}.
     * @param vector the vector of {@link Half} precision numbers to convert. Must not be null.
     * @return a new byte array representing the serialized vector data. This array is never null.
     */
    @Nonnull
    static byte[] bytesFromVector(final Vector<Half> vector) {
        final byte[] vectorBytes = new byte[2 * vector.size()];
        for (int i = 0; i < vector.size(); i ++) {
            final byte[] componentBytes = bytesFromShort(Half.halfToShortBits(vector.getComponent(i)));
            final int indexTimesTwo = i << 1;
            vectorBytes[indexTimesTwo] = componentBytes[0];
            vectorBytes[indexTimesTwo + 1] = componentBytes[1];
        }
        return vectorBytes;
    }

    /**
     * Constructs a short from two bytes in a byte array in big-endian order.
     * <p>
     * This method reads two consecutive bytes from the {@code bytes} array, starting at the given {@code offset}. The
     * byte at {@code offset} is treated as the most significant byte (MSB), and the byte at {@code offset + 1} is the
     * least significant byte (LSB).
     * @param bytes the source byte array from which to read the short.
     * @param offset the starting index in the byte array. This must be an even number
     *        and ensure that {@code offset + 1} is a valid index.
     * @return the short value constructed from the two bytes.
     */
    static short shortFromBytes(final byte[] bytes, final int offset) {
        Verify.verify(offset % 2 == 0);
        int high = bytes[offset] & 0xFF;   // Convert to unsigned int
        int low  = bytes[offset + 1] & 0xFF;

        return (short) ((high << 8) | low);
    }

    /**
     * Converts a {@code short} value into a 2-element byte array.
     * <p>
     * The conversion is performed in big-endian byte order, where the most significant byte (MSB) is placed at index 0
     * and the least significant byte (LSB) is at index 1.
     * @param value the {@code short} value to be converted.
     * @return a new 2-element byte array representing the short value in big-endian order.
     */
    static byte[] bytesFromShort(final short value) {
        byte[] result = new byte[2];
        result[0] = (byte) ((value >> 8) & 0xFF);  // high byte first
        result[1] = (byte) (value & 0xFF);         // low byte second
        return result;
    }
}
