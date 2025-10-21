/*
 * InliningStorageAdapter.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link StorageAdapter} for an HNSW graph that stores node vectors "in-line" with the node's
 * neighbor information.
 * <p>
 * In this storage model, each key-value pair in the database represents a single neighbor relationship. The key
 * contains the primary keys of both the source node and the neighbor node, while the value contains the neighbor's
 * vector. This contrasts with a "compact" storage model where a compact node represents a vector and all of its
 * neighbors. This adapter is responsible for serializing and deserializing these structures to and from the underlying
 * key-value store.
 *
 * @see StorageAdapter
 * @see HNSW
 */
class InliningStorageAdapter extends AbstractStorageAdapter<NodeReferenceWithVector> implements StorageAdapter<NodeReferenceWithVector> {
    /**
     * Constructs a new {@code InliningStorageAdapter} with the given configuration and components.
     * <p>
     * This constructor initializes the storage adapter by passing all necessary components
     * to its superclass.
     *
     * @param config the HNSW configuration to use for the graph
     * @param nodeFactory the factory to create new {@link NodeReferenceWithVector} instances
     * @param subspace the subspace where the HNSW graph data is stored
     * @param onWriteListener the listener to be notified on write operations
     * @param onReadListener the listener to be notified on read operations
     */
    public InliningStorageAdapter(@Nonnull final HNSW.Config config,
                                  @Nonnull final NodeFactory<NodeReferenceWithVector> nodeFactory,
                                  @Nonnull final Subspace subspace,
                                  @Nonnull final OnWriteListener onWriteListener,
                                  @Nonnull final OnReadListener onReadListener) {
        super(config, nodeFactory, subspace, onWriteListener, onReadListener);
    }

    /**
     * Throws {@link IllegalStateException} because an inlining storage adapter cannot be converted to a compact one.
     * <p>
     * This operation is fundamentally not supported for this type of adapter. An inlining adapter stores data directly
     * within a parent structure, which is incompatible with the standalone nature of a compact storage format.
     * @return This method never returns a value as it always throws an exception.
     * @throws IllegalStateException always, as this operation is not supported.
     */
    @Nonnull
    @Override
    public StorageAdapter<NodeReference> asCompactStorageAdapter() {
        throw new IllegalStateException("cannot call this method on an inlining storage adapter");
    }

    /**
     * Returns this object instance as a {@code StorageAdapter} that supports inlining.
     * <p>
     * This implementation returns the current instance ({@code this}) because the class itself is designed to handle
     * inlining directly, thus no separate adapter object is needed.
     * @return a non-null reference to this object as an {@link StorageAdapter} for inlining.
     */
    @Nonnull
    @Override
    public StorageAdapter<NodeReferenceWithVector> asInliningStorageAdapter() {
        return this;
    }

    /**
     * Asynchronously fetches a single node from a given layer by its primary key.
     * <p>
     * This internal method constructs a prefix key based on the {@code layer} and {@code primaryKey}.
     * It then performs an asynchronous range scan to retrieve all key-value pairs associated with that prefix.
     * Finally, it reconstructs the complete {@link Node} object from the collected raw data using
     * the {@code nodeFromRaw} method.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param layer the layer of the node to fetch
     * @param primaryKey the primary key of the node to fetch
     *
     * @return a {@link CompletableFuture} that will complete with the fetched {@link Node} containing
     *         {@link NodeReferenceWithVector}s
     */
    @Nonnull
    @Override
    protected CompletableFuture<Node<NodeReferenceWithVector>> fetchNodeInternal(@Nonnull final ReadTransaction readTransaction,
                                                                                 final int layer,
                                                                                 @Nonnull final Tuple primaryKey) {
        final byte[] rangeKey = getNodeKey(layer, primaryKey);

        return AsyncUtil.collect(readTransaction.getRange(Range.startsWith(rangeKey),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL), readTransaction.getExecutor())
                .thenApply(keyValues -> nodeFromRaw(layer, primaryKey, keyValues));
    }

    /**
     * Constructs a {@code Node} from its raw key-value representation from storage.
     * <p>
     * This method is responsible for deserializing a node and its neighbors. It processes a list of {@code KeyValue}
     * pairs, where each pair represents a neighbor of the node being constructed. Each neighbor is converted from its
     * raw form into a {@link NodeReferenceWithVector} by calling the {@link #neighborFromRaw(int, byte[], byte[])}
     * method.
     * <p>
     * Once the node is created with its primary key and list of neighbors, it notifies the configured
     * {@link OnReadListener} of the read operation.
     *
     * @param layer the layer in the graph where this node exists
     * @param primaryKey the primary key that uniquely identifies the node
     * @param keyValues a list of {@code KeyValue} pairs representing the raw data of the node's neighbors
     *
     * @return a non-null, fully constructed {@code Node} object with its neighbors
     */
    @Nonnull
    private Node<NodeReferenceWithVector> nodeFromRaw(final int layer,
                                                      @Nonnull final Tuple primaryKey,
                                                      @Nonnull final List<KeyValue> keyValues) {
        final OnReadListener onReadListener = getOnReadListener();

        final ImmutableList.Builder<NodeReferenceWithVector> nodeReferencesWithVectorBuilder = ImmutableList.builder();
        for (final KeyValue keyValue : keyValues) {
            nodeReferencesWithVectorBuilder.add(neighborFromRaw(layer, keyValue.getKey(), keyValue.getValue()));
        }

        final Node<NodeReferenceWithVector> node =
                getNodeFactory().create(primaryKey, null, nodeReferencesWithVectorBuilder.build());
        onReadListener.onNodeRead(layer, node);
        return node;
    }

    /**
     * Constructs a {@code NodeReferenceWithVector} from raw key and value byte arrays retrieved from storage.
     * <p>
     * This helper method deserializes a neighbor's data. It unpacks the provided {@code key} to extract the neighbor's
     * primary key and unpacks the {@code value} to extract the neighbor's vector. It also notifies the configured
     * {@link OnReadListener} of the read operation.
     *
     * @param layer the layer of the graph where the neighbor node is located.
     * @param key the raw byte array key from the database, which contains the neighbor's primary key.
     * @param value the raw byte array value from the database, which represents the neighbor's vector.
     * @return a new {@link NodeReferenceWithVector} instance representing the deserialized neighbor.
     * @throws IllegalArgumentException if the key or value byte arrays are malformed and cannot be unpacked.
     */
    @Nonnull
    private NodeReferenceWithVector neighborFromRaw(final int layer, final @Nonnull byte[] key, final byte[] value) {
        final OnReadListener onReadListener = getOnReadListener();
        onReadListener.onKeyValueRead(layer, key, value);

        final Tuple neighborKeyTuple = getDataSubspace().unpack(key);
        final Tuple neighborValueTuple = Tuple.fromBytes(value);

        return neighborFromTuples(neighborKeyTuple, neighborValueTuple);
    }

    /**
     * Constructs a {@code NodeReferenceWithVector} from tuples retrieved from storage.
     * <p>
     * @param keyTuple the key tuple from the database, which contains the neighbor's primary key.
     * @param valueTuple the value tuple from the database, which represents the neighbor's vector.
     * @return a new {@link NodeReferenceWithVector} instance representing the deserialized neighbor.
     * @throws IllegalArgumentException if the key or value byte arrays are malformed and cannot be unpacked.
     */
    @Nonnull
    private NodeReferenceWithVector neighborFromTuples(final @Nonnull Tuple keyTuple, final Tuple valueTuple) {
        final Tuple neighborPrimaryKey = keyTuple.getNestedTuple(2); // neighbor primary key
        final RealVector neighborVector = StorageAdapter.vectorFromTuple(getConfig(), valueTuple); // the entire value is the vector
        return new NodeReferenceWithVector(neighborPrimaryKey, neighborVector);
    }

    /**
     * Writes a given node and its neighbor changes to the specified layer within a transaction.
     * <p>
     * This implementation first converts the provided {@link Node} to an {@link  InliningNode}. It then delegates the
     * writing of neighbor modifications to the {@link NeighborsChangeSet#writeDelta} method. After the changes are
     * written, it notifies the registered {@code OnWriteListener} that the node has been processed
     * via {@code getOnWriteListener().onNodeWritten()}.
     *
     * @param transaction the transaction context for the write operation; must not be null
     * @param node the node to be written, which is expected to be an
     * {@code InliningNode}; must not be null
     * @param layer the layer index where the node and its neighbor changes should be written
     * @param neighborsChangeSet the set of changes to the node's neighbors to be
     * persisted; must not be null
     */
    @Override
    public void writeNodeInternal(@Nonnull final Transaction transaction, @Nonnull final Node<NodeReferenceWithVector> node,
                                  final int layer, @Nonnull final NeighborsChangeSet<NodeReferenceWithVector> neighborsChangeSet) {
        final InliningNode inliningNode = node.asInliningNode();

        neighborsChangeSet.writeDelta(this, transaction, layer, inliningNode, t -> true);
        getOnWriteListener().onNodeWritten(layer, node);
    }

    /**
     * Constructs the raw database key for a node based on its layer and primary key.
     * <p>
     * This key is created by packing a tuple containing the specified {@code layer} and the node's {@code primaryKey}
     * within the data subspace. The resulting byte array is suitable for use in direct database lookups and preserves
     * the sort order of the components.
     *
     * @param layer the layer index where the node resides
     * @param primaryKey the primary key that uniquely identifies the node within its layer,
     * encapsulated in a {@link Tuple}
     *
     * @return a byte array representing the packed key for the specified node
     */
    @Nonnull
    private byte[] getNodeKey(final int layer, @Nonnull final Tuple primaryKey) {
        return getDataSubspace().pack(Tuple.from(layer, primaryKey));
    }

    /**
     * Writes a neighbor for a given node to the underlying storage within a specific transaction.
     * <p>
     * This method serializes the neighbor's vector and constructs a unique key based on the layer, the source
     * {@code node}, and the neighbor's primary key. It then persists this key-value pair using the provided
     * {@link Transaction}. After a successful write, it notifies any registered listeners.
     *
     * @param transaction the {@link Transaction} to use for the write operation
     * @param layer the layer index where the node and its neighbor reside
     * @param node the source {@link Node} for which the neighbor is being written
     * @param neighbor the {@link NodeReferenceWithVector} representing the neighbor to persist
     */
    public void writeNeighbor(@Nonnull final Transaction transaction, final int layer,
                              @Nonnull final Node<NodeReferenceWithVector> node, @Nonnull final NodeReferenceWithVector neighbor) {
        final byte[] neighborKey = getNeighborKey(layer, node, neighbor.getPrimaryKey());
        final byte[] value = StorageAdapter.tupleFromVector(neighbor.getVector()).pack();
        transaction.set(neighborKey,
                value);
        getOnWriteListener().onNeighborWritten(layer, node, neighbor);
        getOnWriteListener().onKeyValueWritten(layer, neighborKey, value);
    }

    /**
     * Deletes a neighbor edge from a given node within a specific layer.
     * <p>
     * This operation removes the key-value pair representing the neighbor relationship from the database within the
     * given {@link Transaction}. It also notifies the {@code onWriteListener} about the deletion.
     *
     * @param transaction the transaction in which to perform the deletion
     * @param layer the layer of the graph where the node resides
     * @param node the node from which the neighbor edge is removed
     * @param neighborPrimaryKey the primary key of the neighbor node to be deleted
     */
    public void deleteNeighbor(@Nonnull final Transaction transaction, final int layer,
                               @Nonnull final Node<NodeReferenceWithVector> node, @Nonnull final Tuple neighborPrimaryKey) {
        transaction.clear(getNeighborKey(layer, node, neighborPrimaryKey));
        getOnWriteListener().onNeighborDeleted(layer, node, neighborPrimaryKey);
    }

    /**
     * Constructs the key for a specific neighbor of a node within a given layer.
     * <p>
     * This key is used to uniquely identify and store the neighbor relationship in the underlying data store. It is
     * formed by packing a {@link Tuple} containing the {@code layer}, the primary key of the source {@code node}, and
     * the {@code neighborPrimaryKey}.
     *
     * @param layer the layer of the graph where the node and its neighbor reside
     * @param node the non-null source node for which the neighbor key is being generated
     * @param neighborPrimaryKey the non-null primary key of the neighbor node
     * @return a non-null byte array representing the packed key for the neighbor relationship
     */
    @Nonnull
    private byte[] getNeighborKey(final int layer,
                                  @Nonnull final Node<NodeReferenceWithVector> node,
                                  @Nonnull final Tuple neighborPrimaryKey) {
        return getDataSubspace().pack(Tuple.from(layer, node.getPrimaryKey(), neighborPrimaryKey));
    }

    /**
     * Scans a specific layer of the graph, reconstructing nodes and their neighbors from the underlying key-value
     * store.
     * <p>
     * This method reads raw {@link com.apple.foundationdb.KeyValue} records from the database within a given layer.
     * It groups adjacent records that belong to the same parent node and uses a {@link NodeFactory} to construct
     * {@link Node} objects. The method supports pagination through the {@code lastPrimaryKey} parameter, allowing for
     * incremental scanning of large layers.
     *
     * @param readTransaction the transaction to use for reading data
     * @param layer the layer of the graph to scan
     * @param lastPrimaryKey the primary key of the last node read in a previous scan, used for pagination.
     * If {@code null}, the scan starts from the beginning of the layer.
     * @param maxNumRead the maximum number of raw key-value records to read from the database
     *
     * @return an {@code Iterable} of {@link Node} objects reconstructed from the scanned layer. Each node contains
     * its neighbors within that layer.
     */
    @Nonnull
    @Override
    public Iterable<Node<NodeReferenceWithVector>> scanLayer(@Nonnull final ReadTransaction readTransaction, int layer,
                                                             @Nullable final Tuple lastPrimaryKey, int maxNumRead) {
        final OnReadListener onReadListener = getOnReadListener();
        final byte[] layerPrefix = getDataSubspace().pack(Tuple.from(layer));
        final Range range =
                lastPrimaryKey == null
                ? Range.startsWith(layerPrefix)
                : new Range(ByteArrayUtil.strinc(getDataSubspace().pack(Tuple.from(layer, lastPrimaryKey))),
                        ByteArrayUtil.strinc(layerPrefix));
        final AsyncIterable<KeyValue> itemsIterable =
                readTransaction.getRange(range,
                        maxNumRead, false, StreamingMode.ITERATOR);
        Tuple nodePrimaryKey = null;
        ImmutableList.Builder<Node<NodeReferenceWithVector>> nodeBuilder = ImmutableList.builder();
        ImmutableList.Builder<NodeReferenceWithVector> neighborsBuilder = null;
        for (final KeyValue item: itemsIterable) {
            final byte[] key = item.getKey();
            final byte[] value = item.getValue();
            onReadListener.onKeyValueRead(layer, key, value);

            final Tuple neighborKeyTuple = getDataSubspace().unpack(key);
            final Tuple neighborValueTuple = Tuple.fromBytes(value);
            final NodeReferenceWithVector neighbor = neighborFromTuples(neighborKeyTuple, neighborValueTuple);
            final Tuple nodePrimaryKeyFromNeighbor = neighborKeyTuple.getNestedTuple(1);
            if (nodePrimaryKey == null || !nodePrimaryKey.equals(nodePrimaryKeyFromNeighbor)) {
                if (nodePrimaryKey != null) {
                    nodeBuilder.add(getNodeFactory().create(nodePrimaryKey, null, neighborsBuilder.build()));
                }
                nodePrimaryKey = nodePrimaryKeyFromNeighbor;
                neighborsBuilder = ImmutableList.builder();
            }
            neighborsBuilder.add(neighbor);
        }

        // there may be a rest; throw it away
        return nodeBuilder.build();
    }
}
