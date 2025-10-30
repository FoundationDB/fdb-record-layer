/*
 * CompactStorageAdapter.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@code CompactStorageAdapter} class is a concrete implementation of {@link StorageAdapter} for managing HNSW
 * graph data in a compact format.
 * <p>
 * It handles the serialization and deserialization of graph nodes to and from a persistent data store. This
 * implementation is optimized for space efficiency by storing nodes with their accompanying vector data and by storing
 * just neighbor primary keys. It extends {@link AbstractStorageAdapter} to inherit common storage logic.
 */
class CompactStorageAdapter extends AbstractStorageAdapter<NodeReference> implements StorageAdapter<NodeReference> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(CompactStorageAdapter.class);

    /**
     * Constructs a new {@code CompactStorageAdapter}.
     *
     * @param config the HNSW graph configuration, must not be null. See {@link Config}.
     * @param nodeFactory the factory used to create new nodes of type {@link NodeReference}, must not be null.
     * @param subspace the {@link Subspace} where the graph data is stored, must not be null.
     * @param onWriteListener the listener to be notified of write events, must not be null.
     * @param onReadListener the listener to be notified of read events, must not be null.
     */
    public CompactStorageAdapter(@Nonnull final Config config,
                                 @Nonnull final NodeFactory<NodeReference> nodeFactory,
                                 @Nonnull final Subspace subspace,
                                 @Nonnull final OnWriteListener onWriteListener,
                                 @Nonnull final OnReadListener onReadListener) {
        super(config, nodeFactory, subspace, onWriteListener, onReadListener);
    }

    /**
     * Returns this storage adapter instance, as it is already a compact storage adapter.
     * @return the current instance, which serves as its own compact representation.
     *         This will never be {@code null}.
     */
    @Nonnull
    @Override
    public StorageAdapter<NodeReference> asCompactStorageAdapter() {
        return this;
    }

    /**
     * Returns this adapter as a {@code StorageAdapter} that supports inlining.
     * <p>
     * This operation is not supported by a compact storage adapter. Calling this method on this implementation will
     * always result in an {@code IllegalStateException}.
     *
     * @return an instance of {@code StorageAdapter} that supports inlining
     *
     * @throws IllegalStateException unconditionally, as this operation is not supported
     * on a compact storage adapter.
     */
    @Nonnull
    @Override
    public StorageAdapter<NodeReferenceWithVector> asInliningStorageAdapter() {
        throw new IllegalStateException("cannot call this method on a compact storage adapter");
    }

    /**
     * Asynchronously fetches a node from the database for a given layer and primary key.
     * <p>
     * This internal method constructs a raw byte key from the {@code layer} and {@code primaryKey} within the store's
     * data subspace. It then uses the provided {@link ReadTransaction} to retrieve the raw value. If a value is found,
     * it is deserialized into a {@link AbstractNode} object using the {@code nodeFromRaw} method.
     *
     * @param readTransaction the transaction to use for the read operation
     * @param storageTransform an affine vector transformation operator that is used to transform the fetched vector
     *        into the current storage space
     * @param layer the layer of the node to fetch
     * @param primaryKey the primary key of the node to fetch
     *
     * @return a future that will complete with the fetched {@link AbstractNode}
     *
     * @throws IllegalStateException if the node cannot be found in the database for the given key
     */
    @Nonnull
    @Override
    protected CompletableFuture<AbstractNode<NodeReference>> fetchNodeInternal(@Nonnull final ReadTransaction readTransaction,
                                                                               @Nonnull final AffineOperator storageTransform,
                                                                               final int layer,
                                                                               @Nonnull final Tuple primaryKey) {
        final byte[] keyBytes = getDataSubspace().pack(Tuple.from(layer, primaryKey));

        return readTransaction.get(keyBytes)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        throw new IllegalStateException("cannot fetch node");
                    }
                    return nodeFromRaw(storageTransform, layer, primaryKey, keyBytes, valueBytes);
                });
    }

    /**
     * Deserializes a raw key-value byte array pair into a {@code Node}.
     * <p>
     * This method first converts the {@code valueBytes} into a {@link Tuple} and then,
     * along with the {@code primaryKey}, constructs the final {@code Node} object.
     * It also notifies any registered {@link OnReadListener} about the raw key-value
     * read and the resulting node creation.
     *
     * @param storageTransform an affine vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param layer the layer of the HNSW where this node resides
     * @param primaryKey the primary key for the node
     * @param keyBytes the raw byte representation of the node's key
     * @param valueBytes the raw byte representation of the node's value, which will be deserialized
     *
     * @return a non-null, deserialized {@link AbstractNode} object
     */
    @Nonnull
    private AbstractNode<NodeReference> nodeFromRaw(@Nonnull final AffineOperator storageTransform, final int layer,
                                                    final @Nonnull Tuple primaryKey,
                                                    @Nonnull final byte[] keyBytes, @Nonnull final byte[] valueBytes) {
        final Tuple nodeTuple = Tuple.fromBytes(valueBytes);
        final AbstractNode<NodeReference> node = nodeFromKeyValuesTuples(storageTransform, primaryKey, nodeTuple);
        final OnReadListener onReadListener = getOnReadListener();
        onReadListener.onNodeRead(layer, node);
        onReadListener.onKeyValueRead(layer, keyBytes, valueBytes);
        return node;
    }

    /**
     * Constructs a compact {@link AbstractNode} from its representation as stored key and value tuples.
     * <p>
     * This method deserializes a node by extracting its components from the provided tuples. It verifies that the
     * node is of type {@link NodeKind#COMPACT} before delegating the final construction to
     * {@link #compactNodeFromTuples(AffineOperator, Tuple, Tuple, Tuple)}. The {@code valueTuple} is expected to have
     * a specific structure: the serialized node kind at index 0, a nested tuple for the vector at index 1, and a nested
     * tuple for the neighbors at index 2.
     *
     * @param storageTransform an affine vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param primaryKey the tuple representing the primary key of the node
     * @param valueTuple the tuple containing the serialized node data, including kind, vector, and neighbors
     *
     * @return the reconstructed compact {@link AbstractNode}
     *
     * @throws com.google.common.base.VerifyException if the node kind encoded in {@code valueTuple} is not
     *         {@link NodeKind#COMPACT}
     */
    @Nonnull
    private AbstractNode<NodeReference> nodeFromKeyValuesTuples(@Nonnull final AffineOperator storageTransform,
                                                                @Nonnull final Tuple primaryKey,
                                                                @Nonnull final Tuple valueTuple) {
        final NodeKind nodeKind = NodeKind.fromSerializedNodeKind((byte)valueTuple.getLong(0));
        Verify.verify(nodeKind == NodeKind.COMPACT);

        final Tuple vectorTuple;
        final Tuple neighborsTuple;

        vectorTuple = valueTuple.getNestedTuple(1);
        neighborsTuple = valueTuple.getNestedTuple(2);
        return compactNodeFromTuples(storageTransform, primaryKey, vectorTuple, neighborsTuple);
    }

    /**
     * Creates a compact in-memory representation of a graph node from its constituent storage tuples.
     * <p>
     * This method deserializes the raw data stored in {@code Tuple} objects into their
     * corresponding in-memory types. It extracts the vector, constructs a list of
     * {@link NodeReference} objects for the neighbors, and then uses a factory to
     * assemble the final {@code Node} object.
     * </p>
     *
     * @param storageTransform an affine vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param primaryKey the tuple representing the node's primary key
     * @param vectorTuple the tuple containing the node's vector data
     * @param neighborsTuple the tuple containing a list of nested tuples, where each nested tuple represents a neighbor
     *
     * @return a new {@code Node} instance containing the deserialized data from the input tuples
     */
    @Nonnull
    private AbstractNode<NodeReference> compactNodeFromTuples(@Nonnull final AffineOperator storageTransform,
                                                              @Nonnull final Tuple primaryKey,
                                                              @Nonnull final Tuple vectorTuple,
                                                              @Nonnull final Tuple neighborsTuple) {
        final RealVector vector =
                storageTransform.invertedApply(StorageAdapter.vectorFromTuple(getConfig(), vectorTuple));
        final List<NodeReference> nodeReferences = Lists.newArrayListWithExpectedSize(neighborsTuple.size());

        for (int i = 0; i < neighborsTuple.size(); i ++) {
            final Tuple neighborTuple = neighborsTuple.getNestedTuple(i);
            nodeReferences.add(new NodeReference(neighborTuple));
        }

        return getNodeFactory().create(primaryKey, vector, nodeReferences);
    }

    /**
     * Writes the internal representation of a compact node to the data store within a given transaction.
     * This method handles the serialization of the node's vector and its final set of neighbors based on the
     * provided {@code neighborsChangeSet}.
     *
     * <p>The node is stored as a {@link Tuple} with the structure {@code (NodeKind, RealVector, NeighborPrimaryKeys)}.
     * The key for the storage is derived from the node's layer and its primary key. After writing, it notifies any
     * registered write listeners via {@code onNodeWritten} and {@code onKeyValueWritten}.
     *
     * @param transaction the {@link Transaction} to use for the write operation.
     * @param quantizer the quantizer to use
     * @param node the {@link AbstractNode} to be serialized and written; it is processed as a {@link CompactNode}.
     * @param layer the graph layer index for the node, used to construct the storage key.
     * @param neighborsChangeSet a {@link NeighborsChangeSet} containing the additions and removals, which are
     * merged to determine the final set of neighbors to be written.
     */
    @Override
    public void writeNodeInternal(@Nonnull final Transaction transaction, @Nonnull final Quantizer quantizer,
                                  @Nonnull final AbstractNode<NodeReference> node, final int layer,
                                  @Nonnull final NeighborsChangeSet<NodeReference> neighborsChangeSet) {
        final byte[] key = getDataSubspace().pack(Tuple.from(layer, node.getPrimaryKey()));

        final List<Object> nodeItems = Lists.newArrayListWithExpectedSize(3);
        nodeItems.add(NodeKind.COMPACT.getSerialized());
        final CompactNode compactNode = node.asCompactNode();
        nodeItems.add(StorageAdapter.tupleFromVector(quantizer.encode(compactNode.getVector())));

        final Iterable<NodeReference> neighbors = neighborsChangeSet.merge();

        final List<Tuple> neighborItems = Lists.newArrayList();
        for (final NodeReference neighborReference : neighbors) {
            neighborItems.add(neighborReference.getPrimaryKey());
        }
        nodeItems.add(Tuple.fromList(neighborItems));

        final Tuple nodeTuple = Tuple.fromList(nodeItems);

        final byte[] value = nodeTuple.pack();
        transaction.set(key, value);
        getOnWriteListener().onNodeWritten(layer, node);
        getOnWriteListener().onKeyValueWritten(layer, key, value);

        if (logger.isTraceEnabled()) {
            logger.trace("written neighbors of primaryKey={}, oldSize={}, newSize={}", node.getPrimaryKey(),
                    node.getNeighbors().size(), neighborItems.size());
        }
    }

    /**
     * Scans a given layer for nodes, returning an iterable over the results.
     * <p>
     * This method reads a limited number of nodes from a specific layer in the underlying data store.
     * The scan can be started from a specific point using the {@code lastPrimaryKey} parameter, which is
     * useful for paginating through the nodes in a large layer.
     *
     * @param readTransaction the transaction to use for reading data; must not be {@code null}
     * @param layer the layer to scan for nodes
     * @param lastPrimaryKey the primary key of the last node from a previous scan. If {@code null},
     * the scan starts from the beginning of the layer.
     * @param maxNumRead the maximum number of nodes to read in this scan
     *
     * @return an {@link Iterable} of {@link AbstractNode} objects found in the specified layer,
     * limited by {@code maxNumRead}
     */
    @Nonnull
    @Override
    public Iterable<AbstractNode<NodeReference>> scanLayer(@Nonnull final ReadTransaction readTransaction, int layer,
                                                           @Nullable final Tuple lastPrimaryKey, int maxNumRead) {
        final byte[] layerPrefix = getDataSubspace().pack(Tuple.from(layer));
        final Range range =
                lastPrimaryKey == null
                ? Range.startsWith(layerPrefix)
                : new Range(ByteArrayUtil.strinc(getDataSubspace().pack(Tuple.from(layer, lastPrimaryKey))),
                        ByteArrayUtil.strinc(layerPrefix));
        final AsyncIterable<KeyValue> itemsIterable =
                readTransaction.getRange(range, maxNumRead, false, StreamingMode.ITERATOR);

        return AsyncUtil.mapIterable(itemsIterable, keyValue -> {
            final byte[] key = keyValue.getKey();
            final byte[] value = keyValue.getValue();
            final Tuple primaryKey = getDataSubspace().unpack(key).getNestedTuple(1);
            return nodeFromRaw(AffineOperator.identity(), layer, primaryKey, key, value);
        });
    }
}
