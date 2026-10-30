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
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
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
     * The maximum size of a value in FoundationDB. There is no shared constant for this in {@code fdb-extensions}
     * ({@code FDBRecordStore.VALUE_SIZE_LIMIT} lives in {@code fdb-record-layer-core}, which depends on this module).
     */
    static final int MAX_APPENDABLE_VALUE_SIZE = 100_000;

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

    @Nonnull
    @Override
    public Transformed<RealVector> getVector(@Nonnull final NodeReference nodeReference, @Nonnull final AbstractNode<NodeReference> node) {
        return node.asCompactNode().getVector();
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
     * @return a future that will complete with the fetched {@link AbstractNode} or {@code null} if the node cannot
     *         be fetched
     */
    @Nonnull
    @Override
    protected CompletableFuture<AbstractNode<NodeReference>> fetchNodeInternal(@Nonnull final ReadTransaction readTransaction,
                                                                               @Nonnull final StorageTransform storageTransform,
                                                                               final int layer,
                                                                               @Nonnull final Tuple primaryKey) {
        final byte[] keyBytes = getNodeKey(layer, primaryKey);
        return readTransaction.get(keyBytes)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        return null;
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
     * @param storageTransform a vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param layer the layer of the HNSW where this node resides
     * @param primaryKey the primary key for the node
     * @param keyBytes the raw byte representation of the node's key
     * @param valueBytes the raw byte representation of the node's value, which will be deserialized
     *
     * @return a non-null, deserialized {@link AbstractNode} object
     */
    @Nonnull
    private AbstractNode<NodeReference> nodeFromRaw(@Nonnull final StorageTransform storageTransform, final int layer,
                                                    final @Nonnull Tuple primaryKey,
                                                    @Nonnull final byte[] keyBytes, @Nonnull final byte[] valueBytes) {
        final Tuple nodeTuple = Tuple.fromBytes(valueBytes);
        final AbstractNode<NodeReference> node =
                nodeFromKeyValuesTuples(storageTransform, primaryKey, nodeTuple, valueBytes.length);
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
     * {@link #compactNodeFromTuples(StorageTransform, Tuple, Tuple, Tuple, Tuple, int)}. The {@code valueTuple} is
     * expected to have one of two structures, depending on the serialized node kind at index 0. For
     * {@link NodeKind#COMPACT}, a nested tuple for the vector at index 1, a nested tuple for the neighbors at index 2
     * and, optionally a tuple for additional values at index 3. For {@link NodeKind#COMPACT_APPENDABLE}, a nested
     * tuple for the vector at index 1, a tuple for the additional values (or {@code null}) at index 2, followed by
     * one nested tuple per neighbor.
     *
     * @param storageTransform a vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param primaryKey the tuple representing the primary key of the node
     * @param valueTuple the tuple containing the serialized node data, including kind, vector, and neighbors
     * @param valueSize the size of the serialized value in bytes
     *
     * @return the reconstructed compact {@link AbstractNode}
     *
     * @throws com.google.common.base.VerifyException if the node kind encoded in {@code valueTuple} is neither
     *         {@link NodeKind#COMPACT} nor {@link NodeKind#COMPACT_APPENDABLE}
     */
    @Nonnull
    private AbstractNode<NodeReference> nodeFromKeyValuesTuples(@Nonnull final StorageTransform storageTransform,
                                                                @Nonnull final Tuple primaryKey,
                                                                @Nonnull final Tuple valueTuple,
                                                                final int valueSize) {
        final NodeKind nodeKind = NodeKind.fromSerializedNodeKind((byte)valueTuple.getLong(0));
        Verify.verify(nodeKind.isCompact());

        final Tuple vectorTuple = valueTuple.getNestedTuple(1);
        if (nodeKind == NodeKind.COMPACT_APPENDABLE) {
            Verify.verify(valueTuple.size() >= 3);
            final Tuple additionalValuesTuple = valueTuple.get(2) == null ? null : valueTuple.getNestedTuple(2);
            final Tuple neighborsTuple = Tuple.fromList(valueTuple.getItems().subList(3, valueTuple.size()));
            return compactNodeFromTuples(storageTransform, primaryKey, vectorTuple, additionalValuesTuple,
                    neighborsTuple, valueSize);
        }

        final Tuple neighborsTuple = valueTuple.getNestedTuple(2);
        final Tuple additionalValuesTuple = valueTuple.size() > 3 ? valueTuple.getNestedTuple(3) : null;

        return compactNodeFromTuples(storageTransform, primaryKey, vectorTuple, additionalValuesTuple, neighborsTuple,
                -1);
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
     * @param storageTransform a vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param primaryKey the tuple representing the node's primary key
     * @param vectorTuple the tuple containing the node's vector data
     * @param neighborsTuple the tuple containing a list of nested tuples, where each nested tuple represents a neighbor
     * @param appendableValueSize the size of the serialized value if it is in the appendable format, {@code -1}
     *        otherwise
     *
     * @return a new {@code Node} instance containing the deserialized data from the input tuples
     */
    @Nonnull
    private AbstractNode<NodeReference> compactNodeFromTuples(@Nonnull final StorageTransform storageTransform,
                                                              @Nonnull final Tuple primaryKey,
                                                              @Nonnull final Tuple vectorTuple,
                                                              @Nullable final Tuple additionalValuesTuple,
                                                              @Nonnull final Tuple neighborsTuple,
                                                              final int appendableValueSize) {
        final Transformed<RealVector> vector =
                storageTransform.transform(StorageHelpers.vectorFromTuple(getConfig(), vectorTuple));
        final List<NodeReference> nodeReferences = Lists.newArrayListWithExpectedSize(neighborsTuple.size());

        for (int i = 0; i < neighborsTuple.size(); i ++) {
            final Tuple neighborTuple = neighborsTuple.getNestedTuple(i);
            nodeReferences.add(new NodeReference(neighborTuple));
        }

        // not using the node factory as it cannot convey how the node is stored
        return new CompactNode(primaryKey, nodeReferences, vector, additionalValuesTuple, appendableValueSize);
    }

    /**
     * Writes the internal representation of a compact node to the data store within a given transaction.
     * This method handles the serialization of the node's vector and its final set of neighbors based on the
     * provided {@code neighborsChangeSet}.
     *
     * <p>If {@link Config#writeAppendableNodes()} is not set, the node is stored as a {@link Tuple} with the structure
     * {@code (NodeKind.COMPACT, RealVector, NeighborPrimaryKeys, [additionalValues])}. Otherwise, it is stored in the
     * format of {@link NodeKind#COMPACT_APPENDABLE}, i.e.
     * {@code (NodeKind.COMPACT_APPENDABLE, RealVector, additionalValues or null, NeighborPrimaryKey...)}. If the
     * node was read from a value in that format and {@code neighborsChangeSet} only adds neighbors, just the added
     * neighbors are appended to the stored value. In all other cases the entire node is written.
     * The key for the storage is derived from the node's layer and its primary key. After writing, it notifies any
     * registered write listeners via {@code onNodeWritten} and {@code onKeyValueWritten}.
     *
     * @param transaction the {@link Transaction} to use for the write operation.
     * @param quantizer the quantizer to use
     * @param layer the graph layer index for the node, used to construct the storage key.
     * @param node the {@link AbstractNode} to be serialized and written; it is processed as a {@link CompactNode}.
     * @param neighborsChangeSet a {@link NeighborsChangeSet} containing the additions and removals, which are
     * merged to determine the final set of neighbors to be written.
     */
    @Override
    public void writeNodeInternal(@Nonnull final Transaction transaction, @Nonnull final Quantizer quantizer,
                                  final int layer, @Nonnull final AbstractNode<NodeReference> node,
                                  @Nonnull final NeighborsChangeSet<NodeReference> neighborsChangeSet) {
        final byte[] key = getNodeKey(layer, node.getPrimaryKey());
        final CompactNode compactNode = node.asCompactNode();

        if (getConfig().writeAppendableNodes() && tryAppendNeighbors(transaction, layer, key, compactNode,
                neighborsChangeSet)) {
            return;
        }

        final Tuple additionalValues = compactNode.getAdditionalValues();
        final boolean writeAppendable = getConfig().writeAppendableNodes();
        final List<Object> nodeItems = Lists.newArrayListWithExpectedSize(additionalValues == null ? 3 : 4);
        nodeItems.add((writeAppendable ? NodeKind.COMPACT_APPENDABLE : NodeKind.COMPACT).getSerialized());
        // getting underlying vector is okay as it is only written to the database
        nodeItems.add(StorageHelpers.tupleFromVector(quantizer.encode(compactNode.getVector())));

        final Iterable<NodeReference> neighbors = neighborsChangeSet.merge();

        final List<Tuple> neighborItems = Lists.newArrayList();
        for (final NodeReference neighborReference : neighbors) {
            neighborItems.add(neighborReference.getPrimaryKey());
        }
        if (writeAppendable) {
            nodeItems.add(additionalValues);
            nodeItems.addAll(neighborItems);
        } else {
            nodeItems.add(Tuple.fromList(neighborItems));
            if (additionalValues != null) {
                nodeItems.add(additionalValues);
            }
        }

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
     * Persists a change set that only adds neighbors by appending the added neighbors to the stored value of the node.
     * This only works if the stored value is known to be in the appendable format, which is the case iff the node was
     * read from such a value, and if the appended value does not exceed {@link #MAX_APPENDABLE_VALUE_SIZE}, as
     * {@link MutationType#APPEND_IF_FITS} silently does nothing otherwise.
     * <p>
     * This relies on the node having been read with a (non-snapshot) read in this transaction, whose read conflict
     * guards against the stored value having been rewritten or deleted concurrently, and on the node not having been
     * modified in this transaction since it was read: both the size check and the change set are based on the node as
     * it was read.
     * <p>
     * The second assumption is not enforced. If several operations of the same transaction run concurrently and
     * modify the same node, each of them works on its own copy of the node. For a full rewrite that means that the last
     * write wins and the neighbors added by the others are lost. For an append no neighbors are lost but a neighbor can
     * be added twice, and the appended neighbors can in aggregate exceed the maximum size of a value, in which case
     * FDB silently ignores the append that does not fit (this adapter only checks the size of the node as it was
     * read). If this turns out to matter, accesses to a node can be serialized per transaction, similar to how an
     * {@code AsyncLock} serializes accesses to a whole index.
     *
     * @param transaction the transaction to use for the write operation
     * @param layer the graph layer index for the node
     * @param key the key of the node
     * @param compactNode the node as it was read
     * @param neighborsChangeSet the change set to persist
     *
     * @return {@code true} if the neighbors were appended, {@code false} if the caller needs to write the entire node
     */
    private boolean tryAppendNeighbors(@Nonnull final Transaction transaction, final int layer, final byte[] key,
                                       @Nonnull final CompactNode compactNode,
                                       @Nonnull final NeighborsChangeSet<NodeReference> neighborsChangeSet) {
        if (!compactNode.isAppendable()) {
            return false;
        }
        final List<NodeReference> insertions = neighborsChangeSet.getPureInsertions();
        if (insertions == null || insertions.isEmpty()) {
            return false;
        }

        final List<Tuple> insertedPrimaryKeys = Lists.newArrayListWithExpectedSize(insertions.size());
        for (final NodeReference insertion : insertions) {
            insertedPrimaryKeys.add(insertion.getPrimaryKey());
        }
        final byte[] appendBytes = Tuple.fromList(insertedPrimaryKeys).pack();
        if ((long)compactNode.getAppendableValueSize() + appendBytes.length > MAX_APPENDABLE_VALUE_SIZE) {
            return false;
        }

        transaction.mutate(MutationType.APPEND_IF_FITS, key, appendBytes);
        getOnWriteListener().onNodeWritten(layer, compactNode);
        getOnWriteListener().onKeyValueWritten(layer, key, appendBytes);

        if (logger.isTraceEnabled()) {
            logger.trace("appended neighbors of primaryKey={}, oldSize={}, numAppended={}",
                    compactNode.getPrimaryKey(), compactNode.getNeighbors().size(), insertions.size());
        }
        return true;
    }

    @Override
    protected void deleteNodeInternal(@Nonnull final Transaction transaction, final int layer,
                                      @Nonnull final Tuple primaryKey) {
        final byte[] key = getNodeKey(layer, primaryKey);
        transaction.clear(key);
        getOnWriteListener().onNodeDeleted(layer, primaryKey);
        getOnWriteListener().onKeyDeleted(layer, key);
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
     * Scans a given layer for nodes, returning an iterable over the results.
     * <p>
     * This method reads a limited number of nodes from a specific layer in the underlying data store.
     * The scan can be started from a specific point using the {@code lastPrimaryKey} parameter, which is
     * useful for paginating through the nodes in a large layer.
     *
     * @param readTransaction the transaction to use for reading data; must not be {@code null}
     * @param storageTransform an affine vector transformation operator that is used to transform the fetched vectors
     *        into the storage space that is currently being used
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
    public AsyncIterable<AbstractNode<NodeReference>> scanLayer(@Nonnull final ReadTransaction readTransaction,
                                                                @Nonnull final StorageTransform storageTransform,
                                                                int layer,
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
            return nodeFromRaw(storageTransform, layer, primaryKey, key, value);
        });
    }
}
