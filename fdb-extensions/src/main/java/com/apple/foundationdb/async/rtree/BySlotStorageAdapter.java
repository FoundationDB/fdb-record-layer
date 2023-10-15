/*
 * BySlotStorageAdapter.java
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Storage adapter that normalizes internal nodes such that each node slot is a key/value pair in the database.
 */
class BySlotStorageAdapter extends AbstractStorageAdapter implements StorageAdapter {
    @Nonnull
    private final RTree.Config config;
    @Nonnull
    private final Subspace subspace;
    @Nullable
    private final NodeSlotIndexAdapter nodeSlotIndexAdapter;
    @Nonnull
    private final Function<RTree.Point, BigInteger> hilbertValueFunction;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    public BySlotStorageAdapter(@Nonnull final RTree.Config config, @Nonnull final Subspace subspace,
                                @Nonnull final Subspace secondarySubspace,
                                @Nonnull final Function<RTree.Point, BigInteger> hilbertValueFunction,
                                @Nonnull final OnWriteListener onWriteListener,
                                @Nonnull final OnReadListener onReadListener) {
        this.config = config;
        this.subspace = subspace;
        this.nodeSlotIndexAdapter = config.isUseSlotIndex() ? new NodeSlotIndexAdapter(secondarySubspace) : null;
        this.hilbertValueFunction = hilbertValueFunction;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
    }

    @Override
    @Nonnull
    public RTree.Config getConfig() {
        return config;
    }

    @Override
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    @Override
    @Nullable
    public NodeSlotIndexAdapter getNodeSlotIndexAdapter() {
        return nodeSlotIndexAdapter;
    }

    @Nonnull
    @Override
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    @Nonnull
    @Override
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

    @Override
    public void writeLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final LeafNode node, @Nonnull final ItemSlot itemSlot) {
        writeNodeSlot(transaction, node, itemSlot);
    }

    private void writeNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
        Tuple keyTuple = Tuple.from(node.getKind().getSerialized());
        keyTuple = keyTuple.addAll(nodeSlot.getSlotKey(config.isStoreHilbertValues()));
        final byte[] packedKey = keyTuple.pack(packWithSubspace(node.getId()));
        final byte[] packedValue = nodeSlot.getSlotValue().pack();
        transaction.set(packedKey, packedValue);
        onWriteListener.onKeyValueWritten(node, packedKey, packedValue);
    }

    @Override
    public void clearLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final LeafNode node, @Nonnull final ItemSlot itemSlot) {
        clearNodeSlot(transaction, node, itemSlot);
    }

    private void clearNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
        Tuple keyTuple = Tuple.from(node.getKind().getSerialized());
        keyTuple = keyTuple.addAll(nodeSlot.getSlotKey(config.isStoreHilbertValues()));
        final byte[] packedKey = keyTuple.pack(packWithSubspace(node.getId()));
        transaction.clear(packedKey);
        onWriteListener.onNodeCleared(node);
    }

    @Override
    public void writeNodes(@Nonnull final Transaction transaction, @Nonnull final List<? extends Node> nodes) {
        for (final Node node : nodes) {
            writeNode(transaction, node);
            onWriteListener.onNodeWritten(node);
        }
    }

    private void writeNode(@Nonnull final Transaction transaction, @Nonnull final Node node) {
        for (final NodeSlot deletedSlot : node.getDeletedSlots()) {
            clearNodeSlot(transaction, node, deletedSlot);
        }

        for (final NodeSlot insertedSlot : node.getInsertedSlots()) {
            writeNodeSlot(transaction, node, insertedSlot);
        }

        for (final NodeSlot nodeSlot : node.getSlots()) {
            if (nodeSlot.isDirty()) {
                clearNodeSlot(transaction, node, nodeSlot.getOriginalNodeSlot());
                writeNodeSlot(transaction, node, nodeSlot);
            }
        }

        final Node rereadNode = fetchNode(transaction, node.getId()).join();
        Verify.verify(rereadNode.size() == node.size());
        onWriteListener.onNodeWritten(node);
    }

    @Nonnull
    @Override
    public CompletableFuture<Node> fetchNodeInternal(@Nonnull final ReadTransaction transaction,
                                                     @Nonnull final byte[] nodeId) {
        return AsyncUtil.collect(transaction.getRange(Range.startsWith(packWithSubspace(nodeId)),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL))
                .thenApply(keyValues -> {
                    final Node node = fromKeyValues(nodeId, keyValues);
                    onReadListener.onNodeRead(node);
                    keyValues.forEach(keyValue -> onReadListener.onKeyValueRead(node, keyValue.getKey(), keyValue.getValue()));
                    return node;
                });
    }

    /**
     * Interpret the returned key/value pairs from a fetch of slots from the database and reconstruct an in-memory
     * {@link Node}.
     *
     * @param nodeId node id
     * @param keyValues a list of key values as returned by the call to {@link Transaction#getRange(Range)}.
     *
     * @return an instance of a subclass of {@link Node} specific to the node kind as read from the database.
     */
    @SuppressWarnings("ConstantValue")
    @Nonnull
    private Node fromKeyValues(@Nonnull final byte[] nodeId, final List<KeyValue> keyValues) {
        List<ItemSlot> itemSlots = null;
        List<ChildSlot> childSlots = null;
        Node.Kind nodeKind = null;

        // one key/value pair corresponds to one slot
        for (final KeyValue keyValue : keyValues) {
            final Tuple keyTuple = getSubspace().unpack(keyValue.getKey()).popFront();
            final Tuple valueTuple = Tuple.fromBytes(keyValue.getValue());

            final Node.Kind currentNodeKind = Node.Kind.fromSerializedNodeKind((byte)keyTuple.getLong(0));
            if (nodeKind == null) {
                nodeKind = currentNodeKind;
            } else if (nodeKind != currentNodeKind) {
                throw new IllegalArgumentException("same node id uses different node kinds");
            }

            final Tuple slotKeyTuple = keyTuple.popFront();
            if (nodeKind == Node.Kind.LEAF) {
                if (itemSlots == null) {
                    itemSlots = Lists.newArrayList();
                }
                itemSlots.add(ItemSlot.fromKeyAndValue(slotKeyTuple, valueTuple, hilbertValueFunction));
            } else {
                Verify.verify(nodeKind == Node.Kind.INTERMEDIATE);
                if (childSlots == null) {
                    childSlots = Lists.newArrayList();
                }
                childSlots.add(ChildSlot.fromKeyAndValue(slotKeyTuple, valueTuple));
            }
        }

        if (nodeKind == null && Arrays.equals(RTree.rootId, nodeId)) {
            // root node but nothing read -- root node is the only node that can be empty --
            // this only happens when the R-Tree is completely empty.
            nodeKind = Node.Kind.LEAF;
            itemSlots = Lists.newArrayList();
        }

        Verify.verify((nodeKind == Node.Kind.LEAF && itemSlots != null && childSlots == null) ||
                      (nodeKind == Node.Kind.INTERMEDIATE && itemSlots == null && childSlots != null));

        if (nodeKind == Node.Kind.LEAF &&
                !config.isStoreHilbertValues()) {
            //
            // We need to sort the slots by the computed Hilbert value/key. This is not necessary when we store
            // the Hilbert value as fdb does the sorting for us.
            //
            itemSlots.sort(ItemSlot.comparator);
        }

        return nodeKind == Node.Kind.LEAF
               ? new LeafNode(nodeId, itemSlots)
               : new IntermediateNode(nodeId, childSlots);
    }
}
