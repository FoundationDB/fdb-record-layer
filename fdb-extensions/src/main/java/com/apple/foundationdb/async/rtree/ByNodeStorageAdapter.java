/*
 * ByNodeStorageAdapter.java
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

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
class ByNodeStorageAdapter extends AbstractStorageAdapter implements StorageAdapter {
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

    public ByNodeStorageAdapter(@Nonnull final RTree.Config config, @Nonnull final Subspace subspace,
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
        writeNode(transaction, node);
    }

    @Override
    public void clearLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final LeafNode node, @Nonnull final ItemSlot itemSlot) {
        writeNode(transaction, node);
    }

    @Override
    public void writeNodes(@Nonnull final Transaction transaction, @Nonnull final List<? extends Node> nodes) {
        for (final Node node : nodes) {
            writeNode(transaction, node);
        }
    }

    private void writeNode(@Nonnull final Transaction transaction, @Nonnull final Node node) {
        final byte[] packedKey = packWithSubspace(node.getId());

        if (node.isEmpty()) {
            // this can only happen when we just deleted the last slot; delete the entire node
            transaction.clear(packedKey);
        } else {
            // updateNodeIndexIfNecessary(transaction, level, node);
            final byte[] packedValue = toTuple(node).pack();
            transaction.set(packedKey, packedValue);
        }
        onWriteListener.onNodeWritten(node);
    }

    @Nonnull
    private Tuple toTuple(@Nonnull final Node node) {
        final List<Tuple> slotTuples = Lists.newArrayListWithExpectedSize(node.size());
        for (final NodeSlot nodeSlot : node.getSlots()) {
            final Tuple slotTuple = Tuple.fromStream(
                    Streams.concat(nodeSlot.getSlotKey(config.isStoreHilbertValues()).getItems().stream(),
                            nodeSlot.getSlotValue().getItems().stream()));
            slotTuples.add(slotTuple);
        }
        return Tuple.from(node.getKind().getSerialized(), slotTuples);
    }

    @Nonnull
    @Override
    public CompletableFuture<Node> fetchNodeInternal(@Nonnull final ReadTransaction transaction,
                                                     @Nonnull final byte[] nodeId) {
        final byte[] key = packWithSubspace(nodeId);
        return transaction.get(key)
                .thenApply(valueBytes -> {
                    final Node node = fromTuple(nodeId, valueBytes == null ? null : Tuple.fromBytes(valueBytes));
                    onReadListener.onNodeRead(node);
                    onReadListener.onKeyValueRead(node, key, valueBytes);
                    return node;
                });
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Node fromTuple(@Nonnull final byte[] nodeId, @Nullable final Tuple tuple) {
        if (tuple == null) {
            if (Arrays.equals(RTree.rootId, nodeId)) {
                return new LeafNode(nodeId, Lists.newArrayList());
            }
            throw new IllegalStateException("unable to find node for given node id");
        }

        final Node.Kind nodeKind = Node.Kind.fromSerializedNodeKind((byte)tuple.getLong(0));
        final List<Object> nodeSlotObjects = tuple.getNestedList(1);

        List<ItemSlot> itemSlots = null;
        List<ChildSlot> childSlots = null;

        for (final Object nodeSlotObject : nodeSlotObjects) {
            final List<Object> nodeSlotItems = (List<Object>)nodeSlotObject;

            switch (nodeKind) {
                case LEAF:
                    final Tuple itemSlotKeyTuple = Tuple.fromList(nodeSlotItems.subList(0, ItemSlot.SLOT_KEY_TUPLE_SIZE));
                    final Tuple itemSlotValueTuple = Tuple.fromList(nodeSlotItems.subList(ItemSlot.SLOT_KEY_TUPLE_SIZE, nodeSlotItems.size()));

                    if (itemSlots == null) {
                        itemSlots = Lists.newArrayListWithExpectedSize(nodeSlotObjects.size());
                    }
                    itemSlots.add(ItemSlot.fromKeyAndValue(itemSlotKeyTuple, itemSlotValueTuple, hilbertValueFunction));
                    break;

                case INTERMEDIATE:
                    final Tuple childSlotKeyTuple = Tuple.fromList(nodeSlotItems.subList(0, ChildSlot.SLOT_KEY_TUPLE_SIZE));
                    final Tuple childSlotValueTuple = Tuple.fromList(nodeSlotItems.subList(ChildSlot.SLOT_KEY_TUPLE_SIZE, nodeSlotItems.size()));

                    if (childSlots == null) {
                        childSlots = Lists.newArrayListWithExpectedSize(nodeSlotObjects.size());
                    }
                    childSlots.add(ChildSlot.fromKeyAndValue(childSlotKeyTuple, childSlotValueTuple));
                    break;
                default:
                    throw new IllegalStateException("unknown node kind");
            }
        }

        Verify.verify((nodeKind == Node.Kind.LEAF && itemSlots != null) ||
                      (nodeKind == Node.Kind.INTERMEDIATE && childSlots != null));

        return nodeKind == Node.Kind.LEAF
               ? new LeafNode(nodeId, itemSlots)
               : new IntermediateNode(nodeId, childSlots);
    }
}
