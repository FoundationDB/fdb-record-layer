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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Storage adapter that represents each node as a single key/value pair in the database. That paradigm
 * greatly simplifies how nodes, node slots, and operations on these data structures are managed. Also, a
 * node that is serialized into a key/value pair using this storage adapter leaves a significantly smaller
 * footprint when compared to the multitude of key/value pairs that are serialized/deserialized using
 * {@link BySlotStorageAdapter}.
 * <br>
 * These advantages are offset by the key disadvantage of having to read/deserialize and the serialize/write
 * the entire node to realize/persist a minute change to one of its slots. That, in turn, may cause a higher
 * likelihood of conflicting with another transaction.
 * <br>
 * Each node is serialized as follows (each {@code (thing)} is a tuple containing {@code thing}):
 * <pre>
 * {@code
 *    key: nodeId: byte[16]
 *    value: Tuple(nodeKind: Long, slotList: (slot1, ..., slotn])
 *           slot:
 *               for leaf nodes:
 *                   (hilbertValue: BigInteger, itemKey: (point: Tuple(d1, ..., dk),
 *                    keySuffix: (...)),
 *                    value: (...))
 *               for intermediate nodes:
 *                   (smallestHV: BigInteger, smallestKey: (...),
 *                    largestHV: BigInteger, largestKey: (...),
 *                    childId: byte[16],
 *                    mbr: (minD1, minD2, ..., minDk, maxD1, maxD2, ..., maxDk)))
 * }
 * </pre>
 */
class ByNodeStorageAdapter extends AbstractStorageAdapter implements StorageAdapter {
    public ByNodeStorageAdapter(@Nonnull final RTree.Config config, @Nonnull final Subspace subspace,
                                @Nonnull final Subspace nodeSlotIndexSubspace,
                                @Nonnull final Function<RTree.Point, BigInteger> hilbertValueFunction,
                                @Nonnull final OnWriteListener onWriteListener,
                                @Nonnull final OnReadListener onReadListener) {
        super(config, subspace, nodeSlotIndexSubspace, hilbertValueFunction, onWriteListener, onReadListener);
    }

    @Override
    public void writeLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final LeafNode node,
                                  @Nonnull final ItemSlot itemSlot) {
        persistNode(transaction, node);
    }

    @Override
    public void clearLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final LeafNode node,
                                  @Nonnull final ItemSlot itemSlot) {
        persistNode(transaction, node);
    }

    private void persistNode(@Nonnull final Transaction transaction, @Nonnull final Node node) {
        final byte[] packedKey = packWithSubspace(node.getId());

        if (node.isEmpty()) {
            // this can only happen when we just deleted the last slot; delete the entire node
            transaction.clear(packedKey);
            getOnWriteListener().onKeyCleared(node, packedKey);
        } else {
            // updateNodeIndexIfNecessary(transaction, level, node);
            final byte[] packedValue = toTuple(node).pack();
            transaction.set(packedKey, packedValue);
            getOnWriteListener().onKeyValueWritten(node, packedKey, packedValue);
        }
    }

    @Nonnull
    private Tuple toTuple(@Nonnull final Node node) {
        final RTree.Config config = getConfig();
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
                    if (valueBytes == null) {
                        return null;
                    }
                    final Node node = fromTuple(nodeId, Tuple.fromBytes(valueBytes));
                    final OnReadListener onReadListener = getOnReadListener();
                    onReadListener.onNodeRead(node);
                    onReadListener.onKeyValueRead(node, key, valueBytes);
                    return node;
                });
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Node fromTuple(@Nonnull final byte[] nodeId, @Nonnull final Tuple tuple) {
        final NodeKind nodeKind = NodeKind.fromSerializedNodeKind((byte)tuple.getLong(0));
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
                    itemSlots.add(ItemSlot.fromKeyAndValue(itemSlotKeyTuple, itemSlotValueTuple, getHilbertValueFunction()));
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

        Verify.verify((nodeKind == NodeKind.LEAF && itemSlots != null) ||
                      (nodeKind == NodeKind.INTERMEDIATE && childSlots != null));

        return nodeKind == NodeKind.LEAF
               ? new LeafNode(nodeId, itemSlots)
               : new IntermediateNode(nodeId, childSlots);
    }

    @Nonnull
    @Override
    public <S extends NodeSlot, N extends AbstractNode<S, N>> AbstractChangeSet<S, N>
            newInsertChangeSet(@Nonnull final N node, final int level, @Nonnull final List<S> insertedSlots) {
        return new InsertChangeSet<>(node, level, insertedSlots);
    }

    @Nonnull
    @Override
    public <S extends NodeSlot, N extends AbstractNode<S, N>> AbstractChangeSet<S, N>
            newUpdateChangeSet(@Nonnull final N node, final int level,
                               @Nonnull final S originalSlot, @Nonnull final S updatedSlot) {
        return new UpdateChangeSet<>(node, level, originalSlot, updatedSlot);
    }

    @Nonnull
    @Override
    public <S extends NodeSlot, N extends AbstractNode<S, N>> AbstractChangeSet<S, N>
            newDeleteChangeSet(@Nonnull final N node, final int level, @Nonnull final List<S> deletedSlots) {
        return new DeleteChangeSet<>(node, level, deletedSlots);
    }

    private class InsertChangeSet<S extends NodeSlot, N extends AbstractNode<S, N>> extends AbstractChangeSet<S, N> {
        @Nonnull
        private final List<S> insertedSlots;

        public InsertChangeSet(@Nonnull final N node, final int level, @Nonnull final List<S> insertedSlots) {
            super(node.getChangeSet(), node, level);
            this.insertedSlots = ImmutableList.copyOf(insertedSlots);
        }

        @Override
        public void apply(@Nonnull final Transaction transaction) {
            super.apply(transaction);

            //
            // If this change set is the first, we persist the node, don't persist the node otherwise. This is a
            // performance optimization to avoid writing and rewriting the node for each change set in the chain
            // of change sets.
            //
            if (getPreviousChangeSet() == null) {
                persistNode(transaction, getNode());
            }
            if (isUpdateNodeSlotIndex()) {
                for (final S insertedSlot : insertedSlots) {
                    insertIntoNodeIndexIfNecessary(transaction, getLevel(), insertedSlot);
                }
            }
        }
    }

    private class UpdateChangeSet<S extends NodeSlot, N extends AbstractNode<S, N>> extends AbstractChangeSet<S, N> {
        @Nonnull
        private final S originalSlot;
        @Nonnull
        private final S updatedSlot;

        public UpdateChangeSet(@Nonnull final N node, final int level, @Nonnull final S originalSlot,
                               @Nonnull final S updatedSlot) {
            super(node.getChangeSet(), node, level);
            this.originalSlot = originalSlot;
            this.updatedSlot = updatedSlot;
        }

        @Override
        public void apply(@Nonnull final Transaction transaction) {
            super.apply(transaction);

            //
            // If this change set is the first, we persist the node, don't persist the node otherwise. This is a
            // performance optimization to avoid writing and rewriting the node for each change set in the chain
            // of change sets.
            //
            if (getPreviousChangeSet() == null) {
                persistNode(transaction, getNode());
            }
            if (isUpdateNodeSlotIndex()) {
                deleteFromNodeIndexIfNecessary(transaction, getLevel(), originalSlot);
                insertIntoNodeIndexIfNecessary(transaction, getLevel(), updatedSlot);
            }
        }
    }

    private class DeleteChangeSet<S extends NodeSlot, N extends AbstractNode<S, N>> extends AbstractChangeSet<S, N> {
        @Nonnull
        private final List<S> deletedSlots;

        public DeleteChangeSet(@Nonnull final N node, final int level, @Nonnull final List<S> deletedSlots) {
            super(node.getChangeSet(), node, level);
            this.deletedSlots = ImmutableList.copyOf(deletedSlots);
        }

        @Override
        public void apply(@Nonnull final Transaction transaction) {
            super.apply(transaction);

            //
            // If this change set is the first, we persist the node, don't persist the node otherwise. This is a
            // performance optimization to avoid writing and rewriting the node for each change set in the chain
            // of change sets.
            //
            if (getPreviousChangeSet() == null) {
                persistNode(transaction, getNode());
            }
            if (isUpdateNodeSlotIndex()) {
                for (final S deletedSlot : deletedSlots) {
                    deleteFromNodeIndexIfNecessary(transaction, getLevel(), deletedSlot);
                }
            }
        }
    }
}
