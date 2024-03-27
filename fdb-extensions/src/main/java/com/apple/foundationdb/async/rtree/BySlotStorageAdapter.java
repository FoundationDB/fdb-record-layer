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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Storage adapter that normalizes internal nodes such that each node slot is a key/value pair in the database.
 */
class BySlotStorageAdapter extends AbstractStorageAdapter implements StorageAdapter {
    public BySlotStorageAdapter(@Nonnull final RTree.Config config, @Nonnull final Subspace subspace,
                                @Nonnull final Subspace nodeSlotIndexSubspace,
                                @Nonnull final Function<RTree.Point, BigInteger> hilbertValueFunction,
                                @Nonnull final OnWriteListener onWriteListener,
                                @Nonnull final OnReadListener onReadListener) {
        super(config, subspace, nodeSlotIndexSubspace, hilbertValueFunction, onWriteListener, onReadListener);
    }

    @Override
    public void writeLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final LeafNode node, @Nonnull final ItemSlot itemSlot) {
        writeNodeSlot(transaction, node, itemSlot);
    }

    private void writeNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
        Tuple keyTuple = Tuple.from(node.getKind().getSerialized());
        keyTuple = keyTuple.addAll(nodeSlot.getSlotKey(getConfig().isStoreHilbertValues()));
        final byte[] packedKey = keyTuple.pack(packWithSubspace(node.getId()));
        final byte[] packedValue = nodeSlot.getSlotValue().pack();
        transaction.set(packedKey, packedValue);
        getOnWriteListener().onKeyValueWritten(node, packedKey, packedValue);
    }

    @Override
    public void clearLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final LeafNode node, @Nonnull final ItemSlot itemSlot) {
        clearNodeSlot(transaction, node, itemSlot);
    }

    private void clearNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
        Tuple keyTuple = Tuple.from(node.getKind().getSerialized());
        keyTuple = keyTuple.addAll(nodeSlot.getSlotKey(getConfig().isStoreHilbertValues()));
        final byte[] packedKey = keyTuple.pack(packWithSubspace(node.getId()));
        transaction.clear(packedKey);
        getOnWriteListener().onKeyCleared(node, packedKey);
    }

    @Nonnull
    @Override
    public CompletableFuture<Node> fetchNodeInternal(@Nonnull final ReadTransaction transaction,
                                                     @Nonnull final byte[] nodeId) {
        return AsyncUtil.collect(transaction.getRange(Range.startsWith(packWithSubspace(nodeId)),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL), transaction.getExecutor())
                .thenApply(keyValues -> {
                    if (keyValues.isEmpty()) {
                        return null;
                    }
                    final Node node = fromKeyValues(nodeId, keyValues);
                    final OnReadListener onReadListener = getOnReadListener();
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
        NodeKind nodeKind = null;

        Verify.verify(!keyValues.isEmpty());

        // one key/value pair corresponds to one slot
        for (final KeyValue keyValue : keyValues) {
            final Tuple keyTuple = getSubspace().unpack(keyValue.getKey()).popFront();
            final Tuple valueTuple = Tuple.fromBytes(keyValue.getValue());

            final NodeKind currentNodeKind = NodeKind.fromSerializedNodeKind((byte)keyTuple.getLong(0));
            if (nodeKind == null) {
                nodeKind = currentNodeKind;
            } else if (nodeKind != currentNodeKind) {
                throw new IllegalArgumentException("same node id uses different node kinds");
            }

            final Tuple slotKeyTuple = keyTuple.popFront();
            if (nodeKind == NodeKind.LEAF) {
                if (itemSlots == null) {
                    itemSlots = Lists.newArrayList();
                }
                itemSlots.add(ItemSlot.fromKeyAndValue(slotKeyTuple, valueTuple, getHilbertValueFunction()));
            } else {
                Verify.verify(nodeKind == NodeKind.INTERMEDIATE);
                if (childSlots == null) {
                    childSlots = Lists.newArrayList();
                }
                childSlots.add(ChildSlot.fromKeyAndValue(slotKeyTuple, valueTuple));
            }
        }

        Verify.verify((nodeKind == NodeKind.LEAF && itemSlots != null && childSlots == null) ||
                      (nodeKind == NodeKind.INTERMEDIATE && itemSlots == null && childSlots != null));

        if (nodeKind == NodeKind.LEAF &&
                !getConfig().isStoreHilbertValues()) {
            //
            // We need to sort the slots by the computed Hilbert value/key. This is not necessary when we store
            // the Hilbert value as fdb does the sorting for us.
            //
            itemSlots.sort(ItemSlot.comparator);
        }

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
            newUpdateChangeSet(@Nonnull final N node, final int level, @Nonnull final S originalSlot, @Nonnull final S updatedSlot) {
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
            for (final S insertedSlot : insertedSlots) {
                writeNodeSlot(transaction, getNode(), insertedSlot);
                if (isUpdateNodeSlotIndex()) {
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
            clearNodeSlot(transaction, getNode(), originalSlot);
            writeNodeSlot(transaction, getNode(), updatedSlot);
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
            for (final S deletedSlot : deletedSlots) {
                clearNodeSlot(transaction, getNode(), deletedSlot);
                if (isUpdateNodeSlotIndex()) {
                    deleteFromNodeIndexIfNecessary(transaction, getLevel(), deletedSlot);
                }
            }
        }
    }
}
