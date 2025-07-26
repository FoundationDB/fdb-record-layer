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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
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
    public ByNodeStorageAdapter(@Nonnull final HNSW.Config config, @Nonnull final Subspace subspace,
                                @Nonnull final Subspace nodeSlotIndexSubspace,
                                @Nonnull final Function<HNSW.Point, BigInteger> hilbertValueFunction,
                                @Nonnull final OnWriteListener onWriteListener,
                                @Nonnull final OnReadListener onReadListener) {
        super(config, subspace, nodeSlotIndexSubspace, hilbertValueFunction, onWriteListener, onReadListener);
    }

    @Override
    public CompletableFuture<EntryPointAndLayer> fetchEntryNodeKey(@Nonnull final ReadTransaction readTransaction) {
        final byte[] key = getEntryNodeSubspace().pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        return null; // not a single node in the index
                    }
                    final OnReadListener onReadListener = getOnReadListener();
                    onReadListener.onKeyValueRead(key, valueBytes);

                    final Tuple entryTuple = Tuple.fromBytes(valueBytes);
                    final int lMax = (int)entryTuple.getLong(0);
                    final Tuple primaryKey = entryTuple.getNestedTuple(1);
                    final Tuple vectorTuple = entryTuple.getNestedTuple(2);
                    return new EntryPointAndLayer(lMax, primaryKey, vectorFromTuple(vectorTuple));
                });
    }

    @Nonnull
    @Override
    protected <N extends Neighbor> CompletableFuture<NodeWithLayer<N>> fetchNodeInternal(@Nonnull final Node.NodeCreator<N> creator,
                                                                                         @Nonnull final ReadTransaction readTransaction,
                                                                                         final int layer,
                                                                                         @Nonnull final Tuple primaryKey) {
        final byte[] key = getDataSubspace().pack(Tuple.from(layer, primaryKey));

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        throw new IllegalStateException("cannot fetch node");
                    }

                    final Tuple nodeTuple = Tuple.fromBytes(valueBytes);
                    final Node<N> node = nodeFromTuple(creator, nodeTuple);
                    final OnReadListener onReadListener = getOnReadListener();
                    onReadListener.onNodeRead(node);
                    onReadListener.onKeyValueRead(key, valueBytes);
                    return node.withLayer(layer);
                });
    }

    @Override
    public void writeLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final DataNode node,
                                  @Nonnull final ItemSlot itemSlot) {
        persistNode(transaction, node);
    }

    @Override
    public void clearLeafNodeSlot(@Nonnull final Transaction transaction, @Nonnull final DataNode node,
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
        final HNSW.Config config = getConfig();
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
    private <N extends Neighbor> Node<N> nodeFromTuple(@Nonnull final Node.NodeCreator<N> creator,
                                                       @Nonnull final Tuple tuple) {
        final NodeKind nodeKind = NodeKind.fromSerializedNodeKind((byte)tuple.getLong(0));
        final Tuple primaryKey = tuple.getNestedTuple(1);
        final Tuple vectorTuple;
        final Tuple neighborsTuple;

        switch (nodeKind) {
            case DATA:
                vectorTuple = tuple.getNestedTuple(2);
                neighborsTuple = tuple.getNestedTuple(3);
                return dataNodeFromTuples(creator, primaryKey, vectorTuple, neighborsTuple);
            case INTERMEDIATE:
                neighborsTuple = tuple.getNestedTuple(3);
                return intermediateNodeFromTuples(creator, primaryKey, neighborsTuple);
            default:
                throw new IllegalStateException("unknown node kind");
        }
    }

    @Nonnull
    private <N extends Neighbor> Node<N> dataNodeFromTuples(@Nonnull final Node.NodeCreator<N> creator,
                                                            @Nonnull final Tuple primaryKey,
                                                            @Nonnull final Tuple vectorTuple,
                                                            @Nonnull final Tuple neighborsTuple) {
        final Vector<Half> vector = vectorFromTuple(vectorTuple);

        List<Neighbor> neighbors = Lists.newArrayListWithExpectedSize(neighborsTuple.size());

        for (final Object neighborObject : neighborsTuple) {
            final Tuple neighborTuple = (Tuple)neighborObject;
            neighbors.add(new Neighbor(neighborTuple));
        }

        return creator.create(NodeKind.DATA, primaryKey, vector, neighbors);
    }

    @Nonnull
    private <N extends Neighbor> Node<N> intermediateNodeFromTuples(@Nonnull final Node.NodeCreator<N> creator,
                                                                    @Nonnull final Tuple primaryKey,
                                                                    @Nonnull final Tuple neighborsTuple) {
        List<NeighborWithVector> neighborsWithVectors = Lists.newArrayListWithExpectedSize(neighborsTuple.size());
        Half[] neighborVectorHalfs = null;

        for (final Object neighborObject : neighborsTuple) {
            final Tuple neighborTuple = (Tuple)neighborObject;
            final Tuple neighborPrimaryKey = neighborTuple.getNestedTuple(0);
            final Tuple neighborVectorTuple = neighborTuple.getNestedTuple(1);
            if (neighborVectorHalfs == null) {
                neighborVectorHalfs = new Half[neighborVectorTuple.size()];
            }

            for (int i = 0; i < neighborVectorTuple.size(); i ++) {
                neighborVectorHalfs[i] = Half.shortBitsToHalf(shortFromBytes(neighborVectorTuple.getBytes(i)));
            }
            neighborsWithVectors.add(new NeighborWithVector(neighborPrimaryKey, new Vector.HalfVector(neighborVectorHalfs)));
        }

        return creator.create(NodeKind.INTERMEDIATE, primaryKey, null, neighborsWithVectors);
    }

    @Nonnull
    private Vector<Half> vectorFromTuple(final Tuple vectorTuple) {
        final Half[] vectorHalfs = new Half[vectorTuple.size()];
        for (int i = 0; i < vectorTuple.size(); i ++) {
            vectorHalfs[i] = Half.shortBitsToHalf(shortFromBytes(vectorTuple.getBytes(i)));
        }
        return new Vector.HalfVector(vectorHalfs);
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

    private short shortFromBytes(byte[] bytes) {
        Verify.verify(bytes.length == 2);
        int high = bytes[0] & 0xFF;   // Convert to unsigned int
        int low  = bytes[1] & 0xFF;

        return (short) ((high << 8) | low);
    }

    private byte[] bytesFromShort(short value) {
        byte[] result = new byte[2];
        result[0] = (byte) ((value >> 8) & 0xFF);  // high byte first
        result[1] = (byte) (value & 0xFF);         // low byte second
        return result;
    }
}
