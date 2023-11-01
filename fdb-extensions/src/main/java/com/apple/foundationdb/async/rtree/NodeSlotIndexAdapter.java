/*
 * NodeSlotIndexAdapter.java
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
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Logic to encapsulate interactions with the (optional) node slot index.
 * <br>
 * In a Hilbert R-tree, Hilbert values are used to cluster the R-tree and also to define an order among points.
 * As an additional emerging property, a total order among nodes can also be defined using Hilbert Values as long as
 * these nodes are on the same <em>level</em> in the R-tree. The level is defined as the distance from the leaf node
 * level to the node in question. Note that we could also define the level as the distance from the root node as the
 * R-tree is always balanced; however, it turns out to be advantageous to start with {@code 0} for the leaf level and
 * increase walking upwards the R-tree.
 * <br>
 * All nodes on the same level can be ordered according to the Hilbert Values (and the key) the node covers.
 * We use the {@code (largest Hilbert Value, largest key)}; the same approach would also work for
 * {@code (smallest Hilbert Value, smallest key)} as this property is symmetrical. The corresponding
 * {@code (largest Hilbert Value, largest key)} pair for a node is already stored in the corresponding child slot of
 * a node's parent node.
 * <br>
 * The node slot index is a persisted index defined as {@code (level, largestHilbertValue, largestKey, nodeId) -> empty}
 * for every node that is not the root node. The root node is not indexed as the root node does not have a
 * {@code (largest Hilbert Value, largest key)} pair (it could be defined as {@code (MaxHilbertValue, MaxKey)} but
 * it's not useful to store that).
 * <br>
 * Once an R-tree grows beyond a certain size, the number of nodes that need to be fetched for an insert/update/delete
 * (also called the update path in {@link RTree}) which is bound by {@code O(logM(numRecords)} still can be
 * substantially higher than the number of nodes that are actually needed to be consulted/written for that
 * insert/update/delete operation. Using the node slot index, the insert/update/delete logic is able to directly
 * (and only) fetch the affected leaf node. Only if further adjustments of the R-tree are needed and propagated upwards
 * the tree, we can again consult the node slot index and fetch the affected intermediate parent node and so on.
 * In this way, only nodes that truly need to be read/updated are fetched from the database leading to a smaller commit
 * size.
 * <br>
 * For each lookup of the node we need to modify we normally perform exactly one scan of the node slot index to obtain
 * the proper node id followed by one get/scan (depending on {@link StorageAdapter}) of the primary space
 * using that node id to fetch the actual node. We cannot assume that the database can do this lookup in constant
 * time, however, at least we can reason about the number of round trips for an insert/update/delete operation.
 * The number of round trips is reduced from {@code O(logM(numNodes))} to {@code O(1)}. In reality, using the index
 * will incur at least three round trips and in general two round trips per level in the R-tree. Depending on the
 * used {@link com.apple.foundationdb.async.rtree.RTree.Config}, that number may well be higher than the
 * {@code logM(numNodes)} round trips needed if the node slot index is not used. Together with the addition mutations
 * needed to maintain the node slot index it may not be useful to use this index at all if the anticipated number of
 * records stored in the R-tree is not big enough. For the {@link RTree#DEFAULT_CONFIG}, the break even point is
 * in the 100k records range, for an R-tree whose {@code M} is smaller, the break even point naturally is also smaller.
 */
class NodeSlotIndexAdapter {

    private static final byte[] emptyArray = { };

    @Nonnull
    private final Subspace nodeSlotIndexSubspace;

    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    NodeSlotIndexAdapter(@Nonnull final Subspace nodeSlotIndexSubspace, @Nonnull final OnWriteListener onWriteListener,
                         @Nonnull final OnReadListener onReadListener) {
        this.nodeSlotIndexSubspace = nodeSlotIndexSubspace;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
    }

    @Nonnull
    public Subspace getNodeSlotIndexSubspace() {
        return nodeSlotIndexSubspace;
    }

    @Nonnull
    CompletableFuture<byte[]> scanIndexForNodeId(@Nonnull final ReadTransaction transaction,
                                                 final int level,
                                                 @Nonnull final BigInteger hilbertValue,
                                                 @Nonnull final Tuple key,
                                                 final boolean isInsertUpdate) {
        final List<Object> keys = Lists.newArrayList();
        keys.add(level);
        keys.add(hilbertValue);
        keys.addAll(key.getItems());
        final byte[] packedKey = nodeSlotIndexSubspace.pack(Tuple.fromList(keys));
        return AsyncUtil.collect(transaction.getRange(new Range(packedKey,
                        nodeSlotIndexSubspace.pack(Tuple.from(level + 1))), 1, false, StreamingMode.WANT_ALL))
                .thenCompose(keyValues -> {
                    Verify.verify(keyValues.size() <= 1);
                    if (!keyValues.isEmpty()) {
                        final KeyValue keyValue = keyValues.get(0);
                        onReadListener.onSlotIndexEntryRead(keyValue.getKey());
                        final Tuple indexKeyTuple = Tuple.fromBytes(keyValue.getKey());
                        return CompletableFuture.completedFuture(getNodeIdFromIndexKeyTuple(indexKeyTuple));
                    }

                    //
                    // If we are on level > 0, this means that we already have fetched a node on a lower level.
                    // If we are on insert/update, we may not find a covering node on the next level as it is not
                    // covering YET (we are in the process of fixing that). If we are, however, on the delete code path,
                    // there should always be such a node that we can find in the index. The only legitimate reason
                    // we may not find that index is that we have reached the level below the root node as the root node
                    // itself is not indexed.
                    //
                    if (!isInsertUpdate && level > 0) {
                        return CompletableFuture.completedFuture(RTree.rootId);
                    }

                    //
                    // If on the insert/update path OR on delete path with level == 0, try to fetch the previous node.
                    //
                    return AsyncUtil.collect(transaction.getRange(new Range(nodeSlotIndexSubspace.pack(Tuple.from(level)),
                                    packedKey), 1, true, StreamingMode.WANT_ALL))
                            .thenApply(previousKeyValues -> {
                                Verify.verify(previousKeyValues.size() <= 1);

                                //
                                // If there is no previous node on the requested level, return the root node, as the
                                // root node itself is not part of the index. If we are on level == 0, this means that
                                // there is only a single root node in the R-tree, or that this R-tree is completely
                                // empty. (The subsequent fetch will tell).
                                //
                                if (previousKeyValues.isEmpty()) {
                                    return RTree.rootId;
                                }

                                final KeyValue previousKeyValue = previousKeyValues.get(0);
                                onReadListener.onSlotIndexEntryRead(previousKeyValue.getKey());

                                //
                                // For a delete (level == 0) implied, we know that the largest node on this level, is
                                // smaller than what we are looking for. That means that the key we are looking for is
                                // not in the R-tree.
                                //
                                if (!isInsertUpdate) {
                                    return null;
                                }

                                //
                                // Return the node we found for insert/update operation.
                                //
                                final Tuple indexKeyTuple = Tuple.fromBytes(previousKeyValue.getKey());
                                return getNodeIdFromIndexKeyTuple(indexKeyTuple);
                            });
                });
    }

    void writeChildSlot(@Nonnull final Transaction transaction, final int level,
                        @Nonnull final ChildSlot childSlot) {
        final Tuple indexKeyTuple = createIndexKeyTuple(level, childSlot);
        final byte[] packedKey = nodeSlotIndexSubspace.pack(indexKeyTuple);
        transaction.set(packedKey, emptyArray);
        onWriteListener.onSlotIndexEntryWritten(packedKey);
    }

    void clearChildSlot(@Nonnull final Transaction transaction, final int level, @Nonnull final ChildSlot childSlot) {
        final Tuple indexKeyTuple = createIndexKeyTuple(level, childSlot);
        final byte[] packedKey = nodeSlotIndexSubspace.pack(indexKeyTuple);
        transaction.clear(packedKey);
        onWriteListener.onSlotIndexEntryCleared(packedKey);
    }

    @Nonnull
    private Tuple createIndexKeyTuple(final int level, @Nonnull final ChildSlot childSlot) {
        return createIndexKeyTuple(level, childSlot.getLargestHilbertValue(), childSlot.getLargestKey(), childSlot.getChildId());
    }

    @Nonnull
    private Tuple createIndexKeyTuple(final int level, @Nonnull final BigInteger largestHilbertValue,
                                      @Nonnull final Tuple largestKey, @Nonnull final byte[] nodeId) {
        final List<Object> keys = Lists.newArrayList();
        keys.add(level);
        keys.add(largestHilbertValue);
        keys.addAll(largestKey.getItems());
        keys.add(nodeId);
        return Tuple.fromList(keys);
    }

    @Nonnull
    private byte[] getNodeIdFromIndexKeyTuple(final Tuple tuple) {
        return tuple.getBytes(tuple.size() - 1);
    }
}
