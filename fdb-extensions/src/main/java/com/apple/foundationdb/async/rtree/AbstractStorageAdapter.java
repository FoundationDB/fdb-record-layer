/*
 * AbstractStorageAdapter.java
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
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Implementations common to all concrete implementations of {@link StorageAdapter}.
 */
abstract class AbstractStorageAdapter implements StorageAdapter {
    @Nonnull
    public byte[] packWithSubspace(final byte[] key) {
        return getSubspace().pack(key);
    }

    @Nonnull
    @Override
    public CompletableFuture<Node> scanNodeIndexAndFetchNode(@Nonnull final ReadTransaction transaction,
                                                             final int level,
                                                             @Nonnull final BigInteger hilbertValue,
                                                             @Nonnull final Tuple key,
                                                             final boolean isInsertUpdate) {
        final NodeSlotIndexAdapter nodeSlotIndexAdapter = getNodeSlotIndexAdapter();
        Objects.requireNonNull(nodeSlotIndexAdapter);
        return nodeSlotIndexAdapter.scanIndexForNodeId(transaction, level, hilbertValue, key, isInsertUpdate)
                .thenCompose(nodeId -> fetchNode(transaction, nodeId));
    }

    @Override
    public void insertIntoNodeIndexIfNecessary(@Nonnull final Transaction transaction, final int level,
                                               @Nonnull final NodeSlot nodeSlot) {
        if (!getConfig().isUseSlotIndex() || !(nodeSlot instanceof ChildSlot)) {
            return;
        }

        final NodeSlotIndexAdapter nodeSlotIndexAdapter = getNodeSlotIndexAdapter();
        Objects.requireNonNull(nodeSlotIndexAdapter);
        nodeSlotIndexAdapter.writeChildSlot(transaction, level, (ChildSlot)nodeSlot);
    }

    @Override
    public void deleteFromNodeIndexIfNecessary(@Nonnull final Transaction transaction, final int level,
                                               @Nonnull final NodeSlot nodeSlot) {
        if (!getConfig().isUseSlotIndex() || !(nodeSlot instanceof ChildSlot)) {
            return;
        }

        final NodeSlotIndexAdapter nodeSlotIndexAdapter = getNodeSlotIndexAdapter();
        Objects.requireNonNull(nodeSlotIndexAdapter);
        nodeSlotIndexAdapter.clearChildSlot(transaction, level, (ChildSlot)nodeSlot);
    }

    @Override
    public void updateNodeIndexIfNecessary(@Nonnull final Transaction transaction, final int level, @Nonnull final Node node) {
        if (!getConfig().isUseSlotIndex()) {
            return;
        }

        // only intermediate nodes need node slot index updates
        if (!(node instanceof IntermediateNode)) {
            return;
        }

        final NodeSlotIndexAdapter nodeSlotIndexAdapter = getNodeSlotIndexAdapter();
        Objects.requireNonNull(nodeSlotIndexAdapter);

        final IntermediateNode intermediateNode = (IntermediateNode)node;
        for (final ChildSlot childSlot : intermediateNode.getSlots()) {
            if (childSlot.isDirty()) {
                Objects.requireNonNull(childSlot.getOriginalNodeSlot());
                nodeSlotIndexAdapter.clearChildSlot(transaction, level, childSlot.getOriginalNodeSlot());
                nodeSlotIndexAdapter.writeChildSlot(transaction, level, childSlot);
                childSlot.markClean();
            }
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Node> fetchNode(@Nonnull final ReadTransaction transaction, @Nonnull final byte[] nodeId) {
        return getOnWriteListener().onAsyncReadForWrite(fetchNodeInternal(transaction, nodeId).thenApply(this::checkNode));
    }

    protected abstract CompletableFuture<Node> fetchNodeInternal(@Nonnull ReadTransaction transaction, @Nonnull byte[] nodeId);

    /**
     * Method to perform basic invariant check(s) on a newly fetched node.
     *
     * @param node the node to check
     * @param <N> the type param for the node in order for this method to not be lossy on the type of the node that
     * was passed in
     *
     * @return the node that was passed in
     */
    @Nonnull
    private <N extends Node> N checkNode(@Nonnull final N node) {
        if (node.size() < getConfig().getMinM() || node.size() > getConfig().getMaxM()) {
            if (!node.isRoot()) {
                throw new IllegalStateException("packing of non-root is out of valid range");
            }
        }
        return node;
    }
}
