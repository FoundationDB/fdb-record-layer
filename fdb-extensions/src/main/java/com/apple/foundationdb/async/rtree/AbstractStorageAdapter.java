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
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import org.jspecify.annotations.Nullable;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Implementations and attributes common to all concrete implementations of {@link StorageAdapter}.
 */
abstract class AbstractStorageAdapter implements StorageAdapter {
    private final RTree.Config config;
    private final Subspace subspace;
    @Nullable
    private final NodeSlotIndexAdapter nodeSlotIndexAdapter;
    private final Function<RTree.Point, BigInteger> hilbertValueFunction;
    private final OnWriteListener onWriteListener;
    private final OnReadListener onReadListener;

    protected AbstractStorageAdapter(final RTree.Config config, final Subspace subspace,
                                     final Subspace nodeSlotIndexSubspace,
                                     final Function<RTree.Point, BigInteger> hilbertValueFunction,
                                     final OnWriteListener onWriteListener,
                                     final OnReadListener onReadListener) {
        this.config = config;
        this.subspace = subspace;
        this.nodeSlotIndexAdapter = config.isUseNodeSlotIndex()
                                    ? new NodeSlotIndexAdapter(nodeSlotIndexSubspace, onWriteListener, onReadListener)
                                    : null;
        this.hilbertValueFunction = hilbertValueFunction;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
    }

    @Override
    public RTree.Config getConfig() {
        return config;
    }

    @Override
    public Subspace getSubspace() {
        return subspace;
    }

    @Nullable
    @Override
    public Subspace getSecondarySubspace() {
        return nodeSlotIndexAdapter == null ? null : nodeSlotIndexAdapter.getNodeSlotIndexSubspace();
    }

    protected Function<RTree.Point, BigInteger> getHilbertValueFunction() {
        return hilbertValueFunction;
    }

    @Override
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    @Override
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

    @Override
    public void writeNodes(final Transaction transaction, final List<? extends Node> nodes) {
        for (final Node node : nodes) {
            writeNode(transaction, node);
        }
    }

    protected void writeNode(final Transaction transaction, final Node node) {
        final Node.ChangeSet changeSet = node.getChangeSet();
        if (changeSet == null) {
            return;
        }

        changeSet.apply(transaction);
        getOnWriteListener().onNodeWritten(node);
    }

    public byte[] packWithSubspace(final byte[] key) {
        return getSubspace().pack(key);
    }

    @Override
    public CompletableFuture<Node> scanNodeIndexAndFetchNode(final ReadTransaction transaction,
                                                             final int level,
                                                             final BigInteger hilbertValue,
                                                             final Tuple key,
                                                             final boolean isInsertUpdate) {
        Objects.requireNonNull(nodeSlotIndexAdapter);
        return nodeSlotIndexAdapter.scanIndexForNodeId(transaction, level, hilbertValue, key, isInsertUpdate)
                .thenCompose(nodeId -> nodeId == null
                                       ? CompletableFuture.completedFuture(null)
                                       : fetchNode(transaction, nodeId));
    }

    @Override
    public void insertIntoNodeIndexIfNecessary(final Transaction transaction, final int level,
                                               final NodeSlot nodeSlot) {
        if (!getConfig().isUseNodeSlotIndex() || !(nodeSlot instanceof ChildSlot)) {
            return;
        }

        Objects.requireNonNull(nodeSlotIndexAdapter);
        nodeSlotIndexAdapter.writeChildSlot(transaction, level, (ChildSlot)nodeSlot);
    }

    @Override
    public void deleteFromNodeIndexIfNecessary(final Transaction transaction, final int level,
                                               final NodeSlot nodeSlot) {
        if (!getConfig().isUseNodeSlotIndex() || !(nodeSlot instanceof ChildSlot)) {
            return;
        }

        Objects.requireNonNull(nodeSlotIndexAdapter);
        nodeSlotIndexAdapter.clearChildSlot(transaction, level, (ChildSlot)nodeSlot);
    }

    @Override
    public CompletableFuture<Node> fetchNode(final ReadTransaction transaction, final byte[] nodeId) {
        return getOnWriteListener().onAsyncReadForWrite(fetchNodeInternal(transaction, nodeId).thenApply(this::checkNode));
    }

    protected abstract CompletableFuture<Node> fetchNodeInternal(ReadTransaction transaction, byte[] nodeId);

    /**
     * Method to perform basic invariant check(s) on a newly-fetched node.
     *
     * @param node the node to check
     * @param <N> the type param for the node in order for this method to not be lossy on the type of the node that
     * was passed in
     *
     * @return the node that was passed in
     */
    @Nullable
    private <N extends Node> N checkNode(@Nullable final N node) {
        if (node != null && (node.size() < getConfig().getMinM() || node.size() > getConfig().getMaxM())) {
            if (!node.isRoot()) {
                throw new IllegalStateException("packing of non-root is out of valid range");
            }
        }
        return node;
    }

    abstract <S extends NodeSlot, N extends AbstractNode<S, N>> AbstractChangeSet<S, N>
            newInsertChangeSet(N node, int level, List<S> insertedSlots);

    abstract <S extends NodeSlot, N extends AbstractNode<S, N>> AbstractChangeSet<S, N>
            newUpdateChangeSet(N node, int level, S originalSlot, S updatedSlot);

    abstract <S extends NodeSlot, N extends AbstractNode<S, N>> AbstractChangeSet<S, N>
            newDeleteChangeSet(N node, int level, List<S> deletedSlots);
}
