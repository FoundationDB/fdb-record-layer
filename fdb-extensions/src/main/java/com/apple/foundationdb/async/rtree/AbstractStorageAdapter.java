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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Implementations common to all concrete implementations of {@link StorageAdapter}.
 */
abstract class AbstractStorageAdapter implements StorageAdapter {
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

    protected AbstractStorageAdapter(@Nonnull final RTree.Config config, @Nonnull final Subspace subspace,
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

    @Nullable
    public Subspace getSecondarySubspace() {
        return nodeSlotIndexAdapter == null ? null : nodeSlotIndexAdapter.getSecondarySubspace();
    }

    @Nonnull
    protected Function<RTree.Point, BigInteger> getHilbertValueFunction() {
        return hilbertValueFunction;
    }

    @Override
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    @Override
    @Nonnull
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

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
        Objects.requireNonNull(nodeSlotIndexAdapter);
        return nodeSlotIndexAdapter.scanIndexForNodeId(transaction, level, hilbertValue, key, isInsertUpdate)
                .thenCompose(nodeId -> nodeId == null
                                       ? CompletableFuture.completedFuture(null)
                                       : fetchNode(transaction, nodeId));
    }

    @Override
    public void insertIntoNodeIndexIfNecessary(@Nonnull final Transaction transaction, final int level,
                                               @Nonnull final NodeSlot nodeSlot) {
        if (!getConfig().isUseSlotIndex() || !(nodeSlot instanceof ChildSlot)) {
            return;
        }

        Objects.requireNonNull(nodeSlotIndexAdapter);
        nodeSlotIndexAdapter.writeChildSlot(transaction, level, (ChildSlot)nodeSlot);
    }

    @Override
    public void deleteFromNodeIndexIfNecessary(@Nonnull final Transaction transaction, final int level,
                                               @Nonnull final NodeSlot nodeSlot) {
        if (!getConfig().isUseSlotIndex() || !(nodeSlot instanceof ChildSlot)) {
            return;
        }

        Objects.requireNonNull(nodeSlotIndexAdapter);
        nodeSlotIndexAdapter.clearChildSlot(transaction, level, (ChildSlot)nodeSlot);
    }

    @Nonnull
    @Override
    public CompletableFuture<Node> fetchNode(@Nonnull final ReadTransaction transaction, @Nonnull final byte[] nodeId) {
        return getOnWriteListener().onAsyncReadForWrite(fetchNodeInternal(transaction, nodeId).thenApply(this::checkNode));
    }

    @Nonnull
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
    @Nullable
    private <N extends Node> N checkNode(@Nullable final N node) {
        if (node != null && (node.size() < getConfig().getMinM() || node.size() > getConfig().getMaxM())) {
            if (!node.isRoot()) {
                throw new IllegalStateException("packing of non-root is out of valid range");
            }
        }
        return node;
    }
}
