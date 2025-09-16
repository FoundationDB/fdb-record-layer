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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Implementations and attributes common to all concrete implementations of {@link StorageAdapter}.
 */
abstract class AbstractStorageAdapter<N extends NodeReference> implements StorageAdapter<N> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(AbstractStorageAdapter.class);

    @Nonnull
    private final HNSW.Config config;
    @Nonnull
    private final NodeFactory<N> nodeFactory;
    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    private final Subspace dataSubspace;

    protected AbstractStorageAdapter(@Nonnull final HNSW.Config config, @Nonnull final NodeFactory<N> nodeFactory,
                                     @Nonnull final Subspace subspace,
                                     @Nonnull final OnWriteListener onWriteListener,
                                     @Nonnull final OnReadListener onReadListener) {
        this.config = config;
        this.nodeFactory = nodeFactory;
        this.subspace = subspace;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
        this.dataSubspace = subspace.subspace(Tuple.from(SUBSPACE_PREFIX_DATA));
    }

    @Override
    @Nonnull
    public HNSW.Config getConfig() {
        return config;
    }

    @Nonnull
    @Override
    public NodeFactory<N> getNodeFactory() {
        return nodeFactory;
    }

    @Nonnull
    @Override
    public NodeKind getNodeKind() {
        return getNodeFactory().getNodeKind();
    }

    @Override
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    @Override
    @Nonnull
    public Subspace getDataSubspace() {
        return dataSubspace;
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
    @Override
    public CompletableFuture<Node<N>> fetchNode(@Nonnull final ReadTransaction readTransaction,
                                                int layer, @Nonnull Tuple primaryKey) {
        return fetchNodeInternal(readTransaction, layer, primaryKey).thenApply(this::checkNode);
    }

    @Nonnull
    protected abstract CompletableFuture<Node<N>> fetchNodeInternal(@Nonnull ReadTransaction readTransaction,
                                                                    int layer, @Nonnull Tuple primaryKey);

    /**
     * Method to perform basic invariant check(s) on a newly-fetched node.
     *
     * @param node the node to check
     * was passed in
     *
     * @return the node that was passed in
     */
    @Nullable
    private Node<N> checkNode(@Nullable final Node<N> node) {
        return node;
    }

    @Override
    public void writeNode(@Nonnull Transaction transaction, @Nonnull Node<N> node, int layer,
                          @Nonnull NeighborsChangeSet<N> changeSet) {
        writeNodeInternal(transaction, node, layer, changeSet);
        if (logger.isDebugEnabled()) {
            logger.debug("written node with key={} at layer={}", node.getPrimaryKey(), layer);
        }
    }

    protected abstract void writeNodeInternal(@Nonnull Transaction transaction, @Nonnull Node<N> node, int layer,
                                              @Nonnull NeighborsChangeSet<N> changeSet);

}
