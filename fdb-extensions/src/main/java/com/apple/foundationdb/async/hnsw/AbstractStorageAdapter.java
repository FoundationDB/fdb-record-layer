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
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Implementations and attributes common to all concrete implementations of {@link StorageAdapter}.
 */
abstract class AbstractStorageAdapter implements StorageAdapter {
    public static final byte SUBSPACE_PREFIX_ENTRY_NODE = 0x01;
    public static final byte SUBSPACE_PREFIX_DATA = 0x02;

    @Nonnull
    private final HNSW.Config config;
    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    private final Subspace entryNodeSubspace;
    private final Subspace dataSubspace;

    protected AbstractStorageAdapter(@Nonnull final HNSW.Config config, @Nonnull final Subspace subspace,
                                     @Nonnull final OnWriteListener onWriteListener,
                                     @Nonnull final OnReadListener onReadListener) {
        this.config = config;
        this.subspace = subspace;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;

        this.entryNodeSubspace = subspace.subspace(Tuple.from(SUBSPACE_PREFIX_ENTRY_NODE));
        this.dataSubspace = subspace.subspace(Tuple.from(SUBSPACE_PREFIX_DATA));
    }

    @Override
    @Nonnull
    public HNSW.Config getConfig() {
        return config;
    }

    @Override
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    @Nullable
    @Override
    public Subspace getSecondarySubspace() {
        return null;
    }

    @Override
    @Nonnull
    public Subspace getEntryNodeSubspace() {
        return entryNodeSubspace;
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
    public <N extends NodeReference> CompletableFuture<Node<N>> fetchNode(@Nonnull final NodeFactory<N> nodeFactory,
                                                                          @Nonnull final ReadTransaction readTransaction,
                                                                          int layer, @Nonnull Tuple primaryKey) {
        return fetchNodeInternal(nodeFactory, readTransaction, layer, primaryKey).thenApply(this::checkNode);
    }

    @Nonnull
    protected abstract <N extends NodeReference> CompletableFuture<Node<N>> fetchNodeInternal(@Nonnull NodeFactory<N> nodeFactory,
                                                                                              @Nonnull ReadTransaction readTransaction,
                                                                                              int layer, @Nonnull Tuple primaryKey);

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
    private <N extends NodeReference> Node<N> checkNode(@Nullable final Node<N> node) {
        return node;
    }
}
