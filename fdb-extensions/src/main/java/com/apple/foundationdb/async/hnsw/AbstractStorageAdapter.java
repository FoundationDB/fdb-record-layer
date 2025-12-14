/*
 * AbstractStorageAdapter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * An abstract base class for {@link StorageAdapter} implementations.
 * <p>
 * This class provides the common infrastructure for managing HNSW graph data within a {@link Subspace}.
 * It handles the configuration, node creation, and listener management, while delegating the actual
 * storage-specific read and write operations to concrete subclasses through the {@code fetchNodeInternal}
 * and {@code writeNodeInternal} abstract methods.
 *
 * @param <N> the type of {@link NodeReference} used to reference nodes in the graph
 */
abstract class AbstractStorageAdapter<N extends NodeReference> implements StorageAdapter<N> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(AbstractStorageAdapter.class);

    @Nonnull
    private final Config config;
    @Nonnull
    private final NodeFactory<N> nodeFactory;
    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    @Nonnull
    private final Subspace dataSubspace;

    /**
     * Constructs a new {@code AbstractStorageAdapter}.
     * <p>
     * This constructor initializes the adapter with the necessary configuration,
     * factories, and listeners for managing an HNSW graph. It also sets up a
     * dedicated data subspace within the provided main subspace for storing node data.
     *
     * @param config the HNSW graph configuration
     * @param nodeFactory the factory to create new nodes of type {@code <N>}
     * @param subspace the primary subspace for storing all graph-related data
     * @param onWriteListener the listener to be called on write operations
     * @param onReadListener the listener to be called on read operations
     */
    protected AbstractStorageAdapter(@Nonnull final Config config, @Nonnull final NodeFactory<N> nodeFactory,
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
    public Config getConfig() {
        return config;
    }

    @Nonnull
    @Override
    public NodeFactory<N> getNodeFactory() {
        return nodeFactory;
    }

    @Override
    public boolean isInliningStorageAdapter() {
        final boolean isInliningStorageAdapter = getNodeFactory().getNodeKind() == NodeKind.INLINING;
        Verify.verify(!isInliningStorageAdapter || this instanceof InliningStorageAdapter);
        return isInliningStorageAdapter;
    }

    @Nonnull
    @Override
    public InliningStorageAdapter asInliningStorageAdapter() {
        Verify.verify(isInliningStorageAdapter());
        return (InliningStorageAdapter)this;
    }

    @Override
    public boolean isCompactStorageAdapter() {
        final boolean isCompactStorageAdapter = getNodeFactory().getNodeKind() == NodeKind.COMPACT;
        Verify.verify(!isCompactStorageAdapter || this instanceof CompactStorageAdapter);
        return isCompactStorageAdapter;
    }

    @Nonnull
    @Override
    public CompactStorageAdapter asCompactStorageAdapter() {
        Verify.verify(isCompactStorageAdapter());
        return (CompactStorageAdapter)this;
    }

    @Override
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Gets the cached subspace for the data associated with this component.
     * <p>
     * The data subspace defines the portion of the directory space where the data
     * for this component is stored.
     *
     * @return the non-null {@link Subspace} for the data
     */
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
    public CompletableFuture<AbstractNode<N>> fetchNode(@Nonnull final ReadTransaction readTransaction,
                                                        @Nonnull final AffineOperator storageTransform,
                                                        int layer, @Nonnull Tuple primaryKey) {
        return fetchNodeInternal(readTransaction, storageTransform, layer, primaryKey).thenApply(this::checkNode);
    }

    /**
     * Asynchronously fetches a specific node from the data store for a given layer and primary key.
     * <p>
     * This is an internal, abstract method that concrete subclasses must implement to define
     * the storage-specific logic for retrieving a node. The operation is performed within the
     * context of the provided {@link ReadTransaction}.
     *
     * @param readTransaction the transaction to use for the read operation; must not be {@code null}
     * @param storageTransform an affine vector transformation operator that is used to transform the fetched vector
     *        into the storage space that is currently being used
     * @param layer the layer index from which to fetch the node
     * @param primaryKey the primary key that uniquely identifies the node to be fetched; must not be {@code null}
     *
     * @return a {@link CompletableFuture} that will be completed with the fetched {@link AbstractNode}.
     *         The future will complete with {@code null} if no node is found for the given key and layer.
     */
    @Nonnull
    protected abstract CompletableFuture<AbstractNode<N>> fetchNodeInternal(@Nonnull ReadTransaction readTransaction,
                                                                            @Nonnull AffineOperator storageTransform,
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
    protected <T extends AbstractNode<N>> T checkNode(@Nullable final T node) {
        return node;
    }

    /**
     * Writes a given node and its neighbor modifications to the underlying storage.
     * <p>
     * This operation is executed within the context of the provided {@link Transaction}.
     * It handles persisting the node's data at a specific {@code layer} and applies
     * the changes to its neighbors as defined in the {@link NeighborsChangeSet}.
     * This method delegates the core writing logic to an internal method and provides
     * debug logging upon completion.
     *
     * @param transaction the non-null {@link Transaction} context for this write operation
     * @param quantizer the quantizer to use
     * @param layer the layer index where the node is being written
     * @param node the non-null {@link Node} to be written to storage
     * @param changeSet the non-null {@link NeighborsChangeSet} detailing the modifications
     * to the node's neighbors
     */
    @Override
    public void writeNode(@Nonnull final Transaction transaction, @Nonnull final Quantizer quantizer,
                          final int layer, @Nonnull final AbstractNode<N> node,
                          @Nonnull final NeighborsChangeSet<N> changeSet) {
        writeNodeInternal(transaction, quantizer, layer, node, changeSet);
        if (logger.isTraceEnabled()) {
            logger.trace("written node with key={} at layer={}", node.getPrimaryKey(), layer);
        }
    }

    /**
     * Writes a single node to the given layer of the data store as part of a larger transaction.
     * <p>
     * This is an abstract method that concrete implementations must provide.
     * It is responsible for the low-level persistence of the given {@code node} at a
     * specific {@code layer}. The implementation should also handle the modifications
     * to the node's neighbors, as detailed in the {@code changeSet}.
     *
     * @param transaction the non-null transaction context for the write operation
     * @param quantizer the quantizer to use
     * @param layer the layer or level of the node in the structure
     * @param node the non-null {@link Node} to write
     * @param changeSet the non-null {@link NeighborsChangeSet} detailing additions or
     * removals of neighbor links
     */
    protected abstract void writeNodeInternal(@Nonnull Transaction transaction, @Nonnull Quantizer quantizer,
                                              int layer, @Nonnull AbstractNode<N> node,
                                              @Nonnull NeighborsChangeSet<N> changeSet);

    @Override
    public void deleteNode(@Nonnull final Transaction transaction, final int layer, @Nonnull final Tuple primaryKey) {
        deleteNodeInternal(transaction, layer, primaryKey);
        if (logger.isTraceEnabled()) {
            logger.trace("deleted node with key={} at layer={}", primaryKey, layer);
        }
    }

    /**
     * Deletes a single node from the given layer of the data store as part of a larger transaction.
     * @param transaction the transaction to use
     * @param layer the layer
     * @param primaryKey the primary key of the node
     */
    protected abstract void deleteNodeInternal(@Nonnull Transaction transaction, int layer, @Nonnull Tuple primaryKey);
}
