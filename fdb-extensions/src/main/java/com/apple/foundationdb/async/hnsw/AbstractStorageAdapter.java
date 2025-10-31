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
 * An abstract base class for {@link StorageAdapter} implementations.
 * <p>
 * This class provides the common infrastructure for managing HNSW graph data within {@link Subspace}.
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

    /**
     * Returns the configuration used to build and search this HNSW graph.
     *
     * @return the current {@link HNSW.Config} object, never {@code null}.
     */
    @Override
    @Nonnull
    public HNSW.Config getConfig() {
        return config;
    }

    /**
     * Gets the factory responsible for creating new nodes.
     * <p>
     * This factory is used to instantiate nodes of the generic type {@code N}
     * for the current context. The {@code @Nonnull} annotation guarantees that
     * this method will never return {@code null}.
     *
     * @return the non-null {@link NodeFactory} instance.
     */
    @Nonnull
    @Override
    public NodeFactory<N> getNodeFactory() {
        return nodeFactory;
    }

    /**
     * Gets the kind of this node, which uniquely identifies the type of node.
     * <p>
     * This method is an override and provides a way to determine the concrete
     * type of node without using {@code instanceof} checks.
     *
     * @return the non-null {@link NodeKind} representing the type of this node.
     */
    @Nonnull
    @Override
    public NodeKind getNodeKind() {
        return getNodeFactory().getNodeKind();
    }

    /**
     * Gets the subspace in which this key or value is stored.
     * <p>
     * This subspace provides a logical separation for keys within the underlying key-value store.
     *
     * @return the non-null {@link Subspace} for this context
     */
    @Override
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Gets the subspace for the data associated with this component.
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

    /**
     * Returns the listener that is notified upon write events.
     * <p>
     * This method is an override and guarantees a non-null return value,
     * as indicated by the {@code @Nonnull} annotation.
     *
     * @return the configured {@link OnWriteListener} instance; will never be {@code null}.
     */
    @Override
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    /**
     * Gets the listener that is notified upon completion of a read operation.
     * <p>
     * This method is an override and provides the currently configured listener instance.
     * The returned listener is guaranteed to be non-null as indicated by the
     * {@code @Nonnull} annotation.
     *
     * @return the non-null {@link OnReadListener} instance.
     */
    @Override
    @Nonnull
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

    /**
     * Asynchronously fetches a node from a specific layer of the HNSW.
     * <p>
     * The node is identified by its {@code layer} and {@code primaryKey}. The entire fetch operation is
     * performed within the given {@link ReadTransaction}. After the underlying
     * fetch operation completes, the retrieved node is validated by the
     * {@link  #checkNode(Node)} method before the returned future is completed.
     *
     * @param readTransaction the non-null transaction to use for the read operation
     * @param layer the layer of the tree from which to fetch the node
     * @param primaryKey the non-null primary key that identifies the node to fetch
     *
     * @return a {@link CompletableFuture} that will complete with the fetched {@link Node}
     * once it has been read from storage and validated
     */
    @Nonnull
    @Override
    public CompletableFuture<Node<N>> fetchNode(@Nonnull final ReadTransaction readTransaction,
                                                int layer, @Nonnull Tuple primaryKey) {
        return fetchNodeInternal(readTransaction, layer, primaryKey).thenApply(this::checkNode);
    }

    /**
     * Asynchronously fetches a specific node from the data store for a given layer and primary key.
     * <p>
     * This is an internal, abstract method that concrete subclasses must implement to define
     * the storage-specific logic for retrieving a node. The operation is performed within the
     * context of the provided {@link ReadTransaction}.
     *
     * @param readTransaction the transaction to use for the read operation; must not be {@code null}
     * @param layer the layer index from which to fetch the node
     * @param primaryKey the primary key that uniquely identifies the node to be fetched; must not be {@code null}
     *
     * @return a {@link CompletableFuture} that will be completed with the fetched {@link Node}.
     * The future will complete with {@code null} if no node is found for the given key and layer.
     */
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
     * @param node the non-null {@link Node} to be written to storage
     * @param layer the layer index where the node is being written
     * @param changeSet the non-null {@link NeighborsChangeSet} detailing the modifications
     * to the node's neighbors
     */
    @Override
    public void writeNode(@Nonnull Transaction transaction, @Nonnull Node<N> node, int layer,
                          @Nonnull NeighborsChangeSet<N> changeSet) {
        writeNodeInternal(transaction, node, layer, changeSet);
        if (logger.isDebugEnabled()) {
            logger.debug("written node with key={} at layer={}", node.getPrimaryKey(), layer);
        }
    }

    /**
     * Writes a single node to the data store as part of a larger transaction.
     * <p>
     * This is an abstract method that concrete implementations must provide.
     * It is responsible for the low-level persistence of the given {@code node} at a
     * specific {@code layer}. The implementation should also handle the modifications
     * to the node's neighbors, as detailed in the {@code changeSet}.
     *
     * @param transaction the non-null transaction context for the write operation
     * @param node the non-null {@link Node} to write
     * @param layer the layer or level of the node in the structure
     * @param changeSet the non-null {@link NeighborsChangeSet} detailing additions or
     * removals of neighbor links
     */
    protected abstract void writeNodeInternal(@Nonnull Transaction transaction, @Nonnull Node<N> node, int layer,
                                              @Nonnull NeighborsChangeSet<N> changeSet);

}
