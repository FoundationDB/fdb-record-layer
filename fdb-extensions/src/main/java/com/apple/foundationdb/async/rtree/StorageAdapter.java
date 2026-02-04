/*
 * StorageAdapter.java
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Storage adapter used for serialization and deserialization of nodes.
 */
interface StorageAdapter {
    /**
     * Get the {@link RTree.Config} associated with this storage adapter.
     * @return the configuration used by this storage adapter
     */
    @Nonnull
    RTree.Config getConfig();

    /**
     * Get the subspace used to store this r-tree.
     *
     * @return r-tree subspace
     */
    @Nonnull
    Subspace getSubspace();

    /**
     * Get the subspace used to store a node slot index if in warranted by the {@link RTree.Config}.
     *
     * @return secondary subspace or {@code null} if we do not maintain a node slot index
     */
    @Nullable
    Subspace getSecondarySubspace();

    /**
     * Get the on-write listener.
     *
     * @return the on-write listener.
     */
    @Nonnull
    OnWriteListener getOnWriteListener();

    /**
     * Get the on-read listener.
     *
     * @return the on-read listener.
     */
    @Nonnull
    OnReadListener getOnReadListener();

    /**
     * Insert a new entry into the node index if configuration indicates we should maintain such an index.
     *
     * @param transaction the transaction to use
     * @param level the level counting starting at {@code 0} indicating the leaf level increasing upwards
     * @param nodeSlot the {@link NodeSlot} to be inserted
     */
    void insertIntoNodeIndexIfNecessary(@Nonnull Transaction transaction, int level, @Nonnull NodeSlot nodeSlot);

    /**
     * Deletes an entry from the node index if configuration indicates we should maintain such an index.
     *
     * @param transaction the transaction to use
     * @param level the level counting starting at {@code 0} indicating the leaf level increasing upwards
     * @param nodeSlot the {@link NodeSlot} to be deleted
     */
    void deleteFromNodeIndexIfNecessary(@Nonnull Transaction transaction, int level, @Nonnull NodeSlot nodeSlot);

    /**
     * Persist a node slot.
     *
     * @param transaction the transaction to use
     * @param node node whose slot to persist
     * @param itemSlot the node slot to persist
     */
    void writeLeafNodeSlot(@Nonnull Transaction transaction, @Nonnull LeafNode node, @Nonnull ItemSlot itemSlot);

    /**
     * Clear out a leaf node slot.
     *
     * @param transaction the transaction to use
     * @param node node whose slot is cleared out
     * @param itemSlot the node slot to clear out
     */
    void clearLeafNodeSlot(@Nonnull Transaction transaction, @Nonnull LeafNode node, @Nonnull ItemSlot itemSlot);

    /**
     * Method to (re-)persist a list of nodes passed in.
     *
     * @param transaction the transaction to use
     * @param nodes a list of nodes to be (re-persisted)
     */
    void writeNodes(@Nonnull Transaction transaction, @Nonnull List<? extends Node> nodes);

    /**
     * Scan the node slot index for the given Hilbert Value/key pair and return the appropriate {@link Node}.
     * Note that this method requires a node slot index to be maintained.
     *
     * @param transaction the transaction to use
     * @param level the level we should search counting upwards starting from level {@code 0} for the leaf node
     * level.
     * @param hilbertValue the Hilbert Value of the {@code (Hilbert Value, key)} pair to search for
     * @param key the key of the {@code (Hilbert Value, key)} pair to search for
     * @param isInsertUpdate a use case indicator determining if this search is going to be used for an
     * update operation or a delete operation
     *
     * @return a future that when completed holds the appropriate {@link Node} or {@code null} if such a
     * {@link Node} could not be found.
     */
    @Nonnull
    CompletableFuture<Node> scanNodeIndexAndFetchNode(@Nonnull ReadTransaction transaction, int level,
                                                      @Nonnull BigInteger hilbertValue, @Nonnull Tuple key,
                                                      boolean isInsertUpdate);

    /**
     * Method to fetch the data needed to construct a {@link Node}. Note that a node on disk is represented by its
     * slots. Each slot is represented by a key/value pair in FDB. Each key (common for both leaf and intermediate
     * nodes) starts with an 8-byte node id (which is usually a serialized {@link UUID}) followed by one byte which
     * indicates the {@link NodeKind} of node the slot belongs to.
     *
     * @param transaction the transaction to use
     * @param nodeId the node id we should use
     *
     * @return A completable future containing the {@link Node} that was fetched from the database once completed.
     * The node may be an object of {@link LeafNode} or of {@link IntermediateNode}.
     */
    @Nonnull
    CompletableFuture<Node> fetchNode(@Nonnull ReadTransaction transaction, @Nonnull byte[] nodeId);
}
