/*
 * OnWriteListener.java
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * Interface for call backs whenever we write data to the database.
 */
public interface OnWriteListener {
    OnWriteListener NOOP = new OnWriteListener() {
    };

    /**
     * Callback method invoked after a node has been successfully written to a specific layer.
     * <p>
     * This is a default method with an empty implementation, allowing implementing classes to override it only if they
     * need to react to this event.
     * @param layer the index of the layer where the node was written.
     * @param node the {@link Node} that was written; guaranteed to be non-null.
     */
    @SuppressWarnings("unused")
    default void onNodeWritten(final int layer, @Nonnull final Node<? extends NodeReference> node) {
        // nothing
    }

    /**
     * Callback method invoked after a node has been successfully deleted from a specific layer.
     * <p>
     * This is a default method with an empty implementation, allowing implementing classes to override it only if they
     * need to react to this event.
     * @param layer the index of the layer where the node was deleted.
     * @param primaryKey the {@link Tuple} used as key to identify the node that was deleted; guaranteed to be non-null.
     */
    @SuppressWarnings("unused")
    default void onNodeDeleted(final int layer, @Nonnull final Tuple primaryKey) {
        // nothing
    }

    /**
     * Callback method invoked when a neighbor is written for a specific node.
     * <p>
     * This method serves as a notification that a neighbor relationship has been established or updated. It is
     * typically called after a write operation successfully adds a {@code neighbor} to the specified {@code node}
     * within a given {@code layer}.
     * <p>
     * As a {@code default} method, the base implementation does nothing. Implementers can override this to perform
     * custom actions, such as updating caches or triggering subsequent events in response to the change.
     * @param layer the index of the layer where the neighbor write operation occurred
     * @param node the {@link Node} for which the neighbor was written; must not be null
     * @param neighbor the {@link NodeReference} of the neighbor that was written; must not be null
     */
    @SuppressWarnings("unused")
    default void onNeighborWritten(final int layer, @Nonnull final Node<? extends NodeReference> node,
                                   @Nonnull final NodeReference neighbor) {
        // nothing
    }

    /**
     * Callback method invoked when a neighbor of a specific node is deleted.
     * <p>
     * This is a default method and its base implementation is a no-op. Implementors of the interface can override this
     * method to react to the deletion of a neighbor node, for example, to clean up related resources or update internal
     * state.
     * @param layer the layer index where the deletion occurred
     * @param node the {@link Node} whose neighbor was deleted
     * @param neighborPrimaryKey the primary key (as a {@link Tuple}) of the neighbor that was deleted
     */
    @SuppressWarnings("unused")
    default void onNeighborDeleted(final int layer, @Nonnull final Node<? extends NodeReference> node,
                                   @Nonnull final Tuple neighborPrimaryKey) {
        // nothing
    }

    @SuppressWarnings("unused")
    default void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
        // nothing
    }

    @SuppressWarnings("unused")
    default void onKeyDeleted(final int layer, @Nonnull final byte[] key) {
        // nothing
    }

    @SuppressWarnings("unused")
    default void onRangeDeleted(final int layer, @Nonnull final Range range) {
        // nothing
    }
}
