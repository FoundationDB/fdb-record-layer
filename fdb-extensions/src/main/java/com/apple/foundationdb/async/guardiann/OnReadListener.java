/*
 * OnReadListener.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.async.common.OnKeyValueReadListener;
import com.apple.foundationdb.async.hnsw.Node;
import com.apple.foundationdb.async.hnsw.NodeReference;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for call backs whenever we read node data from the database.
 */
public interface OnReadListener extends OnKeyValueReadListener {
    OnReadListener NOOP = new OnReadListener() {
    };

    /**
     * A callback method that can be overridden to intercept the result of an asynchronous node read.
     * <p>
     * This method provides a hook for subclasses to inspect or modify the {@code CompletableFuture} after an
     * asynchronous read operation is initiated. The default implementation is a no-op that simply returns the original
     * future. This method is intended to be used to measure elapsed time between the creation of a
     * {@link CompletableFuture} and its completion.
     * @param <N> the type of the {@code NodeReference}
     * @param future the {@code CompletableFuture} representing the pending asynchronous read operation.
     * @return a {@code CompletableFuture} that will complete with the read {@code Node}.
     *         By default, this is the same future that was passed as an argument.
     */
    @SuppressWarnings("unused")
    default <N extends NodeReference, T extends Node<N>> CompletableFuture<T> onAsyncRead(@Nonnull CompletableFuture<T> future) {
        return future;
    }

    /**
     * Callback method invoked when a node is read during a traversal process.
     * <p>
     * This default implementation does nothing. Implementors can override this method to add custom logic that should
     * be executed for each node encountered. This serves as an optional hook for processing nodes as they are read.
     * @param layer the layer or depth of the node in the structure, starting from 0.
     * @param node the {@link Node} that was just read (guaranteed to be non-null).
     */
    @SuppressWarnings("unused")
    default void onNodeRead(int layer, @Nonnull Node<? extends NodeReference> node) {
        // nothing
    }
}
