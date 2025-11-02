/*
 * NodeFactory.java
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

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A factory interface for creating {@link AbstractNode} instances within a Hierarchical Navigable Small World (HNSW)
 * graph.
 * <p>
 * Implementations of this interface define how nodes are constructed, allowing for different node types
 * or storage strategies within the HNSW structure.
 *
 * @param <N> the type of {@link NodeReference} used to refer to nodes in the graph
 */
interface NodeFactory<N extends NodeReference> {
    /**
     * Creates a new node with the specified properties.
     * <p>
     * This method is responsible for instantiating a {@code Node} object, initializing it
     * with a primary key, an optional feature vector, and a list of its initial neighbors.
     *
     * @param primaryKey the {@link Tuple} representing the unique primary key for the new node. Must not be
     *        {@code null}.
     * @param vector the optional feature {@link RealVector} associated with the node, which can be used for similarity
     *        calculations. May be {@code null} if the node does not encode a vector (see {@link CompactNode} versus
     *        {@link InliningNode}).
     * @param neighbors the list of initial {@link NodeReference}s for the new node,
     * establishing its initial connections in the graph. Must not be {@code null}.
     *
     * @return a new, non-null {@link AbstractNode} instance configured with the provided parameters.
     */
    @Nonnull
    AbstractNode<N> create(@Nonnull Tuple primaryKey, @Nullable Transformed<RealVector> vector,
                           @Nonnull List<? extends NodeReference> neighbors);

    /**
     * Gets the kind of this node.
     * @return the kind of this node, never {@code null}.
     */
    @Nonnull
    NodeKind getNodeKind();
}
