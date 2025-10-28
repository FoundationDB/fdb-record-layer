/*
 * Node.java
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

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Represents a node within an HNSW (Hierarchical Navigable Small World) structure.
 * <p>
 * A node corresponds to a data point (vector) in the structure and maintains a list of its neighbors.
 * This interface defines the common contract for different node representations, such as {@link CompactNode}
 * and {@link InliningNode}.
 * </p>
 *
 * @param <N> the type of reference used to point to other nodes, which must extend {@link NodeReference}
 */
public interface Node<N extends NodeReference> {
    /**
     * Gets the primary key for this object.
     * <p>
     * The primary key is represented as a {@link Tuple} and uniquely identifies
     * the object within its storage context. This method is guaranteed to not
     * return a null value.
     *
     * @return the primary key as a {@code Tuple}, which is never {@code null}
     */
    @Nonnull
    Tuple getPrimaryKey();

    /**
     * Returns a self-reference to this object, enabling fluent method chaining. This allows to create node references
     * that contain a vector and are independent of the storage implementation.
     * @param vector the vector of {@code Half} objects to process. This parameter
     * is optional and can be {@code null}.
     *
     * @return a non-null reference to this object ({@code this}) for further
     * method calls.
     */
    @Nonnull
    N getSelfReference(@Nullable RealVector vector);

    /**
     * Gets the list of neighboring nodes.
     * <p>
     * This method is guaranteed to not return {@code null}. If there are no neighbors, an empty list is returned.
     *
     * @return a non-null list of neighboring nodes.
     */
    @Nonnull
    List<N> getNeighbors();

    /**
     * Return the kind of the node, i.e. {@link NodeKind#COMPACT} or {@link NodeKind#INLINING}.
     * @return the kind of this node as a {@link NodeKind}
     */
    @Nonnull
    NodeKind getKind();

    /**
     * Converts this node into its {@link CompactNode} representation.
     * <p>
     * A {@code CompactNode} is a space-efficient implementation {@code Node}. This method provides the
     * conversion logic to transform the current object into that compact form.
     *
     * @return a non-null {@link CompactNode} representing the current node.
     */
    @Nonnull
    CompactNode asCompactNode();

    /**
     * Converts this node into its {@link InliningNode} representation.
     * @return this object cast to an {@link InliningNode}; never {@code null}.
     * @throws ClassCastException if this object is not actually an instance of
     * {@link InliningNode}.
     */
    @Nonnull
    InliningNode asInliningNode();
}
