/*
 * AbstractNode.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * An abstract base class implementing the {@link Node} interface.
 * <p>
 * This class provides the fundamental structure for a node within the HNSW graph,
 * managing a unique {@link Tuple} primary key and an immutable list of its neighbors.
 * Subclasses are expected to provide concrete implementations, potentially adding
 * more state or behavior.
 *
 * @param <N> the type of the node reference used for neighbors, which must extend {@link NodeReference}
 */
abstract class AbstractNode<N extends NodeReference> implements Node<N> {
    @Nonnull
    private final Tuple primaryKey;

    @Nonnull
    private final List<N> neighbors;

    /**
     * Constructs a new {@code AbstractNode} with a specified primary key and a list of neighbors.
     * <p>
     * This constructor creates a defensive, immutable copy of the provided {@code neighbors} list.
     * This ensures that the internal state of the node cannot be modified by external
     * changes to the original list after construction.
     *
     * @param primaryKey the unique identifier for this node; must not be {@code null}
     * @param neighbors the list of nodes connected to this node; must not be {@code null}
     */
    protected AbstractNode(@Nonnull final Tuple primaryKey,
                           @Nonnull final List<N> neighbors) {
        this.primaryKey = primaryKey;
        this.neighbors = ImmutableList.copyOf(neighbors);
    }

    /**
     * Gets the primary key that uniquely identifies this object.
     * @return the primary key {@link Tuple}, which will never be {@code null}.
     */
    @Nonnull
    @Override
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Gets the list of neighbors connected to this node.
     * <p>
     * This method returns a direct reference to the internal list which is
     * immutable.
     * @return a non-null, possibly empty, list of neighbors.
     */
    @Nonnull
    @Override
    public List<N> getNeighbors() {
        return neighbors;
    }


    /**
     * Converts this node into its {@link CompactNode} representation.
     * <p>
     * A {@code CompactNode} is a space-efficient implementation {@code Node}. This method provides the
     * conversion logic to transform the current object into that compact form.
     *
     * @return a non-null {@link CompactNode} representing the current node.
     */
    @Nonnull
    public abstract CompactNode asCompactNode();

    /**
     * Converts this node into its {@link InliningNode} representation.
     * @return this object cast to an {@link InliningNode}; never {@code null}.
     * @throws ClassCastException if this object is not actually an instance of
     * {@link InliningNode}.
     */
    @Nonnull
    public abstract InliningNode asInliningNode();
}
