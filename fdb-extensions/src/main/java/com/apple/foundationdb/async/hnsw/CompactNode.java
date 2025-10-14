/*
 * CompactNode.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Represents a compact node within a graph structure, extending {@link AbstractNode}.
 * <p>
 * This node type is considered "compact" because it directly stores its associated
 * data vector of type {@link Vector}. It is used to represent a vector in a
 * vector space and maintains references to its neighbors via {@link NodeReference} objects.
 *
 * @see AbstractNode
 * @see NodeReference
 */
public class CompactNode extends AbstractNode<NodeReference> {
    @Nonnull
    private static final NodeFactory<NodeReference> FACTORY = new NodeFactory<>() {
        @SuppressWarnings("unchecked")
        @Nonnull
        @Override
        @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
        public Node<NodeReference> create(@Nonnull final Tuple primaryKey, @Nullable final Vector vector,
                                          @Nonnull final List<? extends NodeReference> neighbors) {
            return new CompactNode(primaryKey, Objects.requireNonNull(vector), (List<NodeReference>)neighbors);
        }

        @Nonnull
        @Override
        public NodeKind getNodeKind() {
            return NodeKind.COMPACT;
        }
    };

    @Nonnull
    private final Vector vector;

    /**
     * Constructs a new {@code CompactNode} instance.
     * <p>
     * This constructor initializes the node with its primary key, a data vector,
     * and a list of its neighbors. It delegates the initialization of the
     * {@code primaryKey} and {@code neighbors} to the superclass constructor.
     *
     * @param primaryKey the primary key that uniquely identifies this node; must not be {@code null}.
     * @param vector the data vector of type {@code Vector} associated with this node; must not be {@code null}.
     * @param neighbors a list of {@link NodeReference} objects representing the neighbors of this node; must not be
     *                  {@code null}.
     */
    public CompactNode(@Nonnull final Tuple primaryKey, @Nonnull final Vector vector,
                       @Nonnull final List<NodeReference> neighbors) {
        super(primaryKey, neighbors);
        this.vector = vector;
    }

    /**
     * Returns a {@link NodeReference} that uniquely identifies this node.
     * <p>
     * This implementation creates the reference using the node's primary key, obtained via {@code getPrimaryKey()}. It
     * ignores the provided {@code vector} parameter, which exists to fulfill the contract of the overridden method.
     *
     * @param vector the vector context, which is ignored in this implementation.
     * Per the {@code @Nullable} annotation, this can be {@code null}.
     *
     * @return a non-null {@link NodeReference} to this node.
     */
    @Nonnull
    @Override
    public NodeReference getSelfReference(@Nullable final Vector vector) {
        return new NodeReference(getPrimaryKey());
    }

    /**
     * Gets the kind of this node.
     * This implementation always returns {@link NodeKind#COMPACT}.
     * @return the node kind, which is guaranteed to be {@link NodeKind#COMPACT}.
     */
    @Nonnull
    @Override
    public NodeKind getKind() {
        return NodeKind.COMPACT;
    }

    /**
     * Gets the vector of {@code Half} objects.
     * @return the non-null vector of {@link Half} objects.
     */
    @Nonnull
    public Vector getVector() {
        return vector;
    }

    /**
     * Returns this node as a {@code CompactNode}. As this class is already a {@code CompactNode}, this method provides
     * {@code this}.
     * @return this object cast as a {@code CompactNode}, which is guaranteed to be non-null.
     */
    @Nonnull
    @Override
    public CompactNode asCompactNode() {
        return this;
    }

    /**
     * Returns this node as an {@link InliningNode}.
     * <p>
     * This override is for node types that are not inlining nodes. As such, it
     * will always fail.
     * @return this node as a non-null {@link InliningNode}
     * @throws IllegalStateException always, as this is not an inlining node
     */
    @Nonnull
    @Override
    public InliningNode asInliningNode() {
        throw new IllegalStateException("this is not an inlining node");
    }

    /**
     * Gets the shared factory instance for creating {@link NodeReference} objects.
     * <p>
     * This static factory method is the preferred way to obtain a {@code NodeFactory}
     * for {@link NodeReference} instances, as it returns a shared, pre-configured object.
     *
     * @return a shared, non-null instance of {@code NodeFactory<NodeReference>}
     */
    @Nonnull
    public static NodeFactory<NodeReference> factory() {
        return FACTORY;
    }

    @Override
    public String toString() {
        return "C[primaryKey=" + getPrimaryKey() +
                ";vector=" + vector +
                ";neighbors=" + getNeighbors() + "]";
    }
}
