/*
 * InliningNode.java
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
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Represents a specific type of node within a graph structure that is used to represent nodes in an HNSW structure.
 * <p>
 * This node extends {@link AbstractNode}, does not store its own vector and instead specifically manages neighbors
 * of type {@link NodeReferenceWithVector} (which do store a vector each).
 * It provides a concrete implementation for an "inlining" node, distinguishing it from other node types such as
 * {@link CompactNode}.
 */
public class InliningNode extends AbstractNode<NodeReferenceWithVector> {
    @Nonnull
    private static final NodeFactory<NodeReferenceWithVector> FACTORY = new NodeFactory<>() {
        @SuppressWarnings("unchecked")
        @Nonnull
        @Override
        public Node<NodeReferenceWithVector> create(@Nonnull final Tuple primaryKey,
                                                    @Nullable final Vector<Half> vector,
                                                    @Nonnull final List<? extends NodeReference> neighbors) {
            return new InliningNode(primaryKey, (List<NodeReferenceWithVector>)neighbors);
        }

        @Nonnull
        @Override
        public NodeKind getNodeKind() {
            return NodeKind.INLINING;
        }
    };

    /**
     * Constructs a new {@code InliningNode} with a specified primary key and a list of its neighbors.
     * <p>
     * This constructor initializes the node by calling the constructor of its superclass,
     * passing the primary key and neighbor list.
     *
     * @param primaryKey the non-null primary key of the node, represented by a {@link Tuple}.
     * @param neighbors the non-null list of neighbors for this node, where each neighbor
     * is a {@link NodeReferenceWithVector}.
     */
    public InliningNode(@Nonnull final Tuple primaryKey,
                        @Nonnull final List<NodeReferenceWithVector> neighbors) {
        super(primaryKey, neighbors);
    }

    /**
     * Gets a reference to this node.
     *
     * @param vector the vector to be associated with the node reference. Despite the
     * {@code @Nullable} annotation, this parameter must not be null.
     *
     * @return a new {@link NodeReferenceWithVector} instance containing the node's
     * primary key and the provided vector; will never be null.
     *
     * @throws NullPointerException if the provided {@code vector} is null.
     */
    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public NodeReferenceWithVector getSelfReference(@Nullable final Vector<Half> vector) {
        return new NodeReferenceWithVector(getPrimaryKey(), Objects.requireNonNull(vector));
    }

    /**
     * Gets the kind of this node.
     * @return the non-null {@link NodeKind} of this node, which is always
     * {@code NodeKind.INLINING}.
     */
    @Nonnull
    @Override
    public NodeKind getKind() {
        return NodeKind.INLINING;
    }

    /**
     * Casts this node to a {@link CompactNode}.
     * <p>
     * This implementation always throws an exception because this specific node type
     * cannot be represented as a compact node.
     * @return this node as a {@link CompactNode}, never {@code null}
     * @throws IllegalStateException always, as this node is not a compact node
     */
    @Nonnull
    @Override
    public CompactNode asCompactNode() {
        throw new IllegalStateException("this is not a compact node");
    }

    /**
     * Returns this object as an {@link InliningNode}.
     * <p>
     * As this class is already an instance of {@code InliningNode}, this method simply returns {@code this}.
     * @return this object, which is guaranteed to be an {@code InliningNode} and never {@code null}.
     */
    @Nonnull
    @Override
    public InliningNode asInliningNode() {
        return this;
    }

    /**
     * Returns the singleton factory instance used to create {@link NodeReferenceWithVector} objects.
     * <p>
     * This method provides a standard way to obtain the factory, ensuring that a single, shared instance is used
     * throughout the application.
     *
     * @return the singleton {@link NodeFactory} instance, never {@code null}.
     */
    @Nonnull
    public static NodeFactory<NodeReferenceWithVector> factory() {
        return FACTORY;
    }

    @Override
    public String toString() {
        return "I[primaryKey=" + getPrimaryKey() +
                ";neighbors=" + getNeighbors() + "]";
    }
}
