/*
 * NodeReferenceAndNode.java
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

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A container class that pairs a {@link NodeReferenceWithDistance} with its corresponding {@link Node} object.
 * <p>
 * This is often used during graph traversal or searching, where a reference to a node (along with its distance from a
 * query point) is first identified, and then the complete node data is fetched. This class holds these two related
 * pieces of information together.
 * @param <N> the type of {@link NodeReference} used within the {@link Node}
 */
public class NodeReferenceAndNode<N extends NodeReference> {
    @Nonnull
    private final NodeReferenceWithDistance nodeReferenceWithDistance;
    @Nonnull
    private final Node<N> node;

    /**
     * Constructs a new instance that pairs a node reference (with distance) with its
     * corresponding {@link Node} object.
     * @param nodeReferenceWithDistance the reference to a node, which also includes distance information. Must not be
     *        {@code null}.
     * @param node the actual {@code Node} object that the reference points to. Must not be {@code null}.
     */
    public NodeReferenceAndNode(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance, @Nonnull final Node<N> node) {
        this.nodeReferenceWithDistance = nodeReferenceWithDistance;
        this.node = node;
    }

    /**
     * Gets the node reference and its associated distance.
     * @return the non-null {@link NodeReferenceWithDistance} object.
     */
    @Nonnull
    public NodeReferenceWithDistance getNodeReferenceWithDistance() {
        return nodeReferenceWithDistance;
    }

    /**
     * Gets the underlying node represented by this object.
     * @return the associated {@link Node} instance, never {@code null}.
     */
    @Nonnull
    public Node<N> getNode() {
        return node;
    }

    /**
     * Helper to extract the references from a given collection of objects of this container class.
     * @param referencesAndNodes an iterable of {@link NodeReferenceAndNode} objects from which to extract the
     *        references.
     * @return a {@link List} of {@link NodeReferenceAndNode}s
     */
    @Nonnull
    public static List<NodeReferenceWithDistance> getReferences(@Nonnull List<? extends NodeReferenceAndNode<?>> referencesAndNodes) {
        final ImmutableList.Builder<NodeReferenceWithDistance> referencesBuilder = ImmutableList.builder();
        for (final NodeReferenceAndNode<?> referenceWithNode : referencesAndNodes) {
            referencesBuilder.add(referenceWithNode.getNodeReferenceWithDistance());
        }
        return referencesBuilder.build();
    }
}
