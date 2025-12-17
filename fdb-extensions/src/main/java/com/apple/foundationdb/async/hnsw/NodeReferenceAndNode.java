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
 * A container class that pairs a {@link NodeReference} with its corresponding {@link AbstractNode} object.
 * <p>
 * This is often used during graph traversal or searching, where a reference to a node (along with its distance from a
 * query point) is first identified, and then the complete node data is fetched. This class holds these two related
 * pieces of information together.
 * @param <T> the type of {@link NodeReference} referencing the node
 * @param <N> the type of {@link NodeReference} used within the {@link AbstractNode}, i.e. the type of the neighbor
 *        references
 */
class NodeReferenceAndNode<T extends NodeReference, N extends NodeReference> {
    @Nonnull
    private final T nodeReference;
    @Nonnull
    private final AbstractNode<N> node;

    /**
     * Constructs a new instance that pairs a node reference (with distance) with its
     * corresponding {@link AbstractNode} object.
     * @param nodeReference the reference to a node, which also includes distance information. Must not be
     *        {@code null}.
     * @param node the actual {@link AbstractNode} object that the reference points to. Must not be {@code null}.
     */
    public NodeReferenceAndNode(@Nonnull final T nodeReference,
                                @Nonnull final AbstractNode<N> node) {
        this.nodeReference = nodeReference;
        this.node = node;
    }

    /**
     * Gets the node reference and its associated distance.
     * @return the non-null {@link NodeReferenceWithDistance} object.
     */
    @Nonnull
    public T getNodeReference() {
        return nodeReference;
    }

    /**
     * Gets the underlying node represented by this object.
     * @return the associated {@link Node} instance, never {@code null}.
     */
    @Nonnull
    public AbstractNode<N> getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "NRaN[" + nodeReference + "," + node + ']';
    }

    /**
     * Helper to extract the references from a given collection of objects of this container class.
     * @param referencesAndNodes an iterable of {@link NodeReferenceAndNode} objects from which to extract the
     *        references.
     * @return a {@link List} of {@link NodeReferenceAndNode}s
     */
    @Nonnull
    public static <T extends NodeReferenceWithVector> List<T> getReferences(@Nonnull List<? extends NodeReferenceAndNode<T, ?>> referencesAndNodes) {
        final ImmutableList.Builder<T> referencesBuilder = ImmutableList.builder();
        for (final NodeReferenceAndNode<T, ?> referenceWithNode : referencesAndNodes) {
            referencesBuilder.add(referenceWithNode.getNodeReference());
        }
        return referencesBuilder.build();
    }
}
