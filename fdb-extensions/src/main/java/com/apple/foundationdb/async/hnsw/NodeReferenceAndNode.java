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

public class NodeReferenceAndNode<R extends NodeReference> {
    @Nonnull
    private final NodeReferenceWithDistance nodeReferenceWithDistance;
    @Nonnull
    private final Node<R> node;

    public NodeReferenceAndNode(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance, @Nonnull final Node<R> node) {
        this.nodeReferenceWithDistance = nodeReferenceWithDistance;
        this.node = node;
    }

    @Nonnull
    public NodeReferenceWithDistance getNodeReferenceWithDistance() {
        return nodeReferenceWithDistance;
    }

    @Nonnull
    public Node<R> getNode() {
        return node;
    }

    @Nonnull
    public static <N extends NodeReference> List<NodeReferenceWithDistance> getReferences(@Nonnull List<NodeReferenceAndNode<N>> referencesAndNodes) {
        final ImmutableList.Builder<NodeReferenceWithDistance> referencesBuilder = ImmutableList.builder();
        for (final NodeReferenceAndNode<R> referenceWithNode : referencesAndNodes) {
            referencesBuilder.add(referenceWithNode.getNodeReferenceWithDistance());
        }
        return referencesBuilder.build();
    }
}
