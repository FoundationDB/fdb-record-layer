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
 * TODO.
 */
class InliningNode extends AbstractNode<NodeReferenceWithVector> {
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

    public InliningNode(@Nonnull final Tuple primaryKey,
                        @Nonnull final List<NodeReferenceWithVector> neighbors) {
        super(primaryKey, neighbors);
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public NodeReferenceWithVector getSelfReference(@Nullable final Vector<Half> vector) {
        return new NodeReferenceWithVector(getPrimaryKey(), Objects.requireNonNull(vector));
    }

    @Nonnull
    @Override
    public NodeKind getKind() {
        return NodeKind.INLINING;
    }

    @Nonnull
    @Override
    public CompactNode asCompactNode() {
        throw new IllegalStateException("this is not a compact node");
    }

    @Nonnull
    @Override
    public InliningNode asInliningNode() {
        return this;
    }

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
