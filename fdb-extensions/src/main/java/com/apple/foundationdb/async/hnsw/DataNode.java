/*
 * DataNode.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * TODO.
 */
class DataNode extends AbstractNode<NodeReference> {
    @Nonnull
    private final Vector<Half> vector;

    public DataNode(@Nonnull final Tuple primaryKey, @Nonnull final Vector<Half> vector,
                    @Nonnull final List<NodeReference> nodeReferences) {
        super(primaryKey, nodeReferences);
        this.vector = vector;
    }

    @Nonnull
    @Override
    public NodeKind getKind() {
        return NodeKind.DATA;
    }

    @Nonnull
    public Vector<Half> getVector() {
        return vector;
    }

    @Nonnull
    @Override
    public DataNode asDataNode() {
        return this;
    }

    @Nonnull
    @Override
    public IntermediateNode asIntermediateNode() {
        throw new IllegalStateException("this is not a data node");
    }

    @Override
    public NodeCreator<NodeReference> sameCreator() {
        return DataNode::creator;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static Node<NodeReference> creator(@Nonnull final NodeKind nodeKind,
                                              @Nonnull final Tuple primaryKey,
                                              @Nullable final Vector<Half> vector,
                                              @Nonnull final List<? extends NodeReference> neighbors) {
        Verify.verify(nodeKind == NodeKind.INTERMEDIATE);
        return new DataNode(primaryKey, Objects.requireNonNull(vector), (List<NodeReference>)neighbors);
    }
}
