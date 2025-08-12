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

import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * TODO.
 * @param <N> neighbor type
 */
public interface Node<N extends Neighbor> {
    @Nonnull
    Tuple getPrimaryKey();

    @Nonnull
    Vector<Half> getVector();

    @Nonnull
    Iterable<N> getNeighbors();

    @Nonnull
    N getNeighbor(int index);

    @CanIgnoreReturnValue
    @Nonnull
    Node<N> insert(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex, @Nonnull NodeSlot slot);

    @CanIgnoreReturnValue
    @Nonnull
    Node<N> update(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex, @Nonnull NodeSlot updatedSlot);

    @CanIgnoreReturnValue
    @Nonnull
    Node<N> delete(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex);

    /**
     * Return the kind of the node, i.e. {@link NodeKind#DATA} or {@link NodeKind#INTERMEDIATE}.
     * @return the kind of this node as a {@link NodeKind}
     */
    @Nonnull
    NodeKind getKind();

    @Nonnull
    DataNode asDataNode();

    @Nonnull
    IntermediateNode asIntermediateNode();

    @Nonnull
    NodeWithLayer<N> withLayer(int layer);
}
