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

import com.apple.foundationdb.Transaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * TODO.
 */
class BaseNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    @Nonnull
    private final NodeReferenceAndNode<N> baseNode;

    public BaseNeighborsChangeSet(@Nonnull final NodeReferenceAndNode<N> baseNode) {
        this.baseNode = baseNode;
    }

    @Nullable
    public BaseNeighborsChangeSet<N> getParent() {
        return null;
    }

    @Nonnull
    public List<N> merge() {
        return baseNode.getNode().getNeighbors();
    }

    @Override
    public void writeDelta(@Nonnull final Transaction transaction) {
    }
}
