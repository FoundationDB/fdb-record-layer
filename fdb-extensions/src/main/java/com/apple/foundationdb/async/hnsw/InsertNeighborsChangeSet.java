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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * TODO.
 */
class InsertNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    @Nonnull
    private final NeighborsChangeSet<N> parent;

    @Nonnull
    private final List<N> insertedNeighbors;

    public InsertNeighborsChangeSet(@Nonnull final NeighborsChangeSet<N> parent,
                                    @Nonnull final List<N> insertedNeighbors) {
        this.parent = parent;
        this.insertedNeighbors = ImmutableList.copyOf(insertedNeighbors);
    }

    @Nonnull
    public NeighborsChangeSet<N> getParent() {
        return parent;
    }

    @Nonnull
    public Iterable<N> merge() {
        return Iterables.concat(getParent().merge(), insertedNeighbors);
    }

    @Override
    public void writeDelta(@Nonnull final Transaction transaction) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
