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
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Predicate;

/**
 * TODO.
 */
class BaseNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    @Nonnull
    private final List<N> neighbors;

    public BaseNeighborsChangeSet(@Nonnull final List<N> neighbors) {
        this.neighbors = ImmutableList.copyOf(neighbors);
    }

    @Nullable
    @Override
    public BaseNeighborsChangeSet<N> getParent() {
        return null;
    }

    @Nonnull
    @Override
    public List<N> merge() {
        return neighbors;
    }

    @Override
    public void writeDelta(@Nonnull final InliningStorageAdapter storageAdapter, @Nonnull final Transaction transaction,
                           final int layer, @Nonnull final Node<N> node,
                           @Nonnull final Predicate<Tuple> primaryKeyPredicate) {
        // nothing to be written
    }
}
