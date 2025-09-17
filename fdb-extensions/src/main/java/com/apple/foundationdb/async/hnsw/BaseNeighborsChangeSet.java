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
 * A base implementation of the {@link NeighborsChangeSet} interface.
 * <p>
 * This class represents a complete, non-delta state of a node's neighbors. It holds a fixed, immutable
 * list of neighbors provided at construction time. As such, it does not support parent change sets or writing deltas.
 *
 * @param <N> the type of the node reference, which must extend {@link NodeReference}
 */
class BaseNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    @Nonnull
    private final List<N> neighbors;

    /**
     * Creates a new change set with the specified neighbors.
     * <p>
     * This constructor creates an immutable copy of the provided list.
     *
     * @param neighbors the list of neighbors for this change set; must not be null.
     */
    public BaseNeighborsChangeSet(@Nonnull final List<N> neighbors) {
        this.neighbors = ImmutableList.copyOf(neighbors);
    }

    /**
     * Gets the parent change set.
     * <p>
     * This implementation always returns {@code null}, as this type of change set
     * does not have a parent.
     *
     * @return always {@code null}.
     */
    @Nullable
    @Override
    public BaseNeighborsChangeSet<N> getParent() {
        return null;
    }

    /**
     * Retrieves the list of neighbors associated with this object.
     * <p>
     * This implementation fulfills the {@code merge} contract by simply returning the
     * existing list of neighbors without performing any additional merging logic.
     * @return a non-null list of neighbors. The generic type {@code N} represents
     *         the type of the neighboring elements.
     */
    @Nonnull
    @Override
    public List<N> merge() {
        return neighbors;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation is a no-op and does not write any delta information,
     * as indicated by the empty method body.
     */
    @Override
    public void writeDelta(@Nonnull final InliningStorageAdapter storageAdapter, @Nonnull final Transaction transaction,
                           final int layer, @Nonnull final Node<N> node,
                           @Nonnull final Predicate<Tuple> primaryKeyPredicate) {
        // nothing to be written
    }
}
