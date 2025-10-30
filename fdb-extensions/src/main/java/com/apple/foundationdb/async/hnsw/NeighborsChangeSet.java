/*
 * NeighborsChangeSet.java
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
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * Represents a set of changes to the neighbors of a node within an HNSW graph.
 * <p>
 * Implementations of this interface manage modifications, such as additions or removals of neighbors. often in a
 * layered fashion. This allows for composing changes before they are committed to storage. The {@link #getParent()}
 * method returns the next element in this layered structure while {@link #merge()} consolidates changes into
 * a final neighbor list.
 *
 * @param <N> the type of the node reference, which must extend {@link NodeReference}
 */
interface NeighborsChangeSet<N extends NodeReference> {
    /**
     * Gets the parent change set from which this change set was derived.
     * <p>
     * Change sets can be layered, forming a chain of modifications.
     * This method allows for traversing up this tree to the preceding set of changes.
     *
     * @return the parent {@code NeighborsChangeSet}, or {@code null} if this change set
     * is the root of the change tree and has no parent.
     */
    @Nullable
    NeighborsChangeSet<N> getParent();

    /**
     * Merges multiple internal sequences into a single, consolidated iterable sequence.
     * <p>
     * This method combines distinct internal changesets into one continuous stream of neighbors. The specific order
     * of the merged elements depends on the implementation.
     *
     * @return a non-null {@code Iterable} containing the merged sequence of elements.
     */
    @Nonnull
    Iterable<N> merge();

    /**
     * Writes the neighbor delta for a given {@link AbstractNode} to the specified storage layer.
     * <p>
     * This method processes the provided {@code node} and writes only the records that match the given
     * {@code primaryKeyPredicate} to the storage system via the {@link InliningStorageAdapter}. The entire operation
     * is performed within the context of the supplied {@link Transaction}.
     *
     * @param storageAdapter the storage adapter to which the delta will be written; must not be null
     * @param quantizer quantizer to use
     * @param transaction the transaction context for the write operation; must not be null
     * @param layer the specific storage layer to write the delta to
     * @param node the source node containing the data to be written; must not be null
     * @param primaryKeyPredicate a predicate to filter records by their primary key. Only records
     *        for which the predicate returns {@code true} will be written. Must not be null.
     */
    void writeDelta(@Nonnull InliningStorageAdapter storageAdapter, @Nonnull Transaction transaction,
                    @Nonnull Quantizer quantizer, int layer, @Nonnull AbstractNode<N> node,
                    @Nonnull Predicate<Tuple /* primary key */> primaryKeyPredicate);
}
