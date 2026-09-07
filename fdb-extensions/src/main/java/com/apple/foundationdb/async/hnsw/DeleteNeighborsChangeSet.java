/*
 * DeleteNeighborsChangeSet.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A {@link NeighborsChangeSet} that represents the deletion of a set of neighbors from a parent change set.
 * <p>
 * This class acts as a filter, wrapping a parent {@link NeighborsChangeSet} and providing a view of the neighbors
 * that excludes those whose primary keys have been marked for deletion.
 *
 * @param <N> the type of the node reference, which must extend {@link NodeReference}
 */
class DeleteNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    private static final Logger logger = LoggerFactory.getLogger(DeleteNeighborsChangeSet.class);

    private final NeighborsChangeSet<N> parent;

    private final Set<Tuple /* primary key */> deletedNeighborsPrimaryKeys;

    /**
     * Constructs a new {@code DeleteNeighborsChangeSet}.
     * <p>
     * This object represents a set of changes where specific neighbors are marked for deletion.
     * It holds a reference to a parent {@link NeighborsChangeSet} and creates an immutable copy
     * of the primary keys for the neighbors to be deleted.
     *
     * @param parent the parent {@link NeighborsChangeSet} to which this deletion change belongs. Must not be null.
     * @param deletedNeighborsPrimaryKeys a {@link Collection} of primary keys, represented as {@link Tuple}s,
     * identifying the neighbors to be deleted. Must not be null.
     */
    public DeleteNeighborsChangeSet(final NeighborsChangeSet<N> parent,
                                    final Collection<Tuple> deletedNeighborsPrimaryKeys) {
        this.parent = parent;
        this.deletedNeighborsPrimaryKeys = ImmutableSet.copyOf(deletedNeighborsPrimaryKeys);
    }

    /**
     * Gets the parent change set from which this change set was derived.
     * <p>
     * In a sequence of modifications, each {@code NeighborsChangeSet} is derived from a previous state, which is
     * considered its parent. This method allows traversing the history of changes backward.
     *
     * @return the parent {@link NeighborsChangeSet}
     */
    @Override
    public NeighborsChangeSet<N> getParent() {
        return parent;
    }

    @Override
    public boolean hasChanges() {
        //
        // We can probably do better by testing if the deletion has an effect on the merge, i.e. if the neighbors that
        // are being deleted by this set are in fact part of the underlying set. That case is currently impossible so
        // we just return true for now.
        //
        return true;
    }

    /**
     * Merges the neighbors from the parent context, filtering out any neighbors that have been marked as deleted.
     * <p>
     * This implementation retrieves the collection of neighbors from its parent by calling
     * {@code getParent().merge()}.
     * It then filters this collection, removing any neighbor whose primary key is present in the
     * {@code deletedNeighborsPrimaryKeys} set.
     * This ensures the resulting {@link Iterable} represents a consistent view of neighbors, respecting deletions made
     * in the current context.
     *
     * @return an {@link Iterable} of the merged neighbors, excluding those marked as deleted. This method never returns
     * {@code null}.
     */
    @Override
    // getParent() is overridden below to always return this.parent, which is non-null by construction (see the
    // constructor's contract); SpotBugs's NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE analysis appears to fall back to
    // the wider @Nullable contract declared by NeighborsChangeSet.getParent() rather than this narrowed override.
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public Iterable<N> merge() {
        return Iterables.filter(getParent().merge(),
                current -> !deletedNeighborsPrimaryKeys.contains(Objects.requireNonNull(current).getPrimaryKey()));
    }

    /**
     * Writes the delta of changes for a given node to the storage layer.
     * <p>
     * This implementation first delegates to the parent's {@code writeDelta} method to handle its changes, but modifies
     * the predicate to exclude any neighbors that are marked for deletion in this delta.
     * <p>
     * It then iterates through the set of locally deleted neighbor primary keys. For each key that matches the supplied
     * {@code tuplePredicate}, it instructs the {@link InliningStorageAdapter} to delete the corresponding neighbor
     * relationship for the given {@code node}.
     *
     * @param storageAdapter the storage adapter to which the changes are written
     * @param quantizer the quantizer to use
     * @param transaction the transaction context for the write operations
     * @param layer the layer index where the write operations should occur
     * @param node the node for which the delta is being written
     * @param tuplePredicate a predicate to filter which neighbor tuples should be processed;
     *        only deletions matching this predicate will be written
     */
    @Override
    // See the comment on merge() above: getParent() always returns this.parent (non-null) in this class.
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public void writeDelta(final InliningStorageAdapter storageAdapter, final Transaction transaction,
                           final Quantizer quantizer, final int layer, final AbstractNode<N> node,
                           final Predicate<Tuple> tuplePredicate) {
        Verify.verify(node.isInliningNode());
        getParent().writeDelta(storageAdapter, transaction, quantizer, layer, node,
                tuplePredicate.and(tuple -> !deletedNeighborsPrimaryKeys.contains(tuple)));

        for (final Tuple deletedNeighborPrimaryKey : deletedNeighborsPrimaryKeys) {
            if (tuplePredicate.test(deletedNeighborPrimaryKey)) {
                storageAdapter.deleteNeighbor(transaction, layer, node.asInliningNode(), deletedNeighborPrimaryKey);
                if (logger.isTraceEnabled()) {
                    logger.trace("deleted neighbor of layer={}, primaryKey={} targeting primaryKey={}",
                            layer, node.getPrimaryKey(), deletedNeighborPrimaryKey);
                }
            }
        }
    }
}
