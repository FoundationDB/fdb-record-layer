/*
 * InsertNeighborsChangeSet.java
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Represents an immutable change set for the neighbors of a node in the HNSW graph, specifically
 * capturing the insertion of new neighbors.
 * <p>
 * This class layers new neighbors on top of a parent {@link NeighborsChangeSet}, allowing for a
 * layered representation of modifications. The changes are not applied to the database until
 * {@link #writeDelta} is called.
 *
 * @param <N> the type of the node reference, which must extend {@link NodeReference}
 */
class InsertNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(InsertNeighborsChangeSet.class);

    @Nonnull
    private final NeighborsChangeSet<N> parent;

    @Nonnull
    private final Map<Tuple, N> insertedNeighborsMap;

    /**
     * Creates a new {@code InsertNeighborsChangeSet}.
     * <p>
     * This constructor initializes the change set with its parent and a list of neighbors
     * to be inserted. It internally builds an immutable map of the inserted neighbors,
     * keyed by their primary key for efficient lookups.
     *
     * @param parent the parent {@link NeighborsChangeSet} on which this insertion is based.
     * @param insertedNeighbors the list of neighbors to be inserted.
     */
    public InsertNeighborsChangeSet(@Nonnull final NeighborsChangeSet<N> parent,
                                    @Nonnull final List<N> insertedNeighbors) {
        this.parent = parent;
        final ImmutableMap.Builder<Tuple, N> insertedNeighborsMapBuilder = ImmutableMap.builder();
        for (final N insertedNeighbor : insertedNeighbors) {
            insertedNeighborsMapBuilder.put(insertedNeighbor.getPrimaryKey(), insertedNeighbor);
        }

        this.insertedNeighborsMap = insertedNeighborsMapBuilder.build();
    }

    /**
     * Gets the parent {@code NeighborsChangeSet} from which this change set was derived.
     * @return the parent {@link NeighborsChangeSet}, which is never {@code null}.
     */
    @Nonnull
    @Override
    public NeighborsChangeSet<N> getParent() {
        return parent;
    }

    /**
     * Merges the neighbors from this level of the hierarchy with all neighbors from parent levels.
     * <p>
     * This is achieved by creating a combined view that includes the results of the parent's {@code #merge()} call and
     * the neighbors that have been inserted at the current level. The resulting {@code Iterable} provides a complete
     * set of neighbors from this node and all its ancestors.
     * @return a non-null {@code Iterable} containing all neighbors from this node and its ancestors.
     */
    @Nonnull
    @Override
    public Iterable<N> merge() {
        return Iterables.concat(getParent().merge(), insertedNeighborsMap.values());
    }

    /**
     * Writes the delta of this layer to the specified storage adapter.
     * <p>
     * This implementation first delegates to the parent to write its delta, but excludes any neighbors that have been
     * newly inserted in the current context (i.e., those in {@code insertedNeighborsMap}). It then iterates through its
     * own newly inserted neighbors. For each neighbor that satisfies the given {@code tuplePredicate}, it writes the
     * neighbor relationship to storage via the {@link InliningStorageAdapter}.
     *
     * @param storageAdapter the storage adapter to write to; must not be null
     * @param transaction the transaction context for the write operation; must not be null
     * @param layer the layer index to write the data to
     * @param node the source node for which the neighbor delta is being written; must not be null
     * @param tuplePredicate a predicate to filter which neighbor tuples should be written; must not be null
     */
    @Override
    public void writeDelta(@Nonnull final InliningStorageAdapter storageAdapter, @Nonnull final Transaction transaction,
                           @Nonnull final Quantizer quantizer, final int layer, @Nonnull final AbstractNode<N> node,
                           @Nonnull final Predicate<Tuple> tuplePredicate) {
        getParent().writeDelta(storageAdapter, transaction, quantizer, layer, node,
                tuplePredicate.and(tuple -> !insertedNeighborsMap.containsKey(tuple)));

        for (final Map.Entry<Tuple, N> entry : insertedNeighborsMap.entrySet()) {
            final Tuple primaryKey = entry.getKey();
            if (tuplePredicate.test(primaryKey)) {
                storageAdapter.writeNeighbor(transaction, quantizer, layer, node.asInliningNode(),
                        entry.getValue().asNodeReferenceWithVector());
                if (logger.isTraceEnabled()) {
                    logger.trace("inserted neighbor of primaryKey={} targeting primaryKey={}", node.getPrimaryKey(),
                            primaryKey);
                }
            }
        }
    }
}
