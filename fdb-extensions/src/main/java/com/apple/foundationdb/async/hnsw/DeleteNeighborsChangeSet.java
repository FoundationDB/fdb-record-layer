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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

/**
 * TODO.
 */
class DeleteNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(DeleteNeighborsChangeSet.class);

    @Nonnull
    private final NeighborsChangeSet<N> parent;

    @Nonnull
    private final Set<Tuple /* primary key */> deletedNeighborsPrimaryKeys;

    public DeleteNeighborsChangeSet(@Nonnull final NeighborsChangeSet<N> parent,
                                    @Nonnull final Collection<Tuple> deletedNeighborsPrimaryKeys) {
        this.parent = parent;
        this.deletedNeighborsPrimaryKeys = ImmutableSet.copyOf(deletedNeighborsPrimaryKeys);
    }

    @Nonnull
    @Override
    public NeighborsChangeSet<N> getParent() {
        return parent;
    }

    @Nonnull
    @Override
    public Iterable<N> merge() {
        return Iterables.filter(getParent().merge(),
                current -> !deletedNeighborsPrimaryKeys.contains(current.getPrimaryKey()));
    }

    @Override
    public void writeDelta(@Nonnull final InliningStorageAdapter storageAdapter, @Nonnull final Transaction transaction,
                           final int layer, @Nonnull final Node<N> node, @Nonnull final Predicate<Tuple> tuplePredicate) {
        getParent().writeDelta(storageAdapter, transaction, layer, node,
                tuplePredicate.and(tuple -> !deletedNeighborsPrimaryKeys.contains(tuple)));

        for (final Tuple deletedNeighborPrimaryKey : deletedNeighborsPrimaryKeys) {
            if (tuplePredicate.test(deletedNeighborPrimaryKey)) {
                storageAdapter.deleteNeighbor(transaction, layer, node.asInliningNode(), deletedNeighborPrimaryKey);
                if (logger.isDebugEnabled()) {
                    logger.debug("deleted neighbor of primaryKey={} targeting primaryKey={}", node.getPrimaryKey(),
                            deletedNeighborPrimaryKey);
                }
            }
        }
    }
}
