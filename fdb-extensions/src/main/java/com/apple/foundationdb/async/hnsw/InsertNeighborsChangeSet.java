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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * TODO.
 */
class InsertNeighborsChangeSet<N extends NodeReference> implements NeighborsChangeSet<N> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(InsertNeighborsChangeSet.class);

    @Nonnull
    private final NeighborsChangeSet<N> parent;

    @Nonnull
    private final Map<Tuple, N> insertedNeighborsMap;

    public InsertNeighborsChangeSet(@Nonnull final NeighborsChangeSet<N> parent,
                                    @Nonnull final List<N> insertedNeighbors) {
        this.parent = parent;
        final ImmutableMap.Builder<Tuple, N> insertedNeighborsMapBuilder = ImmutableMap.builder();
        for (final N insertedNeighbor : insertedNeighbors) {
            insertedNeighborsMapBuilder.put(insertedNeighbor.getPrimaryKey(), insertedNeighbor);
        }

        this.insertedNeighborsMap = insertedNeighborsMapBuilder.build();
    }

    @Nonnull
    @Override
    public NeighborsChangeSet<N> getParent() {
        return parent;
    }

    @Nonnull
    @Override
    public Iterable<N> merge() {
        return Iterables.concat(getParent().merge(), insertedNeighborsMap.values());
    }

    @Override
    public void writeDelta(@Nonnull final InliningStorageAdapter storageAdapter, @Nonnull final Transaction transaction,
                           final int layer, @Nonnull final Node<N> node, @Nonnull final Predicate<Tuple> tuplePredicate) {
        getParent().writeDelta(storageAdapter, transaction, layer, node,
                tuplePredicate.and(tuple -> !insertedNeighborsMap.containsKey(tuple)));

        for (final Map.Entry<Tuple, N> entry : insertedNeighborsMap.entrySet()) {
            final Tuple primaryKey = entry.getKey();
            if (tuplePredicate.test(primaryKey)) {
                storageAdapter.writeNeighbor(transaction, layer, node.asInliningNode(),
                        entry.getValue().asNodeReferenceWithVector());
                if (logger.isDebugEnabled()) {
                    logger.debug("inserted neighbor of primaryKey={} targeting primaryKey={}", node.getPrimaryKey(),
                            primaryKey);
                }
            }
        }
    }
}
