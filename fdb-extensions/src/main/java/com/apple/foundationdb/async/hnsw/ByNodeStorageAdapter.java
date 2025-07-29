/*
 * ByNodeStorageAdapter.java
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * TODO.
 */
class ByNodeStorageAdapter extends AbstractStorageAdapter implements StorageAdapter {
    public ByNodeStorageAdapter(@Nonnull final HNSW.Config config, @Nonnull final Subspace subspace,
                                @Nonnull final OnWriteListener onWriteListener,
                                @Nonnull final OnReadListener onReadListener) {
        super(config, subspace, onWriteListener, onReadListener);
    }

    @Nonnull
    @Override
    public NodeFactory<? extends NodeReference> getNodeFactory(final int layer) {
        return layer > 0 ? InliningNode.factory() : CompactNode.factory();
    }

    @Override
    public CompletableFuture<EntryNodeReference> fetchEntryNodeReference(@Nonnull final ReadTransaction readTransaction) {
        final byte[] key = getEntryNodeSubspace().pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        return null; // not a single node in the index
                    }
                    final OnReadListener onReadListener = getOnReadListener();
                    onReadListener.onKeyValueRead(key, valueBytes);

                    final Tuple entryTuple = Tuple.fromBytes(valueBytes);
                    final int lMax = (int)entryTuple.getLong(0);
                    final Tuple primaryKey = entryTuple.getNestedTuple(1);
                    final Tuple vectorTuple = entryTuple.getNestedTuple(2);
                    return new EntryNodeReference(primaryKey, StorageAdapter.vectorFromTuple(vectorTuple), lMax);
                });
    }


    @Override
    public void writeEntryNodeReference(@Nonnull final Transaction transaction,
                                        @Nonnull final EntryNodeReference entryNodeReference) {
        transaction.set(getEntryNodeSubspace().pack(),
                Tuple.from(entryNodeReference.getLayer(),
                        entryNodeReference.getPrimaryKey(),
                        StorageAdapter.tupleFromVector(entryNodeReference.getVector())).pack());
    }


    @Nonnull
    @Override
    protected <R extends NodeReference> CompletableFuture<Node<R>> fetchNodeInternal(@Nonnull final NodeFactory<R> nodeFactory,
                                                                                     @Nonnull final ReadTransaction readTransaction,
                                                                                     final int layer,
                                                                                     @Nonnull final Tuple primaryKey) {
        final byte[] key = getDataSubspace().pack(Tuple.from(layer, primaryKey));

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        throw new IllegalStateException("cannot fetch node");
                    }

                    final Tuple nodeTuple = Tuple.fromBytes(valueBytes);
                    final Node<R> node = Node.nodeFromTuples(nodeFactory, primaryKey, nodeTuple);
                    final OnReadListener onReadListener = getOnReadListener();
                    onReadListener.onNodeRead(node);
                    onReadListener.onKeyValueRead(key, valueBytes);
                    return node;
                });
    }

    @Override
    public <N extends NodeReference> void writeNode(@Nonnull Transaction transaction, @Nonnull final Node<N> node,
                                                    final int layer) {
        final byte[] key = getDataSubspace().pack(Tuple.from(layer, node.getPrimaryKey()));
        transaction.set(key, node.toTuple().pack());
        getOnWriteListener().onNodeWritten(node);
    }
}
