/*
 * CompactStorageAdapter.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * TODO.
 */
class InliningStorageAdapter extends AbstractStorageAdapter<NodeReferenceWithVector> implements StorageAdapter<NodeReferenceWithVector> {
    public InliningStorageAdapter(@Nonnull final HNSW.Config config,
                                  @Nonnull final NodeFactory<NodeReferenceWithVector> nodeFactory,
                                  @Nonnull final Subspace subspace,
                                  @Nonnull final OnWriteListener onWriteListener,
                                  @Nonnull final OnReadListener onReadListener) {
        super(config, nodeFactory, subspace, onWriteListener, onReadListener);
    }

    @Nonnull
    @Override
    public StorageAdapter<NodeReference> asCompactStorageAdapter() {
        throw new IllegalStateException("cannot call this method on an inlining storage adapter");
    }

    @Nonnull
    @Override
    public StorageAdapter<NodeReferenceWithVector> asInliningStorageAdapter() {
        return this;
    }

    @Nonnull
    protected CompletableFuture<Node<NodeReferenceWithVector>> fetchNodeInternal(@Nonnull final ReadTransaction readTransaction,
                                                                                 final int layer,
                                                                                 @Nonnull final Tuple primaryKey) {
        final byte[] rangeKey = getNodeKey(layer, primaryKey);

        return AsyncUtil.collect(readTransaction.getRange(Range.startsWith(rangeKey),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL), readTransaction.getExecutor())
                .thenApply(keyValues -> {
                    final OnReadListener onReadListener = getOnReadListener();

                    final ImmutableList.Builder<NodeReferenceWithVector> nodeReferencesWithVectorBuilder = ImmutableList.builder();
                    for (final KeyValue keyValue : keyValues) {
                        final byte[] key = keyValue.getKey();
                        final byte[] value = keyValue.getValue();
                        onReadListener.onKeyValueRead(key, value);
                        final Tuple neighborKeyTuple = getDataSubspace().unpack(key);
                        final Tuple neighborValueTuple = Tuple.fromBytes(value);

                        final Tuple neighborPrimaryKey = neighborKeyTuple.getNestedTuple(2); // neighbor primary key
                        final Vector<Half> neighborVector = StorageAdapter.vectorFromTuple(neighborValueTuple); // the entire value is the vector
                        nodeReferencesWithVectorBuilder.add(new NodeReferenceWithVector(neighborPrimaryKey, neighborVector));
                    }

                    final Node<NodeReferenceWithVector> node =
                            getNodeFactory().create(primaryKey, null, nodeReferencesWithVectorBuilder.build());
                    onReadListener.onNodeRead(node);
                    return node;
                });
    }

    @Override
    public void writeNodeInternal(@Nonnull final Transaction transaction, @Nonnull final Node<NodeReferenceWithVector> node,
                                  final int layer, @Nonnull final NeighborsChangeSet<NodeReferenceWithVector> neighborsChangeSet) {
        final InliningNode inliningNode = node.asInliningNode();

        neighborsChangeSet.writeDelta(this, transaction, layer, inliningNode, t -> true);
        getOnWriteListener().onNodeWritten(layer, node);
    }

    @Nonnull
    private byte[] getNodeKey(final int layer, @Nonnull final Tuple primaryKey) {
        return getDataSubspace().pack(Tuple.from(layer, primaryKey));
    }

    public void writeNeighbor(@Nonnull final Transaction transaction, final int layer,
                              @Nonnull final Node<NodeReferenceWithVector> node, @Nonnull final NodeReferenceWithVector neighbor) {
        transaction.set(getNeighborKey(layer, node, neighbor.getPrimaryKey()),
                StorageAdapter.tupleFromVector(neighbor.getVector()).pack());
        getOnWriteListener().onNeighborWritten(layer, node, neighbor);
    }

    public void deleteNeighbor(@Nonnull final Transaction transaction, final int layer,
                               @Nonnull final Node<NodeReferenceWithVector> node, @Nonnull final Tuple neighborPrimaryKey) {
        transaction.clear(getNeighborKey(layer, node, neighborPrimaryKey));
        getOnWriteListener().onNeighborDeleted(layer, node, neighborPrimaryKey);
    }

    @Nonnull
    private byte[] getNeighborKey(final int layer,
                                  @Nonnull final Node<NodeReferenceWithVector> node,
                                  @Nonnull final Tuple neighborPrimaryKey) {
        return getDataSubspace().pack(Tuple.from(layer, node.getPrimaryKey(), neighborPrimaryKey));
    }
}
