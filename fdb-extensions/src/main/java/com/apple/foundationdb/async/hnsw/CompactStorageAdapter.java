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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * TODO.
 */
class CompactStorageAdapter extends AbstractStorageAdapter<NodeReference> implements StorageAdapter<NodeReference> {
    public CompactStorageAdapter(@Nonnull final HNSW.Config config, @Nonnull final NodeFactory<NodeReference> nodeFactory,
                                 @Nonnull final Subspace subspace,
                                 @Nonnull final OnWriteListener onWriteListener,
                                 @Nonnull final OnReadListener onReadListener) {
        super(config, nodeFactory, subspace, onWriteListener, onReadListener);
    }

    @Nonnull
    @Override
    public StorageAdapter<NodeReference> asCompactStorageAdapter() {
        return this;
    }

    @Nonnull
    @Override
    public StorageAdapter<NodeReferenceWithVector> asInliningStorageAdapter() {
        throw new IllegalStateException("cannot call this method on a compact storage adapter");
    }

    @Nonnull
    @Override
    protected CompletableFuture<Node<NodeReference>> fetchNodeInternal(@Nonnull final ReadTransaction readTransaction,
                                                                       final int layer,
                                                                       @Nonnull final Tuple primaryKey) {
        final byte[] key = getDataSubspace().pack(Tuple.from(layer, primaryKey));

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        throw new IllegalStateException("cannot fetch node");
                    }

                    final Tuple nodeTuple = Tuple.fromBytes(valueBytes);
                    final Node<NodeReference> node = nodeFromTuples(primaryKey, nodeTuple);
                    final OnReadListener onReadListener = getOnReadListener();
                    onReadListener.onNodeRead(node);
                    onReadListener.onKeyValueRead(key, valueBytes);
                    return node;
                });
    }

    @Nonnull
    private Node<NodeReference> nodeFromTuples(@Nonnull final Tuple primaryKey,
                                               @Nonnull final Tuple valueTuple) {
        final NodeKind nodeKind = NodeKind.fromSerializedNodeKind((byte)valueTuple.getLong(0));
        Verify.verify(nodeKind == NodeKind.COMPACT);

        final Tuple vectorTuple;
        final Tuple neighborsTuple;

        vectorTuple = valueTuple.getNestedTuple(1);
        neighborsTuple = valueTuple.getNestedTuple(2);
        return compactNodeFromTuples(primaryKey, vectorTuple, neighborsTuple);
    }

    @Nonnull
    private Node<NodeReference> compactNodeFromTuples(@Nonnull final Tuple primaryKey,
                                                      @Nonnull final Tuple vectorTuple,
                                                      @Nonnull final Tuple neighborsTuple) {
        final Vector<Half> vector = StorageAdapter.vectorFromTuple(vectorTuple);
        final List<NodeReference> nodeReferences = Lists.newArrayListWithExpectedSize(neighborsTuple.size());

        for (final Object neighborObject : neighborsTuple) {
            final Tuple neighborTuple = (Tuple)neighborObject;
            nodeReferences.add(new NodeReference(neighborTuple));
        }

        return getNodeFactory().create(primaryKey, vector, nodeReferences);
    }


    @Override
    public void writeNode(@Nonnull final Transaction transaction, @Nonnull final Node<NodeReference> node,
                          final int layer, @Nonnull final NeighborsChangeSet<NodeReference> neighborsChangeSet) {
        final byte[] key = getDataSubspace().pack(Tuple.from(layer, node.getPrimaryKey()));

        final List<Object> nodeItems = Lists.newArrayListWithExpectedSize(4);
        nodeItems.add(NodeKind.COMPACT.getSerialized());
        final CompactNode compactNode = node.asCompactNode();
        nodeItems.add(StorageAdapter.tupleFromVector(compactNode.getVector()));

        final Iterable<NodeReference> neighbors = neighborsChangeSet.merge();

        final List<Tuple> neighborItems = Lists.newArrayList();
        for (final NodeReference neighborReference : neighbors) {
            neighborItems.add(neighborReference.getPrimaryKey());
        }
        nodeItems.add(Tuple.fromList(neighborItems));
        final Tuple nodeTuple = Tuple.fromList(nodeItems);

        transaction.set(key, nodeTuple.pack());
        getOnWriteListener().onNodeWritten(layer, node);
    }
}
