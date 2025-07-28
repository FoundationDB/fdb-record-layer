/*
 * Node.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * TODO.
 * @param <R> neighbor type
 */
public interface Node<R extends NodeReference> {
    @Nonnull
    Tuple getPrimaryKey();

    @Nonnull
    List<R> getNeighbors();

    @Nonnull
    R getNeighbor(int index);

    /**
     * Return the kind of the node, i.e. {@link NodeKind#COMPACT} or {@link NodeKind#INLINING}.
     * @return the kind of this node as a {@link NodeKind}
     */
    @Nonnull
    NodeKind getKind();

    @Nonnull
    CompactNode asCompactNode();

    @Nonnull
    InliningNode asInliningNode();

    NodeFactory<R> sameCreator();

    @Nonnull
    Tuple toTuple();

    @Nonnull
    static <N extends NodeReference> Node<N> nodeFromTuples(@Nonnull final NodeFactory<N> creator,
                                                            @Nonnull final Tuple primaryKey,
                                                            @Nonnull final Tuple valueTuple) {
        final NodeKind nodeKind = NodeKind.fromSerializedNodeKind((byte)valueTuple.getLong(0));
        final Tuple vectorTuple;
        final Tuple neighborsTuple;

        switch (nodeKind) {
            case COMPACT:
                vectorTuple = valueTuple.getNestedTuple(1);
                neighborsTuple = valueTuple.getNestedTuple(2);
                return compactNodeFromTuples(creator, primaryKey, vectorTuple, neighborsTuple);
            case INLINING:
                neighborsTuple = valueTuple.getNestedTuple(1);
                return inliningNodeFromTuples(creator, primaryKey, neighborsTuple);
            default:
                throw new IllegalStateException("unknown node kind");
        }
    }

    @Nonnull
    static <N extends NodeReference> Node<N> compactNodeFromTuples(@Nonnull final NodeFactory<N> creator,
                                                                   @Nonnull final Tuple primaryKey,
                                                                   @Nonnull final Tuple vectorTuple,
                                                                   @Nonnull final Tuple neighborsTuple) {
        final Vector<Half> vector = StorageAdapter.vectorFromTuple(vectorTuple);

        List<NodeReference> nodeReferences = Lists.newArrayListWithExpectedSize(neighborsTuple.size());

        for (final Object neighborObject : neighborsTuple) {
            final Tuple neighborTuple = (Tuple)neighborObject;
            nodeReferences.add(new NodeReference(neighborTuple));
        }

        return creator.create(NodeKind.COMPACT, primaryKey, vector, nodeReferences);
    }

    @Nonnull
    static <N extends NodeReference> Node<N> inliningNodeFromTuples(@Nonnull final NodeFactory<N> creator,
                                                                    @Nonnull final Tuple primaryKey,
                                                                    @Nonnull final Tuple neighborsTuple) {
        List<NodeReferenceWithVector> neighborsWithVectors = Lists.newArrayListWithExpectedSize(neighborsTuple.size());

        for (final Object neighborObject : neighborsTuple) {
            final Tuple neighborTuple = (Tuple)neighborObject;
            final Tuple neighborPrimaryKey = neighborTuple.getNestedTuple(0);
            final Tuple neighborVectorTuple = neighborTuple.getNestedTuple(1);
            neighborsWithVectors.add(new NodeReferenceWithVector(neighborPrimaryKey,
                    StorageAdapter.vectorFromTuple(neighborVectorTuple)));
        }

        return creator.create(NodeKind.INLINING, primaryKey, null, neighborsWithVectors);
    }
}
