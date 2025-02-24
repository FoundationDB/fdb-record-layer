/*
 * StableDirectedMultiNetworkConnections.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.google.common.graph;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.graph.GraphConstants.INNER_CAPACITY;
import static com.google.common.graph.GraphConstants.INNER_LOAD_FACTOR;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import com.google.errorprone.annotations.concurrent.LazyInit;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;

/**
 * A copy of {@link DirectedMultiNetworkConnections} that constructs the underlying edge maps with
 * implementations that provide a stable iteration order.
 *
 * @param <N> Node parameter type
 * @param <E> Edge parameter type
 */
@ElementTypesAreNonnullByDefault
final class StableDirectedMultiNetworkConnections<N, E> extends AbstractDirectedNetworkConnections<N, E> {

    @CheckForNull @LazyInit private transient Reference<Multiset<N>> predecessorsReference;

    @CheckForNull @LazyInit private transient Reference<Multiset<N>> successorsReference;

    static <N, E> StableDirectedMultiNetworkConnections<N, E> of() {
        return new StableDirectedMultiNetworkConnections<>(
                new LinkedHashMap<>(INNER_CAPACITY, INNER_LOAD_FACTOR),
                new LinkedHashMap<>(INNER_CAPACITY, INNER_LOAD_FACTOR),
                0);
    }

    static <N, E> StableDirectedMultiNetworkConnections<N, E> ofImmutable(
            Map<E, N> inEdges, Map<E, N> outEdges, int selfLoopCount) {
        return new StableDirectedMultiNetworkConnections<>(
                ImmutableMap.copyOf(inEdges), ImmutableMap.copyOf(outEdges), selfLoopCount);
    }

    private StableDirectedMultiNetworkConnections(
            Map<E, N> inEdges, Map<E, N> outEdges, int selfLoopCount) {
        super(inEdges, outEdges, selfLoopCount);
    }

    @Override
    public Set<N> predecessors() {
        return Collections.unmodifiableSet(predecessorsMultiset().elementSet());
    }

    private Multiset<N> predecessorsMultiset() {
        Multiset<N> predecessors = getReference(predecessorsReference);
        if (predecessors == null) {
            predecessors = LinkedHashMultiset.create(inEdgeMap.values());
            predecessorsReference = new SoftReference<>(predecessors);
        }
        return predecessors;
    }

    @Override
    public Set<N> successors() {
        return Collections.unmodifiableSet(successorsMultiset().elementSet());
    }

    private Multiset<N> successorsMultiset() {
        Multiset<N> successors = getReference(successorsReference);
        if (successors == null) {
            successors = LinkedHashMultiset.create(outEdgeMap.values());
            successorsReference = new SoftReference<>(successors);
        }
        return successors;
    }

    @Override
    public Set<E> edgesConnecting(N node) {
        return new MultiEdgesConnecting<>(outEdgeMap, node) {
            @Override
            public int size() {
                return successorsMultiset().count(node);
            }
        };
    }

    @Override
    public N removeInEdge(E edge, boolean isSelfLoop) {
        N node = super.removeInEdge(edge, isSelfLoop);
        Multiset<N> predecessors = getReference(predecessorsReference);
        if (predecessors != null) {
            checkState(predecessors.remove(node));
        }
        return node;
    }

    @Override
    public N removeOutEdge(E edge) {
        N node = super.removeOutEdge(edge);
        Multiset<N> successors = getReference(successorsReference);
        if (successors != null) {
            checkState(successors.remove(node));
        }
        return node;
    }

    @Override
    public void addInEdge(E edge, N node, boolean isSelfLoop) {
        super.addInEdge(edge, node, isSelfLoop);
        Multiset<N> predecessors = getReference(predecessorsReference);
        if (predecessors != null) {
            checkState(predecessors.add(node));
        }
    }

    @Override
    public void addOutEdge(E edge, N node) {
        super.addOutEdge(edge, node);
        Multiset<N> successors = getReference(successorsReference);
        if (successors != null) {
            checkState(successors.add(node));
        }
    }

    @CheckForNull
    private static <T> T getReference(@CheckForNull Reference<T> reference) {
        return (reference == null) ? null : reference.get();
    }
}
