/*
 * StableStandardMutableNetwork.java
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

import com.google.common.base.Verify;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.graph.GraphConstants.PARALLEL_EDGES_NOT_ALLOWED;
import static com.google.common.graph.GraphConstants.REUSING_EDGE;
import static com.google.common.graph.GraphConstants.SELF_LOOPS_NOT_ALLOWED;

/**
 * This is mostly a copy of {@link StandardMutableNetwork} that delegates the construction of its underlying
 * connections to a class providing stable iteration order over them.
 *
 * @param <N> Node parameter type
 * @param <E> Edge parameter type
 */
@SuppressWarnings("UnstableApiUsage")
@ElementTypesAreNonnullByDefault
public final class StableStandardMutableNetwork<N, E> implements MutableNetwork<N, E> {
    private final StandardMutableNetwork<N, E> standardMutableNetwork;

    public StableStandardMutableNetwork(NetworkBuilder<? super N, ? super E> builder) {
        standardMutableNetwork = new StandardMutableNetwork<>(builder);
    }

    public boolean addNode(N node) {
        checkNotNull(node, "node");
        if (standardMutableNetwork.containsNode(node)) {
            return false;
        }
        addNodeInternal(node);
        return true;
    }

    public boolean addEdge(N nodeU, N nodeV, E edge) {
        checkNotNull(nodeU, "nodeU");
        checkNotNull(nodeV, "nodeV");
        checkNotNull(edge, "edge");

        if (standardMutableNetwork.containsEdge(edge)) {
            EndpointPair<N> existingIncidentNodes = incidentNodes(edge);
            EndpointPair<N> newIncidentNodes = EndpointPair.of(this, nodeU, nodeV);
            checkArgument(
                    existingIncidentNodes.equals(newIncidentNodes),
                    REUSING_EDGE,
                    edge,
                    existingIncidentNodes,
                    newIncidentNodes);
            return false;
        }
        NetworkConnections<N, E> connectionsU = standardMutableNetwork.nodeConnections.get(nodeU);
        if (!allowsParallelEdges()) {
            checkArgument(
                    !(connectionsU != null && connectionsU.successors().contains(nodeV)),
                    PARALLEL_EDGES_NOT_ALLOWED,
                    nodeU,
                    nodeV);
        }
        boolean isSelfLoop = nodeU.equals(nodeV);
        if (!allowsSelfLoops()) {
            checkArgument(!isSelfLoop, SELF_LOOPS_NOT_ALLOWED, nodeU);
        }

        if (connectionsU == null) {
            connectionsU = addNodeInternal(nodeU);
        }
        connectionsU.addOutEdge(edge, nodeV);
        NetworkConnections<N, E> connectionsV = standardMutableNetwork.nodeConnections.get(nodeV);
        if (connectionsV == null) {
            connectionsV = addNodeInternal(nodeV);
        }
        connectionsV.addInEdge(edge, nodeU, isSelfLoop);
        standardMutableNetwork.edgeToReferenceNode.put(edge, nodeU);
        return true;
    }

    @CanIgnoreReturnValue
    private NetworkConnections<N, E> addNodeInternal(N node) {
        NetworkConnections<N, E> connections = newConnections();
        checkState(standardMutableNetwork.nodeConnections.put(node, connections) == null);
        return connections;
    }

    @Nonnull
    private NetworkConnections<N, E> newConnections() {
        Verify.verify(isDirected() && allowsParallelEdges(),
                "Only directed with parallel edges network is supported");
        return StableDirectedMultiNetworkConnections.of();
    }

    public Graph<N> asGraph() {
        return standardMutableNetwork.asGraph();
    }

    public int degree(N node) {
        return standardMutableNetwork.degree(node);
    }

    public int inDegree(N node) {
        return standardMutableNetwork.inDegree(node);
    }

    public int outDegree(N node) {
        return standardMutableNetwork.outDegree(node);
    }

    public Set<E> adjacentEdges(E edge) {
        return standardMutableNetwork.adjacentEdges(edge);
    }

    public Set<E> edgesConnecting(EndpointPair<N> endpoints) {
        return standardMutableNetwork.edgesConnecting(endpoints);
    }

    public Optional<E> edgeConnecting(N nodeU, N nodeV) {
        return standardMutableNetwork.edgeConnecting(nodeU, nodeV);
    }

    public Optional<E> edgeConnecting(EndpointPair<N> endpoints) {
        return standardMutableNetwork.edgeConnecting(endpoints);
    }

    @CheckForNull
    public E edgeConnectingOrNull(N nodeU, N nodeV) {
        return standardMutableNetwork.edgeConnectingOrNull(nodeU, nodeV);
    }

    @CheckForNull
    public E edgeConnectingOrNull(EndpointPair<N> endpoints) {
        return standardMutableNetwork.edgeConnectingOrNull(endpoints);
    }

    public boolean hasEdgeConnecting(N nodeU, N nodeV) {
        return standardMutableNetwork.hasEdgeConnecting(nodeU, nodeV);
    }

    public boolean hasEdgeConnecting(EndpointPair<N> endpoints) {
        return standardMutableNetwork.hasEdgeConnecting(endpoints);
    }

    public Set<N> nodes() {
        return standardMutableNetwork.nodes();
    }

    public Set<E> edges() {
        return standardMutableNetwork.edges();
    }

    public boolean isDirected() {
        return standardMutableNetwork.isDirected();
    }

    public boolean allowsParallelEdges() {
        return standardMutableNetwork.allowsParallelEdges();
    }

    public boolean allowsSelfLoops() {
        return standardMutableNetwork.allowsSelfLoops();
    }

    public ElementOrder<N> nodeOrder() {
        return standardMutableNetwork.nodeOrder();
    }

    public ElementOrder<E> edgeOrder() {
        return standardMutableNetwork.edgeOrder();
    }

    public Set<E> incidentEdges(N node) {
        return standardMutableNetwork.incidentEdges(node);
    }

    public EndpointPair<N> incidentNodes(E edge) {
        return standardMutableNetwork.incidentNodes(edge);
    }

    public Set<N> adjacentNodes(N node) {
        return standardMutableNetwork.adjacentNodes(node);
    }

    public Set<E> edgesConnecting(N nodeU, N nodeV) {
        return standardMutableNetwork.edgesConnecting(nodeU, nodeV);
    }

    public Set<E> inEdges(N node) {
        return standardMutableNetwork.inEdges(node);
    }

    public Set<E> outEdges(N node) {
        return standardMutableNetwork.outEdges(node);
    }

    public Set<N> predecessors(N node) {
        return standardMutableNetwork.predecessors(node);
    }

    public Set<N> successors(N node) {
        return standardMutableNetwork.successors(node);
    }

    public boolean addEdge(EndpointPair<N> endpoints, E edge) {
        return standardMutableNetwork.addEdge(endpoints, edge);
    }

    public boolean removeNode(N node) {
        return standardMutableNetwork.removeNode(node);
    }

    public boolean removeEdge(E edge) {
        return standardMutableNetwork.removeEdge(edge);
    }
}
