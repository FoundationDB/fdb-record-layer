/*
 * PlannerGraph.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.google.common.collect.ImmutableMap;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

/**
 * The planner graph class. Objects of this class are computed by {@link InternalPlannerGraphProperty},
 * i.e., they get computed by walking a {@link PlannerExpression} DAG.
 *
 * Once computed, the property is immutable.
 *
 * @param <N> node type
 * @param <E> edge type
 */
@SuppressWarnings("UnstableApiUsage") // Guava Graph API
public class PlannerGraph<N extends PlannerGraph.AbstractNode, E extends PlannerGraph.AbstractEdge> {

    /**
     * The root of this graph.
     */
    @Nonnull
    private final N root;

    /**
     * The underlying network graph representation.
     */
    @Nonnull
    private final ImmutableNetwork<N, E> network;

    /**
     * Builder class for planner graph. Used during computation of the planner expression property.
     * Note that each PlannerGraph will have a root which is mandatory.
     * @param <N> node type
     * @param <E> edge type
     */
    public static class PlannerGraphBuilder<N extends AbstractNode, E extends AbstractEdge> {
        @Nonnull
        final N root;
        @Nonnull
        final MutableNetwork<N, E> network;

        private PlannerGraphBuilder(final N root) {
            this.root = root;
            this.network =
                    NetworkBuilder.directed()
                            .allowsParallelEdges(true)
                            .allowsSelfLoops(true)
                            .build();
            addNode(root);
        }

        @Nonnull
        public N getRoot() {
            return root;
        }

        @Nonnull
        public PlannerGraphBuilder<N, E> addNode(final N node) {
            network.addNode(node);
            return this;
        }

        @Nonnull
        public PlannerGraphBuilder<N, E> addEdge(final N source, final N target, final E edge) {
            network.addEdge(source, target, edge);
            return this;
        }

        @Nonnull
        public PlannerGraphBuilder<N, E> addGraph(final PlannerGraph<N, E> other) {
            final ImmutableNetwork<N, E> otherNetwork = other.network;

            // Starting from the root node, stop at any edge that leads to a node that is already in this network using
            // classic breadth-first search.
            final Queue<N> queue = new ArrayDeque<>();

            if (!network.nodes().contains(other.root)) {
                addNode(other.root);
                queue.add(other.root);
            }

            while (!queue.isEmpty()) {
                final N currentNode = queue.remove();
                for (final E edge : otherNetwork.inEdges(currentNode)) {
                    final EndpointPair<N> endpointPair = otherNetwork.incidentNodes(edge);
                    final N nodeU = endpointPair.nodeU();
                    if (!network.nodes().contains(nodeU)) {
                        addNode(nodeU);
                        queue.add(nodeU);
                    }
                    addEdge(nodeU, endpointPair.nodeV(), edge);
                }
            }
            return this;
        }

        @Nonnull
        public PlannerGraph<N, E> build() {
            return new PlannerGraph<>(root, network);
        }
    }

    /**
     * Node class functioning as parent for any nodes in the network.
     */
    public abstract static class AbstractNode {
        @Nonnull
        final String name;
        @Nullable
        final String expression;

        public AbstractNode(final String name) {
            this(name, null);
        }

        public AbstractNode(final String name, @Nullable final String expression) {
            this.name = name;
            this.expression = expression;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @Nullable
        public String getExpression() {
            return expression;
        }

        @Nonnull
        public Map<String, String> getAttributes() {
            final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            Optional.ofNullable(getLabel())
                    .ifPresent(label -> builder.put("label", label));
            return builder.build();
        }

        @Nullable
        public abstract String getLabel();
    }

    /**
     * Edge class.
     */
    public abstract static class AbstractEdge {
        public Map<String, String> getAttributes() {
            final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            Optional.ofNullable(getLabel())
                    .ifPresent(label -> builder.put("label", label));
            return builder.build();
        }

        @Nullable
        public abstract String getLabel();
    }

    @Nonnull
    public static <N extends AbstractNode, E extends AbstractEdge> PlannerGraphBuilder<N, E> builder(final N root) {
        return new PlannerGraphBuilder<>(root);
    }

    /**
     * Private constructor. Objects of this class are built by a builder.
     */
    private PlannerGraph(final N root,
                         final Network<N, E> network) {
        this.root = root;
        final MutableNetwork<AbstractNode, AbstractEdge> mutableNetwork =
                NetworkBuilder.directed()
                        .allowsParallelEdges(true)
                        .allowsSelfLoops(true)
                        .build();
        mutableNetwork.addNode(root);
        this.network =
                ImmutableNetwork.copyOf(network);
    }

    @Nonnull
    public N getRoot() {
        return root;
    }

    @Nonnull
    public ImmutableNetwork<N, E> getNetwork() {
        return network;
    }
}
