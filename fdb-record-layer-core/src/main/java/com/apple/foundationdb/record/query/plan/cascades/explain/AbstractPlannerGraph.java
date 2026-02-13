/*
 * AbstractPlannerGraph.java
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

package com.apple.foundationdb.record.query.plan.cascades.explain;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graphs;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The planner graph class. Objects of this class are produced by {@link PlannerGraphVisitor},
 * i.e., they get computed by walking a {@link RelationalExpression} DAG.
 *
 * Once computed, the property is immutable.
 *
 * @param <N> node type
 * @param <E> edge type
 */
@SuppressWarnings("UnstableApiUsage") // Guava Graph API
public class AbstractPlannerGraph<N extends AbstractPlannerGraph.AbstractNode, E extends AbstractPlannerGraph.AbstractEdge> {
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
     * A mapping from objects the nodes in the graph represent to the actual nodes. This is computed by using the
     * network lazily.
     */
    private final Supplier<Map<Object, N>> reverseMapSupplier;

    /**
     * Builder class for planner graph. Used during computation of the planner expression property.
     * Note that each AbstractPlannerGraph will have a root which is mandatory.
     * @param <N> node type
     * @param <E> edge type
     * @param <B> self type for subclass builders
     */
    public abstract static class PlannerGraphBuilder<N extends AbstractNode, E extends AbstractEdge, B extends AbstractPlannerGraph<N, E>> {
        @Nonnull
        final N root;
        @Nonnull
        final MutableNetwork<N, E> network;

        protected PlannerGraphBuilder(@Nonnull final N root) {
            this.root = root;
            this.network =
                    NetworkBuilder.directed()
                            .allowsParallelEdges(true)
                            .allowsSelfLoops(true)
                            .build();
            network.addNode(root);
        }

        protected PlannerGraphBuilder(@Nonnull final AbstractPlannerGraph<N, E> original) {
            this.root = original.getRoot();
            this.network = Graphs.copyOf(original.getNetwork());
        }

        @Nonnull
        public N getRoot() {
            return root;
        }

        @Nonnull
        public MutableNetwork<N, E> getNetwork() {
            return network;
        }

        @Nonnull
        public PlannerGraphBuilder<N, E, B> addNode(@Nonnull final N node) {
            network.addNode(node);
            return this;
        }

        @Nonnull
        public PlannerGraphBuilder<N, E, B> addEdge(@Nonnull final N source,
                                                    @Nonnull final N target,
                                                    @Nonnull final E edge) {
            network.addEdge(source, target, edge);
            return this;
        }

        @Nonnull
        public PlannerGraphBuilder<N, E, B> addGraph(@Nonnull final AbstractPlannerGraph<N, E> other) {
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
        public abstract B build();
    }

    /**
     * Node class functioning as parent for all nodes in the network.
     */
    public abstract static class AbstractNode {
        @Nonnull
        final Object identity;

        @Nonnull
        final String name;
        @Nullable
        final List<String> details;

        @SuppressWarnings("unused") // used in clients
        protected AbstractNode(@Nonnull final Object identity, @Nonnull final String name) {
            this(identity, name, null);
        }

        /**
         * Overloaded constructor.
         * @param identity object identifying this node. Two nodes in the graph are considered equal if and only if their
         *        identity is equal (using reference equality)
         * @param name name of the object to be used to construct the label that is displayed
         * @param details list of strings with auxiliary information about the node. These strings are used
         *        to construct the label of the node depending on the exporter. This parameter is {@code Nullable}. If
         *        null, the label is omitted. If empty, it creates an empty list if the target format supports lists.
         */
        protected AbstractNode(@Nonnull final Object identity, @Nonnull final String name, @Nullable final List<String> details) {
            this.identity = identity;
            this.name = name;
            this.details = details == null
                           ? null
                           : ImmutableList.copyOf(details);
        }

        @Nonnull
        public Object getIdentity() {
            return identity;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @Nullable
        public List<String> getDetails() {
            return details;
        }

        @Nonnull
        public abstract Map<String, Attribute> getAttributes();

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AbstractNode)) {
                return false;
            }
            AbstractNode that = (AbstractNode)o;
            return getIdentity() == that.getIdentity();
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(getIdentity());
        }
    }

    /**
     * Edge class.
     */
    public abstract static class AbstractEdge {
        /**
         * Label of edge, can be null.
         */
        @Nullable
        private final String label;

        /**
         * The dependsOn set is the set of other sibling edges of this edge this edge depends on. This
         * is important for modelling things like join-order.
         */
        @Nonnull
        private final ImmutableSet<? extends AbstractEdge> dependsOn;

        @SuppressWarnings("unused")
        protected AbstractEdge(@Nonnull final Set<? extends AbstractEdge> dependsOn) {
            this(null, dependsOn);
        }

        protected AbstractEdge(@Nullable final String label, @Nonnull final Set<? extends AbstractEdge> dependsOn) {
            this.label = label;
            this.dependsOn = ImmutableSet.copyOf(dependsOn);
        }

        @Nullable
        public String getLabel() {
            return label;
        }

        @Nonnull
        public Set<? extends AbstractEdge> getDependsOn() {
            return dependsOn;
        }

        @Nonnull
        public abstract Map<String, Attribute> getAttributes();
    }

    /**
     * Protected constructor. Objects of this class are built by a builder.
     * @param root root of this graph
     * @param network a network describing the query execution plan rooted at {@code root}
     */
    protected AbstractPlannerGraph(@Nonnull final N root,
                                   @Nonnull final Network<N, E> network) {
        this.root = root;
        final MutableNetwork<AbstractNode, AbstractEdge> mutableNetwork =
                NetworkBuilder.directed()
                        .allowsParallelEdges(true)
                        .allowsSelfLoops(true)
                        .build();
        mutableNetwork.addNode(root);
        this.network =
                ImmutableNetwork.copyOf(network);
        this.reverseMapSupplier =
                Suppliers.memoize(() -> {
                    final IdentityHashMap<Object, N> reverseMap = new IdentityHashMap<>();
                    network.nodes()
                            .forEach(node -> reverseMap.put(node.getIdentity(), node));
                    return reverseMap;
                });
    }

    @Nonnull
    public N getRoot() {
        return root;
    }

    @Nonnull
    public ImmutableNetwork<N, E> getNetwork() {
        return network;
    }

    @Nullable
    public N getNodeForIdentity(final Object identity) {
        return reverseMapSupplier.get()
                .get(identity);
    }
}
