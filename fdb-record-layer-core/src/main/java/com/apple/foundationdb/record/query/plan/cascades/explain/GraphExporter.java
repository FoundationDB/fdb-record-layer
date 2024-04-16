/*
 * GraphExporter.java
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.Network;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Exports a graph into a DOT file.
 *
 * @param <N> the network node type
 * @param <E> the network edge type
 *
 */
@SuppressWarnings("UnstableApiUsage")
public abstract class GraphExporter<N, E> {
    @Nonnull private final ComponentIdProvider<N> vertexIDProvider;
    @Nonnull private final ComponentAttributeProvider<N> vertexAttributeProvider;
    @Nonnull private final ComponentIdProvider<E> edgeIDProvider;
    @Nonnull private final ComponentAttributeProvider<E> edgeAttributeProvider;
    @Nonnull private final ImmutableMap<String, Attribute> graphAttributes;
    @Nonnull private final Map<N, String> vertexIds;
    @Nonnull private final Map<E, String> edgeIds;
    @Nonnull private final ClusterProvider<N, E> clusterProvider;

    /**
     * Shorthand-type for the extended functional interface.
     * @param <T> any type
     */
    public interface ComponentIdProvider<T> extends Function<T, String> {
    }

    /**
     * Shorthand-type for the extended functional interface.
     * @param <T> any type
     */
    public interface ComponentAttributeProvider<T> extends Function<T, Map<String, Attribute>> {
    }

    /**
     * This class represents a cluster with the additional ability to further subdivide the cluster into sub-clusters.
     * @param <N> node class
     * @param <E> edge class
     */
    public static class Cluster<N, E> {
        @Nonnull private final ImmutableSet<N> nodes;
        @Nonnull final ComponentAttributeProvider<Cluster<N, E>> clusterAttributeProvider;
        @Nonnull private final ClusterProvider<N, E> nestedClusterProvider;

        public Cluster(@Nonnull final Set<N> nodes,
                       @Nonnull final ComponentAttributeProvider<Cluster<N, E>> clusterAttributeProvider,
                       @Nonnull final ClusterProvider<N, E> nestedClusterProvider) {
            this.nodes = ImmutableSet.copyOf(nodes);
            this.clusterAttributeProvider = clusterAttributeProvider;
            this.nestedClusterProvider = nestedClusterProvider;
        }

        @Nonnull
        public ImmutableSet<N> getNodes() {
            return nodes;
        }

        @Nonnull
        public ComponentAttributeProvider<Cluster<N, E>> getClusterAttributeProvider() {
            return clusterAttributeProvider;
        }

        @Nonnull
        public ClusterProvider<N, E> getNestedClusterProvider() {
            return nestedClusterProvider;
        }
    }

    /**
     * Shorthand-type for the extended functional interface.
     * @param <N> node type of network
     * @param <E> edge type of network
     */
    public interface ClusterProvider<N, E> extends BiFunction<Network<N, E>, Set<N>, Collection<Cluster<N, E>>> {
    }

    /**
     * Context class used while serializing the graph.
     */
    public class ExporterContext {
        @Nonnull
        private final ImmutableNetwork<N, E> network;
        @Nonnull
        private final PrintWriter printWriter;

        public ExporterContext(@Nonnull final ImmutableNetwork<N, E> network, @Nonnull final PrintWriter printWriter) {
            this.network = network;
            this.printWriter = printWriter;
        }

        @Nonnull
        public ImmutableNetwork<N, E> getNetwork() {
            return network;
        }

        @Nonnull
        public PrintWriter getPrintWriter() {
            return printWriter;
        }

        @Nonnull
        public GraphExporter<N, E> getExporter() {
            return GraphExporter.this;
        }
    }

    /**
     * Constructs a new GraphExporter object with the given ID, label, attribute, and graph ID
     * providers. Note that if a label provider conflicts with a label-supplying attribute provider,
     * the label provider is given precedence.
     *
     * @param vertexIDProvider for generating vertex IDs. Must not be null.
     * @param vertexAttributeProvider for generating vertex attributes. If null, vertex attributes
     *        will not be written to the file.
     * @param edgeIDProvider for generating edge IDs. Must not be null.
     * @param edgeAttributeProvider for generating edge attributes. If null, edge attributes will
     *        not be written to the file.
     * @param graphAttributes map of global graph-wide attributes
     * @param clusterProvider for partitioning the graph into clusters if warranted
     */
    protected GraphExporter(@Nonnull final ComponentIdProvider<N> vertexIDProvider,
                            @Nonnull final ComponentAttributeProvider<N> vertexAttributeProvider,
                            @Nonnull final ComponentIdProvider<E> edgeIDProvider,
                            @Nonnull final ComponentAttributeProvider<E> edgeAttributeProvider,
                            @Nonnull final Map<String, Attribute> graphAttributes,
                            @Nonnull final ClusterProvider<N, E> clusterProvider) {
        this.vertexIDProvider = vertexIDProvider;
        this.vertexAttributeProvider = vertexAttributeProvider;
        this.edgeIDProvider = edgeIDProvider;
        this.edgeAttributeProvider = edgeAttributeProvider;
        this.graphAttributes = ImmutableMap.copyOf(graphAttributes);
        this.vertexIds = new HashMap<>();
        this.edgeIds = new HashMap<>();
        this.clusterProvider = clusterProvider;
    }

    @Nonnull
    protected ComponentIdProvider<N> getVertexIDProvider() {
        return vertexIDProvider;
    }

    @Nonnull
    protected ComponentAttributeProvider<N> getVertexAttributeProvider() {
        return vertexAttributeProvider;
    }

    @Nonnull
    public ComponentIdProvider<E> getEdgeIDProvider() {
        return edgeIDProvider;
    }

    @Nonnull
    protected ComponentAttributeProvider<E> getEdgeAttributeProvider() {
        return edgeAttributeProvider;
    }

    @Nonnull
    protected ImmutableMap<String, Attribute> getGraphAttributes() {
        return graphAttributes;
    }

    @Nonnull
    protected Map<N, String> getVertexIds() {
        return vertexIds;
    }

    @Nonnull
    protected ClusterProvider<N, E> getClusterProvider() {
        return clusterProvider;
    }

    /**
     * Exports a network in DOT format.
     *
     * @param network the network to be exported
     * @param writer the context to which the network to be exported
     */
    public void exportGraph(@Nonnull final ImmutableNetwork<N, E> network,
                            @Nonnull final Writer writer) {
        final ExporterContext context = new ExporterContext(network,
                new PrintWriter(writer));

        renderHeader(context, network);

        // graph entities
        renderGraphAttributes(context, graphAttributes);
        renderNodes(context);
        renderEdges(context);
        renderClusters(context, clusterProvider.apply(network, network.nodes()));
        renderFooter(context);

        context.getPrintWriter().flush();
    }

    /**
     * Get a unique string for a node which adheres to the current language.
     * @param node a node
     * @return a unique identifier
     */
    @Nonnull
    protected String getVertexID(@Nonnull final N node) {
        return vertexIds.computeIfAbsent(node, n -> {
            final String idCandidate = vertexIDProvider.apply(node);

            if (!isValidId(idCandidate)) {
                throw new IllegalArgumentException(
                        "generated id '" + idCandidate + "'for vertex '" + node +
                        "' is not valid");
            }

            return idCandidate;
        });
    }

    /**
     * Get a unique string for a edge which adheres to the current language.
     * @param edge a edge
     * @return a unique identifier
     */
    @Nonnull
    protected String getEdgeID(@Nonnull final E edge) {
        return edgeIds.computeIfAbsent(edge, n -> {
            final String idCandidate = edgeIDProvider.apply(edge);
            if (!isValidId(idCandidate)) {
                throw new IllegalArgumentException(
                        "generated id '" + idCandidate + "'for edge '" + edge +
                        "' is not valid");
            }

            return idCandidate;
        });
    }

    protected abstract boolean isValidId(@Nonnull String idCandidate);

    /**
     * Render the header. To be implemented by subclass.
     *
     * @param context the context
     * @param graph the graph
     */
    protected abstract void renderHeader(@Nonnull ExporterContext context, @Nonnull ImmutableNetwork<N, E> graph);

    /**
     * Render the global graph attributes. To be implemented by subclass.
     *
     * @param context the context
     * @param attributes the attributes of the graph
     */
    protected abstract void renderGraphAttributes(@Nonnull ExporterContext context,
                                                  @Nonnull Map<String, Attribute> attributes);

    /**
     * Render all nodes in the given network.
     *
     * @param context the context
     */
    protected void renderNodes(@Nonnull final ExporterContext context) {
        final ImmutableNetwork<N, E> network = context.getNetwork();

        // vertex set
        for (final N n : network.nodes()) {
            renderNode(context,
                    n,
                    vertexAttributeProvider.apply(n));
        }
    }

    /**
     * Render a node. To be implemented by subclass.
     *
     * @param context the context
     * @param node the node to be rendered
     * @param attributes the attributes of the node
     */
    protected abstract void renderNode(@Nonnull ExporterContext context,
                                       @Nonnull N node,
                                       @Nonnull Map<String, Attribute> attributes);

    /**
     * Render all edges in a given network.
     * @param context the context to use
     */
    protected void renderEdges(@Nonnull final ExporterContext context) {
        final ImmutableNetwork<N, E> network = context.getNetwork();

        // edge set
        for (final E e : network.edges()) {
            final EndpointPair<N> endpointPair = network.incidentNodes(e);
            final N u = endpointPair.nodeU();
            final N v = endpointPair.nodeV();
            renderEdge(context,
                    network.isDirected(),
                    u,
                    v,
                    edgeAttributeProvider.apply(e));
        }
    }

    /**
     * Render an edge. To be implemented by subclass.
     *
     * @param context the context
     * @param isDirected true iff edge is directed
     * @param source the source node of the edge
     * @param target the target node of the edge
     * @param attributes the attributes of the edge
     */
    protected abstract void renderEdge(@Nonnull ExporterContext context,
                                       boolean isDirected,
                                       @Nonnull N source,
                                       @Nonnull N target,
                                       @Nonnull Map<String, Attribute> attributes);

    /**
     * Render all sub clusters in a given network.
     * @param context the context to use
     * @param clusters the clusters on this level
     */
    protected abstract void renderClusters(@Nonnull ExporterContext context,
                                           @Nonnull Collection<Cluster<N, E>> clusters);

    /**
     * Render the footer.
     *
     * @param context the context
     */
    protected abstract void renderFooter(@Nonnull ExporterContext context);
}
