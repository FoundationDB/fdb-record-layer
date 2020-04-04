/*
 * DotExporter.java
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

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
    @Nonnull private final ComponentNameProvider<N> vertexIDProvider;
    @Nonnull private final ComponentAttributeProvider<N> vertexAttributeProvider;
    @Nonnull private final ComponentAttributeProvider<E> edgeAttributeProvider;
    @Nonnull private final ImmutableMap<String, String> graphAttributes;
    @Nonnull private final Map<N, String> vertexIds;
    @Nonnull private final ClusterProvider<N, E> clusterProvider;
    @Nonnull private final ComponentAttributeProvider<N> clusterAttributeProvider;

    /**
     * Shorthand-type for the extended functional interface.
     * @param <T> any type
     */
    public interface ComponentNameProvider<T> extends Function<T, String> {
    }

    /**
     * Shorthand-type for the extended functional interface.
     * @param <T> any type
     */
    public interface ComponentAttributeProvider<T> extends Function<T, Map<String, String>> {
    }

    /**
     * Shorthand-type for the extended functional interface.
     * @param <N> node type of network
     * @param <E> edge type of network
     */
    public interface ClusterProvider<N, E> extends Function<ImmutableNetwork<N, E>, Map<N, Set<N>>> {
    }

    /**
     * Constructs a new GraphExporter object with the given ID, label, attribute, and graph id
     * providers. Note that if a label provider conflicts with a label-supplying attribute provider,
     * the label provider is given precedence.
     *
     * @param vertexIDProvider for generating vertex IDs. Must not be null.
     * @param vertexAttributeProvider for generating vertex attributes. If null, vertex attributes
     *        will not be written to the file.
     * @param edgeAttributeProvider for generating edge attributes. If null, edge attributes will
     *        not be written to the file.
     * @param graphAttributes map of global graph-wide attributes
     * @param clusterProvider for partitioning the graph into clusters if warranted
     * @param clusterAttributeProvider for providing attributes to clusters
     */
    protected GraphExporter(@Nonnull final ComponentNameProvider<N> vertexIDProvider,
                            @Nonnull final ComponentAttributeProvider<N> vertexAttributeProvider,
                            @Nonnull final ComponentAttributeProvider<E> edgeAttributeProvider,
                            @Nonnull final Map<String, String> graphAttributes,
                            @Nonnull final ClusterProvider<N, E> clusterProvider,
                            @Nonnull final ComponentAttributeProvider<N> clusterAttributeProvider) {
        this.vertexIDProvider = vertexIDProvider;
        this.vertexAttributeProvider = vertexAttributeProvider;
        this.edgeAttributeProvider = edgeAttributeProvider;
        this.graphAttributes = ImmutableMap.copyOf(graphAttributes);
        this.vertexIds = new HashMap<>();
        this.clusterProvider = clusterProvider;
        this.clusterAttributeProvider = clusterAttributeProvider;
    }

    /**
     * Exports a network in DOT format.
     *
     * @param network the network to be exported
     * @param writer the writer to which the network to be exported
     */
    public void exportGraph(final ImmutableNetwork<N, E> network, Writer writer) {
        final PrintWriter out = new PrintWriter(writer);

        renderHeader(out, network);

        // graph attributes
        renderGraphAttributes(out, graphAttributes);

        // vertex set
        for (final N n : network.nodes()) {
            renderNode(out,
                    n,
                    vertexAttributeProvider.apply(n));
        }

        // edge set
        for (final E e : network.edges()) {
            final EndpointPair<N> endpointPair = network.incidentNodes(e);
            final N u = endpointPair.nodeU();
            final N v = endpointPair.nodeV();
            renderEdge(out,
                    network.isDirected(),
                    e,
                    u,
                    v,
                    edgeAttributeProvider.apply(e));
        }

        // render clusters
        final Map<N, Set<N>> clusterMap = clusterProvider.apply(network);
        int i = 1;
        for (final Entry<N, Set<N>> cluster : clusterMap.entrySet()) {
            renderCluster(out,
                    String.valueOf(i),
                    cluster.getKey(),
                    cluster.getValue(),
                    clusterAttributeProvider.apply(cluster.getKey()));
            i ++;
        }

        renderFooter(out);

        out.flush();
    }

    /**
     * Get a unique string for a node which adheres to the dot language.
     * @param node a node
     * @return a unique identifier
     */
    protected String getVertexID(final N node) {
        return vertexIds.computeIfAbsent(node, n -> {
            final String idCandidate = vertexIDProvider.apply(node);

            if (!isValidId(idCandidate)) {
                throw new IllegalArgumentException(
                        "generated id '" + idCandidate + "'for vertex '" + node +
                        "' is not valid with respect to the .dot language");
            }

            return idCandidate;
        });
    }

    protected abstract boolean isValidId(final String idCandidate);

    /**
     * Render the header. To be implemented by subclass.
     *
     * @param out the writer
     * @param graph the graph
     */
    protected abstract void renderHeader(PrintWriter out, ImmutableNetwork<N, E> graph);

    /**
     * Render the global graph attributes. To be implemented by subclass.
     *
     * @param out the writer
     * @param attributes the attributes of the graph
     */
    protected abstract void renderGraphAttributes(PrintWriter out,
                                                  Map<String, String> attributes);

    /**
     * Render a node. To be implemented by subclass.
     *
     * @param out the writer
     * @param node the node to be rendered
     * @param attributes the attributes of the node
     */
    protected abstract void renderNode(PrintWriter out,
                                       N node,
                                       Map<String, String> attributes);

    /**
     * Render an edge. To be implemented by subclass.
     *
     * @param out the writer
     * @param isDirected true iff edge is directed
     * @param edge the edge to be rendered
     * @param source the source node of the edge
     * @param target the target node of the edge
     * @param attributes the attributes of the edge
     */
    protected abstract void renderEdge(PrintWriter out,
                                       boolean isDirected,
                                       E edge,
                                       N source,
                                       N target,
                                       Map<String, String> attributes);
    
    /**
     * Render a sub cluster. To be implemented by subclass.
     *
     * @param out the writer
     * @param clusterId id of the cluster, can be used for naming purposes
     * @param head head node representative of the cluster
     * @param nodeSet set of nodes making up the cluster
     * @param attributes the attributes of the sub cluster
     */
    protected abstract void renderCluster(PrintWriter out,
                                          String clusterId,
                                          N head,
                                          Set<N> nodeSet,
                                          Map<String, String> attributes);

    /**
     * Render the footer.
     *
     * @param out the writer
     */
    protected abstract void renderFooter(PrintWriter out);
}
