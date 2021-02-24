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

package com.apple.foundationdb.record.query.plan.temp.explain;

import com.apple.foundationdb.record.query.plan.temp.TopologicalSort;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.escape.Escaper;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.html.HtmlEscapers;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Exports a graph into a DOT file.
 *
 * @param <N> the network node type
 * @param <E> the network edge type
 *
 */
@SuppressWarnings("UnstableApiUsage")
public class DotExporter<N extends PlannerGraph.Node, E extends PlannerGraph.Edge> extends GraphExporter<N, E> {
    /** Edge operation for undirected graphs. */
    private static final String UNDIRECTED_GRAPH_EDGE_OP = "--";
    /** Edge operation for directed graphs. */
    private static final String DIRECTED_GRAPH_EDGE_OP = "->";
    /** Keyword for undirected graphs. */
    private static final String UNDIRECTED_GRAPH_KEYWORD = "graph";
    /** Keyword for directed graphs. */
    private static final String DIRECTED_GRAPH_KEYWORD = "digraph";
    /** Keyword for representing strict graphs. */
    private static final String DONT_ALLOW_MULTIPLE_EDGES_KEYWORD = "strict";
    // patterns for IDs
    private static final Pattern ALPHA_DIG = Pattern.compile("[a-zA-Z_][\\w]*");
    private static final Pattern DOUBLE_QUOTE = Pattern.compile("\".*\"");
    private static final Pattern DOT_NUMBER = Pattern.compile("[-]?([.][0-9]+|[0-9]+([.][0-9]*)?)");
    private static final Pattern HTML = Pattern.compile("<.*>");
    private static final Pattern VARIABLE = Pattern.compile("\\{\\{(.+?)}}");

    /**
     * Default graph id used by the exporter.
     */
    public static final String DEFAULT_GRAPH_ID = "G";

    private static final String INDENT = "  ";

    private static final Escaper escaper = HtmlEscapers.htmlEscaper();

    /**
     * Constructs a new DotExporter object with the given ID, label, attribute, and graph ID
     * providers. Note that if a label provider conflicts with a label-supplying attribute provider,
     * the label provider is given precedence.
     *
     * @param vertexIDProvider for generating vertex IDs. Must not be null.
     * @param vertexAttributeProvider for generating vertex attributes. If null, vertex attributes
     *        will not be written to the file.
     * @param edgeAttributeProvider for generating edge attributes. If null, edge attributes will
     *        not be written to the file.
     * @param graphAttributes map of global attributes
     * @param clusterProvider for partitioning the graph into clusters if warranted
     */
    public DotExporter(@Nonnull final ComponentIdProvider<N> vertexIDProvider,
                       @Nonnull final ComponentAttributeProvider<N> vertexAttributeProvider,
                       @Nonnull final ComponentAttributeProvider<E> edgeAttributeProvider,
                       @Nonnull final Map<String, Attribute> graphAttributes,
                       @Nonnull final ClusterProvider<N, E> clusterProvider) {
        super(vertexIDProvider,
                vertexAttributeProvider,
                ignored -> null, // dot does not support ids for edges
                edgeAttributeProvider,
                graphAttributes,
                clusterProvider);
    }

    /**
     * Test if the ID candidate is a valid ID.
     *
     * @param idCandidate the ID candidate.
     *
     * @return <code>true</code> if it is valid; <code>false</code> otherwise.
     */
    @Override
    protected boolean isValidId(@Nonnull final String idCandidate) {
        return ALPHA_DIG.matcher(idCandidate).matches()
               || DOUBLE_QUOTE.matcher(idCandidate).matches()
               || DOT_NUMBER.matcher(idCandidate).matches() || HTML.matcher(idCandidate).matches();
    }

    @Override
    protected void renderHeader(@Nonnull final ExporterContext context, @Nonnull final ImmutableNetwork<N, E> graph) {
        final PrintWriter out = context.getPrintWriter();
        if (!graph.allowsParallelEdges()) {
            out.print(DONT_ALLOW_MULTIPLE_EDGES_KEYWORD);
            out.print(" ");
        }
        if (graph.isDirected()) {
            out.print(DIRECTED_GRAPH_KEYWORD);
        } else {
            out.print(UNDIRECTED_GRAPH_KEYWORD);
        }
        out.print(" ");
        out.print(DEFAULT_GRAPH_ID);
        out.println(" {");
    }

    @Override
    protected void renderGraphAttributes(@Nonnull final ExporterContext context, @Nonnull final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        // graph attributes
        for (final Entry<String, Attribute> attr : attributes.entrySet()) {
            final Attribute value = attr.getValue();
            if (value.isVisible(context)) {
                out.print(INDENT);
                out.print(attr.getKey());
                out.print('=');
                out.print(value);
                out.println(";");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void renderInvisibleEdges(@Nonnull final ExporterContext context, final N n, final String indentation) {
        final ImmutableNetwork<N, E> network = context.getNetwork();
        final PrintWriter out = context.getPrintWriter();

        // dependsOn is a property of an edge specifying a partial order between that edge and
        // other inEdges to the target of that edge:
        //
        //           n*                                           n*
        //     /  |   \     \                               /  |   \     \
        //    /e1 |e2  \e3   \e4            ==>            /e1 |e2  \e3   \e4
        //   /    |     \     \                           /    |     \     \
        //  n1    n2     n3    n4                       n1 ->  n2 ->  n3 -> n4
        //
        //  dependsOn: e1: [], e2: [e1], e3: [e1], e4: [e1, e2, e3]
        //
        // The dependsOn sets of all such edges targeting n* form a partial ordering (or sometimes a complete
        // ordering). We would like to encode this dependency relationship into the dot in a way that the dependency
        // is shown left-to-right. This is important to read for join order operators that implicitly impose ordering
        // by e.g. designating an outer and an inner edge.
        //
        // As dependsOn is a partial order, depending on the degree of freedom, there usually is not one but many
        // correct left-to-right layouts. A complete ordering will force exactly on left-to-right layout.
        //
        // The way we encode the left-to-rightedness of the edges is by creating invisible edges between n1, ..., nk.

        // If the current node had children that explicitly encode an order we do the following:
        // We create a sub-block and set rank=same, rankDir=LR in order to have all children be rendered
        // on the same level but left to right.
        final Set<E> childrenEdges = network.inEdges(n);

        final boolean needsInvisibleEdges = childrenEdges.stream().anyMatch(edge -> !edge.getDependsOn().isEmpty());

        if (needsInvisibleEdges) {
            final Optional<List<E>> orderedChildrenEdgesOptional =
                    TopologicalSort.anyTopologicalOrderPermutation(childrenEdges, edge -> (Set<E>)edge.getDependsOn());
            Verify.verify(orderedChildrenEdgesOptional.isPresent());
            final List<E> orderedChildrenEdges = orderedChildrenEdgesOptional.get();

            final ArrayList<N> childrenOperatorList = new ArrayList<>();
            for (final E currentEdge : orderedChildrenEdges) {
                final N currentChildNode = network.incidentNodes(currentEdge).nodeU();
                if (currentChildNode instanceof PlannerGraph.ExpressionRefHeadNode) {
                    final Set<N> refMembers = network.predecessors(currentChildNode);
                    for (final N refMember : Sets.filter(refMembers, refMember -> refMember instanceof PlannerGraph.ExpressionRefMemberNode)) {
                        childrenOperatorList.addAll(network.predecessors(refMember));
                    }
                } else {
                    childrenOperatorList.add(currentChildNode);
                }
            }

            // We have already exported all nodes; we will now render "hidden" edges to encode the edge order.
            for (int index = 0; index < childrenOperatorList.size() - 1; index ++) {
                final N currentChildNode = childrenOperatorList.get(index);
                final N nextChildNode = childrenOperatorList.get(index + 1);
                out.println(indentation + "{");
                out.println(indentation + INDENT + "rank=same;");
                out.println(indentation + INDENT + "rankDir=LR;");
                out.print(indentation);
                renderEdge(context,
                        true,
                        currentChildNode,
                        nextChildNode,
                        ImmutableMap.of("color", Attribute.dot("red"), "style", Attribute.dot("invis")));
                out.println(indentation + "}");
            }
        }
    }

    @Override
    protected void renderNode(@Nonnull final ExporterContext context,
                              @Nonnull final N node,
                              @Nonnull final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.print(getVertexID(node));

        out.print(" [ ");
        @Nullable final String htmlLabel;
        if (attributes.get("label") == null) {
            // We allow a node or edge to override the label; if it's not overridden we build it here from name
            // and expression by creating an HTML table
            @Nullable final Attribute name = attributes.get("name");
            @Nullable final Attribute details = attributes.get("details");
            htmlLabel = name == null
                        ? null
                        : getNodeLabel(name, details, attributes);
        } else {
            htmlLabel = getNodeLabel(attributes.get("label"), null, ImmutableMap.of());
        }

        if (htmlLabel != null) {
            out.print("label=");
            out.print(htmlLabel);
            out.print(" ");
        }

        renderAttributes(context, attributes);

        out.print("]");
        out.println(";");
    }

    @Override
    @SuppressWarnings("squid:S3358")
    protected void renderEdge(@Nonnull final ExporterContext context,
                              final boolean isDirected,
                              @Nonnull final N source,
                              @Nonnull final N target,
                              @Nonnull final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.print(getVertexID(source));
        out.print(" ");
        if (isDirected) {
            out.print(DIRECTED_GRAPH_EDGE_OP);
        } else {
            out.print(UNDIRECTED_GRAPH_EDGE_OP);
        }
        out.print(" ");
        out.print(getVertexID(target));

        out.print(" [ ");
        @Nullable final String htmlLabel =
                attributes.containsKey("label")
                ? getEdgeLabel(attributes.get("label"))
                : attributes.containsKey("name")
                  ? getEdgeLabel(attributes.get("name"))
                  : null;

        if (htmlLabel != null) {
            out.print("label=");
            out.print(htmlLabel);
            out.print(" ");
        }

        renderAttributes(context, attributes);

        out.print("]");

        out.println(";");
    }

    @Override
    protected void renderClusters(@Nonnull final ExporterContext context, @Nonnull final Collection<Cluster<N, E>> clusters) {
        renderClusters(context, context.getNetwork().nodes(), clusters, "cluster", INDENT);
    }

    protected void renderClusters(@Nonnull final ExporterContext context,
                                  @NonNull final Set<N> currentNodes,
                                  @Nonnull final Collection<Cluster<N, E>> nestedClusters,
                                  @Nonnull final String prefix,
                                  @Nonnull String indentation) {

        Set<N> remainingNodes = Sets.newHashSet(currentNodes);
        int i = 1;
        for (final Cluster<N, E> nestedCluster : nestedClusters) {
            final ComponentAttributeProvider<Cluster<N, E>> clusterAttributeProvider = nestedCluster.getClusterAttributeProvider();
            renderCluster(context,
                    prefix + "_" + i,
                    nestedCluster,
                    clusterAttributeProvider.apply(nestedCluster),
                    indentation);
            remainingNodes = Sets.difference(remainingNodes, nestedCluster.getNodes());
            i ++;
        }

        for (final N remainingNode : remainingNodes) {
            renderInvisibleEdges(context, remainingNode, indentation);
        }
    }

    /**
     * Render a sub cluster. To be implemented by subclass.
     *
     * @param context the context
     * @param clusterId id of the cluster, can be used for naming purposes
     * @param cluster the cluster to be serialized
     * @param attributes the attributes of the sub cluster
     * @param indentation indentation
     */
    protected void renderCluster(@Nonnull ExporterContext context,
                                 @Nonnull String clusterId,
                                 @Nonnull Cluster<N, E> cluster,
                                 @Nonnull Map<String, Attribute> attributes,
                                 @Nonnull String indentation) {
        final PrintWriter out = context.getPrintWriter();
        out.print(indentation);
        out.print("subgraph " + clusterId + " { ");

        renderClusterAttributes(context, attributes);
        for (final N n : cluster.getNodes()) {
            out.print(getVertexID(n) + "; ");
        }

        final ClusterProvider<N, E> nestedClusterProvider = cluster.getNestedClusterProvider();
        final Collection<Cluster<N, E>> nestedClusters = nestedClusterProvider.apply(context.getNetwork(), cluster.getNodes());
        if (!nestedClusters.isEmpty()) {
            out.println();
            renderClusters(context, cluster.getNodes(), nestedClusters, clusterId, indentation + INDENT);
            out.print(indentation);
        }
        out.println("}");
    }

    /**
     * Compute the footer.
     *
     */
    @Override
    protected void renderFooter(@Nonnull final ExporterContext context) {
        context.getPrintWriter().print("}");
    }

    /**
     * Write out a map of String -> Object as attributes of a vertex or edge.
     * @param context context
     * @param attributes attributes
     */
    private void renderAttributes(@Nonnull final ExporterContext context,
                                  @Nonnull final Map<String, Attribute> attributes) {
        for (final Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            final Attribute value = entry.getValue();
            if (value.isVisible(context)) {
                renderAttribute(context, entry.getKey(), value, " ");
            }
        }
    }

    @SuppressWarnings({"squid:S3358", "unchecked"})
    @Nonnull
    public String getNodeLabel(@Nonnull final Attribute name,
                               @Nullable final Attribute details,
                               @Nonnull final Map<String, Attribute> nodeAttributes) {
        if (details == null || ((List<?>)details.getReference()).isEmpty()) {
            return "<<table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"8\"><tr><td>" +
                   escaper.escape(name.getReference().toString()) +
                   "</td></tr></table>>";
        }

        final String detailsString =
                ((List<?>)details.getReference())
                        .stream()
                        .map(Object::toString)
                        .map(escaper::escape)
                        .map(detail ->
                                substituteVariables(detail,
                                        nodeAttributes,
                                        attribute -> attribute == null
                                                     ? "<b>undefined</b>"
                                                     : (attribute.getReference() instanceof Collection)
                                                       ? escapeCollection((Collection<Attribute>)attribute.getReference())
                                                       : escaper.escape(attribute.getReference().toString())))
                        .map(detail -> "<tr><td>" + detail + "</td></tr>")
                        .collect(Collectors.joining());

        return "<<table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"8\"><tr><td>" +
               escaper.escape(name.getReference().toString()) + "</td></tr>" +
               detailsString + "</table>>";
    }

    @Nonnull
    private String escapeCollection(@Nonnull final Collection<Attribute> attributes) {
        return "[" + attributes.stream().map(a -> escaper.escape(a.toString())).collect(Collectors.joining(", ")) + "]";
    }

    @Nonnull
    private String substituteVariables(@Nonnull final String detail,
                                       @Nonnull final Map<String, Attribute> nodeAttributes,
                                       @Nonnull final Function<Attribute, String> toStringFn) {
        final Matcher matcher = VARIABLE.matcher(detail);

        final StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            @Nullable final Attribute referredAttribute = nodeAttributes.get(matcher.group(1));
            final String replacement = toStringFn.apply(referredAttribute);
            builder.append(detail, i, matcher.start());
            builder.append(replacement);
            i = matcher.end();
        }
        builder.append(detail.substring(i));
        return builder.toString();
    }

    @Nonnull
    public String getEdgeLabel(@Nonnull final Attribute label) {
        return "<&nbsp;" + escaper.escape(label.getReference().toString()) + ">";
    }

    /**
     * Write out a map of String -> Object as attributes of a cluster.
     * @param context context
     * @param attributes attributes
     */
    private void renderClusterAttributes(@Nonnull final ExporterContext context,
                                         @Nonnull final Map<String, Attribute> attributes) {
        for (final Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            final Attribute value = entry.getValue();
            if (value.isVisible(context)) {
                renderAttribute(context, entry.getKey(), value, "; ");
            }
        }
    }

    /**
     * Write out a key value pair for an attribute.
     * @param context context
     * @param attrName name
     * @param attribute attribute value
     * @param suffix -- suffix
     */
    private void renderAttribute(@Nonnull final ExporterContext context,
                                 @Nonnull final String attrName,
                                 @Nullable final Attribute attribute,
                                 @Nonnull final String suffix) {
        if (attribute != null) {
            final PrintWriter out = context.getPrintWriter();
            out.print(attrName + "=");
            out.print("\"" + escaper.escape(attribute.getReference().toString()) + "\"");
            out.print(suffix);
        }
    }
}
