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

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.ImmutableNetwork;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final String UNDIRECTED_GRAPH_EDGEOP = "--";
    /** Edge operation for directed graphs. */
    private static final String DIRECTED_GRAPH_EDGEOP = "->";
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

    /**
     * Default graph id used by the exporter.
     */
    public static final String DEFAULT_GRAPH_ID = "G";

    private static final String INDENT = "  ";
    private static final String INDENT2 = INDENT + INDENT;

    /**
     * The following attributes are never rendered but used instead to compute something that is then rendered.
     * For example, {@code name} and {@code expression} are used together to form {@code label}.
     */
    private static final Set<String> doNotRenderVerbatim = ImmutableSet.of("dependsOn", "expression", "label", "name");

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
     * @param clusterAttributeProvider for providing attributes to clusters
     */
    public DotExporter(@Nonnull final ComponentNameProvider<N> vertexIDProvider,
                       @Nonnull final ComponentAttributeProvider<N> vertexAttributeProvider,
                       @Nonnull final ComponentAttributeProvider<E> edgeAttributeProvider,
                       @Nonnull final Map<String, Attribute> graphAttributes,
                       @Nonnull final ClusterProvider<N, E> clusterProvider,
                       @Nonnull final ComponentAttributeProvider<N> clusterAttributeProvider) {
        super(vertexIDProvider,
                vertexAttributeProvider,
                edgeAttributeProvider,
                graphAttributes,
                clusterProvider,
                clusterAttributeProvider);
    }

    /**
     * Test if the ID candidate is a valid ID.
     *
     * @param idCandidate the ID candidate.
     *
     * @return <code>true</code> if it is valid; <code>false</code> otherwise.
     */
    @Override
    protected boolean isValidId(final String idCandidate) {
        return ALPHA_DIG.matcher(idCandidate).matches()
               || DOUBLE_QUOTE.matcher(idCandidate).matches()
               || DOT_NUMBER.matcher(idCandidate).matches() || HTML.matcher(idCandidate).matches();
    }

    @Override
    protected void renderHeader(final ExporterContext context, final ImmutableNetwork<N, E> graph) {
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
    protected void renderGraphAttributes(final ExporterContext context, final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        // graph attributes
        for (final Entry<String, Attribute> attr : attributes.entrySet()) {
            if (attr.getValue() != null) {
                out.print(INDENT);
                out.print(attr.getKey());
                out.print('=');
                out.print(attr.getValue());
                out.println(";");
            }
        }
    }

    @Override
    protected void renderNodes(final ExporterContext context) {
        final ImmutableNetwork<N, E> network = context.getNetwork();
        final PrintWriter out = context.getPrintWriter();

        super.renderNodes(context);

        // Go through the vertex set a second time to render dependsOn information
        // vertex set. dependsOn is a property of an edge specifying a partial order between that edge and
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
        for (final N n : network.nodes()) {
            // If the current node had children that explicitly encode an order we do the following:
            // We create a sub-block and set rank=same, rankDir=LR in order to have all children be rendered
            // on the same level but left to right.
            final Set<E> childrenEdges = network.inEdges(n);

            // We sort the childrenEdges topologically insertion sort-style O(N^2).
            final List<E> orderedChildrenEdges = new ArrayList<>(childrenEdges.size());

            boolean needsInvisibleEdges = false;
            for (final E toBeInsertedEdge : childrenEdges) {
                final Set<? extends AbstractPlannerGraph.AbstractEdge> dependsOn = toBeInsertedEdge.getDependsOn();

                if (!dependsOn.isEmpty()) {
                    needsInvisibleEdges = true;
                }

                int index = 0;
                while (index < orderedChildrenEdges.size()) {
                    final E currentEdge = orderedChildrenEdges.get(index);
                    if (!dependsOn.contains(currentEdge)) {
                        break;
                    }
                    index ++;
                }
                orderedChildrenEdges.add(index, toBeInsertedEdge);
            }
            if (needsInvisibleEdges) {
                final ArrayList<N> childrenOperatorList = new ArrayList<>();
                for (final E currentEdge : orderedChildrenEdges) {
                    final N currentChildNode = network.incidentNodes(currentEdge).nodeU();
                    if (currentChildNode instanceof PlannerGraph.ExpressionRefHeadNode) {
                        final Set<N> refMembers = network.predecessors(currentChildNode);
                        for (final N refMember : refMembers) {
                            Verify.verify(refMember instanceof PlannerGraph.ExpressionRefMemberNode);
                            childrenOperatorList.addAll(network.predecessors(refMember));
                        }
                    } else {
                        childrenOperatorList.add(currentChildNode);
                    }
                }

                // We have already exported all nodes; we will now render "hidden" edges to encode the edge order.
                for (int index = 0; index < childrenOperatorList.size() - 1; index++) {
                    final N currentChildNode = childrenOperatorList.get(index);
                    final N nextChildNode = childrenOperatorList.get(index + 1);
                    out.println(INDENT + "{");
                    out.println(INDENT2 + "rank=same;");
                    out.println(INDENT2 + "rankDir=LR;");
                    out.print(INDENT);
                    renderEdge(context,
                            true,
                            currentChildNode,
                            nextChildNode,
                            ImmutableMap.of("color", VisualAttribute.of("red"), "style", VisualAttribute.of("invis")));
                    out.println(INDENT + "}");
                }
            }
        }
    }

    @Override
    protected void renderNode(final ExporterContext context, final N node, final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.print(getVertexID(node));
        renderAttributes(context, attributes);
        out.println(";");
    }

    @Override
    protected void renderEdge(final ExporterContext context,
                              final boolean isDirected,
                              final N source,
                              final N target,
                              final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.print(getVertexID(source));
        out.print(" ");
        if (isDirected) {
            out.print(DIRECTED_GRAPH_EDGEOP);
        } else {
            out.print(UNDIRECTED_GRAPH_EDGEOP);
        }
        out.print(" ");
        out.print(getVertexID(target));

        renderAttributes(context, attributes);

        out.println(";");
    }

    @Override
    protected void renderCluster(final ExporterContext context,
                                 final String clusterId,
                                 final N head,
                                 final Set<N> nodeSet,
                                 final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.print("subgraph cluster_" + clusterId + " { ");

        renderClusterAttributes(context, attributes);
        for (final N n : nodeSet) {
            out.print(getVertexID(n) + "; ");
        }
        out.println("}");
    }

    /**
     * Compute the footer.
     *
     */
    @Override
    protected void renderFooter(final ExporterContext context) {
        context.getPrintWriter().print("}");
    }

    /**
     * Write out a map of String -> Object as attributes of a vertex or edge.
     * @param context context
     * @param attributes attributes
     */
    private void renderAttributes(final ExporterContext context,
                                  final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(" [ ");
        @Nullable final String htmlLabel;
        if (attributes.get("label") == null) {
            // We allow a node or edge to override the label; if it's not overridden we build it here from name
            // and expression by creating an html table
            @Nullable final Attribute name = attributes.get("name");
            @Nullable final Attribute expression = attributes.get("expression");
            htmlLabel = name == null
                        ? null
                        : getLabel(name, expression);
        } else {
            htmlLabel = getLabel(attributes.get("label"));
        }

        if (htmlLabel != null) {
            out.print("label=");
            out.print(htmlLabel);
            out.print(" ");
        }

        for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            if (doNotRenderVerbatim.contains(key)) {
                // already handled by special case above
                continue;
            }
            renderAttribute(context, key, entry.getValue(), " ");
        }
        out.print("]");
    }

    public String getLabel(@Nonnull final Attribute name, final Attribute expression) {
        if (expression == null || expression.toString().isEmpty()) {
            return "<<table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"8\"><tr><td>" + name + "</td></tr></table>>";
        }
        return "<<table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"8\"><tr><td>" + name + "</td></tr><tr><td>" +
               expression + "</td></tr></table>>";
    }

    public String getLabel(@Nonnull final Attribute label) {
        return "<&nbsp;" + label + ">";
    }

    /**
     * Write out a map of String -> Object as attributes of a cluster.
     * @param context context
     * @param attributes attributes
     */
    private void renderClusterAttributes(final ExporterContext context,
                                         final Map<String, Attribute> attributes) {
        if (attributes == null) {
            return;
        }

        for (final Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            String name = entry.getKey();
            renderAttribute(context, name, entry.getValue(), "; ");
        }
    }

    /**
     * Write out a key value pair for an attribute.
     * @param context context
     * @param attrName name
     * @param attribute attribute value
     * @param suffix -- suffix
     */
    private void renderAttribute(final ExporterContext context,
                                 final String attrName,
                                 @Nullable final Attribute attribute,
                                 final String suffix) {
        if (attribute != null) {
            final PrintWriter out = context.getPrintWriter();
            out.print(attrName + "=");
            out.print("\"" + escapeDoubleQuotes(attribute.toString()) + "\"");
            out.print(suffix);
        }
    }

    private static String escapeDoubleQuotes(final String labelName) {
        return labelName.replaceAll("\"", Matcher.quoteReplacement("\\\""));
    }
}
