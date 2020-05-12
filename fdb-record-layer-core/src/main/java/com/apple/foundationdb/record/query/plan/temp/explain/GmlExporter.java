/*
 * GmlExporter.java
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

import com.google.common.collect.ImmutableMap;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.ImmutableNetwork;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.util.Collection;
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
public class GmlExporter<N extends PlannerGraph.Node, E extends PlannerGraph.Edge> extends GraphExporter<N, E> {
    // patterns for IDs
    private static final Pattern ALPHA_DIG = Pattern.compile("[a-zA-Z_][\\w]*");
    private static final Pattern DOUBLE_QUOTE = Pattern.compile("\".*\"");
    private static final Pattern DOT_NUMBER = Pattern.compile("[-]?([.][0-9]+|[0-9]+([.][0-9]*)?)");
    private static final Pattern HTML = Pattern.compile("<.*>");

    private static final String INDENT = "  ";
    private static final String INDENT2 = INDENT + INDENT;

    /**
     * Constructs a new GmlExporter object with the given ID, label, attribute, and graph ID
     * providers. Note that if a label provider conflicts with a label-supplying attribute provider,
     * the label provider is given precedence.
     *
     * @param vertexIDProvider for generating vertex IDs. Must not be null.
     * @param vertexAttributeProvider for generating vertex attributes. If null, vertex attributes
     *        will not be written to the file.
     * @param edgeAttributeProvider for generating edge attributes. If null, edge attributes will
     *        not be written to the file.
     * @param graphAttributes map of global graph-wide attributes
     */
    public GmlExporter(@Nonnull final ComponentNameProvider<N> vertexIDProvider,
                       @Nonnull final ComponentAttributeProvider<N> vertexAttributeProvider,
                       @Nonnull final ComponentAttributeProvider<E> edgeAttributeProvider,
                       @Nonnull final Map<String, Attribute> graphAttributes) {
        super(vertexIDProvider,
                vertexAttributeProvider,
                edgeAttributeProvider,
                graphAttributes,
                network -> ImmutableMap.of(),
                node -> ImmutableMap.of());
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
        context.getPrintWriter().println("graph [");
    }

    @Override
    protected void renderGraphAttributes(final ExporterContext context, final Map<String, Attribute> attributes) {
        // graph attributes
        for (final Entry<String, Attribute> attr : attributes.entrySet()) {
            renderGraphAttribute(context, attr.getKey(), attr.getValue());
        }
        renderGraphAttribute(context, "directed", SemanticAttribute.of("1"));
    }

    private void renderGraphAttribute(final ExporterContext context, final String key, @Nullable final Attribute value) {
        if (value != null && value.isSemanticAttribute()) {
            final PrintWriter out = context.getPrintWriter();
            out.print(INDENT);
            out.print(key);
            out.print(' ');
            out.print(value);
            out.println();
        }
    }

    @Override
    protected void renderNode(final ExporterContext context, final N node, final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.println("node [");
        out.print(INDENT);
        out.print(INDENT);
        out.print("id ");
        out.println(getVertexID(node));

        renderAttributes(context, INDENT2, attributes);

        out.print(INDENT);
        out.println("]");
    }

    @Override
    protected void renderEdge(final ExporterContext context,
                              final boolean isDirected,
                              final N source,
                              final N target,
                              final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.println("edge [");
        out.print(INDENT);
        out.print(INDENT);
        out.print("source ");
        out.println(getVertexID(source));
        out.print(INDENT);
        out.print(INDENT);
        out.print("target ");
        out.println(getVertexID(target));

        renderAttributes(context, INDENT2, attributes);

        out.print(INDENT);
        out.println("]");
    }

    @Override
    protected void renderCluster(final ExporterContext context,
                                 final String clusterId,
                                 final N head,
                                 final Set<N> nodeSet,
                                 final Map<String, Attribute> attributes) {
        // no clusters in GML
    }

    @Override
    protected void renderFooter(final ExporterContext context) {
        context.getPrintWriter().print("]");
    }

    /**
     * Write out a map of String -> Object as attributes of a vertex or edge.
     * @param context context
     * @param indentation current indentation of the writer
     * @param attributes attributes
     */
    private void renderAttributes(final ExporterContext context,
                                  final String indentation,
                                  final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();

        for (final Entry<String, Attribute> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Attribute value = entry.getValue();
            if (value.isSemanticAttribute()) {
                out.print(indentation);
                renderAttribute(context, indentation, key, value);
                out.print("\n");
            }
        }
    }

    /**
     * Render one attribute. The complicating factor is that a value may be a nested map itself
     * @param context context
     * @param indentation current indentation of the writer
     * @param attrName attribute name
     * @param attribute attribute; may be any type
     */
    @SuppressWarnings("unchecked")
    private void renderAttribute(final ExporterContext context,
                                 final String indentation,
                                 final String attrName,
                                 final Attribute attribute) {
        if (attribute.isSemanticAttribute()) {
            final PrintWriter out = context.getPrintWriter();
            if (attribute.getReference() instanceof Map) {
                final Map<String, Attribute> nestedAttributes = (Map<String, Attribute>)attribute.getReference();
                out.print(attrName);
                if (nestedAttributes.isEmpty()) {
                    out.print("[ ]");
                } else {
                    out.println(" [");
                    renderAttributes(context,
                            indentation + INDENT,
                            nestedAttributes);
                    out.print(indentation);
                    out.print("]");
                }
            } else if (attribute.getReference() instanceof Collection) {
                // GML uses a weird way of describing bags: make it a map where every key is the same. Not very nice
                // as Java does not like that. Thus special code!
                final Collection<Object> nestedObjects = (Collection<Object>)attribute.getReference();
                out.print(attrName);
                if (nestedObjects.isEmpty()) {
                    out.print(" [ ]");
                } else {
                    out.println(" [");
                    out.print(indentation);
                    for (final Object nestedObject : nestedObjects) {
                        out.print(INDENT);
                        renderAttribute(context, indentation + INDENT, "x", SemanticAttribute.of(nestedObject));
                        out.println();
                        out.print(indentation);
                    }
                    out.print("]");
                }
            } else if (attribute.getReference() instanceof AbstractPlannerGraph.AbstractEdge) {
                final E edge = (E)attribute.getReference();
                final ImmutableNetwork<N, E> network = context.getNetwork();
                final EndpointPair<N> incidentNodes = network.incidentNodes(edge);

                // treat this attribute as a map to denote the source -> target of an edge
                renderAttribute(context,
                        indentation,
                        attrName,
                        SemanticAttribute.of(ImmutableMap.<String, Attribute>builder()
                                .put("source", SemanticAttribute.of(getVertexID(incidentNodes.nodeU())))
                                .put("target", SemanticAttribute.of(getVertexID(incidentNodes.nodeV())))
                                .build()));

            } else {
                out.print(attrName);
                out.print(" ");
                out.print("\"" + escapeDoubleQuotes(String.valueOf(attribute)) + "\"");
            }
        }
    }

    private static String escapeDoubleQuotes(final String labelName) {
        return labelName.replaceAll("\"", Matcher.quoteReplacement("\\\""));
    }
}
