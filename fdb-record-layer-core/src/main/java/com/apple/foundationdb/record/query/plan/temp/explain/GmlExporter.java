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

import com.google.common.collect.ImmutableList;
import com.google.common.escape.Escaper;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.html.HtmlEscapers;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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
    private static final Pattern NUMBER = Pattern.compile("([0-9]+|[0-9]+([.][0-9]*)?)");
    private static final String INDENT = "  ";
    private static final String INDENT2 = INDENT + INDENT;

    private static final Escaper escaper = HtmlEscapers.htmlEscaper();

    /**
     * Constructs a new GmlExporter object with the given ID, label, attribute, and graph ID
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
     */
    public GmlExporter(@Nonnull final ComponentIdProvider<N> vertexIDProvider,
                       @Nonnull final ComponentAttributeProvider<N> vertexAttributeProvider,
                       @Nonnull final ComponentIdProvider<E> edgeIDProvider,
                       @Nonnull final ComponentAttributeProvider<E> edgeAttributeProvider,
                       @Nonnull final Map<String, Attribute> graphAttributes) {
        super(vertexIDProvider,
                vertexAttributeProvider,
                edgeIDProvider,
                edgeAttributeProvider,
                graphAttributes,
                (network, nodes) -> ImmutableList.of());
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
        return NUMBER.matcher(idCandidate).matches();
    }

    @Override
    protected void renderHeader(@Nonnull final ExporterContext context,
                                @Nonnull final ImmutableNetwork<N, E> graph) {
        context.getPrintWriter().println("graph [");
    }

    @Override
    protected void renderGraphAttributes(@Nonnull final ExporterContext context,
                                         @Nonnull final Map<String, Attribute> attributes) {
        // graph attributes
        final PrintWriter out = context.getPrintWriter();
        for (final Entry<String, Attribute> entry : attributes.entrySet()) {
            out.print(INDENT);
            renderAttribute(context, INDENT, entry.getKey(), entry.getValue());
            out.println();
        }
        out.print(INDENT);
        renderAttribute(context, INDENT, "directed", Attribute.gml(1));
        out.println();
    }

    @Override
    protected void renderNode(@Nonnull final ExporterContext context,
                              @Nonnull final N node,
                              @Nonnull final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.println("node [");
        out.print(INDENT2);
        out.print("id ");
        out.println(getVertexID(node));

        if (attributes.get("label") == null) {
            if (attributes.containsKey("details")) {
                out.print(INDENT2);
                final Attribute detailsAttribute = attributes.get("details");
                renderAttribute(context, INDENT2, "details", Attribute.gml(detailsAttribute.getReference()));
                out.println();
            }
        } else {
            out.print(INDENT2);
            final Attribute nameAttribute = attributes.get("label");
            renderAttribute(context, INDENT2, "label", Attribute.gml(nameAttribute.getReference()));
            out.println();
        }

        renderAttributes(context, INDENT2, attributes);

        out.print(INDENT);
        out.println("]");
    }

    @Override
    protected void renderEdge(@Nonnull final ExporterContext context,
                              final boolean isDirected,
                              @Nonnull final N source,
                              @Nonnull final N target,
                              @Nonnull final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();
        out.print(INDENT);
        out.println("edge [");
        final ImmutableNetwork<N, E> network = context.getNetwork();
        final Optional<E> edgeOptional = network.edgeConnecting(source, target);
        edgeOptional.ifPresent(edge -> {
            out.print(INDENT2);
            out.print("id ");
            out.println(getEdgeID(edge));
        });

        out.print(INDENT2);
        out.print("source ");
        out.println(getVertexID(source));
        out.print(INDENT2);
        out.print("target ");
        out.println(getVertexID(target));

        renderAttributes(context, INDENT2, attributes);

        out.print(INDENT);
        out.println("]");
    }

    @Override
    protected void renderClusters(@Nonnull final ExporterContext context, @Nonnull final Collection<Cluster<N, E>> clusters) {
        // no clusters in GML
    }

    @Override
    protected void renderFooter(@Nonnull final ExporterContext context) {
        context.getPrintWriter().print("]");
    }

    /**
     * Write out a map of String -> Object as attributes of a vertex or edge.
     * @param context context
     * @param indentation current indentation of the writer
     * @param attributes attributes
     */
    private void renderAttributes(@Nonnull final ExporterContext context,
                                  @Nonnull final String indentation,
                                  @Nonnull final Map<String, Attribute> attributes) {
        final PrintWriter out = context.getPrintWriter();

        for (final Entry<String, Attribute> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Attribute value = entry.getValue();
            if (value.isVisible(context)) {
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
    private void renderAttribute(@Nonnull final ExporterContext context,
                                 @Nonnull final String indentation,
                                 @Nonnull final String attrName,
                                 @Nonnull final Attribute attribute) {
        if (attribute.isVisible(context)) {
            final PrintWriter out = context.getPrintWriter();
            final Object reference = attribute.getReference();
            if (reference instanceof Map) {
                final Map<String, Attribute> nestedAttributes = (Map<String, Attribute>)reference;
                out.print(attrName);
                if (nestedAttributes.isEmpty()) {
                    out.print(" [ ]");
                } else {
                    out.println(" [");
                    renderAttributes(context,
                            indentation + INDENT,
                            nestedAttributes);
                    out.print(indentation);
                    out.print("]");
                }
            } else if (reference instanceof Collection) {
                // GML uses a weird way of describing bags: make it a map where every key is the same. Not very nice
                // as Java does not like that. Thus special code!
                final Collection<Attribute> nestedAttributes = (Collection<Attribute>)reference;
                out.print(attrName);
                if (nestedAttributes.isEmpty()) {
                    out.print(" [ ]");
                } else {
                    out.println(" [");
                    out.print(indentation);
                    for (final Attribute nestedAttribute : nestedAttributes) {
                        out.print(INDENT);
                        renderAttribute(context, indentation + INDENT, "x", nestedAttribute);
                        out.println();
                        out.print(indentation);
                    }
                    out.print("]");
                }
            } else if (reference instanceof AbstractPlannerGraph.AbstractEdge) {
                final E edge = (E)reference;
                // treat this attribute as a map to denote the source -> target of an edge
                renderAttribute(context,
                        indentation,
                        attrName,
                        Attribute.gml(Integer.parseInt(getEdgeID(edge))));

            } else if (reference instanceof Number) {
                out.print(attrName);
                out.print(" ");
                out.print(reference);
            } else {
                out.print(attrName);
                out.print(" ");
                out.print("\"" + escaper.escape(reference.toString()) + "\"");
            }
        }
    }
}
