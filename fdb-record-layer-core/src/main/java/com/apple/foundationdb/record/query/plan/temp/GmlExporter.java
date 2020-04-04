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
import com.google.common.graph.ImmutableNetwork;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
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
public class GmlExporter<N, E> extends GraphExporter<N, E> {
    // patterns for IDs
    private static final Pattern ALPHA_DIG = Pattern.compile("[a-zA-Z_][\\w]*");
    private static final Pattern DOUBLE_QUOTE = Pattern.compile("\".*\"");
    private static final Pattern DOT_NUMBER = Pattern.compile("[-]?([.][0-9]+|[0-9]+([.][0-9]*)?)");
    private static final Pattern HTML = Pattern.compile("<.*>");

    private static final String INDENT = "  ";

    /**
     * Constructs a new GmlExporter object with the given ID, label, attribute, and graph id
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
                       @Nonnull final Map<String, String> graphAttributes) {
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
    protected void renderHeader(final PrintWriter out, final ImmutableNetwork<N, E> graph) {
        out.println("graph [");
    }

    @Override
    protected void renderGraphAttributes(final PrintWriter out, final Map<String, String> attributes) {
        // graph attributes
        for (final Entry<String, String> attr : attributes.entrySet()) {
            renderGraphAttribute(out, attr.getKey(), attr.getValue());
        }
        renderGraphAttribute(out, "directed", "1");
    }

    private void renderGraphAttribute(final PrintWriter out, final String key, final String value) {
        out.print(INDENT);
        out.print(key);
        out.print(' ');
        out.print(value);
        out.println();
    }

    @Override
    protected void renderNode(final PrintWriter out, final N node, final Map<String, String> attributes) {
        out.print(INDENT);
        out.println("node [");
        out.print(INDENT);
        out.print(INDENT);
        out.print("id ");
        out.println(getVertexID(node));

        renderAttributes(out, attributes);

        out.print(INDENT);
        out.println("]");
    }

    @Override
    protected void renderEdge(final PrintWriter out,
                              final boolean isDirected,
                              final E edge,
                              final N source,
                              final N target,
                              final Map<String, String> attributes) {
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

        renderAttributes(out, attributes);

        out.print(INDENT);
        out.println("]");
    }

    @Override
    protected void renderCluster(final PrintWriter out,
                                 final String clusterId,
                                 final N head,
                                 final Set<N> nodeSet,
                                 final Map<String, String> attributes) {
        // no clusters in GML
    }

    @Override
    protected void renderFooter(final PrintWriter out) {
        out.print("]");
    }

    /**
     * Write out a map of String -> String as attributes of a vertex or edge.
     * @param out target
     * @param attributes attributes
     */
    private void renderAttributes(final PrintWriter out,
                                  final Map<String, String> attributes) {
        final String labelAttribute = attributes.get("label");
        if (labelAttribute != null) {
            out.print(INDENT);
            out.print(INDENT);
            renderAttribute(out, "label", labelAttribute, "\n");
        }
        for (Entry<String, String> entry : attributes.entrySet()) {
            String name = entry.getKey();
            if ("label".equals(name)) {
                // already handled by special case above
                continue;
            }
            out.print(INDENT);
            out.print(INDENT);
            renderAttribute(out, name, entry.getValue(), "\n");
        }
    }

    private void renderAttribute(final PrintWriter out,
                                 final String attrName,
                                 final String attribute,
                                 final String suffix) {
        out.print(attrName);
        out.print(" ");
        out.print("\"" + escapeDoubleQuotes(attribute) + "\"");
        out.print(suffix);
    }

    private static String escapeDoubleQuotes(final String labelName) {
        return labelName.replaceAll("\"", Matcher.quoteReplacement("\\\""));
    }
}
