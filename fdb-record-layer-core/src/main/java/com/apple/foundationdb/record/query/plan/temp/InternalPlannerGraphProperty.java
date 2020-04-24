/*
 * InternalPlannerGraphProperty.java
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

import com.apple.foundationdb.record.query.plan.temp.GraphExporter.ClusterProvider;
import com.apple.foundationdb.record.query.plan.temp.GraphExporter.ComponentAttributeProvider;
import com.apple.foundationdb.record.query.plan.temp.GraphExporter.ComponentNameProvider;
import com.apple.foundationdb.record.query.plan.temp.PlannerGraph.PlannerGraphBuilder;
import com.google.common.base.Throwables;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.escape.Escaper;
import com.google.common.html.HtmlEscapers;
import com.google.common.io.CharStreams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.awt.Desktop;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Class to hold a graph for explain, optimization, and rewrite purposes.
 */
@SuppressWarnings({"UnstableApiUsage"})
public class InternalPlannerGraphProperty implements PlannerProperty<PlannerGraph<InternalPlannerGraphProperty.Node, InternalPlannerGraphProperty.Edge>> {
    /**
     * Node class functioning as parent for any nodes in the network.
     */
    public static class Node extends PlannerGraph.AbstractNode {
        public Node(final String name) {
            this(name, null);
        }

        public Node(final String name, @Nullable final String expression) {
            super(name, expression);
        }

        @Nonnull
        @Override
        public Map<String, String> getAttributes() {
            final Map<String, String> superAttributes = super.getAttributes();
            return ImmutableMap
                    .<String, String>builder()
                    .putAll(superAttributes)
                    .put("color", getColor())
                    .put("shape", getShape())
                    .put("style", getStyle())
                    .put("fillcolor", getFillColor())
                    .put("fontname", "courier")
                    .build();
        }

        @Nullable
        @Override
        public String getLabel() {
            final Escaper escaper = HtmlEscapers.htmlEscaper();
            @Nullable final String e = getExpression();
            final String n = getName();
            if (e == null || e.isEmpty()) {
                return escaper.escape(n);
            }
            return "{" + escaper.escape(n) + "|" +
                   escaper.escape(e) + "}";
        }

        @Nonnull
        public String getColor() {
            return "black";
        }

        @Nonnull
        public String getShape() {
            return "record";
        }

        @Nonnull
        public String getStyle() {
            return "solid";
        }

        @Nonnull
        public String getFillColor() {
            return "black";
        }
    }

    /**
     * Node class.
     */
    public static class SourceNode extends Node {

        public SourceNode(final String name) {
            super(name, null);
        }

        @Nonnull
        @Override
        public String getColor() {
            return "black";
        }

        @Nonnull
        @Override
        public String getStyle() {
            return "filled";
        }

        @Nonnull
        @Override
        public String getFillColor() {
            return "lightblue";
        }
    }

    /**
     * Node class for GroupExpressionRefs -- head.
     */
    public static class ExpressionRefHeadNode extends Node {
        @Nonnull
        final ExpressionRef<? extends PlannerExpression> ref;

        public ExpressionRefHeadNode(final ExpressionRef<? extends PlannerExpression> ref) {
            super(ExpressionRef.class.getSimpleName());
            this.ref = ref;
        }

        @Nonnull
        @Override
        public Map<String, String> getAttributes() {
            final Map<String, String> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, String>builder()
                    .putAll(attributes)
                    .put("fontsize", "6")
                    .put("margin", "0")
                    .put("height", "0")
                    .put("width", "0")
                    .build();
        }

        @Nullable
        @Override
        public String getLabel() {
            return "r";
        }

        @Nonnull
        @Override
        public String getShape() {
            return "circle";
        }

        @Nonnull
        @Override
        public String getStyle() {
            return "filled";
        }

        @Nonnull
        @Override
        public String getFillColor() {
            return "white";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ExpressionRefHeadNode)) {
                return false;
            }
            ExpressionRefHeadNode that = (ExpressionRefHeadNode)o;
            return ref == that.ref;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(ref);
        }
    }

    /**
     * Node class for GroupExpressionRefs - member.
     */
    public static class ExpressionRefMemberNode extends Node {
        public ExpressionRefMemberNode() {
            super(ExpressionRef.class.getSimpleName());
        }

        @Nonnull
        @Override
        public Map<String, String> getAttributes() {
            final Map<String, String> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, String>builder()
                    .putAll(attributes)
                    .put("fontsize", "6")
                    .put("margin", "0")
                    .put("height", "0")
                    .put("width", "0")
                    .build();
        }

        @Nullable
        @Override
        public String getLabel() {
            return "m";
        }

        @Nonnull
        @Override
        public String getShape() {
            return "circle";
        }

        @Nonnull
        @Override
        public String getStyle() {
            return "filled";
        }

        @Nonnull
        @Override
        public String getFillColor() {
            return "white";
        }
    }

    /**
     * Edge class.
     */
    public static class Edge extends PlannerGraph.AbstractEdge {

        @Nonnull
        @Override
        public Map<String, String> getAttributes() {
            final Map<String, String> superAttributes = super.getAttributes();
            return ImmutableMap
                    .<String, String>builder()
                    .putAll(superAttributes)
                    .put("color", getColor())
                    .put("style", getStyle())
                    .build();
        }

        @Nullable
        @Override
        public String getLabel() {
            return null;
        }

        @Nonnull
        public String getColor() {
            return "black";
        }

        @Nonnull
        public String getStyle() {
            return "solid";
        }
    }

    /**
     * Edge class for GroupExpressionRefs.
     */
    public static class GroupExpressionRefEdge extends Edge {
        @Nonnull
        @Override
        public String getColor() {
            return "gray";
        }
    }

    /**
     * Edge class for GroupExpressionRefs.
     */
    public static class GroupExpressionRefInternalEdge extends Edge {
        @Nonnull
        @Override
        public Map<String, String> getAttributes() {
            final Map<String, String> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, String>builder()
                    .putAll(attributes)
                    .put("constraint", "false")
                    .build();
        }

        @Nonnull
        @Override
        public String getStyle() {
            return "invis";
        }
    }

    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param plannerExpression the planner expression to be rendered.
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String show(final PlannerExpression plannerExpression) {
        try {
            final PlannerGraph<InternalPlannerGraphProperty.Node, InternalPlannerGraphProperty.Edge> plannerGraph =
                    Objects.requireNonNull(plannerExpression.acceptPropertyVisitor(new InternalPlannerGraphProperty()));
            final URI uri = InternalPlannerGraphProperty.createHtmlLauncher(Objects.requireNonNull(plannerGraph));
            Desktop.getDesktop().browse(uri);
            return "done";
        } catch (final Exception ex) {
            Throwables.throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * For debugging only! This method locates a template html launcher file from the resources folder and writes a
     * specific launcher html file into a temp directory. If wanted the caller can open the html in a browser of
     * choice.
     *
     * @param plannerGraph -- the planner graph we should create the launcher for
     * @return a URI pointing to an html file in a temp location which renders the graph
     * @throws Exception -- thrown from methods called in here.
     */
    @Nonnull
    public static URI createHtmlLauncher(final PlannerGraph<Node, Edge> plannerGraph) throws Exception {
        final InputStream launcherHtmlInputStream =
                plannerGraph.getClass()
                        .getResourceAsStream("/showPlannerExpression.html");
        final String launcherHtmlString =
                CharStreams.toString(new InputStreamReader(launcherHtmlInputStream, StandardCharsets.UTF_8))
                        .replace("$DOT", exportToDot(plannerGraph));
        final String launcherFile = System.getProperty("java.io.tmpdir") + "local_launcher.html";
        final File launcherTempFile = new File(launcherFile);
        final PrintWriter writer;
        try {
            writer = new PrintWriter(launcherTempFile, "UTF-8");
        } catch (Exception e) {
            throw new Exception("Error opening file for writing: " + launcherTempFile.getAbsolutePath() + " : " + e.getMessage(), e);
        }
        writer.print(launcherHtmlString);
        writer.close();
        return new URI("file:///" + launcherFile.replace("\\", "/"));
    }

    /**
     * Creates a serialized format of this graph as a dot-compatible definition.
     *
     * @param plannerGraph the planner graph we should export to dot
     * @return the graph as string in dot format.
     */
    @Nonnull
    public static String exportToDot(final PlannerGraph<Node, Edge> plannerGraph) {
        final GraphExporter<Node, Edge> exporter = createExporter();
        // export as string
        final Writer writer = new StringWriter();
        exporter.exportGraph(plannerGraph.getNetwork(), writer);
        return writer.toString();
    }

    @Nonnull
    private static GraphExporter<Node, Edge> createExporter() {
        /*
         * Generate a unique identifier for each node.
         */
        final ComponentNameProvider<Node> vertexIdProvider = new ComponentNameProvider<Node>() {
            int counter = 0;
            @Override
            public String apply(final Node component) {
                counter++;
                return component.getClass().getSimpleName() + counter;
            }
        };

        final ClusterProvider<Node, Edge> clusterProvider =
                n -> n.nodes()
                        .stream()
                        .filter(node -> node instanceof ExpressionRefHeadNode || node instanceof ExpressionRefMemberNode)
                        .collect(Collectors.groupingBy(node -> {
                            if (node instanceof ExpressionRefHeadNode) {
                                return node;
                            }
                            if (node instanceof ExpressionRefMemberNode) {
                                final Node head =
                                        n.incidentNodes(Iterables.getOnlyElement(n.outEdges(node)))
                                                .nodeV();
                                Verify.verify(head instanceof ExpressionRefHeadNode);
                                return head;
                            }
                            throw new IllegalArgumentException("impossible case");
                        }, Collectors.toSet()));

        final ComponentAttributeProvider<Node> clusterAttributeProvider =
                node -> ImmutableMap.<String, String>builder()
                        .put("style", "filled")
                        .put("fillcolor", "lightgrey")
                        .put("fontsize", "6")
                        .put("rank", "same")
                        .put("label", "group")
                        .build();

        return new DotExporter<>(vertexIdProvider,
                Node::getAttributes,
                Edge::getAttributes,
                ImmutableMap.of("fontname", "courier", "rankdir", "BT"),
                clusterProvider,
                clusterAttributeProvider);
    }

    @Override
    public boolean shouldVisit(@Nonnull PlannerExpression expression) {
        return true;
    }

    @Override
    public boolean shouldVisit(@Nonnull ExpressionRef<? extends PlannerExpression> ref) {
        return true;
    }

    @Nonnull
    @Override
    public PlannerGraph<Node, Edge> evaluateAtExpression(@Nonnull PlannerExpression expression, @Nonnull List<PlannerGraph<Node, Edge>> childGraphs) {
        final PlannerGraphBuilder<Node, Edge> plannerGraphBuilder = expression.showYourself();
        for (final PlannerGraph<Node, Edge> childGraph : childGraphs) {
            plannerGraphBuilder
                    .addGraph(childGraph)
                    .addEdge(childGraph.getRoot(), plannerGraphBuilder.getRoot(), new GroupExpressionRefEdge());
        }
        return plannerGraphBuilder.build();
    }

    @Nonnull
    @Override
    public PlannerGraph<Node, Edge> evaluateAtRef(@Nonnull ExpressionRef<? extends PlannerExpression> ref, @Nonnull List<PlannerGraph<Node, Edge>> memberResults) {
        if (memberResults.isEmpty()) {
            // should not happen
            return PlannerGraph.<Node, Edge>builder(new ExpressionRefHeadNode(ref)).build();
        }
        
        final Node head = new ExpressionRefHeadNode(ref);
        final PlannerGraphBuilder<Node, Edge> plannerGraphBuilder =
                PlannerGraph.builder(head);

        final List<PlannerGraph<Node, Edge>> memberGraphs =
                memberResults
                        .stream()
                        .map(childGraph -> {
                            final Node member = new ExpressionRefMemberNode();
                            return PlannerGraph.<Node, Edge>builder(member)
                                    .addGraph(childGraph)
                                    .addEdge(childGraph.getRoot(), member, new GroupExpressionRefEdge())
                                    .build();
                        })
                        .collect(Collectors.toList());

        memberGraphs.forEach(memberGraph -> {
            plannerGraphBuilder.addGraph(memberGraph);
            plannerGraphBuilder.addEdge(memberGraph.getRoot(), head, new GroupExpressionRefInternalEdge());
        });
        return plannerGraphBuilder.build();
    }
}
