/*
 * PlannerGraphProperty.java
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

import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.GraphExporter.ClusterProvider;
import com.apple.foundationdb.record.query.plan.temp.explain.GraphExporter.ComponentAttributeProvider;
import com.apple.foundationdb.record.query.plan.temp.explain.GraphExporter.ComponentIdProvider;
import com.google.common.base.Throwables;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;

import javax.annotation.Nonnull;
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
public class PlannerGraphProperty implements PlannerProperty<PlannerGraph> {
    private final boolean isForExplain;
    private final boolean renderSingleGroups;

    /**
     * Helper class to provide an incrementing integer identifier per use.
     * @param <T> the class of the component the {@code apply} method generates an identifier for.
     */
    private static class CountingIdProvider<T> implements ComponentIdProvider<T> {
        int counter = 0;

        @Override
        public String apply(@Nonnull final T t) {
            counter++;
            return String.valueOf(counter);
        }
    }

    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param renderSingleGroups iff true group references with just one member are not rendered
     * @param relationalExpression the planner expression to be rendered.
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String show(final boolean renderSingleGroups, @Nonnull final RelationalExpression relationalExpression) {
        try {
            final AbstractPlannerGraph<PlannerGraph.Node, PlannerGraph.Edge> plannerGraph =
                    Objects.requireNonNull(relationalExpression.acceptPropertyVisitor(forInternalShow(renderSingleGroups)));
            final URI uri = PlannerGraphProperty.createHtmlLauncher(Objects.requireNonNull(plannerGraph));
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
    public static URI createHtmlLauncher(@Nonnull final AbstractPlannerGraph<PlannerGraph.Node, PlannerGraph.Edge> plannerGraph) throws Exception {
        final InputStream launcherHtmlInputStream =
                plannerGraph.getClass()
                        .getResourceAsStream("/showPlannerExpression.html");
        final String dotString = exportToDot(plannerGraph);
        final String launcherHtmlString =
                CharStreams.toString(new InputStreamReader(launcherHtmlInputStream, StandardCharsets.UTF_8))
                        .replace("$DOT", dotString);
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
    public static String exportToDot(@Nonnull final AbstractPlannerGraph<PlannerGraph.Node, PlannerGraph.Edge> plannerGraph) {
        final GraphExporter<PlannerGraph.Node, PlannerGraph.Edge> exporter = createDotExporter();
        // export as string
        final Writer writer = new StringWriter();
        exporter.exportGraph(plannerGraph.getNetwork(), writer);
        return writer.toString();
    }

    @Nonnull
    private static GraphExporter<PlannerGraph.Node, PlannerGraph.Edge> createDotExporter() {
        final ClusterProvider<PlannerGraph.Node, PlannerGraph.Edge> clusterProvider =
                n -> n.nodes()
                        .stream()
                        .filter(node -> node instanceof PlannerGraph.ExpressionRefHeadNode || node instanceof PlannerGraph.ExpressionRefMemberNode)
                        .collect(Collectors.groupingBy(node -> {
                            if (node instanceof PlannerGraph.ExpressionRefHeadNode) {
                                return node;
                            }
                            if (node instanceof PlannerGraph.ExpressionRefMemberNode) {
                                final PlannerGraph.Node head =
                                        n.incidentNodes(Iterables.getOnlyElement(n.outEdges(node)))
                                                .nodeV();
                                Verify.verify(head instanceof PlannerGraph.ExpressionRefHeadNode);
                                return head;
                            }
                            throw new IllegalArgumentException("impossible case");
                        }, Collectors.toSet()));

        final ComponentAttributeProvider<PlannerGraph.Node> clusterAttributeProvider =
                node -> ImmutableMap.<String, Attribute>builder()
                        .put("style", Attribute.dot("filled"))
                        .put("fillcolor", Attribute.dot("lightgrey"))
                        .put("fontsize", Attribute.dot("6"))
                        .put("rank", Attribute.dot("same"))
                        .put("label", Attribute.dot("group"))
                        .build();

        return new DotExporter<>(new CountingIdProvider<>(),
                PlannerGraph.Node::getAttributes,
                PlannerGraph.Edge::getAttributes,
                ImmutableMap.of("fontname", Attribute.dot("courier"),
                        "rankdir", Attribute.dot("BT")),
                clusterProvider,
                clusterAttributeProvider);
    }

    /**
     * Generate the explain of the planner expression that is passed in.
     * @param relationalExpression the planner expression to be explained.
     * @return the explain of the planner expression handing in as a string in GML format.
     */
    @Nonnull
    public static String explain(@Nonnull final RelationalExpression relationalExpression) {
        return explain(relationalExpression, ImmutableMap.of());
    }


    /**
     * Generate the explain of the planner expression that is passed in.
     * @param relationalExpression the planner expression to be explained.
     * @param additionalDescriptionMap a map used to generate names and descriptions for operators.
     * @return the explain of the planner expression handing in as a string in GML format.
     */
    @Nonnull
    public static String explain(@Nonnull final RelationalExpression relationalExpression,
                                 @Nonnull final Map<String, Attribute> additionalDescriptionMap) {
        try {
            final PlannerGraph plannerGraph =
                    Objects.requireNonNull(relationalExpression.acceptPropertyVisitor(forExplain()));
            return exportToGml(plannerGraph, additionalDescriptionMap);
        } catch (final Exception ex) {
            Throwables.throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Creates a serialized format of this graph as a gml-compatible definition.
     * @param plannerGraph the planner graph to be exported
     * @param additionalInfoMap a map used to generate names and descriptions for operators.
     * @return the graph as string in gml format.
     */
    @Nonnull
    public static String exportToGml(@Nonnull final PlannerGraph plannerGraph,
                                     @Nonnull final Map<String, Attribute> additionalInfoMap) {
        // Synthesize the set of NodeWithInfo nodes that are in the plan.
        final ImmutableSet<String> usedInfoIds =
                plannerGraph.getNetwork()
                        .nodes()
                        .stream()
                        .filter(n -> n instanceof PlannerGraph.WithInfoId)
                        .map(n -> (PlannerGraph.WithInfoId)n)
                        .map(PlannerGraph.WithInfoId::getInfoId)
                        .collect(ImmutableSet.toImmutableSet());

        final ImmutableMap.Builder<String, Attribute> infoMapBuilder = ImmutableMap.builder();
        infoMapBuilder.putAll(Maps.filterEntries(NodeInfo.getInfoAttributeMap(NodeInfo.getNodeInfos()), e -> usedInfoIds.contains(Objects.requireNonNull(e).getKey())));
        infoMapBuilder.putAll(Maps.filterEntries(additionalInfoMap, e -> usedInfoIds.contains(Objects.requireNonNull(e).getKey())));
        final ImmutableMap<String, Attribute> infoMap = infoMapBuilder.build();

        final GraphExporter<PlannerGraph.Node, PlannerGraph.Edge> exporter =
                createGmlExporter(infoMap);
        // export as string
        final Writer writer = new StringWriter();
        exporter.exportGraph(plannerGraph.getNetwork(), writer);
        return writer.toString();
    }

    @Nonnull
    private static GraphExporter<PlannerGraph.Node, PlannerGraph.Edge> createGmlExporter(@Nonnull final Map<String, Attribute> infoMap) {
        return new GmlExporter<>(new CountingIdProvider<>(),
                PlannerGraph.Node::getAttributes,
                new CountingIdProvider<>(),
                PlannerGraph.Edge::getAttributes,
                ImmutableMap.of("infos", Attribute.gml(infoMap)));
    }

    public static PlannerGraphProperty forExplain() {
        return new PlannerGraphProperty(true, false);
    }

    public static PlannerGraphProperty forInternalShow(final boolean renderSingleGroups) {
        return new PlannerGraphProperty(false, renderSingleGroups);
    }

    /**
     * Constructor.
     *
     * Creates a property object that can be passed into a {@link RelationalExpression} visitor to create an
     * internal planner graph.
     *
     * @param isForExplain indicates if this property is computed for the purpose of creating an explain
     *        of the execution plan.
     * @param renderSingleGroups indicates if {@link ExpressionRef} instances that contain exactly one variation
     *        are rendered.
     */
    private PlannerGraphProperty(final boolean isForExplain,
                                 final boolean renderSingleGroups) {
        this.isForExplain = isForExplain;
        this.renderSingleGroups = renderSingleGroups;
    }

    @Nonnull
    @Override
    public PlannerGraph evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<PlannerGraph> childGraphs) {
        if (expression instanceof PlannerGraphRewritable) {
            return ((PlannerGraphRewritable)expression).rewritePlannerGraph(childGraphs);
        } else if (isForExplain && expression instanceof ExplainPlannerGraphRewritable) {
            return ((ExplainPlannerGraphRewritable)expression).rewriteExplainPlannerGraph(childGraphs);
        } else if (!isForExplain && expression instanceof InternalPlannerGraphRewritable) {
            return ((InternalPlannerGraphRewritable)expression).rewriteInternalPlannerGraph(childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(expression,
                            expression.getClass().getSimpleName(),
                            ImmutableList.of(),
                            ImmutableMap.of()),
                    childGraphs);
        }
    }

    @Nonnull
    @Override
    public PlannerGraph evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<PlannerGraph> memberResults) {
        if (memberResults.isEmpty()) {
            // should not happen
            return PlannerGraph.builder(new PlannerGraph.ExpressionRefHeadNode(ref)).build();
        }
        if (renderSingleGroups || memberResults.size() > 1) {
            final PlannerGraph.Node head = new PlannerGraph.ExpressionRefHeadNode(ref);
            final PlannerGraph.InternalPlannerGraphBuilder plannerGraphBuilder =
                    PlannerGraph.builder(head);

            final List<PlannerGraph> memberGraphs =
                    memberResults
                            .stream()
                            .map(childGraph -> {
                                final PlannerGraph.Node member = new PlannerGraph.ExpressionRefMemberNode();
                                return PlannerGraph.builder(member)
                                        .addGraph(childGraph)
                                        .addEdge(childGraph.getRoot(), member, new PlannerGraph.GroupExpressionRefEdge())
                                        .build();
                            })
                            .collect(Collectors.toList());

            memberGraphs.forEach(memberGraph -> {
                plannerGraphBuilder.addGraph(memberGraph);
                plannerGraphBuilder.addEdge(memberGraph.getRoot(), head, new PlannerGraph.GroupExpressionRefInternalEdge());
            });
            return plannerGraphBuilder.build();
        } else { // !renderSingleGroups && memberResults.size() == 1
            return Iterables.getOnlyElement(memberResults);
        }
    }
}
