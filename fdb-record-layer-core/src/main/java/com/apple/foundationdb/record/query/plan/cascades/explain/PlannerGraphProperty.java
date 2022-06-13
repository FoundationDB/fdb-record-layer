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

package com.apple.foundationdb.record.query.plan.cascades.explain;

import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRefTraversal;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.GraphExporter.Cluster;
import com.apple.foundationdb.record.query.plan.cascades.explain.GraphExporter.ComponentIdProvider;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph.Edge;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph.Node;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph.PartialMatchEdge;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.Network;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class to hold a graph for explain, optimization, and rewrite purposes.
 */
@SuppressWarnings({"UnstableApiUsage"})
public class PlannerGraphProperty implements ExpressionProperty<PlannerGraph>, RelationalExpressionVisitorWithDefaults<PlannerGraph> {

    public static final int EMPTY_FLAGS                = 0x0000;

    /**
     * Indicates if this property is computed for the purpose of creating an explain
     * of the execution plan.
     */
    public static final int FOR_EXPLAIN                = 0x0001;

    /**
     * Indicates if {@link ExpressionRef} instances that contain exactly one variation
     * are rendered.
     */
    public static final int RENDER_SINGLE_GROUPS       = 0x0002;
    public static final int REMOVE_PLANS               = 0x0004;
    public static final int REMOVE_LOGICAL_EXPRESSIONS = 0x0008;

    private final int flags;

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
     * Validate the flags upon instance creations.
     * @param flags the flags to validate
     */
    private static boolean validateFlags(final int flags) {
        if ((flags & REMOVE_PLANS) != 0 && (flags & REMOVE_LOGICAL_EXPRESSIONS) != 0) {
            return false;
        }
        return true;
    }

    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param renderSingleGroups iff true group references with just one member are not rendered
     * @param relationalExpression the planner expression to be rendered.
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String show(final boolean renderSingleGroups, @Nonnull final RelationalExpression relationalExpression) {
        return show(renderSingleGroups ? RENDER_SINGLE_GROUPS : 0, relationalExpression);
    }

    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param renderSingleGroups iff true group references with just one member are not rendered
     * @param rootReference the planner expression to be rendered.
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String show(final boolean renderSingleGroups, @Nonnull final ExpressionRef<? extends RelationalExpression> rootReference) {
        return show(renderSingleGroups ? RENDER_SINGLE_GROUPS : 0, rootReference);
    }

    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param flags iff true group references with just one member are not rendered
     * @param relationalExpression the planner expression to be rendered.
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String show(final int flags, @Nonnull final RelationalExpression relationalExpression) {
        final PlannerGraph plannerGraph =
                Objects.requireNonNull(relationalExpression.acceptPropertyVisitor(new PlannerGraphProperty(flags)));
        final String dotString = exportToDot(plannerGraph);
        return show(dotString);
    }


    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param flags rendering options
     * @param rootReference the planner expression to be rendered.
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String show(final int flags, @Nonnull final ExpressionRef<? extends RelationalExpression> rootReference) {
        final PlannerGraph plannerGraph =
                Objects.requireNonNull(rootReference.acceptPropertyVisitor(new PlannerGraphProperty(flags)));
        final String dotString = exportToDot(plannerGraph);
        return show(dotString);
    }

    /**
     * Show the planner expression that and all the match candidates rendered in your default browser. This also
     * shows {@link PartialMatch}es between references if they exist.
     * @param renderSingleGroups iff true group references with just one member are not rendered
     * @param queryPlanRootReference the planner expression to be rendered.
     * @param matchCandidates a set of candidates for matching which should also be shown
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String show(final boolean renderSingleGroups,
                              @Nonnull final ExpressionRef<? extends RelationalExpression> queryPlanRootReference,
                              @Nonnull final Set<MatchCandidate> matchCandidates) {
        final PlannerGraph queryPlannerGraph =
                Objects.requireNonNull(queryPlanRootReference.acceptPropertyVisitor(forInternalShow(renderSingleGroups, true)));

        final PlannerGraph.InternalPlannerGraphBuilder graphBuilder = queryPlannerGraph.derived();

        final Map<MatchCandidate, PlannerGraph> matchCandidateMap = matchCandidates.stream()
                .collect(ImmutableMap.toImmutableMap(Function.identity(), matchCandidate -> Objects.requireNonNull(
                        matchCandidate.getTraversal().getRootReference().acceptPropertyVisitor(forInternalShow(renderSingleGroups)))));

        matchCandidateMap.forEach((matchCandidate, matchCandidateGraph) -> graphBuilder.addGraph(matchCandidateGraph));

        final ExpressionRefTraversal queryGraphTraversal = ExpressionRefTraversal.withRoot(queryPlanRootReference);
        final Set<ExpressionRef<? extends RelationalExpression>> queryGraphRefs = queryGraphTraversal.getRefs();

        queryGraphRefs
                .forEach(queryGraphRef -> {
                    for (final MatchCandidate matchCandidate : Sets.intersection(matchCandidates, queryGraphRef.getMatchCandidates())) {
                        final Set<PartialMatch> partialMatchesForCandidate = queryGraphRef.getPartialMatchesForCandidate(matchCandidate);
                        final PlannerGraph matchCandidatePlannerGraph = Objects.requireNonNull(matchCandidateMap.get(matchCandidate));
                        final Node queryRefNode = Objects.requireNonNull(queryPlannerGraph.getNodeForIdentity(queryGraphRef));
                        for (final PartialMatch partialMatchForCandidate : partialMatchesForCandidate) {
                            @Nullable final Node matchCandidateNode = matchCandidatePlannerGraph.getNodeForIdentity(partialMatchForCandidate.getCandidateRef());
                            // should always be true but we don't want to bail out for corrupt graphs
                            if (matchCandidateNode != null) {
                                graphBuilder.addEdge(queryRefNode, matchCandidateNode, new PartialMatchEdge());
                            }
                        }
                    }
                });

        final String dotString = exportToDot(graphBuilder.build(),
                queryPlannerGraph.getNetwork().nodes(),
                nestedClusterProvider -> matchCandidateMap
                        .entrySet()
                        .stream()
                        .map(entry -> new NamedCluster(entry.getKey().getName(), entry.getValue().getNetwork().nodes(), nestedClusterProvider))
                        .collect(Collectors.toList()));
        return show(dotString);
    }

    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param dotString graph serialized as dot-compatible string
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    private static String show(final String dotString) {
        try {
            final URI uri = PlannerGraphProperty.createHtmlLauncher(dotString);
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
     * @param dotString -- a serialized planner graph
     * @return a URI pointing to an html file in a temp location which renders the graph
     * @throws Exception -- thrown from methods called in here.
     */
    @Nonnull
    public static URI createHtmlLauncher(@Nonnull final String dotString) throws Exception {
        final String launcherHtmlString;
        try (InputStream launcherHtmlInputStream = PlannerGraphProperty.class.getResourceAsStream("/showPlannerExpression.html")) {
            launcherHtmlString = CharStreams.toString(new InputStreamReader(launcherHtmlInputStream, StandardCharsets.UTF_8))
                .replace("$DOT", dotString);
        }
        final File launcherTempFile = File.createTempFile("local_launcher-", ".html", new File(System.getProperty("java.io.tmpdir")));
        final String launcherTempFileName = launcherTempFile.toString();
        try {
            try (PrintWriter writer = new PrintWriter(launcherTempFile, "UTF-8")) {
                writer.print(launcherHtmlString);
            }
        } catch (Exception e) {
            throw new Exception("Error writing file: " + launcherTempFile.getAbsolutePath() + " : " + e.getMessage(), e);
        }
        return new URI("file:///" + launcherTempFileName.replace("\\", "/"));
    }

    /**
     * Creates a serialized format of this graph as a dot-compatible definition.
     *
     * @param plannerGraph the planner graph we should export to dot
     * @return the graph as string in dot format.
     */
    @Nonnull
    public static String exportToDot(@Nonnull final AbstractPlannerGraph<Node, Edge> plannerGraph) {
        final ImmutableNetwork<Node, Edge> network = plannerGraph.getNetwork();
        return exportToDot(plannerGraph, network.nodes(), clusterProvider -> ImmutableList.of());
    }

    /**
     * Creates a serialized format of this graph as a dot-compatible definition.
     * @param plannerGraph the planner graph we should export to dot
     * @param queryPlannerNodes set of nodes which is a subset of nodes of {@code plannerGraph} which represents the
     *        actual query graph
     * @param clusteringFunction function to partition the planner graph into clusters. Clusters are not isolated
     *        sub-graphs in this context. It is possible and often desirable for a use case to define edges between
     *        nodes of clusters. Clusters are used by the dot exporter to
     *        <ul>
     *            <li>assign common attributes to all nodes and edges, e.g. like a common gray background</li>
     *            <li>assign a name that is displayed displayed</li>
     *            <li>cause the layout algorithm to pack the nodes in a cluster if they were one big node</li>
     *        </ul>
     * @return the graph as string in dot format.
     */
    @Nonnull
    public static String exportToDot(@Nonnull final AbstractPlannerGraph<Node, Edge> plannerGraph,
                                     @Nonnull final Set<Node> queryPlannerNodes,
                                     @Nonnull final Function<GraphExporter.ClusterProvider<Node, Edge>, Collection<Cluster<Node, Edge>>> clusteringFunction) {
        final GraphExporter<Node, Edge> exporter = new DotExporter<>(new CountingIdProvider<>(),
                Node::getAttributes,
                Edge::getAttributes,
                ImmutableMap.of("fontname", Attribute.dot("courier"),
                        "rankdir", Attribute.dot("BT"),
                        "splines", Attribute.dot("polyline")),
                (network, nodes) -> {
                    final ImmutableList.Builder<Cluster<Node, Edge>> clusterBuilder = ImmutableList.builder();
                    clusterBuilder.addAll(clustersForGroups(plannerGraph.getNetwork(), queryPlannerNodes));
                    clusterBuilder.addAll(clusteringFunction.apply(PlannerGraphProperty::clustersForGroups));
                    return clusterBuilder.build();
                });
        // export as string
        final Writer writer = new StringWriter();
        exporter.exportGraph(plannerGraph.getNetwork(), writer);
        return writer.toString();
    }

    private static class GroupCluster extends Cluster<Node, Edge> {
        public GroupCluster(@Nonnull String label, @Nonnull final Set<Node> nodes) {
            super(nodes,
                    node -> ImmutableMap.<String, Attribute>builder()
                            .put("style", Attribute.dot("filled"))
                            .put("fillcolor", Attribute.dot("lightgrey"))
                            .put("fontsize", Attribute.dot("6"))
                            .put("rank", Attribute.dot("same"))
                            .put("label", Attribute.dot(label))
                            .build(),
                    (network, nestedNodes) -> ImmutableList.of());
        }
    }

    /**
     * Class to represent an actual sub cluster of inside the planner graph.
     */
    public static class NamedCluster extends Cluster<Node, Edge> {
        public NamedCluster(@Nonnull String label,
                            @Nonnull final Set<Node> nodes,
                            @Nonnull GraphExporter.ClusterProvider<Node, Edge> nestedClusterProvider) {
            super(nodes,
                    node -> ImmutableMap.<String, Attribute>builder()
                            .put("style", Attribute.dot("filled"))
                            .put("fillcolor", Attribute.dot("gray95"))
                            .put("pencolor", Attribute.dot("gray95"))
                            .put("rank", Attribute.dot("same"))
                            .put("label", Attribute.dot(label))
                            .build(),
                    nestedClusterProvider);
        }
    }

    private static Collection<Cluster<Node, Edge>> clustersForGroups(final Network<Node, Edge> network, Set<Node> nodes) {
        final Map<PlannerGraph.ExpressionRefHeadNode, Set<Node>> clusterMap = nodes
                .stream()
                .filter(node -> node instanceof PlannerGraph.ExpressionRefHeadNode || node instanceof PlannerGraph.ExpressionRefMemberNode)
                .collect(Collectors.groupingBy(node -> {
                    if (node instanceof PlannerGraph.ExpressionRefHeadNode) {
                        return (PlannerGraph.ExpressionRefHeadNode)node;
                    }
                    if (node instanceof PlannerGraph.ExpressionRefMemberNode) {
                        final Node head =
                                network.incidentNodes(Iterables.getOnlyElement(network.outEdges(node)))
                                        .nodeV();
                        Verify.verify(head instanceof PlannerGraph.ExpressionRefHeadNode);
                        return (PlannerGraph.ExpressionRefHeadNode)head;
                    }
                    throw new IllegalArgumentException("impossible case");
                }, Collectors.toSet()));

        return clusterMap.entrySet()
                .stream()
                .map(entry -> {
                    final String label = Debugger.mapDebugger(debugger -> debugger.nameForObject(entry.getKey().getIdentity()))
                            .orElse("group");
                    return new GroupCluster(label, entry.getValue());
                })
                .collect(Collectors.toList());
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

        final GraphExporter<Node, Edge> exporter =
                createGmlExporter(infoMap);
        // export as string
        final Writer writer = new StringWriter();
        exporter.exportGraph(plannerGraph.getNetwork(), writer);
        return writer.toString();
    }

    @Nonnull
    private static GraphExporter<Node, Edge> createGmlExporter(@Nonnull final Map<String, Attribute> infoMap) {
        return new GmlExporter<>(new CountingIdProvider<>(),
                Node::getAttributes,
                new CountingIdProvider<>(),
                Edge::getAttributes,
                ImmutableMap.of("infos", Attribute.gml(infoMap)));
    }

    public static PlannerGraphProperty forExplain() {
        return new PlannerGraphProperty(FOR_EXPLAIN);
    }

    public static PlannerGraphProperty forInternalShow(final boolean renderSingleGroups) {
        return forInternalShow(renderSingleGroups, false);
    }

    public static PlannerGraphProperty forInternalShow(final boolean renderSingleGroups, final boolean removePlans) {
        return new PlannerGraphProperty((renderSingleGroups ? RENDER_SINGLE_GROUPS : 0) |
                                        (removePlans ? REMOVE_PLANS : 0));
    }

    /**
     * Private constructor.
     *
     * Creates a property object that can be passed into a {@link RelationalExpression} visitor to create an
     * internal planner graph.
     *
     * @param flags for options
     */
    private PlannerGraphProperty(final int flags) {
        Preconditions.checkArgument(validateFlags(flags));
        this.flags = flags;
    }

    public boolean isForExplain() {
        return (flags & FOR_EXPLAIN) != 0;
    }

    public boolean renderSingleGroups() {
        return (flags & RENDER_SINGLE_GROUPS) != 0;
    }

    public boolean removePlansIfPossible() {
        return (flags & REMOVE_PLANS) != 0;
    }

    public boolean removeLogicalExpressions() {
        return (flags & REMOVE_LOGICAL_EXPRESSIONS) != 0;
    }

    @Nonnull
    @Override
    public PlannerGraph evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<PlannerGraph> childGraphs) {
        if (expression instanceof PlannerGraphRewritable) {
            return ((PlannerGraphRewritable)expression).rewritePlannerGraph(childGraphs);
        } else if (isForExplain() && expression instanceof ExplainPlannerGraphRewritable) {
            return ((ExplainPlannerGraphRewritable)expression).rewriteExplainPlannerGraph(childGraphs);
        } else if (!isForExplain() && expression instanceof InternalPlannerGraphRewritable) {
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
    public PlannerGraph evaluateAtRef(@Nonnull final ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<PlannerGraph> memberResults) {
        if (memberResults.isEmpty()) {
            // should not happen -- but we don't want to bail
            return PlannerGraph.builder(new PlannerGraph.ExpressionRefHeadNode(ref)).build();
        }

        if (removePlansIfPossible()) {
            final List<PlannerGraph> filteredMemberResults = memberResults.stream()
                    .filter(graph -> graph.getRoot() instanceof PlannerGraph.WithExpression)
                    .filter(graph -> {
                        final RelationalExpression expression = ((PlannerGraph.WithExpression)graph.getRoot()).getExpression();
                        return !(expression instanceof RecordQueryPlan);
                    })
                    .collect(Collectors.toList());

            // if we filtered down to empty it is better to just show the physical plan, otherwise try to avoid it
            if (!filteredMemberResults.isEmpty()) {
                memberResults = filteredMemberResults;
            }
        } else if (removeLogicalExpressions()) {
            final List<PlannerGraph> filteredMemberResults = memberResults.stream()
                    .filter(graph -> graph.getRoot() instanceof PlannerGraph.WithExpression)
                    .filter(graph -> ((PlannerGraph.WithExpression)graph.getRoot()).getExpression() instanceof RecordQueryPlan)
                    .collect(Collectors.toList());

            // if we filtered down to empty it is better to just show the physical plan, otherwise try to avoid it
            if (!filteredMemberResults.isEmpty()) {
                memberResults = filteredMemberResults;
            }
        }

        if (renderSingleGroups() || memberResults.size() > 1) {
            final Node head = new PlannerGraph.ExpressionRefHeadNode(ref);
            final PlannerGraph.InternalPlannerGraphBuilder plannerGraphBuilder =
                    PlannerGraph.builder(head);

            final List<PlannerGraph> memberGraphs =
                    memberResults
                            .stream()
                            .map(childGraph -> {
                                final Node root = childGraph.getRoot();
                                final Optional<String> debugNameOptional =
                                        Debugger.mapDebugger(debugger -> {
                                            if (root instanceof PlannerGraph.WithExpression) {
                                                final PlannerGraph.WithExpression withExpression = (PlannerGraph.WithExpression)root;
                                                @Nullable final RelationalExpression expression = withExpression.getExpression();
                                                return expression == null ? null : debugger.nameForObject(expression);
                                            }
                                            return null;
                                        });

                                final Node member =
                                        debugNameOptional.map(PlannerGraph.ExpressionRefMemberNode::new)
                                                .orElse(new PlannerGraph.ExpressionRefMemberNode());
                                return PlannerGraph.builder(member)
                                        .addGraph(childGraph)
                                        .addEdge(root, member, new PlannerGraph.GroupExpressionRefEdge())
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
