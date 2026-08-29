/*
 * PlannerGraph.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.graph.Network;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Lightweight class to save some boilerplate.
 */
@SuppressWarnings("UnstableApiUsage")
public class PlannerGraph extends AbstractPlannerGraph<PlannerGraph.Node, PlannerGraph.Edge> {

    public static PlannerGraph fromNodeAndChildGraphs(final Node node,
                                                      final List<? extends PlannerGraph> childGraphs) {
        final List<? extends Quantifier> quantifiers = tryGetQuantifiers(node);
        if (quantifiers.isEmpty() || quantifiers.size() != childGraphs.size()) {
            return fromNodeAndChildGraphs(node, childGraphs, null);
        }

        final List<? extends Quantifier> sortedQuantifiers = Quantifiers.anyTopologicalOrderPermutation(quantifiers);

        final InternalPlannerGraphBuilder plannerGraphBuilder =
                builder(node);

        final Map<CorrelationIdentifier, Edge> aliasToEdgeMap = Maps.newHashMap();
        for (final Quantifier quantifier : sortedQuantifiers) {
            final int index = Streams.mapWithIndex(quantifiers.stream(),
                    (q, i) -> q == quantifier ? (int)i : -1)
                    .filter(i -> i >= 0)
                    .findFirst()
                    .orElseThrow(() -> new RecordCoreException("should have found the quantifier")); // not great, not terrible

            final Set<? extends AbstractEdge> dependsOn =
                    quantifier.getCorrelatedTo()
                            .stream()
                            .filter(aliasToEdgeMap::containsKey)
                            .map(aliasToEdgeMap::get)
                            .collect(ImmutableSet.toImmutableSet());

            final PlannerGraph childGraph = childGraphs.get(index);
            @Nullable final String label =
                    Debugger.mapDebugger(debugger -> quantifier.getAlias().getId()).orElse(null);

            final ReferenceEdge edge;
            if (quantifier instanceof Quantifier.Existential) {
                edge = new ExistentialQuantifierEdge(label, dependsOn);
            } else if (quantifier instanceof Quantifier.ForEach) {
                edge = new ForEachQuantifierEdge(label, ((Quantifier.ForEach)quantifier).isNullOnEmpty(), dependsOn);
            } else if (quantifier instanceof Quantifier.Physical) {
                edge = new PhysicalQuantifierEdge(label, dependsOn);
            } else {
                edge = new ReferenceEdge(label, dependsOn);
            }

            plannerGraphBuilder
                    .addGraph(childGraph)
                    .addEdge(childGraph.getRoot(), plannerGraphBuilder.getRoot(), edge);

            aliasToEdgeMap.put(quantifier.getAlias(), edge);
        }

        return plannerGraphBuilder.build();
    }

    public static PlannerGraph fromNodeAndChildGraphs(final Node node,
                                                      final List<? extends PlannerGraph> sortedChildGraphs,
                                                      @Nullable final List<? extends Quantifier> sortedQuantifiers) {
        // quantifiers are either not given or are of the same cardinality as child graphs
        Preconditions.checkArgument(sortedQuantifiers == null || sortedQuantifiers.size() == sortedChildGraphs.size());

        final InternalPlannerGraphBuilder plannerGraphBuilder =
                builder(node);

        // Traverse results from children and create graph edges. Hand in the directly preceding edge
        // in the dependsOn set. That in turn causes the dot exporter to render the graph left to right which
        // is important for join order, among other things.
        Edge previousEdge = null;
        for (int i = 0; i < sortedChildGraphs.size(); i++) {
            final PlannerGraph childGraph = sortedChildGraphs.get(i);
            final ReferenceEdge edge;
            final Set<? extends AbstractEdge> dependsOn =
                    previousEdge == null
                    ? ImmutableSet.of()
                    : ImmutableSet.of(previousEdge);

            @Nullable final String label;
            if (sortedQuantifiers != null) {
                final var quantifier = sortedQuantifiers.get(i);
                label = Debugger.mapDebugger(debugger -> quantifier.getAlias().getId()).orElse(null);
            } else {
                label = null;
            }

            edge = new ReferenceEdge(label, dependsOn);

            plannerGraphBuilder
                    .addGraph(childGraph)
                    .addEdge(childGraph.getRoot(), plannerGraphBuilder.getRoot(), edge);
            previousEdge = edge;
        }
        return plannerGraphBuilder.build();
    }

    public static PlannerGraph fromNodeInnerAndTargetForModifications(final Node node,
                                                                      final PlannerGraph innerGraph,
                                                                      final PlannerGraph targetGraph) {
        final InternalPlannerGraphBuilder plannerGraphBuilder =
                builder(node);

        ReferenceEdge edge;
        edge = new ReferenceEdge(null, ImmutableSet.of());
        plannerGraphBuilder
                .addGraph(innerGraph)
                .addEdge(innerGraph.getRoot(), plannerGraphBuilder.getRoot(), edge);
        edge = new ModificationTargetEdge(null, ImmutableSet.of(edge));
        plannerGraphBuilder
                .addGraph(targetGraph)
                .addEdge(targetGraph.getRoot(), plannerGraphBuilder.getRoot(), edge);
        return plannerGraphBuilder.build();
    }

    static Optional<? extends RelationalExpression> tryGetExpression(final Node node) {
        if (node instanceof WithExpression) {
            @Nullable final RelationalExpression expression = ((WithExpression)node).getExpression();
            if (expression != null) {
                return Optional.of(expression);
            }
        }
        return Optional.empty();
    }

    static List<? extends Quantifier> tryGetQuantifiers(final Node node) {
        if (node instanceof WithExpression) {
            @Nullable final RelationalExpression expression = ((WithExpression)node).getExpression();
            if (expression != null) {
                return expression.getQuantifiers();
            }
        }
        return ImmutableList.of();
    }

    /**
     * Specific builder for explain planner graph building.
     */
    public static class InternalPlannerGraphBuilder extends PlannerGraphBuilder<Node, Edge, PlannerGraph> {
        public InternalPlannerGraphBuilder(final Node root) {
            super(root);
        }

        public InternalPlannerGraphBuilder(final AbstractPlannerGraph<Node, Edge> original) {
            super(original);
        }

        @Override
        public PlannerGraph build() {
            return new PlannerGraph(getRoot(), getNetwork());
        }
    }

    /**
     * Node class functioning as parent for any nodes in the network.
     */
    @SuppressWarnings("squid:S2160")
    public static class Node extends AbstractPlannerGraph.AbstractNode {
        private final Map<String, Attribute> additionalAttributes;

        public Node(final Object identity, final String name) {
            this(identity, name, null);
        }

        public Node(final Object identity, final String name, @Nullable final List<String> details) {
            this(identity, name, details, ImmutableMap.of());
        }

        public Node(final Object identity, final String name, @Nullable final List<String> details, final Map<String, Attribute> additionalAttributes) {
            super(identity, name, details);
            this.additionalAttributes = ImmutableMap.copyOf(additionalAttributes);
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final ImmutableMap.Builder<String, Attribute> builder = ImmutableMap.builder();
            Optional.ofNullable(getDetails())
                    .ifPresent(details -> builder.put("details",
                            Attribute.invisible(
                                    getDetails().stream()
                                            .map(Attribute::common)
                                            .collect(Collectors.toList()))));
            builder.putAll(additionalAttributes);

            return builder.put("name", Attribute.invisible(getName()))
                    .put("color", Attribute.dot(getColor()))
                    .put("shape", Attribute.dot(getShape()))
                    .put("style", Attribute.dot(getStyle()))
                    .put("fillcolor", Attribute.dot(getFillColor()))
                    .put("fontname", Attribute.dot(getFontName()))
                    .put("fontsize", Attribute.dot(getFontSize()))
                    .put("tooltip", Attribute.dot(getToolTip()))
                    .build();
        }

        public String getColor() {
            return "black";
        }

        public String getShape() {
            return "plain";
        }

        public String getStyle() {
            return "solid";
        }

        public String getFillColor() {
            return "black";
        }

        public String getFontName() {
            return "courier";
        }

        public String getFontSize() {
            return getDetails() == null || getDetails().isEmpty()
                   ? "12"
                   : "8";
        }

        String getToolTip() {
            return getClass().getSimpleName();
        }
    }

    /**
     * Interface to be implemented by all node classes that need to externalize an info id for exporting global
     * information such as names and descriptions.
     */
    public interface WithInfoId {
        String getInfoId();
    }

    /**
     * Interface to be implemented by all node classes that represent an {@link RelationalExpression}.
     */
    public interface WithExpression {
        @Nullable
        RelationalExpression getExpression();
    }

    /**
     * Node class that additionally captures a reference to a {@link NodeInfo}. {@code NodeInfo}s are used to provide
     * names, descriptions, and other cues to the exporter that are specific to the kind of node, not the node itself.
     */
    @SuppressWarnings("squid:S2160")
    public static class NodeWithInfo extends Node implements WithInfoId {
        private final NodeInfo nodeInfo;

        @SuppressWarnings("unused")
        public NodeWithInfo(final Object identity, final NodeInfo nodeInfo) {
            this(identity, nodeInfo, null);
        }

        public NodeWithInfo(final Object identity, final NodeInfo nodeInfo, @Nullable final List<String> details) {
            this(identity, nodeInfo, details, ImmutableMap.of());
        }

        public NodeWithInfo(final Object identity, final NodeInfo nodeInfo, @Nullable final List<String> details, final Map<String, Attribute> additionalAttributes) {
            super(identity, nodeInfo.getName(), details, additionalAttributes);
            this.nodeInfo = nodeInfo;
        }

        public NodeInfo getNodeInfo() {
            return nodeInfo;
        }

        @Override
        public String getInfoId() {
            return getNodeInfo().getId();
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("infoId", Attribute.gml(nodeInfo.getId()))
                    .build();
        }
    }

    /**
     * Node class for data objects.
     */
    @SuppressWarnings("squid:S2160")
    public static class DataNodeWithInfo extends NodeWithInfo {
        private final Type type;

        public DataNodeWithInfo(final NodeInfo nodeInfo, Type type, @Nullable final List<String> sources) {
            this(nodeInfo, type, sources, ImmutableMap.of());
        }

        public DataNodeWithInfo(final NodeInfo nodeInfo, Type type, @Nullable final List<String> sources, final Map<String, Attribute> additionalAttributes) {
            super(new Object(), nodeInfo, sources, additionalAttributes);
            this.type = type;
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("data"))
                    .build();
        }

        @Override
        public String getColor() {
            return "black";
        }

        @Override
        public String getStyle() {
            return "filled";
        }

        @Override
        public String getFillColor() {
            return "lightblue";
        }

        @Override
        String getToolTip() {
            return type.describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }
    }

    /**
     * Node class for temporary data objects.
     */
    public static class TemporaryDataNodeWithInfo extends DataNodeWithInfo {

        public TemporaryDataNodeWithInfo(final Type type, @Nullable final List<String> sources) {
            super(NodeInfo.TEMPORARY_BUFFER_DATA, type, sources);
        }

        public TemporaryDataNodeWithInfo(Type type, @Nullable final List<String> sources, final Map<String, Attribute> additionalAttributes) {
            super(NodeInfo.TEMPORARY_BUFFER_DATA, type, sources, additionalAttributes);
        }

        @Override
        public String getFillColor() {
            return "goldenrod2";
        }
    }

    /**
     * Node class for actual plan operators.
     */
    @SuppressWarnings("squid:S2160")
    public static class OperatorNodeWithInfo extends NodeWithInfo implements WithExpression {
        @Nullable
        private final RecordQueryPlan expression;

        public OperatorNodeWithInfo(final RecordQueryPlan recordQueryPlan,
                                    final NodeInfo nodeInfo) {
            this(recordQueryPlan, nodeInfo, null);
        }

        public OperatorNodeWithInfo(final RecordQueryPlan recordQueryPlan,
                                    final NodeInfo nodeInfo,
                                    @Nullable final List<String> details) {
            this(recordQueryPlan, nodeInfo, details, ImmutableMap.of());
        }

        public OperatorNodeWithInfo(final RecordQueryPlan recordQueryPlan,
                                    final NodeInfo nodeInfo,
                                    @Nullable final List<String> details,
                                    final Map<String, Attribute> additionalAttributes) {
            super(new Object(), nodeInfo, details, additionalAttributes);
            this.expression = recordQueryPlan;
        }
        
        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("operator"))
                    .build();
        }

        @Override
        String getToolTip() {
            return expression == null
                   ? "no plan"
                   : expression.getResultType().describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nullable
        @Override
        public RecordQueryPlan getExpression() {
            return expression;
        }
    }

    /**
     * Node class for actual plan operators.
     */
    @SuppressWarnings("squid:S2160")
    public static class ModificationOperatorNodeWithInfo extends OperatorNodeWithInfo {
        public ModificationOperatorNodeWithInfo(final RecordQueryPlan recordQueryPlan,
                                                final NodeInfo nodeInfo) {
            this(recordQueryPlan, nodeInfo, null);
        }

        public ModificationOperatorNodeWithInfo(final RecordQueryPlan recordQueryPlan,
                                                final NodeInfo nodeInfo,
                                                @Nullable final List<String> details) {
            this(recordQueryPlan, nodeInfo, details, ImmutableMap.of());
        }

        public ModificationOperatorNodeWithInfo(final RecordQueryPlan recordQueryPlan,
                                                final NodeInfo nodeInfo,
                                                @Nullable final List<String> details,
                                                final Map<String, Attribute> additionalAttributes) {
            super(recordQueryPlan, nodeInfo, details, additionalAttributes);
        }

        @Override
        public String getStyle() {
            return "filled";
        }

        @Override
        public String getFillColor() {
            return "lightcoral";
        }
    }

    /**
     * Node class for logical operators.
     */
    @SuppressWarnings("squid:S2160")
    public static class LogicalOperatorNode extends Node implements WithExpression {
        @Nullable
        private final RelationalExpression expression;

        public LogicalOperatorNode(@Nullable final RelationalExpression expression,
                                   final String name,
                                   @Nullable final List<String> details,
                                   final Map<String, Attribute> additionalAttributes) {
            super(new Object(), name, details, additionalAttributes);
            this.expression = expression;
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("operator"))
                    .build();
        }

        @Override
        public String getStyle() {
            return "filled";
        }

        @Override
        public String getFillColor() {
            return "darkseagreen2";
        }

        @Override
        String getToolTip() {
            return expression == null
                   ? "no expression"
                   : expression.getResultType().describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nullable
        @Override
        public RelationalExpression getExpression() {
            return expression;
        }
    }

    /**
     * Node class for logical operators that also have a {@link NodeInfo}.
     */
    @SuppressWarnings("squid:S2160")
    public static class LogicalOperatorNodeWithInfo extends NodeWithInfo implements WithExpression {
        @Nullable
        private final RelationalExpression expression;

        public LogicalOperatorNodeWithInfo(@Nullable final RelationalExpression expression,
                                           final NodeInfo nodeInfo,
                                           @Nullable final List<String> details,
                                           final Map<String, Attribute> additionalAttributes) {
            super(new Object(), nodeInfo, details, additionalAttributes);
            this.expression = expression;
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("operator"))
                    .build();
        }

        @Override
        public String getStyle() {
            return "filled";
        }

        @Override
        public String getFillColor() {
            return "darkseagreen2";
        }

        @Override
        String getToolTip() {
            return expression == null
                   ? "no expression"
                   : expression.getResultType().describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nullable
        @Override
        public RelationalExpression getExpression() {
            return expression;
        }
    }

    /**
     * Node class for logical operators that also have a {@link NodeInfo}.
     */
    public static class ModificationLogicalOperatorNode extends LogicalOperatorNodeWithInfo {
        public ModificationLogicalOperatorNode(@Nullable final RelationalExpression expression,
                                               final NodeInfo nodeInfo,
                                               @Nullable final List<String> details,
                                               final Map<String, Attribute> additionalAttributes) {
            super(expression, nodeInfo, details, additionalAttributes);
        }

        @Override
        public String getFillColor() {
            return "darkseagreen4";
        }
    }

    /**
     * Node class for {@link Reference}s -- head.
     */
    public static class ReferenceHeadNode extends Node {

        public ReferenceHeadNode(final Reference ref) {
            super(ref, Reference.class.getSimpleName());
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("label", Attribute.common(getName()))
                    .put("margin", Attribute.dot("0"))
                    .put("height", Attribute.dot("0"))
                    .put("width", Attribute.dot("0"))
                    .build();
        }

        @Override
        public String getName() {
            return "r";
        }

        @Override
        public String getShape() {
            return "circle";
        }

        @Override
        public String getStyle() {
            return "filled";
        }

        @Override
        public String getFillColor() {
            return "white";
        }

        @Override
        public String getFontSize() {
            return "6";
        }
    }

    /**
     * Node class for {@link Reference} - member.
     */
    public static class ReferenceMemberNode extends Node {
        public ReferenceMemberNode(final String name) {
            super(new Object(), name);
        }

        public ReferenceMemberNode() {
            super(new Object(), "m");
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("label", Attribute.common(getName()))
                    .put("margin", Attribute.dot("0"))
                    .put("height", Attribute.dot("0"))
                    .put("width", Attribute.dot("0"))
                    .build();
        }

        @Override
        public String getShape() {
            return "circle";
        }

        @Override
        public String getStyle() {
            return "filled";
        }

        @Override
        public String getFillColor() {
            return "white";
        }

        @Override
        public String getFontSize() {
            return "6";
        }
    }

    /**
     * Edge class.
     */
    public static class Edge extends AbstractPlannerGraph.AbstractEdge {
        public Edge() {
            this(null, ImmutableSet.of());
        }

        public Edge(@Nullable final String label) {
            this(label, ImmutableSet.of());
        }

        public Edge(final Set<? extends AbstractEdge> dependsOn) {
            this(null, dependsOn);
        }

        public Edge(@Nullable final String label, final Set<? extends AbstractEdge> dependsOn) {
            super(label, dependsOn);
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final ImmutableMap.Builder<String, Attribute> builder = ImmutableMap.builder();

            Optional.ofNullable(getLabel())
                    .ifPresent(label -> builder.put("label", Attribute.common(label)));

            return builder
                    .put("dependsOn", Attribute.gml(getDependsOn().stream().map(Attribute::common).collect(Collectors.toList())))
                    .put("color", Attribute.dot(getColor()))
                    .put("style", Attribute.dot(getStyle()))
                    .put("fontname", Attribute.dot(getFontName()))
                    .put("fontsize", Attribute.dot(getFontSize()))
                    .put("arrowhead", Attribute.dot(getArrowHead()))
                    .put("arrowtail", Attribute.dot(getArrowTail()))
                    .put("dir", Attribute.dot("both"))
                    .build();
        }

        public String getColor() {
            return "black";
        }

        public String getStyle() {
            return "solid";
        }

        public String getFontName() {
            return "courier";
        }

        public String getFontSize() {
            return "8";
        }

        public String getArrowHead() {
            return "normal";
        }

        public String getArrowTail() {
            return "none";
        }
    }

    /**
     * Edge class for {@link Reference}.
     */
    public static class ReferenceEdge extends Edge {
        public ReferenceEdge() {
            this(ImmutableSet.of());
        }

        public ReferenceEdge(final Set<? extends AbstractEdge> dependsOn) {
            this(null, dependsOn);
        }

        public ReferenceEdge(@Nullable final String label, final Set<? extends AbstractEdge> dependsOn) {
            super(label, dependsOn);
        }

        @Override
        public String getColor() {
            return "gray20";
        }
    }

    /**
     * Edge class for for-each quantifiers.
     */
    public static class ForEachQuantifierEdge extends ReferenceEdge {

        private final boolean isNullIsEmpty;

        public ForEachQuantifierEdge() {
            this(null, ImmutableSet.of());
        }

        public ForEachQuantifierEdge(final Set<? extends AbstractEdge> dependsOn) {
            this(null, dependsOn);
        }

        public ForEachQuantifierEdge(@Nullable final String label, final Set<? extends AbstractEdge> dependsOn) {
            this(label, false, dependsOn);
        }

        public ForEachQuantifierEdge(@Nullable final String label, boolean isNullIfEmpty, final Set<? extends AbstractEdge> dependsOn) {
            super(label, dependsOn);
            this.isNullIsEmpty = isNullIfEmpty;
        }

        @Override
        public String getColor() {
            return isNullIsEmpty ? "khaki3" : super.getColor();
        }
    }

    /**
     * Edge class for existential quantifiers.
     */
    public static class ExistentialQuantifierEdge extends ReferenceEdge {
        public ExistentialQuantifierEdge() {
            this(null, ImmutableSet.of());
        }

        public ExistentialQuantifierEdge(final Set<? extends AbstractEdge> dependsOn) {
            super(null, dependsOn);
        }

        public ExistentialQuantifierEdge(@Nullable final String label, final Set<? extends AbstractEdge> dependsOn) {
            super(label, dependsOn);
        }

        @Override
        public String getColor() {
            return "gray70";
        }

        @Override
        public String getArrowHead() {
            return "diamond";
        }
    }

    /**
     * Edge class for physical quantifiers.
     */
    public static class PhysicalQuantifierEdge extends ReferenceEdge {
        public PhysicalQuantifierEdge() {
            this(null, ImmutableSet.of());
        }

        public PhysicalQuantifierEdge(final Set<? extends AbstractEdge> dependsOn) {
            super(null, dependsOn);
        }

        public PhysicalQuantifierEdge(@Nullable final String label, final Set<? extends AbstractEdge> dependsOn) {
            super(label, dependsOn);
        }

        @Override
        public String getStyle() {
            return "bold";
        }
    }

    /**
     * Edge class for modification (delete, insert, update) targets.
     */
    public static class ModificationTargetEdge extends ReferenceEdge {
        public ModificationTargetEdge() {
            this(null, ImmutableSet.of());
        }

        public ModificationTargetEdge(final Set<? extends AbstractEdge> dependsOn) {
            super(null, dependsOn);
        }

        public ModificationTargetEdge(@Nullable final String label, final Set<? extends AbstractEdge> dependsOn) {
            super(label, dependsOn);
        }

        @Override
        public String getArrowHead() {
            return "none";
        }

        @Override
        public String getArrowTail() {
            return "normal";
        }
    }

    /**
     * Edge class for {@link Reference}.
     */
    public static class ReferenceInternalEdge extends Edge {
        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("constraint", Attribute.dot("false"))
                    .build();
        }

        @Override
        public String getStyle() {
            return "invis";
        }
    }

    /**
     * Edge class for matches that connect a query reference to a match candidate reference.
     */
    public static class PartialMatchEdge extends Edge {
        public PartialMatchEdge() {
            this(null);
        }

        public PartialMatchEdge(@Nullable final String label) {
            super(label);
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("constraint", Attribute.dot("false"))
                    .build();
        }

        @Override
        public String getStyle() {
            return "dashed";
        }

    }

    public static InternalPlannerGraphBuilder builder(final Node root) {
        return new InternalPlannerGraphBuilder(root);
    }

    protected PlannerGraph(final Node root, final Network<Node, Edge> network) {
        super(root, network);
    }

    public InternalPlannerGraphBuilder derived() {
        return new InternalPlannerGraphBuilder(this);
    }
}
