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

package com.apple.foundationdb.record.query.plan.temp.explain;

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.Network;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    public static PlannerGraph fromNodeAndChildGraphs(@Nonnull final Node node,
                                                      @Nonnull final List<? extends PlannerGraph> childGraphs) {
        final InternalPlannerGraphBuilder plannerGraphBuilder =
                builder(node);

        // Traverse results from children and create graph edges. Hand in the directly preceding edge
        // in the dependsOn set. That in turn causes the dot exporter to render the graph left to right which
        // is important for join order, among other things.
        Edge previousEdge = null;
        for (final PlannerGraph childGraph : childGraphs) {
            final GroupExpressionRefEdge edge =
                    new GroupExpressionRefEdge(previousEdge == null
                                                            ? ImmutableSet.of()
                                                            : ImmutableSet.of(previousEdge));
            plannerGraphBuilder
                    .addGraph(childGraph)
                    .addEdge(childGraph.getRoot(), plannerGraphBuilder.getRoot(), edge);
            previousEdge = edge;
        }
        return plannerGraphBuilder.build();
    }

    /**
     * Specific builder for explain planner graph building.
     */
    public static class InternalPlannerGraphBuilder extends PlannerGraphBuilder<Node, Edge, PlannerGraph> {
        public InternalPlannerGraphBuilder(final Node root) {
            super(root);
        }

        @Nonnull
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
        @Nonnull
        private final Map<String, Attribute> additionalAttributes;

        public Node(@Nonnull final Object identity, @Nonnull final String name) {
            this(identity, name, null);
        }

        public Node(@Nonnull final Object identity, @Nonnull final String name, @Nullable final List<String> details) {
            this(identity, name, details, ImmutableMap.of());
        }

        public Node(@Nonnull final Object identity, @Nonnull final String name, @Nullable final List<String> details, @Nonnull final Map<String, Attribute> additionalAttributes) {
            super(identity, name, details);
            this.additionalAttributes = ImmutableMap.copyOf(additionalAttributes);
        }

        @Nonnull
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
                    .build();
        }

        @Nonnull
        public String getColor() {
            return "black";
        }

        @Nonnull
        public String getShape() {
            return "plain";
        }

        @Nonnull
        public String getStyle() {
            return "solid";
        }

        @Nonnull
        public String getFillColor() {
            return "black";
        }

        @Nonnull
        public String getFontName() {
            return "courier";
        }

        @Nonnull
        public String getFontSize() {
            return getDetails() == null || getDetails().isEmpty()
                   ? "12"
                   : "8";
        }
    }

    /**
     * Interface to be implemented by all node classes that need to externalize an info id for exporting global
     * information such as names and descriptions.
     */
    public interface WithInfoId {
        @Nonnull
        String getInfoId();
    }

    /**
     * Node class that additionally captures a reference to a {@link NodeInfo}. {@code NodeInfo}s are used to provide
     * names, descriptions, and other cues to the exporter that are specific to the kind of node, not the node itself.
     */
    @SuppressWarnings("squid:S2160")
    public static class NodeWithInfo extends Node implements WithInfoId {
        @Nonnull
        private NodeInfo nodeInfo;

        @SuppressWarnings("unused")
        public NodeWithInfo(@Nonnull final Object identity, @Nonnull final NodeInfo nodeInfo) {
            this(identity, nodeInfo, null);
        }

        public NodeWithInfo(@Nonnull final Object identity, @Nonnull final NodeInfo nodeInfo, @Nullable final List<String> details) {
            this(identity, nodeInfo, details, ImmutableMap.of());
        }

        public NodeWithInfo(@Nonnull final Object identity, @Nonnull final NodeInfo nodeInfo, @Nullable final List<String> details, @Nonnull final Map<String, Attribute> additionalAttributes) {
            super(identity, nodeInfo.getName(), details, additionalAttributes);
            this.nodeInfo = nodeInfo;
        }

        @Nonnull
        public NodeInfo getNodeInfo() {
            return nodeInfo;
        }

        @Override
        public String getInfoId() {
            return getNodeInfo().getId();
        }

        @Nonnull
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
        public DataNodeWithInfo(@Nonnull final NodeInfo nodeInfo) {
            this(nodeInfo, null);
        }

        public DataNodeWithInfo(@Nonnull final NodeInfo nodeInfo, @Nullable final List<String> sources) {
            this(nodeInfo, sources, ImmutableMap.of());
        }

        public DataNodeWithInfo(@Nonnull final NodeInfo nodeInfo, @Nullable final List<String> sources, @Nonnull final Map<String, Attribute> additionalAttributes) {
            super(new Object(), nodeInfo, sources, additionalAttributes);
        }

        @Nonnull
        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("data"))
                    .build();
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
     * Node class for actual plan operators.
     */
    @SuppressWarnings("squid:S2160")
    public static class OperatorNodeWithInfo extends NodeWithInfo {
        public OperatorNodeWithInfo(@Nonnull final RecordQueryPlan recordQueryPlan, @Nonnull final NodeInfo nodeInfo) {
            this(recordQueryPlan, nodeInfo, null);
        }

        public OperatorNodeWithInfo(@Nonnull final RecordQueryPlan recordQueryPlan,
                                    @Nonnull final NodeInfo nodeInfo,
                                    @Nullable final List<String> details) {
            this(recordQueryPlan, nodeInfo, details, ImmutableMap.of());
        }

        public OperatorNodeWithInfo(@Nonnull final RecordQueryPlan recordQueryPlan,
                                    @Nonnull final NodeInfo nodeInfo,
                                    @Nullable final List<String> details,
                                    @Nonnull final Map<String, Attribute> additionalAttributes) {
            super(recordQueryPlan, nodeInfo, details, additionalAttributes);
        }
        
        @Nonnull
        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("operator"))
                    .build();
        }
    }

    /**
     * Node class for logical operators.
     */
    @SuppressWarnings("squid:S2160")
    public static class LogicalOperatorNode extends Node {
        public LogicalOperatorNode(final String name) {
            this(name, null);
        }

        public LogicalOperatorNode(final String name, @Nullable final List<String> details) {
            this(name, details, ImmutableMap.of());
        }

        public LogicalOperatorNode(final String name, @Nullable final List<String> details, final Map<String, Attribute> additionalAttributes) {
            super(new Object(), name, details, additionalAttributes);
        }

        public LogicalOperatorNode(final RelationalExpression relationalExpression,
                                   final String name,
                                   @Nullable final List<String> details,
                                   final Map<String, Attribute> additionalAttributes) {
            super(relationalExpression, name, details, additionalAttributes);
        }

        @Nonnull
        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("operator"))
                    .build();
        }

        @Nonnull
        @Override
        public String getStyle() {
            return "filled";
        }

        @Nonnull
        @Override
        public String getFillColor() {
            return "darkseagreen2";
        }
    }

    /**
     * Node class for logical operators that also have a {@link NodeInfo}.
     */
    @SuppressWarnings("squid:S2160")
    public static class LogicalOperatorNodeWithInfo extends NodeWithInfo {
        public LogicalOperatorNodeWithInfo(final NodeInfo nodeInfo) {
            this(nodeInfo, null);
        }

        public LogicalOperatorNodeWithInfo(final NodeInfo nodeInfo, @Nullable final List<String> details) {
            this(nodeInfo, details, ImmutableMap.of());
        }

        public LogicalOperatorNodeWithInfo(final NodeInfo nodeInfo, @Nullable final List<String> details, final Map<String, Attribute> additionalAttributes) {
            super(new Object(), nodeInfo, details, additionalAttributes);
        }

        public LogicalOperatorNodeWithInfo(final RelationalExpression relationalExpression,
                                           final NodeInfo nodeInfo,
                                           @Nullable final List<String> details,
                                           final Map<String, Attribute> additionalAttributes) {
            super(relationalExpression, nodeInfo, details, additionalAttributes);
        }

        @Nonnull
        @Override
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("classifier", Attribute.gml("operator"))
                    .build();
        }

        @Nonnull
        @Override
        public String getStyle() {
            return "filled";
        }

        @Nonnull
        @Override
        public String getFillColor() {
            return "darkseagreen2";
        }
    }

    /**
     * Node class for GroupExpressionRefs -- head.
     */
    public static class ExpressionRefHeadNode extends Node {

        public ExpressionRefHeadNode(final ExpressionRef<? extends RelationalExpression> ref) {
            super(ref.getMembers(), ExpressionRef.class.getSimpleName());
        }

        @Nonnull
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

        @Nonnull
        @Override
        public String getFontSize() {
            return "6";
        }
    }

    /**
     * Node class for GroupExpressionRefs - member.
     */
    public static class ExpressionRefMemberNode extends Node {
        public ExpressionRefMemberNode() {
            super(new Object(), ExpressionRef.class.getSimpleName());
        }

        @Nonnull
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

        @Nonnull
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

        @Nonnull
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
                    .build();
        }

        @Nonnull
        public String getColor() {
            return "black";
        }

        @Nonnull
        public String getStyle() {
            return "solid";
        }

        @Nonnull
        public String getFontName() {
            return "courier";
        }

        @Nonnull
        public String getFontSize() {
            return "8";
        }
    }

    /**
     * Edge class for GroupExpressionRefs.
     */
    public static class GroupExpressionRefEdge extends Edge {
        public GroupExpressionRefEdge() {
            this(ImmutableSet.of());
        }

        public GroupExpressionRefEdge(final Set<? extends AbstractEdge> dependsOn) {
            super(dependsOn);
        }

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
        public Map<String, Attribute> getAttributes() {
            final Map<String, Attribute> attributes = super.getAttributes();
            return ImmutableMap
                    .<String, Attribute>builder()
                    .putAll(attributes)
                    .put("constraint", Attribute.dot("false"))
                    .build();
        }

        @Nonnull
        @Override
        public String getStyle() {
            return "invis";
        }
    }

    public static InternalPlannerGraphBuilder builder(final Node root) {
        return new InternalPlannerGraphBuilder(root);
    }

    protected PlannerGraph(final Node root, final Network<Node, Edge> network) {
        super(root, network);
    }
}
