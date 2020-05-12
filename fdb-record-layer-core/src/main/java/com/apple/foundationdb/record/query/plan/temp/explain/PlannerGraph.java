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

import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.escape.Escaper;
import com.google.common.graph.Network;
import com.google.common.html.HtmlEscapers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Lightweight class to save some boilerplate.
 */
@SuppressWarnings("UnstableApiUsage")
public class PlannerGraph extends AbstractPlannerGraph<PlannerGraph.Node, PlannerGraph.Edge> {

    private static final Escaper escaper = HtmlEscapers.htmlEscaper();

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
    public static class Node extends AbstractPlannerGraph.AbstractNode {
        public Node(final Object identity, final String name) {
            this(identity, name, null);
        }

        public Node(final Object identity, final String name, @Nullable final String expression) {
            super(identity, name, expression);
        }

        @Nonnull
        @Override
        public Map<String, Attribute> getAttributes() {
            final ImmutableMap.Builder<String, Attribute> builder = ImmutableMap.builder();
            Optional.ofNullable(getExpression())
                    .ifPresent(expression -> builder.put("expression",
                            SemanticAttribute.of(escaper.escape(getExpression()))));
            return builder.put("name", SemanticAttribute.of(escaper.escape(getName())))
                    .put("color", VisualAttribute.of(getColor()))
                    .put("shape", VisualAttribute.of(getShape()))
                    .put("style", VisualAttribute.of(getStyle()))
                    .put("fillcolor", VisualAttribute.of(getFillColor()))
                    .put("fontname", VisualAttribute.of(getFontName()))
                    .put("fontsize", VisualAttribute.of(getFontSize()))
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
            return getExpression() == null
                   ? "12"
                   : "8";
        }
    }

    /**
     * Node class.
     */
    public static class SourceNode extends Node {
        public SourceNode(final String name) {
            this(name, null);
        }

        public SourceNode(final String name, final String expression) {
            super(new Object(), name, expression);
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
                    .put("label", VisualAttribute.of(getName()))
                    .put("margin", VisualAttribute.of("0"))
                    .put("height", VisualAttribute.of("0"))
                    .put("width", VisualAttribute.of("0"))
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
                    .put("label", VisualAttribute.of(getName()))
                    .put("margin", VisualAttribute.of("0"))
                    .put("height", VisualAttribute.of("0"))
                    .put("width", VisualAttribute.of("0"))
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

        public Edge(final String label) {
            this(label, ImmutableSet.of());
        }

        public Edge(final Set<? extends AbstractEdge> dependsOn) {
            this(null, dependsOn);
        }

        public Edge(final String label, final Set<? extends AbstractEdge> dependsOn) {
            super(label, dependsOn);
        }

        @Nonnull
        @Override
        public Map<String, Attribute> getAttributes() {
            final ImmutableMap.Builder<String, Attribute> builder = ImmutableMap.builder();

            Optional.ofNullable(getLabel())
                    .ifPresent(label -> builder.put("label", SemanticAttribute.of(escaper.escape(label))));

            return builder
                    .put("dependsOn", SemanticAttribute.of(getDependsOn()))
                    .put("color", VisualAttribute.of(getColor()))
                    .put("style", VisualAttribute.of(getStyle()))
                    .put("fontname", VisualAttribute.of(getFontName()))
                    .put("fontsize", VisualAttribute.of(getFontSize()))
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
                    .put("constraint", VisualAttribute.of("false"))
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
