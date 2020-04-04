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

import com.apple.foundationdb.record.query.plan.temp.GraphExporter.ComponentNameProvider;
import com.apple.foundationdb.record.query.plan.temp.PlannerGraph.PlannerGraphBuilder;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.escape.Escaper;
import com.google.common.html.HtmlEscapers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

/**
 * Class to hold a graph for explain, optimization, and rewrite purposes.
 */
public class ExplainPlannerGraphProperty implements PlannerProperty<PlannerGraph<ExplainPlannerGraphProperty.Node, ExplainPlannerGraphProperty.Edge>> {
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

        @Nullable
        @Override
        public String getLabel() {
            //noinspection UnstableApiUsage
            final Escaper escaper = HtmlEscapers.htmlEscaper();
            @Nullable final String e = getExpression();
            final String n = getName();
            if (e == null || e.isEmpty()) {
                return escaper.escape(n);
            }
            return "{" + escaper.escape(n) + "|" +
                   escaper.escape(e) + "}";
        }
    }

    /**
     * Edge class.
     */
    public static class Edge extends PlannerGraph.AbstractEdge {
        @Nullable
        @Override
        public String getLabel() {
            return null;
        }
    }

    /**
     * Creates a serialized format of this graph as a gml-compatible definition.
     * @param plannerGraph the planner graph to be exported
     * 
     * @return the graph as string in gml format.
     */
    @Nonnull
    public static String exportToGml(final PlannerGraph<Node, Edge> plannerGraph) {
        final GraphExporter<Node, Edge> exporter = createExporter();
        // export as string
        final Writer writer = new StringWriter();
        exporter.exportGraph(plannerGraph.getNetwork(), writer);
        return writer.toString();
    }

    private static GraphExporter<Node, Edge> createExporter() {
        /*
         * Generate a unique identifier for each node.
         */
        final ComponentNameProvider<Node> vertexIdProvider = new ComponentNameProvider<Node>() {
            int counter = 0;
            @Override
            public String apply(final Node component) {
                counter++;
                return String.valueOf(counter);
            }
        };

        return new GmlExporter<>(vertexIdProvider,
                Node::getAttributes,
                Edge::getAttributes,
                ImmutableMap.of());
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
        final PlannerGraphBuilder<Node, Edge> plannerGraphBuilder = expression.explainYourself();
        for (final PlannerGraph<Node, Edge> childGraph : childGraphs) {
            plannerGraphBuilder
                    .addGraph(childGraph)
                    .addEdge(childGraph.getRoot(), plannerGraphBuilder.getRoot(), new Edge());
        }
        return plannerGraphBuilder.build();
    }

    @Nonnull
    @Override
    public PlannerGraph<Node, Edge> evaluateAtRef(@Nonnull ExpressionRef<? extends PlannerExpression> ref, @Nonnull List<PlannerGraph<Node, Edge>> memberResults) {
        Verify.verify(memberResults.size() == 1);

        return Iterables.getOnlyElement(memberResults);
    }
}
