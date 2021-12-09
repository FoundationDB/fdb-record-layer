/*
 * ParameterRelationshipGraph.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.expressions.BooleanComponent.groupedComparisons;
import static com.apple.foundationdb.record.query.combinatorics.CrossProduct.crossProduct;

/**
 * A class to keep track of the relationships of parameters in a query given a {@link RecordQuery} and a set of
 * pre-bindings. This will allow the query planner to find potential additional optimizations and also it helps
 * define plan-compatibility between two queries before actually planning them. That property is the crucial
 * for any meaningful implementation of a plan cache that is not dependent on literal constants in the query.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings("UnstableApiUsage")
public class ParameterRelationshipGraph {
    private static final ParameterRelationshipGraph UNBOUND = empty();

    /**
     * The guava network backing the parameter relationships. Note that a {@link Network} is a kind of graph
     * implementation that imposes uniqueness on nodes and edges. The nodes in the network are parameters,
     * edges are relationships between parameters like equality, etc. For our purposes, parameter names (the nodes) are
     * considered unique per definition. Edges are created upon insertion and are considered unique by
     * {@code (from, to, relationship type)}.
     * Two parameters can be connected by more than one edge as they may participate in more than one relationship.
     * This graph should not really grow beyond a manageable limit.  Relationships are always computed based on a set
     * of pre-bound parameters.
     */
    @Nonnull
    private final ImmutableNetwork<String, Relationship> network;

    private ParameterRelationshipGraph(@Nonnull final Network<String, Relationship> network) {
        this.network = ImmutableNetwork.copyOf(network);
    }

    public boolean isUnbound() {
        return this == unbound();
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public int hashCode() {
        return network.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ParameterRelationshipGraph)) {
            return false;
        }
        final ParameterRelationshipGraph that = (ParameterRelationshipGraph)o;
        return network.equals(that.network);
    }

    /**
     * Derive a relationship graph from a {@link RecordQuery} and a set of pre-bound parameters in handing in
     * as a {@link Bindings} object.
     * <p>
     * Note that we do not derive additional relationships (yet). So for instance a query
     * </p>
     * <p>
     * {@code
     * ...
     * WHERE x < $p1 AND x = $p2 AND y < $p2 AND y = $p3
     * }
     * </p>
     * with pre-bindings {@code p1 -> 10, p2 -> 10, p3 -> 10} would result in a graph containing
     * <p>
     * {@code
     * p1 EQUALS p1
     * p1 EQUALS p2
     * p2 EQUALS p1
     * p2 EQUALS p2
     * p2 EQUALS p3
     * p3 EQUALS p2
     * p3 EQUALS p3
     * }
     * </p>
     * but it would not contain
     * <p>
     * {@code
     * p1 EQUALS p3
     * p3 EQUALS p1
     * }
     * </p>
     * @param recordQuery query
     * @param preBoundParameterBindings parameter bindings already known at planning time
     * @return a new {@link ParameterRelationshipGraph}
     */
    @Nonnull
    public static ParameterRelationshipGraph fromRecordQueryAndBindings(@Nonnull final RecordQuery recordQuery,
                                                                        @Nonnull final Bindings preBoundParameterBindings) {
        final QueryComponent filter = recordQuery.getFilter();
        final ImmutableNetwork.Builder<String, Relationship> networkBuilder =
                NetworkBuilder.directed()
                        .allowsSelfLoops(true)
                        .allowsParallelEdges(true)
                        .immutable();

        if (filter != null) {
            groupedComparisons(filter)
                    .flatMap(entry -> StreamSupport.stream(crossProduct(ImmutableList.of(entry.getValue(), entry.getValue())).spliterator(), false))
                    .map(list -> Pair.of(list.get(0), list.get(1)))
                    .forEach(pair -> {
                        final Comparisons.ComparisonWithParameter left = pair.getLeft();
                        final Comparisons.ComparisonWithParameter right = pair.getRight();

                        if (left.getParameter().equals(right.getParameter())) {
                            Relationship.addEdge(networkBuilder, RelationshipType.EQUALS, left.getParameter(), right.getParameter());
                            Relationship.addEdge(networkBuilder, RelationshipType.EQUALS, right.getParameter(), left.getParameter());
                        } else if (preBoundParameterBindings.containsBinding(left.getParameter()) &&  // It is possible that some of the parameters are not pre-bound. That's not an error,
                                   preBoundParameterBindings.containsBinding(right.getParameter())) { // it just means we cannot establish any sort of relationship between them.

                            final Object leftComparand = preBoundParameterBindings.get(left.getParameter());
                            final Object rightComparand = preBoundParameterBindings.get(right.getParameter());

                            if (Objects.equals(leftComparand, rightComparand)) {
                                Relationship.addEdge(networkBuilder, RelationshipType.EQUALS, left.getParameter(), right.getParameter());
                                Relationship.addEdge(networkBuilder, RelationshipType.EQUALS, right.getParameter(), left.getParameter());
                            }
                        }
                    });
        }
        return new ParameterRelationshipGraph(networkBuilder.build());
    }

    @Nonnull
    public static ParameterRelationshipGraph empty() {
        return new ParameterRelationshipGraph(NetworkBuilder.directed().allowsSelfLoops(true).allowsParallelEdges(true).<String, Relationship>immutable().build());
    }

    /**
     * Returns a specific instance that must be considered as a {@code null} element.
     * @return the unbound instance
     */
    public static ParameterRelationshipGraph unbound() {
        return UNBOUND;
    }

    @Nonnull
    public Set<String> getRelatedParameters(@Nonnull final String parameter, @Nonnull final RelationshipType relationshipType) {
        return network.outEdges(parameter)
                .stream()
                .filter(relationship -> relationship.getRelationshipKind() == relationshipType)
                .map(relationship -> network.incidentNodes(relationship).nodeV())
                .collect(ImmutableSet.toImmutableSet());
    }

    public boolean containsParameter(@Nonnull final String parameter) {
        return network.nodes().contains(parameter);
    }

    public boolean isCompatible(@Nonnull final Bindings parameterBindings) {
        return network.edges()
                .stream()
                .allMatch(edge -> {
                    final String parameter1 = edge.getParameter1();
                    final String parameter2 = edge.getParameter2();
                    if (edge.getRelationshipKind() == RelationshipType.EQUALS &&
                            !parameter1.equals(parameter2)) {
                        if (!parameterBindings.containsBinding(parameter1) ||
                                !parameterBindings.containsBinding(parameter2)) {
                            return false;
                        }
                        return Objects.equals(parameterBindings.get(parameter1), parameterBindings.get(parameter2));
                    }
                    return true;
                });
    }

    public enum RelationshipType {
        EQUALS,
        NOT_EQUALS
    }

    public static class Relationship {
        @Nonnull
        private final RelationshipType relationshipType;
        @Nonnull
        private final String parameter1;
        @Nonnull
        private final String parameter2;

        public Relationship(@Nonnull final RelationshipType relationshipType, @Nonnull final String parameter1, @Nonnull final String parameter2) {
            this.relationshipType = relationshipType;
            this.parameter1 = parameter1;
            this.parameter2 = parameter2;
        }

        @Nonnull
        public RelationshipType getRelationshipKind() {
            return relationshipType;
        }

        @Nonnull
        public String getParameter1() {
            return parameter1;
        }

        @Nonnull
        public String getParameter2() {
            return parameter2;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Relationship)) {
                return false;
            }
            final Relationship that = (Relationship)o;
            return relationshipType == that.relationshipType && getParameter1().equals(that.getParameter1()) && getParameter2().equals(that.getParameter2());
        }

        @Override
        public int hashCode() {
            return Objects.hash(relationshipType, getParameter1(), getParameter2());
        }

        @SuppressWarnings("SameParameterValue")
        private static void addEdge(@Nonnull ImmutableNetwork.Builder<String, Relationship> networkBuilder,
                                    @Nonnull final RelationshipType relationshipType,
                                    @Nonnull final String parameter1,
                                    @Nonnull final String parameter2) {
            networkBuilder.addEdge(parameter1, parameter2, new Relationship(relationshipType, parameter1, parameter2));
        }

        @Override
        public String toString() {
            return getParameter1() + " --" + getRelationshipKind().name() + "--> " + getParameter2();
        }
    }
}
