/*
 * ExpressionRefTraversal.java
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

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.NetworkBuilder;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Utility class to provide a graph view of a expression reference DAG given by a root expression reference
 * (to a {@link RelationalExpression} that allows to perform traversal operations that are normally not possible
 * on instances of {@link ExpressionRef} such as {@link #getLeafReferences()} and {@link #getParentRefs}.
 *
 * The implementation of this class assumes that the original DAG is not mutated after the traversal is created.
 * If the underlying DAG is mutated gainst assumptions, the semantics of this class is defined in a way that calls to
 * graph-backed methods return data as if the mutation hadn't occurred while references that are being returned as part
 * of the result of method calls naturally reflect any mutations that have occurred.
 */
@SuppressWarnings("UnstableApiUsage")
@API(API.Status.EXPERIMENTAL)
public class ExpressionRefTraversal {
    @Nonnull
    private final ExpressionRef<? extends RelationalExpression> rootReference;
    @Nonnull
    private final MutableNetwork<ExpressionRef<? extends RelationalExpression>, ReferencePath> network;
    @Nonnull
    private final SetMultimap<RelationalExpression, ExpressionRef<? extends RelationalExpression>> containedInMultiMap;
    @Nonnull
    private final Set<ExpressionRef<? extends RelationalExpression>> leafReferences;

    private ExpressionRefTraversal(@Nonnull final ExpressionRef<? extends RelationalExpression> rootReference,
                                   @Nonnull final MutableNetwork<ExpressionRef<? extends RelationalExpression>, ReferencePath> network,
                                   @Nonnull final SetMultimap<RelationalExpression, ExpressionRef<? extends RelationalExpression>> containedInMultiMap,
                                   @Nonnull final Set<ExpressionRef<? extends RelationalExpression>> leafReferences) {
        this.rootReference = rootReference;
        this.network = network;
        this.containedInMultiMap = containedInMultiMap;
        this.leafReferences = leafReferences;
    }

    @Nonnull
    public ExpressionRef<? extends RelationalExpression> getRootReference() {
        return rootReference;
    }

    @Nonnull
    public Set<ExpressionRef<? extends RelationalExpression>> getRefs() {
        return network.nodes();
    }

    @Nonnull
    public Set<ExpressionRef<? extends RelationalExpression>> getLeafReferences() {
        return leafReferences;
    }

    /**
     * Returns all references containing the expression passed in.
     * @param expression the expression
     * @return a set of expression references containing {@code expression}
     */
    public Set<ExpressionRef<? extends RelationalExpression>> getRefsContaining(final RelationalExpression expression) {
        return containedInMultiMap.get(expression);
    }

    /**
     * Return all expression references that contain a path
     * from {@code parent -> expression -> quantifier -> this reference}
     * @param reference reference
     * @return the set of references that are considered parents of this reference.
     */
    @Nonnull
    public Set<ExpressionRef<? extends RelationalExpression>> getParentRefs(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        final ImmutableSet.Builder<ExpressionRef<? extends RelationalExpression>> builder =
                ImmutableSet.builder();
        forEachParentExpression(reference, (ref, expression) -> builder.add(ref));
        return builder.build();
    }

    /**
     * Return all expressions (as {@link RelationalExpression}s) that refer to this expression reference
     * @param reference reference
     * @return the set of expressions (as identity-based set) that are considered parents of this reference.
     */
    @Nonnull
    public Set<RelationalExpression> getParentExpressions(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        final Set<RelationalExpression> result = Sets.newIdentityHashSet();
        forEachParentExpression(reference, (ref, expression) -> result.add(expression));
        return result;
    }

    /**
     * Return all expressions (as {@link RelationalExpression}s) that refer to this expression reference
     * @param reference reference to return the parent reference paths for
     * @return the set of expressions that are considered parents of this reference.
     */
    @Nonnull
    public Set<ReferencePath> getParentRefPaths(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        return network.outEdges(reference);
    }

    public void forEachParentExpression(@Nonnull final ExpressionRef<? extends RelationalExpression> reference,
                                        @Nonnull final BiConsumer<ExpressionRef<? extends RelationalExpression>, RelationalExpression> biConsumer) {
        final Set<ReferencePath> referencePaths = network.outEdges(reference);
        for (final ReferencePath referencePath : referencePaths) {
            final EndpointPair<ExpressionRef<? extends RelationalExpression>> incidentNodes =
                    network.incidentNodes(referencePath);
            biConsumer.accept(incidentNodes.target(), referencePath.getExpression());
        }
    }

    public void addExpression(final ExpressionRef<? extends RelationalExpression> reference, final RelationalExpression expression) {
        descendAndAddExpressions(network,
                containedInMultiMap,
                leafReferences,
                reference,
                expression);
        containedInMultiMap.put(expression, reference);
        if (expression.getQuantifiers().isEmpty()) {
            leafReferences.add(reference);
        }
    }

    /**
     * Construct a traversal object using the {@code rootRef} reference passed in.
     * @param rootRef the reference acting as the root for this traversal object
     * @return a new traversal object
     */
    public static ExpressionRefTraversal withRoot(final ExpressionRef<? extends RelationalExpression> rootRef) {
        final MutableNetwork<ExpressionRef<? extends RelationalExpression>, ReferencePath> network =
                NetworkBuilder.directed()
                        .allowsParallelEdges(true)
                        .allowsSelfLoops(true)
                        .build();

        final SetMultimap<RelationalExpression, ExpressionRef<? extends RelationalExpression>> containedInMap =
                Multimaps.newSetMultimap(Maps.newIdentityHashMap(), Sets::newHashSet);
        final Set<ExpressionRef<? extends RelationalExpression>> leafRefs = Sets.newHashSet();
        collectNetwork(network, containedInMap, leafRefs, rootRef);

        return new ExpressionRefTraversal(rootRef, network, containedInMap, leafRefs);
    }

    private static void collectNetwork(@Nonnull final MutableNetwork<ExpressionRef<? extends RelationalExpression>, ReferencePath> network,
                                       @Nonnull final SetMultimap<RelationalExpression, ExpressionRef<? extends RelationalExpression>> containedInMultiMap,
                                       @Nonnull final Set<ExpressionRef<? extends RelationalExpression>> leafReferences,
                                       @Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        if (network.addNode(reference)) {
            boolean anyLeafExpressions = false;
            for (final RelationalExpression expression : reference.getMembers()) {
                if (expression.getQuantifiers().isEmpty()) {
                    anyLeafExpressions = true;
                } else {
                    descendAndAddExpressions(network, containedInMultiMap, leafReferences, reference, expression);
                }
                containedInMultiMap.put(expression, reference);
            }
            if (anyLeafExpressions) {
                leafReferences.add(reference);
            }
        }
    }

    private static void descendAndAddExpressions(@Nonnull final MutableNetwork<ExpressionRef<? extends RelationalExpression>, ReferencePath> network,
                                                 @Nonnull final SetMultimap<RelationalExpression, ExpressionRef<? extends RelationalExpression>> containedInMultiMap,
                                                 @Nonnull final Set<ExpressionRef<? extends RelationalExpression>> leafReferences,
                                                 @Nonnull final ExpressionRef<? extends RelationalExpression> reference,
                                                 final RelationalExpression expression) {
        for (final Quantifier quantifier : expression.getQuantifiers()) {
            final ExpressionRef<? extends RelationalExpression> rangesOverRef = quantifier.getRangesOver();
            collectNetwork(network, containedInMultiMap, leafReferences, rangesOverRef);
            network.addEdge(rangesOverRef, reference, new ReferencePath(reference, expression, quantifier));
        }
    }

    /**
     * Case class to hold information about the path from an expression to another expression reference.
     */
    public static class ReferencePath {
        @Nonnull
        private final ExpressionRef<? extends RelationalExpression> reference;

        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final Quantifier quantifier;

        public ReferencePath(@Nonnull final ExpressionRef<? extends RelationalExpression> reference,
                             @Nonnull final RelationalExpression expression,
                             @Nonnull final Quantifier quantifier) {
            this.reference = reference;
            this.expression = expression;
            this.quantifier = quantifier;
        }

        @Nonnull
        public ExpressionRef<? extends RelationalExpression> getReference() {
            return reference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        public Quantifier getQuantifier() {
            return quantifier;
        }
    }
}
