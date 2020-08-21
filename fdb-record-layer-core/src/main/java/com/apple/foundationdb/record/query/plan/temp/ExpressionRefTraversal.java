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
import com.google.common.collect.Sets;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Utility class to provide a graph view of a expression reference DAG given by a root expression reference
 * (to a {@link RelationalExpression} that allows to perform traversal operations that are normally not possible
 * on instances of {@link ExpressionRef} such as {@link #getLeafRefs()} and {@link #getParentRefs}.
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
    private final ExpressionRef<? extends RelationalExpression> rootRef;
    @Nonnull
    private final ImmutableNetwork<ExpressionRef<? extends RelationalExpression>, RefPath> network;
    @Nonnull
    private final ImmutableSet<ExpressionRef<? extends RelationalExpression>> leafRefs;

    private ExpressionRefTraversal(@Nonnull final ExpressionRef<? extends RelationalExpression> rootRef,
                                   @Nonnull final Network<ExpressionRef<? extends RelationalExpression>, RefPath> network,
                                   @Nonnull final Set<ExpressionRef<? extends RelationalExpression>> leafRefs) {
        this.rootRef = rootRef;
        this.network = ImmutableNetwork.copyOf(network);
        this.leafRefs = ImmutableSet.copyOf(leafRefs);
    }

    @Nonnull
    public ExpressionRef<? extends RelationalExpression> getRootRef() {
        return rootRef;
    }

    @Nonnull
    public Set<ExpressionRef<? extends RelationalExpression>> getRefs() {
        return network.nodes();
    }

    public Set<ExpressionRef<? extends RelationalExpression>> getLeafRefs() {
        return leafRefs;
    }

    /**
     * Construct a traversal object using the {@code rootRef} reference passed in.
     * @param rootRef the reference acting as the root for this traversal object
     * @return a new traversal object
     */
    public static ExpressionRefTraversal withRoot(final ExpressionRef<? extends RelationalExpression> rootRef) {
        final MutableNetwork<ExpressionRef<? extends RelationalExpression>, RefPath> network =
                NetworkBuilder.directed()
                        .allowsParallelEdges(true)
                        .allowsSelfLoops(true)
                        .build();

        final ImmutableSet.Builder<ExpressionRef<? extends RelationalExpression>> leafRefsBuilder = ImmutableSet.builder();
        collectNetwork(network, leafRefsBuilder, rootRef);

        return new ExpressionRefTraversal(rootRef, network, leafRefsBuilder.build());
    }

    private static void collectNetwork(@Nonnull final MutableNetwork<ExpressionRef<? extends RelationalExpression>, RefPath> network,
                                       @Nonnull final ImmutableSet.Builder<ExpressionRef<? extends RelationalExpression>> leafRefsBuilder,
                                       @Nonnull final ExpressionRef<? extends RelationalExpression> currentRef) {
        if (network.addNode(currentRef)) {
            boolean anyLeafExpressions = false;
            for (final RelationalExpression expression : currentRef.getMembers()) {
                if (expression.getQuantifiers().isEmpty()) {
                    anyLeafExpressions = true;
                } else {
                    for (final Quantifier quantifier : expression.getQuantifiers()) {
                        final ExpressionRef<? extends RelationalExpression> rangesOverRef = quantifier.getRangesOver();
                        collectNetwork(network, leafRefsBuilder, rangesOverRef);
                        network.addEdge(rangesOverRef, currentRef, new RefPath(currentRef, expression, quantifier));
                    }
                }
            }
            if (anyLeafExpressions) {
                leafRefsBuilder.add(currentRef);
            }
        }
    }

    /**
     * Case class to hold information about the path from an expression to another expression reference.
     */
    public static class RefPath {
        @Nonnull
        private final ExpressionRef<? extends RelationalExpression> ref;

        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final Quantifier quantifier;

        public RefPath(@Nonnull final ExpressionRef<? extends RelationalExpression> ref,
                       @Nonnull final RelationalExpression expression,
                       @Nonnull final Quantifier quantifier) {
            this.ref = ref;
            this.expression = expression;
            this.quantifier = quantifier;
        }

        @Nonnull
        public ExpressionRef<? extends RelationalExpression> getRef() {
            return ref;
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
    public Set<RefPath> getParentRefPaths(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        return network.outEdges(reference);
    }

    public void forEachParentExpression(@Nonnull final ExpressionRef<? extends RelationalExpression> reference,
                                        @Nonnull final BiConsumer<ExpressionRef<? extends RelationalExpression>, RelationalExpression> biConsumer) {
        final Set<RefPath> refPaths = network.outEdges(reference);
        for (final RefPath refPath : refPaths) {
            final EndpointPair<ExpressionRef<? extends RelationalExpression>> incidentNodes =
                    network.incidentNodes(refPath);
            biConsumer.accept(incidentNodes.target(), refPath.getExpression());
        }
    }
}
