/*
 * Traversal.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.NetworkBuilder;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Utility class to provide a graph view of a expression reference DAG given by a root expression reference
 * (to a {@link RelationalExpression} that allows to perform traversal operations that are normally not possible
 * on instances of {@link Reference} such as {@link #getLeafReferences()} and {@link #getParentRefs}.
 * <br>
 * The implementation of this class assumes that the original DAG is not mutated after the traversal is created.
 * If the underlying DAG is mutated against assumptions, the semantics of this class is defined in a way that calls to
 * graph-backed methods return data as if the mutation hadn't occurred while references that are being returned as part
 * of the result of method calls naturally reflect any mutations that have occurred.
 */
@SuppressWarnings("UnstableApiUsage")
@API(API.Status.EXPERIMENTAL)
public class Traversal {
    @Nonnull
    private final Reference rootReference;
    @Nonnull
    private final MutableNetwork<Reference, ReferencePath> network;
    @Nonnull
    private final SetMultimap<RelationalExpression, Reference> containedInMultiMap;
    @Nonnull
    private final Set<Reference> leafReferences;

    private Traversal(@Nonnull final Reference rootReference,
                      @Nonnull final MutableNetwork<Reference, ReferencePath> network,
                      @Nonnull final SetMultimap<RelationalExpression, Reference> containedInMultiMap,
                      @Nonnull final Set<Reference> leafReferences) {
        this.rootReference = rootReference;
        this.network = network;
        this.containedInMultiMap = containedInMultiMap;
        this.leafReferences = leafReferences;
    }

    @Nonnull
    public Reference getRootReference() {
        return rootReference;
    }

    @Nonnull
    public Set<Reference> getRefs() {
        return network.nodes();
    }

    @Nonnull
    public Set<Reference> getLeafReferences() {
        return leafReferences;
    }

    /**
     * Returns all references containing the expression passed in.
     * @param expression the expression
     * @return a set of expression references containing {@code expression}
     */
    public Set<Reference> getRefsContaining(final RelationalExpression expression) {
        final var result = containedInMultiMap.get(expression);
        Debugger.sanityCheck(() -> Verify.verify(result.stream().allMatch(ref -> ref.getMembers().stream().anyMatch(e -> e == expression))));
        return result;
    }

    /**
     * Return all expression references that contain a path.
     * from {@code parent -> expression -> quantifier -> this reference}
     * @param reference reference
     * @return the set of references that are considered parents of this reference.
     */
    @Nonnull
    public Set<Reference> getParentRefs(@Nonnull final Reference reference) {
        final ImmutableSet.Builder<Reference> builder =
                ImmutableSet.builder();
        forEachParentExpression(reference, (ref, expression) -> builder.add(ref));
        return builder.build();
    }

    /**
     * Return all expressions (as {@link RelationalExpression}s) that refer to this expression reference.
     * @param reference reference
     * @return the set of expressions (as identity-based set) that are considered parents of this reference.
     */
    @Nonnull
    public Set<RelationalExpression> getParentExpressions(@Nonnull final Reference reference) {
        final Set<RelationalExpression> result = Sets.newIdentityHashSet();
        forEachParentExpression(reference, (ref, expression) -> result.add(expression));
        return result;
    }

    /**
     * Return all expressions (as {@link ReferencePath}s) that refer to this expression reference.
     * @param reference reference to return the parent reference paths for
     * @return the set of expressions that are considered parents of this reference.
     */
    @Nonnull
    public Set<ReferencePath> getParentRefPaths(@Nonnull final Reference reference) {
        return network.outEdges(reference);
    }

    public void forEachParentExpression(@Nonnull final Reference reference,
                                        @Nonnull final BiConsumer<Reference, RelationalExpression> biConsumer) {
        final Set<ReferencePath> referencePaths = network.outEdges(reference);
        for (final ReferencePath referencePath : referencePaths) {
            final EndpointPair<Reference> incidentNodes =
                    network.incidentNodes(referencePath);
            biConsumer.accept(incidentNodes.target(), referencePath.getExpression());
        }
    }

    public void addExpression(@Nonnull final Reference reference, @Nonnull final RelationalExpression expression) {
        descendAndAddExpressions(network,
                containedInMultiMap,
                leafReferences,
                reference,
                expression);
        Debugger.sanityCheck(() -> Verify.verify(reference.getMembers().contains(expression)));
        containedInMultiMap.put(expression, reference);
        if (expression.getQuantifiers().isEmpty()) {
            leafReferences.add(reference);
        }
    }

    public void removeExpression(@Nonnull final Reference reference, @Nonnull final RelationalExpression expression) {
        final var referencePaths = ImmutableSet.copyOf(network.inEdges(reference));

        for (final var referencePath : referencePaths) {
            if (referencePath.expression == expression) {
                network.removeEdge(referencePath);
            }
        }

        containedInMultiMap.removeAll(expression);
    }

    /**
     * Construct a traversal object using the {@code rootRef} reference passed in.
     * @param rootRef the reference acting as the root for this traversal object
     * @return a new traversal object
     */
    public static Traversal withRoot(final Reference rootRef) {
        final MutableNetwork<Reference, ReferencePath> network =
                NetworkBuilder.directed()
                        .allowsParallelEdges(true)
                        .allowsSelfLoops(true)
                        .edgeOrder(ElementOrder.insertion())
                        .nodeOrder(ElementOrder.insertion())
                        .build();

        final SetMultimap<RelationalExpression, Reference> containedInMap =
                Multimaps.newSetMultimap(new LinkedIdentityMap<>(), LinkedIdentitySet::new);
        final Set<Reference> leafRefs = new LinkedIdentitySet<>();
        collectNetwork(network, containedInMap, leafRefs, rootRef);

        return new Traversal(rootRef, network, containedInMap, leafRefs);
    }

    private static void collectNetwork(@Nonnull final MutableNetwork<Reference, ReferencePath> network,
                                       @Nonnull final SetMultimap<RelationalExpression, Reference> containedInMultiMap,
                                       @Nonnull final Set<Reference> leafReferences,
                                       @Nonnull final Reference reference) {
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

    private static void descendAndAddExpressions(@Nonnull final MutableNetwork<Reference, ReferencePath> network,
                                                 @Nonnull final SetMultimap<RelationalExpression, Reference> containedInMultiMap,
                                                 @Nonnull final Set<Reference> leafReferences,
                                                 @Nonnull final Reference reference,
                                                 final RelationalExpression expression) {
        network.addNode(reference);
        for (final Quantifier quantifier : expression.getQuantifiers()) {
            final Reference rangesOverRef = quantifier.getRangesOver();
            collectNetwork(network, containedInMultiMap, leafReferences, rangesOverRef);
            network.addEdge(rangesOverRef, reference, new ReferencePath(reference, expression, quantifier));
        }
    }

    /**
     * Case class to hold information about the path from an expression to another expression reference.
     */
    public static class ReferencePath {
        @Nonnull
        private final Reference reference;

        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final Quantifier quantifier;

        public ReferencePath(@Nonnull final Reference reference,
                             @Nonnull final RelationalExpression expression,
                             @Nonnull final Quantifier quantifier) {
            this.reference = reference;
            this.expression = expression;
            this.quantifier = quantifier;
        }

        @Nonnull
        public Reference getReference() {
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
