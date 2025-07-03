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
import com.google.common.graph.StableStandardMutableNetwork;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;
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
        Debugger.sanityCheck(() ->
                Verify.verify(result.stream()
                        .allMatch(ref -> ref.getAllMemberExpressions()
                                .stream()
                                .anyMatch(e -> e == expression))));
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
        Debugger.sanityCheck(() -> Verify.verify(reference.containsExactly(expression)));
        containedInMultiMap.put(expression, reference);
        if (expression.getQuantifiers().isEmpty()) {
            leafReferences.add(reference);
        }
    }

    public void removeExpression(@Nonnull final Reference reference, @Nonnull final RelationalExpression expression) {
        final var referencePaths = ImmutableSet.copyOf(network.inEdges(reference));

        final var childrenReferences = new LinkedIdentitySet<Reference>();
        for (final var referencePath : referencePaths) {
            if (referencePath.expression == expression) {
                network.removeEdge(referencePath);
            }
            childrenReferences.add(referencePath.getQuantifier().getRangesOver());
        }
        pruneUnreferencedRefs(childrenReferences);
        containedInMultiMap.remove(expression, reference);
        if (leafReferences.contains(reference)) {
            boolean stillHasLeaf = false;
            for (RelationalExpression otherRefExpression : reference.getAllMemberExpressions()) {
                if (containedInMultiMap.containsEntry(otherRefExpression, reference) && otherRefExpression.getQuantifiers().isEmpty()) {
                    stillHasLeaf = true;
                    break;
                }
            }
            if (!stillHasLeaf) {
                leafReferences.remove(reference);
            }
        }
    }

    public void pruneUnreferencedRefs(@Nonnull final Collection<? extends Reference> childrenReferences) {
        for (final var childReference : childrenReferences) {
            if (network.outDegree(childReference) == 0) {
                for (final var memberExpression : childReference.getAllMemberExpressions()) {
                    removeExpression(childReference, memberExpression);
                }
                network.removeNode(childReference);
            }
        }
    }

    /**
     * Construct a traversal object using the {@code rootRef} reference passed in.
     * @param rootRef the reference acting as the root for this traversal object
     * @return a new traversal object
     */
    public static Traversal withRoot(final Reference rootRef) {
        // This diverges from the standard construction of Guava network; it delegates the construction to a slightly
        // modified version that guarantees a stable iteration order over the edges, which is required to guarantee a
        // deterministic planning behavior.
        final MutableNetwork<Reference, ReferencePath> network =
                new StableStandardMutableNetwork<>(NetworkBuilder.directed()
                        .allowsParallelEdges(true)
                        .allowsSelfLoops(true)
                        .edgeOrder(ElementOrder.insertion())
                        .nodeOrder(ElementOrder.insertion()));

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
            for (final RelationalExpression expression : reference.getAllMemberExpressions()) {
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

    public void verifyIntegrity() {
        // Recompute the traversal from the root
        Traversal secondTraversal = Traversal.withRoot(rootReference);

        //
        // Make sure there is nothing missing in the graph
        //
        var missingNodes = secondTraversal.network.nodes().stream()
                    .filter(ref -> !network.nodes().contains(ref))
                    .collect(LinkedIdentitySet.toLinkedIdentitySet());
        Verify.verify(missingNodes.isEmpty(), "graph is missing %d nodes", missingNodes.size());
        var missingLeafNodes = secondTraversal.leafReferences.stream()
                    .filter(ref -> !leafReferences.contains(ref))
                    .collect(LinkedIdentitySet.toLinkedIdentitySet());
        Verify.verify(missingLeafNodes.isEmpty(), "graph is missing %s leaf nodes", missingLeafNodes.size());
        var missingContainedIns = secondTraversal.containedInMultiMap.entries().stream()
                .filter(entry -> !containedInMultiMap.containsEntry(entry.getKey(), entry.getValue()))
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
        Verify.verify(missingContainedIns.isEmpty(), "traversal is missing %s containedIn entries", missingContainedIns.size());
        for (Reference ref : secondTraversal.network.nodes()) {
            Set<ReferencePath> expectedOut = secondTraversal.network.outEdges(ref);
            Set<ReferencePath> edgesOut = network.outEdges(ref);
            Set<ReferencePath> missingEdges = expectedOut.stream()
                    .filter(path -> !edgesOut.contains(path))
                    .collect(LinkedIdentitySet.toLinkedIdentitySet());
            Verify.verify(missingEdges.isEmpty(), "missing %s expected edges for reference %s", missingEdges.size(), ref);
        }

        //
        // Make sure everything in the network is in the manual traversal
        //
        Set<Reference> extraNodes = network.nodes().stream()
                .filter(ref -> !secondTraversal.network.nodes().contains(ref))
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
        Verify.verify(extraNodes.isEmpty(), "network contains %s extra nodes", extraNodes.size());
        Set<Reference> extraLeafs = leafReferences.stream()
                .filter(ref -> !secondTraversal.leafReferences.contains(ref))
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
        Verify.verify(extraLeafs.isEmpty(), "network contains %s extra leaf nodes", extraLeafs.size());
        var extraContainedIn = containedInMultiMap.entries().stream()
                .filter(entry -> !secondTraversal.containedInMultiMap.containsEntry(entry.getKey(), entry.getValue()))
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
        Verify.verify(extraContainedIn.isEmpty(), "contained in map contains %s extra entries", extraContainedIn.size());

        for (Reference ref : network.nodes()) {
            Set<ReferencePath> edgesOut = network.outEdges(ref);
            Set<ReferencePath> expectedOut = secondTraversal.network.outEdges(ref);
            Set<ReferencePath> extraEdges = edgesOut.stream()
                    .filter(path -> !expectedOut.contains(path))
                    .collect(LinkedIdentitySet.toLinkedIdentitySet());
            Verify.verify(extraEdges.isEmpty(), "network contained %s expected edges for reference %s", extraEdges.size(), ref);
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

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            final ReferencePath that = (ReferencePath)object;
            // Use referential equality here, as the data structures are all based on identity sets, and so
            // we only want to identify two paths as the same if they refer to the same objects
            return reference == that.reference && expression == that.expression && quantifier == that.quantifier;
        }

        @Override
        public int hashCode() {
            // Note that we're using the identity hash codes here as equality is based on pointer equality
            return Objects.hash(System.identityHashCode(reference), System.identityHashCode(expression), System.identityHashCode(quantifier));
        }
    }
}
