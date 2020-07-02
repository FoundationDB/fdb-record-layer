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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * TDB.

 */
@SuppressWarnings("UnstableApiUsage")
public class ExpressionRefTraversal {

    @Nonnull
    private final ExpressionRef<? extends RelationalExpression> rootRef;
    @Nonnull
    private final Network<ExpressionRef<? extends RelationalExpression>, RefPath> network;

    private ExpressionRefTraversal(@Nonnull final ExpressionRef<? extends RelationalExpression> rootRef,
                                   @Nonnull final Network<ExpressionRef<? extends RelationalExpression>, RefPath> network) {
        this.rootRef = rootRef;
        this.network = network;
    }

    @Nonnull
    public FullyTraversableExpressionRef<? extends RelationalExpression> getRoot() {
        return new FullyTraversableExpressionRef<>(rootRef);
    }

    @Nonnull
    public <T extends RelationalExpression> ExpressionRefDelegate<T> from(@Nonnull final ExpressionRef<T> regularRef) {
        return new ExpressionRefDelegate<>(regularRef);
    }

    public List<FullyTraversableExpressionRef<? extends RelationalExpression>> getLeaves() {
        return ImmutableList.of();
    }

    public static ExpressionRefTraversal withRoot(final ExpressionRef<? extends RelationalExpression> rootRef) {
        final MutableNetwork<ExpressionRef<? extends RelationalExpression>, RefPath> network =
                NetworkBuilder.directed()
                        .allowsParallelEdges(true)
                        .allowsSelfLoops(true)
                        .build();

        return new ExpressionRefTraversal(rootRef, ImmutableNetwork.copyOf(collectNetwork(network, rootRef)));
    }

    private static MutableNetwork<ExpressionRef<? extends RelationalExpression>, RefPath> collectNetwork(@Nonnull final MutableNetwork<ExpressionRef<? extends RelationalExpression>, RefPath> network,
                                                                                                         @Nonnull final ExpressionRef<? extends RelationalExpression> currentRef) {
        if (network.addNode(currentRef)) {
            for (final RelationalExpression expression : currentRef.getMembers()) {
                for (final Quantifier quantifier : expression.getQuantifiers()) {
                    final ExpressionRef<? extends RelationalExpression> rangesOverRef = quantifier.getRangesOver();
                    collectNetwork(network, rangesOverRef);
                    network.addEdge(rangesOverRef, currentRef, new RefPath(expression, quantifier));
                }
            }
        }
        return network;
    }

    /**
     * BLA.
     */
    public static class RefPath {
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final Quantifier quantifier;

        public RefPath(@Nonnull final RelationalExpression expression, @Nonnull final Quantifier quantifier) {
            this.expression = expression;
            this.quantifier = quantifier;
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
     * BLA.
     * @param <T> type
     */
    public class FullyTraversableExpressionRef<T extends RelationalExpression> extends ExpressionRefDelegate<T> {
        public FullyTraversableExpressionRef(final ExpressionRef<T> delegate) {
            super(delegate);
        }

        @Nonnull
        public Set<ExpressionRefDelegate<? extends RelationalExpression>> getParentRefs() {
            final Set<RefPath> refPaths = network.outEdges(getDelegate());
            final ImmutableSet.Builder<ExpressionRefDelegate<? extends RelationalExpression>> builder =
                    ImmutableSet.builder();

            for (final RefPath refPath : refPaths) {
                final EndpointPair<ExpressionRef<? extends RelationalExpression>> incidentNodes =
                        network.incidentNodes(refPath);

                builder.add(new ExpressionRefDelegate<>(incidentNodes.target()));
            }

            return builder.build();
        }
    }

}
