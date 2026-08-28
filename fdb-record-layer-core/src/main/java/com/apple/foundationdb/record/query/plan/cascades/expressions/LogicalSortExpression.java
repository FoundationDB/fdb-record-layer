/*
 * LogicalSortExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A relational planner expression that represents an unimplemented sort on the records produced by its inner
 * relational planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalSortExpression extends AbstractRelationalExpressionWithChildren implements InternalPlannerGraphRewritable {
    private final RequestedOrdering ordering;

    private final Quantifier inner;

    public LogicalSortExpression(final RequestedOrdering ordering, final Quantifier inner) {
        this.ordering = ordering;
        this.inner = inner;
    }

    @Deprecated
    public LogicalSortExpression(List<Value> sortValues,
                                 final boolean reverse,
                                 final Quantifier inner) {
        this(buildRequestedOrdering(sortValues, reverse, inner), inner);
    }

    public static RequestedOrdering buildRequestedOrdering(List<Value> sortValues,
                                                           boolean reverse,
                                                           final Quantifier inner) {
        final OrderingPart.RequestedSortOrder order = OrderingPart.RequestedSortOrder.fromIsReverse(reverse);
        final RequestedOrdering.Distinctness distinctness = RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS;
        final var requestedOrderingParts =
                sortValues.stream().map(value -> new OrderingPart.RequestedOrderingPart(value, order)).collect(Collectors.toList());
        return RequestedOrdering.ofParts(requestedOrderingParts, distinctness, false, inner.getCorrelatedTo());
    }

    public static LogicalSortExpression unsorted(final Quantifier inner) {
        return new LogicalSortExpression(RequestedOrdering.preserve(), inner);
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(getInner());
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    public RequestedOrdering getOrdering() {
        return ordering;
    }

    private Quantifier getInner() {
        return inner;
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public LogicalSortExpression translateCorrelations(final TranslationMap translationMap,
                                                       final boolean shouldSimplifyValues,
                                                       final List<? extends Quantifier> translatedQuantifiers) {
        return new LogicalSortExpression(getOrdering(), Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }

        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final LogicalSortExpression other = (LogicalSortExpression) otherExpression;
        return ordering.equals(other.ordering);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return ordering.hashCode();
    }

    @Override
    public PlannerGraph rewriteInternalPlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        if (ordering.isPreserve()) {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                            NodeInfo.SORT_OPERATOR,
                            ImmutableList.of("PRESERVE ORDER"),
                            ImmutableMap.of()),
                    childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                            NodeInfo.SORT_OPERATOR,
                            ImmutableList.of("BY {{expression}}"),
                            ImmutableMap.of("expression", Attribute.gml(ordering.toString()))),
                    childGraphs);

        }
    }
}
