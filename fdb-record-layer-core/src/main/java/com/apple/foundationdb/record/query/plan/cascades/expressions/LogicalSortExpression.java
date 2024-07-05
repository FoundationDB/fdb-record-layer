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

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A relational planner expression that represents an unimplemented sort on the records produced by its inner
 * relational planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalSortExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {
    @Nonnull
    private final RequestedOrdering ordering;

    @Nonnull
    private final Quantifier inner;

    public LogicalSortExpression(@Nonnull RequestedOrdering ordering, @Nonnull Quantifier inner) {
        this.ordering = ordering;
        this.inner = inner;
    }

    public LogicalSortExpression(@Nonnull List<Value> sortValues,
                                 final boolean reverse,
                                 @Nonnull final Quantifier inner) {
        this(buildOrdering(sortValues, reverse), inner);
    }

    @Nonnull
    private static RequestedOrdering buildOrdering(@Nonnull List<Value> sortValues, boolean reverse) {
        final OrderingPart.RequestedSortOrder order = OrderingPart.RequestedSortOrder.fromIsReverse(reverse);
        final RequestedOrdering.Distinctness distinctness = RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS;
        return new RequestedOrdering(sortValues.stream().map(value -> new OrderingPart.RequestedOrderingPart(value, order)).collect(Collectors.toList()), distinctness);
    }

    @Nonnull
    public static LogicalSortExpression unsorted(@Nonnull final Quantifier inner) {
        return new LogicalSortExpression(new RequestedOrdering(Collections.emptyList(), RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS), inner);
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(getInner());
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    public RequestedOrdering getOrdering() {
        return ordering;
    }

    @Nonnull
    private Quantifier getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public LogicalSortExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                       @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new LogicalSortExpression(getOrdering(),
                Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
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
    public int hashCodeWithoutChildren() {
        return ordering.hashCode();
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
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
