/*
 * GroupByExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.KeyPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A logical {@code group by} expression that represents grouping incoming tuples and aggregating each group.
 */
@API(API.Status.EXPERIMENTAL)
public class GroupByExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {

    @Nonnull
    private final Value groupingValue;

    @Nonnull
    private final AggregateValue aggregateValue;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final Quantifier inner;

    public GroupByExpression(@Nonnull final Value groupingValue, @Nonnull final AggregateValue aggregateValue, @Nonnull final Value resultValue, @Nonnull final Quantifier inner) {
        this.groupingValue = groupingValue;
        this.aggregateValue = aggregateValue;
        this.resultValue = resultValue;
        this.inner = inner;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        if (!groupingValue.equals(((GroupByExpression)other).groupingValue)) {
            return false;
        }

        if (!aggregateValue.equals(((GroupByExpression)other).aggregateValue)) {
            return false;
        }

        return resultValue.equals(other.getResultValue());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(resultValue, groupingValue, aggregateValue);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
        return semanticEquals(other);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final Value translatedResultValue = resultValue.translateCorrelations(translationMap);
        final AggregateValue translatedAggregateValue = aggregateValue.translateCorrelations(translationMap);
        final Value translatedGroupingValue = groupingValue.translateCorrelations(translationMap);
        if (translatedAggregateValue != aggregateValue || translatedResultValue != resultValue || translatedGroupingValue != groupingValue) {
            return new GroupByExpression(translatedGroupingValue, translatedAggregateValue, translatedResultValue, Iterables.getOnlyElement(translatedQuantifiers));
        }
        return this;
    }

    @Override
    public String toString() {
        return "GroupBy(" + groupingValue + "), aggregationValue: " + aggregateValue + ", resultValue: " + resultValue;
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "GROUP BY " + groupingValue,
                        List.of("AGG. " + aggregateValue, "RES. " + resultValue),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Nonnull
    public Value getGroupingValue() {
        return groupingValue;
    }

    @Nonnull
    public AggregateValue getAggregateValue() {
        return aggregateValue;
    }

    /**
     * Returns the ordering requirements of the underlying scan for the group by to work. This is used by the planner
     * to choose a compatibly-ordered access path.
     *
     * @return The ordering requirements.
     */
    @Nonnull
    public RequestedOrdering getOrderingRequirement() {
        Verify.verify(getGroupingValue() instanceof RecordConstructorValue);
        final var groupingExpr = (RecordConstructorValue)getGroupingValue();
        return new RequestedOrdering(
                groupingExpr.getResultType().getFields().stream().map(field -> KeyPart.of(field.toKeyExpression())).collect(Collectors.toList()),
                RequestedOrdering.Distinctness.NOT_DISTINCT);
    }
}
