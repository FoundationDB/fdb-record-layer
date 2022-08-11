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
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A logical {@code group by} expression that represents grouping incoming tuples and aggregating each group.
 */
@API(API.Status.EXPERIMENTAL)
public class GroupByExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {

    @Nullable
    private final Value groupingValue;

    @Nonnull
    private final AggregateValue aggregateValue;

    @Nonnull
    private final Supplier<Value> resultValue = Suppliers.memoize(this::computeResultValue);

    @Nonnull
    private final Supplier<CorrelationIdentifier> groupingValueAlias;

    @Nonnull
    private final Supplier<CorrelationIdentifier> aggregateValueAlias;

    @Nonnull
    private final Quantifier inner;

    /**
     * Creates a new instance of {@link GroupByExpression}.
     *
     * @param aggregateValue The aggregation {@code Value} applied to each group.
     * @param groupingValue The grouping {@code Value} used to determine individual groups, can be {@code null} indicating no grouping.
     * @param inner The underlying source of tuples to be grouped.
     */
    public GroupByExpression(@Nonnull final AggregateValue aggregateValue,
                             @Nullable final Value groupingValue,
                             @Nonnull final Quantifier inner) {
        this.groupingValue = groupingValue;
        this.aggregateValue = aggregateValue;
        this.inner = inner;
        this.groupingValueAlias = Suppliers.memoize(() -> groupingValue == null ? null : getAlias(groupingValue));
        this.aggregateValueAlias = Suppliers.memoize(() -> getAlias(aggregateValue));
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
        return resultValue.get();
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
        final var otherGroupByExpr = ((GroupByExpression)other);
        return Objects.equals(groupingValue, otherGroupByExpr.groupingValue) &&
               Objects.equals(aggregateValue, otherGroupByExpr.aggregateValue);
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
        final AggregateValue translatedAggregateValue = aggregateValue.translateCorrelations(translationMap);
        final Value translatedGroupingValue = groupingValue == null ? null : groupingValue.translateCorrelations(translationMap);
        if (translatedAggregateValue != aggregateValue || translatedGroupingValue != groupingValue) {
            return new GroupByExpression(translatedAggregateValue, translatedGroupingValue, Iterables.getOnlyElement(translatedQuantifiers));
        }
        return this;
    }

    @Override
    public String toString() {
        if (groupingValue != null) {
            return "GroupBy(" + groupingValue + "), aggregationValue: " + aggregateValue + ", resultValue: " + resultValue.get();
        } else {
            return "GroupBy(NULL), aggregationValue: " + aggregateValue + ", resultValue: " + resultValue.get();
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        if (groupingValue == null) {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}", "RESULT {{result}}"),
                            ImmutableMap.of("agg", Attribute.gml(aggregateValue.toString()),
                                    "result", Attribute.gml(resultValue.get().toString()))),
                    childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}", "GROUP BY {{grouping}}", "RESULT {{result}}"),
                            ImmutableMap.of("agg", Attribute.gml(aggregateValue.toString()),
                                    "grouping", Attribute.gml(groupingValue.toString()),
                                    "result", Attribute.gml(resultValue.get().toString()))),
                    childGraphs);
        }
    }

    @Nullable
    public Value getGroupingValue() {
        return groupingValue;
    }

    @Nullable
    public CorrelationIdentifier getGroupingValueAlias() {
        return groupingValueAlias.get();
    }

    @Nonnull
    public AggregateValue getAggregateValue() {
        return aggregateValue;
    }

    @Nonnull
    public CorrelationIdentifier getAggregateValueAlias() {
        return aggregateValueAlias.get();
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
        // deriving the ordering columns correctly requires fix for https://github.com/FoundationDB/fdb-record-layer/issues/1212
        // perform pseudo-derivation until we have a fix.
        final var groupingExpr = (RecordConstructorValue)getGroupingValue();
        Verify.verify(groupingExpr.getResultType().getFields().size() == 1);
        final var field = groupingExpr.getResultType().getFields().get(0);
        Verify.verify(field.getFieldType() instanceof Type.Record);
        final var recordType = (Type.Record)field.getFieldType();
        return new RequestedOrdering(
                recordType.getFields().stream().map(innerField -> KeyPart.of(innerField.toKeyExpression())).collect(Collectors.toList()),
                RequestedOrdering.Distinctness.NOT_DISTINCT);
    }

    @Nonnull
    private Value computeResultValue() {
        return RecordQueryStreamingAggregationPlan.flattenedResults(groupingValue, aggregateValue, getGroupingValueAlias(), getAggregateValueAlias());
    }

    @Nonnull
    private static CorrelationIdentifier getAlias(@Nonnull final Value value) {
        Verify.verify(value.getResultType().getTypeCode() == Type.TypeCode.RECORD);
        Type.Record resultType = (Type.Record)value.getResultType();
        Verify.verify(resultType.getFields().size() == 1);
        Verify.verify(resultType.getFields().get(0).getFieldNameOptional().isPresent());
        return CorrelationIdentifier.of(resultType.getFields().get(0).getFieldName());
    }
}
