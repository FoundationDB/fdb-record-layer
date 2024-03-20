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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A logical {@code group by} expression that represents grouping incoming tuples and aggregating each group.
 */
@API(API.Status.EXPERIMENTAL)
public class GroupByExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {

    @Nonnull
    private final List<Value> groupingValues;

    @Nonnull
    private final List<AggregateValue> aggregateValues;

    @Nonnull
    private final Supplier<Value> computeResultSupplier;

    @Nonnull
    private final Supplier<RequestedOrdering> computeRequestedOrderingSupplier;

    @Nonnull
    private final Quantifier inner;

    /**
     * Creates a new instance of {@link GroupByExpression}.
     *
     * @param aggregateValues The aggregation {@code Value}s applied to each group.
     * @param groupingValues The grouping {@code Value}s used to determine individual groups, can be empty indicating no grouping.
     * @param inner The underlying source of tuples to be grouped.
     */
    public GroupByExpression(@Nonnull final List<AggregateValue> aggregateValues,
                             @Nonnull final List<Value> groupingValues,
                             @Nonnull final Quantifier inner) {
        this.aggregateValues = ImmutableList.copyOf(aggregateValues);
        this.groupingValues = ImmutableList.copyOf(groupingValues);
        this.computeResultSupplier = Suppliers.memoize(this::computeResultValue);
        this.computeRequestedOrderingSupplier = Suppliers.memoize(this::computeRequestedOrdering);
        this.inner = inner;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return getResultValue().getCorrelatedTo();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return computeResultSupplier.get();
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

        if (this.getGroupingValues().size() != otherGroupByExpr.getGroupingValues().size()) {
            return false;
        }

        if (this.getAggregateValues().size() != otherGroupByExpr.getAggregateValues().size()) {
            return false;
        }

        for (int i = 0; i < this.getGroupingValues().size(); i++) {
            if (this.getAggregateValues().get(i).semanticEquals(otherGroupByExpr.getAggregateValues().get(i), equivalences)) {
                return false;
            }
        }

        for (int i = 0; i < this.getGroupingValues().size(); i++) {
            if (this.getGroupingValues().get(i).semanticEquals(otherGroupByExpr.getGroupingValues().get(i), equivalences)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
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
        final List<AggregateValue> translatedAggregateValues = getAggregateValues()
                .stream()
                .map(aggregateValue -> (AggregateValue)aggregateValue.translateCorrelations(translationMap))
                .collect(ImmutableList.toImmutableList());
        final var translatedGroupingValues = getGroupingValues()
                .stream()
                .map(groupingValue -> groupingValue.translateCorrelations(translationMap))
                .collect(ImmutableList.toImmutableList());
        return new GroupByExpression(translatedAggregateValues, translatedGroupingValues, Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Override
    public String toString() {
        return "GroupBy("
                + (getGroupingValues().isEmpty() ? "NULL" : groupingValues.stream().map(Objects::toString).collect(Collectors.joining(",")))
                + "), aggregationValue: " + getAggregateValues() + ", resultValue: " + computeResultSupplier.get();
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        if (getGroupingValues().isEmpty()) {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}"),
                            ImmutableMap.of("agg", Attribute.gml(getAggregateValues().toString()))),
                    childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}", "GROUP BY {{grouping}}"),
                            ImmutableMap.of("agg", Attribute.gml(getAggregateValues().toString()),
                                    "grouping", Attribute.gml(getGroupingValues().toString()))),
                    childGraphs);
        }
    }

    @Nonnull
    public List<Value> getGroupingValues() {
        return groupingValues;
    }

    @Nonnull
    public List<AggregateValue> getAggregateValues() {
        return aggregateValues;
    }

    /**
     * Returns the ordering requirements of the underlying scan for the group by to work. This is used by the planner
     * to choose a compatibly-ordered access path.
     *
     * @return The ordering requirements.
     */
    @Nonnull
    public RequestedOrdering getRequestedOrdering() {
        return computeRequestedOrderingSupplier.get();
    }

    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final var matchInfo = partialMatch.getMatchInfo();
        final var quantifier = Iterables.getOnlyElement(getQuantifiers());

        // if the match requires, for the moment, any, compensation, we reject it.
        final Optional<Compensation> childCompensation = matchInfo.getChildPartialMatch(quantifier)
                                .map(childPartialMatch -> childPartialMatch.compensate(boundParameterPrefixMap));

        if (childCompensation.isPresent() && (childCompensation.get().isImpossible() || childCompensation.get().isNeeded())) {
            return Compensation.impossibleCompensation();
        }

        return Compensation.noCompensation();
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap aliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {

        // the candidate must be a GROUP-BY expression.
        if (candidateExpression.getClass() != this.getClass()) {
            return ImmutableList.of();
        }

        final var otherGroupByExpression = (GroupByExpression)candidateExpression;

        // the grouping values are encoded directly in the underlying SELECT-WHERE, reaching this point means that the
        // grouping values had exact match so we don't need to check them.


        // check that aggregate value is the same.
        final var otherAggregateValue = otherGroupByExpression.getAggregateValues();
        if (RecordConstructorValue.ofUnnamed(aggregateValues).subsumedBy(RecordConstructorValue.ofUnnamed(otherAggregateValue), aliasMap)) {
            // placeholder for information needed for later compensation.
            return MatchInfo.tryMerge(partialMatchMap, ImmutableMap.of(), PredicateMap.empty(), Optional.empty())
                    .map(ImmutableList::of)
                    .orElse(ImmutableList.of());
        }
        return ImmutableList.of();
    }

    @Nonnull
    private Value computeResultValue() {
        // this preserves any names given to grouping columns and aggregates.
        final ImmutableList.Builder<Column<? extends Value>> columnsBuilder = ImmutableList.builder();
        for (final var groupingValue : groupingValues) {
            if (groupingValue instanceof Column) {
                columnsBuilder.add((Column<? extends Value>)groupingValue);
            } else if (groupingValue instanceof FieldValue) {
                final var groupingFieldValue = (FieldValue)groupingValue;
                columnsBuilder.add(Column.of(groupingFieldValue.getLastFieldName(), groupingFieldValue));
            } else {
                columnsBuilder.add(Column.unnamedOf(groupingValue));
            }
        }
        for (final var aggregateValue : aggregateValues) {
            if (aggregateValue instanceof Column) {
                columnsBuilder.add((Column<? extends Value>)aggregateValue);
            } else if (aggregateValue instanceof FieldValue) {
                final var aggregateFieldValue = (FieldValue)aggregateValue;
                columnsBuilder.add(Column.of(aggregateFieldValue.getLastFieldName(), aggregateFieldValue));
            } else {
                columnsBuilder.add(Column.unnamedOf(aggregateValue));
            }
        }
        return RecordConstructorValue.ofColumns(columnsBuilder.build());
    }

    @Nonnull
    private RequestedOrdering computeRequestedOrdering() {
        if (groupingValues.stream().allMatch(Value::isConstant)) {
            return RequestedOrdering.preserve();
        }

        return new RequestedOrdering(
                groupingValues.stream().map(OrderingPart::of).collect(ImmutableList.toImmutableList()),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);
    }
}
