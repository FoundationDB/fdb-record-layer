/*
 * RecordQueryStreamingAggregationPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.aggregate.AggregateCursor;
import com.apple.foundationdb.record.cursors.aggregate.StreamGrouping;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that applies an aggregate function(s) to its inputs and also places them into groups.
 * This plan will:
 * <UL>
 * <LI>Group items by its list of group {@link Value}s. This list (order matters) holds the {@link Value}s that will
 * decide
 * how records are collated into groups. Within each group, all concrete fields are equal</LI>
 * <LI>Aggregate items by its list of {@link AggregateValue} values. Each concrete {@link AggregateValue} (their order
 * in the list determines their order in the output) will calculate the aggregation function (SUM, MIN, MAX etc.) over
 * its field</LI>
 * </UL>
 */
@API(API.Status.INTERNAL)
public class RecordQueryStreamingAggregationPlan implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Streaming-Aggregator-Plan");
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryStreamingAggregationPlan.class);

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final AggregateValue aggregateValue;
    @Nullable
    private final Value groupingKeyValue;
    @Nonnull
    private final CorrelationIdentifier groupingKeyAlias;
    @Nonnull
    private final CorrelationIdentifier aggregateAlias;
    @Nonnull
    private final Value completeResultValue;

    /**
     * Construct a new plan.
     *
     * @param inner the quantifier that this plan owns
     * @param groupingKeyValue the {@link Value} to group by
     * @param aggregateValue the {@link AggregateValue} to aggregate by grouping key
     */
    public RecordQueryStreamingAggregationPlan(@Nonnull final Quantifier.Physical inner, @Nullable final Value groupingKeyValue, @Nonnull final AggregateValue aggregateValue) {
        this.inner = inner;
        this.groupingKeyValue = groupingKeyValue;
        this.aggregateValue = aggregateValue;
        this.groupingKeyAlias = CorrelationIdentifier.uniqueID();
        this.aggregateAlias = CorrelationIdentifier.uniqueID();
        this.completeResultValue = computeCompleteResultValue(groupingKeyValue, aggregateValue, groupingKeyAlias, aggregateAlias);
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                                     @Nonnull EvaluationContext context,
                                                                     @Nullable byte[] continuation,
                                                                     @Nonnull ExecuteProperties executeProperties) {
        final var innerCursor = getInnerPlan().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit());

        final var streamGrouping =
                new StreamGrouping<>(groupingKeyValue,
                        aggregateValue,
                        completeResultValue,
                        groupingKeyAlias,
                        aggregateAlias,
                        (FDBRecordStoreBase<Message>)store,
                        context,
                        inner.getAlias());
        return new AggregateCursor<>(innerCursor, streamGrouping).map(QueryResult::of);
    }

    @Override
    public boolean isReverse() {
        return getInnerPlan().isReverse();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        return ImmutableSet.copyOf(Iterables.concat(
                RecordQueryPlanWithChild.super.getDynamicTypes(),
                groupingKeyValue == null ? ImmutableSet.of() : groupingKeyValue.getDynamicTypes(),
                aggregateValue.getDynamicTypes()));
    }

    @Nonnull
    @Override
    public String toString() {
        return getInnerPlan() + " | AGGREGATE BY " + aggregateValue + ", GROUP BY " + groupingKeyValue;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.copyOf(
                Iterables.concat(groupingKeyValue == null ? ImmutableSet.of() : groupingKeyValue.getCorrelatedTo(),
                        aggregateValue.getCorrelatedTo()));
    }

    @Nonnull
    @Override
    public RecordQueryStreamingAggregationPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                            @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryStreamingAggregationPlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class), groupingKeyValue, aggregateValue);
    }

    @Nonnull
    @Override
    public RecordQueryStreamingAggregationPlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryStreamingAggregationPlan(Quantifier.physical(GroupExpressionRef.of(child), inner.getAlias()), groupingKeyValue, aggregateValue);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return completeResultValue;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        // Results are combination of groupCriteria and aggregateValues
        return semanticEqualsForResults(otherExpression, equivalencesMap);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH, groupingKeyValue, aggregateValue);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getInnerPlan(), groupingKeyValue, aggregateValue);
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_AGGREGATE);
        getInnerPlan().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInnerPlan().getComplexity();
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     *
     * @param childGraphs planner graphs of children expression that already have been computed
     *
     * @return the rewritten planner graph that models the plan as a node that uses the expression attribute
     * to depict the record types this operator filters.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        if (groupingKeyValue != null) {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.OperatorNodeWithInfo(this,
                            NodeInfo.STREAMING_AGGREGATE_OPERATOR,
                            ImmutableList.of("COLLECT {{agg}}", "GROUP BY {{groupingKey}}"),
                            ImmutableMap.of("agg", Attribute.gml(aggregateValue.toString()), "groupingKey", Attribute.gml(groupingKeyValue.toString()))),
                    childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.OperatorNodeWithInfo(this,
                            NodeInfo.STREAMING_AGGREGATE_OPERATOR,
                            ImmutableList.of("COLLECT {{agg}}"),
                            ImmutableMap.of("agg", Attribute.gml(aggregateValue.toString()))),
                    childGraphs);
        }
    }

    @Nonnull
    private static Value computeCompleteResultValue(@Nullable final Value groupingKeyValue,
                                                    @Nonnull final AggregateValue aggregateValue,
                                                    @Nonnull final CorrelationIdentifier groupingKeyAlias,
                                                    @Nonnull final CorrelationIdentifier aggregateAlias) {
        if (groupingKeyValue != null) {
            return RecordConstructorValue.ofUnnamed(ImmutableList.of(
                    QuantifiedObjectValue.of(groupingKeyAlias, groupingKeyValue.getResultType()),
                    QuantifiedObjectValue.of(aggregateAlias, aggregateValue.getResultType())));
        } else {
            return QuantifiedObjectValue.of(aggregateAlias, aggregateValue.getResultType());
        }
    }
}
