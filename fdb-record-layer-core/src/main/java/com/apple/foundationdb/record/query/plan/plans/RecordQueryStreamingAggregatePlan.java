/*
 * RecordQueryStreamingAggregatePlan.java
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
import com.apple.foundationdb.record.cursors.aggregate.GroupAggregator;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
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
import java.util.Set;
import java.util.function.Supplier;

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
public class RecordQueryStreamingAggregatePlan implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Streaming-Aggregator-Plan");
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryStreamingAggregatePlan.class);

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Supplier<List<? extends Value>> resultValuesSupplier;
    @Nonnull
    private final List<AggregateValue<?>> aggregateValues;
    @Nonnull
    private final List<Value> groupByCriteria;

    /**
     * Construct a new plan.
     *
     * @param inner the quantifier that this plan owns
     * @param groupByCriteria the list of {@link Value} to group by
     * @param aggregateValues the list of {@link AggregateValue} to aggregate by
     */
    public RecordQueryStreamingAggregatePlan(@Nonnull final Quantifier.Physical inner, @Nonnull final List<Value> groupByCriteria, @Nonnull final List<AggregateValue<?>> aggregateValues) {
        this.inner = inner;
        this.groupByCriteria = groupByCriteria;
        this.aggregateValues = aggregateValues;
        // todo: use Suppliers.memoize
        this.resultValuesSupplier = (() -> ImmutableList.<Value>builder().addAll(this.groupByCriteria).addAll(this.aggregateValues).build());
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                                     @Nonnull EvaluationContext context,
                                                                     @Nullable byte[] continuation,
                                                                     @Nonnull ExecuteProperties executeProperties) {
        final RecordCursor<QueryResult> innerCursor = getInnerPlan().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit());
        @SuppressWarnings("unchecked")
        GroupAggregator<Message> groupAggregator = new GroupAggregator<>(groupByCriteria, aggregateValues, (FDBRecordStoreBase<Message>)store, context, inner.getAlias());
        return new AggregateCursor<>(innerCursor, groupAggregator);
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
    public String toString() {
        return getInnerPlan().toString() + " | AGGREGATE BY " + aggregateValues + ", GROUP BY " + groupByCriteria;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        // TODO
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryStreamingAggregatePlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                          @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryStreamingAggregatePlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class), groupByCriteria, aggregateValues);
    }

    @Nonnull
    @Override
    public RecordQueryStreamingAggregatePlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryStreamingAggregatePlan(Quantifier.physical(GroupExpressionRef.of(child)), groupByCriteria, aggregateValues);
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValues() {
        return resultValuesSupplier.get();
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
        return (true);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return BASE_HASH.hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getInnerPlan(), groupByCriteria, aggregateValues);
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
        return PlannerGraph.fromNodeAndChildGraphs(
                // TODO
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.STREAMING_AGGREGATE_OPERATOR,
                        ImmutableList.of("SUM(A) GROUP BY {B}"),
                        ImmutableMap.of()),
                childGraphs);
    }
}
