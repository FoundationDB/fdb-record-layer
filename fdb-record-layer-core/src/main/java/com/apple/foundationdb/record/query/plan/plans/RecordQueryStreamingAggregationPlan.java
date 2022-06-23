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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OrdinalFieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
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
     * @param groupingKeyAlias the identifier of {@code groupingKeyValue}
     * @param aggregateAlias the identifier of {@code aggregateValue}
     * @param completeResultValue the {@link Value} of the aggregate results
     */
    private RecordQueryStreamingAggregationPlan(@Nonnull final Quantifier.Physical inner,
                                                @Nullable final Value groupingKeyValue,
                                                @Nonnull final AggregateValue aggregateValue,
                                                @Nonnull final CorrelationIdentifier groupingKeyAlias,
                                                @Nonnull final CorrelationIdentifier aggregateAlias,
                                                @Nonnull final Value completeResultValue) {
        this.inner = inner;
        this.groupingKeyValue = groupingKeyValue;
        this.aggregateValue = aggregateValue;
        this.groupingKeyAlias = groupingKeyAlias;
        this.aggregateAlias = aggregateAlias;
        this.completeResultValue = completeResultValue;
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
        return new AggregateCursor<>(innerCursor, streamGrouping);
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
    public RecordQueryStreamingAggregationPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                                     @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedGroupingKeyValue = groupingKeyValue == null ? null : groupingKeyValue.translateCorrelations(translationMap);
        final var translatedAggregateValue = aggregateValue.translateCorrelations(translationMap);

        return new RecordQueryStreamingAggregationPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                translatedGroupingKeyValue,
                translatedAggregateValue,
                groupingKeyAlias,
                aggregateAlias,
                completeResultValue);
    }

    @Nonnull
    @Override
    public RecordQueryStreamingAggregationPlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryStreamingAggregationPlan(Quantifier.physical(GroupExpressionRef.of(child), inner.getAlias()),
                groupingKeyValue,
                aggregateValue,
                groupingKeyAlias,
                aggregateAlias,
                completeResultValue);
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
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
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
    public static Value flattenedResults(@Nullable final Value groupingKeyValue,
                                         @Nonnull final AggregateValue aggregateValue,
                                         @Nonnull final CorrelationIdentifier groupingKeyAlias,
                                         @Nonnull final CorrelationIdentifier aggregateAlias) {
        final var valuesBuilder = ImmutableList.<Value>builder();
        if (groupingKeyValue != null) {
            final var groupingResultType = groupingKeyValue.getResultType();
            if (groupingResultType.getTypeCode() == Type.TypeCode.RECORD) {
                Verify.verify(groupingResultType instanceof Type.Record);
                final var groupingResultRecordType = (Type.Record)groupingResultType;
                List<Type.Record.Field> fields = groupingResultRecordType.getFields();
                for (var i = 0; i < fields.size(); i++) {
                    valuesBuilder.add(OrdinalFieldValue.of(ObjectValue.of(groupingKeyAlias, groupingResultRecordType), i));
                }
            } else {
                valuesBuilder.add(groupingKeyValue);
            }
        }

        final var aggregateResultType = aggregateValue.getResultType();
        if (aggregateResultType.getTypeCode() == Type.TypeCode.RECORD) {
            Verify.verify(aggregateResultType instanceof Type.Record);
            final var aggregateResultRecordType = (Type.Record)aggregateResultType;
            List<Type.Record.Field> fields = aggregateResultRecordType.getFields();
            for (var i = 0; i < fields.size(); i++) {
                valuesBuilder.add(OrdinalFieldValue.of(ObjectValue.of(aggregateAlias, aggregateResultRecordType), i));
            }
        } else {
            valuesBuilder.add(aggregateValue);
        }

        return RecordConstructorValue.ofUnnamed(valuesBuilder.build());
    }

    @Nonnull
    public static Value nestedResults(@Nullable final Value groupingKeyValue,
                                      @Nonnull final AggregateValue aggregateValue,
                                      @Nonnull final CorrelationIdentifier groupingKeyAlias,
                                      @Nonnull final CorrelationIdentifier aggregateAlias) {
        if (groupingKeyValue != null) {
            return RecordConstructorValue.ofUnnamed(ImmutableList.of(
                    ObjectValue.of(groupingKeyAlias, groupingKeyValue.getResultType()),
                    ObjectValue.of(aggregateAlias, aggregateValue.getResultType())));
        } else {
            return RecordConstructorValue.ofUnnamed(ImmutableList.of(
                    RecordConstructorValue.ofUnnamed(ImmutableList.of()),
                    QuantifiedObjectValue.of(aggregateAlias, aggregateValue.getResultType())));
        }
    }

    public static RecordQueryStreamingAggregationPlan of(@Nonnull final Quantifier.Physical inner,
                                                         @Nullable final Value groupingKeyValue,
                                                         @Nonnull final AggregateValue aggregateValue,
                                                         @Nonnull final CompleteResultValueSupplier completeResultValueSupplier) {
        final var groupingKeyAlias = CorrelationIdentifier.uniqueID();
        final var aggregateAlias = CorrelationIdentifier.uniqueID();

        return new RecordQueryStreamingAggregationPlan(inner,
                groupingKeyValue,
                aggregateValue,
                groupingKeyAlias,
                aggregateAlias,
                completeResultValueSupplier.supply(groupingKeyValue, aggregateValue, groupingKeyAlias, aggregateAlias));
    }

    /**
     * Lambda for assembling the complete result from the grouping key(s) and aggregate value(s).
     */
    @FunctionalInterface
    public interface CompleteResultValueSupplier {
        Value supply(@Nullable Value groupingKeyValue,
                     @Nonnull AggregateValue aggregateValue,
                     @Nonnull CorrelationIdentifier groupingKeyAlias,
                     @Nonnull CorrelationIdentifier aggregateAlias);
    }
}
