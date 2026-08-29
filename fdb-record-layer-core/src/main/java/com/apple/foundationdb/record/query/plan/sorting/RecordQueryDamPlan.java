/*
 * RecordQueryDamPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryDamPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithChild;
import com.apple.foundationdb.record.sorting.MemorySortCursor;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A query plan implementing a dam in-memory.
 * A dam consumes the last record from the inner before it produces records which introduces a before and after
 * into the data flow. A consumer of this plan knows that when the first record is observed, the last record of the dam's
 * input has been consumed already.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQueryDamPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Dam-Plan");

    private final Quantifier.Physical inner;

    /**
     * The <em>sort</em> key. This key is used to create an instance of {@link RecordQuerySortAdapter} to extract
     * the actual key information from the incoming records. Keys, identify records, but, unlike in
     * {@link RecordQuerySortPlan}, keys are not needed to define an order among records as a dam iterates its records
     * in insertion order. The actual key comparison defined by {@link RecordQuerySortAdapter} is only used to establish
     * equality.
     */
    private final RecordQuerySortKey key;

    @HeuristicPlanner
    public RecordQueryDamPlan(RecordQueryPlan plan, RecordQuerySortKey key) {
        this(Quantifier.physical(Reference.plannedOf(Debugger.verifyHeuristicPlanner(plan))), key);
    }

    private RecordQueryDamPlan(Quantifier.Physical inner, RecordQuerySortKey key) {
        this.inner = inner;
        this.key = key;
    }

    @Override
    @SuppressWarnings({"PMD.CloseResource", "resource"})
    public <M extends Message> RecordCursor<QueryResult> executePlan(FDBRecordStoreBase<M> store,
                                                                     EvaluationContext context,
                                                                     @Nullable byte[] continuation,
                                                                     ExecuteProperties executeProperties) {
        //
        // We need to feed through everything from the inner plan, even just to get the top few.
        // If the in-memory-dam is in-memory-limited, we may actually go over the input multiple times which is not
        // desirable for a proper dam. However, in reality we use that plan operator in a way that we are
        // in-memory-unlimited which in turn ensures that we go over the input exactly once.
        //
        final ExecuteProperties executeInner = executeProperties.clearSkipAndLimit();
        final Function<byte[], RecordCursor<FDBQueriedRecord<M>>> innerCursor =
                innerContinuation -> getChild().executePlan(store, context, innerContinuation, executeInner)
                        .map(QueryResult::getQueriedRecord);
        final int skip = executeProperties.getSkip();
        final int limit = executeProperties.getReturnedRowLimitOrMax();
        final RecordQuerySortAdapter<M> adapter = key.getAdapterForDam(store);
        final FDBStoreTimer timer = store.getTimer();
        final RecordCursor<FDBQueriedRecord<M>> dammed =
                MemorySortCursor.createDam(adapter, innerCursor, timer, continuation).skipThenLimit(skip, limit);
        return dammed.map(queriedRecord -> QueryResult.fromQueriedRecord(getResultValue().getResultType(), context, queriedRecord));
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    public RecordQuerySortKey getKey() {
        return key;
    }

    @Override
    public boolean isReverse() {
        return key.isReverse();
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public Value getResultValue() {
        return new QueriedValue();
    }

    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public String toString() {
        return "Dam(" + getChild() + ")";
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public RecordQueryDamPlan translateCorrelations(final TranslationMap translationMap,
                                                    final boolean shouldSimplifyValues,
                                                    final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryDamPlan((Quantifier.Physical)Iterables.getOnlyElement(translatedQuantifiers), key);
    }

    @Override
    public RecordQueryPlanWithChild withChild(Reference childRef) {
        return new RecordQueryDamPlan(Quantifier.physical(childRef, inner.getAlias()), key);
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
        final RecordQueryDamPlan other = (RecordQueryDamPlan) otherExpression;
        return key.equals(other.key);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(key);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild(), getKey());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_DAM);
        getChild().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        // TODO: Does not introduce any additional complexity, so not currently a good measure of dam vs no-dam.
        return getChild().getComplexity();
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.DAM_OPERATOR),
                childGraphs);
    }

    @Override
    public PRecordQueryDamPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryDamPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setKey(key.toProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setDamPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryDamPlan fromProto(final PlanSerializationContext serializationContext,
                                               final PRecordQueryDamPlan recordQueryDamPlan) {
        return new RecordQueryDamPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryDamPlan.getInner())),
                RecordQuerySortKey.fromProto(serializationContext, Objects.requireNonNull(recordQueryDamPlan.getKey())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryDamPlan, RecordQueryDamPlan> {
        @Override
        public Class<PRecordQueryDamPlan> getProtoMessageClass() {
            return PRecordQueryDamPlan.class;
        }

        @Override
        public RecordQueryDamPlan fromProto(final PlanSerializationContext serializationContext,
                                            final PRecordQueryDamPlan recordQueryDamPlanProto) {
            return RecordQueryDamPlan.fromProto(serializationContext, recordQueryDamPlanProto);
        }
    }
}
