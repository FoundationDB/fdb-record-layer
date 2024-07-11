/*
 * RecordQuerySortPlan.java
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
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQuerySortPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithChild;
import com.apple.foundationdb.record.sorting.FileSortCursor;
import com.apple.foundationdb.record.sorting.MemorySortCursor;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A query plan implementing sorting in-memory, possibly spilling to disk.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQuerySortPlan implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Sort-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final RecordQuerySortKey key;

    public RecordQuerySortPlan(@Nonnull final RecordQueryPlan plan, @Nonnull final RecordQuerySortKey key) {
        this(Quantifier.physical(Reference.of(plan)), key);
    }

    private RecordQuerySortPlan(@Nonnull final Quantifier.Physical inner, @Nonnull final RecordQuerySortKey key) {
        this.inner = inner;
        this.key = key;
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                                     @Nonnull EvaluationContext context,
                                                                     @Nullable byte[] continuation,
                                                                     @Nonnull ExecuteProperties executeProperties) {
        // Since we are sorting, we need to feed through everything from the inner plan,
        // even just to get the top few.
        final ExecuteProperties executeInner = executeProperties.clearSkipAndLimit();
        final Function<byte[], RecordCursor<FDBQueriedRecord<M>>> innerCursor =
                innerContinuation -> getChild().executePlan(store, context, innerContinuation, executeInner)
                        .map(QueryResult::<M>getQueriedRecord);
        final int skip = executeProperties.getSkip();
        final int limit = executeProperties.getReturnedRowLimitOrMax();
        final int maxRecordsToRead = limit == Integer.MAX_VALUE ? limit : skip + limit;
        final RecordQuerySortAdapter<M> adapter = key.getAdapter(store, maxRecordsToRead);
        final FDBStoreTimer timer = store.getTimer();
        final RecordCursor<FDBQueriedRecord<M>> sorted;
        if (adapter.isMemoryOnly()) {
            sorted = MemorySortCursor.createSort(adapter, innerCursor, timer, continuation).skipThenLimit(skip, limit);
        } else {
            sorted = FileSortCursor.create(adapter, innerCursor, timer, continuation, skip, limit);
        }
        return sorted.map(QueryResult::fromQueriedRecord);
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    public RecordQuerySortKey getKey() {
        return key;
    }

    @Override
    public boolean isReverse() {
        return key.isReverse();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQuerySortPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQuerySortPlan((Quantifier.Physical)Iterables.getOnlyElement(translatedQuantifiers), key);
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQuerySortPlan(Quantifier.physical(childRef), key);
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
        final RecordQuerySortPlan other = (RecordQuerySortPlan) otherExpression;
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
    public int hashCodeWithoutChildren() {
        return Objects.hash(key);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild(), getKey());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_SORT);
        getChild().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        // TODO: Does not introduce any additional complexity, so not currently a good measure of sort vs no-sort.
        return getChild().getComplexity();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.SORT_OPERATOR),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQuerySortPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQuerySortPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setKey(key.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setSortPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQuerySortPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQuerySortPlan recordQuerySortPlanProto) {
        return new RecordQuerySortPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQuerySortPlanProto.getInner())),
                RecordQuerySortKey.fromProto(serializationContext, Objects.requireNonNull(recordQuerySortPlanProto.getKey())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQuerySortPlan, RecordQuerySortPlan> {
        @Nonnull
        @Override
        public Class<PRecordQuerySortPlan> getProtoMessageClass() {
            return PRecordQuerySortPlan.class;
        }

        @Nonnull
        @Override
        public RecordQuerySortPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PRecordQuerySortPlan recordQuerySortPlanProto) {
            return RecordQuerySortPlan.fromProto(serializationContext, recordQuerySortPlanProto);
        }
    }
}
