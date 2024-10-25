/*
 * RecordQueryTableQueuePlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryTableQueuePlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TableQueue;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * TODO: expand doc.
 */
@API(API.Status.INTERNAL)
public class RecordQueryTableQueuePlan implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Table-Queue-Plan");

    @Nonnull
    private final TableQueue tableQueue;

    @Nonnull
    private final Type resultType;

    public RecordQueryTableQueuePlan(final Type resultType) {
        this(TableQueue.newInstance(), resultType);
    }

    public RecordQueryTableQueuePlan(@Nonnull TableQueue tableQueue, @Nonnull Type resultType) {
        this.tableQueue = tableQueue;
        this.resultType = resultType;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                                     @Nonnull EvaluationContext context,
                                                                     @Nullable byte[] continuation,
                                                                     @Nonnull ExecuteProperties executeProperties) {
        return new TableQueue.VolatileListCursor(tableQueue::getReadBuffer, continuation);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryTableQueuePlan translateCorrelations(@Nonnull TranslationMap translationMap,
                                                           @Nonnull List<? extends Quantifier> translatedQuantifiers) {
        return this;
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public RecordQueryTableQueuePlan strictlySorted(@Nonnull Memoizer memoizer) {
        return this;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return false;
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(resultType);
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        return ImmutableSet.of(resultType); // TODO: this needs improvement.
    }


    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherTableQueuePlan =  (RecordQueryTableQueuePlan)otherExpression;

        return otherTableQueuePlan.resultType.equals(resultType);
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
        return Objects.hash(getResultValue());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of()),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryTableQueuePlan toProto(@Nonnull PlanSerializationContext serializationContext) {
        return PRecordQueryTableQueuePlan.newBuilder()
                .setTableQueue(tableQueue.toProto())
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setTableQueuePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryTableQueuePlan fromProto(@Nonnull PlanSerializationContext serializationContext,
                                                      @Nonnull PRecordQueryTableQueuePlan tableQueuePlanProto) {
        final Type resultType = Type.fromTypeProto(serializationContext, tableQueuePlanProto.getResultType());
        // we need to deserialize the type right now, ideally we should have access to a TypeRepository that we can
        // (re)use but we do not at the moment.
        TypeRepository temporaryTypeRepository = TypeRepository.newBuilder().addTypeIfNeeded(resultType).build();
        @Nullable final var descriptor = temporaryTypeRepository.getMessageDescriptor(resultType);
        final TableQueue tableQueue = TableQueue.fromProto(tableQueuePlanProto.getTableQueue(), descriptor);
        return new RecordQueryTableQueuePlan(tableQueue, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryTableQueuePlan, RecordQueryTableQueuePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryTableQueuePlan> getProtoMessageClass() {
            return PRecordQueryTableQueuePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryTableQueuePlan fromProto(@Nonnull PlanSerializationContext serializationContext,
                                                   @Nonnull PRecordQueryTableQueuePlan recordQueryTableQueuePlan) {
            return RecordQueryTableQueuePlan.fromProto(serializationContext, recordQueryTableQueuePlan);
        }
    }
}
