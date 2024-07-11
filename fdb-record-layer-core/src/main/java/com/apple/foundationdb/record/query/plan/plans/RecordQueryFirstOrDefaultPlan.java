/*
 * RecordQueryMapPlan.java
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
import com.apple.foundationdb.record.cursors.FutureCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.values.DerivedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that only flows the first record it processes from its inner and then stops. If the inner is empty,
 * i.e. does not produce any records, a default value that is passed into the constructor is returned in place of
 * the first record.
 */
@API(API.Status.INTERNAL)
public class RecordQueryFirstOrDefaultPlan implements RecordQueryPlanWithChild, RelationalExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-First-Or-Default-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Value onEmptyResultValue;
    @Nonnull
    private final Value resultValue;

    public RecordQueryFirstOrDefaultPlan(@Nonnull Quantifier.Physical inner,
                                         @Nonnull Value onEmptyResultValue) {
        Verify.verify(inner.getFlowedObjectType().nullable().equals(onEmptyResultValue.getResultType().nullable()));
        this.inner = inner;
        this.onEmptyResultValue = onEmptyResultValue;
        this.resultValue = new DerivedValue(ImmutableList.of(inner.getFlowedObjectValue(), onEmptyResultValue), inner.getFlowedObjectType());
    }

    @Nonnull
    public Value getOnEmptyResultValue() {
        return onEmptyResultValue;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return new FutureCursor<>(store.getExecutor(),
                getChild().executePlan(store, context, continuation, executeProperties).first()
                        .thenApply(resultOptional ->
                                resultOptional.orElseGet(() -> QueryResult.ofComputed(onEmptyResultValue.eval(store, context)))));
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryFirstOrDefaultPlan(Quantifier.physical(childRef, inner.getAlias()), onEmptyResultValue);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return onEmptyResultValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        final Value rebasedResultValues = onEmptyResultValue.translateCorrelations(translationMap);
        return new RecordQueryFirstOrDefaultPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class), rebasedResultValues);
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public boolean isStrictlySorted() {
        return false;
    }

    @Override
    public RecordQueryFirstOrDefaultPlan strictlySorted(@Nonnull Memoizer memoizer) {
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return semanticEqualsForResults(otherExpression, aliasMap);
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
        return getChild().getComplexity();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild(), getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("FIRST {{inner}} OR {{expr}}"),
                        ImmutableMap.of("inner", Attribute.gml("$" + inner.getAlias()),
                                "expr", Attribute.gml(onEmptyResultValue.toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryFirstOrDefaultPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryFirstOrDefaultPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setOnEmptyResultValue(onEmptyResultValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setFirstOrDefaultPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryFirstOrDefaultPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                          @Nonnull final PRecordQueryFirstOrDefaultPlan recordQueryFirstOrDefaultPlanProto) {
        return new RecordQueryFirstOrDefaultPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryFirstOrDefaultPlanProto.getInner())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryFirstOrDefaultPlanProto.getOnEmptyResultValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryFirstOrDefaultPlan, RecordQueryFirstOrDefaultPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryFirstOrDefaultPlan> getProtoMessageClass() {
            return PRecordQueryFirstOrDefaultPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryFirstOrDefaultPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PRecordQueryFirstOrDefaultPlan recordQueryFirstOrDefaultPlanProto) {
            return RecordQueryFirstOrDefaultPlan.fromProto(serializationContext, recordQueryFirstOrDefaultPlanProto);
        }
    }
}
