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
import com.apple.foundationdb.record.planprotos.PRecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.DerivedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that only flows the first record it processes from its inner and then stops. If the inner is empty,
 * i.e. does not produce any records, a default value that is passed into the constructor is returned in place of
 * the first record.
 */
@API(API.Status.INTERNAL)
public class RecordQueryFirstOrDefaultPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-First-Or-Default-Plan");

    private final Quantifier.Physical inner;
    private final Value onEmptyResultValue;
    private final Value resultValue;

    public RecordQueryFirstOrDefaultPlan(final Quantifier.Physical inner,
                                         final Value onEmptyResultValue) {
        final var innerType = inner.getFlowedObjectType();
        Verify.verify(innerType.nullable().equals(onEmptyResultValue.getResultType().nullable()));
        this.inner = inner;
        this.onEmptyResultValue = onEmptyResultValue;
        this.resultValue = new DerivedValue(ImmutableList.of(inner.getFlowedObjectValue(), onEmptyResultValue),
                innerType.withNullability(onEmptyResultValue.getResultType().isNullable()));
    }

    public Value getOnEmptyResultValue() {
        return onEmptyResultValue;
    }

    @SuppressWarnings("resource")
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store,
                                                                     final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     final ExecuteProperties executeProperties) {
        // Note that a null child continuation is always handed to the child cursor below.
        // This is because the returned FutureCursor only ever returns a single value, and so if
        // that lambda is called, it indicates that the original continuation is null, and we
        // are starting the plan from the beginning. That this doesn't handle the inner cursor
        // halting with an out-of-band no-next-reasons, which currently is treated the same way
        // as the inner cursor being empty.
        // See: https://github.com/FoundationDB/fdb-record-layer/issues/3220
        return RecordCursor.fromFuture(store.getExecutor(),
                () -> getChild().executePlan(store, context, null, executeProperties).first()
                        .thenApply(resultOptional -> resultOptional.orElseGet(() -> QueryResult.ofComputed(onEmptyResultValue.eval(store, context)))),
                continuation);
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Override
    public RecordQueryPlanWithChild withChild(final Reference childRef) {
        return new RecordQueryFirstOrDefaultPlan(Quantifier.physical(childRef, inner.getAlias()), onEmptyResultValue);
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return onEmptyResultValue.getCorrelatedTo();
    }

    @Override
    public RelationalExpression translateCorrelations(final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        final Value rebasedOnEmptyResultValue =
                onEmptyResultValue.translateCorrelations(translationMap, shouldSimplifyValues);
        return new RecordQueryFirstOrDefaultPlan(Iterables.getOnlyElement(translatedQuantifiers)
                .narrow(Quantifier.Physical.class), rebasedOnEmptyResultValue);
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
    public RecordQueryFirstOrDefaultPlan strictlySorted(FinalMemoizer memoizer) {
        return this;
    }

    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap aliasMap) {
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
    public int computeHashCodeWithoutChildren() {
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
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild(), getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.name() + " is not supported");
        }
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("FIRST {{inner}} OR {{expr}}"),
                        ImmutableMap.of("inner", Attribute.gml("$" + inner.getAlias()),
                                "expr", Attribute.gml(onEmptyResultValue.toString()))),
                childGraphs);
    }

    @Override
    public PRecordQueryFirstOrDefaultPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryFirstOrDefaultPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setOnEmptyResultValue(onEmptyResultValue.toValueProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setFirstOrDefaultPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryFirstOrDefaultPlan fromProto(final PlanSerializationContext serializationContext,
                                                          final PRecordQueryFirstOrDefaultPlan recordQueryFirstOrDefaultPlanProto) {
        return new RecordQueryFirstOrDefaultPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryFirstOrDefaultPlanProto.getInner())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryFirstOrDefaultPlanProto.getOnEmptyResultValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryFirstOrDefaultPlan, RecordQueryFirstOrDefaultPlan> {
        @Override
        public Class<PRecordQueryFirstOrDefaultPlan> getProtoMessageClass() {
            return PRecordQueryFirstOrDefaultPlan.class;
        }

        @Override
        public RecordQueryFirstOrDefaultPlan fromProto(final PlanSerializationContext serializationContext,
                                                       final PRecordQueryFirstOrDefaultPlan recordQueryFirstOrDefaultPlanProto) {
            return RecordQueryFirstOrDefaultPlan.fromProto(serializationContext, recordQueryFirstOrDefaultPlanProto);
        }
    }
}
