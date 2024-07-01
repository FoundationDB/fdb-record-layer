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
import com.apple.foundationdb.record.planprotos.PRecordQueryMapPlan;
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
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
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
import java.util.function.Supplier;

/**
 * A query plan that applies the values it contains over the incoming ones. In a sense, this is similar to the {@code Stream.map()}
 * method: Mapping one {@link Value} to another.
 */
@API(API.Status.INTERNAL)
public class RecordQueryMapPlan implements RecordQueryPlanWithChild, RelationalExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Map-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Value resultValue;
    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;
    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlatedToWithoutChildrenSupplier;

    public RecordQueryMapPlan(@Nonnull Quantifier.Physical inner,
                              @Nonnull Value resultValue) {
        this.inner = inner;
        this.resultValue = resultValue;
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
        this.correlatedToWithoutChildrenSupplier = Suppliers.memoize(this::computeCorrelatedToWithoutChildren);
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return getChild().executePlan(store, context, continuation, executeProperties)
                .map(innerResult -> {
                    final EvaluationContext nestedContext = context.withBinding(inner.getAlias(), innerResult);
                    // Apply (map) each value to the incoming record
                    return innerResult.withComputed(resultValue.eval(store, nestedContext));
                });
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryMapPlan(Quantifier.physical(childRef, inner.getAlias()), resultValue);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return correlatedToWithoutChildrenSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return resultValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RecordQueryMapPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        final Value translatedResultValue = resultValue.translateCorrelations(translationMap);
        return new RecordQueryMapPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class), translatedResultValue);
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
    public RecordQueryMapPlan strictlySorted(@Nonnull Memoizer memoizer) {
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
        return hashCodeWithoutChildrenSupplier.get();
    }

    private int computeHashCodeWithoutChildren() {
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
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    public Quantifier.Physical getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("MAP {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(getResultValue().toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryMapPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryMapPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setResultValue(resultValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setMapPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryMapPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PRecordQueryMapPlan mapPlanProto) {
        return new RecordQueryMapPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(mapPlanProto.getInner())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(mapPlanProto.getResultValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryMapPlan, RecordQueryMapPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryMapPlan> getProtoMessageClass() {
            return PRecordQueryMapPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryMapPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PRecordQueryMapPlan recordQueryMapPlanProto) {
            return RecordQueryMapPlan.fromProto(serializationContext, recordQueryMapPlanProto);
        }
    }
}
