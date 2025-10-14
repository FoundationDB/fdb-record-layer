/*
 * RecordQueryDefaultOnEmptyPlan.java
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
import com.apple.foundationdb.record.planprotos.PRecordQueryDefaultOnEmptyPlan;
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
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
 * A query plan that only flows all the records it processes from its inner. If the inner is empty, i.e. does not
 * produce any records, a default value that is passed into the constructor is returned in place of the first record.
 */
@API(API.Status.INTERNAL)
public class RecordQueryDefaultOnEmptyPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Default-On-Empty-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Value onEmptyResultValue;
    @Nonnull
    private final Value resultValue;

    public RecordQueryDefaultOnEmptyPlan(@Nonnull Quantifier.Physical inner,
                                         @Nonnull Value onEmptyResultValue) {
        Verify.verify(inner.getFlowedObjectType().nullable().equals(onEmptyResultValue.getResultType().nullable()));
        this.inner = inner;
        this.onEmptyResultValue = onEmptyResultValue;
        this.resultValue = new DerivedValue(ImmutableList.of(inner.getFlowedObjectValue(), onEmptyResultValue), chooseNullableType(inner.getFlowedObjectType(), onEmptyResultValue.getResultType()));
    }

    /**
     * Choose the type that is nullable, if any. That is, if one type is nullable and the other
     * one is not, return the nullable one. Otherwise (that is, the two types have the same nullability),
     * choose {@code type1}.
     *
     * @param type1 the first type
     * @param type2 the second type
     * @return whichever of {@code type1} and {@code type2} are nullable
     *   or {@code type1} if both are not nullable (or both are nullable)
     */
    @Nonnull
    private static Type chooseNullableType(@Nonnull Type type1, @Nonnull Type type2) {
        if (type1.isNullable()) {
            return type1;
        } else {
            return type2.isNullable() ? type2 : type1;
        }
    }

    @Nonnull
    public Value getOnEmptyResultValue() {
        return onEmptyResultValue;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return RecordCursor.orElse(cont -> getChild().executePlan(store, context, cont, executeProperties.clearSkipAndLimit()),
                        (executor, cont) ->
                                RecordCursor.fromList(executor, ImmutableList.of(QueryResult.ofComputed(onEmptyResultValue.eval(store, context))), cont), continuation)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryDefaultOnEmptyPlan(Quantifier.physical(childRef, inner.getAlias()), onEmptyResultValue);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return onEmptyResultValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        final Value rebasedOnEmptyResultValue =
                onEmptyResultValue.translateCorrelations(translationMap, shouldSimplifyValues);
        return new RecordQueryDefaultOnEmptyPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                rebasedOnEmptyResultValue);
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public boolean isStrictlySorted() {
        return getChild().isStrictlySorted();
    }

    @Override
    public RecordQueryDefaultOnEmptyPlan strictlySorted(@Nonnull FinalMemoizer memoizer) {
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
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
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
                        ImmutableList.of("{{inner}} OR {{expr}}"),
                        ImmutableMap.of("inner", Attribute.gml("$" + inner.getAlias()),
                                "expr", Attribute.gml(onEmptyResultValue.toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryDefaultOnEmptyPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryDefaultOnEmptyPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setOnEmptyResultValue(onEmptyResultValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setDefaultOnEmptyPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryDefaultOnEmptyPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                          @Nonnull final PRecordQueryDefaultOnEmptyPlan recordQueryDefaultOnEmptyPlanProto) {
        return new RecordQueryDefaultOnEmptyPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryDefaultOnEmptyPlanProto.getInner())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryDefaultOnEmptyPlanProto.getOnEmptyResultValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryDefaultOnEmptyPlan, RecordQueryDefaultOnEmptyPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryDefaultOnEmptyPlan> getProtoMessageClass() {
            return PRecordQueryDefaultOnEmptyPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryDefaultOnEmptyPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PRecordQueryDefaultOnEmptyPlan recordQueryDefaultOnEmptyPlanProto) {
            return RecordQueryDefaultOnEmptyPlan.fromProto(serializationContext, recordQueryDefaultOnEmptyPlanProto);
        }
    }
}
