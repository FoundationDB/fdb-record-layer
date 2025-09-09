/*
 * RecordQueryIntersectionOnValuesPlan.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryMultiIntersectionOnValuesPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionMultiCursor;
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Intersection plan that compares using a {@link Value}.
 */
@SuppressWarnings("java:S2160")
public class RecordQueryMultiIntersectionOnValuesPlan extends RecordQueryIntersectionPlan implements RecordQueryPlanWithComparisonKeyValues {

    /**
     * A list of {@link ProvidedOrderingPart}s that is used to compute the comparison key function. This attribute is
     * transient and therefore not plan-serialized
     */
    @Nullable
    private final List<ProvidedOrderingPart> comparisonKeyOrderingParts;
    @Nonnull
    private final Value resultValue;

    protected RecordQueryMultiIntersectionOnValuesPlan(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PRecordQueryMultiIntersectionOnValuesPlan recordQueryMultiIntersectionOnValuesPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryMultiIntersectionOnValuesPlanProto.getSuper()));
        this.comparisonKeyOrderingParts = null;
        this.resultValue = Value.fromValueProto(serializationContext,
                recordQueryMultiIntersectionOnValuesPlanProto.getResultValue());
    }

    private RecordQueryMultiIntersectionOnValuesPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                     @Nullable final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                     @Nonnull final List<? extends Value> comparisonKeyValues,
                                                     @Nonnull final Value resultValue,
                                                     final boolean reverse) {
        super(quantifiers,
                new ComparisonKeyFunction.OnValues(Quantifier.current(), comparisonKeyValues),
                reverse);
        this.comparisonKeyOrderingParts =
                comparisonKeyOrderingParts == null ? null : ImmutableList.copyOf(comparisonKeyOrderingParts);
        this.resultValue = resultValue;
    }

    @Nonnull
    @Override
    public ComparisonKeyFunction.OnValues getComparisonKeyFunction() {
        return (ComparisonKeyFunction.OnValues)super.getComparisonKeyFunction();
    }

    @Nonnull
    @Override
    public List<? extends Value> getRequiredValues(@Nonnull final CorrelationIdentifier newBaseAlias,
                                                   @Nonnull final Type inputType) {
        throw new UnsupportedOperationException();
    }

    @HeuristicPlanner
    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public List<ProvidedOrderingPart> getComparisonKeyOrderingParts() {
        return Objects.requireNonNull(comparisonKeyOrderingParts);
    }

    @Nonnull
    @Override
    public List<? extends Value> getComparisonKeyValues() {
        return getComparisonKeyFunction().getComparisonKeyValues();
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        return Streams.concat(getComparisonKeyValues().stream(), Stream.of(resultValue))
                .flatMap(comparisonKeyValue ->
                        comparisonKeyValue.getDynamicTypes().stream()).collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    protected Value computeResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    @SuppressWarnings("resource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final var quantifiers = getQuantifiers();
        final ExecuteProperties childExecuteProperties = executeProperties.clearSkipAndLimit();
        return IntersectionMultiCursor.create(
                        getComparisonKeyFunction().apply(store, context),
                        reverse,
                        quantifiers.stream()
                                .map(physical -> ((Quantifier.Physical)physical).getRangesOverPlan())
                                .map(childPlan -> (Function<byte[], RecordCursor<QueryResult>>)
                                        ((byte[] childContinuation) -> childPlan
                                                .executePlan(store, context, childContinuation, childExecuteProperties)))
                                .collect(ImmutableList.toImmutableList()),
                        continuation,
                        store.getTimer())
                .map(multiResult -> {
                    Verify.verify(getQuantifiers().size() == multiResult.size());
                    final var childEvaluationContextBuilder = context.childBuilder();
                    for (int i = 0; i < quantifiers.size(); i++) {
                        childEvaluationContextBuilder.setBinding(quantifiers.get(i).getAlias(), multiResult.get(i));
                    }
                    final var childEvaluationContext =
                            childEvaluationContextBuilder.build(context.getTypeRepository());
                    return QueryResult.ofComputed(getResultValue().eval(store, childEvaluationContext));
                })
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nonnull
    @Override
    public RecordQueryMultiIntersectionOnValuesPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                                          final boolean shouldSimplifyValues,
                                                                          @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryMultiIntersectionOnValuesPlan(Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers),
                comparisonKeyOrderingParts,
                getComparisonKeyValues(),
                resultValue,
                isReverse());
    }

    @Nonnull
    @Override
    public RecordQueryMultiIntersectionOnValuesPlan withChildrenReferences(@Nonnull final List<? extends Reference> newChildren) {
        return new RecordQueryMultiIntersectionOnValuesPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                comparisonKeyOrderingParts,
                getComparisonKeyValues(),
                resultValue,
                isReverse());
    }

    @HeuristicPlanner
    @Override
    public RecordQueryMultiIntersectionOnValuesPlan strictlySorted(@Nonnull final FinalMemoizer finalMemoizer) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public PRecordQueryMultiIntersectionOnValuesPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryMultiIntersectionOnValuesPlan.newBuilder()
                .setSuper(toRecordQueryIntersectionPlan(serializationContext))
                .setResultValue(resultValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder()
                .setMultiIntersectionOnValuesPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.INTERSECTION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKeyFunction}}", "RESULT {{resultValue}}"),
                        ImmutableMap.of("comparisonKeyFunction", Attribute.gml(getComparisonKeyFunction().toString()),
                                "resultValue", Attribute.gml(resultValue.toString()))),
                childGraphs);
    }

    @Nonnull
    public static RecordQueryMultiIntersectionOnValuesPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                     @Nonnull final PRecordQueryMultiIntersectionOnValuesPlan recordQueryMultiIntersectionOnValuesPlanProto) {
        return new RecordQueryMultiIntersectionOnValuesPlan(serializationContext, recordQueryMultiIntersectionOnValuesPlanProto);
    }

    @Nonnull
    public static RecordQueryMultiIntersectionOnValuesPlan intersection(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                                        @Nonnull final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                                        @Nonnull final Value resultValue,
                                                                        final boolean isReverse) {
        return new RecordQueryMultiIntersectionOnValuesPlan(quantifiers,
                comparisonKeyOrderingParts,
                ProvidedOrderingPart.comparisonKeyValues(comparisonKeyOrderingParts, isReverse),
                resultValue,
                isReverse);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryMultiIntersectionOnValuesPlan, RecordQueryMultiIntersectionOnValuesPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryMultiIntersectionOnValuesPlan> getProtoMessageClass() {
            return PRecordQueryMultiIntersectionOnValuesPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryMultiIntersectionOnValuesPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                  @Nonnull final PRecordQueryMultiIntersectionOnValuesPlan recordQueryMultiIntersectionOnValuesPlanProto) {
            return RecordQueryMultiIntersectionOnValuesPlan.fromProto(serializationContext, recordQueryMultiIntersectionOnValuesPlanProto);
        }
    }
}
