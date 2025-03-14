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
import com.apple.foundationdb.record.EvaluationContextBuilder;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionMultiCursor;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
                                                       @Nonnull final PRecordQueryIntersectionOnValuesPlan recordQueryIntersectionOnValuesPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryIntersectionOnValuesPlanProto.getSuper()));
        this.comparisonKeyOrderingParts = null;
    }

    private RecordQueryMultiIntersectionOnValuesPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                     @Nullable final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                     @Nonnull final List<? extends Value> comparisonKeyValues,
                                                     final boolean reverse) {
        super(quantifiers,
                new ComparisonKeyFunction.OnValues(Quantifier.current(), comparisonKeyValues),
                reverse);
        this.comparisonKeyOrderingParts =
                comparisonKeyOrderingParts == null ? null : ImmutableList.copyOf(comparisonKeyOrderingParts);
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
        throw new RecordCoreException("this plan does not support getRequiredValues()");
    }

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        throw new RecordCoreException("this plan does not support getRequiredFields()");
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
        return getComparisonKeyValues().stream().flatMap(comparisonKeyValue -> comparisonKeyValue.getDynamicTypes().stream()).collect(ImmutableSet.toImmutableSet());
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
                                .collect(Collectors.toList()),
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
                isReverse());
    }

    @Override
    public RecordQueryMultiIntersectionOnValuesPlan strictlySorted(@Nonnull final Memoizer memoizer) {
        return this;
    }

    @Nonnull
    @Override
    public PRecordQueryIntersectionOnValuesPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryIntersectionOnValuesPlan.newBuilder().setSuper(toRecordQueryIntersectionPlan(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setIntersectionOnValuesPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryMultiIntersectionOnValuesPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                     @Nonnull final PRecordQueryIntersectionOnValuesPlan recordQueryIntersectionOnValuesPlanProto) {
        return new RecordQueryMultiIntersectionOnValuesPlan(serializationContext, recordQueryIntersectionOnValuesPlanProto);
    }

    @Nonnull
    public static RecordQueryMultiIntersectionOnValuesPlan intersection(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                                        @Nonnull final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                                        final boolean isReverse) {
        return new RecordQueryMultiIntersectionOnValuesPlan(quantifiers,
                comparisonKeyOrderingParts,
                ProvidedOrderingPart.comparisonKeyValues(comparisonKeyOrderingParts, isReverse),
                isReverse);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryIntersectionOnValuesPlan, RecordQueryMultiIntersectionOnValuesPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryIntersectionOnValuesPlan> getProtoMessageClass() {
            return PRecordQueryIntersectionOnValuesPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryMultiIntersectionOnValuesPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                  @Nonnull final PRecordQueryIntersectionOnValuesPlan recordQueryIntersectionOnValuesPlanProto) {
            return RecordQueryMultiIntersectionOnValuesPlan.fromProto(serializationContext, recordQueryIntersectionOnValuesPlanProto);
        }
    }
}
