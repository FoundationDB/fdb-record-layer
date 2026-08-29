/*
 * RecordQueryUnionOnValuesPlan.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryInUnionOnValuesPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Union plan that compares using a {@link Value}.
 */
@SuppressWarnings("java:S2160")
public class RecordQueryInUnionOnValuesPlan extends RecordQueryInUnionPlan implements RecordQueryPlanWithComparisonKeyValues {

    /**
     * A list of {@link ProvidedOrderingPart}s that is used to compute the comparison key function. This attribute is
     * transient and therefore not plan-serialized
     */
    @Nullable
    private final List<ProvidedOrderingPart> comparisonKeyOrderingParts;

    protected RecordQueryInUnionOnValuesPlan(final PlanSerializationContext serializationContext,
                                             final PRecordQueryInUnionOnValuesPlan recordQueryInUnionOnValuesPlanProto) {
        super(serializationContext, recordQueryInUnionOnValuesPlanProto.getSuper());
        this.comparisonKeyOrderingParts = null;
    }

    public RecordQueryInUnionOnValuesPlan(final Quantifier.Physical inner,
                                          final List<? extends InSource> inSources,
                                          @Nullable final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                          final List<? extends Value> comparisonKeyValues,
                                          final boolean isReverse,
                                          final int maxNumberOfValuesAllowed,
                                          final Bindings.Internal internal) {
        super(inner,
                inSources,
                new ComparisonKeyFunction.OnValues(Quantifier.current(), comparisonKeyValues),
                isReverse,
                maxNumberOfValuesAllowed,
                internal);
        this.comparisonKeyOrderingParts =
                comparisonKeyOrderingParts == null
                ? null
                : ImmutableList.copyOf(comparisonKeyOrderingParts);
    }

    @Override
    public ComparisonKeyFunction.OnValues getComparisonKeyFunction() {
        return (ComparisonKeyFunction.OnValues)super.getComparisonKeyFunction();
    }

    @Override
    public List<? extends Value> getRequiredValues(final CorrelationIdentifier newBaseAlias, final Type inputType) {
        final var ruleSet = DefaultValueSimplificationRuleSet.instance();
        return getComparisonKeyValues().stream()
                .map(comparisonKeyValue -> comparisonKeyValue.rebase(AliasMap.ofAliases(Quantifier.current(), newBaseAlias))
                        .simplify(ruleSet, EvaluationContext.empty(), AliasMap.emptyMap(), getCorrelatedTo()))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Set<KeyExpression> getRequiredFields() {
        throw new RecordCoreException("this plan does not support this getRequiredFields()");
    }

    @Override
    public List<ProvidedOrderingPart> getComparisonKeyOrderingParts() {
        return Objects.requireNonNull(comparisonKeyOrderingParts);
    }

    @Override
    public List<? extends Value> getComparisonKeyValues() {
        return getComparisonKeyFunction().getComparisonKeyValues();
    }

    @Override
    public Set<Type> getDynamicTypes() {
        return getComparisonKeyValues().stream().flatMap(comparisonKeyValue -> comparisonKeyValue.getDynamicTypes().stream()).collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public RecordQueryInUnionOnValuesPlan withChildrenReferences(final List<? extends Reference> newChildren) {
        return withChild(Iterables.getOnlyElement(newChildren));
    }

    @Override
    public RecordQueryInUnionOnValuesPlan translateCorrelations(final TranslationMap translationMap,
                                                                final boolean shouldSimplifyValues,
                                                                final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInUnionOnValuesPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class), getInSources(),
                comparisonKeyOrderingParts, getComparisonKeyValues(), reverse, maxNumberOfValuesAllowed,
                internal);
    }

    @Override
    public RecordQueryInUnionOnValuesPlan withChild(final Reference childRef) {
        return new RecordQueryInUnionOnValuesPlan(Quantifier.physical(childRef, inner.getAlias()),
                getInSources(),
                comparisonKeyOrderingParts,
                getComparisonKeyValues(),
                reverse,
                maxNumberOfValuesAllowed,
                internal);
    }

    @Override
    public PRecordQueryInUnionOnValuesPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryInUnionOnValuesPlan.newBuilder()
                .setSuper(toRecordQueryInUnionPlanProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setInUnionOnValuesPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryInUnionOnValuesPlan fromProto(final PlanSerializationContext serializationContext,
                                                           final PRecordQueryInUnionOnValuesPlan recordQueryInUnionOnValuesPlanProto) {
        return new RecordQueryInUnionOnValuesPlan(serializationContext, recordQueryInUnionOnValuesPlanProto);
    }

    public static RecordQueryInUnionOnValuesPlan inUnion(final Quantifier.Physical inner,
                                                         final List<? extends InSource> inSources,
                                                         final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                         final boolean isReverse,
                                                         final int maxNumberOfValuesAllowed,
                                                         final Bindings.Internal internal) {
        return new RecordQueryInUnionOnValuesPlan(inner,
                inSources,
                comparisonKeyOrderingParts,
                ProvidedOrderingPart.comparisonKeyValues(comparisonKeyOrderingParts, isReverse),
                isReverse,
                maxNumberOfValuesAllowed,
                internal);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryInUnionOnValuesPlan, RecordQueryInUnionOnValuesPlan> {
        @Override
        public Class<PRecordQueryInUnionOnValuesPlan> getProtoMessageClass() {
            return PRecordQueryInUnionOnValuesPlan.class;
        }

        @Override
        public RecordQueryInUnionOnValuesPlan fromProto(final PlanSerializationContext serializationContext,
                                                        final PRecordQueryInUnionOnValuesPlan recordQueryInUnionOnValuesPlanProto) {
            return RecordQueryInUnionOnValuesPlan.fromProto(serializationContext, recordQueryInUnionOnValuesPlanProto);
        }
    }
}
