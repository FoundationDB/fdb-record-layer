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

import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Union plan that compares using a {@link Value}.
 */
@SuppressWarnings("java:S2160")
public class RecordQueryUnionOnValuesPlan extends RecordQueryUnionPlan  implements RecordQueryPlanWithComparisonKeyValues {

    protected RecordQueryUnionOnValuesPlan(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PRecordQueryUnionOnValuesPlan recordQueryUnionOnValuesPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryUnionOnValuesPlanProto.getSuper()));
    }

    public RecordQueryUnionOnValuesPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                        @Nonnull final List<? extends Value> comparisonKeyValues,
                                        final boolean reverse,
                                        final boolean showComparisonKey) {
        super(quantifiers,
                new ComparisonKeyFunction.OnValues(Quantifier.current(), comparisonKeyValues),
                reverse,
                showComparisonKey);
    }

    @Nonnull
    @Override
    public ComparisonKeyFunction.OnValues getComparisonKeyFunction() {
        return (ComparisonKeyFunction.OnValues)super.getComparisonKeyFunction();
    }

    @Nonnull
    @Override
    public List<? extends Value> getRequiredValues(@Nonnull final CorrelationIdentifier newBaseAlias, @Nonnull final Type inputType) {
        final var ruleSet = DefaultValueSimplificationRuleSet.ofSimplificationRules();
        return getComparisonKeyValues().stream()
                .map(comparisonKeyValue -> comparisonKeyValue.rebase(AliasMap.ofAliases(Quantifier.current(), newBaseAlias)).simplify(ruleSet, AliasMap.emptyMap(), getCorrelatedTo()))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        throw new RecordCoreException("this plan does not support this getRequiredFields()");
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
    public RecordQueryUnionOnValuesPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUnionOnValuesPlan(Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers),
                getComparisonKeyValues(),
                isReverse(),
                showComparisonKey);
    }

    @Nonnull
    @Override
    public RecordQueryUnionOnValuesPlan withChildrenReferences(@Nonnull final List<? extends Reference> newChildren) {
        return new RecordQueryUnionOnValuesPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                getComparisonKeyValues(),
                isReverse(),
                showComparisonKey);
    }

    @Nonnull
    @Override
    public PRecordQueryUnionOnValuesPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryUnionOnValuesPlan.newBuilder()
                .setSuper(toRecordQueryUnionPlanProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setUnionOnValuesPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryUnionOnValuesPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PRecordQueryUnionOnValuesPlan recordQueryUnionOnValuesPlanProto) {
        return new RecordQueryUnionOnValuesPlan(serializationContext, recordQueryUnionOnValuesPlanProto);
    }

    @Nonnull
    public static RecordQueryUnionOnValuesPlan union(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                     @Nonnull final List<? extends Value> comparisonKeyValues,
                                                     final boolean reverse,
                                                     final boolean showComparisonKey) {
        return new RecordQueryUnionOnValuesPlan(quantifiers, comparisonKeyValues, reverse, showComparisonKey);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryUnionOnValuesPlan, RecordQueryUnionOnValuesPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryUnionOnValuesPlan> getProtoMessageClass() {
            return PRecordQueryUnionOnValuesPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryUnionOnValuesPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PRecordQueryUnionOnValuesPlan recordQueryUnionOnValuesPlanProto) {
            return RecordQueryUnionOnValuesPlan.fromProto(serializationContext, recordQueryUnionOnValuesPlanProto);
        }
    }
}
