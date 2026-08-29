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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
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

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Intersection plan that compares using a {@link Value}.
 */
@SuppressWarnings("java:S2160")
public class RecordQueryIntersectionOnValuesPlan extends RecordQueryIntersectionPlan implements RecordQueryPlanWithComparisonKeyValues {

    /**
     * A list of {@link ProvidedOrderingPart}s that is used to compute the comparison key function. This attribute is
     * transient and therefore not plan-serialized
     */
    @Nullable
    private final List<ProvidedOrderingPart> comparisonKeyOrderingParts;

    protected RecordQueryIntersectionOnValuesPlan(final PlanSerializationContext serializationContext,
                                                  final PRecordQueryIntersectionOnValuesPlan recordQueryIntersectionOnValuesPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryIntersectionOnValuesPlanProto.getSuper()));
        this.comparisonKeyOrderingParts = null;
    }

    private RecordQueryIntersectionOnValuesPlan(final List<Quantifier.Physical> quantifiers,
                                                @Nullable final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                final List<? extends Value> comparisonKeyValues,
                                                final boolean reverse) {
        super(quantifiers,
                new ComparisonKeyFunction.OnValues(Quantifier.current(), comparisonKeyValues),
                reverse);
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
                .map(comparisonKeyValue ->
                        comparisonKeyValue.rebase(AliasMap.ofAliases(Quantifier.current(), newBaseAlias))
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
    public RecordQueryIntersectionOnValuesPlan translateCorrelations(final TranslationMap translationMap,
                                                                     final boolean shouldSimplifyValues,
                                                                     final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryIntersectionOnValuesPlan(
                Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers), comparisonKeyOrderingParts,
                getComparisonKeyValues(), isReverse());
    }

    @Override
    public RecordQueryIntersectionOnValuesPlan withChildrenReferences(final List<? extends Reference> newChildren) {
        return new RecordQueryIntersectionOnValuesPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                comparisonKeyOrderingParts,
                getComparisonKeyValues(),
                isReverse());
    }

    @Override
    public RecordQueryIntersectionOnValuesPlan strictlySorted(final FinalMemoizer memoizer) {
        final var quantifiers =
                Quantifiers.fromPlans(getChildren()
                        .stream()
                        .map(p -> memoizer.memoizePlan(p.strictlySorted(memoizer))).collect(Collectors.toList()));
        return new RecordQueryIntersectionOnValuesPlan(quantifiers, comparisonKeyOrderingParts, getComparisonKeyValues(), reverse);
    }

    @Override
    public PRecordQueryIntersectionOnValuesPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryIntersectionOnValuesPlan.newBuilder().setSuper(toRecordQueryIntersectionPlan(serializationContext)).build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setIntersectionOnValuesPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryIntersectionOnValuesPlan fromProto(final PlanSerializationContext serializationContext,
                                                                final PRecordQueryIntersectionOnValuesPlan recordQueryIntersectionOnValuesPlanProto) {
        return new RecordQueryIntersectionOnValuesPlan(serializationContext, recordQueryIntersectionOnValuesPlanProto);
    }

    public static RecordQueryIntersectionOnValuesPlan intersection(final List<Quantifier.Physical> quantifiers,
                                                                   final List<ProvidedOrderingPart> comparisonKeyOrderingParts,
                                                                   final boolean isReverse) {
        return new RecordQueryIntersectionOnValuesPlan(quantifiers,
                comparisonKeyOrderingParts,
                ProvidedOrderingPart.comparisonKeyValues(comparisonKeyOrderingParts, isReverse),
                isReverse);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryIntersectionOnValuesPlan, RecordQueryIntersectionOnValuesPlan> {
        @Override
        public Class<PRecordQueryIntersectionOnValuesPlan> getProtoMessageClass() {
            return PRecordQueryIntersectionOnValuesPlan.class;
        }

        @Override
        public RecordQueryIntersectionOnValuesPlan fromProto(final PlanSerializationContext serializationContext,
                                                             final PRecordQueryIntersectionOnValuesPlan recordQueryIntersectionOnValuesPlanProto) {
            return RecordQueryIntersectionOnValuesPlan.fromProto(serializationContext, recordQueryIntersectionOnValuesPlanProto);
        }
    }
}
