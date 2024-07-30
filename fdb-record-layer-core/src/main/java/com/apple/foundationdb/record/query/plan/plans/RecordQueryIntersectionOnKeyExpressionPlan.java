/*
 * RecordQueryIntersectionOnKeyExpressionPlan.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Intersection plan that compares using a {@link KeyExpression}.
 */
public class RecordQueryIntersectionOnKeyExpressionPlan extends RecordQueryIntersectionPlan {

    protected RecordQueryIntersectionOnKeyExpressionPlan(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PRecordQueryIntersectionOnKeyExpressionPlan recordQueryIntersectionOnKeyExpressionPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryIntersectionOnKeyExpressionPlanProto.getSuper()));
    }

    public RecordQueryIntersectionOnKeyExpressionPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                      @Nonnull final KeyExpression comparisonKey,
                                                      final boolean reverse) {
        super(quantifiers,
                new ComparisonKeyFunction.OnKeyExpression(comparisonKey),
                reverse);
    }

    @Nonnull
    @Override
    public ComparisonKeyFunction.OnKeyExpression getComparisonKeyFunction() {
        return (ComparisonKeyFunction.OnKeyExpression)super.getComparisonKeyFunction();
    }

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        return ImmutableSet.copyOf(getComparisonKeyExpression().normalizeKeyForPositions());
    }

    @Nonnull
    public KeyExpression getComparisonKeyExpression() {
        return getComparisonKeyFunction().getComparisonKey();
    }

    @Nonnull
    @Override
    public RecordQueryIntersectionOnKeyExpressionPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryIntersectionOnKeyExpressionPlan(Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers),
                getComparisonKeyExpression(),
                isReverse());
    }

    @Nonnull
    @Override
    public RecordQueryIntersectionOnKeyExpressionPlan withChildrenReferences(@Nonnull final List<? extends Reference> newChildren) {
        return new RecordQueryIntersectionOnKeyExpressionPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                getComparisonKeyExpression(),
                isReverse());
    }

    @Override
    public RecordQueryIntersectionOnKeyExpressionPlan strictlySorted(@Nonnull final Memoizer memoizer) {
        final var quantifiers =
                Quantifiers.fromPlans(getChildren()
                        .stream()
                        .map(p -> memoizer.memoizePlans((RecordQueryPlan)p.strictlySorted(memoizer))).collect(Collectors.toList()));
        return new RecordQueryIntersectionOnKeyExpressionPlan(quantifiers, getComparisonKeyExpression(), reverse);
    }

    @Nonnull
    @Override
    public PRecordQueryIntersectionOnKeyExpressionPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryIntersectionOnKeyExpressionPlan.newBuilder().setSuper(toRecordQueryIntersectionPlan(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setIntersectionOnKeyExpressionPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryIntersectionOnKeyExpressionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                       @Nonnull final PRecordQueryIntersectionOnKeyExpressionPlan recordQueryIntersectionOnKeyExpressionPlanProto) {
        return new RecordQueryIntersectionOnKeyExpressionPlan(serializationContext, recordQueryIntersectionOnKeyExpressionPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryIntersectionOnKeyExpressionPlan, RecordQueryIntersectionOnKeyExpressionPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryIntersectionOnKeyExpressionPlan> getProtoMessageClass() {
            return PRecordQueryIntersectionOnKeyExpressionPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryIntersectionOnKeyExpressionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                    @Nonnull final PRecordQueryIntersectionOnKeyExpressionPlan recordQueryIntersectionOnKeyExpressionPlanProto) {
            return RecordQueryIntersectionOnKeyExpressionPlan.fromProto(serializationContext, recordQueryIntersectionOnKeyExpressionPlanProto);
        }
    }
}
