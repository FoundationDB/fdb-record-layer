/*
 * RecordQueryUnionOnKeyExpressionPlan.java
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
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryUnionOnKeyExpressionPlan;
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

/**
 * Union plan that compares using a {@link KeyExpression}.
 */
public class RecordQueryUnionOnKeyExpressionPlan extends RecordQueryUnionPlan {

    protected RecordQueryUnionOnKeyExpressionPlan(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PRecordQueryUnionOnKeyExpressionPlan recordQueryUnionOnKeyExpressionPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryUnionOnKeyExpressionPlanProto.getSuper()));
    }

    public RecordQueryUnionOnKeyExpressionPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                               @Nonnull final KeyExpression comparisonKey,
                                               final boolean reverse,
                                               final boolean showComparisonKey) {
        super(quantifiers,
                new ComparisonKeyFunction.OnKeyExpression(comparisonKey),
                reverse,
                showComparisonKey);
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
    public RecordQueryUnionOnKeyExpressionPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUnionOnKeyExpressionPlan(Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers),
                getComparisonKeyExpression(),
                isReverse(),
                showComparisonKey);
    }

    @Nonnull
    @Override
    public RecordQueryUnionOnKeyExpressionPlan withChildrenReferences(@Nonnull final List<? extends Reference> newChildren) {
        return new RecordQueryUnionOnKeyExpressionPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                getComparisonKeyExpression(),
                isReverse(),
                showComparisonKey);
    }

    @Nonnull
    @Override
    public PRecordQueryUnionOnKeyExpressionPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryUnionOnKeyExpressionPlan.newBuilder()
                .setSuper(toRecordQueryUnionPlanProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setUnionOnKeyExpressionPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryUnionOnKeyExpressionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                @Nonnull final PRecordQueryUnionOnKeyExpressionPlan recordQueryUnionOnKeyExpressionPlanProto) {
        return new RecordQueryUnionOnKeyExpressionPlan(serializationContext, recordQueryUnionOnKeyExpressionPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryUnionOnKeyExpressionPlan, RecordQueryUnionOnKeyExpressionPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryUnionOnKeyExpressionPlan> getProtoMessageClass() {
            return PRecordQueryUnionOnKeyExpressionPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryUnionOnKeyExpressionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                             @Nonnull final PRecordQueryUnionOnKeyExpressionPlan recordQueryUnionOnKeyExpressionPlanProto) {
            return RecordQueryUnionOnKeyExpressionPlan.fromProto(serializationContext, recordQueryUnionOnKeyExpressionPlanProto);
        }
    }
}
