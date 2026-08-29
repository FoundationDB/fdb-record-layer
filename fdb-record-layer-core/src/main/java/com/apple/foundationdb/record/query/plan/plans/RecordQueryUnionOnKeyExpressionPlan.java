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

import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
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

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Union plan that compares using a {@link KeyExpression}.
 */
@HeuristicPlanner
public class RecordQueryUnionOnKeyExpressionPlan extends RecordQueryUnionPlan {

    protected RecordQueryUnionOnKeyExpressionPlan(final PlanSerializationContext serializationContext,
                                                  final PRecordQueryUnionOnKeyExpressionPlan recordQueryUnionOnKeyExpressionPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryUnionOnKeyExpressionPlanProto.getSuper()));
    }

    public RecordQueryUnionOnKeyExpressionPlan(final List<Quantifier.Physical> quantifiers,
                                               final KeyExpression comparisonKey,
                                               final boolean reverse,
                                               final boolean showComparisonKey) {
        super(quantifiers,
                new ComparisonKeyFunction.OnKeyExpression(comparisonKey),
                reverse,
                showComparisonKey);
    }

    @Override
    public ComparisonKeyFunction.OnKeyExpression getComparisonKeyFunction() {
        return (ComparisonKeyFunction.OnKeyExpression)super.getComparisonKeyFunction();
    }

    @Override
    public Set<KeyExpression> getRequiredFields() {
        return ImmutableSet.copyOf(getComparisonKeyExpression().normalizeKeyForPositions());
    }

    public KeyExpression getComparisonKeyExpression() {
        return getComparisonKeyFunction().getComparisonKey();
    }

    @Override
    public RecordQueryUnionOnKeyExpressionPlan translateCorrelations(final TranslationMap translationMap,
                                                                     final boolean shouldSimplifyValues,
                                                                     final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUnionOnKeyExpressionPlan(Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers),
                getComparisonKeyExpression(),
                isReverse(),
                showComparisonKey);
    }

    @Override
    public RecordQueryUnionOnKeyExpressionPlan withChildrenReferences(final List<? extends Reference> newChildren) {
        return new RecordQueryUnionOnKeyExpressionPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                getComparisonKeyExpression(),
                isReverse(),
                showComparisonKey);
    }

    @Override
    public PRecordQueryUnionOnKeyExpressionPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryUnionOnKeyExpressionPlan.newBuilder()
                .setSuper(toRecordQueryUnionPlanProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setUnionOnKeyExpressionPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryUnionOnKeyExpressionPlan fromProto(final PlanSerializationContext serializationContext,
                                                                final PRecordQueryUnionOnKeyExpressionPlan recordQueryUnionOnKeyExpressionPlanProto) {
        return new RecordQueryUnionOnKeyExpressionPlan(serializationContext, recordQueryUnionOnKeyExpressionPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryUnionOnKeyExpressionPlan, RecordQueryUnionOnKeyExpressionPlan> {
        @Override
        public Class<PRecordQueryUnionOnKeyExpressionPlan> getProtoMessageClass() {
            return PRecordQueryUnionOnKeyExpressionPlan.class;
        }

        @Override
        public RecordQueryUnionOnKeyExpressionPlan fromProto(final PlanSerializationContext serializationContext,
                                                             final PRecordQueryUnionOnKeyExpressionPlan recordQueryUnionOnKeyExpressionPlanProto) {
            return RecordQueryUnionOnKeyExpressionPlan.fromProto(serializationContext, recordQueryUnionOnKeyExpressionPlanProto);
        }
    }
}
