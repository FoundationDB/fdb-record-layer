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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Union plan that compares using a {@link KeyExpression}.
 */
@HeuristicPlanner
public class RecordQueryInUnionOnKeyExpressionPlan extends RecordQueryInUnionPlan {
    protected RecordQueryInUnionOnKeyExpressionPlan(final PlanSerializationContext serializationContext,
                                                    final PRecordQueryInUnionOnKeyExpressionPlan recordQueryInUnionOnKeyExpressionPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryInUnionOnKeyExpressionPlanProto.getSuper()));
    }

    public RecordQueryInUnionOnKeyExpressionPlan(final Quantifier.Physical inner,
                                                 final List<? extends InSource> inSources,
                                                 final KeyExpression comparisonKeyExpression,
                                                 final boolean reverse,
                                                 final int maxNumberOfValuesAllowed,
                                                 final Bindings.Internal internal) {
        super(inner,
                inSources,
                new ComparisonKeyFunction.OnKeyExpression(comparisonKeyExpression),
                reverse,
                maxNumberOfValuesAllowed,
                internal);
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
    public RecordQueryInUnionOnKeyExpressionPlan withChildrenReferences(final List<? extends Reference> newChildren) {
        return withChild(Iterables.getOnlyElement(newChildren));
    }

    @Override
    public RecordQueryInUnionOnKeyExpressionPlan translateCorrelations(final TranslationMap translationMap,
                                                                       final boolean shouldSimplifyValues,
                                                                       final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInUnionOnKeyExpressionPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class), getInSources(),
                getComparisonKeyExpression(), reverse, maxNumberOfValuesAllowed, internal);
    }

    @Override
    public RecordQueryInUnionOnKeyExpressionPlan withChild(final Reference childRef) {
        return new RecordQueryInUnionOnKeyExpressionPlan(Quantifier.physical(childRef, inner.getAlias()),
                getInSources(),
                getComparisonKeyExpression(),
                reverse,
                maxNumberOfValuesAllowed,
                internal);
    }

    @Override
    public PRecordQueryInUnionOnKeyExpressionPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryInUnionOnKeyExpressionPlan.newBuilder()
                .setSuper(toRecordQueryInUnionPlanProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setInUnionOnKeyExpressionPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryInUnionOnKeyExpressionPlan fromProto(final PlanSerializationContext serializationContext,
                                                                  final PRecordQueryInUnionOnKeyExpressionPlan recordQueryInUnionOnKeyExpressionPlanProto) {
        return new RecordQueryInUnionOnKeyExpressionPlan(serializationContext, recordQueryInUnionOnKeyExpressionPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryInUnionOnKeyExpressionPlan, RecordQueryInUnionOnKeyExpressionPlan> {
        @Override
        public Class<PRecordQueryInUnionOnKeyExpressionPlan> getProtoMessageClass() {
            return PRecordQueryInUnionOnKeyExpressionPlan.class;
        }

        @Override
        public RecordQueryInUnionOnKeyExpressionPlan fromProto(final PlanSerializationContext serializationContext,
                                                               final PRecordQueryInUnionOnKeyExpressionPlan recordQueryInUnionOnKeyExpressionPlanProto) {
            return RecordQueryInUnionOnKeyExpressionPlan.fromProto(serializationContext, recordQueryInUnionOnKeyExpressionPlanProto);
        }
    }
}
