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

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Union plan that compares using a {@link KeyExpression}.
 */
public class RecordQueryInUnionOnKeyExpressionPlan extends RecordQueryInUnionPlan {
    protected RecordQueryInUnionOnKeyExpressionPlan(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PRecordQueryInUnionOnKeyExpressionPlan recordQueryInUnionOnKeyExpressionPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryInUnionOnKeyExpressionPlanProto.getSuper()));
    }

    public RecordQueryInUnionOnKeyExpressionPlan(@Nonnull final Quantifier.Physical inner,
                                                 @Nonnull final List<? extends InSource> inSources,
                                                 @Nonnull final KeyExpression comparisonKeyExpression,
                                                 final boolean reverse,
                                                 final int maxNumberOfValuesAllowed,
                                                 @Nonnull final Bindings.Internal internal) {
        super(inner,
                inSources,
                new ComparisonKeyFunction.OnKeyExpression(comparisonKeyExpression),
                reverse,
                maxNumberOfValuesAllowed,
                internal);
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
    public RecordQueryInUnionOnKeyExpressionPlan withChildrenReferences(@Nonnull final List<? extends Reference> newChildren) {
        return withChild(Iterables.getOnlyElement(newChildren));
    }

    @Nonnull
    @Override
    public RecordQueryInUnionOnKeyExpressionPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInUnionOnKeyExpressionPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getInSources(),
                getComparisonKeyExpression(),
                reverse,
                maxNumberOfValuesAllowed,
                internal);
    }

    @Nonnull
    @Override
    public RecordQueryInUnionOnKeyExpressionPlan withChild(@Nonnull final Reference childRef) {
        return new RecordQueryInUnionOnKeyExpressionPlan(Quantifier.physical(childRef),
                getInSources(),
                getComparisonKeyExpression(),
                reverse,
                maxNumberOfValuesAllowed,
                internal);
    }

    @Nonnull
    @Override
    public PRecordQueryInUnionOnKeyExpressionPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryInUnionOnKeyExpressionPlan.newBuilder()
                .setSuper(toRecordQueryInUnionPlanProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setInUnionOnKeyExpressionPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryInUnionOnKeyExpressionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                  @Nonnull final PRecordQueryInUnionOnKeyExpressionPlan recordQueryInUnionOnKeyExpressionPlanProto) {
        return new RecordQueryInUnionOnKeyExpressionPlan(serializationContext, recordQueryInUnionOnKeyExpressionPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryInUnionOnKeyExpressionPlan, RecordQueryInUnionOnKeyExpressionPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryInUnionOnKeyExpressionPlan> getProtoMessageClass() {
            return PRecordQueryInUnionOnKeyExpressionPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryInUnionOnKeyExpressionPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                               @Nonnull final PRecordQueryInUnionOnKeyExpressionPlan recordQueryInUnionOnKeyExpressionPlanProto) {
            return RecordQueryInUnionOnKeyExpressionPlan.fromProto(serializationContext, recordQueryInUnionOnKeyExpressionPlanProto);
        }
    }
}
