/*
 * RecordQueryRecursivePlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.RecursiveCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryRecursivePlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that recursively applies a plan to earlier results, starting with a root plan.
 */
@API(API.Status.INTERNAL)
public class RecordQueryRecursivePlan implements RecordQueryPlanWithChildren, RelationalExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Recursive-Plan");

    @Nonnull
    private final Quantifier.Physical rootQuantifier;
    @Nonnull
    private final Quantifier.Physical childQuantifier;
    @Nonnull
    private final Value resultValue;
    private final boolean inheritRecordProperties;

    public RecordQueryRecursivePlan(@Nonnull final Quantifier.Physical rootQuantifier,
                                    @Nonnull final Quantifier.Physical childQuantifier,
                                    @Nonnull final Value resultValue,
                                    final boolean inheritRecordProperties) {
        this.rootQuantifier = rootQuantifier;
        this.childQuantifier = childQuantifier;
        this.resultValue = resultValue;
        this.inheritRecordProperties = inheritRecordProperties;
    }

    @Nonnull
    public Quantifier.Physical getRootQuantifier() {
        return rootQuantifier;
    }

    @Nonnull
    public Quantifier.Physical getChildQuantifier() {
        return childQuantifier;
    }

    public boolean isInheritRecordProperties() {
        return inheritRecordProperties;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {

        final var nestedExecuteProperties = executeProperties.clearSkipAndLimit();
        return RecursiveCursor.create(
                        rootContinuation ->
                                rootQuantifier.getRangesOverPlan().executePlan(store, context, rootContinuation, nestedExecuteProperties),
                        (parentResult, depth, innerContinuation) -> {
                            // TODO: Consider binding depth as well.
                            final CorrelationIdentifier priorId = CorrelationIdentifier.of("prior_" + childQuantifier.getAlias().getId());
                            final EvaluationContext childContext = context.withBinding(priorId, parentResult);
                            return childQuantifier.getRangesOverPlan().executePlan(store, childContext, innerContinuation, nestedExecuteProperties);
                        },
                        null,
                        continuation
                ).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit())
                .map(childResult -> {
                    // TODO: Consider returning depth and is_leaf as well.
                    final EvaluationContext childContext = context.withBinding(childQuantifier.getAlias(), childResult.getValue());
                    final var computed = resultValue.eval(store, childContext);
                    return inheritRecordProperties ? childResult.getValue().withComputed(computed) : QueryResult.ofComputed(computed);
                });
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return ImmutableList.of(rootQuantifier.getRangesOverPlan(), childQuantifier.getRangesOverPlan());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return resultValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RecordQueryRecursivePlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        final Value translatedResultValue = resultValue.translateCorrelations(translationMap);
        return new RecordQueryRecursivePlan(
                translatedQuantifiers.get(0).narrow(Quantifier.Physical.class),
                translatedQuantifiers.get(1).narrow(Quantifier.Physical.class),
                translatedResultValue,
                inheritRecordProperties
        );
    }

    @Override
    public boolean isReverse() {
        return Quantifiers.isReversed(Quantifiers.narrow(Quantifier.Physical.class, getQuantifiers()));
    }

    @Override
    public RecordQueryRecursivePlan strictlySorted(@Nonnull Memoizer memoizer) {
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return semanticEqualsForResults(otherExpression, aliasMap);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getResultValue(), inheritRecordProperties);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
    }

    @Override
    public int getComplexity() {
        return rootQuantifier.getRangesOverPlan().getComplexity() * childQuantifier.getRangesOverPlan().getComplexity();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren(), getResultValue(), inheritRecordProperties);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(rootQuantifier, childQuantifier);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR,
                        ImmutableList.of("RECURSIVE {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(getResultValue().toString()))),
                childGraphs,
                getQuantifiers());
    }

    @Nonnull
    @Override
    public PRecordQueryRecursivePlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryRecursivePlan.newBuilder()
                .setRootQuantifier(rootQuantifier.toProto(serializationContext))
                .setChildQuantifier(childQuantifier.toProto(serializationContext))
                .setResultValue(resultValue.toValueProto(serializationContext))
                .setInheritRecordProperties(inheritRecordProperties)
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setRecursivePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryRecursivePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordQueryRecursivePlan recordQueryRecursivePlanProto) {
        return new RecordQueryRecursivePlan(
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryRecursivePlanProto.getRootQuantifier())),
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryRecursivePlanProto.getChildQuantifier())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryRecursivePlanProto.getResultValue())),
                recordQueryRecursivePlanProto.getInheritRecordProperties()
        );
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryRecursivePlan, RecordQueryRecursivePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryRecursivePlan> getProtoMessageClass() {
            return PRecordQueryRecursivePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryRecursivePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PRecordQueryRecursivePlan recordQueryRecursivePlanProto) {
            return RecordQueryRecursivePlan.fromProto(serializationContext, recordQueryRecursivePlanProto);
        }
    }
}
