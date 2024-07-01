/*
 * RecordQueryFlatMapPlan.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryFlatMapPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
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
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A query plan that applies the values it contains over the incoming ones. In a sense, this is similar to the {@code Stream.map()}
 * method: Mapping one {@link Value} to another.
 */
@API(API.Status.INTERNAL)
public class RecordQueryFlatMapPlan implements RecordQueryPlanWithChildren, RelationalExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Flat-Map-Plan");

    @Nonnull
    private final Quantifier.Physical outerQuantifier;
    @Nonnull
    private final Quantifier.Physical innerQuantifier;
    @Nonnull
    private final Value resultValue;
    private final boolean inheritOuterRecordProperties;
    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;
    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlatedToWithoutChildrenSupplier;

    public RecordQueryFlatMapPlan(@Nonnull final Quantifier.Physical outerQuantifier,
                                  @Nonnull final Quantifier.Physical innerQuantifier,
                                  @Nonnull final Value resultValue,
                                  final boolean inheritOuterRecordProperties) {
        this.outerQuantifier = outerQuantifier;
        this.innerQuantifier = innerQuantifier;
        this.resultValue = resultValue;
        this.inheritOuterRecordProperties = inheritOuterRecordProperties;
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
        this.correlatedToWithoutChildrenSupplier = Suppliers.memoize(this::computeCorrelatedToWithoutChildren);
    }

    @Nonnull
    public Quantifier.Physical getOuterQuantifier() {
        return outerQuantifier;
    }

    @Nonnull
    public Quantifier.Physical getInnerQuantifier() {
        return innerQuantifier;
    }

    public boolean isInheritOuterRecordProperties() {
        return inheritOuterRecordProperties;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {

        final var nestedExecuteProperties = executeProperties.clearSkipAndLimit();
        return RecordCursor.flatMapPipelined(
                outerContinuation ->
                        outerQuantifier.getRangesOverPlan().executePlan(store, context, outerContinuation, nestedExecuteProperties),
                (outerResult, innerContinuation) -> {
                    final EvaluationContext fromOuterContext = context.withBinding(outerQuantifier.getAlias(), outerResult);

                    return innerQuantifier.getRangesOverPlan().executePlan(store, fromOuterContext, innerContinuation, nestedExecuteProperties)
                            .map(innerResult -> {
                                final EvaluationContext nestedContext =
                                        fromOuterContext.withBinding(innerQuantifier.getAlias(), innerResult);
                                final var computed = resultValue.eval(store, nestedContext);
                                return inheritOuterRecordProperties
                                       ? outerResult.withComputed(computed)
                                       : QueryResult.ofComputed(computed);
                            });
                },
                continuation,
                5).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
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
        return ImmutableList.of(outerQuantifier.getRangesOverPlan(), innerQuantifier.getRangesOverPlan());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return correlatedToWithoutChildrenSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return resultValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RecordQueryFlatMapPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        final Value translatedResultValue = resultValue.translateCorrelations(translationMap);
        return new RecordQueryFlatMapPlan(translatedQuantifiers.get(0).narrow(Quantifier.Physical.class),
                translatedQuantifiers.get(1).narrow(Quantifier.Physical.class),
                translatedResultValue,
                inheritOuterRecordProperties);
    }

    @Override
    public boolean isReverse() {
        return Quantifiers.isReversed(Quantifiers.narrow(Quantifier.Physical.class, getQuantifiers()));
    }

    @Override
    public RecordQueryFlatMapPlan strictlySorted(@Nonnull Memoizer memoizer) {
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
        return hashCodeWithoutChildrenSupplier.get();
    }

    private int computeHashCodeWithoutChildren() {
        return Objects.hash(getResultValue(), inheritOuterRecordProperties);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return outerQuantifier.getRangesOverPlan().getComplexity() * innerQuantifier.getRangesOverPlan().getComplexity();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren(), inheritOuterRecordProperties, getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(outerQuantifier, innerQuantifier);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR,
                        ImmutableList.of("FLATMAP {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(getResultValue().toString()))),
                childGraphs,
                getQuantifiers());
    }

    @Nonnull
    @Override
    public PRecordQueryFlatMapPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryFlatMapPlan.newBuilder()
                .setOuterQuantifier(outerQuantifier.toProto(serializationContext))
                .setInnerQuantifier(innerQuantifier.toProto(serializationContext))
                .setResultValue(resultValue.toValueProto(serializationContext))
                .setInheritOuterRecordProperties(inheritOuterRecordProperties)
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setFlatMapPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryFlatMapPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordQueryFlatMapPlan recordQueryFlatMapPlanProto) {
        Verify.verifyNotNull(recordQueryFlatMapPlanProto.hasInheritOuterRecordProperties());
        return new RecordQueryFlatMapPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryFlatMapPlanProto.getOuterQuantifier())),
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryFlatMapPlanProto.getInnerQuantifier())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryFlatMapPlanProto.getResultValue())),
                recordQueryFlatMapPlanProto.getInheritOuterRecordProperties());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryFlatMapPlan, RecordQueryFlatMapPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryFlatMapPlan> getProtoMessageClass() {
            return PRecordQueryFlatMapPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryFlatMapPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQueryFlatMapPlan recordQueryFlatMapPlanProto) {
            return RecordQueryFlatMapPlan.fromProto(serializationContext, recordQueryFlatMapPlanProto);
        }
    }
}
