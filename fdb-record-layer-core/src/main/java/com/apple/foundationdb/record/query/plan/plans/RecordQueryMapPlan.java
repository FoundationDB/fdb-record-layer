/*
 * RecordQueryMapPlan.java
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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryMapPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.WithIndentationsExplainFormatter;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that applies the values it contains over the incoming ones. In a sense, this is similar to the {@code Stream.map()}
 * method: Mapping one {@link Value} to another.
 */
@API(API.Status.INTERNAL)
public class RecordQueryMapPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild, ExplainPlannerGraphRewritable, InternalPlannerGraphRewritable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Map-Plan");

    private final Quantifier.Physical inner;
    private final Value resultValue;

    public RecordQueryMapPlan(final Quantifier.Physical inner,
                              final Value resultValue) {
        this.inner = inner;
        this.resultValue = resultValue;
    }

    @SuppressWarnings("resource")
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store,
                                                                     final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     final ExecuteProperties executeProperties) {
        return getChild().executePlan(store, context, continuation, executeProperties)
                .map(innerResult -> {
                    final EvaluationContext nestedContext = context.withBinding(Bindings.Internal.CORRELATION, inner.getAlias(), innerResult);
                    // Apply (map) each value to the incoming record
                    return innerResult.withComputed(resultValue.eval(store, nestedContext));
                });
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Override
    public RecordQueryPlanWithChild withChild(final Reference childRef) {
        return new RecordQueryMapPlan(Quantifier.physical(childRef, inner.getAlias()), resultValue);
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return resultValue.getCorrelatedTo();
    }

    @Override
    public RecordQueryMapPlan translateCorrelations(final TranslationMap translationMap,
                                                    final boolean shouldSimplifyValues,
                                                    final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        final Value translatedResultValue =
                resultValue.translateCorrelations(translationMap, shouldSimplifyValues);
        return new RecordQueryMapPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                translatedResultValue);
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public boolean isStrictlySorted() {
        return false;
    }

    @Override
    public RecordQueryMapPlan strictlySorted(FinalMemoizer memoizer) {
        return this;
    }

    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap aliasMap) {
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return getChild().getComplexity();
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild(), getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    public Quantifier.Physical getInner() {
        return inner;
    }

    @Override
    public PlannerGraph rewriteExplainPlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return rewritePlannerGraph(childGraphs);
    }

    @Override
    public PlannerGraph rewriteInternalPlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        final var explainFormatter =
                WithIndentationsExplainFormatter.forDot(1);

        final var mapString =
                "MAP " + getResultValue().explain()
                        .getExplainTokens()
                        .render(explainFormatter);

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(
                        this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of(mapString),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("MAP {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(getResultValue().toString()))),
                childGraphs);
    }

    @Override
    public PRecordQueryMapPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryMapPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setResultValue(resultValue.toValueProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setMapPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryMapPlan fromProto(final PlanSerializationContext serializationContext,
                                               final PRecordQueryMapPlan mapPlanProto) {
        return new RecordQueryMapPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(mapPlanProto.getInner())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(mapPlanProto.getResultValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryMapPlan, RecordQueryMapPlan> {
        @Override
        public Class<PRecordQueryMapPlan> getProtoMessageClass() {
            return PRecordQueryMapPlan.class;
        }

        @Override
        public RecordQueryMapPlan fromProto(final PlanSerializationContext serializationContext,
                                            final PRecordQueryMapPlan recordQueryMapPlanProto) {
            return RecordQueryMapPlan.fromProto(serializationContext, recordQueryMapPlanProto);
        }
    }
}
