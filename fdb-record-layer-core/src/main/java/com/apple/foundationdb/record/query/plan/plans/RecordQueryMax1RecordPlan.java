/*
 * RecordQueryMax1RecordPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreCardinalityViolationException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PRecordQueryMax1RecordPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A query plan that flows the records of its inner unchanged, but raises a
 * {@link RecordCoreCardinalityViolationException} if there is more than one of them. It is the enforcement half of a
 * SQL scalar subquery, where returning more than one row is a cardinality violation.
 */
@API(API.Status.INTERNAL)
public class RecordQueryMax1RecordPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Max-1-Record-Plan");

    @Nonnull
    private final Quantifier.Physical inner;

    public RecordQueryMax1RecordPlan(@Nonnull final Quantifier.Physical inner) {
        this.inner = inner;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        // Apply the skip and limit to the records of this plan rather than to those of the inner. Otherwise, a limit
        // of 1 would hide a potential second record.
        return RecordCursor.fromFuture(store.getExecutor(),
                        () -> max1Record(store, context, executeProperties.clearSkipAndLimit()), continuation)
                .filter(Objects::nonNull)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    /**
     * Draws up to two records from the inner and returns the first one, or {@code null} if the inner is empty. Both
     * are drawn up front rather than checked lazily as records flow past, since a consumer of this plan, such as the
     * {@link RecordQueryFirstOrDefaultPlan} above it, might stop after the first record and never ask for the second
     * one. If there are two records, this method raises a {@link RecordCoreCardinalityViolationException}.
     *
     * <p>In case the inner stops early on an out-of-band limit, such as a scan limit, before producing two records,
     * this method raises a {@link RecordCoreException}, since it is then unknown whether the inner is empty or has a
     * second record. The cursor of this plan cannot resume the inner across a continuation, as it would have to hold
     * back the first record. The same limitation also exists in {@link RecordQueryFirstOrDefaultPlan}; see
     * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/3220">Issue #3220</a>.
     */
    @SuppressWarnings({"resource", "PMD.CloseResource"}) // closed once the returned future completes
    @Nonnull
    private <M extends Message> CompletableFuture<QueryResult> max1Record(
            @Nonnull final FDBRecordStoreBase<M> store,
            @Nonnull final EvaluationContext context,
            @Nonnull final ExecuteProperties innerExecuteProperties) {
        final RecordCursor<QueryResult> innerCursor =
                getChild().executePlan(store, context, null, innerExecuteProperties).limitRowsTo(2);
        final AtomicReference<RecordCursorResult<QueryResult>> terminatingResultRef = new AtomicReference<>();
        return innerCursor.asList(terminatingResultRef)
                .thenApply(results -> {
                    if (results.size() > 1) {
                        throw new RecordCoreCardinalityViolationException(
                                "plan required to return at most one record returned more than one record")
                                .addLogInfo(LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                    }
                    final RecordCursor.NoNextReason noNextReason = terminatingResultRef.get().getNoNextReason();
                    if (noNextReason.isOutOfBand()) {
                        throw new RecordCoreException(
                                "plan required to return at most one record stopped early on an out-of-band limit")
                                .addLogInfo(LogMessageKeys.NO_NEXT_REASON, noNextReason)
                                .addLogInfo(LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                    }
                    return results.isEmpty() ? null : results.get(0);
                })
                // Close the inner cursor once done with it, as closing the cursor of this plan does not reach it.
                .whenComplete((ignore, err) -> innerCursor.close());
    }

    @Nonnull
    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryMax1RecordPlan(Quantifier.physical(childRef, inner.getAlias()));
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryMax1RecordPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                        final boolean shouldSimplifyValues,
                                                        @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryMax1RecordPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class));
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public boolean isStrictlySorted() {
        return false;
    }

    @Nonnull
    @Override
    public RecordQueryMax1RecordPlan strictlySorted(@Nonnull final FinalMemoizer memoizer) {
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Nonnull
    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
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
    public int computeHashCodeWithoutChildren() {
        return BASE_HASH.hashCode();
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return getChild().getComplexity();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("MAX 1 RECORD")),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryMax1RecordPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryMax1RecordPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setMax1RecordPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryMax1RecordPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordQueryMax1RecordPlan recordQueryMax1RecordPlanProto) {
        return new RecordQueryMax1RecordPlan(
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryMax1RecordPlanProto.getInner())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryMax1RecordPlan, RecordQueryMax1RecordPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryMax1RecordPlan> getProtoMessageClass() {
            return PRecordQueryMax1RecordPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryMax1RecordPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQueryMax1RecordPlan recordQueryMax1RecordPlanProto) {
            return RecordQueryMax1RecordPlan.fromProto(serializationContext, recordQueryMax1RecordPlanProto);
        }
    }
}
