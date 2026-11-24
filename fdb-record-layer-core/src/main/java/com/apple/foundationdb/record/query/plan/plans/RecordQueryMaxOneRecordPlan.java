/*
 * RecordQueryMaxOneRecordPlan.java
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
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreCardinalityViolationException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.cursors.FutureResultCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PQueryResult;
import com.apple.foundationdb.record.planprotos.PRecordQueryMaxOneRecordPlan;
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
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
 * {@link RecordCoreCardinalityViolationException} if there is more than one record. This is useful to enforce the
 * cardinality constraint of a SQL scalar subquery at runtime (in case it cannot be proven at planning time).
 */
@API(API.Status.INTERNAL)
public class RecordQueryMaxOneRecordPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Max-1-Record-Plan");

    @Nonnull
    private final Quantifier.Physical inner;

    public RecordQueryMaxOneRecordPlan(@Nonnull final Quantifier.Physical inner) {
        this.inner = inner;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final RecordCursorProto.MaxOneRecordCursorContinuation parsedContinuation = parseContinuation(continuation);
        if (parsedContinuation != null && parsedContinuation.getDone()) {
            return RecordCursor.empty(store.getExecutor());
        }
        // Note: It’s important that we don’t limit the inner plan, as that could hide a potential second record.
        final ExecuteProperties innerExecuteProperties = executeProperties.clearSkipAndLimit();
        return new FutureResultCursor<>(store.getExecutor(),
                        maxOneRecord(store, context, innerExecuteProperties, parsedContinuation))
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    /**
     * Draws up to two records from the inner and returns the first one, or no record if the inner is empty. Both are
     * drawn up front rather than checked lazily as records flow past, since a consumer of this plan (typically a
     * {@link RecordQueryFirstOrDefaultPlan}) might stop after the first record and never ask for the second one.
     * If there are two records, this method raises a {@link RecordCoreCardinalityViolationException}.
     *
     * <p>In case the inner stops early on an out-of-band completion, such as a scan limit, before producing two
     * records, it is unknown whether the inner is empty or has a second record. This method then returns a result
     * without a value, carrying the reason the inner stopped for and a continuation to pick up from there. If the
     * inner has produced a record by then, the continuation holds that record back, serialized as a
     * {@link PQueryResult}. Such a record is returned with its value only, i.e., it does not carry the stored record
     * or the primary key that it might have carried had it been returned without being held back.
     */
    @SuppressWarnings({"resource", "PMD.CloseResource"}) // closed once the returned future completes
    @Nonnull
    private <M extends Message> CompletableFuture<RecordCursorResult<QueryResult>> maxOneRecord(
            @Nonnull final FDBRecordStoreBase<M> store,
            @Nonnull final EvaluationContext context,
            @Nonnull final ExecuteProperties innerExecuteProperties,
            @Nullable final RecordCursorProto.MaxOneRecordCursorContinuation continuation) {
        @Nullable final QueryResult heldResult;
        @Nullable final byte[] innerContinuation;
        if (continuation == null) {
            heldResult = null;
            innerContinuation = null;
        } else {
            heldResult = continuation.hasHeldResult()
                         ? QueryResult.from(getResultDescriptor(context), continuation.getHeldResult())
                         : null;
            innerContinuation = continuation.hasInnerContinuation() ? continuation.getInnerContinuation().toByteArray() : null;
        }
        // Having drawn one record already, a single further record is enough to know that there are two.
        final RecordCursor<QueryResult> innerCursor =
                getChild().executePlan(store, context, innerContinuation, innerExecuteProperties)
                        .limitRowsTo(heldResult == null ? 2 : 1);
        final AtomicReference<RecordCursorResult<QueryResult>> terminatingResultRef = new AtomicReference<>();
        return innerCursor
                .asList(terminatingResultRef)
                .thenApply(drawnResults -> {
                    final List<QueryResult> results = heldResult == null
                                                      ? drawnResults
                                                      : ImmutableList.<QueryResult>builder().add(heldResult).addAll(drawnResults).build();
                    if (results.size() > 1) {
                        throw new RecordCoreCardinalityViolationException(
                                "A plan required to return at most one record returned more than one record")
                                .addLogInfo(LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
                    }
                    final RecordCursorResult<QueryResult> terminatingResult = terminatingResultRef.get();
                    if (!terminatingResult.getNoNextReason().isSourceExhausted()) {
                        return RecordCursorResult.<QueryResult>withoutNextValue(
                                resumeContinuation(context, terminatingResult, results), terminatingResult.getNoNextReason());
                    }
                    if (results.isEmpty()) {
                        return RecordCursorResult.<QueryResult>exhausted();
                    }
                    return RecordCursorResult.withNextValue(results.get(0), doneContinuation());
                })
                // Close the inner cursor once done with it. (The `FutureResultCursor` of this plan isn’t aware of the
                // inner cursor and would otherwise leak it.)
                .whenComplete((ignore, err) -> innerCursor.close());
    }

    @Nonnull
    private RecordCursorContinuation resumeContinuation(@Nonnull final EvaluationContext context,
                                                        @Nonnull final RecordCursorResult<QueryResult> terminatingResult,
                                                        @Nonnull final List<QueryResult> results) {
        // Note: Setting `done` explicitly keeps the serialized continuation from being empty.
        final RecordCursorProto.MaxOneRecordCursorContinuation.Builder builder =
                RecordCursorProto.MaxOneRecordCursorContinuation.newBuilder().setDone(false);
        final ByteString innerContinuation = terminatingResult.getContinuation().toByteString();
        if (!innerContinuation.isEmpty()) {
            builder.setInnerContinuation(innerContinuation);
        }
        if (!results.isEmpty()) {
            final QueryResult heldResult = results.get(0);
            if (heldResult.getDatum() instanceof Message && getResultDescriptor(context) == null) {
                // Fail here rather than when resuming, as that is where the record would be deserialized.
                throw new RecordCoreException(
                        "A plan required to return at most one record stopped early on an out-of-band completion, "
                        + "and cannot hold back a record of a type that is unknown to the type repository")
                        .addLogInfo(LogMessageKeys.NO_NEXT_REASON, terminatingResult.getNoNextReason())
                        .addLogInfo(LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
            try {
                builder.setHeldResult(heldResult.toProto());
            } catch (RecordCoreException ex) {
                throw new RecordCoreException(
                        "A plan required to return at most one record stopped early on an out-of-band completion, "
                        + "and cannot hold back a record that cannot be serialized", ex)
                        .addLogInfo(LogMessageKeys.NO_NEXT_REASON, terminatingResult.getNoNextReason())
                        .addLogInfo(LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        }
        return ByteArrayContinuation.fromNullable(builder.build().toByteArray());
    }

    @Nonnull
    private static RecordCursorContinuation doneContinuation() {
        return ByteArrayContinuation.fromNullable(
                RecordCursorProto.MaxOneRecordCursorContinuation.newBuilder().setDone(true).build().toByteArray());
    }

    @Nullable
    private static RecordCursorProto.MaxOneRecordCursorContinuation parseContinuation(@Nullable final byte[] continuation) {
        if (continuation == null) {
            return null;
        }
        try {
            return RecordCursorProto.MaxOneRecordCursorContinuation.parseFrom(continuation);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(continuation));
        }
    }

    /**
     * Returns the descriptor needed to deserialize a record that has been held back, or {@code null} if the records
     * flowing through this plan are not messages.
     */
    @Nullable
    private Descriptors.Descriptor getResultDescriptor(@Nonnull final EvaluationContext context) {
        final Type resultType = Objects.requireNonNull(getResultValue().getResultType());
        return resultType.isRecord() ? context.getTypeRepository().getMessageDescriptor(resultType) : null;
    }

    @Nonnull
    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryMaxOneRecordPlan(Quantifier.physical(childRef, inner.getAlias()));
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
    public RecordQueryMaxOneRecordPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                        final boolean shouldSimplifyValues,
                                                        @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryMaxOneRecordPlan(
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
    public RecordQueryMaxOneRecordPlan strictlySorted(@Nonnull final FinalMemoizer memoizer) {
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
    public PRecordQueryMaxOneRecordPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryMaxOneRecordPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setMaxOneRecordPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryMaxOneRecordPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordQueryMaxOneRecordPlan recordQueryMaxOneRecordPlanProto) {
        return new RecordQueryMaxOneRecordPlan(
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryMaxOneRecordPlanProto.getInner())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryMaxOneRecordPlan, RecordQueryMaxOneRecordPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryMaxOneRecordPlan> getProtoMessageClass() {
            return PRecordQueryMaxOneRecordPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryMaxOneRecordPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQueryMaxOneRecordPlan recordQueryMaxOneRecordPlanProto) {
            return RecordQueryMaxOneRecordPlan.fromProto(serializationContext, recordQueryMaxOneRecordPlanProto);
        }
    }
}
