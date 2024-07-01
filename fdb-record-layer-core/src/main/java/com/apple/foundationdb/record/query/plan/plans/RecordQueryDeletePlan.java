/*
 * RecordQueryDeletePlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryDeletePlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A query plan that deletes records. The record that is to be deleted is identified through the primary key of the
 * {@link QueryResult} that represents the in record. Note that
 * {@link com.apple.foundationdb.record.query.plan.cascades.rules.ImplementDeleteRule} only allows implementation of
 * a {@link com.apple.foundationdb.record.query.plan.cascades.expressions.DeleteExpression} if the plan partition
 * of the child guarantees
 * {@link com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty#STORED_RECORD}.
 * Not that we hold on to a target record type in this plan operator only for debugging purposes at the moment.
 */
@API(API.Status.INTERNAL)
public class RecordQueryDeletePlan implements RecordQueryPlanWithChild, PlannerGraphRewritable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Delete-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryDeletePlan.class);

    @Nonnull
    private final Quantifier.Physical inner;

    @Nonnull
    private final Supplier<Value> resultValueSupplier;

    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;

    protected RecordQueryDeletePlan(@Nonnull final Quantifier.Physical inner) {
        this.inner = inner;
        this.resultValueSupplier = Suppliers.memoize(inner::getFlowedObjectValue);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        if (executeProperties.isDryRun()) {
            return RecordCursor.flatMapPipelined(
                    outerContinuation -> getInnerPlan().executePlan(store, context, outerContinuation, executeProperties.clearSkipAndLimit()),
                    (outerQueryResult, innerContinuation) ->
                            RecordCursor.fromFuture(store.dryRunDeleteRecordAsync(Verify.verifyNotNull(outerQueryResult.getPrimaryKey())))
                                    .filter(isDeleted -> isDeleted)
                                    .map(ignored -> outerQueryResult),
                    continuation, store.getPipelineSize(PipelineOperation.DELETE));
        } else {
            return RecordCursor.flatMapPipelined(
                    outerContinuation -> getInnerPlan().executePlan(store, context, outerContinuation, executeProperties.clearSkipAndLimit()),
                    (outerQueryResult, innerContinuation) ->
                            RecordCursor.fromFuture(store.deleteRecordAsync(Verify.verifyNotNull(outerQueryResult.getPrimaryKey())))
                                    .filter(isDeleted -> isDeleted)
                                    .map(ignored -> outerQueryResult),
                    continuation, store.getPipelineSize(PipelineOperation.DELETE));
        }
    }

    @Override
    public boolean isReverse() {
        return getInnerPlan().isReverse();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(this.inner);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValueSupplier.get();
    }

    @Nonnull
    @Override
    public RecordQueryDeletePlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                       @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryDeletePlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class));
    }

    @Nonnull
    @Override
    public RecordQueryDeletePlan withChild(@Nonnull final Reference childRef) {
        return new RecordQueryDeletePlan(Quantifier.physical(childRef));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        return getClass() == other.getClass();
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
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getInnerPlan());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // TODO timer.increment(FDBStoreTimer.Counts.PLAN_TYPE_FILTER);
        getInnerPlan().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInnerPlan().getComplexity();
    }

    /**
     * Rewrite the planner graph for better visualization.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models the filter as a node that uses the expression attribute
     *         to depict the record types this operator filters.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                                getResultType(),
                                ImmutableList.of()),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("DELETE"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    @Nonnull
    @Override
    public PRecordQueryDeletePlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryDeletePlan.newBuilder().setInner(inner.toProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setDeletePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryDeletePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PRecordQueryDeletePlan recordQueryDeletePlanProto) {
        return new RecordQueryDeletePlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryDeletePlanProto.getInner())));
    }

    /**
     * Factory method to create a {@link RecordQueryInsertPlan}.
     * @param inner an input value to transform
     * @return a newly created {@link RecordQueryInsertPlan}
     */
    @Nonnull
    public static RecordQueryDeletePlan deletePlan(@Nonnull final Quantifier.Physical inner) {
        return new RecordQueryDeletePlan(inner);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryDeletePlan, RecordQueryDeletePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryDeletePlan> getProtoMessageClass() {
            return PRecordQueryDeletePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryDeletePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PRecordQueryDeletePlan recordQueryDeletePlanProto) {
            return RecordQueryDeletePlan.fromProto(serializationContext, recordQueryDeletePlanProto);
        }
    }
}
