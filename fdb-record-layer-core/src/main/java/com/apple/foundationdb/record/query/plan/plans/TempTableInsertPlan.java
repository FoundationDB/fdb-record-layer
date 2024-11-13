/*
 * TempTableInsertPlan.java
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
import com.apple.foundationdb.record.cursors.TempTableInsertCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PTempTableInsertPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A query plan that inserts records into a temporary in-memory buffer {@link TempTable}.
 */
@API(API.Status.INTERNAL)
public class TempTableInsertPlan extends RecordQueryAbstractDataModificationPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Temp-Table-Insert-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(TempTableInsertPlan.class);

    @Nonnull
    private final Value tempTableReferenceValue;

    private final boolean isOwningTempTable;

    protected TempTableInsertPlan(@Nonnull final PlanSerializationContext serializationContext,
                                  @Nonnull final PTempTableInsertPlan tempTableInsertPlanProto) {
        super(serializationContext, Objects.requireNonNull(tempTableInsertPlanProto.getSuper()));
        this.tempTableReferenceValue = Value.fromValueProto(serializationContext, tempTableInsertPlanProto.getTempTableReferenceValue());
        this.isOwningTempTable = tempTableInsertPlanProto.getIsOwningTempTable();
    }

    private TempTableInsertPlan(@Nonnull final Quantifier.Physical inner,
                                @Nonnull final Value computationValue,
                                @Nonnull final Value tempTableReferenceValue,
                                boolean isOwningTempTable) {
        super(inner, null, (Type.Record)((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType(), null,
                null, computationValue, currentModifiedRecordAlias());
        this.tempTableReferenceValue = tempTableReferenceValue;
        this.isOwningTempTable = isOwningTempTable;
    }

    @Override
    public PipelineOperation getPipelineOperation() {
        return PipelineOperation.INSERT;
    }

    @Nonnull
    @Override
    @SuppressWarnings({"PMD.CloseResource", "resource", "unchecked"})
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final RecordCursor<QueryResult> childCursor;
        if (isOwningTempTable) {
            final var typeDescriptor = getInnerTypeDescriptor(context);
            childCursor = TempTableInsertCursor.from(continuation,
                    proto -> {
                        final var tempTable = Objects.requireNonNull((TempTable<QueryResult>)getTempTableReferenceValue().eval(store, context));
                        if (proto != null) {
                            TempTable.from(proto, typeDescriptor).getReadBuffer().forEach(tempTable::add);
                        }
                        return tempTable;
                    },
                    childContinuation -> getInnerPlan().executePlan(store, context, childContinuation, executeProperties.clearSkipAndLimit()));
            childCursor.map(queryResult -> Pair.of(queryResult, (M)Preconditions.checkNotNull(queryResult.getMessage())))
                    .mapPipelined(pair -> {
                        final var queryResult = pair.getLeft();
                        final var nestedContext = context.childBuilder()
                                .setBinding(getInner().getAlias(), pair.getLeft()) // pre-mutation
                                .setBinding(getCurrentModifiedRecordAlias(), queryResult.getMessage()) // post-mutation
                                .build(context.getTypeRepository());
                        final var result = getComputationValue().eval(store, nestedContext);
                        return CompletableFuture.completedFuture(QueryResult.ofComputed(result, queryResult.getPrimaryKey()));
                    }, store.getPipelineSize(getPipelineOperation()));

        } else {
            childCursor = getInnerPlan().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit());
            childCursor.map(queryResult -> Pair.of(queryResult, (M)Preconditions.checkNotNull(queryResult.getMessage())))
                    .mapPipelined(pair -> saveRecordAsync(store, context, pair.getRight(), executeProperties.isDryRun())
                                    .thenApply(queryResult -> {
                                        final var nestedContext = context.childBuilder()
                                                .setBinding(getInner().getAlias(), pair.getLeft()) // pre-mutation
                                                .setBinding(getCurrentModifiedRecordAlias(), queryResult.getMessage()) // post-mutation
                                                .build(context.getTypeRepository());
                                        final var result = getComputationValue().eval(store, nestedContext);
                                        return QueryResult.ofComputed(result, queryResult.getPrimaryKey());
                                    }),
                            store.getPipelineSize(getPipelineOperation()));
        }
        return childCursor;
    }

    @Nullable
    private Descriptors.Descriptor getInnerTypeDescriptor(@Nonnull final EvaluationContext evaluationContext) {
        final Descriptors.Descriptor typeDescriptor;
        if (tempTableReferenceValue.getResultType().isRelation() && ((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType().isRecord()) {
            final var type = (Type.Record)((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType();
            final var builder = TypeRepository.newBuilder();
            type.defineProtoType(builder);
            typeDescriptor = builder.build().getMessageDescriptor(type);
        } else {
            typeDescriptor = null;
        }
        return typeDescriptor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nonnull <M extends Message> CompletableFuture<QueryResult> saveRecordAsync(@Nonnull final FDBRecordStoreBase<M> store,
                                                                                       @Nonnull final EvaluationContext context,
                                                                                       @Nonnull final M message,
                                                                                       boolean isDryRun) {
        // dry run is ignored since inserting into a table queue has no storage side effects.
        final var queryResult = QueryResult.ofComputed(message);
        final var tempTable = Objects.requireNonNull((TempTable<QueryResult>)getTempTableReferenceValue().eval(store, context));
        tempTable.add(queryResult);
        return CompletableFuture.completedFuture(queryResult);
    }

    @Nonnull
    @Override
    public TempTableInsertPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new TempTableInsertPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getComputationValue().translateCorrelations(translationMap),
                getTempTableReferenceValue().translateCorrelations(translationMap),
                isOwningTempTable);
    }

    @Nonnull
    @Override
    public TempTableInsertPlan withChild(@Nonnull final Reference childRef) {
        return new TempTableInsertPlan(Quantifier.physical(childRef),
                getComputationValue(),
                getTempTableReferenceValue(),
                isOwningTempTable);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), getTempTableReferenceValue(),
                super.hashCodeWithoutChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getTempTableReferenceValue(), super.planHash(mode));
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    /**
     * Rewrite the planner graph for better visualization.
     *
     * @param childGraphs planner graphs of children expression that already have been computed
     *
     * @return the rewritten planner graph that models the filter as a node that uses the expression attribute
     * to depict the record types this operator filters.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {

        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.TemporaryDataNodeWithInfo(getTargetType(), ImmutableList.of(getTempTableReferenceValue().toString())),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("TempTableInsert"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    @Nonnull
    @Override
    public PTempTableInsertPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PTempTableInsertPlan.newBuilder()
                .setSuper(toRecordQueryAbstractModificationPlanProto(serializationContext))
                .setTempTableReferenceValue(getTempTableReferenceValue().toValueProto(serializationContext))
                .setIsOwningTempTable(isOwningTempTable)
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setTempTableInsertPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static TempTableInsertPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PTempTableInsertPlan tempTableInsertPlanProto) {
        return new TempTableInsertPlan(serializationContext, tempTableInsertPlanProto);
    }

    /**
     * Factory method to create a {@link TempTableInsertPlan}.
     *
     * @param inner an input value to transform
     * @param computationValue a value to be computed based on the {@code inner} and
     * {@link RecordQueryAbstractDataModificationPlan#currentModifiedRecordAlias()}
     * @param tempTableReferenceValue The table queue identifier to insert into.
     *
     * @return a newly created {@link TempTableInsertPlan}
     */
    @Nonnull
    public static TempTableInsertPlan insertPlan(@Nonnull final Quantifier.Physical inner,
                                                 @Nonnull final Value computationValue,
                                                 @Nonnull final Value tempTableReferenceValue) {
        return new TempTableInsertPlan(inner, computationValue, tempTableReferenceValue, true);
    }

    @Nonnull
    public Value getTempTableReferenceValue() {
        return tempTableReferenceValue;
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PTempTableInsertPlan, TempTableInsertPlan> {
        @Nonnull
        @Override
        public Class<PTempTableInsertPlan> getProtoMessageClass() {
            return PTempTableInsertPlan.class;
        }

        @Nonnull
        @Override
        public TempTableInsertPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PTempTableInsertPlan tempTableInsertPlanProto) {
            return TempTableInsertPlan.fromProto(serializationContext, tempTableInsertPlanProto);
        }
    }
}
