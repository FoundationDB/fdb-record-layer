/*
 * TqInsertPlan.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PTqInsertPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.TableQueue;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
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
 * A query plan that inserts records into a temporary in-memory buffer {@link TableQueue}.
 */
@API(API.Status.INTERNAL)
public class TqInsertPlan extends RecordQueryAbstractDataModificationPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Tq-Insert-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(TqInsertPlan.class);

    @Nonnull
    private final String tableQueueName;

    protected TqInsertPlan(@Nonnull final PlanSerializationContext serializationContext,
                           @Nonnull final PTqInsertPlan tqInsertPlanProto) {
        super(serializationContext, Objects.requireNonNull(tqInsertPlanProto.getSuper()));
        this.tableQueueName = tqInsertPlanProto.getTableQueueName();
    }

    private TqInsertPlan(@Nonnull final Quantifier.Physical inner,
                         @Nonnull final String recordType,
                         @Nonnull final Type.Record targetType,
                         @Nullable final MessageHelpers.CoercionTrieNode coercionsTrie,
                         @Nonnull final Value computationValue,
                         @Nonnull final String tableQueueName) {
        super(inner, recordType, targetType, null, coercionsTrie, computationValue, currentModifiedRecordAlias());
        this.tableQueueName = tableQueueName;
    }

    @Override
    public PipelineOperation getPipelineOperation() {
        return PipelineOperation.INSERT;
    }

    @Nonnull
    @Override
    protected <M extends Message> Descriptors.Descriptor getTargetDescriptor(@Nonnull final FDBRecordStoreBase<M> store) {
        TypeRepository temporaryTypeRepository = TypeRepository.newBuilder().addTypeIfNeeded(getTargetType()).build();
        return temporaryTypeRepository.getMessageDescriptor(super.getResultType().getInnerType());
    }

    @Override
    public @Nonnull <M extends Message> CompletableFuture<QueryResult> saveRecordAsync(@Nonnull FDBRecordStoreBase<M> store,
                                                                                       @Nonnull EvaluationContext context,
                                                                                       @Nonnull M message,
                                                                                       boolean isDryRun) {
        // dry run is ignored since inserting into a table queue has no storage side effects.
        final var queryResult = QueryResult.ofComputed(message);
        final var tableQueue = context.getTableQueue(tableQueueName);
        tableQueue.add(queryResult);
        return CompletableFuture.completedFuture(queryResult);
    }

    @Nonnull
    @Override
    public TqInsertPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                              @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new TqInsertPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getTargetRecordType(),
                getTargetType(),
                getCoercionTrie(),
                getComputationValue(),
                tableQueueName);
    }

    @Nonnull
    @Override
    public TqInsertPlan withChild(@Nonnull final Reference childRef) {
        return new TqInsertPlan(Quantifier.physical(childRef),
                getTargetRecordType(),
                getTargetType(),
                getCoercionTrie(),
                getComputationValue(),
                tableQueueName);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), super.hashCodeWithoutChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, super.planHash(mode));
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
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {

        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.TemporaryDataNodeWithInfo(getTargetType(), ImmutableList.of(tableQueueName)),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("TQINSERT"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    @Nonnull
    @Override
    public PTqInsertPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PTqInsertPlan.newBuilder().setSuper(toRecordQueryAbstractModificationPlanProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setTqInsertPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static TqInsertPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PTqInsertPlan tqInsertPlanProto) {
        return new TqInsertPlan(serializationContext, tqInsertPlanProto);
    }

    /**
     * Factory method to create a {@link TqInsertPlan}.
     *
     * @param inner an input value to transform
     * @param recordType the name of the record type this update modifies
     * @param targetType a target type to coerce the current record to prior to the update
     * @param computationValue a value to be computed based on the {@code inner} and
     * {@link RecordQueryAbstractDataModificationPlan#currentModifiedRecordAlias()}
     *
     * @return a newly created {@link TqInsertPlan}
     */
    @Nonnull
    public static TqInsertPlan insertPlan(@Nonnull final Quantifier.Physical inner,
                                          @Nonnull final String recordType,
                                          @Nonnull final Type.Record targetType,
                                          @Nonnull final Value computationValue,
                                          @Nonnull final String tableQueueName) {
        return new TqInsertPlan(inner,
                recordType,
                targetType,
                PromoteValue.computePromotionsTrie(targetType, inner.getFlowedObjectType(), null),
                computationValue,
                tableQueueName);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PTqInsertPlan, TqInsertPlan> {
        @Nonnull
        @Override
        public Class<PTqInsertPlan> getProtoMessageClass() {
            return PTqInsertPlan.class;
        }

        @Nonnull
        @Override
        public TqInsertPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PTqInsertPlan tqInsertPlanProto) {
            return TqInsertPlan.fromProto(serializationContext, tqInsertPlanProto);
        }
    }
}
