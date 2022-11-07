/*
 * RecordQueryUpdatePlan.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A query plan that filters out records from a child plan that are not of the designated record type(s).
 */
@API(API.Status.INTERNAL)
public class RecordQueryUpdatePlan extends RecordQueryAbstractDataModificationPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Update-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUpdatePlan.class);

    private RecordQueryUpdatePlan(@Nonnull final Quantifier.Physical inner,
                                  @Nonnull final String targetRecordType,
                                  @Nonnull final Type.Record targetType,
                                  @Nonnull final Descriptors.Descriptor targetDescriptor,
                                  @Nullable final MessageHelpers.TransformationTrieNode transformationsTrie,
                                  @Nullable final MessageHelpers.CoercionTrieNode coercionsTrie,
                                  @Nonnull final Value computationValue) {
        super(inner, targetRecordType, targetType, targetDescriptor, transformationsTrie, coercionsTrie, computationValue);
    }

    public <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final M message) {
        return store.saveRecordAsync(message, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED);
    }

    @Nonnull
    @Override
    public RecordQueryUpdatePlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                       @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUpdatePlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getTargetRecordType(),
                getTargetType(),
                getTargetDescriptor(),
                translateTransformationsTrie(translationMap),
                getCoercionTrie(),
                getComputationValue().translateCorrelations(translationMap));
    }

    @Nonnull
    @Override
    public RecordQueryUpdatePlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryUpdatePlan(Quantifier.physical(GroupExpressionRef.of(child)),
                getTargetRecordType(),
                getTargetType(),
                getTargetDescriptor(),
                getTransformationsTrie(),
                getCoercionTrie(),
                getComputationValue());
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(), super.hashCodeWithoutChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, super.planHash(hashKind));
    }

    @Nonnull
    @Override
    public String toString() {
        // TODO provide proper explain
        return getInnerPlan() + " | " + "UPDATE " + getTargetRecordType();
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
        Verify.verify(childGraphs.size() == 1);
        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                                getResultType(),
                                ImmutableList.of(getTargetRecordType())),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("UPDATE"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    /**
     * Factory method to create a {@link RecordQueryUpdatePlan}.
     * <br>
     * Example:
     * <pre>
     * {@code
     *    RecordQueryAbstractDataModificationPlan.forTransformMap(inner,
     *                                          ...,
     *                                          ImmutableMap.of(path1, new LiteralValue<>("1"),
     *                                                          path2, new LiteralValue<>(2),
     *                                                          path3, new LiteralValue<>(3)));
     * }
     * </pre>
     * transforms the current inner's object, according to the transform map, i.e. the data underneath {@code path1}
     * is transformed to the value {@code "1"}, the data underneath {@code path2} is transformed to the value {@code 2},
     * and the data underneath {@code path2} is transformed to the value {@code 3}.
     * @param inner an input value to transform
     * @param targetRecordType the name of the record type this update modifies
     * @param targetType a target type to coerce the current record to prior to the update
     * @param targetDescriptor a descriptor to coerce the current record to prior to the update
     * @param transformMap a map of field paths to values.
     * @param computationValue a value to be computed based on the {@code inner} and
     *        {@link RecordQueryAbstractDataModificationPlan#CURRENT_MODIFIED_RECORD}
     * @return a newly created {@link RecordQueryUpdatePlan}
     */
    @Nonnull
    public static RecordQueryUpdatePlan updatePlan(@Nonnull final Quantifier.Physical inner,
                                                   @Nonnull final String targetRecordType,
                                                   @Nonnull final Type.Record targetType,
                                                   @Nonnull final Descriptors.Descriptor targetDescriptor,
                                                   @Nonnull final Map<FieldValue.FieldPath, Value> transformMap,
                                                   @Nonnull final Value computationValue) {
        final var transformationsTrie = computeTrieForFieldPaths(checkAndPrepareOrderedFieldPaths(transformMap), transformMap);
        return new RecordQueryUpdatePlan(inner,
                targetRecordType,
                targetType,
                targetDescriptor,
                transformationsTrie,
                computePromotionsTrie(targetType, inner.getFlowedObjectType(), transformationsTrie),
                computationValue);
    }
}
