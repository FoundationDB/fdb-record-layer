/*
 * RecordQueryInsertPlan.java
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
 * A query plan that filters out records from a child plan that are not of the designated record type(s).
 */
@API(API.Status.INTERNAL)
public class RecordQueryInsertPlan extends RecordQueryAbstractDataModificationPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Insert-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryInsertPlan.class);

    private RecordQueryInsertPlan(@Nonnull final Quantifier.Physical inner,
                                  @Nonnull final String recordType,
                                  @Nonnull final Type.Record targetType,
                                  @Nonnull final Descriptors.Descriptor targetDescriptor,
                                  @Nullable final TrieNode promotionsTrie) {
        super(inner, recordType, targetType, targetDescriptor, null, promotionsTrie);
    }

    public <M extends Message> CompletableFuture<FDBStoredRecord<M>> saveRecordAsync(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final M message) {
        return store.saveRecordAsync(message, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS);
    }

    @Nonnull
    @Override
    public String toString() {
        // TODO provide proper explain
        return getInnerPlan() + " | " + "UPDATE " + getTargetRecordType();
    }

    @Nonnull
    @Override
    public RecordQueryInsertPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                       @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInsertPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getTargetRecordType(),
                getTargetType(),
                getTargetDescriptor(),
                getPromotionsTrie());
    }

    @Nonnull
    @Override
    public RecordQueryInsertPlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryInsertPlan(Quantifier.physical(GroupExpressionRef.of(child)),
                getTargetRecordType(),
                getTargetType(),
                getTargetDescriptor(),
                getPromotionsTrie());
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

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
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
                                ImmutableList.of(getTargetRecordType())),
                        ImmutableList.of());

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("INSERT"),
                        ImmutableMap.of()),
                ImmutableList.<PlannerGraph>builder().addAll(childGraphs).add(graphForTarget).build());
    }

    /**
     * Factory method to create a {@link RecordQueryInsertPlan}.
     * @param inner an input value to transform
     * @param recordType the name of the record type this update modifies
     * @param targetType a target type to coerce the current record to prior to the update
     * @param targetDescriptor a descriptor to coerce the current record to prior to the update
     * @return a newly created {@link RecordQueryInsertPlan}
     */
    @Nonnull
    public static RecordQueryInsertPlan insertPlan(@Nonnull final Quantifier.Physical inner,
                                                   @Nonnull final String recordType,
                                                   @Nonnull final Type.Record targetType,
                                                   @Nonnull final Descriptors.Descriptor targetDescriptor) {
        return new RecordQueryInsertPlan(inner,
                recordType,
                targetType,
                targetDescriptor,
                computePromotionsTrie(targetType, inner.getFlowedObjectType(), null));
    }
}
