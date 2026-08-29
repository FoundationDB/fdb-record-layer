/*
 * RecordQueryUnorderedUnionPlan.java
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
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnorderedUnionCursor;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A query plan that returns results from two-or-more cursors as they as ready. Unlike the {@link RecordQueryUnionPlan},
 * there are no ordering restrictions placed on the child plans (i.e., the children are free to return results
 * in any order). However, this plan also makes no effort to remove duplicates from its children, and it also
 * makes no guarantees as to what order it will return results.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQueryUnorderedUnionPlan extends RecordQueryUnionPlanBase {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Unordered-Union-Plan");

    protected RecordQueryUnorderedUnionPlan(final PlanSerializationContext serializationContext,
                                            final PRecordQueryUnorderedUnionPlan recordQueryUnorderedUnionPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryUnorderedUnionPlanProto.getSuper()));
    }

    private RecordQueryUnorderedUnionPlan(final List<Quantifier.Physical> quantifiers,
                                          final boolean reverse) {
        super(quantifiers, reverse);
    }

    @Override
    <M extends Message> RecordCursor<QueryResult> createUnionCursor(FDBRecordStoreBase<M> store,
                                                                    EvaluationContext context,
                                                                    List<Function<byte[], RecordCursor<QueryResult>>> childCursorFunctions,
                                                                    @Nullable byte[] continuation) {
        return UnorderedUnionCursor.create(childCursorFunctions, continuation, store.getTimer());
    }

    @Override
    public String getDelimiter() {
        return " " + UNION + " ";
    }

    @Override
    StoreTimer.Count getPlanCount() {
        return FDBStoreTimer.Counts.PLAN_UNORDERED_UNION;
    }

    public static RecordQueryUnorderedUnionPlan fromQuantifiers(List<Quantifier.Physical> quantifiers) {
        return new RecordQueryUnorderedUnionPlan(quantifiers, Quantifiers.isReversed(quantifiers));
    }

    @HeuristicPlanner
    public static RecordQueryUnorderedUnionPlan from(List<? extends RecordQueryPlan> children) {
        Debugger.verifyHeuristicPlanner();
        final boolean reverse = children.get(0).isReverse();
        ImmutableList.Builder<Reference> builder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            builder.add(Reference.plannedOf(child));
        }
        return new RecordQueryUnorderedUnionPlan(Quantifiers.fromPlans(builder.build()), reverse);
    }

    @HeuristicPlanner
    public static RecordQueryUnorderedUnionPlan from(RecordQueryPlan left, RecordQueryPlan right) {
        Debugger.verifyHeuristicPlanner();
        return new RecordQueryUnorderedUnionPlan(
                Quantifiers.fromPlans(ImmutableList.of(Reference.plannedOf(left), Reference.plannedOf(right))),
                left.isReverse());
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public RecordQueryUnorderedUnionPlan translateCorrelations(final TranslationMap translationMap,
                                                               final boolean shouldSimplifyValues,
                                                               final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUnorderedUnionPlan(
                Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers), isReverse());
    }

    @Override
    public RecordQueryUnorderedUnionPlan withChildrenReferences(final List<? extends Reference> newChildren) {
        return new RecordQueryUnorderedUnionPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                isReverse());
    }

    @Override
    public Set<KeyExpression> getRequiredFields() {
        return ImmutableSet.of();
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.UNORDERED_UNION_OPERATOR),
                childGraphs);
    }

    @Override
    public int planHash(final PlanHashable.PlanHashMode mode) {
        return super.basePlanHash(mode, BASE_HASH);
    }

    @Override
    public PRecordQueryUnorderedUnionPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryUnorderedUnionPlan.newBuilder()
                .setSuper(toRecordQueryUnionPlanBaseProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setUnorderedUnionPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryUnorderedUnionPlan fromProto(final PlanSerializationContext serializationContext,
                                                          final PRecordQueryUnorderedUnionPlan recordQueryUnorderedUnionPlanProto) {
        return new RecordQueryUnorderedUnionPlan(serializationContext, recordQueryUnorderedUnionPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryUnorderedUnionPlan, RecordQueryUnorderedUnionPlan> {
        @Override
        public Class<PRecordQueryUnorderedUnionPlan> getProtoMessageClass() {
            return PRecordQueryUnorderedUnionPlan.class;
        }

        @Override
        public RecordQueryUnorderedUnionPlan fromProto(final PlanSerializationContext serializationContext,
                                                       final PRecordQueryUnorderedUnionPlan recordQueryUnorderedUnionPlanProto) {
            return RecordQueryUnorderedUnionPlan.fromProto(serializationContext, recordQueryUnorderedUnionPlanProto);
        }
    }
}
