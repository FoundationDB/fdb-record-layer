/*
 * RecordQueryInParameterJoinPlan.java
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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Objects;

/**
 * A query plan that executes a child plan once for each of the elements of an {@code IN} list taken from a parameter.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings({"squid:S1206", "squid:S2160", "PMD.OverrideBothEqualsAndHashcode"})
public class RecordQueryInParameterJoinPlan extends RecordQueryInJoinPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-In-Parameter-Join-Plan");

    protected RecordQueryInParameterJoinPlan(final PlanSerializationContext serializationContext,
                                             final PRecordQueryInParameterJoinPlan recordQueryInParameterJoinPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryInParameterJoinPlanProto.getSuper()));
    }

    @HeuristicPlanner
    public RecordQueryInParameterJoinPlan(final RecordQueryPlan plan,
                                          final String bindingName,
                                          final Bindings.Internal internal,
                                          final String externalBinding,
                                          final boolean sortValues,
                                          final boolean sortReverse) {
        this(Quantifier.physical(Reference.plannedOf(Debugger.verifyHeuristicPlanner(plan))),
                bindingName,
                internal,
                externalBinding,
                sortValues,
                sortReverse);
    }

    public RecordQueryInParameterJoinPlan(final Quantifier.Physical inner,
                                          final String bindingName,
                                          final Bindings.Internal internal,
                                          final String externalBinding,
                                          final boolean sortValues,
                                          final boolean sortReverse) {
        this(inner,
                sortValues
                ? new SortedInParameterSource(bindingName, externalBinding, sortReverse)
                : new InParameterSource(bindingName, externalBinding),
                internal);
    }

    public RecordQueryInParameterJoinPlan(final Quantifier.Physical inner,
                                          final InSource inSource,
                                          final Bindings.Internal internal) {
        super(inner, inSource, internal);
    }

    private InParameterSource inParameterSource() {
        return (InParameterSource)inSource;
    }

    public String getExternalBinding() {
        return inParameterSource().getParameterName();
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    public RecordQueryInParameterJoinPlan translateCorrelations(final TranslationMap translationMap,
                                                                final boolean shouldSimplifyValues,
                                                                final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInParameterJoinPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class), inSource,
                internal);
    }

    @Override
    public RecordQueryPlanWithChild withChild(final Reference childRef) {
        return new RecordQueryInParameterJoinPlan(Quantifier.physical(childRef, inner.getAlias()), inSource, internal);
    }

    @Override
    @SuppressWarnings("fallthrough")
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                if (internal == Bindings.Internal.IN) {
                    return super.basePlanHash(mode, BASE_HASH) + inParameterSource().getParameterName().hashCode();
                }
                // fall through
            case FOR_CONTINUATION:
                if (internal == Bindings.Internal.IN) {
                    return super.basePlanHash(mode, BASE_HASH, inParameterSource().getParameterName());
                }
                return super.basePlanHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_PARAMETER);
        getInnerPlan().logPlanStructure(timer);
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models this operator as a logical nested loop join
     *         joining an outer table of iterated values over a parameter in the IN clause to the correlated inner
     *         result of executing (usually) an index lookup for each bound outer value.
     */
    @Override
    public PlannerGraph rewritePlannerGraph(List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR);
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.NodeWithInfo explodeNode =
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.TABLE_FUNCTION_OPERATOR,
                        ImmutableList.of("EXPLODE({{externalBinding}})"),
                        ImmutableMap.of("externalBinding", Attribute.gml(inParameterSource().getParameterName())));
        final PlannerGraph.Edge fromExplodeEdge = new PlannerGraph.Edge();
        return PlannerGraph.builder(root)
                .addGraph(graphForInner)
                .addNode(explodeNode)
                .addEdge(explodeNode, root, fromExplodeEdge)
                .addEdge(graphForInner.getRoot(), root, new PlannerGraph.Edge(ImmutableSet.of(fromExplodeEdge)))
                .build();
    }

    @Override
    public PRecordQueryInParameterJoinPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryInParameterJoinPlan.newBuilder()
                .setSuper(toRecordQueryInJoinPlanProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setInParameterJoinPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryInParameterJoinPlan fromProto(final PlanSerializationContext serializationContext,
                                                           final PRecordQueryInParameterJoinPlan recordQueryInParameterJoinPlanProto) {
        return new RecordQueryInParameterJoinPlan(serializationContext, recordQueryInParameterJoinPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryInParameterJoinPlan, RecordQueryInParameterJoinPlan> {
        @Override
        public Class<PRecordQueryInParameterJoinPlan> getProtoMessageClass() {
            return PRecordQueryInParameterJoinPlan.class;
        }

        @Override
        public RecordQueryInParameterJoinPlan fromProto(final PlanSerializationContext serializationContext,
                                                        final PRecordQueryInParameterJoinPlan recordQueryInParameterJoinPlanProto) {
            return RecordQueryInParameterJoinPlan.fromProto(serializationContext, recordQueryInParameterJoinPlanProto);
        }
    }
}
