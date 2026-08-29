/*
 * RecordQueryInComparandJoinPlan.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecordQueryInComparandJoinPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Objects;

/**
 * A query plan that executes a child plan once for each of the elements extracted from a
 * {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison Comparison} object's
 * comparand.
 */
@API(API.Status.INTERNAL)
public class RecordQueryInComparandJoinPlan extends RecordQueryInJoinPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-In-Comparand-Join-Plan");

    protected RecordQueryInComparandJoinPlan(final PlanSerializationContext serializationContext,
                                             final PRecordQueryInComparandJoinPlan recordQueryInComparandJoinPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryInComparandJoinPlanProto.getSuper()));
    }

    @HeuristicPlanner
    public RecordQueryInComparandJoinPlan(final RecordQueryPlan plan,
                                          final String bindingName,
                                          final Bindings.Internal internal,
                                          final Comparisons.Comparison comparison,
                                          final boolean sortValues,
                                          final boolean sortReverse) {
        this(Quantifier.physical(Reference.plannedOf(Debugger.verifyHeuristicPlanner(plan))),
                bindingName,
                internal,
                comparison,
                sortValues,
                sortReverse);
    }

    public RecordQueryInComparandJoinPlan(final Quantifier.Physical inner,
                                          final String bindingName,
                                          final Bindings.Internal internal,
                                          final Comparisons.Comparison comparison,
                                          final boolean sortValues,
                                          final boolean sortReverse) {
        this(inner,
                sortValues
                ? new SortedInComparandSource(bindingName, comparison, sortReverse)
                : new InComparandSource(bindingName, comparison),
                internal);
    }

    public RecordQueryInComparandJoinPlan(final Quantifier.Physical inner, final InComparandSource inSource, final Bindings.Internal internal) {
        super(inner, inSource, internal);
    }

    @Override
    public RelationalExpression translateCorrelations(final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInComparandJoinPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                inComparandSource(),
                internal);
    }

    @Override
    public RecordQueryPlanWithChild withChild(final Reference childRef) {
        return new RecordQueryInComparandJoinPlan(Quantifier.physical(childRef, inner.getAlias()), inComparandSource(), internal);
    }

    private InComparandSource inComparandSource() {
        return (InComparandSource)inSource;
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        if (internal == Bindings.Internal.IN) {
            return super.basePlanHash(mode, BASE_HASH, inComparandSource());
        } else {
            return super.basePlanHash(mode, BASE_HASH);
        }
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_COMPARAND);
        getInnerPlan().logPlanStructure(timer);
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR);
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.NodeWithInfo explodeNode =
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.TABLE_FUNCTION_OPERATOR,
                        ImmutableList.of("EXPLODE({{externalBinding}})"),
                        ImmutableMap.of("externalBinding", Attribute.gml(inComparandSource().getComparison().typelessString())));
        final PlannerGraph.Edge fromExplodeEdge = new PlannerGraph.Edge();
        return PlannerGraph.builder(root)
                .addGraph(graphForInner)
                .addNode(explodeNode)
                .addEdge(explodeNode, root, fromExplodeEdge)
                .addEdge(graphForInner.getRoot(), root, new PlannerGraph.Edge(ImmutableSet.of(fromExplodeEdge)))
                .build();
    }

    @Override
    public PRecordQueryInComparandJoinPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryInComparandJoinPlan.newBuilder()
                .setSuper(toRecordQueryInJoinPlanProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setInComparandJoinPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryInComparandJoinPlan fromProto(final PlanSerializationContext serializationContext,
                                                           final PRecordQueryInComparandJoinPlan recordQueryInComparandJoinPlanProto) {
        return new RecordQueryInComparandJoinPlan(serializationContext, recordQueryInComparandJoinPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryInComparandJoinPlan, RecordQueryInComparandJoinPlan> {
        @Override
        public Class<PRecordQueryInComparandJoinPlan> getProtoMessageClass() {
            return PRecordQueryInComparandJoinPlan.class;
        }

        @Override
        public RecordQueryInComparandJoinPlan fromProto(final PlanSerializationContext serializationContext,
                                                        final PRecordQueryInComparandJoinPlan recordQueryInComparandJoinPlanProto) {
            return RecordQueryInComparandJoinPlan.fromProto(serializationContext, recordQueryInComparandJoinPlanProto);
        }
    }
}
