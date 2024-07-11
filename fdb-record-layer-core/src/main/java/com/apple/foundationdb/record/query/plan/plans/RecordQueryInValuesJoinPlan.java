/*
 * RecordQueryInValuesJoinPlan.java
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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A query plan that executes a child plan once for each of the elements of a constant {@code IN} list.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings({"squid:S1206", "squid:S2160", "PMD.OverrideBothEqualsAndHashcode"})
public class RecordQueryInValuesJoinPlan extends RecordQueryInJoinPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-In-Values-Join-Plan");

    protected RecordQueryInValuesJoinPlan(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PRecordQueryInValuesJoinPlan recordQueryInValuesJoinPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryInValuesJoinPlanProto.getSuper()));
    }

    public RecordQueryInValuesJoinPlan(final RecordQueryPlan plan,
                                       @Nonnull final String bindingName,
                                       @Nonnull final Bindings.Internal internal,
                                       @Nonnull final List<Object> values,
                                       final boolean sortValues,
                                       final boolean sortReverse) {
        this(Quantifier.physical(Reference.of(plan)),
                bindingName,
                internal,
                values,
                sortValues,
                sortReverse);
    }

    private RecordQueryInValuesJoinPlan(@Nonnull final Quantifier.Physical inner,
                                        @Nonnull final String bindingName,
                                        @Nonnull final Bindings.Internal internal,
                                        @Nonnull final List<Object> values,
                                        final boolean sortValues,
                                        final boolean sortReverse) {
        super(inner,
                sortValues
                ? new SortedInValuesSource(bindingName, values, sortReverse)
                : new InValuesSource(bindingName, values),
                internal);
    }

    public RecordQueryInValuesJoinPlan(@Nonnull final Quantifier.Physical inner,
                                       @Nonnull final InValuesSource inSource,
                                       @Nonnull final Bindings.Internal internal) {
        super(inner, inSource, internal);
    }

    @Nonnull
    private InValuesSource inValuesSource() {
        return (InValuesSource)inSource;
    }

    @Override
    @Nonnull
    public List<Object> getValues(EvaluationContext context) {
        return getInListValues();
    }

    @Nonnull
    public List<Object> getInListValues() {
        return inSource.getValues();
    }

    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public RecordQueryInValuesJoinPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInValuesJoinPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                inValuesSource(),
                internal);
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryInValuesJoinPlan(Quantifier.physical(childRef), inValuesSource(), internal);
    }

    @Override
    @SuppressWarnings("fallthrough")
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                if (internal == Bindings.Internal.IN) {
                    return super.basePlanHash(mode, BASE_HASH) + PlanHashable.iterablePlanHash(mode, inSource.getValues());
                }
                // fall through
            case FOR_CONTINUATION:
                if (internal == Bindings.Internal.IN) {
                    return super.basePlanHash(mode, BASE_HASH, inSource.getValues());
                }
                return super.basePlanHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " not supported");
        }
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_VALUES);
        getChild().logPlanStructure(timer);
    }

    /**
     * Rewrite the planner graph for better visualization of this plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models this operator as a logical nested loop join
     *         joining an outer table of values in the IN clause to the correlated inner result of executing (usually)
     *         an index lookup for each bound outer value.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR);
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.DataNodeWithInfo valuesNode =
                new PlannerGraph.DataNodeWithInfo(NodeInfo.VALUES_DATA,
                        getResultType(),
                        ImmutableList.of("VALUES({{values}}"),
                        ImmutableMap.of("values",
                                Attribute.gml(Objects.requireNonNull(getInListValues()).stream()
                                        .map(String::valueOf)
                                        .map(Attribute::gml)
                                        .collect(ImmutableList.toImmutableList()))));
        final PlannerGraph.Edge fromValuesEdge = new PlannerGraph.Edge();
        return PlannerGraph.builder(root)
                .addGraph(graphForInner)
                .addNode(valuesNode)
                .addEdge(valuesNode, root, fromValuesEdge)
                .addEdge(graphForInner.getRoot(), root, new PlannerGraph.Edge(ImmutableSet.of(fromValuesEdge)))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryInValuesJoinPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryInValuesJoinPlan.newBuilder()
                .setSuper(toRecordQueryInJoinPlanProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setInValuesJoinPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryInValuesJoinPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PRecordQueryInValuesJoinPlan recordQueryInValuesJoinPlanProto) {
        return new RecordQueryInValuesJoinPlan(serializationContext, recordQueryInValuesJoinPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryInValuesJoinPlan, RecordQueryInValuesJoinPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryInValuesJoinPlan> getProtoMessageClass() {
            return PRecordQueryInValuesJoinPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryInValuesJoinPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PRecordQueryInValuesJoinPlan recordQueryInValuesJoinPlanProto) {
            return RecordQueryInValuesJoinPlan.fromProto(serializationContext, recordQueryInValuesJoinPlanProto);
        }
    }
}
