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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that executes a child plan once for each of the elements of a constant {@code IN} list.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings({"squid:S1206", "squid:S2160", "PMD.OverrideBothEqualsAndHashcode"})
public class RecordQueryInValuesJoinPlan extends RecordQueryInJoinPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-In-Values-Join-Plan");

    public RecordQueryInValuesJoinPlan(final RecordQueryPlan plan,
                                       @Nonnull final String bindingName,
                                       @Nonnull final Bindings.Internal internal,
                                       @Nonnull final List<Object> values,
                                       final boolean sortValues,
                                       final boolean sortReverse) {
        this(Quantifier.physical(GroupExpressionRef.of(plan)),
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
        StringBuilder str = new StringBuilder(getInnerPlan().toString());
        str.append(" WHERE ").append(inSource.getBindingName())
                .append(" IN ").append(getInListValues());
        return str.toString();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryInValuesJoinPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<Quantifier> translatedQuantifiers) {
        return new RecordQueryInValuesJoinPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                inValuesSource(),
                internal);
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryInValuesJoinPlan(Quantifier.physical(GroupExpressionRef.of(child)), inValuesSource(), internal);
    }

    @Override
    @SuppressWarnings("fallthrough")
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                if (internal == Bindings.Internal.IN) {
                    return super.basePlanHash(hashKind, BASE_HASH) + PlanHashable.iterablePlanHash(hashKind, inSource.getValues());
                }
                // fall through
            case FOR_CONTINUATION:
                if (internal == Bindings.Internal.IN) {
                    return super.basePlanHash(hashKind, BASE_HASH, inSource.getValues());
                }
                // fall through
            case STRUCTURAL_WITHOUT_LITERALS:
                return super.basePlanHash(hashKind, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " not supported");
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
     *         a index lookup for each bound outer value.
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
}
