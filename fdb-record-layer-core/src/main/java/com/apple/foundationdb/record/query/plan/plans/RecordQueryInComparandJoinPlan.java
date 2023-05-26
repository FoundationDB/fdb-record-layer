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
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A query plan that executes a child plan once for each of the elements extracted from a
 * {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison Comparison} object's
 * comparand.
 */
@API(API.Status.INTERNAL)
public class RecordQueryInComparandJoinPlan extends RecordQueryInJoinPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-In-Comparand-Join-Plan");

    public RecordQueryInComparandJoinPlan(@Nonnull final RecordQueryPlan plan,
                                          @Nonnull final String bindingName,
                                          @Nonnull final Bindings.Internal internal,
                                          @Nonnull final Comparisons.Comparison comparison,
                                          final boolean sortValues,
                                          final boolean sortReverse) {
        this(Quantifier.physical(GroupExpressionRef.of(plan)),
                bindingName,
                internal,
                comparison,
                sortValues,
                sortReverse);
    }

    public RecordQueryInComparandJoinPlan(@Nonnull final Quantifier.Physical inner,
                                          @Nonnull final String bindingName,
                                          @Nonnull final Bindings.Internal internal,
                                          @Nonnull final Comparisons.Comparison comparison,
                                          final boolean sortValues,
                                          final boolean sortReverse) {
        this(inner,
                sortValues
                ? new SortedInComparandSource(bindingName, comparison, sortReverse)
                : new InComparandSource(bindingName, comparison),
                internal);
    }

    public RecordQueryInComparandJoinPlan(@Nonnull final Quantifier.Physical inner, @Nonnull final InComparandSource inSource, @Nonnull final Bindings.Internal internal) {
        super(inner, inSource, internal);
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInComparandJoinPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                inComparandSource(),
                internal);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final ExpressionRef<? extends RecordQueryPlan> childRef) {
        return new RecordQueryInComparandJoinPlan(Quantifier.physical(childRef), inComparandSource(), internal);
    }

    private InComparandSource inComparandSource() {
        return (InComparandSource)inSource;
    }

    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        if (internal == Bindings.Internal.IN) {
            return super.basePlanHash(hashKind, BASE_HASH, inComparandSource());
        } else {
            return super.basePlanHash(hashKind, BASE_HASH);
        }
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_COMPARAND);
        getInnerPlan().logPlanStructure(timer);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
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
}
