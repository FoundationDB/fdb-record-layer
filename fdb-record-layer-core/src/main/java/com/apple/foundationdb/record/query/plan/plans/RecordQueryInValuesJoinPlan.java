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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A query plan that executes a child plan once for each of the elements of a constant {@code IN} list.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryInValuesJoinPlan extends RecordQueryInJoinPlan implements PlannerGraphRewritable {
    @Nullable
    private final List<Object> values;

    public RecordQueryInValuesJoinPlan(RecordQueryPlan plan, String bindingName, @Nullable List<Object> values, boolean sortValues, boolean sortReverse) {
        super(plan, bindingName, sortValues, sortReverse);
        this.values = sortValues(values);
    }

    @Override
    @Nullable
    public List<Object> getValues(EvaluationContext context) {
        return values;
    }


    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getInner().toString());
        str.append(" WHERE ").append(bindingName)
                .append(" IN ").append(values);
        if (sortValuesNeeded) {
            str.append(" SORTED");
            if (sortReverse) {
                str.append(" DESC");
            }
        }
        return str.toString();
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        return otherExpression instanceof RecordQueryInValuesJoinPlan &&
               super.equalsWithoutChildren(otherExpression) &&
               Objects.equals(values, ((RecordQueryInValuesJoinPlan)otherExpression).values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RecordQueryInValuesJoinPlan that = (RecordQueryInValuesJoinPlan)o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), values);
    }

    @Override
    public int planHash() {
        return super.planHash() + PlanHashable.iterablePlanHash(values);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_VALUES);
        getChild().logPlanStructure(timer);
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models this operator as a logical nested loop join
     *         joining an outer table of values in the IN clause to the correlated inner result of executing (usually)
     *         a index lookup for each bound outer value.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.Node(this,
                        getClass().getSimpleName());
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.SourceNode valuesNode =
                new PlannerGraph.SourceNode("Values",
                        Objects.requireNonNull(values).stream().map(String::valueOf).collect(Collectors.joining(", ")));
        final PlannerGraph.Edge fromValuesEdge = new PlannerGraph.Edge();
        return PlannerGraph.builder(root)
                .addGraph(graphForInner)
                .addNode(valuesNode)
                .addEdge(valuesNode, root, fromValuesEdge)
                .addEdge(graphForInner.getRoot(), root, new PlannerGraph.Edge(ImmutableSet.of(fromValuesEdge)))
                .build();
    }
}
