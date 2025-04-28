/*
 * PlannerGraphRewritable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.explain;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Interface to allow {@link RelationalExpression}s to rewrite their own
 * graph representation.
 *
 * This interface allows a {@link RelationalExpression} to create/modify
 * low level planner graph structures while a planner graph is created through an expression walk in
 * {@link PlannerGraphVisitor}. Such a rewrite can be useful if the standard representation is confusing, misleading,
 * or just too complicated. For instance, {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex}
 * additionally creates a separate node underneath the actual plan operator to show the index as a record producer.
 * The nature of the rewrite is highly operator-specific, therefore, the rewrite is not part of the visitor or some
 * other common logic. Expressions that decide to implement this interface must then implement
 * {@link #rewritePlannerGraph} to do the actual rewrite.
 *
 * Implementors of this interface should rewrite the planner graph for both internal show as well as
 * explain purposes. There are two other flavors of this interface: see {@link InternalPlannerGraphRewritable} and
 * {@link ExplainPlannerGraphRewritable}. Those interfaces allow an expression to rewrite only for one of the use
 * cases or differently for either of them.
 *
 * Note that these three interfaces do not form a hierarchy themselves. Also, in order to keep the code from bleeding
 * into general expression structures, we didn't want to have another set of methods on
 * {@link RelationalExpression}.
 * The approach taken here works more like a tag interface that can be chosen by a specific operator to do something
 * very specific without cluttering the general code path.
 */
public interface PlannerGraphRewritable {
    /**
     * Method to rewrite the planner graph.
     *
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return a new planner graph that can combine the {@code childGraph}s in a meaningful way. Note that there
     *         is no obligation to use the {@code childGraph}s at all, this method can create a new independent
     *         planner graph completely from scratch.
     */
    @Nonnull
    PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs);
}
