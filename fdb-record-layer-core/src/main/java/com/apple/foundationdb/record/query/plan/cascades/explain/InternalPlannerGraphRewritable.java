/*
 * InternalPlannerGraphRewritable.java
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
 * internal graph representation. Please see {@link PlannerGraphRewritable} for a more comprehensive explanation.
 * <br>
 * This particular class allows {@link RelationalExpression}s to specify
 * how a {@link PlannerGraph} is modified when we use the {@link PlannerGraphVisitor} for developers working
 * on the query planner.
 * @see PlannerGraphRewritable for a more comprehensive explanation of graph rewriting.
 */
public interface InternalPlannerGraphRewritable {
    /**
     * Method to rewrite the planner graph.
     *
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return a new planner graph that can combine the {@code childGraph}s in a meaningful way. Note that there
     *         is no obligation to use the {@code childGraph}s at all, this method can create a new independent
     *         planner graph completely from scratch.
     */
    @Nonnull
    PlannerGraph rewriteInternalPlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs);
}
