/*
 * PlanOpsMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.FindExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class for caching a mapping from operation types to expressions. This is used by the {@link CascadesCostModel}s,
 * as certain tiebreakers operate by counting the set of underlying elements of a given type. This caches the
 * result so that multiple invocations of the same type do not require walking the tree multiple times.
 */
@API(API.Status.INTERNAL)
public class PlanOpsMap {
    @Nonnull
    private final RelationalExpression expression;
    @Nonnull
    private final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> underlying = new HashMap<>();

    public PlanOpsMap(@Nonnull final RelationalExpression expression) {
        this.expression = expression;
    }

    /**
     * Get the set of expressions of a given type. This will accumulate all the expressions of the
     * provided type from an expression's DAG and return them in a single set. This method will cache its results,
     * so if this is invoked multiple times with the same class, then the same result will be returned
     * without doing a recalculation.
     *
     * @param expressionClass the type of expression to collect
     * @return a set containing all the expressions of a given type in the map's tree
     */
    @Nonnull
    public Set<RelationalExpression> get(@Nonnull final Class<? extends RelationalExpression> expressionClass) {
        return underlying.computeIfAbsent(expressionClass, this::findExpressions);
    }

    @Nonnull
    private Set<RelationalExpression> findExpressions(@Nonnull Class<? extends RelationalExpression> expressionClass) {
        return FindExpressionVisitor.findExpressions(expressionClass, expression);
    }
}
