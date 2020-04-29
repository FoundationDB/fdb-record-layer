/*
 * LogicalFilterExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpressionWithPredicate;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Objects;

/**
 * A relational planner expression that represents an unimplemented filter on the records produced by its inner
 * relational planner expression.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalFilterExpression implements RelationalExpressionWithChildren, PlannerExpressionWithPredicate {
    @Nonnull
    private final Source baseSource;
    @Nonnull
    private final QueryPredicate filter;
    @Nonnull
    private final ExpressionRef<RelationalPlannerExpression> inner;

    public LogicalFilterExpression(@Nonnull Source baseSource,
                                   @Nonnull QueryPredicate filter,
                                   @Nonnull RelationalPlannerExpression inner) {
        this(baseSource, filter, GroupExpressionRef.of(inner));
    }

    public LogicalFilterExpression(@Nonnull Source baseSource,
                                   @Nonnull QueryPredicate filter,
                                   @Nonnull ExpressionRef<RelationalPlannerExpression> inner) {
        this.baseSource = baseSource;
        this.filter = filter;
        this.inner = inner;
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(inner);
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public QueryPredicate getPredicate() {
        return filter;
    }

    @Nonnull
    public Source getBaseSource() {
        return baseSource;
    }

    @Nonnull
    public RelationalPlannerExpression getInner() {
        return inner.get();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        return otherExpression instanceof LogicalFilterExpression && filter.equals(((LogicalFilterExpression)otherExpression).getPredicate());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalFilterExpression that = (LogicalFilterExpression)o;
        return Objects.equals(getPredicate(), that.getPredicate()) &&
               Objects.equals(getInner(), that.getInner());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPredicate(), getInner());
    }
}
