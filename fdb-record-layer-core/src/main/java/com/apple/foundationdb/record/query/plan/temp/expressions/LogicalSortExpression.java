/*
 * LogicalSortExpression.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A relational planner expression that represents an unimplemented sort on the records produced by its inner
 * relational planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalSortExpression implements RelationalExpressionWithChildren {
    @Nonnull
    private final ExpressionRef<KeyExpression> sort;
    private final boolean reverse;
    @Nonnull
    private final ExpressionRef<RelationalPlannerExpression> inner;
    @Nonnull
    private final List<ExpressionRef<? extends PlannerExpression>> expressionChildren;

    public LogicalSortExpression(@Nonnull KeyExpression sort, boolean reverse, @Nonnull RelationalPlannerExpression inner) {
        this(SingleExpressionRef.of(sort), reverse, SingleExpressionRef.of(inner));
    }

    public LogicalSortExpression(@Nonnull ExpressionRef<KeyExpression> sort, boolean reverse, @Nonnull ExpressionRef<RelationalPlannerExpression> inner) {
        this.sort = sort;
        this.reverse = reverse;
        this.inner = inner;
        this.expressionChildren = ImmutableList.of(this.sort, this.inner);
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return expressionChildren.iterator();
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    public KeyExpression getSort() {
        return sort.get();
    }

    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    public RelationalPlannerExpression getInner() {
        return inner.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSortExpression that = (LogicalSortExpression)o;
        return isReverse() == that.isReverse() &&
               Objects.equals(getSort(), that.getSort()) &&
               Objects.equals(getInner(), that.getInner());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSort(), isReverse(), getInner());
    }
}
