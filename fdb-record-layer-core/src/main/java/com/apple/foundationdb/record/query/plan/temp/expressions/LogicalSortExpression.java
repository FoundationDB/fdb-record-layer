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
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.Collections;
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
    private final List<Element> grouping;
    @Nonnull
    private final List<Element> sort;
    private final boolean reverse;
    @Nonnull
    private final ExpressionRef<RelationalPlannerExpression> inner;

    public LogicalSortExpression(@Nonnull List<Element> sort, boolean reverse, @Nonnull RelationalPlannerExpression inner) {
        this(sort, reverse, SingleExpressionRef.of(inner));
    }

    public LogicalSortExpression(@Nonnull List<Element> grouping, @Nonnull List<Element> sort, boolean reverse, @Nonnull RelationalPlannerExpression inner) {
        this.grouping = grouping;
        this.sort = sort;
        this.reverse = reverse;
        this.inner = SingleExpressionRef.of(inner);
    }


    public LogicalSortExpression(@Nonnull List<Element> sort, boolean reverse, @Nonnull ExpressionRef<RelationalPlannerExpression> inner) {
        this(Collections.emptyList(), sort, reverse, inner);
    }

    public LogicalSortExpression(@Nonnull List<Element> grouping, @Nonnull List<Element> sort, boolean reverse,
                                 @Nonnull ExpressionRef<RelationalPlannerExpression> inner) {
        this.grouping = grouping;
        this.sort = sort;
        this.reverse = reverse;
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
    public List<Element> getSortPrefix() {
        return ImmutableList.<Element>builderWithExpectedSize(grouping.size() + 1)
                .addAll(grouping)
                .add(sort.get(0))
                .build();
    }

    @Nonnull
    public List<Element> getSortSuffix() {
        return sort.subList(1, sort.size());
    }

    @Nonnull
    public List<Element> getGrouping() {
        return grouping;
    }

    @Nonnull
    public List<Element> getSort() {
        return sort;
    }

    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    public RelationalPlannerExpression getInner() {
        return inner.get();
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        if (!(otherExpression instanceof LogicalSortExpression)) {
            return false;
        }
        final LogicalSortExpression other = (LogicalSortExpression) otherExpression;
        return sort.equals(other.sort) && reverse == other.reverse;
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
