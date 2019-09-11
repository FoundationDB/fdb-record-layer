/*
 * LogicalUnorderedUnionExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.NestedContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

/**
 * A relational planner expression that represents an unimplemented unordered union of its children.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalUnorderedUnionExpression implements RelationalExpressionWithChildren {
    @Nonnull
    private List<ExpressionRef<RelationalPlannerExpression>> expressionChildren;

    public LogicalUnorderedUnionExpression(@Nonnull List<ExpressionRef<RelationalPlannerExpression>> expressionChildren) {
        this.expressionChildren = expressionChildren;
    }

    @Override
    public int getRelationalChildCount() {
        return expressionChildren.size();
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return expressionChildren.iterator();
    }

    @Nullable
    @Override
    public ExpressionRef<RelationalPlannerExpression> asNestedWith(@Nonnull NestedContext nestedContext,
                                                                   @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef) {
        final ImmutableList.Builder<ExpressionRef<RelationalPlannerExpression>> nestedChildren = ImmutableList.builder();
        for (ExpressionRef<RelationalPlannerExpression> child : expressionChildren) {
            final ExpressionRef<RelationalPlannerExpression> nestedChild = nestedContext.getNestedRelationalPlannerExpression(child);
            if (nestedChild == null) {
                return null;
            }
            nestedChildren.add(nestedChild);
        }
        return thisRef.getNewRefWith(new LogicalUnorderedUnionExpression(nestedChildren.build()));
    }

    @Nullable
    @Override
    public ExpressionRef<RelationalPlannerExpression> asUnnestedWith(@Nonnull NestedContext nestedContext,
                                                                     @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef) {
        final ImmutableList.Builder<ExpressionRef<RelationalPlannerExpression>> unnestedChildren = ImmutableList.builder();
        for (ExpressionRef<RelationalPlannerExpression> child : expressionChildren) {
            final ExpressionRef<RelationalPlannerExpression> unnestedChild = nestedContext.getUnnestedRelationalPlannerExpression(child);
            if (unnestedChild == null) {
                return null;
            }
            unnestedChildren.add(unnestedChild);
        }
        return thisRef.getNewRefWith(new LogicalUnorderedUnionExpression(unnestedChildren.build()));
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        return otherExpression instanceof LogicalUnorderedUnionExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || o.getClass() != getClass())  {
            return false;
        }
        LogicalUnorderedUnionExpression that = (LogicalUnorderedUnionExpression)o;
        return expressionChildren.equals(that.expressionChildren);
    }

    @Override
    public int hashCode() {
        return expressionChildren.hashCode();
    }
}
