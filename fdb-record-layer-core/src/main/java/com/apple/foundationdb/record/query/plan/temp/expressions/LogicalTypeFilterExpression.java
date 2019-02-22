/*
 * LogicalTypeFilterExpression.java
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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A relational planner expression that represents an unimplemented type filter on the records produced by its inner
 * relational planner expression.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalTypeFilterExpression implements TypeFilterExpression {
    @Nonnull
    private final Collection<String> recordTypes;
    @Nonnull
    private final ExpressionRef<RelationalPlannerExpression> inner;
    @Nonnull
    private final List<ExpressionRef<? extends PlannerExpression>> expressionChildren;

    public LogicalTypeFilterExpression(@Nonnull Collection<String> recordTypes, @Nonnull RelationalPlannerExpression inner) {
        this(recordTypes, SingleExpressionRef.of(inner));
    }

    public LogicalTypeFilterExpression(@Nonnull Collection<String> recordTypes, @Nonnull ExpressionRef<RelationalPlannerExpression> inner) {
        this.recordTypes = recordTypes;
        this.inner = inner;
        this.expressionChildren = ImmutableList.of(this.inner);
    }

    @Override
    @Nonnull
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return expressionChildren.iterator();
    }

    @Override
    @Nonnull
    public Collection<String> getRecordTypes() {
        return recordTypes;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    public RelationalPlannerExpression getInner() {
        return inner.get();
    }
}
