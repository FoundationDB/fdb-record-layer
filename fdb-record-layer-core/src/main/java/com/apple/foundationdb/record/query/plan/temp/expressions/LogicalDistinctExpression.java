/*
 * LogicalDistinctPlan.java
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
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * A relational planner expression representing a stream of unique records. This expression has a single child which
 * is also a {@link RelationalExpression}. This expression represents this underlying expression with its result
 * set de-duplicated.
 *
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalDistinctExpression implements RelationalExpressionWithChildren {
    @Nonnull
    private ExpressionRef<RelationalExpression> inner;

    public LogicalDistinctExpression(@Nonnull RelationalExpression inner) {
        this(GroupExpressionRef.of(inner));
    }

    public LogicalDistinctExpression(@Nonnull ExpressionRef<RelationalExpression> inner) {
        this.inner = inner;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends RelationalExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(inner);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        return otherExpression instanceof LogicalDistinctExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalDistinctExpression that = (LogicalDistinctExpression)o;
        return inner.equals(that.inner);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}
