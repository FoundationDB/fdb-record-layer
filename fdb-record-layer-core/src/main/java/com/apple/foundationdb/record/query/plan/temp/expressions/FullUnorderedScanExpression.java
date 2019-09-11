/*
 * FullUnorderedScanExpression.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;

/**
 * A planner expression representing a full, unordered scan of the records by primary key, which is the logical version
 * of a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan} with
 * {@link com.apple.foundationdb.record.query.plan.ScanComparisons#EMPTY}. Unlike a
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan}, a {@code FullUnorderedScanExpression}
 * is not implicitly ordered by the primary key and thus can be nested/unnested freely with respect to any
 * {@link NestedContext}.
 *
 * <p>
 * This expression is useful as the source of records for the initial planner expression produced from a
 * {@link com.apple.foundationdb.record.query.RecordQuery}.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class FullUnorderedScanExpression implements RelationalPlannerExpression {
    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Collections.emptyIterator();
    }

    @Nullable
    @Override
    public ExpressionRef<RelationalPlannerExpression> asNestedWith(@Nonnull NestedContext nestedContext, @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef) {
        return thisRef;
    }

    @Nullable
    @Override
    public ExpressionRef<RelationalPlannerExpression> asUnnestedWith(@Nonnull NestedContext nestedContext, @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef) {
        return thisRef;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        return equals(otherExpression);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FullUnorderedScanExpression;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "FullUnorderedScan";
    }
}
