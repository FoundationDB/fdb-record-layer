/*
 * RelationalExpressionDepthProperty.java
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A property representing the minimum depth of any of a set of relational planner expression types in a relational
 * planner expression: that is, the smallest integer such that one of those types is exactly that many relational
 * planner expressions away from the root expression.
 */
public class RelationalExpressionDepthProperty implements ExpressionProperty<Integer>, RelationalExpressionVisitorWithDefaults<Integer> {
    public static final RelationalExpressionDepthProperty TYPE_FILTER_DEPTH = new RelationalExpressionDepthProperty(
            ImmutableSet.of(LogicalTypeFilterExpression.class, RecordQueryTypeFilterPlan.class));
    public static final RelationalExpressionDepthProperty DISTINCT_FILTER_DEPTH = new RelationalExpressionDepthProperty(
            ImmutableSet.of(LogicalDistinctExpression.class, RecordQueryUnorderedPrimaryKeyDistinctPlan.class));
    public static final RelationalExpressionDepthProperty FETCH_DEPTH = new RelationalExpressionDepthProperty(
            ImmutableSet.of(RecordQueryFetchFromPartialRecordPlan.class, RecordQueryPlanWithIndex.class));

    @Nonnull
    private final Set<Class<? extends RelationalExpression>> types;

    public RelationalExpressionDepthProperty(@Nonnull Set<Class<? extends RelationalExpression>> types) {
        this.types = types;
    }

    @Nonnull
    @Override
    public Integer evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Integer> childResults) {
        for (Class<? extends RelationalExpression> type : types) {
            if (type.isInstance(expression)) {
                return 0;
            }
        }

        int min = Integer.MAX_VALUE;
        for (Integer result : childResults) {
            if (result != null && result < min) {
                min = result;
            }
        }
        return min == Integer.MAX_VALUE ? Integer.MAX_VALUE : min + 1;
    }

    @Nonnull
    @Override
    public Integer evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Integer> memberResults) {
        return Collections.min(memberResults);
    }

    public int evaluate(@Nonnull RelationalExpression expression) {
        return expression.acceptPropertyVisitor(this);
    }
}
