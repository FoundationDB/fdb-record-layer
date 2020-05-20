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

package com.apple.foundationdb.record.query.plan.temp.properties;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A property representing the minimum depth of any of a set of relational planner expression types in a relational
 * planner expression: that is, the smallest integer such that one of those types is exactly that many relational
 * planner expressions away from the root expression.
 */
public class RelationalExpressionDepthProperty implements PlannerProperty<Integer> {
    public static final RelationalExpressionDepthProperty TYPE_FILTER_DEPTH = new RelationalExpressionDepthProperty(
            ImmutableSet.of(LogicalTypeFilterExpression.class, RecordQueryTypeFilterPlan.class));
    public static final RelationalExpressionDepthProperty DISTINCT_FILTER_DEPTH = new RelationalExpressionDepthProperty(
            ImmutableSet.of(LogicalDistinctExpression.class, RecordQueryUnorderedPrimaryKeyDistinctPlan.class));

    @Nonnull
    private final Set<Class<? extends RelationalExpression>> types;

    public RelationalExpressionDepthProperty(@Nonnull Set<Class<? extends RelationalExpression>> types) {
        this.types = types;
    }

    @Override
    public boolean shouldVisit(@Nonnull RelationalExpression expression) {
        return true;
    }

    @Override
    public boolean shouldVisit(@Nonnull ExpressionRef<? extends RelationalExpression> ref) {
        return true;
    }

    @Override
    public boolean shouldVisit(@Nonnull final Quantifier quantifier) {
        return true;
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

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public Integer evaluateAtQuantifier(@Nonnull final Quantifier quantifier, @Nullable final Integer rangesOverResult) {
        // since we do visit expression references in this property we can insist on rangesOverResult not being null
        return Objects.requireNonNull(rangesOverResult);
    }
}
