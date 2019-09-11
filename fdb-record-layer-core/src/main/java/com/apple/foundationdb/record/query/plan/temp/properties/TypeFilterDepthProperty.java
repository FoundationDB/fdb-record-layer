/*
 * TypeFilterDepthProperty.java
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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * A property representing the minimum depth of a type filter in a relational planner expression: that is, the smallest
 * integer such that there is a {@link RecordQueryTypeFilterPlan} or {@link LogicalTypeFilterExpression} exactly that
 * many relational planner expressions away from the root expression.
 */
public class TypeFilterDepthProperty implements PlannerProperty<Integer> {
    private static final TypeFilterDepthProperty INSTANCE = new TypeFilterDepthProperty();

    @Override
    public boolean shouldVisit(@Nonnull PlannerExpression expression) {
        return expression instanceof RelationalPlannerExpression;
    }

    @Override
    public boolean shouldVisit(@Nonnull ExpressionRef<? extends PlannerExpression> ref) {
        return true;
    }

    @Nonnull
    @Override
    public Integer evaluateAtExpression(@Nonnull PlannerExpression expression, @Nonnull List<Integer> childResults) {
        if (expression instanceof RecordQueryTypeFilterPlan ||
                expression instanceof LogicalTypeFilterExpression) {
            return 0;
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
    public Integer evaluateAtRef(@Nonnull ExpressionRef<? extends PlannerExpression> ref, @Nonnull List<Integer> memberResults) {
        return Collections.min(memberResults);
    }

    public static int evaluate(@Nonnull PlannerExpression expression) {
        return expression.acceptPropertyVisitor(INSTANCE);
    }

}
