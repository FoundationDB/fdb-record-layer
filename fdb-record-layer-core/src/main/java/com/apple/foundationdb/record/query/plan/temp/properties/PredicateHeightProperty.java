/*
 * PredicateHeightProperty.java
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

import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A property representing the maximum height of any of the {@link QueryComponent} trees in the given expression.
 *
 * <p>
 * For a {@link com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression}, this is the
 * maximum among all {@code QueryComponent}s present in the tree rooted at this expression. For a {@code QueryComponent},
 * this is the maximum among all {@code QueryComponent} trees rooted at this expression.
 * </p>
 */
public class PredicateHeightProperty implements PlannerProperty<Integer> {
    private static final PredicateHeightProperty INSTANCE = new PredicateHeightProperty();

    @Override
    public boolean shouldVisit(@Nonnull PlannerExpression expression) {
        return true;
    }

    @Override
    public boolean shouldVisit(@Nonnull ExpressionRef<? extends PlannerExpression> ref) {
        return true;
    }

    @Nonnull
    @Override
    public Integer evaluateAtExpression(@Nonnull PlannerExpression expression, @Nonnull List<Integer> childResults) {
        int maxChildDepth = 0;
        for (Integer childDepth : childResults) {
            if (childDepth != null && childDepth > maxChildDepth) {
                maxChildDepth = childDepth;
            }
        }
        return maxChildDepth + (expression instanceof QueryComponent ? 1 : 0);
    }

    @Nonnull
    @Override
    public Integer evaluateAtRef(@Nonnull ExpressionRef<? extends PlannerExpression> ref, @Nonnull List<Integer> memberResults) {
        int maxResultDepth = 0;
        for (Integer memberDepth : memberResults) {
            if (memberDepth != null && memberDepth > maxResultDepth) {
                maxResultDepth = memberDepth;
            }
        }
        return maxResultDepth;
    }

    public static int evaluate(@Nonnull PlannerExpression expression) {
        Integer result = expression.acceptPropertyVisitor(INSTANCE);
        if (result == null) {
            return 0;
        }
        return result;
    }
}
