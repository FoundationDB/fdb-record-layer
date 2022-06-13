/*
 * FieldWithComparisonCountProperty.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.ComponentWithChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithNoChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithSingleChild;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A property that counts the number of {@link FieldWithComparison}s that appear in a planner expression tree.
 * This property is used as the number of "unsatisfied filters" when picking between query plans that scan different
 * indexes.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldWithComparisonCountProperty implements ExpressionProperty<Integer>, RelationalExpressionVisitorWithDefaults<Integer> {
    private static final FieldWithComparisonCountProperty INSTANCE = new FieldWithComparisonCountProperty();

    @Nonnull
    @Override
    public Integer evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Integer> childResults) {
        int total = 0;
        if (expression instanceof RecordQueryFilterPlan) {
            QueryComponent filter = ((RecordQueryFilterPlan)expression).getConjunctedFilter();
            total = getFieldWithComparisonCount(filter);
        }

        for (Integer childCount : childResults) {
            if (childCount != null) {
                total += childCount;
            }
        }
        return total;
    }

    private static int getFieldWithComparisonCount(@Nonnull QueryComponent component) {
        if (component instanceof FieldWithComparison)  {
            return 1;
        } else if (component instanceof ComponentWithNoChildren) {
            return 0;
        } else if (component instanceof ComponentWithSingleChild) {
            return getFieldWithComparisonCount(((ComponentWithSingleChild)component).getChild());
        } else if (component instanceof ComponentWithChildren) {
            return ((ComponentWithChildren)component).getChildren().stream()
                    .mapToInt(FieldWithComparisonCountProperty::getFieldWithComparisonCount)
                    .sum();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Nonnull
    @Override
    public Integer evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Integer> memberResults) {
        int min = Integer.MAX_VALUE;
        for (int memberResult : memberResults) {
            if (memberResult < min) {
                min = memberResult;
            }
        }
        return min;
    }

    public static int evaluate(ExpressionRef<? extends RelationalExpression> ref) {
        return ref.acceptPropertyVisitor(INSTANCE);
    }

    public static int evaluate(@Nonnull RelationalExpression expression) {
        Integer result = expression.acceptPropertyVisitor(INSTANCE);
        if (result == null) {
            return Integer.MAX_VALUE;
        }
        return result;
    }
}
