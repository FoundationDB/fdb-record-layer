/*
 * TypeFilterCountProperty.java
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

import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.TypeFilterExpression;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A property that determines the sum, over all elements of a {@code PlannerExpression} tree, of the number of record
 * types that are passed by type filters in the tree. Records that are filtered for more than one type filter are
 * not de-duplicated. For example, if there are two {@link TypeFilterExpression}s, one that filters for records
 * of types {@code Type1} or {@code Type2} and one that filters for records of type {@code Type1}, then this property
 * would evaluate to 3.
 *
 * <p>
 * This property provides some heuristic sense of how much work is being done by type filters, since unnecessary ones
 * are aggressively pruned by the planner.
 * </p>
 */
public class TypeFilterCountProperty implements PlannerProperty<Integer> {
    private static final TypeFilterCountProperty INSTANCE = new TypeFilterCountProperty();

    @Nonnull
    @Override
    public Integer evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Integer> childResults) {
        int total = expression instanceof TypeFilterExpression ?
                    ((TypeFilterExpression)expression).getRecordTypes().size() : 0;
        for (Integer childCount : childResults) {
            if (childCount != null) {
                total += childCount;
            }
        }
        return total;
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

    public static int evaluate(@Nonnull RelationalExpression expression) {
        Integer result = expression.acceptPropertyVisitor(INSTANCE);
        if (result == null) {
            return 0;
        }
        return result;
    }
}
