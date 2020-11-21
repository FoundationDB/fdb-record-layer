/*
 * PredicateCountProperty.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicate;
import com.apple.foundationdb.record.query.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.predicates.NotPredicate;
import com.apple.foundationdb.record.query.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.predicates.QueryComponentPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A property that counts the number of {@link com.apple.foundationdb.record.query.predicates.ValuePredicate}s that appear in a planner expression tree.
 * This property is used as the number of "unsatisfied filters" when picking between query plans that scan different
 * indexes.
 */
@API(API.Status.EXPERIMENTAL)
public class PredicateCountProperty implements PlannerProperty<Integer> {
    private static final PredicateCountProperty INSTANCE = new PredicateCountProperty();

    @Nonnull
    @Override
    public Integer evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Integer> childResults) {
        int total = 0;
        if (expression instanceof RelationalExpressionWithPredicate) {
            total = getValuePredicateCount(((RelationalExpressionWithPredicate)expression).getPredicate());
        }
        for (Integer childCount : childResults) {
            if (childCount != null) {
                total += childCount;
            }
        }
        return total;
    }

    private static int getValuePredicateCount(@Nonnull QueryPredicate predicate) {
        if (predicate instanceof PredicateWithValue ||
                predicate instanceof QueryComponentPredicate) {
            return 1;
        } else if (predicate instanceof NotPredicate) {
            return getValuePredicateCount(((NotPredicate)predicate).getChild());
        } else if (predicate instanceof AndOrPredicate) {
            return ((AndOrPredicate)predicate).getChildren().stream()
                    .mapToInt(PredicateCountProperty::getValuePredicateCount)
                    .sum();
        } else if (predicate instanceof ExistsPredicate) {
            // exists() should not count as it would only reapply the predicates that are underneath using its
            // alternative expression
            return 0;
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
