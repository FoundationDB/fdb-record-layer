/*
 * ExpressionCountProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public final class ExpressionCountProperty implements ExpressionProperty<Integer> {
    @Nonnull
    private static final ExpressionCountProperty TABLE_FUNCTION_COUNT = ExpressionCountProperty.ofTypes(
            ImmutableSet.of(TableFunctionExpression.class));

    private final Predicate<? super RelationalExpression> filter;

    private ExpressionCountProperty(@Nonnull Predicate<? super RelationalExpression> filter) {
        this.filter = filter;
    }

    @Nonnull
    @Override
    public RelationalExpressionVisitor<Integer> createVisitor() {
        return new ExpressionCountVisitor(filter);
    }

    public int evaluate(RelationalExpression expression) {
        return createVisitor().visit(expression);
    }

    @Nonnull
    public static ExpressionCountProperty tableFunctionCount() {
        return TABLE_FUNCTION_COUNT;
    }

    private static final class ExpressionCountVisitor implements SimpleExpressionVisitor<Integer> {
        @Nonnull
        private final Predicate<? super RelationalExpression> filter;

        private ExpressionCountVisitor(@Nonnull Predicate<? super RelationalExpression> filter) {
            this.filter = filter;
        }

        @Nonnull
        @Override
        public Integer evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<Integer> childResults) {
            int fromCurrent = filter.test(expression) ? 1 : 0;
            return fromCurrent + childResults.stream().mapToInt(Number::intValue).sum();
        }

        @Nonnull
        @Override
        public Integer evaluateAtRef(@Nonnull final Reference ref, @Nonnull final List<Integer> memberResults) {
            return memberResults.stream().mapToInt(Number::intValue).min().orElse(Integer.MAX_VALUE);
        }

    }

    @Nonnull
    private static ExpressionCountProperty ofTypes(Set<Class<? extends RelationalExpression>> expressionTypes) {
        return new ExpressionCountProperty(expr -> expressionTypes.stream().anyMatch(type -> type.isInstance(expr)));
    }
}
