/*
 * ExpressionDepthProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A property representing the minimum depth of any of a set of relational planner expression types in a relational
 * planner expression: that is, the smallest integer such that one of those types is exactly that many relational
 * planner expressions away from the root expression.
 */
public class ExpressionCountProperty implements ExpressionProperty<Integer> {
    @Nonnull
    private static final ExpressionCountProperty SELECT_COUNT = ofTrackedTypes(SelectExpression.class, LogicalFilterExpression.class);
    @Nonnull
    private static final ExpressionCountProperty TABLE_FUNCTION_COUNT = ofTrackedTypes(TableFunctionExpression.class);

    private final boolean isTracked;
    private final Predicate<? super RelationalExpression> filter;

    private ExpressionCountProperty(@Nonnull final Predicate<? super RelationalExpression> filter,
                                    final boolean isTracked) {
        this.filter = filter;
        this.isTracked = isTracked;
    }

    @Nonnull
    @Override
    public ExpressionCountVisitor createVisitor() {
        return new ExpressionCountVisitor(filter, this);
    }

    public int evaluate(@Nonnull final Reference reference) {
        return evaluate(reference.get());
    }

    public int evaluate(@Nonnull final RelationalExpression expression) {
        return Objects.requireNonNull(createVisitor().visit(expression));
    }

    @Nonnull
    public static ExpressionCountProperty selectCount() {
        return SELECT_COUNT;
    }

    @Nonnull
    public static ExpressionCountProperty tableFunctionCount() {
        return TABLE_FUNCTION_COUNT;
    }

    public static class ExpressionCountVisitor implements RelationalExpressionVisitorWithDefaults<Integer> {
        @Nonnull
        private final Predicate<? super RelationalExpression> filter;
        @Nonnull
        private final ExpressionCountProperty property;

        private ExpressionCountVisitor(@Nonnull Predicate<? super RelationalExpression> filter,
                                       ExpressionCountProperty property) {
            this.filter = filter;
            this.property = property;
        }

        @Nonnull
        @Override
        public Integer visitDefault(@Nonnull final RelationalExpression expression) {
            return fromChildren(expression).stream().mapToInt(Integer::intValue).sum() +
                    (filter.test(expression) ? 1 : 0);
        }

        @Nonnull
        private List<Integer> fromChildren(@Nonnull final RelationalExpression expression) {
            return expression.getQuantifiers()
                    .stream()
                    .map(quantifier -> forReference(quantifier.getRangesOver()))
                    .collect(ImmutableList.toImmutableList());
        }

        private int forReference(@Nonnull final Reference reference) {
            final var finalExpressions = reference.getFinalExpressions();
            Verify.verify(finalExpressions.size() == 1);
            if (property.isTracked) {
                final var memberResults =
                        reference.getPropertyForExpressions(property).values();
                return Iterables.getOnlyElement(memberResults);
            }
            return visit(Iterables.getOnlyElement(finalExpressions));
        }
    }

    @Nonnull
    @SafeVarargs
    private static ExpressionCountProperty ofTrackedTypes(Class<? extends RelationalExpression>... expressionTypes) {
        return ofTypes(true, expressionTypes);
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    private static ExpressionCountProperty ofTypes(final boolean isTracked,
                                                   Class<? extends RelationalExpression>... expressionTypes) {
        return new ExpressionCountProperty(expr -> Arrays.stream(expressionTypes)
                .anyMatch(type -> type.isInstance(expr)), isTracked);
    }
}
