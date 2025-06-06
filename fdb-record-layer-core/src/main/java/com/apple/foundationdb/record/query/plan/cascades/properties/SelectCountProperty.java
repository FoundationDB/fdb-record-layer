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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A property representing the minimum depth of any of a set of relational planner expression types in a relational
 * planner expression: that is, the smallest integer such that one of those types is exactly that many relational
 * planner expressions away from the root expression.
 */
public class SelectCountProperty implements ExpressionProperty<Integer> {
    private static final SelectCountProperty SELECT_COUNT = new SelectCountProperty();

    private SelectCountProperty() {
    }

    @Nonnull
    @Override
    public SelectCountVisitor createVisitor() {
        return new SelectCountVisitor();
    }

    public int evaluate(@Nonnull final Reference reference) {
        return evaluate(reference.get());
    }

    public int evaluate(@Nonnull final RelationalExpression expression) {
        return Objects.requireNonNull(createVisitor().visit(expression));
    }

    @Nonnull
    public static SelectCountProperty selectCount() {
        return SELECT_COUNT;
    }

    public static class SelectCountVisitor implements RelationalExpressionVisitorWithDefaults<Integer> {
        @Nonnull
        @Override
        public Integer visitSelectExpression(@Nonnull final SelectExpression selectExpression) {
            return visitDefault(selectExpression) + 1;
        }

        @Nonnull
        @Override
        public Integer visitDefault(@Nonnull final RelationalExpression expression) {
            return fromChildren(expression).stream().mapToInt(Integer::intValue).sum();
        }

        @Nonnull
        private List<Integer> fromChildren(@Nonnull final RelationalExpression expression) {
            return expression.getQuantifiers()
                    .stream()
                    .map(quantifier -> forReference(quantifier.getRangesOver()))
                    .collect(ImmutableList.toImmutableList());
        }

        private int forReference(@Nonnull Reference reference) {
            final var memberResults =
                    reference.getPropertyForExpressions(SELECT_COUNT).values();
            Verify.verify(memberResults.size() == 1);
            return Iterables.getOnlyElement(memberResults);
        }
    }
}
