/*
 * PredicateCountProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

public final class PredicateCountProperty implements ExpressionProperty<Integer> {
    @Nonnull
    private static final PredicateCountProperty PREDICATE_COUNT = new PredicateCountProperty();

    private PredicateCountProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public PredicateCountVisitor createVisitor() {
        return PredicateCountVisitor.VISITOR;
    }

    public int evaluate(RelationalExpression expression) {
        return createVisitor().visit(expression);
    }

    @Nonnull
    public static PredicateCountProperty predicateCount() {
        return PREDICATE_COUNT;
    }

    public static final class PredicateCountVisitor implements RelationalExpressionVisitorWithDefaults<Integer> {
        @Nonnull
        private static final PredicateCountVisitor VISITOR = new PredicateCountVisitor(PredicateCountProperty.PREDICATE_COUNT);

        @Nonnull
        private final PredicateCountProperty property;

        private PredicateCountVisitor(@Nonnull final PredicateCountProperty property) {
            this.property = property;
        }

        @Nonnull
        @Override
        public Integer visitDefault(@Nonnull final RelationalExpression expression) {
            final var childResults = fromChildren(expression);
            final var childPredicatesCount = childResults.stream().mapToInt(Integer::intValue).sum();
            if (!(expression instanceof RelationalExpressionWithPredicates)) {
                return childPredicatesCount;
            }
            return childPredicatesCount + ((RelationalExpressionWithPredicates)expression).getPredicates().size();
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
            final var memberResults = reference.getPropertyForExpressions(property).values();
            return Iterables.getOnlyElement(memberResults);
        }
    }
}
