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
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

public class PredicateCountProperty implements ExpressionProperty<Integer> {
    private static final PredicateCountProperty PREDICATE_COUNT = new PredicateCountProperty();

    private PredicateCountProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public RelationalExpressionVisitor<Integer> createVisitor() {
        return PredicateCountVisitor.VISITOR;
    }

    public int evaluate(RelationalExpression expression) {
        return createVisitor().visit(expression);
    }

    @Nonnull
    public static PredicateCountProperty predicateCount() {
        return PREDICATE_COUNT;
    }

    private static final class PredicateCountVisitor implements SimpleExpressionVisitor<Integer> {
        private static final PredicateCountVisitor VISITOR = new PredicateCountVisitor();

        @Nonnull
        @Override
        public Integer evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<Integer> childResults) {
            final var childPredicates = childResults.stream().mapToInt(Integer::intValue).sum();
            if (!(expression instanceof RelationalExpressionWithPredicates)) {
                return childPredicates;
            }
            return childPredicates + ((RelationalExpressionWithPredicates)expression).getPredicates().size();
        }

        @Nonnull
        @Override
        public Integer evaluateAtRef(@Nonnull final Reference ref, @Nonnull final List<Integer> memberResults) {
            if (memberResults.size() == 1) {
                return Iterables.getOnlyElement(memberResults);
            }
            return memberResults.stream().mapToInt(Integer::intValue).max().orElse(0);
        }
    }
}
