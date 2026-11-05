/*
 * CompileTimeOnlyValueCountProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.WithValue;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * The number of {@link Value.CompileTimeOnlyValue}s a {@link RelationalExpression} and everything underneath it
 * computes, counting the values in an expression's result value as well as those in its predicates. Such a value is a
 * placeholder that a rewrite has to eliminate -- it can be evaluated neither at runtime nor at compile-time -- so a
 * non-zero count means rewriting is unfinished.
 */
public class CompileTimeOnlyValueCountProperty implements ExpressionProperty<Integer> {
    @Nonnull
    private static final CompileTimeOnlyValueCountProperty COMPILE_TIME_ONLY_VALUE_COUNT =
            new CompileTimeOnlyValueCountProperty(true);

    private final boolean isTracked;

    private CompileTimeOnlyValueCountProperty(final boolean isTracked) {
        this.isTracked = isTracked;
    }

    @Nonnull
    @Override
    public CompileTimeOnlyValueCountVisitor createVisitor() {
        return new CompileTimeOnlyValueCountVisitor(this);
    }

    public int evaluate(@Nonnull final Reference reference) {
        return evaluate(reference.get());
    }

    public int evaluate(@Nonnull final RelationalExpression expression) {
        return Objects.requireNonNull(createVisitor().visit(expression));
    }

    @Nonnull
    public static CompileTimeOnlyValueCountProperty compileTimeOnlyValueCount() {
        return COMPILE_TIME_ONLY_VALUE_COUNT;
    }

    /**
     * The values the given expression computes itself: its result value together with the values its predicates compare.
     */
    @Nonnull
    private static Stream<Value> valuesOf(@Nonnull final RelationalExpression expression) {
        final var resultValue = Stream.<Value>of(expression.getResultValue());
        if (!(expression instanceof RelationalExpressionWithPredicates)) {
            return resultValue;
        }
        return Stream.concat(resultValue,
                ((RelationalExpressionWithPredicates)expression).getPredicates()
                        .stream()
                        .flatMap(CompileTimeOnlyValueCountProperty::predicateValues));
    }

    /**
     * The values the given predicate computes, looking inside compound predicates as well: a value can sit in either
     * conjunct of an {@code AND}.
     */
    @Nonnull
    private static Stream<Value> predicateValues(@Nonnull final QueryPredicate predicate) {
        return predicate.preOrderStream()
                .filter(WithValue.class::isInstance)
                .map(nestedPredicate -> (WithValue<?>)nestedPredicate)
                .map(WithValue::getValue)
                .filter(Objects::nonNull);
    }

    /**
     * Counts the compile-time-only values in an expression and, through the references it ranges over, in the entire
     * subtree underneath it.
     */
    public static class CompileTimeOnlyValueCountVisitor implements RelationalExpressionVisitorWithDefaults<Integer> {
        @Nonnull
        private final CompileTimeOnlyValueCountProperty property;

        private CompileTimeOnlyValueCountVisitor(@Nonnull final CompileTimeOnlyValueCountProperty property) {
            this.property = property;
        }

        @Nonnull
        @Override
        public Integer visitDefault(@Nonnull final RelationalExpression expression) {
            return countIn(expression) + fromChildren(expression).stream().mapToInt(Integer::intValue).sum();
        }

        private static int countIn(@Nonnull final RelationalExpression expression) {
            return (int)valuesOf(expression)
                    .flatMap(Value::preOrderStream)
                    .filter(Value.CompileTimeOnlyValue.class::isInstance)
                    .count();
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
}
