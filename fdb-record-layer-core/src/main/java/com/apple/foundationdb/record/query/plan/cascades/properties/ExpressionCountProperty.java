/*
 * ExpressionCountProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * A property that determines the sum, over all elements of a {@code PlannerExpression} tree, of the number of occurrences
 * of specific types of {@link RelationalExpression}.
 *
 * <p>
 * This property provides some heuristic sense of how much work is being done by a plan.
 * </p>
 */
public class ExpressionCountProperty implements ExpressionProperty<Map<Class<? extends RelationalExpression>, Integer>>, RelationalExpressionVisitorWithDefaults<Map<Class<? extends RelationalExpression>, Integer>> {
    private final Set<Class<? extends RelationalExpression>> expressionClasses;

    public ExpressionCountProperty(final Set<Class<? extends RelationalExpression>> expressionClasses) {
        this.expressionClasses =
                ImmutableSet.copyOf(expressionClasses);
    }

    @Nonnull
    @Override
    public Map<Class<? extends RelationalExpression>, Integer> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Map<Class<? extends RelationalExpression>, Integer>> childResults) {
        final Map<Class<? extends RelationalExpression>, Integer> currentMap = Maps.newHashMap();
        for (final Class<? extends RelationalExpression> expressionClass : expressionClasses) {
            if (expressionClass.isInstance(expression)) {
                currentMap.compute(expressionClass,
                        (clazz, oldCount) -> (oldCount == null ? 0 : oldCount) + 1);
            }
        }

        return mergeMaps(Iterables.concat(childResults, ImmutableList.of(currentMap)),
                0,
                (o, n) -> {
                    if (o == null) {
                        return n;
                    }
                    if (n == null) {
                        return o;
                    }

                    return o + n;
                });
    }

    @Nonnull
    @Override
    public Map<Class<? extends RelationalExpression>, Integer> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Map<Class<? extends RelationalExpression>, Integer>> memberResults) {
        return mergeMaps(memberResults,
                Integer.MAX_VALUE,
                (o, n) -> {
                    if (o == null) {
                        return n;
                    }
                    if (n == null) {
                        return o;
                    }

                    return Math.min(o, n);
                });
    }

    @SuppressWarnings({"SameParameterValue", "java:S4276"})
    private Map<Class<? extends RelationalExpression>, Integer> mergeMaps(@Nonnull Iterable<Map<Class<? extends RelationalExpression>, Integer>> childResults,
                                                                          @Nullable Integer identity,
                                                                          @Nonnull BinaryOperator<Integer> mergeOperator) {
        final ImmutableMap.Builder<Class<? extends RelationalExpression>, Integer> resultMap = ImmutableMap.builder();
        for (final Class<? extends RelationalExpression> expressionClass : expressionClasses) {
            Integer accumulated = identity;
            for (final Map<Class<? extends RelationalExpression>, Integer> childResult : childResults) {
                accumulated = mergeOperator.apply(accumulated, childResult.get(expressionClass));
            }
            if (accumulated != null) {
                resultMap.put(expressionClass, accumulated);
            }
        }
        return resultMap.build();
    }

    public static int count(@Nonnull Class<? extends RelationalExpression> expressionClass, @Nonnull RelationalExpression expression) {
        return count(ImmutableSet.of(expressionClass), expression);
    }

    public static int count(@Nonnull Set<Class<? extends RelationalExpression>> expressionClasses, @Nonnull RelationalExpression expression) {
        final Map<Class<? extends RelationalExpression>, Integer> expressionClassToCountMap = new ExpressionCountProperty(expressionClasses).visit(expression);
        if (expressionClassToCountMap == null) {
            return 0;
        }
        return expressionClassToCountMap.values().stream().mapToInt(count -> count == null ? 0 : count).sum();
    }

    @Nonnull
    public static Map<Class<? extends RelationalExpression>, Integer> evaluate(@Nonnull Set<Class<? extends RelationalExpression>> expressionClasses, @Nonnull RelationalExpression expression) {
        @Nullable final Map<Class<? extends RelationalExpression>, Integer> nullableResult =
                expression.acceptPropertyVisitor(new ExpressionCountProperty(expressionClasses));
        return nullableResult == null ? ImmutableMap.of() : nullableResult;
    }
}
