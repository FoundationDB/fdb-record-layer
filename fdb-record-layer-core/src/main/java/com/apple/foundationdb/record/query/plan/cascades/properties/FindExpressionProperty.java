/*
 * FindExpressionProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.google.common.base.Verify;
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

/**
 * A property that finds all occurrences of expressions of interest in a planner graph.
 */
public class FindExpressionProperty implements ExpressionProperty<Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>>, RelationalExpressionVisitorWithDefaults<Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> {
    private final Set<Class<? extends RelationalExpression>> expressionClasses;

    public FindExpressionProperty(@Nonnull final Set<Class<? extends RelationalExpression>> expressionClasses) {
        this.expressionClasses =
                ImmutableSet.copyOf(expressionClasses);
    }

    @Nonnull
    @Override
    public Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> childResults) {
        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> currentMap = Maps.newHashMap();
        for (final Class<? extends RelationalExpression> expressionClass : expressionClasses) {
            currentMap.compute(expressionClass,
                    (clazz, oldSet) -> {
                        if (expressionClass.isInstance(expression)) {
                            if (oldSet == null) {
                                return LinkedIdentitySet.of(expression);
                            } else {
                                oldSet.add(expression);
                                return oldSet;
                            }
                        } else {
                            return oldSet == null ? LinkedIdentitySet.of() : oldSet;
                        }
                    });
        }

        return mergeMaps(Iterables.concat(childResults, ImmutableList.of(currentMap)));
    }

    @Nonnull
    @Override
    public Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> memberResults) {
        Verify.verify(memberResults.size() == 1);
        return Iterables.getOnlyElement(memberResults);
    }

    @SuppressWarnings({"SameParameterValue", "java:S4276"})
    private Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> mergeMaps(@Nonnull final Iterable<Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> childResults) {
        final ImmutableMap.Builder<Class<? extends RelationalExpression>, Set<RelationalExpression>> resultMap = ImmutableMap.builder();
        for (final Class<? extends RelationalExpression> expressionClass : expressionClasses) {
            final Set<RelationalExpression> accumulated = new LinkedIdentitySet<>();
            for (final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> childResult : childResults) {
                final Set<RelationalExpression> childResultForClass = childResult.get(expressionClass);
                if (childResultForClass != null) {
                    accumulated.addAll(childResultForClass);
                }
            }
            resultMap.put(expressionClass, LinkedIdentitySet.copyOf(accumulated));
        }
        return resultMap.build();
    }

    public static Set<? extends RelationalExpression> findExpressions(@Nonnull final Class<? extends RelationalExpression> expressionClass, @Nonnull final RelationalExpression expression) {
        return findExpressions(ImmutableSet.of(expressionClass), expression);
    }

    public static Set<? extends RelationalExpression> findExpressions(@Nonnull final Set<Class<? extends RelationalExpression>> expressionClasses, @Nonnull final RelationalExpression expression) {
        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> expressionClassToExpressionsMap = new FindExpressionProperty(expressionClasses).visit(expression);
        if (expressionClassToExpressionsMap == null) {
            return LinkedIdentitySet.of();
        }

        final Set<RelationalExpression> accumulated = new LinkedIdentitySet<>();
        for (final Set<RelationalExpression> value : expressionClassToExpressionsMap.values()) {
            accumulated.addAll(value);
        }
        return accumulated;
    }

    @SafeVarargs
    public static Set<? extends RelationalExpression> slice(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> inMap, @Nonnull final Class<? extends RelationalExpression>... expressionClasses) {
        final Set<RelationalExpression> accumulated = new LinkedIdentitySet<>();
        for (final Class<? extends RelationalExpression> expressionClass : expressionClasses) {
            final Set<RelationalExpression> childResultForClass = inMap.get(expressionClass);
            if (childResultForClass != null) {
                accumulated.addAll(childResultForClass);
            }
        }

        return accumulated;
    }

    public static int countExpressions(@Nonnull final Class<? extends RelationalExpression> expressionClass, @Nonnull final RelationalExpression expression) {
        return countExpressions(ImmutableSet.of(expressionClass), expression);
    }

    public static int countExpressions(@Nonnull final Set<Class<? extends RelationalExpression>> expressionClasses, @Nonnull final RelationalExpression expression) {
        return findExpressions(expressionClasses, expression).size();
    }

    @Nonnull
    public static Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> evaluate(@Nonnull Set<Class<? extends RelationalExpression>> expressionClasses, @Nonnull RelationalExpression expression) {
        @Nullable final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> nullableResult =
                expression.acceptPropertyVisitor(new FindExpressionProperty(expressionClasses));
        return nullableResult == null ? ImmutableMap.of() : nullableResult;
    }
}
