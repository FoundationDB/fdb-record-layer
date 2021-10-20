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
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.predicates.CreatesDynamicTypesValue;
import com.apple.foundationdb.record.query.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A property that computes the set of complex (dynamic) types that is used by the graph passed in.
 */
@API(API.Status.EXPERIMENTAL)
public class UsedTypesProperty implements PlannerProperty<Set<Type>> {
    private static final UsedTypesProperty INSTANCE = new UsedTypesProperty();

    @Nonnull
    @Override
    public Set<Type> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Set<Type>> childResults) {
        final ImmutableSet.Builder<Type> resultBuilder = ImmutableSet.builder();
        for (final Set<Type> childResult : childResults) {
            resultBuilder.addAll(childResult);
        }

        if (expression instanceof RelationalExpressionWithPredicates) {
            final List<? extends QueryPredicate> predicates = ((RelationalExpressionWithPredicates)expression).getPredicates();

            for (final QueryPredicate predicate : predicates) {
                final Set<Type> typesForPredicate =
                        predicate.fold(p -> {
                            if (p instanceof PredicateWithValue) {
                                return typesForValue(((PredicateWithValue)p).getValue());
                            }
                            return ImmutableSet.<Type>of();
                        }, (thisTypes, childTypeSets) -> {
                            final ImmutableSet.Builder<Type> nestedBuilder = ImmutableSet.builder();
                            for (final Set<Type> childTypes : childTypeSets) {
                                nestedBuilder.addAll(childTypes);
                            }
                            nestedBuilder.addAll(thisTypes);
                            return nestedBuilder.build();
                        });
                resultBuilder.addAll(typesForPredicate);
            }
        }

        resultBuilder.addAll(typesForValue(expression.getResultValue()));

        return resultBuilder.build();
    }

    @Nonnull
    private static Set<Type> typesForValue(@Nonnull final Value value) {
        return value.fold(p -> {
            if (p instanceof CreatesDynamicTypesValue) {
                return ImmutableSet.of(p.getResultType());
            }
            return ImmutableSet.<Type>of();
        }, (thisTypes, childTypeSets) -> {
            final ImmutableSet.Builder<Type> nestedBuilder = ImmutableSet.builder();
            for (final Set<Type> childTypes : childTypeSets) {
                nestedBuilder.addAll(childTypes);
            }
            nestedBuilder.addAll(thisTypes);
            return nestedBuilder.build();
        });
    }

    @Nonnull
    @Override
    public Set<Type> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Set<Type>> memberResults) {
        return unionTypes(memberResults);
    }

    @Nonnull
    private static Set<Type> unionTypes(@Nonnull final Collection<Set<Type>> types) {
        final ImmutableSet.Builder<Type> resultBuilder = ImmutableSet.builder();
        for (final Set<Type> childResult : types) {
            resultBuilder.addAll(childResult);
        }
        return resultBuilder.build();
    }

    public static Set<Type> evaluate(ExpressionRef<? extends RelationalExpression> ref) {
        return ref.acceptPropertyVisitor(INSTANCE);
    }

    public static Set<Type> evaluate(@Nonnull RelationalExpression expression) {
        final Set<Type> result = expression.acceptPropertyVisitor(INSTANCE);
        if (result == null) {
            return ImmutableSet.of();
        }
        return result;
    }
}
