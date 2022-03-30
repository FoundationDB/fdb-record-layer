/*
 * UsedTypesProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.Type;
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

        resultBuilder.addAll(expression.getDynamicTypes());

        return resultBuilder.build();
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
