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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A property that computes the set of complex (dynamic) types that is used by the graph passed in.
 */
@API(API.Status.EXPERIMENTAL)
public class UsedTypesProperty implements ExpressionProperty<Set<Type>> {
    private static final UsedTypesProperty USED_TYPES = new UsedTypesProperty();

    private UsedTypesProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public UsedTypesVisitor createVisitor() {
        return new UsedTypesVisitor();
    }

    public Set<Type> evaluate(@Nonnull final Reference ref) {
        return Objects.requireNonNull(ref.acceptVisitor(createVisitor()));
    }

    public Set<Type> evaluate(@Nonnull final RelationalExpression expression) {
        return Objects.requireNonNull(expression.acceptVisitor(createVisitor()));
    }

    @Nonnull
    public static UsedTypesProperty usedTypes() {
        return USED_TYPES;
    }

    public static class UsedTypesVisitor implements SimpleExpressionVisitor<Set<Type>> {
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
        public Set<Type> evaluateAtRef(@Nonnull Reference ref, @Nonnull List<Set<Type>> memberResults) {
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
    }
}
