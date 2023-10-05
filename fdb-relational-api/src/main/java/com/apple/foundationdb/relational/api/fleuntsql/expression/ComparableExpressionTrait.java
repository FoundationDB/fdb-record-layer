/*
 * ComparableExpressionTrait.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.fleuntsql.expression;

import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.List;

/**
 * This represents a comparable expression.
 *
 * @param <P> The data type of the comparable expression.
 * @param <T> The type of argument expression(s).
 */
@Immutable
public interface ComparableExpressionTrait<P extends DataType, T extends Expression<P>> extends ScalarExpression<P> {
    @Nonnull
    default BooleanExpressionTrait isEqualTo(@Nonnull final T right) {
        return new BooleanFunction(Operation.EQUAL, List.of(this, right));
    }

    @Nonnull
    default BooleanExpressionTrait notEquals(@Nonnull final T right) {
        return new BooleanFunction(Operation.NOT_EQUAL, List.of(this, right));
    }

    @Nonnull
    default BooleanExpressionTrait greaterThan(@Nonnull final T right) {
        return new BooleanFunction(Operation.GREATER_THAN, List.of(this, right));
    }

    @Nonnull
    default BooleanExpressionTrait greaterThanOrEquals(@Nonnull final T right) {
        return new BooleanFunction(Operation.GREATER_THAN_EQUALS, List.of(this, right));
    }

    @Nonnull
    default BooleanExpressionTrait lessThan(@Nonnull final T right) {
        return new BooleanFunction(Operation.LESS_THAN, List.of(this, right));
    }

    @Nonnull
    default BooleanExpressionTrait lessThanOrEqual(@Nonnull final T right) {
        return new BooleanFunction(Operation.LESS_THAN_EQUALS, List.of(this, right));
    }

    @Nonnull
    default ComparableExpressionTrait<P, T> greatest(@Nonnull final List<ComparableExpressionTrait<P, T>> arguments) {
        if (arguments.isEmpty()) {
            return this;
        }
        final ImmutableList.Builder<ComparableExpressionTrait<P, T>> allArgumentsBuilder = ImmutableList.builder();
        allArgumentsBuilder.add(this);
        allArgumentsBuilder.addAll(arguments);
        final var allArguments = allArgumentsBuilder.build();
        for (final var argument : allArguments) {
            Assert.thatUnchecked(argument.getType().getCode().equals(allArguments.get(0).getType().getCode()),
                    "all items must have the same type");
        }
        return new ComparableFunction<>(arguments.get(0).getType(), Operation.GREATEST, allArguments);
    }

    @Override
    P getType();
}
