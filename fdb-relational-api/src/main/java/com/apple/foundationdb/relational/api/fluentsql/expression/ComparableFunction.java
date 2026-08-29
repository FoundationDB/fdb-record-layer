/*
 * ComparableFunction.java
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

package com.apple.foundationdb.relational.api.fluentsql.expression;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.fluentsql.FluentVisitor;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a comparable function.
 *
 * @param <P> The data type of the comparable expression.
 * @param <T> The type of argument expression(s).
 */
@Immutable
@API(API.Status.EXPERIMENTAL)
public class ComparableFunction<P extends DataType, T extends Expression<P>> implements ComparableExpressionTrait<P, T>, FunctionLike<P> {

    private final Operation operator;

    private final ImmutableList<Expression<P>> args;

    private final P type;

    public ComparableFunction(P type, Operation operator, ImmutableList<ComparableExpressionTrait<P, T>> args) {
        this.type = type;
        this.operator = operator;
        this.args = ImmutableList.copyOf(args);
    }

    @Nullable
    @Override
    public <R, C> R accept(FluentVisitor<R, C> visitor, C context) {
        return visitor.visit((FunctionLike<?>) this, context);
    }

    @Override
    public P getType() {
        return type;
    }

    @Override
    public Iterable<Expression<?>> getArguments() {
        return ImmutableList.copyOf(args);
    }

    @Override
    public Operation getName() {
        return operator;
    }

    @Override
    public String toString() {
        return operator + "(" + args.stream().map(Object::toString).collect(Collectors.joining(",")) + ") : " + type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ComparableFunction<?, ?> that = (ComparableFunction<?, ?>) o;
        return operator == that.operator && Objects.equals(args, that.args) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, args, type);
    }
}
