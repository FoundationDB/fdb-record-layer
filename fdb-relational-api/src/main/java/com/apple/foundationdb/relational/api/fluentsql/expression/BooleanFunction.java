/*
 * BooleanFunction.java
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

import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This represents a boolean function.
 */
@Immutable
@API(API.Status.EXPERIMENTAL)
public class BooleanFunction implements BooleanExpressionTrait, FunctionLike<DataType.BooleanType> {

    private final Operation operator;
    private final List<Expression<?>> args;

    public BooleanFunction(final Operation operator,
                           final List<Expression<?>> args) {
        this.operator = operator;
        this.args = ImmutableList.copyOf(args);
    }

    @Override
    @Nullable
    public <R, C> R accept(final FluentVisitor<R, C> visitor, final C context) {
        return visitor.visit(this, context);
    }

    @Override
    public Iterable<Expression<?>> getArguments() {
        return args;
    }

    @Override
    public Operation getName() {
        return operator;
    }

    @Override
    public DataType getType() {
        return DataType.BooleanType.nullable();
    }

    @Override
    public String toString() {
        return operator + "(" + args.stream().map(Object::toString).collect(Collectors.joining(",")) + ") : " + getType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BooleanFunction that = (BooleanFunction) o;
        return operator == that.operator && Objects.equals(args, that.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, args);
    }
}
