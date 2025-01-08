/*
 * NestedBooleanExpression.java
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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.Objects;

/**
 * A helper expression for supporting syntactical nesting.
 * TODO: remove this class.
 */
@Immutable
@API(API.Status.EXPERIMENTAL)
public class NestedBooleanExpression implements BooleanExpressionTrait {

    @Nonnull
    private final BooleanExpressionTrait underlying;

    public NestedBooleanExpression(@Nonnull final BooleanExpressionTrait value) {
        this.underlying = value;
    }

    @Nonnull
    public BooleanExpressionTrait getValue() {
        return underlying;
    }

    @Override
    public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
        return visitor.visit(this, context);
    }

    @Override
    public DataType getType() {
        return underlying.getType();
    }

    @Override
    public String toString() {
        return "(" + underlying + ") : " + getType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NestedBooleanExpression that = (NestedBooleanExpression) o;
        return Objects.equals(underlying, that.underlying);
    }

    @Override
    public int hashCode() {
        return Objects.hash(underlying);
    }
}
