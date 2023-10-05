/*
 * FluentVisitor.java
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

package com.apple.foundationdb.relational.api.fleuntsql;

import com.apple.foundationdb.relational.api.fleuntsql.expression.BooleanFunction;
import com.apple.foundationdb.relational.api.fleuntsql.expression.BooleanLiteral;
import com.apple.foundationdb.relational.api.fleuntsql.expression.ComparableFunction;
import com.apple.foundationdb.relational.api.fleuntsql.expression.ExpressionFragment;
import com.apple.foundationdb.relational.api.fleuntsql.expression.Field;
import com.apple.foundationdb.relational.api.fleuntsql.expression.FunctionLike;
import com.apple.foundationdb.relational.api.fleuntsql.expression.NestedBooleanExpression;
import com.apple.foundationdb.relational.api.fleuntsql.expression.NumericFunction;
import com.apple.foundationdb.relational.api.fleuntsql.expression.NumericLiteral;
import com.apple.foundationdb.relational.api.fleuntsql.expression.StringLiteral;
import com.apple.foundationdb.relational.api.fleuntsql.expression.UserDefinedField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface FluentVisitor<R, C> {

    @Nullable
    R visit(@Nonnull final BooleanFunction booleanFunction, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final NumericFunction<?> numericFunction, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final ComparableFunction<?, ?> comparableFunction, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final FunctionLike<?> function, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final BooleanLiteral booleanLiteral, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final NestedBooleanExpression nestedBooleanExpression, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final NumericLiteral<?, ?> numericLiteral, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final StringLiteral stringLiteral, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final ExpressionFragment<?> expression, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final Field<?> field, @Nonnull final C context);

    @Nullable
    R visit(@Nonnull final UserDefinedField<?> userDefinedField, @Nonnull final C context);
}
