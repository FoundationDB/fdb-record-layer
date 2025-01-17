/*
 * FluentVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.fluentsql;

import com.apple.foundationdb.relational.api.fluentsql.expression.BooleanFunction;
import com.apple.foundationdb.relational.api.fluentsql.expression.BooleanLiteral;
import com.apple.foundationdb.relational.api.fluentsql.expression.ComparableFunction;
import com.apple.foundationdb.relational.api.fluentsql.expression.ExpressionFragment;
import com.apple.foundationdb.relational.api.fluentsql.expression.Field;
import com.apple.foundationdb.relational.api.fluentsql.expression.FunctionLike;
import com.apple.foundationdb.relational.api.fluentsql.expression.NestedBooleanExpression;
import com.apple.foundationdb.relational.api.fluentsql.expression.NumericFunction;
import com.apple.foundationdb.relational.api.fluentsql.expression.NumericLiteral;
import com.apple.foundationdb.relational.api.fluentsql.expression.StringLiteral;
import com.apple.foundationdb.relational.api.fluentsql.expression.UserDefinedField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface FluentVisitor<R, C> {

    @Nullable
    R visit(@Nonnull BooleanFunction booleanFunction, @Nonnull C context);

    @Nullable
    R visit(@Nonnull NumericFunction<?> numericFunction, @Nonnull C context);

    @Nullable
    R visit(@Nonnull ComparableFunction<?, ?> comparableFunction, @Nonnull C context);

    @Nullable
    R visit(@Nonnull FunctionLike<?> function, @Nonnull C context);

    @Nullable
    R visit(@Nonnull BooleanLiteral booleanLiteral, @Nonnull C context);

    @Nullable
    R visit(@Nonnull NestedBooleanExpression nestedBooleanExpression, @Nonnull C context);

    @Nullable
    R visit(@Nonnull NumericLiteral<?, ?> numericLiteral, @Nonnull C context);

    @Nullable
    R visit(@Nonnull StringLiteral stringLiteral, @Nonnull C context);

    @Nullable
    R visit(@Nonnull ExpressionFragment<?> expression, @Nonnull C context);

    @Nullable
    R visit(@Nonnull Field<?> field, @Nonnull C context);

    @Nullable
    R visit(@Nonnull UserDefinedField<?> userDefinedField, @Nonnull C context);
}
