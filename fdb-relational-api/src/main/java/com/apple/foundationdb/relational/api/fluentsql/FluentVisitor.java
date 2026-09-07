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

import org.jspecify.annotations.Nullable;

public interface FluentVisitor<R, C> {

    @Nullable
    R visit(BooleanFunction booleanFunction, C context);

    @Nullable
    R visit(NumericFunction<?> numericFunction, C context);

    @Nullable
    R visit(ComparableFunction<?, ?> comparableFunction, C context);

    @Nullable
    R visit(FunctionLike<?> function, C context);

    @Nullable
    R visit(BooleanLiteral booleanLiteral, C context);

    @Nullable
    R visit(NestedBooleanExpression nestedBooleanExpression, C context);

    @Nullable
    R visit(NumericLiteral<?, ?> numericLiteral, C context);

    @Nullable
    R visit(StringLiteral stringLiteral, C context);

    @Nullable
    R visit(ExpressionFragment<?> expression, C context);

    @Nullable
    R visit(Field<?> field, C context);

    @Nullable
    R visit(UserDefinedField<?> userDefinedField, C context);
}
