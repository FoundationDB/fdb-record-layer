/*
 * UpdateStatement.java
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

package com.apple.foundationdb.relational.api.fluentsql.statement;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.fluentsql.expression.BooleanExpressionTrait;
import com.apple.foundationdb.relational.api.fluentsql.expression.Expression;
import com.apple.foundationdb.relational.api.fluentsql.expression.ExpressionFactory;
import com.apple.foundationdb.relational.api.fluentsql.expression.Field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;

@Immutable
public interface UpdateStatement extends StructuredQuery {
    @Nonnull
    ImmutableMap<Field<?>, Expression<?>> getSetClauses();

    @Nonnull
    ImmutableList<Expression<?>> getReturning();

    @Nullable
    BooleanExpressionTrait getWhereClause();

    @Nonnull
    String getTable();

    interface Builder {
        @Nonnull
        Map<Field<?>, Expression<?>> getSetClauses();

        @Nonnull
        Builder addSetClause(@Nonnull final Field<?> field, @Nonnull final Expression<?> newValue);

        @Nonnull
        Builder clearSetClauses();

        @Nonnull
        @Deprecated
        Builder removeSetClause(@Nonnull final String field);

        @Nonnull
        Builder removeSetClause(@Nonnull final Field<?> field);

        @Nonnull
        List<Expression<?>> getReturning();

        @Nonnull
        Builder addReturning(@Nonnull final Expression<?> expression);

        @Nonnull
        Builder clearReturning();

        @Nullable
        BooleanExpressionTrait getWhereClause();

        @Nonnull
        Builder addWhereClause(@Nonnull final BooleanExpressionTrait expression);

        @Nonnull
        Builder clearWhereClause();

        @Nonnull
        Builder withOption(@Nonnull final QueryOptions... options);

        @Nonnull
        String getTable();

        @Nonnull
        Builder setTable(@Nonnull final String table);

        @Nonnull
        Builder resolveSetFields(@Nonnull final ExpressionFactory expressionFactory);

        @Nonnull
        UpdateStatement build() throws RelationalException;

    }
}
