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

import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Immutable
public interface UpdateStatement extends StructuredQuery {
    Map<Field<?>, Expression<?>> getSetClauses();

    List<Expression<?>> getReturning();

    @Nullable
    BooleanExpressionTrait getWhereClause();

    Set<QueryOptions> getOptions();

    String getTable();

    interface Builder {
        Map<Field<?>, Expression<?>> getSetClauses();

        Builder addSetClause(Field<?> field, Expression<?> newValue);

        Builder clearSetClauses();

        Builder removeSetClause(Field<?> field);

        List<Expression<?>> getReturning();

        Builder addReturning(Expression<?> expression);

        Builder clearReturning();

        @Nullable
        BooleanExpressionTrait getWhereClause();

        Builder addWhereClause(BooleanExpressionTrait expression);

        Builder clearWhereClause();

        Builder withOption(QueryOptions... options);

        Set<QueryOptions> getOptions();

        String getTable();

        Builder setTable(String table);

        Builder resolveSetFields(ExpressionFactory expressionFactory);

        UpdateStatement build() throws RelationalException;

    }
}
