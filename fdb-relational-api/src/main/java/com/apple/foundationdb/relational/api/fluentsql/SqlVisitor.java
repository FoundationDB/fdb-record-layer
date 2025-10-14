/*
 * SqlVisitor.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.fluentsql.expression.BooleanFunction;
import com.apple.foundationdb.relational.api.fluentsql.expression.BooleanLiteral;
import com.apple.foundationdb.relational.api.fluentsql.expression.ComparableFunction;
import com.apple.foundationdb.relational.api.fluentsql.expression.ExpressionFragment;
import com.apple.foundationdb.relational.api.fluentsql.expression.Field;
import com.apple.foundationdb.relational.api.fluentsql.expression.FunctionLike;
import com.apple.foundationdb.relational.api.fluentsql.expression.NestedBooleanExpression;
import com.apple.foundationdb.relational.api.fluentsql.expression.NumericFunction;
import com.apple.foundationdb.relational.api.fluentsql.expression.NumericLiteral;
import com.apple.foundationdb.relational.api.fluentsql.expression.Operation;
import com.apple.foundationdb.relational.api.fluentsql.expression.StringLiteral;
import com.apple.foundationdb.relational.api.fluentsql.expression.UserDefinedField;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Immutable
@API(API.Status.EXPERIMENTAL)
public class SqlVisitor implements FluentVisitor<Void, StringBuilder> {

    private static final Map<Operation, String> operationSqlSymbols;

    static {
        operationSqlSymbols = ImmutableMap.<Operation, String>builder()
                .put(Operation.EQUAL, "=")
                .put(Operation.NOT_EQUAL, "<>")
                .put(Operation.IS_NULL, "IS NULL")
                .put(Operation.IS_NOT_NULL, "IS NOT NULL")
                .put(Operation.GREATER_THAN, ">")
                .put(Operation.LESS_THAN, "<")
                .put(Operation.GREATER_THAN_EQUALS, ">=")
                .put(Operation.LESS_THAN_EQUALS, "<=")
                .put(Operation.AND, "AND")
                .put(Operation.OR, "OR")
                .put(Operation.NOT, "NOT")
                .put(Operation.ADD, "+")
                .put(Operation.DIV, "/")
                .put(Operation.MUL, "*")
                .put(Operation.SUB, "-")
                .put(Operation.MOD, "MOD")
                .put(Operation.GREATEST, "GREATEST")
                .put(Operation.JAVA_CALL, "JAVA_CALL").build();
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final BooleanFunction booleanFunction, @Nonnull final StringBuilder context) {
        return visit((FunctionLike<?>) booleanFunction, context);
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final NumericFunction<?> numericFunction, @Nonnull final StringBuilder context) {
        return visit((FunctionLike<?>) numericFunction, context);
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final ComparableFunction<?, ?> comparableFunction, @Nonnull final StringBuilder context) {
        return visit((FunctionLike<?>) comparableFunction, context);
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final FunctionLike<?> function, @Nonnull final StringBuilder context) {
        switch (function.getName()) {
            case JAVA_CALL: // fallthrough
            case GREATEST: {
                context.append(operationSqlSymbols.get(function.getName())).append("(");
                int size = Iterables.size(function.getArguments());
                int cnt = 0;
                for (final var argument : function.getArguments()) {
                    argument.accept(this, context);
                    if (cnt < size - 1) {
                        context.append(",");
                    }
                    cnt++;
                }
                context.append(")");
            }
                break;
            case EQUAL: // fallthrough
            case NOT_EQUAL: // fallthrough
            case GREATER_THAN: // fallthrough
            case LESS_THAN: // fallthrough
            case GREATER_THAN_EQUALS: // fallthrough
            case LESS_THAN_EQUALS: // fallthrough
            case AND: // fallthrough
            case OR: // fallthrough
            case NOT: // fallthrough
            case ADD: // fallthrough
            case DIV: // fallthrough
            case MUL: // fallthrough
            case SUB: // fallthrough
            case MOD: {
                int size = Iterables.size(function.getArguments());
                int cnt = 0;
                for (final var argument : function.getArguments()) {
                    argument.accept(this, context);
                    if (cnt < size - 1) {
                        context.append(" ").append(operationSqlSymbols.get(function.getName())).append(" ");
                    }
                    cnt++;
                }
            }
                break;
            case IS_NULL: // fallthrough
            case IS_NOT_NULL: {
                Iterables.getOnlyElement(function.getArguments()).accept(this, context);
                context.append(" ").append(operationSqlSymbols.get(function.getName()));
            }
                break;
            default:
                throw new RelationalException("unknown symbol '" + function.getName() + "'", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }
        return null;
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final BooleanLiteral booleanLiteral, @Nonnull final StringBuilder context) {
        context.append(booleanLiteral.getValue());
        return null;
    }

    @Nullable
    @Override
    public Void visit(@Nonnull NestedBooleanExpression expression, @Nonnull StringBuilder context) {
        context.append("( ");
        expression.getValue().accept(this, context);
        context.append(" )");
        return null;
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final NumericLiteral<?, ?> numericLiteral, @Nonnull final StringBuilder context) {
        context.append(numericLiteral.getValue());
        return null;
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final StringLiteral stringLiteral, @Nonnull final StringBuilder context) {
        context.append('\'').append(stringLiteral.getValue()).append('\'');
        return null;
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final ExpressionFragment<?> expression, @Nonnull final StringBuilder context) {
        context.append(expression.getFragment());
        return null;
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final Field<?> field, @Nonnull final StringBuilder context) {
        context.append(StreamSupport.stream(field.getParts().spliterator(), false).map(f -> "\"" + f + "\"").collect(Collectors.joining(".")));
        return null;
    }

    @Nullable
    @Override
    public Void visit(@Nonnull final UserDefinedField<?> userDefinedField, @Nonnull final StringBuilder context) {
        context.append(StreamSupport.stream(userDefinedField.getParts().spliterator(), false).collect(Collectors.joining(".")));
        return null;
    }
}
