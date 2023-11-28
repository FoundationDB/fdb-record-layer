/*
 * BooleanLiteral.java
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

import com.apple.foundationdb.relational.api.fluentsql.FluentVisitor;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Objects;

/**
 * This represents a boolean literal.
 */
@Immutable
public final class BooleanLiteral implements Literal<Boolean, DataType.BooleanType>, BooleanExpressionTrait {
    @Nonnull
    private static final BooleanLiteral TRUE = new BooleanLiteral(DataType.BooleanType.notNullable(), true);

    @Nonnull
    private static final BooleanLiteral FALSE = new BooleanLiteral(DataType.BooleanType.notNullable(), false);

    private static final BooleanLiteral NULL = new BooleanLiteral(DataType.BooleanType.nullable(), null);

    @Nullable
    private final Boolean literal;

    @Nonnull
    private final DataType.BooleanType type;

    private BooleanLiteral(@Nonnull final DataType.BooleanType type, @Nullable final Boolean literal) {
        this.type = type;
        this.literal = literal;
    }

    @Nullable
    @Override
    public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
        return visitor.visit(this, context);
    }

    @Nullable
    @Override
    public Boolean getValue() {
        return literal;
    }

    @Nonnull
    public static BooleanLiteral trueLiteral() {
        return TRUE;
    }

    @Nonnull
    public static BooleanLiteral falseLiteral() {
        return FALSE;
    }

    @Nonnull
    public static BooleanLiteral nullLiteral() {
        return NULL;
    }

    @Nonnull
    @Override
    public DataType.BooleanType getType() {
        return type;
    }

    @Override
    public String toString() {
        return literal == null ? "NULL" : (literal + " : " + type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BooleanLiteral that = (BooleanLiteral) o;
        return Objects.equals(literal, that.literal) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(literal, type);
    }
}
