/*
 * StringLiteral.java
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
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Objects;

/**
 * This represents a string literal.
 */
@Immutable
@API(API.Status.EXPERIMENTAL)
public class StringLiteral implements Literal<String, DataType.StringType> {

    @Nullable
    private final String literal;

    @Nonnull
    private final DataType.StringType type;

    public StringLiteral(@Nonnull final DataType.StringType type, @Nullable String literal) {
        this.type = type;
        this.literal = literal;
    }

    @Nullable
    @Override
    public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
        return visitor.visit(this, context);
    }

    @Nonnull
    @Override
    public DataType.StringType getType() {
        return type;
    }

    @Nullable
    @Override
    public String getValue() {
        return literal;
    }

    @Override
    public String toString() {
        return literal == null ? "NULL" : (literal + " : " + type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(literal, type);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final StringLiteral that = (StringLiteral) other;
        return Objects.equals(literal, that.literal) && Objects.equals(type, that.type);
    }
}
