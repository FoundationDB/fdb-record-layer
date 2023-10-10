/*
 * ExpressionFactory.java
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

package com.apple.foundationdb.relational.api.fleuntsql.expression;

import com.apple.foundationdb.relational.api.fleuntsql.expression.details.Mixins;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;

/**
 * A factory that can be used to construct a {@link Expression}.
 */
@Immutable
public interface ExpressionFactory {

    @Nonnull
    default BooleanLiteral literal(Boolean value) {
        if (value == null) {
            return BooleanLiteral.nullLiteral();
        }
        return value ? BooleanLiteral.trueLiteral() : BooleanLiteral.falseLiteral();
    }

    @Nonnull
    default BooleanLiteral literal(boolean value) {
        return value ? BooleanLiteral.trueLiteral() : BooleanLiteral.falseLiteral();
    }

    @Nonnull
    default NumericLiteral<Integer, DataType.IntegerType> literal(Integer value) {
        final var type = value == null ? DataType.IntegerType.notNullable() : DataType.IntegerType.nullable();
        return new NumericLiteral<>(type, value);
    }

    @Nonnull
    default NumericLiteral<Integer, DataType.IntegerType> literal(int value) {
        return new NumericLiteral<>(DataType.IntegerType.notNullable(), value);
    }

    @Nonnull
    default NumericLiteral<Long, DataType.LongType> literal(@Nullable final Long value) {
        final var type = value == null ? DataType.LongType.notNullable() : DataType.LongType.nullable();
        return new NumericLiteral<>(type, value);
    }

    @Nonnull
    default NumericLiteral<Long, DataType.LongType> literal(long value) {
        return new NumericLiteral<>(DataType.LongType.notNullable(), value);
    }

    @Nonnull
    default NumericLiteral<Double, DataType.DoubleType> literal(@Nullable final Double value) {
        final var type = value == null ? DataType.DoubleType.notNullable() : DataType.DoubleType.nullable();
        return new NumericLiteral<>(type, value);
    }

    @Nonnull
    default NumericLiteral<Double, DataType.DoubleType> literal(double value) {
        return new NumericLiteral<>(DataType.DoubleType.notNullable(), value);
    }

    @Nonnull
    default NumericLiteral<Float, DataType.FloatType> literal(@Nullable final Float value) {
        final var type = value == null ? DataType.FloatType.notNullable() : DataType.FloatType.nullable();
        return new NumericLiteral<>(type, value);
    }

    @Nonnull
    default NumericLiteral<Float, DataType.FloatType> literal(float value) {
        return new NumericLiteral<>(DataType.FloatType.notNullable(), value);
    }

    @Nonnull
    default StringLiteral literal(@Nullable final String value) {
        final var type = value == null ? DataType.StringType.notNullable() : DataType.StringType.nullable();
        return new StringLiteral(type, value);
    }

    default Mixins.BooleanField field(@Nonnull final DataType.BooleanType type, @Nonnull final String part) {
        return field(type, List.of(part)).asBoolean(type.isNullable());
    }

    @Nonnull
    default Mixins.BooleanField field(@Nonnull final DataType.BooleanType type, @Nonnull final Iterable<String> parts) {
        return field((DataType) type, parts).asBoolean(type.isNullable());
    }

    default Mixins.IntField field(@Nonnull final DataType.IntegerType type, @Nonnull final String part) {
        return field(type, List.of(part));
    }

    @Nonnull
    default Mixins.IntField field(@Nonnull final DataType.IntegerType type, @Nonnull final Iterable<String> parts) {
        return field((DataType) type, parts).asInt(type.isNullable());
    }

    default Mixins.LongField field(@Nonnull final DataType.LongType type, @Nonnull final String part) {
        return field(type, List.of(part)).asLong(type.isNullable());
    }

    @Nonnull
    default Mixins.LongField field(@Nonnull final DataType.LongType type, @Nonnull final Iterable<String> parts) {
        return field((DataType) type, parts).asLong(type.isNullable());
    }

    default Mixins.DoubleField field(@Nonnull final DataType.DoubleType type, @Nonnull final String part) {
        return field(type, List.of(part)).asDouble(type.isNullable());
    }

    @Nonnull
    default Mixins.DoubleField field(@Nonnull final DataType.DoubleType type, @Nonnull final Iterable<String> parts) {
        return field((DataType) type, parts).asDouble(type.isNullable());
    }

    default Mixins.FloatField field(@Nonnull final DataType.FloatType type, @Nonnull final String part) {
        return field(type, List.of(part)).asFloat(type.isNullable());
    }

    @Nonnull
    default Mixins.FloatField field(@Nonnull final DataType.FloatType type, @Nonnull final Iterable<String> parts) {
        return field((DataType) type, parts).asFloat(type.isNullable());
    }

    default Mixins.StringField field(@Nonnull final DataType.StringType type, @Nonnull final String part) {
        return field(type, List.of(part)).asString(type.isNullable());
    }

    @Nonnull
    default Mixins.StringField field(@Nonnull final DataType.StringType type, @Nonnull final Iterable<String> parts) {
        return field((DataType) type, parts).asString(type.isNullable());
    }

    @Nonnull
    default <T extends DataType> Field<T> field(@Nonnull final T type, @Nonnull final String part) {
        return field(type, List.of(part));
    }

    @Nonnull
    default <T extends DataType> Field<T> field(@Nonnull final T type, @Nonnull final Iterable<String> parts) {
        return new UserDefinedField<>(type, parts);
    }

    @Nonnull
    default Field<?> field(@Nonnull final String tableName, @Nonnull final String part) {
        return field(tableName, List.of(part));
    }

    @Nonnull
    Field<?> field(@Nonnull final String tableName, @Nonnull final Iterable<String> parts);

    @Nonnull
    default ExpressionFragment<?> parseFragment(@Nonnull final String fragment) {
        return new ParsingFragment<>(DataType.UnknownType.instance(), fragment);
    }
}
