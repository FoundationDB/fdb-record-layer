/*
 * UserDefinedField.java
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
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.fluentsql.FluentVisitor;
import com.apple.foundationdb.relational.api.fluentsql.expression.details.Mixins;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This represents a user-defined field, this allows the user to explicitly set the field
 * name and type without resorting to any form of metadata resolution.
 * <br>
 * This could be useful for testing, and referring to qualified fields with subquery aliases for example.
 *
 * @param <T> the type of the field.
 */
@API(API.Status.EXPERIMENTAL)
public class UserDefinedField<T extends DataType> implements Field<T> {

    @Nonnull
    private final String name;

    @Nonnull
    private final ImmutableList<String> parts;

    @Nonnull
    private final T type;

    @Nonnull
    @SuppressWarnings("this-escape")
    private final java.util.function.Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    public UserDefinedField(@Nonnull T type, @Nonnull final Iterable<String> parts) {
        this.name = Iterables.getLast(parts);
        this.parts = ImmutableList.copyOf(parts);
        this.type = type;
    }

    @Nullable
    @Override
    public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
        return visitor.visit(this, context);
    }

    @Nonnull
    @Override
    public Iterable<String> getParts() {
        return parts;
    }

    @Nonnull
    @Override
    public T getType() {
        return type;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public Field<?> subField(@Nonnull final String part) {
        return subField(DataType.UnknownType.instance(), part);
    }

    @Nonnull
    public Field<?> subField(@Nonnull final DataType type,
                             @Nonnull final String name) {
        final var newFields = Streams.concat(parts.stream(), Stream.of(name)).collect(Collectors.toUnmodifiableList());
        return new UserDefinedField<>(type, newFields);
    }

    @Nonnull
    @Override
    public Mixins.BooleanField asBoolean() {
        expectingType(DataType.Code.BOOLEAN);
        return Field.super.asBoolean();
    }

    @Nonnull
    @Override
    public Mixins.IntField asInt() {
        expectingType(DataType.Code.INTEGER);
        return Field.super.asInt();
    }

    @Nonnull
    @Override
    public Mixins.LongField asLong() {
        expectingType(DataType.Code.LONG);
        return Field.super.asLong();
    }

    @Nonnull
    @Override
    public Mixins.FloatField asFloat() {
        expectingType(DataType.Code.FLOAT);
        return Field.super.asFloat();
    }

    @Nonnull
    @Override
    public Mixins.DoubleField asDouble() {
        expectingType(DataType.Code.DOUBLE);
        return Field.super.asDouble();
    }

    @Nonnull
    @Override
    public Mixins.StringField asString() {
        expectingType(DataType.Code.STRING);
        return Field.super.asString();
    }

    private void expectingType(@Nonnull final DataType.Code code) {
        if (type.getCode() != DataType.Code.UNKNOWN && !type.getCode().equals(code)) {
            throw new RelationalException("Type mismatch, expected type '" + code.name() + "', actual type '" + type.getCode().name() + "'", ErrorCode.DATATYPE_MISMATCH).toUncheckedWrappedException();
        }
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof Field)) {
            return false;
        }
        final var otherFieldImpl = (Field<?>) other;

        return parts.equals(otherFieldImpl.getParts()) &&
                getType().equals(otherFieldImpl.getType());
    }

    @Override
    public String toString() {
        return String.join(".", parts) + " : " + type;
    }

    private int computeHashCode() {
        return Objects.hash(parts, getType());
    }
}
