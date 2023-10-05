/*
 * FieldImpl.java
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

package com.apple.foundationdb.relational.recordlayer.structuredsql.expression;

import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.fleuntsql.FluentVisitor;
import com.apple.foundationdb.relational.api.fleuntsql.expression.Field;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class FieldImpl<T extends DataType> implements Field<T> {
    @Nonnull
    private final Iterable<String> parts;

    @Nonnull
    private final String name;

    @Nonnull
    private final ExpressionFactoryImpl expressionFactory;

    @Nonnull
    private final T dataType;

    @Nonnull
    private final java.util.function.Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    FieldImpl(@Nonnull final Iterable<String> parts,
              @Nonnull final ExpressionFactoryImpl expressionFactory,
              @Nonnull final T dataType) {
        this.name = Iterables.getLast(parts);
        this.parts = parts;
        this.expressionFactory = expressionFactory;
        this.dataType = dataType;
    }

    @Nonnull
    @Override
    public Iterable<String> getParts() {
        return parts;
    }

    @Nonnull
    @Override
    public Field<?> subField(@Nonnull String part) {
        return ((FieldImpl<?>) expressionFactory.resolve(dataType, List.of(part))).withPrefix(parts);
    }

    @Nonnull
    private Field<T> withPrefix(@Nonnull final Iterable<String> prefix) {
        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.addAll(prefix).addAll(parts);
        return new FieldImpl<>(builder.build(), expressionFactory, dataType);
    }

    @Nullable
    @Override
    public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
        return visitor.visit(this, context);
    }

    @Override
    @Nonnull
    public String getName() {
        return name;
    }

    @Override
    public T getType() {
        return dataType;
    }

    private int computeHashCode() {
        return Objects.hash(parts, dataType);
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
        final var otherField = (Field<?>) other;

        return getParts().equals(otherField.getParts()) &&
                getType().equals(otherField.getType());
    }

    @Override
    public String toString() {
        return String.join(".", parts) + " : " + getType();
    }
}
