/*
 * FieldImpl.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.fluentsql.FluentVisitor;
import com.apple.foundationdb.relational.api.fluentsql.expression.Field;
import com.apple.foundationdb.relational.api.metadata.DataType;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public class FieldImpl<T extends DataType> implements Field<T> {
    private final Iterable<String> parts;

    private final String name;

    private final ExpressionFactoryImpl expressionFactory;

    private final T dataType;

    private final java.util.function.Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    FieldImpl(final Iterable<String> parts,
              final ExpressionFactoryImpl expressionFactory,
              final T dataType) {
        this.name = Iterables.getLast(parts);
        this.parts = parts;
        this.expressionFactory = expressionFactory;
        this.dataType = dataType;
    }

    @Override
    public Iterable<String> getParts() {
        return parts;
    }

    @Override
    public Field<?> subField(String part) {
        return ((FieldImpl<?>) expressionFactory.resolve(dataType, List.of(part))).withPrefix(parts);
    }

    private Field<T> withPrefix(final Iterable<String> prefix) {
        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.addAll(prefix).addAll(parts);
        return new FieldImpl<>(builder.build(), expressionFactory, dataType);
    }

    @Nullable
    @Override
    public <R, C> R accept(FluentVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
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
