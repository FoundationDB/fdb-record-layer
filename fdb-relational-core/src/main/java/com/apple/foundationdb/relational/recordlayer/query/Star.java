/*
 * Star.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This represents a SQL {@code *} expression, it is capable of understanding the expanded representation of the star
 * (in terms of other {@link Expression}(s).
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class Star extends Expression {

    @Nonnull
    private final List<Expression> expansion;

    private Star(@Nonnull Optional<Identifier> qualifier, @Nonnull DataType dataType, @Nonnull Value expression,
                 @Nonnull Iterable<Expression> expansion) {
        super(qualifier, dataType, expression);
        Assert.thatUnchecked(expression.getResultType().isRecord());
        Assert.thatUnchecked(dataType.getCode() == DataType.Code.STRUCT);
        Assert.thatUnchecked(Iterables.size(expansion) == ((DataType.StructType) dataType).getFields().size());
        this.expansion = ImmutableList.copyOf(expansion);
    }

    @Nonnull
    public List<Expression> getExpansion() {
        return expansion;
    }

    @Nonnull
    @Override
    public Expression withQualifier(@Nonnull Collection<String> qualifier) {
        if (getName().isEmpty()) {
            return this;
        }
        final var name = getName().get();
        final var newNameMaybe = name.withQualifier(qualifier);
        final var newExpansionMaybe = expansion.stream().map(expression -> expression.withQualifier(qualifier)).collect(ImmutableList.toImmutableList());
        if (!newNameMaybe.equals(name) || !Objects.equals(newExpansionMaybe, expansion)) {
            return new Star(Optional.of(newNameMaybe), getDataType(), getUnderlying(), newExpansionMaybe);
        }
        return this;
    }

    @Nonnull
    @Override
    public Expression withName(@Nonnull Identifier name) {
        Assert.failUnchecked("attempt to name a star expression");
        return null;
    }

    @Nonnull
    @Override
    public Expression withUnderlying(@Nonnull Value underlying) {
        Assert.failUnchecked("attempt to replace underlying value of a star expression");
        return null;
    }

    @Nonnull
    @Override
    public Expressions dereferenced(@Nonnull QueryExecutionContext.Literals literals) {
        return Expressions.of(expansion).dereferenced(literals);
    }

    @Override
    public String toString() {
        return "* ≍" + (expansion.stream()
                .flatMap(exp -> exp.getName().stream()))
                .map(Identifier::toString)
                .collect(Collectors.joining(",")) + "|" + getDataType() + "| ⇾ " + getUnderlying();
    }

    @Nonnull
    public static Star overQuantifier(@Nonnull Optional<Identifier> qualifier,
                                      @Nonnull Value quantifier,
                                      @Nonnull String typeName,
                                      @Nonnull Expressions expansion) {
        final var starType = createStarType(typeName, expansion);
        return new Star(qualifier, starType, quantifier, expansion);
    }

    @Nonnull
    public static Star overQuantifiers(@Nonnull Optional<Identifier> qualifier,
                                       @Nonnull List<QuantifiedObjectValue> quantifiers,
                                       @Nonnull String typeName,
                                       @Nonnull Expressions expansion) {
        final var underlyingStarType = quantifiers.size() == 1 ? quantifiers.get(0) : RecordConstructorValue.ofUnnamed(quantifiers);
        final var starType = createStarType(typeName, expansion);
        return new Star(qualifier, starType, underlyingStarType, expansion);
    }

    @Nonnull
    public static Star overIndividualExpressions(@Nonnull Optional<Identifier> qualifier,
                                                 @Nonnull String typeName,
                                                 @Nonnull Expressions expansion) {
        final var starType = createStarType(typeName, expansion);
        return new Star(qualifier, starType, RecordConstructorValue.ofColumns(expansion.underlyingAsColumns()), expansion);
    }

    @Nonnull
    private static DataType.StructType createStarType(@Nonnull String name,
                                                      @Nonnull Expressions expansion) {
        final ImmutableList.Builder<DataType.StructType.Field> fields = ImmutableList.builder();
        int i = 0;
        for (final var expression : expansion) {
            fields.add(DataType.StructType.Field.from(expression.getName().map(Identifier::toString)
                    .orElseGet(() -> expression.getUnderlying().toString()), expression.getDataType(), i));
            i++;
        }
        return DataType.StructType.from(name, fields.build(), true);
    }
}
