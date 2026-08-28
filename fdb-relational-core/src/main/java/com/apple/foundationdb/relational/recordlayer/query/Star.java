/*
 * Star.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
@API(API.Status.EXPERIMENTAL)
public final class Star extends Expression {

    private final List<Expression> expansion;

    private Star(Optional<Identifier> qualifier, DataType dataType, Value expression,
                 Iterable<Expression> expansion) {
        super(qualifier, dataType, expression);
        Assert.thatUnchecked(expression.getResultType().isRecord());
        Assert.thatUnchecked(dataType.getCode() == DataType.Code.STRUCT);
        Assert.thatUnchecked(Iterables.size(expansion) == ((DataType.StructType) dataType).getFields().size());
        this.expansion = ImmutableList.copyOf(expansion);
    }

    @Override
    protected Expression createNew(Optional<Identifier> newName, DataType newDataType, Value newUnderlying, Visibility newVisibility) {
        throw new RelationalException("attempt to recreate new star expression", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
    }

    public List<Expression> getExpansion() {
        return expansion;
    }

    @Override
    public Expression withQualifier(Collection<String> qualifier) {
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

    @Override
    public Expression withName(Identifier name) {
        Assert.failUnchecked("attempt to name a star expression");
        return null;
    }

    @Override
    public Expression withUnderlying(Value underlying) {
        Assert.failUnchecked("attempt to replace underlying value of a star expression");
        return null;
    }

    /**
     * Replaces all the {@link ConstantObjectValue} objects with corresponding {@link LiteralValue}s of the star expansion.
     *
     * @param literals The array of literals.
     *
     * @return a new {@link Expressions} list where each {@link Expression} internal {@link Value} with {@link LiteralValue}s
     * instead of any {@link ConstantObjectValue}s.
     */
    @Override
    public Expressions dereferenced(Literals literals) {
        return Expressions.of(expansion).dereferenced(literals);
    }

    @Override
    public EphemeralExpression asEphemeral() {
        throw new RelationalException("attempt to create an ephemeral expression from a star", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
    }

    @Override
    public String toString() {
        return "* ≍" + (expansion.stream()
                .flatMap(exp -> exp.getName().stream()))
                .map(Identifier::toString)
                .collect(Collectors.joining(",")) + "|" + getDataType() + "| ⇾ " + getUnderlying();
    }

    public static Star overQuantifier(Optional<Identifier> qualifier,
                                      Value quantifier,
                                      String typeName,
                                      Expressions expansion) {
        final var starType = createStarType(typeName, expansion);
        return new Star(qualifier, starType, ensureValueConsistentWithExpansion(quantifier, expansion), expansion);
    }

    public static Star overQuantifiers(Optional<Identifier> qualifier,
                                       List<QuantifiedObjectValue> quantifiers,
                                       String typeName,
                                       Expressions expansion) {
        final var underlyingStarType = quantifiers.size() == 1 ? quantifiers.get(0) : RecordConstructorValue.ofUnnamed(quantifiers);
        final var starType = createStarType(typeName, expansion);
        return new Star(qualifier, starType, ensureValueConsistentWithExpansion(underlyingStarType, expansion), expansion);
    }

    private static Value ensureValueConsistentWithExpansion(Value possibleValue, Expressions expansion) {
        // Try expanding the expansion and creating a record constructor value. If it has the same type as a proposed
        // pre-existing value, use that instead. This allows us to avoid inserting unnecessary RCVs if there's already
        // a value of the correct type, but it also allows us to modify the underlying type coming from a quantifier
        // (e.g., to remove pseudo-columns)
        final RecordConstructorValue rcv = RecordConstructorValue.ofColumns(expansion.underlyingAsColumns());
        if (rcv.getResultType().equals(possibleValue.getResultType())) {
            return possibleValue;
        } else {
            return rcv;
        }
    }

    public static Star overIndividualExpressions(Optional<Identifier> qualifier,
                                                 String typeName,
                                                 Expressions expansion) {
        final var starType = createStarType(typeName, expansion);
        return new Star(qualifier, starType, RecordConstructorValue.ofColumns(expansion.underlyingAsColumns()), expansion);
    }

    private static DataType.StructType createStarType(String name,
                                                      Expressions expansion) {
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
