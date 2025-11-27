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
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;

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
@API(API.Status.EXPERIMENTAL)
public final class Star extends Expression {

    @Nonnull
    private final Expressions expansion;

    private Star(@Nonnull Optional<Identifier> qualifier, @Nonnull DataType dataType, @Nonnull Value expression,
                 @Nonnull Expressions expansion) {
        super(qualifier, dataType, expression);
        Assert.thatUnchecked(expression.getResultType().isRecord());
        Assert.thatUnchecked(dataType.getCode() == DataType.Code.STRUCT);
        Assert.thatUnchecked(expansion.size() == ((DataType.StructType) dataType).getFields().size());
        this.expansion = expansion;
    }

    @Nonnull
    public Expressions getExpansion() {
        return expansion;
    }

    @Nonnull
    @Override
    public Expression withQualifier(@Nonnull Collection<String> qualifier) {
        if (getName().isEmpty()) {
            return this;
        }
        final Identifier name = getName().get();
        final Identifier newNameMaybe = name.withQualifier(qualifier);
        final Expressions newExpansionMaybe = expansion.withQualifier(qualifier);
        if (!newNameMaybe.equals(name) || !Objects.equals(newExpansionMaybe, expansion)) {
            // Keep the same underlying value - it doesn't need to change when adding qualifiers
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

    /**
     * Replaces all the {@link ConstantObjectValue} objects with corresponding {@link LiteralValue}s of the star expansion.
     *
     * @param literals The array of literals.
     *
     * @return a new {@link Expressions} list where each {@link Expression} internal {@link Value} with {@link LiteralValue}s
     * instead of any {@link ConstantObjectValue}s.
     */
    @Nonnull
    @Override
    public Expressions dereferenced(@Nonnull Literals literals) {
        return expansion.dereferenced(literals);
    }

    @Nonnull
    @Override
    public EphemeralExpression asEphemeral() {
        Assert.failUnchecked("attempt to create an ephemeral expression from a star");
        return null;
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
        // Note: quantifier parameter is unused because expansion is already filtered to visible columns
        // by SemanticAnalyzer, and we need to create the underlying from that filtered expansion
        return createStar(qualifier, typeName, expansion);
    }

    @Nonnull
    public static Star overQuantifiers(@Nonnull Optional<Identifier> qualifier,
                                       @Nonnull List<QuantifiedObjectValue> quantifiers,
                                       @Nonnull String typeName,
                                       @Nonnull Expressions expansion) {
        // Note: quantifiers parameter is unused because expansion is already filtered to visible columns
        // by SemanticAnalyzer, and we need to create the underlying from that filtered expansion
        return createStar(qualifier, typeName, expansion);
    }

    @Nonnull
    public static Star overIndividualExpressions(@Nonnull Optional<Identifier> qualifier,
                                                 @Nonnull String typeName,
                                                 @Nonnull Expressions expansion) {
        return createStar(qualifier, typeName, expansion);
    }

    @Nonnull
    private static Star createStar(@Nonnull Optional<Identifier> qualifier,
                                   @Nonnull String typeName,
                                   @Nonnull Expressions expansion) {
        final var starType = createStarType(typeName, expansion);
        // The expansion is already filtered to visible columns only by SemanticAnalyzer,
        // so we create the underlying value from the filtered expansion
        final var filteredUnderlying = RecordConstructorValue.ofColumns(expansion.underlyingAsColumns());
        return new Star(qualifier, starType, filteredUnderlying, expansion);
    }

    /**
     * Creates the {@link DataType.StructType} for a star expression from the expansion.
     * <p>
     * The {@code expansion} parameter must contain only visible columns. This invariant is enforced
     * by {@link com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer#expandStar},
     * which filters invisible columns via {@link Expressions#nonInvisible()} before creating the star.
     *
     * @param name the name for the struct type
     * @param expansion the expressions to include in the star (must not contain invisible expressions)
     * @return the struct type representing the star's schema
     */
    @Nonnull
    private static DataType.StructType createStarType(@Nonnull String name,
                                                      @Nonnull Expressions expansion) {
        final ImmutableList.Builder<DataType.StructType.Field> fields = ImmutableList.builder();
        int i = 0;
        for (final var expression : expansion) {
            // Expansion is already filtered to visible columns only in SemanticAnalyzer.expandStar()
            // Enforce this invariant with an assertion to catch bugs if filtering logic changes
            Assert.thatUnchecked(!expression.isInvisible(),
                    "Star expansion should not contain invisible expressions");
            fields.add(DataType.StructType.Field.from(expression.getName().map(Identifier::toString)
                    .orElseGet(() -> expression.getUnderlying().toString()), expression.getDataType(), i, false));
            i++;
        }
        return DataType.StructType.from(name, fields.build(), true);
    }
}
