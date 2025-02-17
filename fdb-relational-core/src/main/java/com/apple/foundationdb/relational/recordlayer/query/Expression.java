/*
 * Expression.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NotValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This represents a logical expression that has an optional name, a type, and an underlying representation used to
 * generate a logical plan.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public class Expression {

    @Nonnull
    private final Optional<Identifier> name;

    @Nonnull
    private final DataType dataType;

    @Nonnull
    private final Supplier<Value> underlying;

    public Expression(@Nonnull Optional<Identifier> name,
                      @Nonnull DataType dataType,
                      @Nonnull Value expression) {
        this(name, dataType, () -> expression);
    }

    public Expression(@Nonnull Optional<Identifier> name,
                      @Nonnull DataType dataType,
                      @Nonnull Supplier<Value> valueSupplier) {
        this.name = name;
        this.dataType = dataType;
        this.underlying = Suppliers.memoize(valueSupplier::get);
    }

    @Nonnull
    public Optional<Identifier> getName() {
        return name;
    }

    @Nonnull
    public DataType getDataType() {
        return dataType;
    }

    @Nonnull
    public Value getUnderlying() {
        return underlying.get();
    }

    @Nonnull
    public Expression withName(@Nonnull Identifier name) {
        if (getName().isPresent() && getName().get().equals(name)) {
            return this;
        }
        return new Expression(Optional.of(name), getDataType(), getUnderlying());
    }

    @Nonnull
    public Expression withUnderlying(@Nonnull Value underlying) {
        if (getUnderlying().semanticEquals(underlying, AliasMap.identitiesFor(underlying.getCorrelatedTo()))) {
            return this;
        }
        return new Expression(getName(), DataTypeUtils.toRelationalType(underlying.getResultType()), underlying);
    }

    @Nonnull
    public Expression clearQualifier() {
        if (getName().isEmpty()) {
            return this;
        }
        final var name = getName().get();
        if (!name.isQualified()) {
            return this;
        }
        return new Expression(Optional.of(name.withoutQualifier()), getDataType(), getUnderlying());
    }

    @Nonnull
    public Expression withQualifier(@Nonnull final Collection<String> qualifier) {
        return replaceQualifier(ignored -> qualifier);
    }

    @Nonnull
    public Expression replaceQualifier(@Nonnull Function<Collection<String>, Collection<String>> replaceFunc) {
        if (getName().isEmpty()) {
            return this;
        }
        final var name = getName().get();
        final var newNameMaybe = name.replaceQualifier(replaceFunc);
        if (newNameMaybe.equals(name)) {
            return this;
        }
        return new Expression(Optional.of(newNameMaybe), getDataType(), getUnderlying());
    }

    @Nonnull
    public Expression withQualifier(@Nonnull final Optional<Identifier> qualifier) {
        if (getName().isEmpty()) {
            return this;
        }
        final var name = getName().get();
        if (qualifier.isEmpty() && !name.isQualified()) {
            return this;
        }
        if (name.isQualified() && qualifier.isPresent() && qualifier.get().fullyQualifiedName().equals(name.getQualifier())) {
            return this;
        }
        if (qualifier.isEmpty()) {
            return new Expression(Optional.of(name.withoutQualifier()), getDataType(), getUnderlying());
        }
        final var newName = name.withQualifier(qualifier.get().fullyQualifiedName());
        return new Expression(Optional.of(newName), getDataType(), getUnderlying());
    }

    public boolean isAggregate() {
        return underlying instanceof AggregateValue && !(underlying instanceof RecordConstructorValue);
    }

    @Nonnull
    public Expression pullUp(@Nonnull Value value, @Nonnull CorrelationIdentifier correlationIdentifier,
                             @Nonnull Set<CorrelationIdentifier> constantAliases) {
        final var aliasMap = AliasMap.identitiesFor(value.getCorrelatedTo());
        final var simplifiedValue = value.simplify(aliasMap, constantAliases);
        final var underlying = getUnderlying();
        final var pulledUpUnderlying = Assert.notNullUnchecked(underlying.replace(
                subExpression -> {
                    final var pulledUpExpressionMap = simplifiedValue.pullUp(List.of(subExpression),
                            aliasMap, constantAliases, correlationIdentifier);
                    if (pulledUpExpressionMap.containsKey(subExpression)) {
                        return pulledUpExpressionMap.get(subExpression);
                    }
                    return subExpression;
                }
        ));
        return this.withUnderlying(pulledUpUnderlying);
    }

    public boolean canBeDerivedFrom(@Nonnull final Expression expression,
                                    @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var value = expression.getUnderlying();
        final var aliasMap = AliasMap.identitiesFor(value.getCorrelatedTo());
        final var simplifiedValue = value.simplify(aliasMap, constantAliases);
        final var thisValue = getUnderlying();
        final var quantifier = CorrelationIdentifier.uniqueID();
        final var result = simplifiedValue.pullUp(ImmutableList.of(thisValue), aliasMap, constantAliases, quantifier);
        return result.containsKey(thisValue);
    }

    /**
     * Replaces all the {@link ConstantObjectValue} objects with corresponding {@link LiteralValue}s.
     *
     * @param literals The array of literals.
     *
     * @return a new {@link Expressions} list where each {@link Expression} internal {@link Value} with {@link LiteralValue}s
     * instead of any {@link ConstantObjectValue}s.
     */
    @Nonnull
    public Expressions dereferenced(@Nonnull QueryExecutionContext.Literals literals) {
        return Expressions.ofSingle(withUnderlying(Assert.notNullUnchecked(underlying.get().replace(value -> {
            if (value instanceof ConstantObjectValue) {
                final ConstantObjectValue constantObjectValue = (ConstantObjectValue) value;
                return new LiteralValue<>(constantObjectValue.getResultType(), literals.asMap().get(constantObjectValue.getConstantId()));
            }
            return value;
        }))));
    }

    @Nonnull
    public EphemeralExpression asEphemeral() {
        Verify.verify(getName().isPresent());
        return new EphemeralExpression(getName().get(), getDataType(), getUnderlying());
    }

    @Override
    public String toString() {
        return getName().orElse(Identifier.of("??")) + "|" + getDataType() + "| ⇾ " + getUnderlying();
    }

    @Nonnull
    public static Expression ofUnnamed(@Nonnull Value value) {
        return ofUnnamed(DataTypeUtils.toRelationalType(value.getResultType()), value);
    }

    @Nonnull
    public static Expression of(@Nonnull Value value, @Nonnull Identifier identifier) {
        return new Expression(Optional.of(identifier), DataTypeUtils.toRelationalType(value.getResultType()), value);
    }

    @Nonnull
    public static Expression ofUnnamed(@Nonnull DataType dataType,
                                       @Nonnull Value expression) {
        return new Expression(Optional.empty(), dataType, expression);
    }

    @Nonnull
    public static Expression fromUnderlying(@Nonnull Value underlying) {
        return new Expression(Optional.empty(), DataTypeUtils.toRelationalType(underlying.getResultType()), underlying);
    }

    @Nonnull
    public static Expression fromColumn(@Nonnull Column<? extends Value> column) {
        final var result = Expression.ofUnnamed(column.getValue());
        if (column.getField().getFieldNameOptional().isPresent()) {
            return result.withName(Identifier.of(column.getField().getFieldName()));
        }
        return result;
    }

    public static final class Utils {

        private Utils() {
        }

        @Nonnull
        public static Iterable<Value> filterUnderlyingAggregates(@Nonnull Expression expression) {
            return filterUnderlying(expression, true);
        }

        @Nonnull
        public static Iterable<Value> filterUnderlyingNonAggregates(@Nonnull Expression expression) {
            return filterUnderlying(expression, false);
        }

        @Nonnull
        private static Iterable<Value> filterUnderlying(@Nonnull Expression expression, boolean onlyAggregates) {
            return Streams.stream(expression.getUnderlying().preOrderIterator(value ->
                    value instanceof ArithmeticValue ||
                            value instanceof AndOrValue ||
                            value instanceof NotValue ||
                            value instanceof RecordConstructorValue ||
                            value instanceof RelOpValue))
                    .filter(leaf -> onlyAggregates == (!leaf.getResultType().isRecord() && leaf instanceof AggregateValue))
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        public static QueryPredicate toUnderlyingPredicate(@Nonnull Expression expression,
                                                           @Nonnull CorrelationIdentifier innermostAlias,
                                                           boolean forDdl) {
            final var value = Assert.castUnchecked(expression.getUnderlying(), BooleanValue.class);
            if (forDdl) {
                final var result = value.toQueryPredicate(ParseHelpers.EMPTY_TYPE_REPOSITORY, innermostAlias);
                Assert.thatUnchecked(result.isPresent());
                return result.get();
            } else {
                final var result = value.toQueryPredicate(null, innermostAlias);
                Assert.thatUnchecked(result.isPresent());
                return result.get();
            }
        }
    }
}
