/*
 * Expressions.java
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@API(API.Status.EXPERIMENTAL)
public final class Expressions implements Iterable<Expression> {

    @Nonnull
    private static final Expressions EMPTY = new Expressions(ImmutableList.of());

    @Nonnull
    private final List<Expression> underlying;

    private Expressions(@Nonnull Iterable<Expression> underlying) {
        this.underlying = ImmutableList.copyOf(underlying);
    }

    @Nonnull
    @Override
    public Iterator<Expression> iterator() {
        return underlying.iterator();
    }

    @Nonnull
    public Expressions expanded() {
        return Expressions.of(underlying.stream()
                .flatMap(item -> item instanceof Star ?
                        ((Star) item).getExpansion().stream() :
                        Stream.of(item)).collect(ImmutableList.toImmutableList()));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    public Expressions rewireQov(@Nonnull Value value) {
        final ImmutableList.Builder<Expression> pulledUpOutputBuilder = ImmutableList.builder();
        int colCount = 0;
        for (final var expression : this) {
            final var newUnderlying = FieldValue.ofOrdinalNumber(value, colCount);
            pulledUpOutputBuilder.add(expression.withUnderlying(newUnderlying));
            colCount++;
        }
        return Expressions.of(pulledUpOutputBuilder.build());
    }

    @Nonnull
    public Expressions pullUp(@Nonnull Value value, @Nonnull CorrelationIdentifier correlationIdentifier,
                              @Nonnull Set<CorrelationIdentifier> constantAliases) {
        final ImmutableList.Builder<Expression> pulledUpOutputBuilder = ImmutableList.builder();
        final var aliasMap = AliasMap.identitiesFor(value.getCorrelatedTo());
        final var simplifiedValue = value.simplify(aliasMap, constantAliases);
        for (final var expression : this) {
            final var underlying = expression.getUnderlying();
            final var pulledUpUnderlying = Assert.notNullUnchecked(underlying.replace(
                    subExpression -> {
                        final var pulledUpExpressionMap = simplifiedValue.pullUp(List.of(subExpression), aliasMap, constantAliases, correlationIdentifier);
                        if (pulledUpExpressionMap.containsKey(subExpression)) {
                            return pulledUpExpressionMap.get(subExpression);
                        }
                        return subExpression;
                    }
            ));
            pulledUpOutputBuilder.add(expression.withUnderlying(pulledUpUnderlying));
        }
        return Expressions.of(pulledUpOutputBuilder.build());
    }

    @Nonnull
    public Expressions difference(@Nonnull Expressions that) {
        if (Iterables.isEmpty(that)) {
            return this;
        }
        if (Iterables.isEmpty(this)) {
            return Expressions.empty();
        }
        final var expandedThat = that.expanded();
        final var otherAsSet = Streams.stream(expandedThat)
                .map(item -> {
                    final var value = item.getUnderlying();
                    final Correlated.BoundEquivalence<Value> boundEquivalence = new Correlated.BoundEquivalence<>(AliasMap.identitiesFor(value.getCorrelatedTo()));
                    return boundEquivalence.wrap(item.getUnderlying());
                })
                .collect(ImmutableSet.toImmutableSet());
        final var expandedThis = expanded();
        final ImmutableList.Builder<Expression> resultBuilder = ImmutableList.builder();
        for (final var thisExpression : expandedThis) {
            final var thisUnderlying = thisExpression.getUnderlying();
            final Correlated.BoundEquivalence<Value> boundEquivalence = new Correlated.BoundEquivalence<>(AliasMap.identitiesFor(thisUnderlying.getCorrelatedTo()));
            if (otherAsSet.contains(boundEquivalence.wrap(thisExpression.getUnderlying()))) {
                continue;
            }
            resultBuilder.add(thisExpression);
        }
        return Expressions.of(resultBuilder.build());
    }

    @Nonnull
    public Expressions concat(@Nonnull Expressions other) {
        return Expressions.of(Iterables.concat(this.underlying, other.underlying));
    }

    @Nonnull
    public Expressions concat(@Nonnull Expression expression) {
        return Expressions.of(Iterables.concat(this.underlying, ImmutableList.of(expression)));
    }

    @Nonnull
    public Expressions dereferenced(@Nonnull QueryExecutionContext.Literals literals) {
        return Expressions.of(this.stream().flatMap(e -> e.dereferenced(literals).stream()).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public Expressions nonEphemeral() {
        return Expressions.of(stream().filter(e -> !(e instanceof EphemeralExpression)).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public Expression getSingleItem() {
        Assert.thatUnchecked(size() == 1, "invalid attempt to get single item");
        return asList().get(0);
    }

    @Nonnull
    public Set<Value> collectAggregateValues() {
        final ImmutableSet.Builder<Value> resultBuilder = ImmutableSet.builder();
        for (final var expression : this) {
            resultBuilder.addAll(Expression.Utils.filterUnderlyingAggregates(expression));
        }
        return resultBuilder.build();
    }

    @Nonnull
    public Expressions replaceQualifier(@Nonnull Function<Collection<String>, Collection<String>> replaceFunc) {
        return Expressions.of(underlying.stream().map(expression -> expression.replaceQualifier(replaceFunc))
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public Expressions withQualifier(@Nonnull final Identifier qualifier) {
        return Expressions.of(underlying.stream().map(expression -> expression.withQualifier(Optional.of(qualifier)))
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public Expressions clearQualifier() {
        return Expressions.of(underlying.stream().map(Expression::clearQualifier).collect(ImmutableList.toImmutableList()));
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals") // used as optimization
    public boolean isEmpty() {
        return this == empty() || Iterables.isEmpty(this);
    }

    public int size() {
        return underlying.size();
    }

    @Nonnull
    public Iterable<Value> underlying() {
        return Streams.stream(this).map(Expression::getUnderlying).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public List<Type> underlyingTypes() {
        return Streams.stream(underlying()).map(Value::getResultType).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public Stream<Expression> stream() {
        return underlying.stream();
    }

    @Nonnull
    public Collection<Column<? extends Value>> underlyingAsColumns() {
        return Streams.stream(this)
                .map(expression -> Column.of(expression.getName().map(Identifier::getName), expression.getUnderlying()))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public Iterable<Value> underlyingRebased(@Nonnull CorrelationIdentifier source, @Nonnull CorrelationIdentifier target) {
        final var aliasMap = AliasMap.ofAliases(source, target);
        return Streams.stream(underlying()).map(value -> value.rebase(aliasMap)).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public List<Expression> asList() {
        return underlying;
    }

    @Override
    public String toString() {
        return underlying.stream().map(Expression::toString).collect(Collectors.joining(",", "[", "]"));
    }

    @Nonnull
    public static Expressions of(@Nonnull Iterable<Expression> expressions) {
        return new Expressions(expressions);
    }

    @Nonnull
    public static Expressions of(@Nonnull final Expression[] expressions) {
        List<Expression> expressionsList = ImmutableList.copyOf(expressions);
        return Expressions.of(expressionsList);
    }

    @Nonnull
    public static Expressions ofSingle(@Nonnull Expression expression) {
        return new Expressions(ImmutableList.of(expression));
    }

    @Nonnull
    public static Expressions fromQuantifier(@Nonnull Quantifier quantifier) {
        return Expressions.of(quantifier.getFlowedColumns().stream().map(Expression::fromColumn)
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static Expressions fromUnderlying(@Nonnull Iterable<Value> values) {
        return Expressions.of(Streams.stream(values).map(Expression::fromUnderlying).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static Expressions empty() {
        return EMPTY;
    }
}
