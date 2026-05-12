/*
 * WindowExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowedValue;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@API(API.Status.EXPERIMENTAL)
public class WindowExpression extends Expression {

    @Nonnull
    private final List<OrderByExpression> orderByExpressions;

    public WindowExpression(@Nullable final Identifier name, @Nonnull final DataType dataType,
                            @Nonnull final Iterable<OrderByExpression> orderByExpressions,
                            @Nonnull final WindowedValue windowedValue) {
        this(name, dataType, orderByExpressions, windowedValue, Visibility.VISIBLE);
    }

    public WindowExpression(@Nullable final Identifier name, @Nonnull final DataType dataType,
                            @Nonnull final Iterable<OrderByExpression> orderByExpressions,
                            @Nonnull final WindowedValue windowedValue,
                            @Nonnull final Visibility visibility) {
        super(Optional.ofNullable(name), dataType, windowedValue, visibility);
        this.orderByExpressions = ImmutableList.copyOf(orderByExpressions);
    }

    @Nonnull
    @Override
    protected Expression createNew(@Nonnull final Optional<Identifier> newName, @Nonnull final DataType newDataType,
                                   @Nonnull final Value newUnderlying, @Nonnull final Visibility newVisibility) {
        if (newUnderlying instanceof WindowedValue) {
            return new WindowExpression(newName.orElse(null), newDataType, orderByExpressions,
                    (WindowedValue)newUnderlying, newVisibility);
        } else {
            return new Expression(newName, newDataType, newUnderlying);
        }
    }

    @Nonnull
    public List<OrderByExpression> getOrderByExpressions() {
        return orderByExpressions;
    }

    @Nonnull
    @Override
    public WindowedValue getUnderlying() {
        return (WindowedValue)super.getUnderlying();
    }

    @Nonnull
    public Expressions getPartitioningAndOrderingParts() {
        final var partitioningExprs = Expressions.fromUnderlying(getUnderlying().getPartitioningValues());
        final var orderByExprs = Expressions.fromUnderlying(orderByExpressions.stream().map(OrderByExpression::getExpression)
                .map(Expression::getUnderlying).collect(ImmutableList.toImmutableList()));
        return partitioningExprs.concat(orderByExprs);
    }

    @Nonnull
    public WindowExpression adjustOrderingParts(@Nonnull final Value value,
                                             @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var orderingParts = orderByExpressions.stream().map(obe -> obe.withExpression(obe.getExpression().pullUp(value, Quantifier.current(), constantAliases)))
                .map(obe -> new OrderingPart.RequestedOrderingPart(obe.getExpression().getUnderlying(), obe.toSortOrder()))
                .collect(ImmutableList.toImmutableList());
        return (WindowExpression)withUnderlying(getUnderlying().withOrderingParts(orderingParts));
    }

    @Nonnull
    private Expressions computeExpansion() {
        return Expressions.fromUnderlying(getUnderlying().getOrderingParts().stream()
                        .map(OrderingPart::getValue).collect(ImmutableList.toImmutableList()))
                .concat(this);
    }

    @Override
    public boolean isWindow() {
        return true;
    }
}
