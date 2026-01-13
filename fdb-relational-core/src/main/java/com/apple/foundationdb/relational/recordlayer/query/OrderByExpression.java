/*
 * OrderByExpression.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@API(API.Status.EXPERIMENTAL)
public final class OrderByExpression {

    @Nonnull
    private final Expression expression;
    private final boolean descending;
    private final boolean nullsLast;

    private OrderByExpression(@Nonnull Expression expression, boolean descending, boolean nullsLast) {
        this.expression = expression;
        this.descending = descending;
        this.nullsLast = nullsLast;
    }

    @Nonnull
    public Expression getExpression() {
        return expression;
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private OrderByExpression withExpression(@Nonnull Expression expression) {
        if (this.expression == expression) {
            return this;
        }
        return new OrderByExpression(expression, descending, nullsLast);
    }

    @Nonnull
    public static OrderByExpression of(@Nonnull Expression expression, boolean descending, boolean nullsLast) {
        return new OrderByExpression(expression, descending, nullsLast);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    public static Stream<OrderByExpression> pullUp(@Nonnull final Stream<OrderByExpression> orderBys,
                                                   @Nonnull final Value value,
                                                   @Nonnull final CorrelationIdentifier correlationIdentifier,
                                                   @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                                   @Nonnull final Optional<Identifier> qualifier) {
        final var aliasMap = AliasMap.identitiesFor(value.getCorrelatedTo());
        final var simplifiedValue = value.simplify(EvaluationContext.empty(), aliasMap, constantAliases);
        return orderBys
                // expand *
                .flatMap(orderBy ->
                        orderBy.getExpression() instanceof Star ?
                                ((Star) orderBy.getExpression()).getExpansion().stream().map(orderBy::withExpression) :
                                Stream.of(orderBy))
                .map(orderBy -> {
                    final var orderByExpression = orderBy.getExpression();
                    final var underlying = orderByExpression.getUnderlyingValue();
                    final var pulledUpUnderlying = Assert.notNullUnchecked(underlying.replace(
                            subExpression -> {
                                final var pulledUpExpressionMap =
                                        simplifiedValue.pullUp(List.of(subExpression), EvaluationContext.empty(),
                                                aliasMap, constantAliases, correlationIdentifier);
                                if (pulledUpExpressionMap.containsKey(subExpression)) {
                                    return pulledUpExpressionMap.get(subExpression);
                                }
                                return subExpression;
                            }
                    ));
                    final var pulledUpExpression = pulledUpUnderlying == underlying ? orderByExpression :
                            orderByExpression.withUnderlying(pulledUpUnderlying);
                    final var qualifiedExpression = qualifier.map(pulledUpExpression::withName).orElseGet(pulledUpExpression::clearQualifier);
                    if (qualifiedExpression == orderByExpression) {
                        return orderBy;
                    }
                    return orderBy.withExpression(qualifiedExpression);
                });
    }

    @Nonnull
    public OrderingPart.RequestedSortOrder toSortOrder() {
        if (descending) {
            return nullsLast ? OrderingPart.RequestedSortOrder.DESCENDING : OrderingPart.RequestedSortOrder.DESCENDING_NULLS_FIRST;
        } else {
            return nullsLast ? OrderingPart.RequestedSortOrder.ASCENDING_NULLS_LAST : OrderingPart.RequestedSortOrder.ASCENDING;
        }
    }

    @Nonnull
    public static Stream<OrderingPart.RequestedOrderingPart> toOrderingParts(@Nonnull Stream<OrderByExpression> orderBys,
                                                                             @Nonnull CorrelationIdentifier rebaseSource,
                                                                             @Nonnull CorrelationIdentifier rebaseTarget) {
        final var aliasMap = AliasMap.ofAliases(rebaseSource, rebaseTarget);
        return orderBys.map(orderBy -> {
            final var rebased = orderBy.getExpression().getUnderlyingValue().rebase(aliasMap);
            final var sortOrder = orderBy.toSortOrder();
            return new OrderingPart.RequestedOrderingPart(rebased, sortOrder);
        });
    }

    @Override
    public String toString() {
        final StringBuilder str = new StringBuilder(expression.toString());
        if (descending) {
            str.append(" DESC");
        }
        if (descending != nullsLast) {
            str.append(" NULLS ");
            str.append(nullsLast ? "LAST" : "FIRST");
        }
        return str.toString();
    }
}
