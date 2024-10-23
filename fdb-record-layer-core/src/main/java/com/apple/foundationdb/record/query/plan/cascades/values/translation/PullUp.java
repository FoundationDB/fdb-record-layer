/*
 * PullUp.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Chain to pull up {@link Value} trees through a series of relational expressions.
 */
public class PullUp {
    @Nullable
    private final PullUp parentPullUp;
    @Nonnull
    private final CorrelationIdentifier baseAlias;
    @Nonnull
    private final Value pullThroughValue;
    @Nonnull
    private final Set<CorrelationIdentifier> constantAliases;

    private PullUp(@Nullable PullUp parentPullUp,
                   @Nonnull final CorrelationIdentifier baseAlias,
                   @Nonnull final Value pullThroughValue,
                   @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        this.parentPullUp = parentPullUp;
        this.baseAlias = baseAlias;
        this.pullThroughValue = pullThroughValue;
        this.constantAliases = ImmutableSet.copyOf(constantAliases);
    }

    @Nonnull
    private PullUp nest(@Nonnull final CorrelationIdentifier baseAlias,
                        @Nonnull final CorrelationIdentifier nestingAlias,
                        @Nonnull final Value lowerPullThroughValue,
                        @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var completePullThroughValue =
                pullThroughValue.translateCorrelationsAndSimplify(
                        TranslationMap.builder()
                                .when(nestingAlias).then(((sourceAlias, leafValue) -> lowerPullThroughValue))
                                .build());
        return new PullUp(baseAlias, completePullThroughValue, constantAliases);
    }

    @Nonnull
    private PullUp nest(@Nonnull final CorrelationIdentifier baseAlias,
                        @Nonnull final CorrelationIdentifier nestingAlias,
                        @Nonnull final CorrelationIdentifier lowerAlias,
                        @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var completePullThroughValue =
                pullThroughValue.translateCorrelations(
                        TranslationMap.builder()
                                .when(nestingAlias).then(((sourceAlias, leafValue) -> {
                                    if (leafValue instanceof QuantifiedObjectValue) {
                                        return QuantifiedObjectValue.of(lowerAlias, leafValue.getResultType());
                                    }
                                    throw new RecordCoreException("unexpected correlated leaf value");
                                }))
                                .build());
        return new PullUp(baseAlias, completePullThroughValue, constantAliases);
    }

    @Nonnull
    public Optional<Value> pullUp(@Nonnull final Value value) {
        final var pullUpMap =
                pullThroughValue.pullUp(ImmutableList.of(value),
                        AliasMap.emptyMap(),
                        constantAliases,
                        baseAlias);
        return Optional.ofNullable(pullUpMap.get(value));
    }

    @Nonnull
    public Map<Value, Value> pullUp(@Nonnull final List<Value> values, @Nonnull final CorrelationIdentifier upperBaseAlias) {
        return pullThroughValue.pullUp(values,
                AliasMap.emptyMap(),
                constantAliases,
                upperBaseAlias);
    }

    @Nonnull
    public static RelationalExpressionVisitor<PullUp> topVisitor(@Nonnull final CorrelationIdentifier baseAlias) {
        return new TopPullUpVisitor(baseAlias);
    }

    @Nonnull
    public static RelationalExpressionVisitor<PullUp> nestingVisitor(@Nonnull final PullUp pullUp,
                                                                     @Nonnull final CorrelationIdentifier baseAlias,
                                                                     @Nonnull final Quantifier nestingQuantifier) {
        return new NestingPullUpVisitor(pullUp, baseAlias, nestingQuantifier);
    }

    private static class TopPullUpVisitor implements RelationalExpressionVisitorWithDefaults<PullUp> {
        @Nonnull
        private final CorrelationIdentifier baseAlias;

        public TopPullUpVisitor(@Nonnull final CorrelationIdentifier baseAlias) {
            this.baseAlias = baseAlias;
        }

        @Nonnull
        @Override
        public PullUp visitDefault(@Nonnull final RelationalExpression element) {
            return new PullUp(baseAlias, element.getResultValue(), ImmutableSet.of());
        }
    }

    private static class NestingPullUpVisitor implements RelationalExpressionVisitorWithDefaults<PullUp> {
        @Nonnull
        private final PullUp pullUp;
        @Nonnull
        private final CorrelationIdentifier baseAlias;
        @Nonnull
        private final Quantifier nestingQuantifier;

        public NestingPullUpVisitor(@Nonnull final PullUp pullUp, @Nonnull final CorrelationIdentifier baseAlias,
                                    @Nonnull final Quantifier nestingQuantifier) {
            this.pullUp = pullUp;
            this.baseAlias = baseAlias;
            this.nestingQuantifier = nestingQuantifier;
        }

        @Nonnull
        @Override
        public PullUp visitLogicalTypeFilterExpression(@Nonnull final LogicalTypeFilterExpression logicalTypeFilterExpression) {
            return pullUp.nest(baseAlias, nestingQuantifier.getAlias(), logicalTypeFilterExpression.getInner().getAlias(),
                    nestingQuantifier.getCorrelatedTo());
        }

        @Nonnull
        @Override
        public PullUp visitDefault(@Nonnull final RelationalExpression relationalExpression) {
            return pullUp.nest(baseAlias, nestingQuantifier.getAlias(), relationalExpression.getResultValue(),
                    nestingQuantifier.getCorrelatedTo());
        }
    }
}
