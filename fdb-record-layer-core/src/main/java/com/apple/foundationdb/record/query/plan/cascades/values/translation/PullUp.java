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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Chain to pull up {@link Value} trees through a series of relational expressions.
 */
public class PullUp {
    @Nullable
    private final PullUp parentPullUp;
    @Nonnull
    private final CorrelationIdentifier nestingAlias;
    @Nonnull
    private final Value pullThroughValue;
    @Nonnull
    private final Set<CorrelationIdentifier> constantAliases;

    @Nonnull
    private final Supplier<CorrelationIdentifier> topAliasSupplier;

    private PullUp(@Nullable PullUp parentPullUp,
                   @Nonnull final CorrelationIdentifier nestingAlias,
                   @Nonnull final Value pullThroughValue,
                   @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        this.parentPullUp = parentPullUp;
        this.nestingAlias = nestingAlias;
        this.pullThroughValue = pullThroughValue;
        this.constantAliases = ImmutableSet.copyOf(constantAliases);
        this.topAliasSupplier = Suppliers.memoize(this::computeTopAlias);
    }

    @Nullable
    public PullUp getParentPullUp() {
        return parentPullUp;
    }

    public boolean isRoot() {
        return parentPullUp == null;
    }

    @Nonnull
    public Value getPullThroughValue() {
        return pullThroughValue;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getConstantAliases() {
        return constantAliases;
    }

    @Nonnull
    public CorrelationIdentifier getNestingAlias() {
        return nestingAlias;
    }

    @Nonnull
    public CorrelationIdentifier getTopAlias() {
        return topAliasSupplier.get();
    }

    @Nonnull
    private CorrelationIdentifier computeTopAlias() {
        for (var currentPullUp = this; ; ) {
            if (currentPullUp.getParentPullUp() == null) {
                return currentPullUp.getNestingAlias();
            }
            currentPullUp = currentPullUp.getParentPullUp();
        }
    }

    @Nonnull
    private PullUp nest(@Nonnull final CorrelationIdentifier nestingAlias,
                        @Nonnull final Value lowerPullThroughValue,
                        @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return new PullUp(this, nestingAlias, lowerPullThroughValue, constantAliases);
    }

    @Nonnull
    private PullUp nest(@Nonnull final CorrelationIdentifier nestingAlias,
                        @Nonnull final CorrelationIdentifier lowerAlias,
                        @Nonnull final Type lowerType,
                        @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return new PullUp(this, nestingAlias, QuantifiedObjectValue.of(lowerAlias, lowerType), constantAliases);
    }

    @Nonnull
    public Optional<Value> pullUpMaybe(@Nonnull final Value value) {
        //
        // The following loop would probably be more self-explanatory if it were written as a recursion but
        // this unrolled version probably performs better as this may prove to be a tight loop.
        //
        var currentValue = value;
        for (var currentPullUp = this; ; ) {
            final var maxMatchMap =
                    MaxMatchMap.calculate(currentValue, currentPullUp.getPullThroughValue());
            final var currentValueOptional =
                    maxMatchMap.translateQueryValueMaybe(currentPullUp.getNestingAlias());
            if (currentValueOptional.isEmpty()) {
                return Optional.empty();
            }
            currentValue = currentValueOptional.get()
                    .simplify(AliasMap.emptyMap(), currentPullUp.getConstantAliases());

            if (currentPullUp.getParentPullUp() == null) {
                return Optional.of(currentValue);
            }
            currentPullUp = currentPullUp.getParentPullUp();
        }
    }

    @Nonnull
    public static RelationalExpressionVisitor<PullUp> topVisitor(@Nonnull final CorrelationIdentifier baseAlias) {
        return new TopPullUpVisitor(baseAlias);
    }

    @Nonnull
    public static RelationalExpressionVisitor<PullUp> nestingVisitor(@Nonnull final PullUp pullUp,
                                                                     @Nonnull final CorrelationIdentifier nestingAlias) {
        return new NestingPullUpVisitor(pullUp, nestingAlias);
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
            return new PullUp(null, baseAlias, element.getResultValue(), ImmutableSet.of());
        }
    }

    private static class NestingPullUpVisitor implements RelationalExpressionVisitorWithDefaults<PullUp> {
        @Nonnull
        private final PullUp pullUp;
        @Nonnull
        private final CorrelationIdentifier nestingAlias;

        public NestingPullUpVisitor(@Nonnull final PullUp pullUp,
                                    @Nonnull final CorrelationIdentifier nestingAlias) {
            this.pullUp = pullUp;
            this.nestingAlias = nestingAlias;
        }

        @Nonnull
        @Override
        public PullUp visitLogicalTypeFilterExpression(@Nonnull final LogicalTypeFilterExpression logicalTypeFilterExpression) {
            return pullUp.nest(nestingAlias,
                    logicalTypeFilterExpression.getInner().getAlias(),
                    logicalTypeFilterExpression.getInner().getFlowedObjectType(),
                    logicalTypeFilterExpression.getCorrelatedTo());
        }

        @Nonnull
        @Override
        public PullUp visitDefault(@Nonnull final RelationalExpression relationalExpression) {
            return pullUp.nest(nestingAlias, relationalExpression.getResultValue(),
                    relationalExpression.getCorrelatedTo());
        }
    }
}
