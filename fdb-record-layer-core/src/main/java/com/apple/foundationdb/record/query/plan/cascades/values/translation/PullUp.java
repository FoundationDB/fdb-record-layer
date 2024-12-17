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
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

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
    private final Set<CorrelationIdentifier> rangedOverAliases;

    @Nonnull
    private final PullUp rootPullUp;

    private PullUp(@Nullable final PullUp parentPullUp,
                   @Nonnull final CorrelationIdentifier nestingAlias,
                   @Nonnull final Value pullThroughValue,
                   @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        this.parentPullUp = parentPullUp;
        this.nestingAlias = nestingAlias;
        this.pullThroughValue = pullThroughValue;
        this.rangedOverAliases = ImmutableSet.copyOf(rangedOverAliases);
        this.rootPullUp = parentPullUp == null ? this : parentPullUp.getRootPullUp();
    }

    @Nullable
    public PullUp getParentPullUp() {
        return parentPullUp;
    }

    @Nonnull
    public PullUp getRootPullUp() {
        return rootPullUp;
    }

    @Nonnull
    public CorrelationIdentifier getNestingAlias() {
        return nestingAlias;
    }

    public boolean isRoot() {
        return parentPullUp == null;
    }

    @Nonnull
    public Value getPullThroughValue() {
        return pullThroughValue;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getRangedOverAliases() {
        return rangedOverAliases;
    }

    @Nonnull
    public CorrelationIdentifier getTopAlias() {
        return getRootPullUp().getNestingAlias();
    }

    @Nonnull
    private static PullUp of(@Nullable final PullUp parentPullUp,
                             @Nonnull final CorrelationIdentifier nestingAlias,
                             @Nonnull final CorrelationIdentifier lowerAlias,
                             @Nonnull final Type lowerType,
                             @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        return of(parentPullUp, nestingAlias, QuantifiedObjectValue.of(lowerAlias, lowerType),
                rangedOverAliases);
    }

    @Nonnull
    private static PullUp of(@Nullable final PullUp parentPullUp,
                             @Nonnull final CorrelationIdentifier nestingAlias,
                             @Nonnull final Value lowerPullThroughValue,
                             @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        return new PullUp(parentPullUp, nestingAlias, lowerPullThroughValue, rangedOverAliases);
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
                    MaxMatchMap.calculate(currentValue, currentPullUp.getPullThroughValue(),
                            currentPullUp.getRangedOverAliases());
            final var currentValueOptional =
                    maxMatchMap.translateQueryValueMaybe(currentPullUp.getNestingAlias());
            if (currentValueOptional.isEmpty()) {
                return Optional.empty();
            }
            currentValue = currentValueOptional.get()
                    .simplify(AliasMap.emptyMap(), currentPullUp.getRangedOverAliases());

            if (currentPullUp.getParentPullUp() == null) {
                return Optional.of(currentValue);
            }
            currentPullUp = currentPullUp.getParentPullUp();
        }
    }

    @Nonnull
    public static RelationalExpressionVisitor<PullUp> visitor(@Nullable final PullUp parentPullUp,
                                                              @Nonnull final CorrelationIdentifier nestingAlias) {
        return new PullUpVisitor(parentPullUp, nestingAlias);
    }

    private static class PullUpVisitor implements RelationalExpressionVisitorWithDefaults<PullUp> {
        @Nullable
        private final PullUp parentPullUp;
        @Nonnull
        private final CorrelationIdentifier nestingAlias;

        public PullUpVisitor(@Nullable final PullUp parentPullUp,
                             @Nonnull final CorrelationIdentifier nestingAlias) {
            this.parentPullUp = parentPullUp;
            this.nestingAlias = nestingAlias;
        }

        @Nonnull
        @Override
        public PullUp visitLogicalTypeFilterExpression(@Nonnull final LogicalTypeFilterExpression logicalTypeFilterExpression) {
            return of(parentPullUp, nestingAlias, logicalTypeFilterExpression.getInner().getAlias(),
                    logicalTypeFilterExpression.getInner().getFlowedObjectType(),
                    Quantifiers.aliases(logicalTypeFilterExpression.getQuantifiers()));
        }

        @Nonnull
        @Override
        public PullUp visitDefault(@Nonnull final RelationalExpression relationalExpression) {
            return of(parentPullUp, nestingAlias, relationalExpression.getResultValue(),
                    Quantifiers.aliases(relationalExpression.getQuantifiers()));
        }
    }
}
