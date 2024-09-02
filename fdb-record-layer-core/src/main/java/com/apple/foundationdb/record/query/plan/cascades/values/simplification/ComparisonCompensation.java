/*
 * ValueCompensation.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Value.InvertableValue;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;

/**
 * Functional interface to perform compensation for partially matched {@link Value}-trees.
 */
public interface ComparisonCompensation {
    ComparisonCompensation NO_COMPENSATION = new ComparisonCompensation() {
        @Nonnull
        @Override
        public Value applyToValue(@Nonnull final Value value) {
            return value;
        }

        @Nonnull
        @Override
        public Optional<Value> unapplyMaybe(@Nonnull final Value value) {
            return Optional.of(value);
        }

        @Override
        public Optional<Comparisons.Comparison> applyToComparisonMaybe(@Nonnull final Comparisons.Comparison comparison) {
            return Optional.of(comparison);
        }
    };

    @Nonnull
    Value applyToValue(@Nonnull Value value);

    @Nonnull
    Optional<Value> unapplyMaybe(@Nonnull Value value);

    Optional<Comparisons.Comparison> applyToComparisonMaybe(@Nonnull Comparisons.Comparison comparison);

    @Nonnull
    static ComparisonCompensation noCompensation() {
        return NO_COMPENSATION;
    }

    /**
     * Chaining comparison compensation.
     */
    class NestedInvertableComparisonCompensation implements ComparisonCompensation {
        private final InvertableValue<?> otherCurrent;
        private final NonnullPair<ComparisonCompensation, QueryPlanConstraint> childResult;

        public NestedInvertableComparisonCompensation(final InvertableValue<?> otherCurrent,
                                                      final NonnullPair<ComparisonCompensation, QueryPlanConstraint> childResult) {
            this.otherCurrent = otherCurrent;
            this.childResult = childResult;
        }

        @Nonnull
        @Override
        public Value applyToValue(@Nonnull final Value value) {
            final var childCompensation = childResult.getLeft();
            return otherCurrent.withChildren(ImmutableList.of(childCompensation.applyToValue(value)));
        }

        @Nonnull
        @Override
        public Optional<Value> unapplyMaybe(@Nonnull final Value value) {
            final var equalsWithoutChildren = otherCurrent.equalsWithoutChildren(value);
            if (equalsWithoutChildren.isTrue() && equalsWithoutChildren.getConstraint().getPredicate().isTautology()) {
                final var nestedComparisonCompensation = childResult.getLeft();
                return nestedComparisonCompensation.unapplyMaybe(Iterables.getOnlyElement(value.getChildren()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<Comparisons.Comparison> applyToComparisonMaybe(@Nonnull final Comparisons.Comparison comparison) {
            Verify.verify(comparison instanceof Comparisons.ValueComparison ||
                    comparison instanceof Comparisons.SimpleComparison);
            final var childCompensation = childResult.getLeft();
            final var compensatedChildrenComparisonOptional = childCompensation.applyToComparisonMaybe(comparison);
            return compensatedChildrenComparisonOptional.flatMap(compensatedChildrenComparison -> {
                final Ordering.OrderPreservingKind orderPreservingKind =
                        (otherCurrent instanceof Ordering.OrderPreservingValue)
                        ? ((Ordering.OrderPreservingValue)otherCurrent).getOrderPreservingKind()
                        : Ordering.OrderPreservingKind.NOT_ORDER_PRESERVING;
                return adjustComparisonTypeMaybe(compensatedChildrenComparison.getType(), orderPreservingKind)
                        .map(adjustedComparisonType -> comparison.withType(adjustedComparisonType)
                                .withValue(otherCurrent.withChildren(ImmutableList.of(Objects.requireNonNull(comparison.getValue())))));
            });
        }

        @Nonnull
        private static Optional<Comparisons.Type> adjustComparisonTypeMaybe(@Nonnull final Comparisons.Type comparisonType,
                                                                            @Nonnull final Ordering.OrderPreservingKind orderPreservingKind) {
            if (comparisonType.isEquality()) {
                return Optional.of(comparisonType);
            }

            switch (orderPreservingKind) {
                case NOT_ORDER_PRESERVING:
                    return Optional.empty();
                case DIRECT_ORDER_PRESERVING:
                    switch (comparisonType) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUALS:
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUALS:
                            return Optional.of(comparisonType);
                        default:
                            return Optional.empty();
                    }
                case INVERSE_ORDER_PRESERVING:
                    switch (comparisonType) {
                        case LESS_THAN:
                            return Optional.of(Comparisons.Type.GREATER_THAN);
                        case LESS_THAN_OR_EQUALS:
                            return Optional.of(Comparisons.Type.GREATER_THAN_OR_EQUALS);
                        case GREATER_THAN:
                            return Optional.of(Comparisons.Type.LESS_THAN);
                        case GREATER_THAN_OR_EQUALS:
                            return Optional.of(Comparisons.Type.LESS_THAN_OR_EQUALS);
                        default:
                            return Optional.empty();
                    }
                default:
                    throw new RecordCoreException("unknown order preserving enum");
            }
        }
    }
}
