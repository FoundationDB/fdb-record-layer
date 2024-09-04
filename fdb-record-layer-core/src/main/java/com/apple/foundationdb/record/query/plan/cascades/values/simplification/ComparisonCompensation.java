/*
 * ComparisonCompensation.java
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
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
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
 * Interface to define compensation for partially matched {@link Value}-trees and their associated comparisons, as
 * e.g. used by query predicates.
 * Consider a predicate whose comparing {@link Value} is compared using a {@link Comparisons.Comparison} (which may
 * in turn also contain a {@link Value} tree). When the comparing value is matched to a placeholder in a
 * match candidate, it may exactly match ({@code fieldValue(qov(q), a) --> fieldValue(qov(q'), a)}), or it may match
 * but only through a subtree, i.e. ({@code fieldValue(qov(q), a) --> to_ordered_bytes(fieldValue(qov(q'), a), "↓")}).
 * In this particular case, we know that {@code to_ordered_bytes(...)} is applied on top of the field value, thus,
 * the comparison (the right side) needs to be adjusted to also apply {@code to_ordered_bytes(...)} on top of the
 * original comparison. This would strictly speaking only sufficient if the comparison is an equality comparison.
 * For all inequalities we need to reason about order-preserving properties of the values we try to adjust the
 * comparison with. In this case, to_ordered_bytes(..., "↓") is inversely order-preserving, thus e.g. a
 * {@code [> some value]} can be adjusted to {@code [< to_ordered_bytes(some value, "↓")]}.
 * See also {@link Value#matchAndCompensateComparisonMaybe(Value, ValueEquivalence)} for a detailed example.
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

    /**
     * Apply the compensation to a value tree.
     * @param value the original value
     * @return the compensated value
     */
    @Nonnull
    Value applyToValue(@Nonnull Value value);

    /**
     * Unapply the compensation. For example, if {@code value} is {@code [< to_ordered_bytes(some value, "↓")]} and the
     * compensation was computed by matching {@code fieldValue(qov(q), a) --> to_ordered_bytes(fieldValue(qov(q'), a), "↓")},
     * this method returns {@code some value}
     * @param value the value to unapply the compensation to
     * @return the original value
     */
    @Nonnull
    Optional<Value> unapplyMaybe(@Nonnull Value value);

    /**
     * Apply the compensation to a value tree.
     * @param comparison the original comparison
     * @return the compensated adjusted comparison
     */
    Optional<Comparisons.Comparison> applyToComparisonMaybe(@Nonnull Comparisons.Comparison comparison);

    @Nonnull
    static ComparisonCompensation noCompensation() {
        return NO_COMPENSATION;
    }

    /**
     * Nested chaining comparison compensation.
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
