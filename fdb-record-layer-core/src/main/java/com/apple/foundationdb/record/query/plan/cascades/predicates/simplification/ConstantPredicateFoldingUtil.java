/*
 * ConstantPredicateFoldingTrait.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Trait that facilitates folding a constant {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
public final class ConstantPredicateFoldingUtil {

    public enum PredicateCategory {
        SINGLETON_NULL(ConstantPredicate.NULL),
        SINGLETON_TRUE(ConstantPredicate.TRUE),
        SINGLETON_FALSE(ConstantPredicate.FALSE),
        UNKNOWN(null),
        ;

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        @Nonnull
        private final Optional<ConstantPredicate> predicateMaybe;

        PredicateCategory(@Nullable ConstantPredicate predicate) {
            this.predicateMaybe = Optional.ofNullable(predicate);
        }

        @Nonnull
        Optional<ConstantPredicate> getPredicateMaybe() {
            return predicateMaybe;
        }
    }

    /**
     * Analyzes the provided operand and comparison to produce a simplified, semantically equivalent {@link QueryPredicate}.
     *
     * @param operand The operand {@link Value} on the left-hand side of the predicate.
     * @param comparison The comparison, including both the operator and the operand on the right-hand side.
     * @return An {@code Optional} containing the simplified {@link QueryPredicate} if both the operand and comparison
     *         evaluate to constant values. Returns an empty {@code Optional} otherwise.
     */
    public static PredicateCategory foldComparisonMaybe(@Nonnull final Value operand,
                                                        @Nonnull final Comparisons.Comparison comparison) {
        final var comparisonType = comparison.getType();
        final var lhsOperand = EffectiveConstant.from(operand);
        if (comparisonType.isUnary()) {
            switch (comparisonType) {
                case IS_NULL:
                    switch (lhsOperand) {
                        case NULL:
                            return PredicateCategory.SINGLETON_TRUE;
                        case TRUE: // fallthrough
                        case FALSE: // fallthrough
                        case NOT_NULL:
                            return PredicateCategory.SINGLETON_FALSE;
                        default:
                            return PredicateCategory.UNKNOWN;
                    }
                case NOT_NULL:
                    switch (lhsOperand) {
                        case NULL:
                            return PredicateCategory.SINGLETON_FALSE;
                        case TRUE: // fallthrough
                        case FALSE: // fallthrough
                        case NOT_NULL:
                            return PredicateCategory.SINGLETON_TRUE;
                        default:
                            return PredicateCategory.UNKNOWN;
                    }
                default:
                    return PredicateCategory.UNKNOWN;
            }
        }

        EffectiveConstant rhsOperand;

        if (comparison instanceof Comparisons.ValueComparison) {
            final var valueComparison = (Comparisons.ValueComparison)comparison;
            final var rhsValue = valueComparison.getValue();
            rhsOperand = EffectiveConstant.from(rhsValue);
        } else if (comparison instanceof Comparisons.SimpleComparison) {
            final var simpleComparison = (Comparisons.SimpleComparison)comparison;
            final var rhsValue = simpleComparison.getComparand();
            rhsOperand = EffectiveConstant.from(rhsValue);
        } else {
            return PredicateCategory.UNKNOWN;
        }

        switch (comparisonType) {
            case EQUALS: // fallthrough
            case NOT_EQUALS:
                if (lhsOperand == EffectiveConstant.NULL || rhsOperand == EffectiveConstant.NULL) {
                    return PredicateCategory.SINGLETON_NULL;
                }
                break;
            default:
                return PredicateCategory.UNKNOWN;
        }

        if (rhsOperand.isUnknownLiteral() || lhsOperand.isUnknownLiteral()) {
            return PredicateCategory.UNKNOWN;
        }

        switch (comparisonType) {
            case EQUALS:
                if (lhsOperand.equals(rhsOperand)) {
                    return PredicateCategory.SINGLETON_TRUE;
                } else {
                    return PredicateCategory.SINGLETON_FALSE;
                }
            case NOT_EQUALS:
                if (!lhsOperand.equals(rhsOperand)) {
                    return PredicateCategory.SINGLETON_TRUE;
                } else {
                    return PredicateCategory.SINGLETON_FALSE;
                }
            default:
                return PredicateCategory.UNKNOWN;
        }
    }

    public enum EffectiveConstant {
        TRUE(true),
        FALSE(true),
        NULL(true),
        NOT_NULL(false),
        UNKNOWN(false);

        private final boolean isKnownLiteral;

        EffectiveConstant(final boolean isKnownLiteral) {
            this.isKnownLiteral = isKnownLiteral;
        }

        boolean isUnknownLiteral() {
            return !isKnownLiteral;
        }

        public static EffectiveConstant from(@Nullable Object value) {
            if (value == null) {
                return EffectiveConstant.NULL;
            }

            if (value instanceof Value) {
                return from((Value)value);
            }

            if (value instanceof Boolean) {
                if ((Boolean)value) {
                    return EffectiveConstant.TRUE;
                }
                return EffectiveConstant.FALSE;
            }

            return EffectiveConstant.NOT_NULL;
        }

        public static EffectiveConstant from(@Nonnull final Value value) {
            if (value instanceof NullValue) {
                return EffectiveConstant.NULL;
            }

            if (value.getResultType().getTypeCode() == Type.TypeCode.BOOLEAN && value instanceof LiteralValue<?>) {
                final var plainValue = (Boolean)((LiteralValue<?>)value).getLiteralValue();
                if (plainValue == null) {
                    return EffectiveConstant.NULL;
                } else if (plainValue) {
                    return EffectiveConstant.TRUE;
                } else {
                    return EffectiveConstant.FALSE;
                }
            }
            if (value.getResultType().isNotNullable()) {
                return EffectiveConstant.NOT_NULL;
            }
            return EffectiveConstant.UNKNOWN;
        }
    }
}
