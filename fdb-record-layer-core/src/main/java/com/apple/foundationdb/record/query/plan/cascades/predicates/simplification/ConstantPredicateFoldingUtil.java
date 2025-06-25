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

    /**
     * Analyzes the provided operand and comparison to produce a simplified, semantically equivalent {@link QueryPredicate}.
     *
     * @param operand The operand {@link Value} on the left-hand side of the predicate.
     * @param comparison The comparison, including both the operator and the operand on the right-hand side.
     * @return An {@code Optional} containing the simplified {@link QueryPredicate} if both the operand and comparison
     *         evaluate to constant values. Returns an empty {@code Optional} otherwise.
     */
    public static Optional<QueryPredicate> foldComparisonMaybe(@Nonnull final Value operand,
                                                               @Nonnull final Comparisons.Comparison comparison) {
        final var comparisonType = comparison.getType();
        final var lhsOperand = EffectiveConstant.from(operand);
        if (comparisonType.isUnary()) {
            switch (comparisonType) {
                case IS_NULL:
                    switch (lhsOperand) {
                        case NULL:
                            return Optional.of(ConstantPredicate.TRUE);
                        case TRUE: // fallthrough
                        case FALSE: // fallthrough
                        case NOT_NULL:
                            return Optional.of(ConstantPredicate.FALSE);
                        default:
                            return Optional.empty();
                    }
                case NOT_NULL:
                    switch (lhsOperand) {
                        case NULL:
                            return Optional.of(ConstantPredicate.FALSE);
                        case TRUE: // fallthrough
                        case FALSE: // fallthrough
                        case NOT_NULL:
                            return Optional.of(ConstantPredicate.TRUE);
                        default:
                            return Optional.empty();
                    }
                default:
                    return Optional.empty();
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
            return Optional.empty();
        }

        switch (comparisonType) {
            case EQUALS: // fallthrough
            case NOT_EQUALS:
                if (lhsOperand == EffectiveConstant.NULL || rhsOperand == EffectiveConstant.NULL) {
                    return Optional.of(ConstantPredicate.NULL);
                }
                break;
            default:
                return Optional.empty();
        }

        if (rhsOperand.isUnknownLiteral() || lhsOperand.isUnknownLiteral()) {
            return Optional.empty();
        }

        switch (comparisonType) {
            case EQUALS:
                if (lhsOperand.equals(rhsOperand)) {
                    return Optional.of(ConstantPredicate.TRUE);
                } else {
                    return Optional.of(ConstantPredicate.FALSE);
                }
            case NOT_EQUALS:
                if (!lhsOperand.equals(rhsOperand)) {
                    return Optional.of(ConstantPredicate.TRUE);
                } else {
                    return Optional.of(ConstantPredicate.FALSE);
                }
            default:
                return Optional.empty();
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
