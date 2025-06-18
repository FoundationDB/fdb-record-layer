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
import java.util.Optional;

/**
 * Trait that facilitates folding a constant {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
interface ConstantPredicateFoldingTrait {

    /**
     * Analyzes the provided operand and comparison to produce a simplified, semantically equivalent {@link QueryPredicate}.
     *
     * @param operand The operand {@link Value} on the left-hand side of the predicate.
     * @param comparison The comparison, including both the operator and the operand on the right-hand side.
     * @return An {@code Optional} containing the simplified {@link QueryPredicate} if both the operand and comparison
     *         evaluate to constant values. Returns an empty {@code Optional} otherwise.
     */
    default Optional<QueryPredicate> foldComparisonMaybe(@Nonnull final Value operand,
                                                         @Nonnull final Comparisons.Comparison comparison) {
        final var comparisonType = comparison.getType();
        if (comparisonType.isUnary()) {
            switch (comparisonType) {
                case IS_NULL:
                    if (operand instanceof NullValue) {
                        return Optional.of(ConstantPredicate.TRUE);
                    } else if (operand.getResultType().isNotNullable()) {
                        return Optional.of(ConstantPredicate.FALSE);
                    } else {
                        return Optional.empty();
                    }
                case NOT_NULL:
                    if (operand instanceof NullValue) {
                        return Optional.of(ConstantPredicate.FALSE);
                    } else if (operand.getResultType().isNotNullable()) {
                        return Optional.of(ConstantPredicate.TRUE);
                    } else {
                        return Optional.empty();
                    }
                default:
                    return Optional.empty();
            }
        }

        if (!(comparison instanceof Comparisons.ValueComparison)) {
            return Optional.empty();
        }

        final var valueComparison = (Comparisons.ValueComparison)comparison;
        final var rhsValue = valueComparison.getValue();
        final var rhsOperand = EffectiveConstant.from(rhsValue);
        final var lhsOperand = EffectiveConstant.from(operand);

        if (rhsOperand == EffectiveConstant.UNKNOWN || lhsOperand == EffectiveConstant.UNKNOWN) {
            return Optional.empty();
        }

        switch (comparisonType) {
            case EQUALS:
                if (lhsOperand == EffectiveConstant.NULL || rhsOperand == EffectiveConstant.NULL) {
                    return Optional.of(ConstantPredicate.NULL);
                } else if (lhsOperand.equals(rhsOperand)) {
                    return Optional.of(ConstantPredicate.TRUE);
                } else {
                    return Optional.of(ConstantPredicate.FALSE);
                }
            case NOT_EQUALS:
                if (lhsOperand == EffectiveConstant.NULL || rhsOperand == EffectiveConstant.NULL) {
                    return Optional.of(ConstantPredicate.NULL);
                } else if (!lhsOperand.equals(rhsOperand)) {
                    return Optional.of(ConstantPredicate.TRUE);
                } else {
                    return Optional.of(ConstantPredicate.FALSE);
                }
            default:
                return Optional.empty();
        }
    }

    enum EffectiveConstant {
        TRUE,
        FALSE,
        NULL,
        UNKNOWN;

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

            return EffectiveConstant.UNKNOWN;
        }
    }
}
