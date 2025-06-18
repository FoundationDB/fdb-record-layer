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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Trait that facilitates folding a constant {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
interface ConstantPredicateFoldingTrait {

    /**
     * Analyzes the provided operand and comparison to produce a simplified, semantically equivalent {@link QueryPredicate}.
     *
     * @param operand The operand on the left-hand side of the predicate.
     * @param comparison The comparison, including both the operator and the operand on the right-hand side.
     * @return An {@code Optional} containing the simplified {@link QueryPredicate} if both the operand and comparison
     *         evaluate to constant values. Returns an empty {@code Optional} otherwise.
     */
    @SuppressWarnings("unchecked")
    default Optional<QueryPredicate> foldComparisonMaybe(@Nullable final Object operand,
                                                         @Nonnull final Comparisons.Comparison comparison) {
        final var comparisonType = comparison.getType();
        if (comparisonType.isUnary()) {
            switch (comparisonType) {
                case IS_NULL:
                    if (operand == null) {
                        return Optional.of(ConstantPredicate.TRUE);
                    } else {
                        return Optional.of(ConstantPredicate.FALSE);
                    }
                case NOT_NULL:
                    if (operand == null) {
                        return Optional.of(ConstantPredicate.FALSE);
                    } else {
                        return Optional.of(ConstantPredicate.TRUE);
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

        if (rhsValue instanceof NullValue ||
                rhsValue.getResultType().getTypeCode() == Type.TypeCode.BOOLEAN && rhsValue instanceof LiteralValue<?>) {

            final Object rhsOperand;
            if (rhsValue instanceof NullValue) {
                rhsOperand = null;
            } else {
                rhsOperand = ((LiteralValue<Boolean>)rhsValue).getLiteralValue();
            }

            switch (comparisonType) {
                case EQUALS:
                    if (operand == null || rhsOperand == null) {
                        return Optional.of(ConstantPredicate.NULL);
                    } else if (operand.equals(rhsOperand)) {
                        return Optional.of(ConstantPredicate.TRUE);
                    } else {
                        return Optional.of(ConstantPredicate.FALSE);
                    }
                case NOT_EQUALS:
                    if (operand == null || rhsOperand == null) {
                        return Optional.of(ConstantPredicate.NULL);
                    } else if (!operand.equals(rhsOperand)) {
                        return Optional.of(ConstantPredicate.TRUE);
                    } else {
                        return Optional.of(ConstantPredicate.FALSE);
                    }
                default:
                    break;
            }
        }

        return Optional.empty();
    }

}
