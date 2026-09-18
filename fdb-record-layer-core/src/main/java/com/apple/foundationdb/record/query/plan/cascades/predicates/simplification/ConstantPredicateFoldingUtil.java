/*
 * ConstantPredicateFoldingUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.RegularTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Trait that facilitates folding a constant {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
public final class ConstantPredicateFoldingUtil {

    /**
     * Analyzes the provided operand and comparison to produce a simplified, semantically equivalent
     * {@link QueryPredicate}.
     *
     * @param operand The operand {@link Value} on the left-hand side of the predicate.
     * @param comparison The comparison, including both the operator and the operand on the right-hand side.
     *
     * @return An {@code Optional} containing the simplified {@link QueryPredicate} if both the operand and comparison
     * evaluate to constant values. Returns an empty {@code Optional} otherwise.
     */
    public static PredicateCategory foldComparisonMaybe(@Nonnull final Value operand,
                                                        @Nonnull final Comparisons.Comparison comparison) {
        final var comparisonType = comparison.getType();
        final var lhsOperand = EffectiveConstant.from(operand);
        if (comparisonType.isUnary()) {
            switch (comparisonType) {
                case IS_NULL:
                    return switch (lhsOperand) {
                        case NULL -> PredicateCategory.SINGLETON_TRUE;
                        case TRUE, FALSE, NOT_NULL -> PredicateCategory.SINGLETON_FALSE;
                        default -> PredicateCategory.UNKNOWN;
                    };
                case NOT_NULL:
                    return switch (lhsOperand) {
                        case NULL -> PredicateCategory.SINGLETON_FALSE;
                        case TRUE, FALSE, NOT_NULL -> PredicateCategory.SINGLETON_TRUE;
                        default -> PredicateCategory.UNKNOWN;
                    };
                default:
                    return PredicateCategory.UNKNOWN;
            }
        }

        EffectiveConstant rhsOperand;

        if (comparison instanceof final Comparisons.ValueComparison valueComparison) {
            final var rhsValue = valueComparison.getValue();
            rhsOperand = EffectiveConstant.from(rhsValue);
        } else if (comparison instanceof final Comparisons.SimpleComparison simpleComparison) {
            final var rhsValue = simpleComparison.getComparand();
            rhsOperand = EffectiveConstant.from(rhsValue);
        } else {
            return PredicateCategory.UNKNOWN;
        }

        switch (comparisonType) {
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case STARTS_WITH:
                // Standard binary comparisons all return NULL when either operand is NULL.
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

    /**
     * Converts a {@link ConstantPredicate} to a {@link EffectiveConstant}.
     */
    @Nonnull
    private static EffectiveConstant toEffectiveConstant(final ConstantPredicate constantPredicate) {
        if (constantPredicate.isTautology()) {
            return EffectiveConstant.TRUE;
        } else if (constantPredicate.isContradiction()) {
            return EffectiveConstant.FALSE;
        } else {
            return EffectiveConstant.NULL;
        }
    }

    /**
     * Folds {@code predicate} under the assumption that {@code alias} evaluates to {@code NULL}. This is done by
     * substituting a typed {@link NullValue} for every leaf value correlated to {@code alias} and running the
     * constant-folding predicate-simplification rule set on the result.
     *
     * <p>This method can be used to analyze whether a predicate provably “accepts null” or “rejects null”.
     * A predicate {@code p} <i>accepts null</i> at alias {@code q} if evaluating {@code p} on a tuple where
     * {@code q} is bound to {@code NULL} yields {@code TRUE}. Conversely, {@code p} <i>rejects null</i> at {@code q}
     * if it yields {@code FALSE} or {@code UNKNOWN}. (In SQL {@code WHERE}/{@code HAVING}/{@code ON} clauses, those
     * two outcomes are indistinguishable, as the row gets filtered out either way.)
     *
     * <p>The four possible outcomes are:
     * <ul>
     *     <li>{@link EffectiveConstant#TRUE TRUE} — The predicate folds to {@link ConstantPredicate#TRUE} and provably
     *     accepts null at {@code alias}.</li>
     *     <li>{@link EffectiveConstant#FALSE FALSE} — The predicate folds to {@link ConstantPredicate#FALSE} and
     *     provably rejects null.</li>
     *     <li>{@link EffectiveConstant#NULL NULL} — The predicate folds to {@link ConstantPredicate#NULL} and
     *     provably rejects null.</li>
     *     <li>{@link EffectiveConstant#UNKNOWN UNKNOWN} — The simplifier could not fold to a constant. This can happen
     *     when the predicate references aliases other than {@code alias} that aren't constant in
     *     {@code evaluationContext}, or when it involves comparison types that the constant folding cannot handle.</li>
     * </ul>
     *
     * @param predicate the predicate to analyze
     * @param alias the quantifier alias to substitute with null
     * @param evaluationContext the context in which any remaining parameter references should be resolved
     *
     * @return the determined {@link EffectiveConstant}
     */
    public static EffectiveConstant foldPredicateAtNull(@Nonnull final QueryPredicate predicate,
                                                        @Nonnull final CorrelationIdentifier alias,
                                                        @Nonnull final EvaluationContext evaluationContext) {
        final RegularTranslationMap nullifyingMap = TranslationMap.regularBuilder()
                .when(alias)
                .then((sourceAlias, leafValue) -> new NullValue(leafValue.getResultType()))
                .build();
        final QueryPredicate nullifiedPredicate = predicate.translateCorrelations(nullifyingMap, true);
        // TODO Issue #4222: Make `optimize()` aware of the filtering context (i.e., that NULL ≡ FALSE in a WHERE/ON/HAVING filter).
        //    This would help with predicates that mix correlations to {@code alias} with correlations to other,
        //    non-constant aliases, such as `q.a = 42 AND r.b > 10`, which after substitution becomes `NULL AND r.b > 10`.
        //    `NULL AND …` is provably not-TRUE, but the simplifier cannot fold that unless it assumes NULL ≡ FALSE.
        final QueryPredicate simplifiedPredicate = Simplification.optimize(nullifiedPredicate,
                evaluationContext,
                AliasMap.emptyMap(),
                ImmutableSet.of(),
                ConstantFoldingRuleSet.ofSimplificationRules()).get();
        return (simplifiedPredicate instanceof ConstantPredicate constantPredicate)
               ? toEffectiveConstant(constantPredicate)
               : EffectiveConstant.UNKNOWN;
    }

    /**
     * Returns whether {@code predicate} provably rejects null at {@code alias}. This is a convenience
     * wrapper around {@link #foldPredicateAtNull}.
     */
    public static boolean rejectsNull(@Nonnull final QueryPredicate predicate,
                                      @Nonnull final CorrelationIdentifier alias,
                                      @Nonnull final EvaluationContext evaluationContext) {
        final EffectiveConstant outcome = foldPredicateAtNull(predicate, alias, evaluationContext);
        return outcome == EffectiveConstant.FALSE || outcome == EffectiveConstant.NULL;
    }

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

    public enum EffectiveConstant {
        /**
         * The boolean constant {@code TRUE}.
         */
        TRUE(true),
        /**
         * The boolean constant {@code UNKNOWN}.
         */
        FALSE(true),
        /**
         * The {@code NULL} value (or the boolean constant {@code UNKNOWN}).
         */
        NULL(true),
        /**
         * An unknown value that is not {@code NULL}.
         */
        NOT_NULL(false),
        /**
         * An unknown value (that may or may not be {@code NULL}).
         */
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
